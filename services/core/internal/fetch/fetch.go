// Package fetch downloads public source files with host limits and durable snapshots.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/time/rate"
)

var ErrDisallowed = errors.New("fetch: disallowed by robots.txt")
var ErrTooLarge = errors.New("fetch: response exceeds MaxBytes")

const defaultUserAgent = "RowanBot/1.0 (+https://rowan.ai/bot)"

// Fetcher configuration must be set before the first Get and remain immutable.
// A Fetcher must not be copied after first use.
type Fetcher struct {
	Pool        *pgxpool.Pool
	Client      *http.Client // nil defaults to http.DefaultClient.
	SnapshotDir string
	UserAgent   string                // Empty defaults to RowanBot/1.0 (+https://rowan.ai/bot).
	HostRate    map[string]rate.Limit // Host including port; defaults to 1/s, burst 1.
	Concurrency int                   // Per host; zero defaults to 2.
	MaxBytes    int64                 // Zero defaults to 2 GiB.
	MaxRetries  int                   // Zero defaults to 3 retries after the first attempt.
	Now         func() time.Time

	sleep func(time.Duration)
	mu    sync.Mutex
	hosts map[string]*hostState
}

type Result struct {
	URL, SnapshotRef, Path string
	ContentHash            []byte
	Bytes                  int64
	NotModified            bool
	FetchedAt              time.Time
}

type hostState struct {
	limiter      *rate.Limiter
	semaphore    chan struct{}
	robotsLock   chan struct{}
	robots       rules
	robotsAt     time.Time
	robotsLoaded bool
}

func (f *Fetcher) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// RedactURL removes credential query values before a URL crosses the fetch boundary.
// Invalid URLs return a constant because parse errors can contain the original URL.
func RedactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "[invalid URL]"
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		u.RawQuery = "REDACTED"
	} else {
		for key := range q {
			switch strings.ToLower(key) {
			case "api_key", "key", "token":
				q.Set(key, "REDACTED")
			}
		}
		u.RawQuery = q.Encode()
	}
	u.User = nil
	u.Fragment, u.RawFragment = "", ""
	return u.String()
}

// safeError retains errors.Is/As behavior without printing an unsafe underlying error.
type safeError struct {
	message string
	cause   error
}

func (e *safeError) Error() string { return e.message }
func (e *safeError) Unwrap() error { return e.cause }

func safeFailure(operation string, err error) error {
	return &safeError{message: operation, cause: err}
}

func validURL(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil
}

func (f *Fetcher) host(u *url.URL) (*hostState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.ToLower(u.Host) // Include the port; separate servers need separate policies.
	if state := f.hosts[key]; state != nil {
		return state, nil
	}
	limit := rate.Limit(1)
	if override, ok := f.HostRate[key]; ok {
		limit = override
	} else if override, ok := f.HostRate[u.Host]; ok {
		limit = override
	}
	if limit <= 0 || math.IsNaN(float64(limit)) {
		return nil, errors.New("fetch: HostRate must be positive")
	}
	concurrency := f.Concurrency
	if concurrency == 0 {
		concurrency = 2
	}
	state := &hostState{
		limiter: rate.NewLimiter(limit, 1), semaphore: make(chan struct{}, concurrency),
		robotsLock: make(chan struct{}, 1),
	}
	if f.hosts == nil {
		f.hosts = make(map[string]*hostState)
	}
	f.hosts[key] = state
	return state, nil
}

func (f *Fetcher) wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.sleep != nil {
		f.sleep(duration)
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.ParseUint(strings.TrimSpace(header), 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		return time.Duration(min(seconds, 300)) * time.Second
	}
	// Saturation prevents overflow for unusually large configured retry counts.
	if attempt >= 34 {
		return time.Duration(math.MaxInt64)
	}
	return time.Second * time.Duration(uint64(1)<<attempt)
}

func retryableNetworkError(err error) bool {
	var requestErr *url.Error
	if !errors.As(err, &requestErr) {
		return false
	}
	var networkErr net.Error
	return errors.As(requestErr.Err, &networkErr) || errors.Is(requestErr.Err, io.EOF) || errors.Is(requestErr.Err, io.ErrUnexpectedEOF)
}

type limitedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *limitedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

func (f *Fetcher) attempt(ctx context.Context, u *url.URL, headers http.Header, host *hostState) (*http.Response, error) {
	select {
	case host.semaphore <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-host.semaphore }
	if err := host.limiter.Wait(ctx); err != nil {
		release()
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		release()
		return nil, safeFailure("fetch: create request failed", err)
	}
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	userAgent := f.UserAgent
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	req.Header.Set("User-Agent", userAgent)
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	// Follow redirects ourselves so every hop observes its own host limits and robots.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := copyClient.Do(req)
	if err != nil {
		release()
		return nil, safeFailure("fetch: HTTP request failed", err)
	}
	resp.Body = &limitedBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}

func (f *Fetcher) request(ctx context.Context, u *url.URL, headers http.Header, obeyRobots bool) (*http.Response, error) {
	maxRetries := f.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	for redirects := 0; redirects <= 10; redirects++ {
		if !validURL(u) {
			return nil, errors.New("fetch: invalid HTTP URL")
		}
		host, err := f.host(u)
		if err != nil {
			return nil, err
		}
		if obeyRobots {
			if err := f.checkRobots(ctx, u, host); err != nil {
				return nil, err
			}
		}
		var resp *http.Response
		for attempt := 0; ; attempt++ {
			resp, err = f.attempt(ctx, u, headers, host)
			if ctx.Err() != nil {
				if resp != nil {
					resp.Body.Close()
				}
				return nil, ctx.Err()
			}
			retry := retryableNetworkError(err) || err == nil && (resp.StatusCode == 429 || resp.StatusCode == 503)
			if !retry || attempt >= maxRetries {
				break
			}
			var retryAfter string
			if resp != nil {
				retryAfter = resp.Header.Get("Retry-After")
				resp.Body.Close()
			}
			if err := f.wait(ctx, retryDelay(retryAfter, attempt)); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		switch resp.StatusCode {
		case 301, 302, 303, 307, 308:
			location := resp.Header.Get("Location")
			resp.Body.Close()
			if location == "" {
				return nil, errors.New("fetch: redirect missing Location")
			}
			next, parseErr := u.Parse(location)
			if parseErr != nil {
				return nil, safeFailure("fetch: invalid redirect URL", parseErr)
			}
			u = next
		default:
			return resp, nil
		}
	}
	return nil, errors.New("fetch: too many redirects")
}

// Get downloads rawURL, returning only a redacted URL and a durable snapshot.
func (f *Fetcher) Get(ctx context.Context, rawURL string) (Result, error) {
	redacted := RedactURL(rawURL)
	result, err := f.get(ctx, rawURL, redacted)
	if err != nil {
		return Result{}, fmt.Errorf("fetch %s: %w", redacted, err)
	}
	return result, nil
}
