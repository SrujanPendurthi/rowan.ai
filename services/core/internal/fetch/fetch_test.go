package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"rowan.ai/rowan/services/core/internal/testdb"
)

func newTestFetcher(t *testing.T, server *httptest.Server) *Fetcher {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &Fetcher{
		Pool: testdb.New(t), Client: server.Client(), SnapshotDir: t.TempDir(),
		HostRate: map[string]rate.Limit{u.Host: rate.Inf},
	}
}

func TestGetStoresSnapshot(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", "Thu, 19 Nov 2026 12:00:00 GMT")
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	got, err := f.Get(context.Background(), s.URL+"/x")
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("hello"))
	hexHash := hex.EncodeToString(hash[:])
	body, err := os.ReadFile(got.Path)
	if err != nil || string(body) != "hello" || got.SnapshotRef != "sha256:"+hexHash || got.Bytes != 5 || !filepath.IsAbs(got.Path) || got.NotModified || got.FetchedAt.IsZero() || hex.EncodeToString(got.ContentHash) != hexHash {
		t.Fatalf("snapshot = %+v, body=%q, err=%v", got, body, err)
	}
	if got.Path != filepath.Join(f.SnapshotDir, hexHash[:2], hexHash) {
		t.Fatalf("unexpected content-addressed path: %s", got.Path)
	}
	var etag, modified, ref string
	var storedHash []byte
	if err := f.Pool.QueryRow(context.Background(), "SELECT etag, last_modified, snapshot_ref, content_hash FROM fetch_snapshots WHERE url=$1", got.URL).Scan(&etag, &modified, &ref, &storedHash); err != nil {
		t.Fatal(err)
	}
	if etag != `"v1"` || modified != "Thu, 19 Nov 2026 12:00:00 GMT" || ref != got.SnapshotRef || hex.EncodeToString(storedHash) != hexHash {
		t.Fatalf("stored snapshot mismatch: %s %s %s %x", etag, modified, ref, storedHash)
	}
}

func TestGetConditional304(t *testing.T) {
	var hits int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		hits++
		if hits > 1 {
			if r.Header.Get("If-None-Match") != `"v1"` || r.Header.Get("If-Modified-Since") != "Thu, 19 Nov 2026 12:00:00 GMT" {
				t.Error("conditional headers missing")
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", "Thu, 19 Nov 2026 12:00:00 GMT")
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	f.Now = func() time.Time { return now }
	first, err := f.Get(context.Background(), s.URL+"/x")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	second, err := f.Get(context.Background(), s.URL+"/x")
	if err != nil || !second.NotModified || second.Path != first.Path || !second.FetchedAt.Equal(first.FetchedAt) {
		t.Fatalf("conditional result = %+v, %v", second, err)
	}
	var checked time.Time
	if err := f.Pool.QueryRow(context.Background(), "SELECT checked_at FROM fetch_snapshots WHERE url=$1", first.URL).Scan(&checked); err != nil || !checked.Equal(now) {
		t.Fatalf("checked_at = %v, %v", checked, err)
	}
}

func TestGetRobotsDisallow(t *testing.T) {
	testRobotsStatus(t, 200, "User-agent: rowanbot\nDisallow: /x", true)
}

func TestGetRobots404AllowsAll(t *testing.T) { testRobotsStatus(t, 404, "", false) }
func TestGetRobots500Disallows(t *testing.T) { testRobotsStatus(t, 500, "", true) }

func testRobotsStatus(t *testing.T, status int, body string, disallow bool) {
	t.Helper()
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.WriteHeader(status)
			fmt.Fprint(w, body)
			return
		}
		hits.Add(1)
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	f.sleep = func(time.Duration) {}
	_, err := f.Get(context.Background(), s.URL+"/x")
	if disallow {
		if !errors.Is(err, ErrDisallowed) || hits.Load() != 0 {
			t.Fatalf("disallowed request: hits=%d, err=%v", hits.Load(), err)
		}
	} else if err != nil || hits.Load() != 1 {
		t.Fatalf("allowed request: hits=%d, err=%v", hits.Load(), err)
	}
}

func TestGetRetriesRetryAfter(t *testing.T) {
	var hits int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		hits++
		if hits == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	var waited time.Duration
	f.sleep = func(d time.Duration) { waited += d }
	if _, err := f.Get(context.Background(), s.URL+"/x"); err != nil || hits != 2 || waited < time.Second {
		t.Fatalf("retry: hits=%d, waited=%v, err=%v", hits, waited, err)
	}
}

func TestGetPerHostConcurrency(t *testing.T) {
	var inFlight, maximum atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Go(func() {
			if _, err := f.Get(context.Background(), fmt.Sprintf("%s/%d", s.URL, i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if maximum.Load() > 2 || maximum.Load() == 0 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
}

func TestGetTooLarge(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "0123456789")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	f.MaxBytes = 4
	if _, err := f.Get(context.Background(), s.URL+"/x"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("error = %v", err)
	}
	files, err := os.ReadDir(f.SnapshotDir)
	if err != nil || len(files) != 0 {
		t.Fatalf("snapshot directory = %v, %v", files, err)
	}
}

func TestRedactsSecretsInURL(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("api_key") != "SECRET" {
			t.Error("outbound key was redacted")
		}
		fmt.Fprint(w, "hello")
	}))
	f := newTestFetcher(t, s)
	f.sleep = func(time.Duration) {}
	raw := s.URL + "/x?api_key=SECRET&key=SECRET&token=SECRET&date=11/2026"
	got, err := f.Get(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := f.Pool.QueryRow(context.Background(), "SELECT url FROM fetch_snapshots").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != got.URL || strings.Contains(stored, "SECRET") || !strings.Contains(stored, "api_key=REDACTED") {
		t.Fatalf("redaction: %q, %q", stored, got.URL)
	}
	s.Close()
	if _, err := f.Get(context.Background(), raw); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("unsafe network error = %v", err)
	}
}

func TestGetSendsUserAgent(t *testing.T) {
	for _, custom := range []string{"", "CustomFetcher/2"} {
		t.Run(custom, func(t *testing.T) {
			want := custom
			if want == "" {
				want = "RowanBot/1.0 (+https://rowan.ai/bot)"
			}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("User-Agent") != want {
					t.Errorf("User-Agent = %q", r.Header.Get("User-Agent"))
				}
				if r.URL.Path == "/robots.txt" {
					http.NotFound(w, r)
					return
				}
				fmt.Fprint(w, "hello")
			}))
			defer s.Close()
			f := newTestFetcher(t, s)
			f.UserAgent = custom
			if _, err := f.Get(context.Background(), s.URL+"/x"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGet304MissingFileRefetchesOnce(t *testing.T) {
	var hits int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		hits++
		if hits == 2 {
			if r.Header.Get("If-None-Match") != `"v1"` {
				t.Error("missing conditional request")
			}
			w.WriteHeader(304)
			return
		}
		if hits == 3 && (r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "") {
			t.Error("recovery request must be unconditional")
		}
		w.Header().Set("ETag", `"v1"`)
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	first, err := f.Get(context.Background(), s.URL+"/x")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(first.Path); err != nil {
		t.Fatal(err)
	}
	second, err := f.Get(context.Background(), s.URL+"/x")
	if err != nil || second.NotModified || hits != 3 || second.Path != first.Path {
		t.Fatalf("recovery = %+v, hits=%d, err=%v", second, hits, err)
	}
	if body, err := os.ReadFile(second.Path); err != nil || string(body) != "hello" {
		t.Fatalf("recovered body = %q, %v", body, err)
	}
}

func TestGetRobotsCacheAndPortIsolation(t *testing.T) {
	var robotsHits, targetHits atomic.Int32
	var disallow atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			robotsHits.Add(1)
			if disallow.Load() {
				fmt.Fprint(w, "User-agent: *\nDisallow: /")
			}
			return
		}
		targetHits.Add(1)
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	f.Now = func() time.Time { return now }
	for range 2 {
		if _, err := f.Get(context.Background(), s.URL+"/x"); err != nil {
			t.Fatal(err)
		}
	}
	disallow.Store(true)
	now = now.Add(24*time.Hour - time.Nanosecond)
	if _, err := f.Get(context.Background(), s.URL+"/x"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Nanosecond)
	if _, err := f.Get(context.Background(), s.URL+"/x"); !errors.Is(err, ErrDisallowed) {
		t.Fatalf("expired policy: %v", err)
	}
	if robotsHits.Load() != 2 || targetHits.Load() != 3 {
		t.Fatalf("cache hits robots=%d, target=%d", robotsHits.Load(), targetHits.Load())
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "other")
	}))
	defer other.Close()
	u, _ := url.Parse(other.URL)
	f.HostRate[u.Host] = rate.Inf
	if _, err := f.Get(context.Background(), other.URL+"/x"); err != nil {
		t.Fatalf("port reused another host's robots: %v", err)
	}
}

func TestGetRobotsNetworkFailureDisallows(t *testing.T) {
	var targetHits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		targetHits.Add(1)
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	f.sleep = func(time.Duration) {}
	if _, err := f.Get(context.Background(), s.URL+"/x?api_key=SECRET"); !errors.Is(err, ErrDisallowed) || strings.Contains(err.Error(), "SECRET") || targetHits.Load() != 0 {
		t.Fatalf("network robots failure: %v, hits=%d", err, targetHits.Load())
	}
}

func TestGetRedirectChecksTargetRobots(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			fmt.Fprint(w, "User-agent: rowanbot\nDisallow: /private")
			return
		}
		targetHits.Add(1)
		fmt.Fprint(w, "hello")
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, target.URL+"/private?token=SECRET", http.StatusFound)
	}))
	defer source.Close()
	f := newTestFetcher(t, source)
	u, _ := url.Parse(target.URL)
	f.HostRate[u.Host] = rate.Inf
	if _, err := f.Get(context.Background(), source.URL+"/x"); !errors.Is(err, ErrDisallowed) || strings.Contains(err.Error(), "SECRET") || targetHits.Load() != 0 {
		t.Fatalf("redirect bypassed robots: %v, hits=%d", err, targetHits.Load())
	}
}

func TestGetRetryStatusesAndLimits(t *testing.T) {
	for _, tc := range []struct {
		status, wantHits int
		retryAfter       string
		wantDelay        time.Duration
	}{
		{503, 3, "9999", 10 * time.Minute},
		{429, 3, "invalid", 3 * time.Second},
		{500, 1, "", 0},
		{404, 1, "", 0},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var hits int
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/robots.txt" {
					http.NotFound(w, r)
					return
				}
				hits++
				w.Header().Set("Retry-After", tc.retryAfter)
				w.WriteHeader(tc.status)
			}))
			defer s.Close()
			f := newTestFetcher(t, s)
			f.MaxRetries = 2
			var waited time.Duration
			f.sleep = func(d time.Duration) { waited += d }
			if _, err := f.Get(context.Background(), s.URL+"/x?token=SECRET"); err == nil || strings.Contains(err.Error(), "SECRET") || hits != tc.wantHits || waited != tc.wantDelay {
				t.Fatalf("status=%d hits=%d delay=%v err=%v", tc.status, hits, waited, err)
			}
		})
	}
}

func TestGetCancellationDuringRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(429)
		cancel()
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	if _, err := f.Get(ctx, s.URL+"/x?key=SECRET"); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestHostDefaultsAndRateDeadline(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("request should not reach server") }))
	defer s.Close()
	f := newTestFetcher(t, s)
	f.HostRate = nil
	u, _ := url.Parse(s.URL)
	host, err := f.host(u)
	if err != nil {
		t.Fatal(err)
	}
	if host.limiter.Limit() != 1 || host.limiter.Burst() != 1 || cap(host.semaphore) != 2 {
		t.Fatal("incorrect host defaults")
	}
	if !host.limiter.Allow() {
		t.Fatal("initial burst missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var sleeps int
	f.sleep = func(time.Duration) { sleeps++ }
	if _, err := f.request(ctx, u, nil, false); err == nil {
		t.Fatal("rate wait must respect deadline")
	}
	if sleeps != 0 {
		t.Fatalf("rate wait is not a network error; retried %d times", sleeps)
	}
}

func TestRedactURL(t *testing.T) {
	for _, raw := range []string{
		"https://example.invalid/x?api_key=SECRET&api_key=OTHER&key=SECRET&token=SECRET&date=11/2026",
		"https://example.invalid/x?%61pi_key=SECRET&Token=SECRET",
		"https://example.invalid/x?api_key=SECRET;bad",
		"https://example.invalid/%zz?token=SECRET",
	} {
		if got := RedactURL(raw); strings.Contains(got, "SECRET") || strings.Contains(got, "OTHER") {
			t.Fatalf("unsafe redaction: %q", got)
		}
	}
}

func TestRetryDelayBackoff(t *testing.T) {
	if got := retryDelay("", 10); got != 1024*time.Second {
		t.Fatalf("backoff = %v, want 1024s", got)
	}
	if got := retryDelay("184467440737095516160", 0); got != 5*time.Minute {
		t.Fatalf("overflow Retry-After = %v", got)
	}
}

func TestRequestDoesNotRetryInvalidHeaders(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid request reached server") }))
	defer s.Close()
	f := newTestFetcher(t, s)
	f.UserAgent = "invalid\nheader"
	var sleeps int
	f.sleep = func(time.Duration) { sleeps++ }
	u, _ := url.Parse(s.URL)
	if _, err := f.request(context.Background(), u, nil, false); err == nil {
		t.Fatal("invalid header must fail")
	}
	if sleeps != 0 {
		t.Fatalf("invalid headers are not network failures; retried %d times", sleeps)
	}
}

func TestGetRetriesNetworkFailure(t *testing.T) {
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		if hits.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, "hello")
	}))
	defer s.Close()
	f := newTestFetcher(t, s)
	var waited time.Duration
	f.sleep = func(d time.Duration) { waited += d }
	if _, err := f.Get(context.Background(), s.URL+"/x"); err != nil || hits.Load() != 2 || waited != time.Second {
		t.Fatalf("network retry: hits=%d wait=%v err=%v", hits.Load(), waited, err)
	}
}

func TestGetSizeBoundaryAndStreamFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
		fail bool
	}{
		{"exact-limit", 4, false}, {"truncated-body", 100, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/robots.txt" {
					http.NotFound(w, r)
					return
				}
				if tc.fail {
					w.Header().Set("Content-Length", "10")
				}
				fmt.Fprint(w, "data")
			}))
			defer s.Close()
			f := newTestFetcher(t, s)
			f.MaxBytes = tc.size
			got, err := f.Get(context.Background(), s.URL+"/x?token=SECRET")
			if !tc.fail {
				if err != nil || got.Bytes != 4 {
					t.Fatalf("exact limit: %+v %v", got, err)
				}
				return
			}
			if err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("stream error = %v", err)
			}
			files, readErr := os.ReadDir(f.SnapshotDir)
			if readErr != nil || len(files) != 0 {
				t.Fatalf("temporary files remain: %v %v", files, readErr)
			}
			var rows int
			if err := f.Pool.QueryRow(context.Background(), "SELECT count(*) FROM fetch_snapshots").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("failed stream persisted metadata: rows=%d err=%v", rows, err)
			}
		})
	}
}
