package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
)

type snapshot struct {
	Result
	etag, lastModified string
}

func (f *Fetcher) loadSnapshot(ctx context.Context, redacted string) (snapshot, bool, error) {
	var cached snapshot
	cached.URL = redacted
	err := f.Pool.QueryRow(ctx, `
		SELECT etag, last_modified, content_hash, snapshot_ref, bytes, fetched_at
		FROM fetch_snapshots WHERE url=$1`, redacted).Scan(
		&cached.etag, &cached.lastModified, &cached.ContentHash, &cached.SnapshotRef, &cached.Bytes, &cached.FetchedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return snapshot{}, false, nil
	}
	if err != nil {
		return snapshot{}, false, safeFailure("load snapshot metadata failed", err)
	}
	if len(cached.ContentHash) != sha256.Size || cached.Bytes < 0 {
		return snapshot{}, false, errors.New("invalid cached snapshot metadata")
	}
	path, err := f.snapshotPath(cached.ContentHash)
	if err != nil {
		return snapshot{}, false, err
	}
	cached.Path = path
	return cached, true, nil
}

func (f *Fetcher) snapshotPath(hash []byte) (string, error) {
	hexHash := hex.EncodeToString(hash)
	path, err := filepath.Abs(filepath.Join(f.SnapshotDir, hexHash[:2], hexHash))
	if err != nil {
		return "", safeFailure("resolve snapshot path failed", err)
	}
	return path, nil
}

func (f *Fetcher) get(ctx context.Context, rawURL, redacted string) (Result, error) {
	u, err := url.Parse(rawURL)
	if err != nil || !validURL(u) {
		return Result{}, errors.New("invalid HTTP URL")
	}
	if f.Pool == nil || f.SnapshotDir == "" {
		return Result{}, errors.New("Pool and SnapshotDir are required")
	}
	if f.Concurrency < 0 || f.MaxRetries < 0 || f.MaxBytes < 0 || f.MaxBytes == math.MaxInt64 {
		return Result{}, errors.New("invalid fetcher limits")
	}
	cached, exists, err := f.loadSnapshot(ctx, redacted)
	if err != nil {
		return Result{}, err
	}
	headers := make(http.Header)
	if exists {
		if cached.etag != "" {
			headers.Set("If-None-Match", cached.etag)
		}
		if cached.lastModified != "" {
			headers.Set("If-Modified-Since", cached.lastModified)
		}
	}
	for unconditional := false; ; unconditional = true {
		resp, err := f.request(ctx, u, headers, true)
		if err != nil {
			return Result{}, err
		}
		if resp.StatusCode == http.StatusNotModified {
			resp.Body.Close()
			if !exists || unconditional {
				return Result{}, errors.New("unexpected HTTP 304 without a usable snapshot")
			}
			info, statErr := os.Stat(cached.Path)
			if statErr != nil || !info.Mode().IsRegular() || info.Size() != cached.Bytes {
				// A conditional response cannot restore an evicted snapshot file.
				headers = make(http.Header)
				continue
			}
			_, err := f.Pool.Exec(ctx, `UPDATE fetch_snapshots SET checked_at=$2
				WHERE url=$1 AND content_hash=$3`, redacted, f.now(), cached.ContentHash)
			if err != nil {
				return Result{}, safeFailure("update snapshot check time failed", err)
			}
			cached.NotModified = true
			return cached.Result, nil
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return Result{}, fmt.Errorf("HTTP status %d", resp.StatusCode)
		}
		result, err := f.writeSnapshot(ctx, resp, redacted)
		resp.Body.Close()
		if err != nil {
			return Result{}, err
		}
		_, err = f.Pool.Exec(ctx, `
			INSERT INTO fetch_snapshots
			(url, etag, last_modified, content_hash, snapshot_ref, bytes, fetched_at, checked_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$7)
			ON CONFLICT (url) DO UPDATE SET
			etag=EXCLUDED.etag, last_modified=EXCLUDED.last_modified,
			content_hash=EXCLUDED.content_hash, snapshot_ref=EXCLUDED.snapshot_ref,
			bytes=EXCLUDED.bytes, fetched_at=EXCLUDED.fetched_at, checked_at=EXCLUDED.checked_at`,
			redacted, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), result.ContentHash,
			result.SnapshotRef, result.Bytes, result.FetchedAt,
		)
		if err != nil {
			return Result{}, safeFailure("save snapshot metadata failed", err)
		}
		return result, nil
	}
}

func (f *Fetcher) writeSnapshot(ctx context.Context, resp *http.Response, redacted string) (Result, error) {
	maxBytes := f.MaxBytes
	if maxBytes == 0 {
		maxBytes = 2 << 30
	}
	if err := os.MkdirAll(f.SnapshotDir, 0o755); err != nil {
		return Result{}, safeFailure("create snapshot directory failed", err)
	}
	file, err := os.CreateTemp(f.SnapshotDir, ".fetch-*")
	if err != nil {
		return Result{}, safeFailure("create temporary snapshot failed", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return Result{}, safeFailure("stream snapshot failed", err)
	}
	if n > maxBytes {
		return Result{}, ErrTooLarge
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := file.Sync(); err != nil {
		return Result{}, safeFailure("sync snapshot failed", err)
	}
	if err := file.Close(); err != nil {
		return Result{}, safeFailure("close snapshot failed", err)
	}
	contentHash := hash.Sum(nil)
	path, err := f.snapshotPath(contentHash)
	if err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Result{}, safeFailure("create content directory failed", err)
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return Result{}, safeFailure("publish snapshot failed", err)
	}
	return Result{
		URL: redacted, SnapshotRef: "sha256:" + hex.EncodeToString(contentHash), Path: path,
		ContentHash: contentHash, Bytes: n, FetchedAt: f.now(),
	}, nil
}
