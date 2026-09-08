package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dkotTech/mwc"
)

func TestMain(m *testing.M) {
	slog.SetDefault(discardLogger())
	os.Exit(m.Run())
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func idle(ctx context.Context) (*mwc.Job, error) {
	return mwc.Run(func() error { <-ctx.Done(); return nil }, nil), nil
}

// manager starts a supervisor for the given jobs and stops it when the test ends.
func manager(t *testing.T, jobs map[string]mwc.StartFunc, opts ...mwc.Option) *mwc.Manager {
	t.Helper()
	opts = append(opts, mwc.WithLogger(discardLogger()), mwc.WithBackoff(time.Millisecond, 5*time.Millisecond))
	m, err := mwc.Start(context.Background(), jobs, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Shutdown(context.Background()) })
	return m
}

func probe(t *testing.T, m *mwc.Manager, method string) (*http.Response, Response) {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler(m).ServeHTTP(rec, httptest.NewRequest(method, "/healthz", nil))
	res := rec.Result()
	var body Response
	if method == http.MethodGet {
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
	}
	return res, body
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestHealthy(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle, "db": idle})
	res, body := probe(t, m, http.MethodGet)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q", ct)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control = %q", cc)
	}
	if !body.Healthy || len(body.Jobs) != 0 {
		t.Errorf("body = %+v, want healthy and no jobs", body)
	}

	rec := httptest.NewRecorder()
	Handler(m).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if raw := rec.Body.String(); strings.Contains(raw, `"jobs"`) {
		t.Errorf("a healthy response must not carry an empty jobs list: %s", raw)
	}
}

func TestUnhealthyListsTheJobs(t *testing.T) {
	boom := func(ctx context.Context) (*mwc.Job, error) {
		return mwc.Run(func() error { return errors.New("boom") }, nil), nil
	}
	m := manager(t, map[string]mwc.StartFunc{"api": idle, "mailer": boom, "cron": boom}, mwc.WithMaxRestarts(1))
	waitFor(t, func() bool {
		st := m.Status()
		return st["mailer"].State == mwc.Failed && st["cron"].State == mwc.Failed
	})

	res, body := probe(t, m, http.MethodGet)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if body.Healthy || len(body.Jobs) != 2 {
		t.Fatalf("body = %+v, want unhealthy with two jobs", body)
	}
	if body.Jobs[0].Name != "cron" || body.Jobs[1].Name != "mailer" {
		t.Errorf("jobs not sorted by name: %+v", body.Jobs)
	}
	if j := body.Jobs[1]; j.State != "failed" || j.Restarts != 2 || j.LastError != "boom" {
		t.Errorf("job = %+v", j)
	}
}

func TestCleanExitIsHealthy(t *testing.T) {
	done := func(ctx context.Context) (*mwc.Job, error) {
		return mwc.Run(func() error { return nil }, nil), nil
	}
	m := manager(t, map[string]mwc.StartFunc{"migrate": done, "api": idle})
	waitFor(t, func() bool { return m.Status()["migrate"].State == mwc.Exited })
	if res, _ := probe(t, m, http.MethodGet); res.StatusCode != http.StatusOK {
		t.Errorf("status = %d after a clean exit, want 200", res.StatusCode)
	}
}

func TestShutdownIsUnhealthy(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, body := probe(t, m, http.MethodGet)
	if res.StatusCode != http.StatusServiceUnavailable || len(body.Jobs) != 1 || body.Jobs[0].State != "stopped" {
		t.Errorf("status = %d, body = %+v; want 503 with the stopped job", res.StatusCode, body)
	}
}

func TestHead(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	rec := httptest.NewRecorder()
	Handler(m).ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Errorf("HEAD: status %d, %d body bytes; want 200 and no body", rec.Code, rec.Body.Len())
	}
}

func TestOtherMethods(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		Handler(m).ServeHTTP(rec, httptest.NewRequest(method, "/healthz", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", method, rec.Code)
		}
	}
}
