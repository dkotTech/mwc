package ui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
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

func get(t *testing.T, h http.Handler, path string) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec.Result()
}

func body(t *testing.T, h http.Handler, path string) string {
	t.Helper()
	b, err := io.ReadAll(get(t, h, path).Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPage(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle, "db": idle})
	res := get(t, Handler(m), "/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content type = %q", ct)
	}

	got := body(t, Handler(m), "/")
	for _, want := range []string{"<title>mwc jobs</title>", "2 running", ">api<", ">db<", "running</span>"} {
		if !strings.Contains(got, want) {
			t.Errorf("page missing %q", want)
		}
	}
}

func TestPageWithoutJobs(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{})
	if got := body(t, Handler(m), "/"); !strings.Contains(got, "no jobs") {
		t.Errorf("empty page missing placeholder:\n%s", got)
	}
}

func TestJSON(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	res := get(t, Handler(m), "/api/status")
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content type = %q", ct)
	}

	var got status
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(got.Jobs))
	}
	job := got.Jobs[0]
	if job.Name != "api" || job.State != "running" || job.Restarts != 0 || job.LastError != "" {
		t.Errorf("job = %+v", job)
	}
	if job.ForMs < 0 || job.ForMs > time.Minute.Milliseconds() {
		t.Errorf("forMs = %dms, want a small positive number", job.ForMs)
	}
	// The rendered strings belong to the page, not to the JSON.
	if strings.Contains(body(t, Handler(m), "/api/status"), `"For"`) {
		t.Error("JSON must not carry the server-rendered fields")
	}
}

func TestFailingJobIsReported(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{
		"flaky": func(ctx context.Context) (*mwc.Job, error) {
			return mwc.Run(func() error { return errors.New("<b>boom</b> & bust") }, nil), nil
		},
	}, mwc.WithMaxRestarts(1))
	<-m.Done()

	page := body(t, Handler(m), "/")
	if !strings.Contains(page, "1 failed") || !strings.Contains(page, "failed</span>") {
		t.Errorf("page does not show the failure:\n%s", page)
	}
	// Job names and error text are attacker-controlled in the general case.
	if strings.Contains(page, "<b>boom</b>") {
		t.Errorf("error text was not escaped:\n%s", page)
	}
	if !strings.Contains(page, "&lt;b&gt;boom&lt;/b&gt; &amp; bust") {
		t.Errorf("escaped error text missing:\n%s", page)
	}

	var got status
	json.NewDecoder(get(t, Handler(m), "/api/status").Body).Decode(&got)
	if got.Jobs[0].LastError != "<b>boom</b> & bust" || got.Jobs[0].Restarts != 2 {
		t.Errorf("json job = %+v", got.Jobs[0])
	}
}

func TestThemeToggle(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	page := body(t, Handler(m), "/")
	for _, want := range []string{
		`id="theme"`,                  // the button
		`class="sun"`, `class="moon"`, // both icons ship with the page
		`:root[data-theme="dark"]`,      // an explicit choice overrides the system
		`(prefers-color-scheme: dark)`,  // and the system is still the default
		`localStorage.getItem("theme")`, // the choice survives a reload
	} {
		if !strings.Contains(page, want) {
			t.Errorf("page missing %q", want)
		}
	}
}

func TestFontIsServed(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	res := get(t, Handler(m), "/font.woff2")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "font/woff2" {
		t.Errorf("content type = %q", ct)
	}
	font, _ := io.ReadAll(res.Body)
	if len(font) < 1024 || string(font[:4]) != "wOF2" {
		t.Errorf("got %d bytes starting with %q, want a woff2 file", len(font), font[:min(4, len(font))])
	}
	if !strings.Contains(body(t, Handler(m), "/"), `url("font.woff2")`) {
		t.Error("the page does not reference the font it ships")
	}
	// The SIL Open Font License requires the license to travel with the font.
	if _, err := os.Stat("DepartureMono-LICENSE.txt"); err != nil {
		t.Errorf("font license missing from the package: %v", err)
	}
}

func TestUnknownPathIs404(t *testing.T) {
	m := manager(t, map[string]mwc.StartFunc{"api": idle})
	h := Handler(m)
	for _, path := range []string{"/nope", "/api", "/index.html"} {
		if code := get(t, h, path).StatusCode; code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, code)
		}
	}
}

func TestServeStartsAndStopsWithManager(t *testing.T) {
	addr := freeAddr(t)
	m := manager(t, map[string]mwc.StartFunc{"api": idle}, Serve(addr))

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}
	var res *http.Response
	var err error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if res, err = client.Get("http://" + addr); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		t.Fatalf("status page never came up: %v", err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(page), ">api<") {
		t.Errorf("page missing the job:\n%s", page)
	}

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get("http://" + addr); err == nil {
		t.Error("status page still served after Shutdown")
	}
}

func TestServeOutlivesJobsThatFailedForGood(t *testing.T) {
	addr := freeAddr(t)
	boom := func(ctx context.Context) (*mwc.Job, error) {
		return mwc.Run(func() error { return errors.New("boom") }, nil), nil
	}
	m, err := mwc.Start(context.Background(), map[string]mwc.StartFunc{"api": boom},
		mwc.WithLogger(discardLogger()),
		mwc.WithBackoff(time.Millisecond, time.Millisecond),
		mwc.WithMaxRestarts(1),
		Serve(addr))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the job never gave up")
	}

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}
	res, err := client.Get("http://" + addr)
	if err != nil {
		t.Fatalf("page gone once every job had failed: %v", err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(page), "failed") {
		t.Errorf("page does not report the failure:\n%s", page)
	}

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get("http://" + addr); err == nil {
		t.Error("status page still served after Shutdown")
	}
}

func TestListenErrorDoesNotBreakTheManager(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	// The failure has to reach the logger the application configured, not
	// whatever slog.Default() happens to be.
	logs := &safeBuf{}
	m, err := mwc.Start(context.Background(), map[string]mwc.StartFunc{"api": idle},
		mwc.WithLogger(slog.New(slog.NewTextHandler(logs, nil))), Serve(busy.Addr().String()))
	if err != nil {
		t.Fatalf("a busy UI port must not fail Start: %v", err)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "cannot listen") {
		t.Errorf("listen failure was not logged: %q", logs.String())
	}
}

// freeAddr returns an address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}
