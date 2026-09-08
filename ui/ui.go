// Package ui serves the state of a mwc.Manager as a single HTML page.
//
// Serve returns a mwc.Option, so the whole UI is one argument to Start:
//
//	m, err := mwc.Start(ctx, jobs, ui.Serve("127.0.0.1:8080"))
//
// The page has no external resources: it renders Status server-side, then
// polls /api/status. It stays up until the Manager is shut down.
//
// It shows job names and error messages, so bind it to loopback or an
// otherwise trusted network.
//
// The typeface is Departure Mono by Helena Zhang, under the SIL Open Font
// License 1.1 (DepartureMono-LICENSE.txt).
package ui

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/dkotTech/mwc"
)

//go:embed page.html
var pageHTML string

// A pixel typeface on an 11px grid, hence the 11s in the page.
//
//go:embed departure-mono.woff2
var fontWOFF2 []byte

var page = template.Must(template.New("page").Parse(pageHTML))

// stopGrace is short on purpose: mwc.Manager.Shutdown waits for it.
const stopGrace = time.Second

// Serve returns a mwc.Option that serves the status page on addr until the
// Manager is shut down. A listen error is logged, not fatal.
func Serve(addr string) mwc.Option {
	return mwc.WithObserver(func(m *mwc.Manager) {
		log := m.Logger()
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("status ui: cannot listen", "addr", addr, "err", err)
			return
		}
		srv := &http.Server{Handler: Handler(m), ReadHeaderTimeout: 5 * time.Second}
		served := make(chan struct{})
		go func() {
			defer close(served)
			if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				log.Error("status ui: serve failed", "err", err)
			}
		}()
		log.Info("status ui listening", "url", "http://"+ln.Addr().String())

		// Stopping, not Done: the page matters most once the jobs are gone.
		<-m.Stopping()
		ctx, cancel := context.WithTimeout(context.Background(), stopGrace)
		defer cancel()
		if srv.Shutdown(ctx) != nil {
			srv.Close()
		}
		<-served
	})
}

// Handler serves the page at "/" and the same data at "/api/status":
//
//	mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(m)))
func Handler(m *mwc.Manager) http.Handler {
	log := m.Logger()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := page.Execute(w, snapshot(m)); err != nil {
			log.Error("status ui: render failed", "err", err)
		}
	})
	mux.HandleFunc("GET /font.woff2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "font/woff2")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		w.Write(fontWOFF2)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(snapshot(m))
	})
	return mux
}

// status is both the template data and the JSON payload.
type status struct {
	Summary string `json:"-"`
	Jobs    []job  `json:"jobs"`
}

type job struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Restarts  int    `json:"restarts"`
	For       string `json:"-"`
	ForMs     int64  `json:"forMs"`
	LastError string `json:"lastError,omitempty"`
}

func snapshot(m *mwc.Manager) status {
	now := time.Now()
	jobs := m.Status()
	out := status{Jobs: []job{}}
	counts := map[string]int{}

	for _, name := range slices.Sorted(maps.Keys(jobs)) {
		j := jobs[name]
		// An age, not a timestamp: the browser's clock may differ.
		age := max(now.Sub(j.Since), 0)
		row := job{
			Name:     name,
			State:    j.State.String(),
			Restarts: j.Restarts,
			For:      age.Truncate(time.Second).String(),
			ForMs:    age.Milliseconds(),
		}
		if j.LastErr != nil {
			row.LastError = j.LastErr.Error()
		}
		counts[row.State]++
		out.Jobs = append(out.Jobs, row)
	}

	var parts []string
	for _, state := range []string{"running", "restarting", "failed", "exited", "stopped"} {
		if n := counts[state]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, state))
		}
	}
	out.Summary = strings.Join(parts, " · ")
	return out
}
