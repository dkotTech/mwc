// Command example supervises two jobs and shows their state at
// http://127.0.0.1:8080. The api job at http://127.0.0.1:8081 serves the
// readiness probe at /healthz and the Prometheus metrics at /metrics. It logs
// through zap, to show any logger fits.
//
//	cd example && go run .
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/dkotTech/mwc"
	"github.com/dkotTech/mwc/health"
	"github.com/dkotTech/mwc/metrics"
	"github.com/dkotTech/mwc/ui"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

func main() {
	cfg := zap.NewDevelopmentConfig() // JSON on stderr
	zl, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer zl.Sync()
	// zapslog stacktraces Errors by default; a failing job is not a crash.
	noStacks := zapslog.AddStacktraceAt(slog.LevelError + 4)
	log := slog.New(zapslog.NewHandler(zl.Core(), noStacks))

	// The application's own registry; the api job below serves it at /metrics.
	reg := prometheus.NewRegistry()
	prom := metrics.New()
	reg.MustRegister(prom)

	var manager atomic.Pointer[mwc.Manager]
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		m := manager.Load()
		if m == nil {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		health.Handler(m).ServeHTTP(w, r)
	})

	// Main runs the jobs until Ctrl-C and shuts them down; the error is a
	// failed Start, a failed Shutdown or a job that gave up.
	err = mwc.Main(map[string]mwc.StartFunc{"api": api(mux), "mailer": mailer},
		mwc.WithLogger(log),
		mwc.WithJitter(0.2),
		// The api is the process: if it cannot be kept up, stop everything
		// and let the orchestrator restart the whole thing.
		mwc.WithJob("api", mwc.MaxRestarts(3), mwc.Critical()),
		prom.Option(),
		// Where metrics or alerts would hook in; here it only logs.
		mwc.WithOnStateChange(func(name string, s mwc.Status) {
			log.Debug("state change", "job", name, "state", s.State.String(), "restarts", s.Restarts)
		}),
		// Main never hands the Manager out; an Observer gets it at Start.
		mwc.WithObserver(func(m *mwc.Manager) { manager.Store(m) }),
		ui.Serve("127.0.0.1:8080"))
	if err != nil {
		log.Error("mwc", "err", err)
		os.Exit(1)
	}
}

// api is the application's own HTTP server, where the readiness probe and the
// metrics live too. It binds up front, so a busy port fails Start.
func api(h http.Handler) mwc.StartFunc {
	return func(ctx context.Context) (*mwc.Job, error) {
		ln, err := net.Listen("tcp", "127.0.0.1:8081")
		if err != nil {
			return nil, err
		}
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
		return mwc.Run(func() error {
			if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		}, srv.Shutdown), nil
	}
}

// mailer keeps failing, so the page has restarts to show.
func mailer(ctx context.Context) (*mwc.Job, error) {
	return mwc.Run(func() error {
		select {
		case <-time.After(5 * time.Second):
			return errors.New("smtp: 535 authentication failed")
		case <-ctx.Done():
			return nil
		}
	}, nil), nil
}
