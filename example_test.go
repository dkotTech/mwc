package mwc_test

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/dkotTech/mwc"
	"github.com/dkotTech/mwc/ui"
)

// httpServer adapts *http.Server to a mwc.StartFunc.
func httpServer(addr string, h http.Handler) mwc.StartFunc {
	return func(ctx context.Context) (*mwc.Job, error) {
		// Bind synchronously so that a busy port is reported as a start error.
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		srv := &http.Server{Handler: h}
		return mwc.Run(func() error {
			err := srv.Serve(ln)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		}, srv.Shutdown), nil
	}
}

func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	m, err := mwc.Start(ctx, map[string]mwc.StartFunc{
		"api":     httpServer(":8080", http.NewServeMux()),
		"metrics": httpServer(":9090", http.NewServeMux()),
	},
		mwc.WithBackoff(time.Second, 30*time.Second),
		mwc.WithShutdownTimeout(10*time.Second),
		ui.Serve("127.0.0.1:7000"), // status page at http://127.0.0.1:7000
	)
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done() // wait for SIGINT

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Shutdown(shutdownCtx); err != nil {
		log.Println("shutdown:", err)
	}
}
