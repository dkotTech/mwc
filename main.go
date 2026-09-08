package mwc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// Main is the body of a main function: it starts the jobs and runs them
// until SIGINT or SIGTERM, or until no job is running any more, then stops
// everything, bounded by WithShutdownTimeout, and returns.
//
//	func main() {
//		if err := mwc.Main(jobs, ui.Serve("127.0.0.1:8080")); err != nil {
//			log.Fatal(err)
//		}
//	}
//
// The error is Start's if a job failed to start, or joins the shutdown
// errors with one per job that went Failed - a process whose jobs all died
// should exit non-zero. Jobs that all exited cleanly are a normal end and
// return nil.
func Main(jobs map[string]StartFunc, opts ...Option) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, jobs, opts...)
}

// run is Main with the signal context given, so that it can be tested.
func run(ctx context.Context, jobs map[string]StartFunc, opts ...Option) error {
	m, err := Start(ctx, jobs, opts...)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		m.log.Info("shutting down", "reason", "signal")
	case <-m.Done():
		m.log.Info("shutting down", "reason", "no job running")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	defer cancel()
	errs := []error{m.Shutdown(shutdownCtx)}
	for name, st := range m.Status() {
		if st.State == Failed {
			errs = append(errs, fmt.Errorf("mwc: job %q failed: %w", name, st.LastErr))
		}
	}
	return errors.Join(errs...)
}
