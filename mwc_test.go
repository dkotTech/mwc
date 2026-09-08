package mwc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var quiet = WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

func fastOpts(extra ...Option) []Option {
	return append([]Option{quiet, WithBackoff(time.Millisecond, 5*time.Millisecond)}, extra...)
}

// idle returns a StartFunc whose job runs until its context is cancelled.
func idle(shutdown func(context.Context) error) StartFunc {
	return func(ctx context.Context) (*Job, error) {
		return Run(func() error { <-ctx.Done(); return nil }, shutdown), nil
	}
}

// isRunning reports whether the job exists and is up. Running is State's zero
// value, so comparing the result of a map miss to it proves nothing.
func isRunning(m *Manager, name string) bool {
	st, ok := m.Status()[name]
	return ok && st.State == Running
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

func TestRestartOnFailure(t *testing.T) {
	var starts atomic.Int32
	fail := make(chan error, 1)
	m, err := Start(context.Background(), map[string]StartFunc{
		"svc": func(ctx context.Context) (*Job, error) {
			starts.Add(1)
			return Run(func() error {
				select {
				case err := <-fail:
					return err
				case <-ctx.Done():
					return nil
				}
			}, nil), nil
		},
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())

	fail <- errors.New("boom")
	waitFor(t, func() bool { return starts.Load() == 2 })
	waitFor(t, func() bool { return isRunning(m, "svc") })

	st := m.Status()["svc"]
	if st.Restarts != 1 || st.LastErr != nil {
		t.Fatalf("unexpected status %+v, want 1 restart and no error once running again", st)
	}
}

func TestShutdownStopsAll(t *testing.T) {
	var stopped atomic.Int32
	mk := idle(func(context.Context) error {
		stopped.Add(1)
		return nil
	})
	m, err := Start(context.Background(), map[string]StartFunc{"a": mk, "b": mk, "c": mk}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 3 {
		t.Fatalf("stopped %d jobs, want 3", stopped.Load())
	}
	for name, st := range m.Status() {
		if st.State != Stopped {
			t.Errorf("%s: state %v, want stopped", name, st.State)
		}
	}
	select {
	case <-m.Done():
	default:
		t.Fatal("Done not closed after Shutdown")
	}
}

func TestShutdownCollectsErrors(t *testing.T) {
	m, err := Start(context.Background(), map[string]StartFunc{
		"ok":  idle(nil),
		"bad": idle(func(context.Context) error { return errors.New("cannot stop") }),
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	err = m.Shutdown(context.Background())
	if err == nil || err.Error() != `job "bad": shutdown: cannot stop` {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestMaxRestarts(t *testing.T) {
	var starts atomic.Int32
	m, err := Start(context.Background(), map[string]StartFunc{
		"flaky": func(ctx context.Context) (*Job, error) {
			starts.Add(1)
			return Run(func() error { return errors.New("dead") }, nil), nil
		},
	}, fastOpts(WithMaxRestarts(3))...)
	if err != nil {
		t.Fatal(err)
	}
	<-m.Done()
	if starts.Load() != 4 { // initial + 3 restarts
		t.Fatalf("starts = %d, want 4", starts.Load())
	}
	st := m.Status()["flaky"]
	if st.State != Failed || st.Restarts != 4 {
		t.Fatalf("unexpected status %+v", st)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRestartAfterStartError(t *testing.T) {
	var starts atomic.Int32
	m, err := Start(context.Background(), map[string]StartFunc{
		"svc": func(ctx context.Context) (*Job, error) {
			switch starts.Add(1) {
			case 1:
				return Run(func() error { return errors.New("crash") }, nil), nil
			case 2:
				return nil, errors.New("port busy")
			default:
				return Run(func() error { <-ctx.Done(); return nil }, nil), nil
			}
		},
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	waitFor(t, func() bool { return starts.Load() == 3 && isRunning(m, "svc") })
	if r := m.Status()["svc"].Restarts; r != 2 {
		t.Fatalf("restarts = %d, want 2", r)
	}
}

func TestInitialStartFailureRollsBack(t *testing.T) {
	var stopped atomic.Int32
	_, err := Start(context.Background(), map[string]StartFunc{
		"a-ok": idle(func(context.Context) error {
			stopped.Add(1)
			return nil
		}),
		"b-bad": func(ctx context.Context) (*Job, error) {
			return nil, errors.New("nope")
		},
	}, fastOpts()...)
	if err == nil || err.Error() != `mwc: job "b-bad": start: nope` {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopped.Load() != 1 {
		t.Fatalf("already started job was not shut down")
	}
}

func TestJobWithoutDoneIsStartError(t *testing.T) {
	_, err := Start(context.Background(), map[string]StartFunc{
		"nil": func(ctx context.Context) (*Job, error) { return nil, nil },
	}, fastOpts()...)
	if err == nil {
		t.Fatal("expected start error for job without Done")
	}
	_, err = Start(context.Background(), map[string]StartFunc{
		"nilfn": nil,
	}, fastOpts()...)
	if err == nil || err.Error() != `mwc: job "nilfn": nil start function` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCleanExitNotRestarted(t *testing.T) {
	var starts atomic.Int32
	m, err := Start(context.Background(), map[string]StartFunc{
		"once": func(ctx context.Context) (*Job, error) {
			starts.Add(1)
			return Run(func() error { return nil }, nil), nil
		},
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	<-m.Done()
	if starts.Load() != 1 || m.Status()["once"].State != Exited {
		t.Fatalf("starts=%d status=%+v", starts.Load(), m.Status()["once"])
	}
}

func TestPanicIsRestarted(t *testing.T) {
	var starts atomic.Int32
	var failure atomic.Pointer[Status] // the Restarting status, where the error shows
	m, err := Start(context.Background(), map[string]StartFunc{
		"p": func(ctx context.Context) (*Job, error) {
			if starts.Add(1) == 1 {
				return Run(func() error { panic("oops") }, nil), nil
			}
			return Run(func() error { <-ctx.Done(); return nil }, nil), nil
		},
	}, fastOpts(WithOnStateChange(func(_ string, s Status) {
		if s.State == Restarting {
			failure.Store(&s)
		}
	}))...)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	waitFor(t, func() bool { return m.Status()["p"].State == Running && starts.Load() == 2 })
	var pe *PanicError
	if st := failure.Load(); st == nil || !errors.As(st.LastErr, &pe) || pe.Value != "oops" {
		t.Fatalf("restarting status = %+v, want a PanicError", failure.Load())
	}
}

func TestParentContextCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var stopped atomic.Bool
	m, err := Start(ctx, map[string]StartFunc{
		"svc": idle(func(context.Context) error {
			stopped.Store(true)
			return nil
		}),
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	<-m.Done()
	if !stopped.Load() || m.Status()["svc"].State != Stopped {
		t.Fatalf("job not stopped on parent cancel: %+v", m.Status()["svc"])
	}
}

func TestShutdownTimeout(t *testing.T) {
	m, err := Start(context.Background(), map[string]StartFunc{
		"stuck": func(ctx context.Context) (*Job, error) {
			return Run(func() error { select {} }, func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}), nil
		},
	}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = m.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

func TestObserverRunsUntilStoppingAndIsWaitedFor(t *testing.T) {
	var seen atomic.Int32
	var finished atomic.Bool
	m, err := Start(context.Background(), map[string]StartFunc{"svc": idle(nil)},
		append(fastOpts(), WithObserver(func(m *Manager) {
			for {
				if isRunning(m, "svc") {
					seen.Add(1)
				}
				select {
				case <-m.Stopping():
					finished.Store(true)
					return
				case <-time.After(time.Millisecond):
				}
			}
		}))...)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return seen.Load() > 0 })
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !finished.Load() {
		t.Fatal("Shutdown returned before the observer finished")
	}
}

// An observer is there to be looked at when things go wrong, so it must not
// end with the jobs it is reporting on.
func TestObserverOutlivesFailedJobs(t *testing.T) {
	running := make(chan struct{})
	var ended atomic.Bool
	m, err := Start(context.Background(), map[string]StartFunc{
		"svc": func(ctx context.Context) (*Job, error) {
			return Run(func() error { return errors.New("boom") }, nil), nil
		},
	}, append(fastOpts(WithMaxRestarts(1)), WithObserver(func(m *Manager) {
		close(running)
		<-m.Stopping()
		ended.Store(true)
	}))...)
	if err != nil {
		t.Fatal(err)
	}
	<-running
	select {
	case <-m.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the job never gave up")
	}
	if ended.Load() {
		t.Fatal("observer ended with the jobs instead of with the Manager")
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !ended.Load() {
		t.Fatal("observer did not return on Stopping")
	}
}

// A Shutdown that runs out of time must say what is still running, and must
// not report success while an observer is still holding resources.
func TestShutdownDeadlineReportsTheObserver(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	m, err := Start(context.Background(), map[string]StartFunc{"svc": idle(nil)},
		append(fastOpts(), WithObserver(func(m *Manager) { <-release }))...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = m.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if !strings.Contains(err.Error(), "observer") {
		t.Errorf("err = %v, want it to name the observer", err)
	}
}

func TestBackoff(t *testing.T) {
	m := &Manager{backoffMin: time.Second, backoffMax: 10 * time.Second}
	if got := m.backoff(0); got != time.Second {
		t.Errorf("backoff(0) = %v, want 1s", got)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := m.backoff(i + 1); got != w {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := m.backoff(100); got != 10*time.Second {
		t.Errorf("backoff(100) = %v (overflow?)", got)
	}
}

func TestOnStateChange(t *testing.T) {
	type change struct {
		name string
		st   Status
	}
	var mu sync.Mutex
	var seen []change
	var m *Manager
	record := func(name string, s Status) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, change{name, s})
		if m != nil {
			m.Status() // the hook runs outside the lock: this must not deadlock
		}
	}
	fail := make(chan error, 1)
	var err error
	m, err = Start(context.Background(), map[string]StartFunc{
		"svc": func(ctx context.Context) (*Job, error) {
			return Run(func() error {
				select {
				case err := <-fail:
					return err
				case <-ctx.Done():
					return nil
				}
			}, nil), nil
		},
	}, fastOpts(WithOnStateChange(record))...)
	if err != nil {
		t.Fatal(err)
	}

	fail <- errors.New("boom")
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 3
	})
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []State{Running, Restarting, Running, Stopped}
	if len(seen) != len(want) {
		t.Fatalf("got %d changes %+v, want %d", len(seen), seen, len(want))
	}
	for i, c := range seen {
		if c.name != "svc" || c.st.State != want[i] || c.st.Since.IsZero() {
			t.Errorf("change %d = %q %+v, want state %v", i, c.name, c.st, want[i])
		}
	}
	if r := seen[1]; r.st.Restarts != 1 || r.st.LastErr == nil || r.st.LastErr.Error() != "boom" {
		t.Errorf("restarting status = %+v, want the failure", r.st)
	}
}

func TestOnStateChangeSeesRollbackOfFailedStart(t *testing.T) {
	var stopped atomic.Int32
	m, err := Start(context.Background(), map[string]StartFunc{
		"a": idle(nil),
		"b": func(context.Context) (*Job, error) { return nil, errors.New("no") },
	}, fastOpts(WithOnStateChange(func(name string, s Status) {
		if s.State == Stopped {
			stopped.Add(1)
		}
	}))...)
	if err == nil || m != nil {
		t.Fatal("Start must fail")
	}
	if stopped.Load() != 1 {
		t.Errorf("saw %d stops during rollback, want 1", stopped.Load())
	}
}

func TestHealthy(t *testing.T) {
	finish := make(chan struct{})
	fail := make(chan error, 1)
	m, err := Start(context.Background(), map[string]StartFunc{
		"steady": idle(nil),
		"oneoff": func(ctx context.Context) (*Job, error) {
			return Run(func() error { <-finish; return nil }, nil), nil
		},
		"flaky": func(ctx context.Context) (*Job, error) {
			return Run(func() error {
				select {
				case err := <-fail:
					return err
				case <-ctx.Done():
					return nil
				}
			}, nil), nil
		},
	}, fastOpts(WithMaxRestarts(1), WithBackoff(time.Hour, time.Hour))...)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Healthy() {
		t.Fatal("healthy = false with every job running")
	}

	close(finish) // a clean exit is not a problem
	waitFor(t, func() bool { return m.Status()["oneoff"].State == Exited })
	if !m.Healthy() {
		t.Error("healthy = false after a job exited cleanly")
	}

	fail <- errors.New("boom") // waiting out a long backoff is
	waitFor(t, func() bool { return m.Status()["flaky"].State == Restarting })
	if m.Healthy() {
		t.Error("healthy = true with a job restarting")
	}

	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Healthy() {
		t.Error("healthy = true after Shutdown")
	}
}

func TestHealthyFalseOnceAJobGaveUp(t *testing.T) {
	boom := func(ctx context.Context) (*Job, error) {
		return Run(func() error { return errors.New("boom") }, nil), nil
	}
	m, err := Start(context.Background(), map[string]StartFunc{"a": idle(nil), "b": boom}, fastOpts(WithMaxRestarts(1))...)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	waitFor(t, func() bool { return m.Status()["b"].State == Failed })
	if m.Healthy() {
		t.Error("healthy = true with a failed job")
	}
}

func TestStartRefusesCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var started atomic.Int32
	m, err := Start(ctx, map[string]StartFunc{
		"a": func(ctx context.Context) (*Job, error) { started.Add(1); return idle(nil)(ctx) },
	}, fastOpts()...)
	if m != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, %v; want nil, context.Canceled", m, err)
	}
	if started.Load() != 0 {
		t.Error("a job was started under a cancelled context")
	}
}

// A job that dies as the stop is requested may reach restart, since
// supervise's select picks Done or stopping at random. It must go Stopped at
// once: no failure counted, no restart scheduled.
func TestRestartDuringShutdownIsAStop(t *testing.T) {
	var seen []State
	m := &Manager{
		log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		stopping: make(chan struct{}),
		onChange: []func(string, Status){func(_ string, s Status) { seen = append(seen, s.State) }},
	}
	close(m.stopping)
	s := &jobState{name: "a", status: Status{State: Running, Since: time.Now()}}
	boom := errors.New("boom")

	job, cancel, ok := m.restart(s, boom)
	if ok || job != nil || cancel != nil {
		t.Fatalf("restart = %v, %v, %v; want nil, nil, false", job, cancel, ok)
	}
	if s.status.State != Stopped || s.status.Restarts != 0 || s.status.LastErr != boom {
		t.Errorf("status = %+v, want Stopped, 0 restarts, the error kept", s.status)
	}
	if len(seen) != 1 || seen[0] != Stopped {
		t.Errorf("hook saw %v, want [stopped]", seen)
	}
}

func TestStableAfterZeroNeverResets(t *testing.T) {
	boom := func(ctx context.Context) (*Job, error) {
		return Run(func() error { return errors.New("boom") }, nil), nil
	}
	m, err := Start(context.Background(), map[string]StartFunc{"b": boom},
		fastOpts(WithMaxRestarts(3), WithStableAfter(0))...)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	waitFor(t, func() bool { return m.Status()["b"].State == Failed })
	if r := m.Status()["b"].Restarts; r != 4 {
		t.Errorf("restarts = %d, want 4: the limit and one more", r)
	}
}
