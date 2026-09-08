// Package mwc supervises a set of named, long-running jobs (such as servers):
// Start launches them all and restarts any that exits with an error, and the
// returned Manager stops everything with one Shutdown.
package mwc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"
)

// State is what a job is currently doing.
type State int

const (
	Running    State = iota // up
	Restarting              // failed, waiting out the backoff delay
	Exited                  // finished on its own, not restarted
	Failed                  // failed and the restart limit was exhausted
	Stopped                 // stopped by Manager.Shutdown
)

func (s State) String() string {
	switch s {
	case Running:
		return "running"
	case Restarting:
		return "restarting"
	case Exited:
		return "exited"
	case Failed:
		return "failed"
	case Stopped:
		return "stopped"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// Healthy reports whether a job in this state counts as healthy: Running or
// Exited on its own. See Manager.Healthy.
func (s State) Healthy() bool { return s == Running || s == Exited }

// Status is a snapshot of one job.
type Status struct {
	State    State
	Restarts int       // since the job last ran long enough to count as stable
	LastErr  error     // last exit or start error; nil once the job runs again
	Since    time.Time // when State was entered
}

// PanicError reports a panic recovered by Run.
type PanicError struct {
	Value any
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// Option configures a Manager.
type Option func(*Manager)

// WithLogger sets the logger. Default: slog.Default().
func WithLogger(l *slog.Logger) Option { return func(m *Manager) { m.log = l } }

// WithBackoff sets the restart delay: it doubles from min up to max.
// Default: 1s, 1m.
func WithBackoff(min, max time.Duration) Option {
	return func(m *Manager) { m.policy.backoffMin, m.policy.backoffMax = min, max }
}

// WithJitter spreads each restart delay by a random factor in [1-f, 1+f],
// so replicas that lost the same dependency do not all retry in the same
// instant. f is clamped to 0..1. Default: 0, no jitter.
func WithJitter(f float64) Option { return func(m *Manager) { m.policy.jitter = f } }

// WithMaxRestarts caps consecutive restarts per job, which then goes Failed.
// Default: 0, unlimited.
func WithMaxRestarts(n int) Option { return func(m *Manager) { m.policy.maxRestarts = n } }

// WithStableAfter sets how long a job must run to reset its restart counter
// and backoff. Default: 1m. 0 means never: the counter only grows, so
// WithMaxRestarts counts every failure over the job's lifetime.
func WithStableAfter(d time.Duration) Option { return func(m *Manager) { m.policy.stableAfter = d } }

// Observer watches a running Manager, typically to render its Status; see
// package mwc/ui. It runs in its own goroutine and must return on
// m.Stopping(), not m.Done(). Shutdown waits for it, so it must not call
// Shutdown.
type Observer func(m *Manager)

// WithObserver registers an Observer.
func WithObserver(o Observer) Option {
	return func(m *Manager) { m.observers = append(m.observers, o) }
}

// WithShutdownTimeout bounds the graceful stop of each job when Start's
// context is cancelled; Shutdown uses the caller's context. Default: 30s.
func WithShutdownTimeout(d time.Duration) Option { return func(m *Manager) { m.shutdownTimeout = d } }

// WithOnStateChange registers fn to be called with a job's name and new
// Status each time its State changes: on start, on each failure and restart,
// on exit and on stop - including the stops of a failed Start. It is the hook
// for metrics and alerts.
//
// fn runs on the job's supervisor goroutine, so it must return promptly and
// must not call Shutdown. Calls for one job never overlap; calls for
// different jobs may.
func WithOnStateChange(fn func(name string, s Status)) Option {
	return func(m *Manager) { m.onChange = append(m.onChange, fn) }
}

// Manager supervises a set of jobs.
type Manager struct {
	log             *slog.Logger
	policy          policy // defaults; see WithJob
	jobOpts         map[string][]JobOption
	shutdownTimeout time.Duration

	jobCtx context.Context // parent of every job context; never cancelled
	wg     sync.WaitGroup
	done   chan struct{} // closed when every supervisor has returned

	observers []Observer
	onChange  []func(name string, s Status)
	obsWG     sync.WaitGroup
	obsDone   chan struct{} // closed when every observer has returned

	stopOnce   sync.Once
	stopping   chan struct{}   // closed by stop
	stopCtx    context.Context // bounds the graceful stop; set before stopping
	unwatch    func() bool     // cancels the parent-context watcher
	mu         sync.Mutex
	jobs       map[string]*jobState
	stopErrors []error
}

type jobState struct {
	name   string
	start  StartFunc
	policy policy
	status Status
}

// Start launches every job and begins supervising them. If one fails to
// start, those already running are stopped and the error returned. Cancelling
// ctx is a Shutdown bounded by WithShutdownTimeout.
func Start(ctx context.Context, jobs map[string]StartFunc, opts ...Option) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("mwc: start: %w", err)
	}
	m := &Manager{
		log: slog.Default(),
		policy: policy{
			backoffMin:  time.Second,
			backoffMax:  time.Minute,
			stableAfter: time.Minute,
		},
		shutdownTimeout: 30 * time.Second,
		jobCtx:          context.WithoutCancel(ctx),
		done:            make(chan struct{}),
		obsDone:         make(chan struct{}),
		stopping:        make(chan struct{}),
		jobs:            make(map[string]*jobState, len(jobs)),
	}
	for _, o := range opts {
		o(m)
	}
	m.policy.normalize()

	// Validate first, then start in a deterministic order.
	for name := range m.jobOpts {
		if _, ok := jobs[name]; !ok {
			return nil, fmt.Errorf("mwc: WithJob %q: no such job", name)
		}
	}
	names := slices.Sorted(maps.Keys(jobs))
	policies := make(map[string]policy, len(jobs))
	for _, name := range names {
		if jobs[name] == nil {
			return nil, fmt.Errorf("mwc: job %q: nil start function", name)
		}
		p := m.policy
		for _, o := range m.jobOpts[name] {
			o(&p)
		}
		p.normalize()
		if p.critical && p.maxRestarts == 0 {
			return nil, fmt.Errorf("mwc: job %q: Critical needs a restart limit, see MaxRestarts", name)
		}
		policies[name] = p
	}

	for _, name := range names {
		s := &jobState{name: name, start: jobs[name], policy: policies[name]}
		job, cancelJob, err := m.launch(s)
		if err != nil {
			rollbackCtx, cancel := context.WithTimeout(ctx, m.shutdownTimeout)
			m.stop(rollbackCtx)
			m.wg.Wait()
			cancel()
			return nil, fmt.Errorf("mwc: job %q: start: %w", name, err)
		}
		m.mu.Lock()
		m.jobs[name] = s
		m.mu.Unlock()
		m.wg.Go(func() { m.supervise(s, job, cancelJob) })
	}
	go func() {
		m.wg.Wait()
		close(m.done)
	}()
	// Before any observer runs: no half-built Manager.
	m.unwatch = context.AfterFunc(ctx, func() {
		stopCtx, cancel := context.WithTimeout(m.jobCtx, m.shutdownTimeout)
		defer cancel()
		m.stop(stopCtx)
		<-m.done
	})
	for _, o := range m.observers {
		m.obsWG.Go(func() { o(m) })
	}
	go func() {
		m.obsWG.Wait()
		close(m.obsDone)
	}()
	return m, nil
}

// Shutdown stops every job, then waits for the jobs and the observers, all
// bounded by ctx. The returned error joins the jobs' shutdown errors with
// whatever the deadline cut short. Repeated calls just wait.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.unwatch()
	m.stop(ctx)

	var errs []error
	if err := waitClosed(ctx, m.done); err != nil {
		errs = append(errs, fmt.Errorf("mwc: shutdown: jobs still running: %w", err))
	}
	if err := waitClosed(ctx, m.obsDone); err != nil {
		m.log.Warn("observer did not return before shutdown deadline")
		errs = append(errs, fmt.Errorf("mwc: shutdown: observer still running: %w", err))
	}

	m.mu.Lock()
	errs = append(errs, m.stopErrors...)
	m.mu.Unlock()
	return errors.Join(errs...)
}

// waitClosed waits for c until ctx expires. A closed c beats an expired ctx,
// which a plain select would decide at random.
func waitClosed(ctx context.Context, c <-chan struct{}) error {
	select {
	case <-c:
		return nil
	default:
	}
	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done is closed once no job is running, stopped or dead alike.
func (m *Manager) Done() <-chan struct{} { return m.done }

// Stopping is closed once a shutdown was requested: the signal for whatever
// should outlive the jobs, such as an Observer.
func (m *Manager) Stopping() <-chan struct{} { return m.stopping }

// Logger returns the Manager's logger; see WithLogger.
func (m *Manager) Logger() *slog.Logger { return m.log }

// Status snapshots every job, keyed by name.
func (m *Manager) Status() map[string]Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Status, len(m.jobs))
	for name, s := range m.jobs {
		out[name] = s.status
	}
	return out
}

// Healthy reports whether every job is Running or has Exited on its own: the
// answer for a readiness probe. A job that is Restarting, Failed or Stopped
// makes it false, so a shutting-down process reports unhealthy too.
func (m *Manager) Healthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.jobs {
		if !s.status.State.Healthy() {
			return false
		}
	}
	return true
}

// stop requests a graceful stop; only the first call has effect.
func (m *Manager) stop(ctx context.Context) {
	m.stopOnce.Do(func() {
		m.stopCtx = ctx
		close(m.stopping)
	})
}

// setStatus records the transition and tells the WithOnStateChange hooks,
// outside the lock: they are free to call Status.
func (m *Manager) setStatus(s *jobState, state State, err error) {
	m.mu.Lock()
	s.status.State = state
	s.status.Since = time.Now()
	if err != nil {
		s.status.LastErr = err
	} else if state == Running {
		s.status.LastErr = nil
	}
	st := s.status
	m.mu.Unlock()
	for _, fn := range m.onChange {
		fn(s.name, st)
	}
}

// launch starts the job with its own context and marks it Running.
func (m *Manager) launch(s *jobState) (*Job, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(m.jobCtx)
	job, err := s.start(ctx)
	if err == nil && (job == nil || job.Done == nil) {
		err = errors.New("start returned no Done channel")
	}
	if err != nil {
		cancel()
		m.log.Error("job failed to start", "job", s.name, "err", err)
		return nil, nil, err
	}
	m.setStatus(s, Running, nil)
	m.log.Info("job started", "job", s.name)
	return job, cancel, nil
}

// supervise owns one job's lifecycle: exit, restart, stop.
func (m *Manager) supervise(s *jobState, job *Job, cancelJob context.CancelFunc) {
	for {
		select {
		case <-m.stopping:
			m.stopJob(s, job, cancelJob)
			return
		case err := <-job.Done:
			cancelJob()
			if err == nil {
				m.log.Info("job exited, not restarting", "job", s.name)
				m.setStatus(s, Exited, nil)
				return
			}
			var ok bool
			if job, cancelJob, ok = m.restart(s, err); !ok {
				return
			}
		}
	}
}

// restart re-launches after backoff, false once the limit is reached or the
// manager is stopping.
func (m *Manager) restart(s *jobState, err error) (*Job, context.CancelFunc, bool) {
	// A job that died as the stop was requested is not a failure to restart:
	// supervise may have picked Done over stopping, which a select decides at
	// random. Its exit is recorded, not counted.
	select {
	case <-m.stopping:
		m.log.Warn("job exited with error during shutdown", "job", s.name, "err", err)
		m.setStatus(s, Stopped, err)
		return nil, nil, false
	default:
	}

	p := &s.policy
	m.mu.Lock()
	uptime := time.Since(s.status.Since)
	if p.stableAfter > 0 && uptime >= p.stableAfter {
		s.status.Restarts = 0
	}
	m.mu.Unlock()
	m.log.Error("job failed", "job", s.name, "err", err, "uptime", uptime)

	for {
		m.mu.Lock()
		s.status.Restarts++
		attempt := s.status.Restarts
		m.mu.Unlock()
		if p.maxRestarts > 0 && attempt > p.maxRestarts {
			m.log.Error("job restart limit reached, giving up", "job", s.name, "restarts", p.maxRestarts, "err", err)
			m.setStatus(s, Failed, err)
			if p.critical {
				m.log.Error("critical job failed, stopping every job", "job", s.name)
				m.stopFromInside()
			}
			return nil, nil, false
		}

		delay := p.backoff(attempt)
		m.log.Warn("job restart scheduled", "job", s.name, "attempt", attempt, "delay", delay)
		m.setStatus(s, Restarting, err)
		select {
		case <-m.stopping:
			m.setStatus(s, Stopped, nil)
			return nil, nil, false
		case <-time.After(delay):
		}

		job, cancelJob, startErr := m.launch(s)
		if startErr != nil {
			err = startErr
			continue
		}
		m.log.Info("job restarted", "job", s.name, "attempt", attempt)
		return job, cancelJob, true
	}
}

// stopFromInside is the stop the Manager asks for itself, when a critical
// job has failed: graceful, bounded by WithShutdownTimeout, like the one on
// the cancellation of Start's context. It does not wait: the caller is a
// supervisor goroutine, and Done closes once every job has stopped.
func (m *Manager) stopFromInside() {
	stopCtx, cancel := context.WithTimeout(m.jobCtx, m.shutdownTimeout)
	m.stop(stopCtx)
	go func() {
		<-m.done
		cancel()
	}()
}

// stopJob calls Shutdown, cancels the job context and waits for the exit.
func (m *Manager) stopJob(s *jobState, job *Job, cancelJob context.CancelFunc) {
	ctx := m.stopCtx
	var err error
	if job.Shutdown != nil {
		err = job.Shutdown(ctx)
	}
	if err != nil {
		m.log.Error("job shutdown failed", "job", s.name, "err", err)
		m.mu.Lock()
		m.stopErrors = append(m.stopErrors, fmt.Errorf("job %q: shutdown: %w", s.name, err))
		m.mu.Unlock()
	}
	cancelJob()
	select {
	case exitErr := <-job.Done:
		if exitErr != nil {
			m.log.Warn("job exited with error during shutdown", "job", s.name, "err", exitErr)
		}
	case <-ctx.Done():
		m.log.Warn("job did not exit before shutdown deadline", "job", s.name)
	}
	m.setStatus(s, Stopped, err)
	m.log.Info("job stopped", "job", s.name)
}
