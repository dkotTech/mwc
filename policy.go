package mwc

import (
	"math/rand/v2"
	"time"
)

// policy is how one job is restarted. The Manager holds the defaults, set by
// the With* options; WithJob overrides them for a single job.
type policy struct {
	backoffMin  time.Duration
	backoffMax  time.Duration
	jitter      float64 // fraction of the delay, 0..1
	maxRestarts int
	stableAfter time.Duration
	critical    bool
}

// normalize keeps the values in the range the rest of the code assumes.
func (p *policy) normalize() {
	p.backoffMin = max(p.backoffMin, time.Millisecond)
	p.backoffMax = max(p.backoffMax, p.backoffMin)
	p.jitter = min(max(p.jitter, 0), 1)
}

// backoff for attempt n (1-based): exponential from backoffMin, capped at
// backoffMax, then spread by jitter.
func (p *policy) backoff(attempt int) time.Duration {
	d := p.backoffMin
	for i := 1; i < attempt && d > 0 && d < p.backoffMax; i++ {
		d *= 2
	}
	if d <= 0 { // overflow
		d = p.backoffMax
	}
	d = min(d, p.backoffMax)
	if p.jitter > 0 {
		// A factor in [1-jitter, 1+jitter].
		f := 1 + p.jitter*(2*rand.Float64()-1)
		d = time.Duration(float64(d) * f)
	}
	return d
}

// JobOption overrides the Manager's restart policy for one job; see WithJob.
type JobOption func(*policy)

// WithJob applies opts to the named job only. The job must be in the map
// given to Start, otherwise Start fails: a misspelled name would silently
// leave the job on the defaults.
//
//	mwc.WithJob("db", mwc.MaxRestarts(3), mwc.Critical())
func WithJob(name string, opts ...JobOption) Option {
	return func(m *Manager) {
		if m.jobOpts == nil {
			m.jobOpts = make(map[string][]JobOption)
		}
		m.jobOpts[name] = append(m.jobOpts[name], opts...)
	}
}

// Backoff is WithBackoff for one job.
func Backoff(min, max time.Duration) JobOption {
	return func(p *policy) { p.backoffMin, p.backoffMax = min, max }
}

// Jitter is WithJitter for one job.
func Jitter(f float64) JobOption { return func(p *policy) { p.jitter = f } }

// MaxRestarts is WithMaxRestarts for one job.
func MaxRestarts(n int) JobOption { return func(p *policy) { p.maxRestarts = n } }

// StableAfter is WithStableAfter for one job.
func StableAfter(d time.Duration) JobOption { return func(p *policy) { p.stableAfter = d } }

// Critical marks a job the process cannot do without: when it goes Failed,
// the Manager stops every other job, as if Shutdown had been called, and
// Done closes. Without it a process whose core has died keeps running with
// only the readiness probe to tell.
//
// A critical job needs a restart limit, MaxRestarts or WithMaxRestarts;
// otherwise it never gives up and never goes Failed, so Start refuses it.
// A critical job that exits cleanly is not a failure and stops nothing.
func Critical() JobOption { return func(p *policy) { p.critical = true } }
