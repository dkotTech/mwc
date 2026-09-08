package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dkotTech/mwc"
)

// states in the order their mwc_job_state series are emitted.
var states = []mwc.State{mwc.Running, mwc.Restarting, mwc.Exited, mwc.Failed, mwc.Stopped}

var (
	descHealthy = prometheus.NewDesc("mwc_healthy",
		"1 while every job is running or has exited cleanly, else 0.", nil, nil)
	descJobs = prometheus.NewDesc("mwc_jobs",
		"Number of supervised jobs.", nil, nil)
	descState = prometheus.NewDesc("mwc_job_state",
		"1 for the job's current state, 0 for the others.", []string{"job", "state"}, nil)
	descStateSeconds = prometheus.NewDesc("mwc_job_state_seconds",
		"Seconds since the job entered its current state.", []string{"job"}, nil)
	descConsecutive = prometheus.NewDesc("mwc_job_consecutive_restarts",
		"Restarts since the job last ran long enough to count as stable.", []string{"job"}, nil)
	descStarts = prometheus.NewDesc("mwc_job_starts_total",
		"Times the job was started, the first start included.", []string{"job"}, nil)
	descFailures = prometheus.NewDesc("mwc_job_failures_total",
		"Times the job exited with an error or failed to start.", []string{"job"}, nil)
)

// Collector is a prometheus.Collector for one Manager.
type Collector struct {
	bind     sync.Once
	mu       sync.Mutex
	m        *mwc.Manager
	starts   map[string]float64 // transitions to Running
	failures map[string]float64 // transitions to Restarting or Failed
	now      func() time.Time   // for tests
}

// New returns an unbound Collector; give its Option to Start and register it.
func New() *Collector {
	return &Collector{
		starts:   map[string]float64{},
		failures: map[string]float64{},
		now:      time.Now,
	}
}

// Option binds the Collector to the Manager being started. Only the first
// binding counts: a second one, to the same or another Manager, is ignored,
// so the counters are never fed twice.
func (c *Collector) Option() mwc.Option {
	return func(m *mwc.Manager) {
		c.bind.Do(func() {
			c.mu.Lock()
			c.m = m
			c.mu.Unlock()
			mwc.WithOnStateChange(c.observe)(m)
		})
	}
}

func (c *Collector) observe(name string, s mwc.Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch s.State {
	case mwc.Running:
		c.starts[name]++
	case mwc.Restarting, mwc.Failed:
		c.failures[name]++
	}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descHealthy, descJobs, descState, descStateSeconds, descConsecutive, descStarts, descFailures} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	m := c.m
	now := c.now()
	starts := make(map[string]float64, len(c.starts))
	failures := make(map[string]float64, len(c.failures))
	for k, v := range c.starts {
		starts[k] = v
	}
	for k, v := range c.failures {
		failures[k] = v
	}
	c.mu.Unlock()
	if m == nil {
		return
	}

	status := m.Status()
	ch <- prometheus.MustNewConstMetric(descHealthy, prometheus.GaugeValue, boolean(m.Healthy()))
	ch <- prometheus.MustNewConstMetric(descJobs, prometheus.GaugeValue, float64(len(status)))
	for name, s := range status {
		for _, st := range states {
			ch <- prometheus.MustNewConstMetric(descState, prometheus.GaugeValue, boolean(s.State == st), name, st.String())
		}
		ch <- prometheus.MustNewConstMetric(descStateSeconds, prometheus.GaugeValue, max(now.Sub(s.Since), 0).Seconds(), name)
		ch <- prometheus.MustNewConstMetric(descConsecutive, prometheus.GaugeValue, float64(s.Restarts), name)
		ch <- prometheus.MustNewConstMetric(descStarts, prometheus.CounterValue, starts[name], name)
		ch <- prometheus.MustNewConstMetric(descFailures, prometheus.CounterValue, failures[name], name)
	}
}

func boolean(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
