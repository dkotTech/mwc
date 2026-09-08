package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

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

// expect checks the collector's output against the given exposition lines,
// for the metric families those lines name. HELP lines may be left out; they
// are filled in from the collector's descriptors.
func expect(t *testing.T, c *Collector, exposition string) {
	t.Helper()
	help := map[string]string{}
	ch := make(chan *prometheus.Desc, 16)
	c.Describe(ch)
	close(ch)
	re := regexp.MustCompile(`fqName: "([^"]+)", help: "([^"]*)"`)
	for d := range ch {
		if m := re.FindStringSubmatch(d.String()); m != nil {
			help[m[1]] = m[2]
		}
	}

	var names []string
	var b strings.Builder
	for _, l := range strings.Split(exposition, "\n") {
		if strings.HasPrefix(l, "# TYPE ") {
			name := strings.Fields(l)[2]
			names = append(names, name)
			if !strings.Contains(exposition, "# HELP "+name+" ") {
				b.WriteString("# HELP " + name + " " + help[name] + "\n")
			}
		}
		b.WriteString(l + "\n")
	}
	if err := testutil.CollectAndCompare(c, strings.NewReader(b.String()), names...); err != nil {
		t.Error(err)
	}
}

func TestCollectorIsValid(t *testing.T) {
	c := New()
	manager(t, map[string]mwc.StartFunc{"api": idle, "db": idle}, c.Option())
	// Lint the metric names, help strings and label sets.
	problems, err := testutil.CollectAndLint(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("%s: %s", p.Metric, p.Text)
	}
	// And a registry accepts it: no duplicate or inconsistent descriptors.
	if err := prometheus.NewPedanticRegistry().Register(c); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(c); n != 2+2*(5+4) {
		t.Errorf("collected %d samples, want %d", n, 2+2*(5+4))
	}
}

func TestRunning(t *testing.T) {
	c := New()
	manager(t, map[string]mwc.StartFunc{"api": idle, "db": idle}, c.Option())
	expect(t, c, `
# HELP mwc_healthy 1 while every job is running or has exited cleanly, else 0.
# TYPE mwc_healthy gauge
mwc_healthy 1
# HELP mwc_jobs Number of supervised jobs.
# TYPE mwc_jobs gauge
mwc_jobs 2
# HELP mwc_job_state 1 for the job's current state, 0 for the others.
# TYPE mwc_job_state gauge
mwc_job_state{job="api",state="running"} 1
mwc_job_state{job="api",state="restarting"} 0
mwc_job_state{job="api",state="exited"} 0
mwc_job_state{job="api",state="failed"} 0
mwc_job_state{job="api",state="stopped"} 0
mwc_job_state{job="db",state="running"} 1
mwc_job_state{job="db",state="restarting"} 0
mwc_job_state{job="db",state="exited"} 0
mwc_job_state{job="db",state="failed"} 0
mwc_job_state{job="db",state="stopped"} 0
# HELP mwc_job_consecutive_restarts Restarts since the job last ran long enough to count as stable.
# TYPE mwc_job_consecutive_restarts gauge
mwc_job_consecutive_restarts{job="api"} 0
mwc_job_consecutive_restarts{job="db"} 0
# HELP mwc_job_starts_total Times the job was started, the first start included.
# TYPE mwc_job_starts_total counter
mwc_job_starts_total{job="api"} 1
mwc_job_starts_total{job="db"} 1
# HELP mwc_job_failures_total Times the job exited with an error or failed to start.
# TYPE mwc_job_failures_total counter
mwc_job_failures_total{job="api"} 0
mwc_job_failures_total{job="db"} 0
`)
}

func TestStateSeconds(t *testing.T) {
	c := New()
	base := time.Now()
	c.now = func() time.Time { return base.Add(90 * time.Second) }
	manager(t, map[string]mwc.StartFunc{"api": idle}, c.Option())

	v := testutil.ToFloat64(gather(t, c, "mwc_job_state_seconds"))
	if v < 89 || v > 91 {
		t.Errorf("state_seconds = %v, want about 90", v)
	}
}

// gather returns the single sample of the named family as a Collector for
// testutil.ToFloat64.
func gather(t *testing.T, c *Collector, name string) prometheus.Collector {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	var found []prometheus.Metric
	for m := range ch {
		if strings.HasPrefix(m.Desc().String(), `Desc{fqName: "`+name+`"`) {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d samples of %s, want 1", len(found), name)
	}
	return single{found[0]}
}

type single struct{ m prometheus.Metric }

func (s single) Describe(ch chan<- *prometheus.Desc) { ch <- s.m.Desc() }
func (s single) Collect(ch chan<- prometheus.Metric) { ch <- s.m }

func TestCountersFollowRestarts(t *testing.T) {
	c := New()
	fail := make(chan error, 1)
	m := manager(t, map[string]mwc.StartFunc{
		"svc": func(ctx context.Context) (*mwc.Job, error) {
			return mwc.Run(func() error {
				select {
				case err := <-fail:
					return err
				case <-ctx.Done():
					return nil
				}
			}, nil), nil
		},
	}, c.Option())

	fail <- errors.New("boom")
	waitFor(t, func() bool { return testutil.ToFloat64(gather(t, c, "mwc_job_starts_total")) == 2 })
	waitFor(t, func() bool { return m.Status()["svc"].State == mwc.Running })

	expect(t, c, `
# TYPE mwc_healthy gauge
mwc_healthy 1
# TYPE mwc_job_consecutive_restarts gauge
mwc_job_consecutive_restarts{job="svc"} 1
# TYPE mwc_job_starts_total counter
mwc_job_starts_total{job="svc"} 2
# TYPE mwc_job_failures_total counter
mwc_job_failures_total{job="svc"} 1
`)
}

func TestFailedJob(t *testing.T) {
	c := New()
	boom := func(ctx context.Context) (*mwc.Job, error) {
		return mwc.Run(func() error { return errors.New("boom") }, nil), nil
	}
	m := manager(t, map[string]mwc.StartFunc{"api": idle, "mailer": boom}, c.Option(), mwc.WithMaxRestarts(1))
	waitFor(t, func() bool { return m.Status()["mailer"].State == mwc.Failed })

	expect(t, c, `
# TYPE mwc_healthy gauge
mwc_healthy 0
# TYPE mwc_job_state gauge
mwc_job_state{job="api",state="running"} 1
mwc_job_state{job="api",state="restarting"} 0
mwc_job_state{job="api",state="exited"} 0
mwc_job_state{job="api",state="failed"} 0
mwc_job_state{job="api",state="stopped"} 0
mwc_job_state{job="mailer",state="running"} 0
mwc_job_state{job="mailer",state="restarting"} 0
mwc_job_state{job="mailer",state="exited"} 0
mwc_job_state{job="mailer",state="failed"} 1
mwc_job_state{job="mailer",state="stopped"} 0
# TYPE mwc_job_starts_total counter
mwc_job_starts_total{job="api"} 1
mwc_job_starts_total{job="mailer"} 2
# TYPE mwc_job_failures_total counter
mwc_job_failures_total{job="api"} 0
mwc_job_failures_total{job="mailer"} 2
`)
}

func TestShutdownIsVisible(t *testing.T) {
	c := New()
	m := manager(t, map[string]mwc.StartFunc{"api": idle}, c.Option())
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	expect(t, c, `
# TYPE mwc_healthy gauge
mwc_healthy 0
# TYPE mwc_job_state gauge
mwc_job_state{job="api",state="running"} 0
mwc_job_state{job="api",state="restarting"} 0
mwc_job_state{job="api",state="exited"} 0
mwc_job_state{job="api",state="failed"} 0
mwc_job_state{job="api",state="stopped"} 1
# TYPE mwc_job_starts_total counter
mwc_job_starts_total{job="api"} 1
# TYPE mwc_job_failures_total counter
mwc_job_failures_total{job="api"} 0
`)
}

func TestUnboundCollector(t *testing.T) {
	c := New()
	if err := prometheus.NewPedanticRegistry().Register(c); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Errorf("unbound collector produced %d samples, want 0", n)
	}
}

func TestOptionBindsOnce(t *testing.T) {
	c := New()
	manager(t, map[string]mwc.StartFunc{"api": idle}, c.Option(), c.Option())
	expect(t, c, `
# TYPE mwc_job_starts_total counter
mwc_job_starts_total{job="api"} 1
`)
}
