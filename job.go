package mwc

import "context"

// Job is a running unit of work, typically a long-lived server.
//
// Shutdown may be nil; the job then stops on the cancellation of its context.
//
// Done is required: exactly one value, or a close, when the job exits. A
// non-nil error gets the job restarted, a nil one does not.
type Job struct {
	Shutdown func(ctx context.Context) error
	Done     <-chan error
}

// StartFunc launches a job. It must return promptly: the long-running work
// goes in a goroutine. ctx is cancelled when the job is stopped, right after
// Job.Shutdown.
type StartFunc func(ctx context.Context) (*Job, error)

// Run is the common "blocking serve + graceful stop" case; see the package
// Example. run goes in a goroutine, its result to Job.Done, a panic in it to
// PanicError - so a panicking job restarts instead of the process dying.
func Run(run func() error, shutdown func(ctx context.Context) error) *Job {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- &PanicError{Value: r}
			}
		}()
		done <- run()
	}()
	return &Job{Shutdown: shutdown, Done: done}
}
