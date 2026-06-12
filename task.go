package task

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrRunnerClosed        = errors.New("runner is closed")
	ErrRunnerAlreadyClosed = errors.New("runner is already closed")
)

// Future represents a task that will be executed in the future.
// The Await function will block until the task is completed.
// If the context passed to the Await function is canceled, the Await function will return but the task will still be executed.
// if the task needs to be canceled, the context passed to the Submit function should be canceled.
type Future interface {
	Await(ctx context.Context) error
}

type task struct {
	ctx    context.Context // the context passed to Submit
	merged *mergedContext  // ctx merged with the runner's context, reused across yields
	fn     func(ctx context.Context) error
	result error // set before done is closed
	done   chan struct{}
}

var _ Future = (*task)(nil)

// Await blocks until the task has completed or the given context is
// canceled. It is safe to call Await multiple times and from multiple
// goroutines; once the task has completed every call returns the same
// result.
func (t *task) Await(ctx context.Context) error {
	select {
	case <-t.done:
		return t.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *task) complete(err error) {
	t.result = err
	close(t.done)
}

type Runner interface {
	// Submit a task to be executed by the runner's workers. the context passed to the Submit function will be merged with the runner's context.
	// and the merged context will be passed to the submitted function. This ensures that if the runner is closed, the submitted function will
	// be canceled.
	Submit(ctx context.Context, fn func(context.Context) error) Future
	// Close the runner and wait for all the submitted tasks to be completed.
	// If the given context is canceled before the tasks have drained, Close
	// returns the context's error but the runner keeps draining in the
	// background. Calling this function again after closure will return
	// ErrRunnerAlreadyClosed.
	Close(ctx context.Context) error
}

type runner struct {
	workerSize int
	bufferSize int

	ctx    context.Context // canceled when the runner closes
	cancel context.CancelFunc

	tasks chan *task
	stop  chan struct{} // closed once all pending tasks have completed after Close

	mu      sync.RWMutex
	closed  bool
	pending sync.WaitGroup // tracks submitted but not yet completed tasks
	wg      sync.WaitGroup // tracks worker goroutines
}

var _ Runner = (*runner)(nil)

// The given context will be passed to the submitted function.
func (r *runner) Submit(ctx context.Context, fn func(context.Context) error) Future {
	t := &task{
		ctx:  ctx,
		fn:   fn,
		done: make(chan struct{}),
	}

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		t.complete(ErrRunnerClosed)
		return t
	}
	r.pending.Add(1)
	r.mu.RUnlock()

	t.merged = newMergedContext(ctx, r.ctx)
	r.tasks <- t

	return t
}

func (r *runner) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRunnerAlreadyClosed
	}
	r.closed = true
	r.mu.Unlock()

	// Signal running tasks to stop via the merged contexts.
	r.cancel()

	go func() {
		r.pending.Wait()
		close(r.stop)
	}()

	select {
	case <-r.stop:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.wg.Wait()

	return nil
}

// run executes a task once and either completes it or re-queues it when the
// task yields.
func (r *runner) run(t *task) {
	err := t.fn(t.merged)

	y, ok := err.(*yield)
	if !ok {
		r.finish(t, err)
		return
	}

	// If the caller passed a different context to Yield, adopt it for the
	// next execution.
	if y.ctx != nil && y.ctx != context.Context(t.merged) && y.ctx != t.ctx {
		t.merged.release()
		t.ctx = y.ctx
		t.merged = newMergedContext(y.ctx, r.ctx)
	}

	if y.delay > 0 {
		go r.requeueAfter(t, y.delay)
		return
	}

	r.requeue(t)
}

// finish resolves the task's future. A cancellation caused by the runner
// closing (rather than by the caller's own context) is reported as
// ErrRunnerClosed.
func (r *runner) finish(t *task, err error) {
	if err != nil && errors.Is(err, context.Canceled) && r.ctx.Err() != nil && t.ctx.Err() == nil {
		err = ErrRunnerClosed
	}
	t.merged.release()
	t.complete(err)
	r.pending.Done()
}

// requeue puts a yielded task back on the queue. It never blocks the calling
// worker: if the queue is full the hand-off is done by a goroutine, since a
// worker blocking on its own queue could deadlock the pool.
func (r *runner) requeue(t *task) {
	if err := t.merged.Err(); err != nil {
		r.finish(t, err)
		return
	}

	select {
	case r.tasks <- t:
	default:
		go func() {
			select {
			case r.tasks <- t:
			case <-t.merged.Done():
				r.finish(t, t.merged.Err())
			}
		}()
	}
}

// requeueAfter re-queues a yielded task after the given delay, unless the
// task's context is canceled first.
func (r *runner) requeueAfter(t *task, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-t.merged.Done():
		r.finish(t, t.merged.Err())
		return
	}

	select {
	case r.tasks <- t:
	case <-t.merged.Done():
		r.finish(t, t.merged.Err())
	}
}

type runnerOpt func(*runner)

// WithWorkerSize sets the number of workers that will be used to execute the submitted tasks.
// if the worker size is less than or equal to 0, it will be set to 1.
func WithWorkerSize(worker int) runnerOpt {
	return func(r *runner) {
		if worker <= 0 {
			worker = 1
		}
		r.workerSize = worker
	}
}

// WithBufferSize sets the buffer size of the tasks channel.
// if the buffer size is less than 0, it will be set to 1.
func WithBufferSize(bufferSize int) runnerOpt {
	return func(r *runner) {
		if bufferSize < 0 {
			bufferSize = 1
		}
		r.bufferSize = bufferSize
	}
}

// NewRunner creates a new runner with the given options.
// The default worker size is 10 and the default buffer size is 100.
func NewRunner(opts ...runnerOpt) Runner {
	r := &runner{
		workerSize: 10,
		bufferSize: 100,
		stop:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.tasks = make(chan *task, r.bufferSize)

	r.wg.Add(r.workerSize)

	for range r.workerSize {
		go func() {
			defer r.wg.Done()

			for {
				select {
				case t := <-r.tasks:
					r.run(t)
				case <-r.stop:
					return
				}
			}
		}()
	}

	return r
}

type yield struct {
	ctx   context.Context
	delay time.Duration
}

var _ error = (*yield)(nil)

func (y *yield) Error() string {
	return "task yielded"
}

type yieldOpt func(*yield)

// WithDelay makes the yielded task wait for the given duration before it is
// re-queued. Negative delays are treated as zero.
func WithDelay(delay time.Duration) yieldOpt {
	return func(y *yield) {
		if delay < 0 {
			delay = 0
		}
		y.delay = delay
	}
}

// Yield returns a special error that, when returned from a submitted
// function, re-queues the task instead of completing it, similar to
// runtime.Gosched(). The given context is used for the next execution.
func Yield(ctx context.Context, opts ...yieldOpt) error {
	y := &yield{
		ctx: ctx,
	}

	for _, opt := range opts {
		opt(y)
	}

	return y
}

// Yeild is a misspelling of Yield, kept for backward compatibility.
//
// Deprecated: Use Yield instead.
func Yeild(ctx context.Context, opts ...yieldOpt) error {
	return Yield(ctx, opts...)
}
