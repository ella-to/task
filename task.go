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

type futureFunc func(ctx context.Context) error

func (f futureFunc) Await(ctx context.Context) error {
	return f(ctx)
}

type task struct {
	ctx context.Context
	fn  func(ctx context.Context) error
	err chan error
}

type Runner interface {
	// Submit a task to be executed by the runner's workers. the context passed to the Submit function will be merged with the runner's context.
	// and the merged context will be passed to the submitted function. This ensures that if the runner is closed, the submitted function will
	// be canceled.
	Submit(ctx context.Context, fn func(context.Context) error) Future
	// Close the runner and wait for all the submitted tasks to be completed.
	// Calling this function again after closure will return ErrRunnerAlreadyClosed.
	Close(ctx context.Context) error
}

type runner struct {
	workerSize int
	bufferSize int
	tasks      chan *task
	closed     chan struct{}
	wg         sync.WaitGroup
}

var _ Runner = (*runner)(nil)

// The given context will be passed to the submitted function.
func (r *runner) Submit(ctx context.Context, fn func(context.Context) error) Future {
	err := make(chan error, 1)
	futureErr := make(chan error, 1)

	select {
	case <-r.closed:
		err <- ErrRunnerClosed
	default:
		r.tasks <- &task{
			ctx: ctx,
			fn:  fn,
			err: err,
		}
	}

	go func() {
		for {
			select {
			case <-r.closed:
				futureErr <- ErrRunnerClosed
				return
			case <-ctx.Done():
				futureErr <- ctx.Err()
				return
			case e := <-err:
				if yeild, ok := e.(*yeild); ok {
					if yeild.delay > 0 {
						select {
						case <-ctx.Done():
							futureErr <- ctx.Err()
							return
						case <-time.After(yeild.delay):
						}
					}
					r.tasks <- &task{
						ctx: yeild.ctx,
						fn:  fn,
						err: err,
					}
				} else {
					futureErr <- e
					return
				}
			}
		}
	}()

	return futureFunc(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-futureErr:
			return e
		}
	})
}

func (r *runner) Close(ctx context.Context) error {
	select {
	case <-r.closed:
		return ErrRunnerAlreadyClosed
	default:
		close(r.tasks)
		close(r.closed)
	}

	r.wg.Wait()

	return nil
}

type runnerOpt func(*runner)

// WithWorkerSize sets the number of workers that will be used to execute the submitted tasks.
// if the worker size is less than or equal to 0, it will be set to 1.
func WithWorkerSize(worker int) runnerOpt {
	return func(r *runner) {
		if worker < 0 {
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
		closed:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	r.tasks = make(chan *task, r.bufferSize)

	r.wg.Add(r.workerSize)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-r.closed
		cancel()
	}()

	for range r.workerSize {
		go func() {
			defer r.wg.Done()

			for {
				task, ok := <-r.tasks
				if !ok {
					return
				}

				task.err <- task.fn(NewMergedContext(task.ctx, ctx))
			}
		}()
	}

	return r
}

type yeild struct {
	ctx   context.Context
	delay time.Duration
}

var _ error = (*yeild)(nil)

func (y *yeild) Error() string {
	return ""
}

type yeildOpt func(*yeild)

func WithDelay(delay time.Duration) yeildOpt {
	return func(y *yeild) {
		y.delay = delay
	}
}

func Yeild(ctx context.Context, opts ...yeildOpt) *yeild {
	y := &yeild{
		ctx: ctx,
	}

	for _, opt := range opts {
		opt(y)
	}

	return y
}
