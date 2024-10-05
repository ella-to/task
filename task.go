package task

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrRunnerClosed        = errors.New("runner is closed")
	ErrRunnerAlreadyClosed = errors.New("runner is already closed")
)

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
	Submit(ctx context.Context, fn func(context.Context) error) Future
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

	return futureFunc(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-err:
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

func WithWorkerSize(worker int) runnerOpt {
	return func(r *runner) {
		r.workerSize = worker
	}
}

func WithBufferSize(bufferSize int) runnerOpt {
	return func(r *runner) {
		r.bufferSize = bufferSize
	}
}

func NewRunner(opts ...runnerOpt) Runner {
	r := &runner{
		workerSize: 10,
		bufferSize: 100,
		closed:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.workerSize == 0 {
		panic("worker size must be greater than 0, why are you even use this?")
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
