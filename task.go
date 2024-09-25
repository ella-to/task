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

type Future[R any] interface {
	Await(ctx context.Context) (R, error)
}

type futureFunc[R any] func(ctx context.Context) (R, error)

var _ Future[any] = futureFunc[any](nil)

func (f futureFunc[R]) Await(ctx context.Context) (R, error) {
	return f(ctx)
}

func newFutureError[R any](err error) futureFunc[R] {
	return func(ctx context.Context) (result R, _ error) {
		return result, err
	}
}

type Runner[V, R any] interface {
	Submit(ctx context.Context, value V) Future[R]
	Close(ctx context.Context) error
}

type task[V, R any] struct {
	ctx    context.Context
	value  V
	result chan R
	err    chan error
}

type runner[V, R any] struct {
	workerSize int
	poolSize   int
	bufferSize int
	fn         func(context.Context, V) (R, error)
	tasks      chan *task[V, R]
	tasksPool  chan *task[V, R]
	closed     chan struct{}
	wg         sync.WaitGroup
}

var _ Runner[any, any] = (*runner[any, any])(nil)

func (r *runner[V, R]) getTask(ctx context.Context) (*task[V, R], error) {
	select {
	case <-r.closed:
		return nil, ErrRunnerClosed
	case task := <-r.tasksPool:
		return task, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *runner[V, R]) putTask(task *task[V, R]) {
	select {
	case <-r.closed:
		return
	case r.tasksPool <- task:
	}
}

func (r *runner[V, R]) Submit(ctx context.Context, value V) Future[R] {
	t, err := r.getTask(ctx)
	if err != nil {
		return newFutureError[R](err)
	}

	var cancel func()

	t.ctx, cancel = context.WithCancel(ctx)
	t.value = value

	select {
	case <-r.closed:
		cancel()
		return newFutureError[R](ErrRunnerClosed)
	case r.tasks <- t:
	}

	return futureFunc[R](func(ctx context.Context) (result R, err error) {
		defer r.putTask(t)
		defer cancel()

		select {
		case <-r.closed:
			return result, ErrRunnerClosed
		case <-ctx.Done():
			return result, ctx.Err()
		case err = <-t.err:
			return result, err
		case result = <-t.result:
			return result, nil
		}
	})
}

func (r *runner[V, R]) Close(ctx context.Context) error {
	select {
	case <-r.closed:
		return ErrRunnerAlreadyClosed
	default:
		close(r.closed)
	}

	r.wg.Wait()

	return nil
}

type runnerOpt[V, R any] func(*runner[V, R])

func WithWorkerSize[V, R any](worker int) runnerOpt[V, R] {
	return func(r *runner[V, R]) {
		r.workerSize = worker
	}
}

func WithPoolSize[V, R any](poolSize int) runnerOpt[V, R] {
	return func(r *runner[V, R]) {
		r.poolSize = poolSize
	}
}

func WithBufferSize[V, R any](bufferSize int) runnerOpt[V, R] {
	return func(r *runner[V, R]) {
		r.bufferSize = bufferSize
	}
}

func WithFn[V, R any](fn func(context.Context, V) (R, error)) runnerOpt[V, R] {
	return func(r *runner[V, R]) {
		r.fn = fn
	}
}

func NewRunner[V, R any](opts ...runnerOpt[V, R]) Runner[V, R] {
	r := &runner[V, R]{
		workerSize: 10,
		poolSize:   10,
		bufferSize: 10000,
		fn:         func(context.Context, V) (r R, err error) { return },
		closed:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(r)
	}

	r.tasks = make(chan *task[V, R], r.bufferSize)
	r.tasksPool = make(chan *task[V, R], r.poolSize)

	for range r.poolSize {
		r.tasksPool <- &task[V, R]{
			result: make(chan R, 1),
			err:    make(chan error, 1),
		}
	}

	r.wg.Add(r.workerSize)

	for range r.workerSize {
		go func() {
			defer r.wg.Done()

			for {
				select {
				case <-r.closed:
					return
				case task, ok := <-r.tasks:
					if !ok {
						return
					}
					result, err := r.fn(task.ctx, task.value)
					if err != nil {
						task.err <- err
					} else {
						task.result <- result
					}
				}
			}
		}()
	}

	return r
}
