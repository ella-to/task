package task_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"ella.to/task"
)

// TestSubmitAfterClose ensures submitting to a closed runner returns a future
// resolved with ErrRunnerClosed instead of panicking.
func TestSubmitAfterClose(t *testing.T) {
	runner := task.NewRunner()

	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	future := runner.Submit(context.Background(), func(ctx context.Context) error {
		return nil
	})

	if err := future.Await(context.Background()); !errors.Is(err, task.ErrRunnerClosed) {
		t.Fatalf("expected %v, got %v", task.ErrRunnerClosed, err)
	}
}

// TestCloseTwice ensures a second Close returns ErrRunnerAlreadyClosed
// instead of panicking on a double close.
func TestCloseTwice(t *testing.T) {
	runner := task.NewRunner()

	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := runner.Close(context.Background()); !errors.Is(err, task.ErrRunnerAlreadyClosed) {
		t.Fatalf("expected %v, got %v", task.ErrRunnerAlreadyClosed, err)
	}
}

// TestCloseConcurrent ensures concurrent Close calls do not panic and exactly
// one of them succeeds.
func TestCloseConcurrent(t *testing.T) {
	runner := task.NewRunner()

	const n = 8
	errs := make([]error, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			errs[i] = runner.Close(context.Background())
		}()
	}
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, task.ErrRunnerAlreadyClosed) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful Close, got %d", succeeded)
	}
}

// TestSubmitCloseConcurrent hammers Submit while Close runs to make sure the
// runner never panics with a send on a closed channel.
func TestSubmitCloseConcurrent(t *testing.T) {
	for i := 0; i < 50; i++ {
		runner := task.NewRunner(
			task.WithWorkerSize(4),
			task.WithBufferSize(2),
		)

		var wg sync.WaitGroup
		for j := 0; j < 8; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 20; k++ {
					runner.Submit(context.Background(), func(ctx context.Context) error {
						return nil
					})
				}
			}()
		}

		runner.Close(context.Background())
		wg.Wait()
	}
}

// TestAwaitMultipleTimes ensures a future can be awaited more than once and
// from multiple goroutines, returning the same result every time.
func TestAwaitMultipleTimes(t *testing.T) {
	runner := task.NewRunner()
	defer runner.Close(context.Background())

	wantErr := errors.New("boom")
	future := runner.Submit(context.Background(), func(ctx context.Context) error {
		return wantErr
	})

	for i := 0; i < 3; i++ {
		if err := future.Await(context.Background()); !errors.Is(err, wantErr) {
			t.Fatalf("await %d: expected %v, got %v", i, wantErr, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := future.Await(context.Background()); !errors.Is(err, wantErr) {
				t.Errorf("expected %v, got %v", wantErr, err)
			}
		}()
	}
	wg.Wait()
}

// TestWorkerSizeZero ensures a worker size of 0 falls back to 1 worker
// instead of deadlocking with no workers at all.
func TestWorkerSizeZero(t *testing.T) {
	runner := task.NewRunner(task.WithWorkerSize(0))
	defer runner.Close(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runner.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		}).Await(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task never ran; runner has no workers")
	}
}

// TestCloseRespectsContext ensures Close gives up waiting when its context
// expires while a task is still running.
func TestCloseRespectsContext(t *testing.T) {
	runner := task.NewRunner(task.WithWorkerSize(1))

	release := make(chan struct{})
	runner.Submit(context.Background(), func(ctx context.Context) error {
		<-release // ignore cancellation on purpose
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := runner.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected %v, got %v", context.DeadlineExceeded, err)
	}

	close(release)
}

// TestAwaitContextCanceled ensures Await returns the await context's error
// while the task keeps running.
func TestAwaitContextCanceled(t *testing.T) {
	runner := task.NewRunner()
	defer runner.Close(context.Background())

	release := make(chan struct{})
	future := runner.Submit(context.Background(), func(ctx context.Context) error {
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := future.Await(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}

	close(release)

	if err := future.Await(context.Background()); err != nil {
		t.Fatalf("expected nil after task completion, got %v", err)
	}
}

// TestYieldCanceledBySubmitContext ensures a yielding task stops with the
// submit context's error when that context is canceled.
func TestYieldCanceledBySubmitContext(t *testing.T) {
	runner := task.NewRunner(task.WithWorkerSize(1))
	defer runner.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	var once sync.Once

	future := runner.Submit(ctx, func(ctx context.Context) error {
		once.Do(func() { close(started) })
		return task.Yield(ctx, task.WithDelay(10*time.Millisecond))
	})

	<-started
	cancel()

	if err := future.Await(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}
}

// TestYieldRunnerClosed ensures a yielding task resolves with
// ErrRunnerClosed when the runner is closed mid-yield, instead of panicking
// on the closed task queue.
func TestYieldRunnerClosed(t *testing.T) {
	runner := task.NewRunner(task.WithWorkerSize(1))

	started := make(chan struct{})
	var once sync.Once

	future := runner.Submit(context.Background(), func(ctx context.Context) error {
		once.Do(func() { close(started) })
		return task.Yield(ctx, task.WithDelay(10*time.Millisecond))
	})

	<-started
	if err := runner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := future.Await(context.Background()); !errors.Is(err, task.ErrRunnerClosed) {
		t.Fatalf("expected %v, got %v", task.ErrRunnerClosed, err)
	}
}

// TestRunnerClosedDeterministic runs the close-cancels-task scenario many
// times: a task canceled by the runner closing must always report
// ErrRunnerClosed, never a bare context.Canceled.
func TestRunnerClosedDeterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		runner := task.NewRunner(task.WithWorkerSize(1))

		future := runner.Submit(context.Background(), func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		if err := runner.Close(context.Background()); err != nil {
			t.Fatal(err)
		}

		if err := future.Await(context.Background()); !errors.Is(err, task.ErrRunnerClosed) {
			t.Fatalf("iteration %d: expected %v, got %v", i, task.ErrRunnerClosed, err)
		}
	}
}
