package task_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"ella.to/task"
)

func TestTaskRunnerWihtoutError(t *testing.T) {
	runner := task.NewRunner(
		task.WithBufferSize(10),
		task.WithWorkerSize(10),
	)
	defer runner.Close(context.TODO())

	future := runner.Submit(context.TODO(), func(ctx context.Context) error {
		return nil
	})

	if err := future.Await(context.TODO()); err != nil {
		t.Fatal(err)
	}
}

func TestTaskRunnerWithError(t *testing.T) {
	runner := task.NewRunner(
		task.WithBufferSize(10),
		task.WithWorkerSize(10),
	)
	defer runner.Close(context.TODO())

	future := runner.Submit(context.TODO(), func(ctx context.Context) error {
		return fmt.Errorf("error")
	})

	if err := future.Await(context.TODO()); err == nil {
		t.Fatal("expected error, got nil")
	} else if err.Error() != "error" {
		t.Fatalf("expected error, got %v", err)
	}
}

func TestTaskRunnerClosed(t *testing.T) {
	runner := task.NewRunner(
		task.WithBufferSize(10),
		task.WithWorkerSize(10),
	)

	var count atomic.Int64
	future := runner.Submit(context.TODO(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			count.Add(1)
		}
		return nil
	})

	runner.Close(context.TODO())

	if err := future.Await(context.TODO()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}

	if count.Load() > 0 {
		t.Fatalf("expected 0, got %v", count.Load())
	}
}

func TestTaskRunnerMultiple(t *testing.T) {
	runner := task.NewRunner(
		task.WithBufferSize(1),
		task.WithWorkerSize(10),
	)

	var count atomic.Int64
	for i := 0; i < 100; i++ {
		runner.Submit(context.TODO(), func(ctx context.Context) error {
			count.Add(1)
			return nil
		})
	}

	runner.Close(context.TODO())

	if count.Load() != 100 {
		t.Fatalf("expected 100, got %v", count.Load())
	}
}
