package task_test

import (
	"context"
	"testing"
	"time"

	"ella.to/task"
)

// TestMergedContextDone ensures that Done() closes when either context is canceled.
func TestMergedContextDone(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	// Neither context is canceled, Done should not be closed yet
	select {
	case <-mergedCtx.Done():
		t.Fatal("Merged context should not be canceled yet")
	default:
	}

	// Cancel the first context
	cancel1()

	select {
	case <-mergedCtx.Done():
		// Success: merged context should be canceled
	case <-time.After(time.Second):
		t.Fatal("Merged context should have been canceled after ctx1 was canceled")
	}

	// Cancel the second context (shouldn't affect the already canceled merged context)
	cancel2()

	if err := mergedCtx.Err(); err == nil {
		t.Fatal("Merged context should return a non-nil error after cancellation")
	}
}

// TestMergedContextDeadline ensures that Deadline() returns the earliest deadline.
func TestMergedContextDeadline(t *testing.T) {
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	deadline, ok := mergedCtx.Deadline()
	if !ok {
		t.Fatal("Merged context should have a deadline")
	}

	if time.Until(deadline) > 3*time.Second {
		t.Fatalf("Merged context should have the earliest deadline (3s), got %v", deadline)
	}
}

// TestMergedContextNoDeadline ensures that Deadline() returns false if neither context has a deadline.
func TestMergedContextNoDeadline(t *testing.T) {
	ctx1 := context.Background()
	ctx2 := context.Background()

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	_, ok := mergedCtx.Deadline()
	if ok {
		t.Fatal("Merged context should not have a deadline")
	}
}

// TestMergedContextErr ensures that Err() returns the correct error after cancellation.
func TestMergedContextErr(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	// Neither context is canceled yet
	if mergedCtx.Err() != nil {
		t.Fatal("Merged context Err() should be nil before cancellation")
	}

	// Cancel ctx1
	cancel1()

	if mergedCtx.Err() == nil {
		t.Fatal("Merged context should return an error after ctx1 is canceled")
	}

	// Cancel ctx2 (merged context is already canceled, so this should not change its Err())
	cancel2()

	if mergedCtx.Err() == nil {
		t.Fatal("Merged context should return an error after ctx2 is canceled")
	}
}

// TestMergedContextValue ensures that Value() returns the correct values, prioritizing ctx1.
func TestMergedContextValue(t *testing.T) {
	ctx1 := context.WithValue(context.Background(), "key1", "value1")
	ctx2 := context.WithValue(context.Background(), "key2", "value2")

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	if got := mergedCtx.Value("key1"); got != "value1" {
		t.Fatalf("Expected value1 from ctx1, got %v", got)
	}

	if got := mergedCtx.Value("key2"); got != "value2" {
		t.Fatalf("Expected value2 from ctx2, got %v", got)
	}

	if got := mergedCtx.Value("nonexistent"); got != nil {
		t.Fatalf("Expected nil for nonexistent key, got %v", got)
	}
}

// TestMergedContextCancelPropagation ensures that merged context is canceled when both parent contexts are done.
func TestMergedContextCancelPropagation(t *testing.T) {
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	select {
	case <-mergedCtx.Done():
		t.Fatal("Merged context should not be canceled yet")
	case <-time.After(50 * time.Millisecond):
		// Success: Merged context still active
	}

	select {
	case <-mergedCtx.Done():
		// Success: merged context should be canceled after ctx1 timeout
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Merged context should have been canceled after ctx1 timeout")
	}
}

func TestMergedContextCancelFirstContext(t *testing.T) {
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()

	ctx2 := context.Background()

	mergedCtx := task.NewMergedContext(ctx1, ctx2)

	select {
	case <-mergedCtx.Done():
		t.Fatal("Merged context should not be canceled yet")
	case <-time.After(50 * time.Millisecond):
		// Success: Merged context still active
	}

	select {
	case <-mergedCtx.Done():
		// Success: merged context should be canceled after ctx1 timeout
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Merged context should have been canceled after ctx1 timeout")
	}
}
