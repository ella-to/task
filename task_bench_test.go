package task_test

import (
	"context"
	"fmt"
	"testing"

	"ella.to/task"
)

func noopTask(ctx context.Context) error { return nil }

// BenchmarkSubmitAwait measures the round-trip latency of submitting a single
// task and waiting for its result.
func BenchmarkSubmitAwait(b *testing.B) {
	runner := task.NewRunner(
		task.WithWorkerSize(4),
		task.WithBufferSize(128),
	)
	defer runner.Close(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.Submit(ctx, noopTask).Await(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSubmitAwaitParallel measures submit/await throughput under
// concurrent producers.
func BenchmarkSubmitAwaitParallel(b *testing.B) {
	runner := task.NewRunner(
		task.WithWorkerSize(8),
		task.WithBufferSize(256),
	)
	defer runner.Close(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := runner.Submit(ctx, noopTask).Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSubmitBatchAwait submits a batch of tasks first and then awaits
// them all, measuring pipelined throughput rather than round-trip latency.
func BenchmarkSubmitBatchAwait(b *testing.B) {
	const batch = 100

	runner := task.NewRunner(
		task.WithWorkerSize(8),
		task.WithBufferSize(batch),
	)
	defer runner.Close(context.Background())

	ctx := context.Background()
	futures := make([]task.Future, batch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range futures {
			futures[j] = runner.Submit(ctx, noopTask)
		}
		for _, f := range futures {
			if err := f.Await(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkYield measures the cost of a task yielding back to the runner a
// fixed number of times before completing.
func BenchmarkYield(b *testing.B) {
	for _, yields := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("yields-%d", yields), func(b *testing.B) {
			runner := task.NewRunner(
				task.WithWorkerSize(1),
				task.WithBufferSize(16),
			)
			defer runner.Close(context.Background())

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				err := runner.Submit(ctx, func(ctx context.Context) error {
					if count < yields {
						count++
						return task.Yeild(ctx)
					}
					return nil
				}).Await(ctx)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWorkerScaling measures throughput across different worker pool
// sizes with a fixed batch of tasks per iteration.
func BenchmarkWorkerScaling(b *testing.B) {
	const batch = 64

	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			runner := task.NewRunner(
				task.WithWorkerSize(workers),
				task.WithBufferSize(batch),
			)
			defer runner.Close(context.Background())

			ctx := context.Background()
			futures := make([]task.Future, batch)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := range futures {
					futures[j] = runner.Submit(ctx, noopTask)
				}
				for _, f := range futures {
					if err := f.Await(ctx); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkMergedContextNew measures the cost of creating a merged context.
func BenchmarkMergedContextNew(b *testing.B) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = task.NewMergedContext(ctx1, ctx2)
	}
}

// BenchmarkMergedContextValue measures value lookups through a merged context.
func BenchmarkMergedContextValue(b *testing.B) {
	type key string

	ctx1 := context.WithValue(context.Background(), key("k1"), "v1")
	ctx2 := context.WithValue(context.Background(), key("k2"), "v2")

	merged := task.NewMergedContext(ctx1, ctx2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if merged.Value(key("k2")) != "v2" {
			b.Fatal("unexpected value")
		}
	}
}

// BenchmarkMergedContextCancel measures creation plus cancellation
// propagation through a merged context.
func BenchmarkMergedContextCancel(b *testing.B) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(parent)
		merged := task.NewMergedContext(ctx, parent)
		cancel()
		<-merged.Done()
	}
}
