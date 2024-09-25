package task_test

import (
	"context"
	"sync"
	"testing"

	"ella.to/task"
)

func TestBasic(t *testing.T) {

	runner := task.NewRunner(
		task.WithBufferSize[string, string](10),
		task.WithPoolSize[string, string](10),
		task.WithWorkerSize[string, string](10),
		task.WithFn(func(ctx context.Context, name string) (string, error) {
			return "Hello, " + name, nil
		}),
	)
	defer runner.Close(context.Background())

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100000; i++ {
				result, err := runner.Submit(context.Background(), "Alice").Await(context.Background())

				if err != nil {
					t.Fatal(err)
				}

				if result != "Hello, Alice" {
					t.Fatalf("unexpected result: %s", result)
				}
			}
		}()
	}

	wg.Wait()
}
