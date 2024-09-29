package task_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"ella.to/task"
)

func TestBasic(t *testing.T) {

	runner := task.NewRunner(
		func(ctx context.Context, name string) (string, error) {
			return "Hello, " + name, nil
		},
		task.WithBufferSize[string, string](10),
		task.WithPoolSize[string, string](10),
		task.WithWorkerSize[string, string](10),
	)
	defer runner.Close(context.Background())

	var wg sync.WaitGroup
	var errs []error

	errs = make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < 100000; i++ {
				result, err := runner.Submit(context.Background(), "Alice").Await(context.Background())

				if err != nil {
					errs[idx] = err
					return
				}

				if result != "Hello, Alice" {
					errs[idx] = fmt.Errorf("unexpected result: %s", result)
				}
			}
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
}
