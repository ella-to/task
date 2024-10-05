```
████████╗░█████╗░░██████╗██╗░░██╗
╚══██╔══╝██╔══██╗██╔════╝██║░██╔╝
░░░██║░░░███████║╚█████╗░█████═╝░
░░░██║░░░██╔══██║░╚═══██╗██╔═██╗░
░░░██║░░░██║░░██║██████╔╝██║░╚██╗
░░░╚═╝░░░╚═╝░░╚═╝╚═════╝░╚═╝░░╚═╝
```

# Introduction

A lightweight and efficient task runner designed to manage and execute tasks concurrently. This library is ideal for building worker pools and scheduling asynchronous jobs with ease.

# Features

- [x] Worker Pool: Efficiently manage multiple concurrent tasks with configurable worker limits.
- [x] Task Scheduling: Supports scheduling tasks to run in the background.
- [x] Context Management: Handle context propagation for better control over task lifecycle.
- [x] Easy Integration: Seamlessly integrate into any Go project.

# Installation

To install the package, use the following command:

```bash
go get ella.to/task
```

# Usage

Below is a basic example of how to use the Task Runner:

```golang
package main

import (
    "context"
    "sync/atomic"

    "ella.to/task"
)


func main() {
    runner := task.NewRunner(
		task.WithBufferSize(10),
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
        panic("expect count to be 100")
    }
}
```
