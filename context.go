package task

import (
	"context"
	"time"
)

// mergedContext implements context.Context and merges two contexts.
type mergedContext struct {
	ctx1, ctx2 context.Context
	done       chan struct{}
}

// NewMergedContext returns a new context that merges two contexts.
func NewMergedContext(ctx1, ctx2 context.Context) context.Context {
	m := &mergedContext{
		ctx1: ctx1,
		ctx2: ctx2,
		done: make(chan struct{}),
	}

	go m.propagateCancellation()

	return m
}

// propagateCancellation listens for cancellation from either context.
func (m *mergedContext) propagateCancellation() {
	select {
	case <-m.ctx1.Done():
		close(m.done)
	case <-m.ctx2.Done():
		close(m.done)
	}
}

// Done returns a channel that is closed when either of the parent contexts is done.
func (m *mergedContext) Done() <-chan struct{} {
	return m.done
}

// Err returns the first error encountered from either context.
func (m *mergedContext) Err() error {
	if err := m.ctx1.Err(); err != nil {
		return err
	}
	return m.ctx2.Err()
}

// Deadline returns the earliest deadline from both contexts.
func (m *mergedContext) Deadline() (time.Time, bool) {
	d1, ok1 := m.ctx1.Deadline()
	d2, ok2 := m.ctx2.Deadline()

	if !ok1 && !ok2 {
		return time.Time{}, false
	}
	if !ok1 {
		return d2, ok2
	}
	if !ok2 {
		return d1, ok1
	}
	// Return the earlier of the two deadlines
	if d1.Before(d2) {
		return d1, true
	}
	return d2, true
}

// Value searches for a value in both contexts, giving priority to ctx1.
func (m *mergedContext) Value(key interface{}) interface{} {
	if val := m.ctx1.Value(key); val != nil {
		return val
	}
	return m.ctx2.Value(key)
}
