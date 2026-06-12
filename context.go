package task

import (
	"context"
	"sync"
	"time"
)

// mergedContext implements context.Context and merges two contexts.
//
// Cancellation is propagated with context.AfterFunc, so no goroutine is
// parked for the lifetime of the merged context. When only one parent is
// cancelable, the parent's Done channel is shared directly and no
// registration is needed at all.
type mergedContext struct {
	ctx1, ctx2 context.Context

	// done is nil when neither parent is cancelable, a parent's Done channel
	// when only one parent is cancelable, or an owned channel closed by the
	// AfterFunc callbacks when both parents are cancelable.
	done <-chan struct{}

	// stop1 and stop2 unregister the AfterFunc callbacks. They are non-nil
	// only when both parents are cancelable.
	stop1, stop2 func() bool
}

// NewMergedContext returns a new context that merges two contexts.
// It is canceled as soon as either parent is canceled, exposes the earlier
// of the two deadlines, and looks up values in ctx1 first, then ctx2.
func NewMergedContext(ctx1, ctx2 context.Context) context.Context {
	return newMergedContext(ctx1, ctx2)
}

func newMergedContext(ctx1, ctx2 context.Context) *mergedContext {
	m := &mergedContext{
		ctx1: ctx1,
		ctx2: ctx2,
	}

	d1, d2 := ctx1.Done(), ctx2.Done()
	switch {
	case d1 == nil && d2 == nil:
		// Neither parent can be canceled; Done stays nil.
	case d1 == nil:
		m.done = d2
	case d2 == nil:
		m.done = d1
	default:
		done := make(chan struct{})
		m.done = done

		var once sync.Once
		cancel := func() {
			once.Do(func() {
				close(done)
			})
		}

		m.stop1 = context.AfterFunc(ctx1, cancel)
		m.stop2 = context.AfterFunc(ctx2, cancel)
	}

	return m
}

// release unregisters the cancellation callbacks from both parents. It must
// only be called once no further use of Done is expected, e.g. when the
// owning task has completed.
func (m *mergedContext) release() {
	if m.stop1 != nil {
		m.stop1()
	}
	if m.stop2 != nil {
		m.stop2()
	}
}

// Done returns a channel that is closed when either of the parent contexts
// is done. It returns nil if neither parent can be canceled.
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
func (m *mergedContext) Value(key any) any {
	if val := m.ctx1.Value(key); val != nil {
		return val
	}
	return m.ctx2.Value(key)
}
