package sync_testing

import (
	"runtime"
	"sync"
	"time"

	"github.com/fluxio/multierror"
)

// MaximizeContention starts N goroutines, each performing one of the provided
// functions, and executes all of them as concurrently as possible.  If 3
// functions are provided, then each function is run N/3 times.
func MaximizeContention(N int, f ...func()) {
	var ready, done sync.WaitGroup
	ready.Add(N)
	done.Add(N)
	for i := 0; i < N; i++ {
		idx := i % len(f)
		go func() {
			ready.Done() // we're ready to go!
			ready.Wait() // is everyone else ready too?
			f[idx]()     // GOOOOOOOO!
			done.Done()  // ok, we're done.
		}()
	}
	done.Wait()
}

// MaximizeContentionWithErrors operates identically to MaximizeContention but
// returns a multierror with all non-nil errors.
func MaximizeContentionWithErrors(N int, f ...func() error) error {
	var ready, done sync.WaitGroup
	ready.Add(N)
	done.Add(N)
	var mu sync.Mutex
	var errs multierror.Accumulator
	for i := 0; i < N; i++ {
		idx := i % len(f)
		go func() {
			ready.Done()    // we're ready to go!
			ready.Wait()    // is everyone else ready too?
			err := f[idx]() // GOOOOOOOO!
			done.Done()     // ok, we're done.
			mu.Lock()
			errs.Push(err)
			mu.Unlock()
		}()
	}
	done.Wait()
	return errs.Error()
}

// ParallelOpTracker is a utility structure designed to help detect operations
// that execute in parallel.  Typical usage is:
//
//    N := 1000  // approximate number of operations.
//    ops := NewParallelOpTracker(N)
//
//    ops.Do("sync-op")                     // never be done in parallel with other ops
//    MaximizeContention(N,
//       func() { ops.Do("something") },    // may be done in parallel
//       func() { ops.Do("other thing") })  // may be done in parallel
//    ops.Do("sync-op")                     // never be done in parallel with other ops
//
//    So(ops.During("sync-op"), ShouldNotContainKey, "something")
//    So(ops.During("sync-op"), ShouldNotContainKey, "other thing")
//    So(ops.During("sync-op"), ShouldNotContainKey, "sync-op")
//
type ParallelOpTracker struct {
	ch     chan string
	events []string
}

// NewParallelOpTracker returns an initialized ParallelOpTracker for the
// approximate number of events.
func NewParallelOpTracker(expectedNumOps int) *ParallelOpTracker {
	p := &ParallelOpTracker{}
	p.init(expectedNumOps)
	return p
}

func (o *ParallelOpTracker) init(expectedNumOps int) {
	o.ch = make(chan string, expectedNumOps*2*10)
	o.events = nil
}

// Do performs an operation.  All calls to Do() must be performed before any
// calls to Events() or During().
func (o *ParallelOpTracker) Do(op string) {
	o.ch <- "start " + op
	runtime.Gosched()
	time.Sleep(10 * time.Microsecond)
	runtime.Gosched()
	o.ch <- "stop " + op
}

// Events returns the ordered list of events that occurred.
func (o *ParallelOpTracker) Events() []string {
	if o.events == nil {
		o.events = []string{}
		close(o.ch)
		for e := range o.ch {
			o.events = append(o.events, e)
		}
	}
	return o.events
}

// During returns a map of events that occurred during a particular op.
func (o *ParallelOpTracker) During(op string) map[string]int {
	return getParallelOpsFor(o.Events(), op)
}

func parseOp(ev string) (action, op string) {
	if ev[:5] == "start" {
		return "start", ev[6:]
	}
	if ev[:4] == "stop" {
		return "stop", ev[5:]
	}
	panic("Corrupt event: " + ev)
}

func getParallelOpsFor(events []string, tgtOp string) map[string]int {
	parallelOps := map[string]int{}
	active := 0
	for _, ev := range events {
		action, op := parseOp(ev)
		if op == tgtOp {
			if action == "start" {
				active++
			} else {
				active--
			}
			if active > 1 {
				parallelOps[op]++
			}
		} else if active != 0 {
			parallelOps[op]++
		}
	}
	return parallelOps
}
