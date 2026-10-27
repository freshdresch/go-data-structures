// Package workloads holds what the Go programs of the crossbeam benchmark suite share: the
// capacity table, the four topologies, the ring hot loops, and the output format.
package workloads

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// Threads is the number of senders in the mpsc and mpmc topologies, and of receivers in mpmc.
const Threads = 4

// Topology is how many goroutines send and receive, and whether sending and receiving overlap.
type Topology int

const (
	// Seq sends every message and then receives them all, on one goroutine.
	Seq Topology = iota
	// SPSC has one sender and one receiver.
	SPSC
	// MPSC has Threads senders and one receiver.
	MPSC
	// MPMC has Threads senders and Threads receivers.
	MPMC
)

// String returns the lowercase topology name used in workload names.
func (t Topology) String() string {
	switch t {
	case Seq:
		return "seq"
	case SPSC:
		return "spsc"
	case MPSC:
		return "mpsc"
	case MPMC:
		return "mpmc"
	}
	return fmt.Sprintf("Topology(%d)", int(t))
}

// Workload is one named benchmark: a queue capacity and a topology.
type Workload struct {
	Name     string
	Prefix   string
	Capacity int
	Topology Topology
}

// prefix names a capacity, as the first part of a workload name.
type prefix struct {
	name     string
	capacity int
}

// prefixes is the capacity table, smallest first.
var prefixes = []prefix{
	{name: "bounded1", capacity: 1},
	{name: "bounded1024", capacity: 1 << 10},
	{name: "bounded2p18", capacity: 1 << 18},
	{name: "bounded2p23", capacity: 1 << 23},
}

func newWorkload(p prefix, topology Topology) Workload {
	return Workload{
		Name:     p.name + "_" + topology.String(),
		Prefix:   p.name,
		Capacity: p.capacity,
		Topology: topology,
	}
}

// message returns the i-th value that sender sends. Recording tags it with the sender, so the
// tests can check each sender's order. Timed builds send i, as upstream does.
func message(sender, i int) uintptr {
	if recording {
		return uintptr(sender)<<32 | uintptr(i)
	}
	return uintptr(i)
}

// All returns the 13 workloads, in a new slice on every call.
func All() []Workload {
	var all []Workload
	for _, p := range prefixes {
		for _, topology := range []Topology{SPSC, MPSC, MPMC} {
			all = append(all, newWorkload(p, topology))
		}
	}
	// seq sends every message before receiving any, so it runs only where every message fits.
	return append(all, newWorkload(prefixes[len(prefixes)-1], Seq))
}

// Lookup returns the workload with the given name, and whether it exists.
func Lookup(name string) (Workload, bool) {
	for _, w := range All() {
		if w.Name == name {
			return w, true
		}
	}
	return Workload{}, false
}

// Print writes one result line in the format plot.py parses. It discards a write error, so a
// failed write shows up only as a missing line.
func Print(w io.Writer, name, kind string, elapsed time.Duration) {
	_, _ = fmt.Fprintf(w, "%-25v %-15v %7.6f sec\n", name, kind, elapsed.Seconds())
}

// SendFunc sends count messages as the given sender.
type SendFunc func(sender, count int) error

// RecvFunc receives count messages. When recording is on, it appends them to got.
type RecvFunc func(count int, got *[]uintptr) error

// Run runs topology with messages in total and returns what each receiver got. The slices are
// empty unless recording is on. Messages must be a positive multiple of Threads.
//
// Run joins every goroutine it starts and returns the first error, senders before receivers.
// An error on one side does not stop the other, so a failing queue can leave Run waiting.
func Run(topology Topology, send SendFunc, recv RecvFunc, messages int) ([][]uintptr, error) {
	if messages <= 0 || messages%Threads != 0 {
		return nil, fmt.Errorf(
			"workloads: %d messages is not a positive multiple of %d",
			messages,
			Threads,
		)
	}
	if topology < Seq || topology > MPMC {
		return nil, fmt.Errorf("workloads: unknown topology %v", topology)
	}
	return runTopology(topology, send, recv, messages)
}

// Queue is a ring built for one family and workload.
type Queue struct {
	Value    any // the queue itself, so tests can check its type
	Capacity int
	Send     SendFunc
	Recv     RecvFunc
}

// ErrUnsupported reports a family and topology pair that has no queue.
var ErrUnsupported = errors.New("workloads: family does not support topology")

// Build returns the ring that family uses for w, with default options. The families are dpdk,
// frontier, dpdk-mpmc, and frontier-mpmc. A pair with no queue returns a wrapped ErrUnsupported.
func Build(family string, w Workload) (Queue, error) {
	return build(family, w)
}

// Warm sends q.Capacity messages and receives them on the calling goroutine, so the timed run
// starts with every slot touched.
func Warm(q Queue) error {
	if err := q.Send(0, q.Capacity); err != nil {
		return err
	}
	var discard []uintptr
	return q.Recv(q.Capacity, &discard)
}
