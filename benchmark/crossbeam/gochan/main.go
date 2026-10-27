// Command gochan times one crossbeam benchmark workload on a Go channel and prints one result
// line.
//
// It derives from crossbeam-rs/crossbeam crossbeam-channel/benchmarks/go.go at commit 099d046,
// which is licensed under the MIT license. Copyright (c) 2019 The Crossbeam Project Developers.
// The changes are: the topologies take the channel and the message count, the message count
// and thread count come from the workloads package, the select variants are dropped, and the
// program times one workload per run.
//
// Usage: gochan <workload>
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/freshdresch/go-data-structures/benchmark/crossbeam/workloads"
)

func main() { os.Exit(run(os.Args[1:], workloads.Messages, os.Stdout, os.Stderr)) }

// run runs the workload that args name with messages in total and returns the exit status: 0
// on success, and 2 for bad arguments or a bad message count.
func run(args []string, messages int, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		printUsage(stderr)
		return 2
	}
	workload, ok := workloads.Lookup(args[0])
	if !ok {
		printUsage(stderr)
		return 2
	}
	if messages <= 0 || messages%workloads.Threads != 0 {
		_, _ = fmt.Fprintf(
			stderr,
			"gochan: %d messages is not a positive multiple of %d\n",
			messages,
			workloads.Threads,
		)
		return 2
	}

	c := newChannel(workload)
	warm(c, workload.Capacity)
	runtime.GC()

	start := time.Now()
	runTopology(workload.Topology, c, messages)
	elapsed := time.Since(start)
	workloads.Print(stdout, workload.Name, "Go chan", elapsed)
	return 0
}

// newChannel returns the channel that the workload's topology runs on, with the workload's
// capacity.
func newChannel(workload workloads.Workload) chan uintptr {
	return make(chan uintptr, workload.Capacity)
}

// warm sends capacity values and then receives them on the calling goroutine, so the timed run
// starts with every slot touched.
func warm(c chan uintptr, capacity int) {
	for i := range capacity {
		c <- uintptr(i)
	}
	for range capacity {
		<-c
	}
}

// runTopology runs the topology on c with messages in total.
func runTopology(topology workloads.Topology, c chan uintptr, messages int) {
	switch topology {
	case workloads.Seq:
		seq(c, messages)
	case workloads.SPSC:
		spsc(c, messages)
	case workloads.MPSC:
		mpsc(c, messages)
	case workloads.MPMC:
		mpmc(c, messages)
	}
}

// seq sends every message and then receives them all, on one goroutine.
func seq(c chan uintptr, messages int) {
	for i := range messages {
		c <- uintptr(i)
	}
	for range messages {
		<-c
	}
}

// spsc sends messages from one goroutine and receives them on the calling goroutine.
func spsc(c chan uintptr, messages int) {
	done := make(chan bool)

	go func() {
		for i := range messages {
			c <- uintptr(i)
		}
		done <- true
	}()

	for range messages {
		<-c
	}

	<-done
}

// mpsc sends messages from workloads.Threads goroutines and receives them on the calling
// goroutine.
func mpsc(c chan uintptr, messages int) {
	done := make(chan bool)

	for range workloads.Threads {
		go func() {
			for i := range messages / workloads.Threads {
				c <- uintptr(i)
			}
			done <- true
		}()
	}

	for range messages {
		<-c
	}

	for range workloads.Threads {
		<-done
	}
}

// mpmc sends messages from workloads.Threads goroutines and receives them on workloads.Threads
// goroutines.
func mpmc(c chan uintptr, messages int) {
	done := make(chan bool)

	for range workloads.Threads {
		go func() {
			for i := range messages / workloads.Threads {
				c <- uintptr(i)
			}
			done <- true
		}()
	}

	for range workloads.Threads {
		go func() {
			for range messages / workloads.Threads {
				<-c
			}
			done <- true
		}()
	}

	for range 2 * workloads.Threads {
		<-done
	}
}

// printUsage writes the usage text. A program that cannot write to stderr has nowhere to report
// that, so write errors are discarded.
func printUsage(w io.Writer) {
	var names []string
	for _, workload := range workloads.All() {
		names = append(names, workload.Name)
	}
	_, _ = fmt.Fprintln(w, "usage: gochan <workload>")
	_, _ = fmt.Fprintln(w, "workloads:", strings.Join(names, ", "))
}
