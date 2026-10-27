// Command ring times one crossbeam benchmark workload on a ring queue from ring/dpdk or
// ring/frontier, and prints one result line.
//
// Usage: ring <family> <workload>
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/freshdresch/go-data-structures/benchmark/crossbeam/workloads"
)

// families are the queue families workloads.Build accepts.
var families = []string{"dpdk", "frontier", "dpdk-mpmc", "frontier-mpmc"}

func main() { os.Exit(run(os.Args[1:], 5_000_000, os.Stdout, os.Stderr)) }

// run runs the workload that args name with messages in total and returns the exit status: 0
// on success, 1 if the queue fails, and 2 for bad arguments.
func run(args []string, messages int, stdout, stderr io.Writer) int {
	if len(args) != 2 || !slices.Contains(families, args[0]) {
		printUsage(stderr)
		return 2
	}
	family := args[0]
	workload, ok := workloads.Lookup(args[1])
	if !ok || messages <= 0 || messages%workloads.Threads != 0 {
		printUsage(stderr)
		return 2
	}

	queue, err := workloads.Build(family, workload)
	if errors.Is(err, workloads.ErrUnsupported) {
		_, _ = fmt.Fprintln(stderr, err)
		printUsage(stderr)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if err := workloads.Warm(queue); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	runtime.GC()

	start := time.Now()
	_, err = workloads.Run(workload.Topology, queue.Send, queue.Recv, messages)
	elapsed := time.Since(start)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	workloads.Print(stdout, workload.Name, "Go "+family, elapsed)
	return 0
}

// printUsage writes the usage text. A program that cannot write to stderr has nowhere to report
// that, so write errors are discarded, here and for the other stderr messages.
func printUsage(w io.Writer) {
	var names []string
	for _, workload := range workloads.All() {
		names = append(names, workload.Name)
	}
	_, _ = fmt.Fprintln(w, "usage: ring <family> <workload>")
	_, _ = fmt.Fprintln(w, "families:", strings.Join(families, ", "))
	_, _ = fmt.Fprintln(w, "workloads:", strings.Join(names, ", "))
}
