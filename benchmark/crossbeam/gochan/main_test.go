//go:build crossbeamcheck

package main

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/benchmark/crossbeam/workloads"
)

func TestNewChannelCapacity(t *testing.T) {
	for _, workload := range workloads.All() {
		require.Equal(t, workload.Capacity, cap(newChannel(workload)), workload.Name)
	}
}

func TestRunSmoke(t *testing.T) {
	for _, workload := range workloads.All() {
		if workload.Prefix != "bounded1024" && workload.Prefix != "bounded2p18" {
			continue
		}
		t.Run(workload.Name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			status := run([]string{workload.Name}, 1_000, &stdout, &stderr)
			require.Equal(t, 0, status, stderr.String())
			requireResultLine(t, workload.Name, stdout.String(), stderr.String())
		})
	}
}

func TestRunRejectsArguments(t *testing.T) {
	cases := map[string][]string{
		"no arguments":     nil,
		"two arguments":    {"bounded1024_mpmc", "extra"},
		"unknown workload": {"bounded1000_mpmc"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			require.Equal(t, 2, run(args, 1_000, &stdout, &stderr))
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), "usage:")
		})
	}
}

func TestRunRejectsMessageCount(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run([]string{"bounded1024_mpmc"}, 0, &stdout, &stderr))
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "not a positive multiple")
	require.NotContains(t, stderr.String(), "usage:")
}

// TestRunTopologyDrainsChannel runs the concurrent topologies on a channel with capacity 1, so
// every send can block. A receiver loop that stops early then leaves a sender blocked, and the
// test times out instead of passing.
func TestRunTopologyDrainsChannel(t *testing.T) {
	cases := []struct {
		topology workloads.Topology
		capacity int
	}{
		{workloads.SPSC, 1},
		{workloads.MPSC, 1},
		{workloads.MPMC, 1},
		{workloads.Seq, 1024},
	}
	for _, tc := range cases {
		t.Run(tc.topology.String(), func(t *testing.T) {
			c := make(chan uintptr, tc.capacity)
			require.NoError(t, runTopology(tc.topology, c, 1_000))
			require.Empty(t, c)
		})
	}
}

func TestRunTopologyRejectsUnknownTopology(t *testing.T) {
	err := runTopology(workloads.Topology(99), make(chan uintptr, 1), 1_000)
	require.Error(t, err)
}

// requireResultLine requires that a run wrote nothing to stderr and exactly one result line for
// name to stdout.
func requireResultLine(t *testing.T, name, stdout, stderr string) {
	t.Helper()
	require.Empty(t, stderr)
	pattern := `^` + regexp.QuoteMeta(name) + ` +Go chan +\d+\.\d{6} sec\n$`
	require.Regexp(t, regexp.MustCompile(pattern), stdout)
}
