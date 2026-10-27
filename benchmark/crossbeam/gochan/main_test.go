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
		if workload.Topology == workloads.Seq {
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
		})
	}
}

// requireResultLine requires that a run wrote nothing to stderr and exactly one result line for
// name to stdout.
func requireResultLine(t *testing.T, name, stdout, stderr string) {
	t.Helper()
	require.Empty(t, stderr)
	pattern := `^` + regexp.QuoteMeta(name) + ` +Go chan +\d+\.\d{6} sec\n$`
	require.Regexp(t, regexp.MustCompile(pattern), stdout)
}
