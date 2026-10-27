//go:build crossbeamcheck

package main

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/benchmark/crossbeam/workloads"
)

func TestRunSmoke(t *testing.T) {
	for _, family := range workloads.Families() {
		for _, workload := range workloads.All() {
			if workload.Prefix != "bounded1024" && workload.Prefix != "bounded2p18" {
				continue
			}
			if family == "frontier" && workload.Topology == workloads.SPSC {
				continue
			}
			t.Run(family+"/"+workload.Name, func(t *testing.T) {
				var stdout, stderr bytes.Buffer
				status := run([]string{family, workload.Name}, 1_000, &stdout, &stderr)
				require.Equal(t, 0, status, stderr.String())
				requireResultLine(t, workload.Name, family, stdout.String(), stderr.String())
			})
		}
	}
}

func TestRunRejectsArguments(t *testing.T) {
	cases := map[string][]string{
		"no arguments":     nil,
		"one argument":     {"dpdk"},
		"three arguments":  {"dpdk", "bounded1024_mpmc", "extra"},
		"unknown family":   {"chan", "bounded1024_mpmc"},
		"unknown workload": {"dpdk", "bounded1000_mpmc"},
		"frontier seq":     {"frontier", "bounded2p23_seq"},
		"frontier spsc":    {"frontier", "bounded1_spsc"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			require.Equal(t, 2, run(args, 1_000, &stdout, &stderr))
			require.Empty(t, stdout.String())
		})
	}
}

func TestRunAcceptsDPDKMPMCWithMPMC(t *testing.T) {
	var stdout, stderr bytes.Buffer
	status := run([]string{"dpdk-mpmc", "bounded1024_mpmc"}, 1_000, &stdout, &stderr)
	require.Equal(t, 0, status, stderr.String())
	requireResultLine(t, "bounded1024_mpmc", "dpdk-mpmc", stdout.String(), stderr.String())
}

// requireResultLine requires that a run wrote nothing to stderr and exactly one result line for
// name and family to stdout.
func requireResultLine(t *testing.T, name, family, stdout, stderr string) {
	t.Helper()
	require.Empty(t, stderr)
	pattern := `^` + regexp.QuoteMeta(name) + ` +Go ` + regexp.QuoteMeta(family) +
		` +\d+\.\d{6} sec\n$`
	require.Regexp(t, regexp.MustCompile(pattern), stdout)
}
