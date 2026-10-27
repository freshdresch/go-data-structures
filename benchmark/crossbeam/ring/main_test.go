//go:build crossbeamcheck

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/freshdresch/go-data-structures/benchmark/crossbeam/workloads"
)

func TestRunSmoke(t *testing.T) {
	for _, family := range families {
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
				output := stdout.String()
				require.Equal(t, 1, strings.Count(output, "\n"), output)
				require.True(t, strings.HasPrefix(output, workload.Name+" "), output)
				require.Contains(t, output, "Go "+family)
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
}
