//go:build crossbeamcheck

// These tests run with recording on, so senders tag each value with their index and receivers
// keep what they got. Run has no way to stop one side when the other fails, so a queue that
// loses a message leaves a test waiting. The -timeout of go test bounds that wait.
package workloads

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var families = []string{"dpdk", "frontier", "dpdk-mpmc", "frontier-mpmc"}

var topologies = []Topology{Seq, SPSC, MPSC, MPMC}

// expectedType returns the %T of the queue family uses for topology, or "" if it has none.
func expectedType(family string, topology Topology) string {
	switch family {
	case "dpdk":
		switch topology {
		case Seq, SPSC:
			return "*dpdk.SPSCQueue[uintptr]"
		case MPSC:
			return "*dpdk.MPSCQueue[uintptr]"
		default:
			return "*dpdk.MPMCQueue[uintptr]"
		}
	case "frontier":
		switch topology {
		case MPSC:
			return "*frontier.MPSCQueue[uintptr]"
		case MPMC:
			return "*frontier.MPMCQueue[uintptr]"
		default:
			return ""
		}
	case "dpdk-mpmc":
		return "*dpdk.MPMCQueue[uintptr]"
	default:
		return "*frontier.MPMCQueue[uintptr]"
	}
}

func TestPrefixCapacities(t *testing.T) {
	expected := map[string]int{
		"bounded1":    1,
		"bounded1024": 1024,
		"bounded2p18": 262_144,
		"bounded2p23": 8_388_608,
	}
	require.Len(t, prefixes, len(expected))
	for _, p := range prefixes {
		require.Equal(t, expected[p.name], p.capacity, p.name)
	}
	for _, w := range All() {
		require.Equal(t, expected[w.Prefix], w.Capacity, w.Name)
	}
}

func TestAllNames(t *testing.T) {
	expected := []string{
		"bounded1_spsc", "bounded1_mpsc", "bounded1_mpmc",
		"bounded1024_spsc", "bounded1024_mpsc", "bounded1024_mpmc",
		"bounded2p18_spsc", "bounded2p18_mpsc", "bounded2p18_mpmc",
		"bounded2p23_spsc", "bounded2p23_mpsc", "bounded2p23_mpmc",
		"bounded2p23_seq",
	}
	var names []string
	for _, w := range All() {
		names = append(names, w.Name)
		looked, ok := Lookup(w.Name)
		require.True(t, ok, w.Name)
		require.Equal(t, w, looked)
	}
	require.Equal(t, expected, names)
}

func TestBuildCapacityAndType(t *testing.T) {
	for _, family := range families {
		for _, w := range All() {
			t.Run(family+"/"+w.Name, func(t *testing.T) {
				queue, err := Build(family, w)
				want := expectedType(family, w.Topology)
				if want == "" {
					require.ErrorIs(t, err, ErrUnsupported)
					return
				}
				require.NoError(t, err)
				require.Equal(t, want, fmt.Sprintf("%T", queue.Value))
				require.Equal(t, w.Capacity, queue.Capacity)
				capacityOf, ok := queue.Value.(interface{ Capacity() int })
				require.True(t, ok)
				require.Equal(t, w.Capacity, capacityOf.Capacity())
			})
		}
	}
}

func TestDelivery(t *testing.T) {
	const messages = 100_000
	for _, family := range families {
		for _, topology := range topologies {
			// seq sends everything before receiving, so it runs only where every message fits.
			capacities := []int{1, 1 << 10, 1 << 18}
			if topology == Seq {
				capacities = []int{1 << 18}
			}
			if expectedType(family, topology) == "" {
				continue
			}
			for _, capacity := range capacities {
				name := fmt.Sprintf("%s/%v/%d", family, topology, capacity)
				t.Run(name, func(t *testing.T) {
					w := Workload{Name: name, Capacity: capacity, Topology: topology}
					queue, err := Build(family, w)
					require.NoError(t, err)
					require.NoError(t, Warm(queue))
					got, err := Run(topology, queue.Send, queue.Recv, messages)
					require.NoError(t, err)
					require.NoError(t, checkDelivery(topology, got, messages))
				})
			}
		}
	}
}

// checkDelivery reports whether got is what topology's senders sent with messages in total:
// every value exactly once, and each sender's values in increasing order within each receiver.
// The expected values come from the topology, never from what arrived.
func checkDelivery(topology Topology, got [][]uintptr, messages int) error {
	senders, receivers := 1, 1
	switch topology {
	case MPSC:
		senders = Threads
	case MPMC:
		senders, receivers = Threads, Threads
	}
	if len(got) != receivers {
		return fmt.Errorf("got %d receiver slices, want %d", len(got), receivers)
	}
	perSender := messages / senders
	seen := make(map[uintptr]bool, messages)
	for r, values := range got {
		last := make([]int, senders)
		for k := range last {
			last[k] = -1
		}
		for _, v := range values {
			sender := int(v >> 32)
			i := int(v & (1<<32 - 1))
			if sender >= senders || i >= perSender {
				return fmt.Errorf("receiver %d got %#x, which no sender sends", r, v)
			}
			if seen[v] {
				return fmt.Errorf("receiver %d got %#x twice", r, v)
			}
			if i <= last[sender] {
				return fmt.Errorf("receiver %d got %#x after index %d", r, v, last[sender])
			}
			seen[v] = true
			last[sender] = i
		}
	}
	if len(seen) != senders*perSender {
		return fmt.Errorf("got %d distinct values, want %d", len(seen), senders*perSender)
	}
	return nil
}

func TestRunRejectsMessageCount(t *testing.T) {
	errCalled := errors.New("called")
	send := func(int, int) error { return errCalled }
	recv := func(int, *[]uintptr) error { return errCalled }
	for _, messages := range []int{-Threads, 0, 1, Threads + 1} {
		for _, topology := range topologies {
			_, err := Run(topology, send, recv, messages)
			require.Error(t, err, "%v with %d messages", topology, messages)
			require.NotErrorIs(t, err, errCalled)
		}
	}
}
