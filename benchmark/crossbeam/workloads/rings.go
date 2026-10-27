package workloads

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
	"github.com/freshdresch/go-data-structures/ring/frontier"
)

// queueType is one of the five queue types a family can use.
type queueType int

const (
	dpdkSPSC queueType = iota
	dpdkMPSC
	dpdkMPMC
	frontierMPSC
	frontierMPMC
)

// build does the work of Build. Send and Recv are closures over the concrete queue, so each
// calls its hot loop directly.
func build(family string, w Workload) (Queue, error) {
	chosen, err := chooseQueue(family, w.Topology)
	if err != nil {
		return Queue{}, err
	}
	queue := Queue{Capacity: w.Capacity}
	switch chosen {
	case dpdkSPSC:
		q, err := dpdk.NewSPSCQueue[uintptr](w.Capacity)
		if err != nil {
			return Queue{}, constructorError(family, w, err)
		}
		queue.Value = q
		queue.Send = func(sender, count int) error { return sendDPDKSPSC(q, sender, count) }
		queue.Recv = func(count int, got *[]uintptr) error { return recvDPDKSPSC(q, count, got) }
	case dpdkMPSC:
		q, err := dpdk.NewMPSCQueue[uintptr](w.Capacity)
		if err != nil {
			return Queue{}, constructorError(family, w, err)
		}
		queue.Value = q
		queue.Send = func(sender, count int) error { return sendDPDKMPSC(q, sender, count) }
		queue.Recv = func(count int, got *[]uintptr) error { return recvDPDKMPSC(q, count, got) }
	case dpdkMPMC:
		q, err := dpdk.NewMPMCQueue[uintptr](w.Capacity)
		if err != nil {
			return Queue{}, constructorError(family, w, err)
		}
		queue.Value = q
		queue.Send = func(sender, count int) error { return sendDPDKMPMC(q, sender, count) }
		queue.Recv = func(count int, got *[]uintptr) error { return recvDPDKMPMC(q, count, got) }
	case frontierMPSC:
		q, err := frontier.NewMPSCQueue[uintptr](w.Capacity)
		if err != nil {
			return Queue{}, constructorError(family, w, err)
		}
		queue.Value = q
		queue.Send = func(sender, count int) error { return sendFrontierMPSC(q, sender, count) }
		queue.Recv = func(count int, got *[]uintptr) error {
			return recvFrontierMPSC(q, count, got)
		}
	case frontierMPMC:
		q, err := frontier.NewMPMCQueue[uintptr](w.Capacity)
		if err != nil {
			return Queue{}, constructorError(family, w, err)
		}
		queue.Value = q
		queue.Send = func(sender, count int) error { return sendFrontierMPMC(q, sender, count) }
		queue.Recv = func(count int, got *[]uintptr) error {
			return recvFrontierMPMC(q, count, got)
		}
	}
	return queue, nil
}

func constructorError(family string, w Workload, err error) error {
	return fmt.Errorf("workloads: build %s %s: %w", family, w.Name, err)
}

// chooseQueue returns the queue type that family uses for topology.
func chooseQueue(family string, topology Topology) (queueType, error) {
	switch family {
	case "dpdk":
		return chooseDPDK(topology), nil
	case "frontier":
		return chooseFrontier(topology)
	case "dpdk-mpmc":
		return dpdkMPMC, nil
	case "frontier-mpmc":
		return frontierMPMC, nil
	}
	return 0, fmt.Errorf("workloads: unknown family %q", family)
}

// chooseDPDK returns the dpdk queue type specialized for topology.
func chooseDPDK(topology Topology) queueType {
	switch topology {
	case Seq, SPSC:
		return dpdkSPSC
	case MPSC:
		return dpdkMPSC
	default:
		return dpdkMPMC
	}
}

// chooseFrontier returns the frontier queue type specialized for topology. ring/frontier has no
// SPSC queue, so seq and spsc are unsupported.
func chooseFrontier(topology Topology) (queueType, error) {
	switch topology {
	case MPSC:
		return frontierMPSC, nil
	case MPMC:
		return frontierMPMC, nil
	default:
		return 0, fmt.Errorf("%w: frontier %v", ErrUnsupported, topology)
	}
}

// The hot loops below take each queue's concrete type, so every Enqueue and Dequeue is a direct
// call. A generic loop would call them through the generic dictionary, indirectly. Each loop
// yields after ErrFull or ErrEmpty and returns any other error.

func sendDPDKSPSC(q *dpdk.SPSCQueue[uintptr], sender, count int) error {
	for i := range count {
		v := message(sender, i)
		for {
			err := q.Enqueue(v)
			if err == nil {
				break
			}
			if !errors.Is(err, dpdk.ErrFull) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func recvDPDKSPSC(q *dpdk.SPSCQueue[uintptr], count int, got *[]uintptr) error {
	for range count {
		for {
			v, err := q.Dequeue()
			if err == nil {
				if recording {
					*got = append(*got, v)
				}
				break
			}
			if !errors.Is(err, dpdk.ErrEmpty) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func sendDPDKMPSC(q *dpdk.MPSCQueue[uintptr], sender, count int) error {
	for i := range count {
		v := message(sender, i)
		for {
			err := q.Enqueue(v)
			if err == nil {
				break
			}
			if !errors.Is(err, dpdk.ErrFull) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func recvDPDKMPSC(q *dpdk.MPSCQueue[uintptr], count int, got *[]uintptr) error {
	for range count {
		for {
			v, err := q.Dequeue()
			if err == nil {
				if recording {
					*got = append(*got, v)
				}
				break
			}
			if !errors.Is(err, dpdk.ErrEmpty) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func sendDPDKMPMC(q *dpdk.MPMCQueue[uintptr], sender, count int) error {
	for i := range count {
		v := message(sender, i)
		for {
			err := q.Enqueue(v)
			if err == nil {
				break
			}
			if !errors.Is(err, dpdk.ErrFull) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func recvDPDKMPMC(q *dpdk.MPMCQueue[uintptr], count int, got *[]uintptr) error {
	for range count {
		for {
			v, err := q.Dequeue()
			if err == nil {
				if recording {
					*got = append(*got, v)
				}
				break
			}
			if !errors.Is(err, dpdk.ErrEmpty) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func sendFrontierMPSC(q *frontier.MPSCQueue[uintptr], sender, count int) error {
	for i := range count {
		v := message(sender, i)
		for {
			err := q.Enqueue(v)
			if err == nil {
				break
			}
			if !errors.Is(err, frontier.ErrFull) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func recvFrontierMPSC(q *frontier.MPSCQueue[uintptr], count int, got *[]uintptr) error {
	for range count {
		for {
			v, err := q.Dequeue()
			if err == nil {
				if recording {
					*got = append(*got, v)
				}
				break
			}
			if !errors.Is(err, frontier.ErrEmpty) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func sendFrontierMPMC(q *frontier.MPMCQueue[uintptr], sender, count int) error {
	for i := range count {
		v := message(sender, i)
		for {
			err := q.Enqueue(v)
			if err == nil {
				break
			}
			if !errors.Is(err, frontier.ErrFull) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}

func recvFrontierMPMC(q *frontier.MPMCQueue[uintptr], count int, got *[]uintptr) error {
	for range count {
		for {
			v, err := q.Dequeue()
			if err == nil {
				if recording {
					*got = append(*got, v)
				}
				break
			}
			if !errors.Is(err, frontier.ErrEmpty) {
				return err
			}
			runtime.Gosched()
		}
	}
	return nil
}
