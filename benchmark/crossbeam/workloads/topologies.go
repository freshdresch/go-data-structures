package workloads

import "sync"

// runTopology does the work of Run, whose arguments it trusts.
func runTopology(
	topology Topology,
	send SendFunc,
	recv RecvFunc,
	messages int,
) ([][]uintptr, error) {
	switch topology {
	case Seq:
		return runSeq(send, recv, messages)
	case SPSC:
		return runConcurrent(send, recv, messages, 1, 1)
	case MPSC:
		return runConcurrent(send, recv, messages, Threads, 1)
	default:
		return runConcurrent(send, recv, messages, Threads, Threads)
	}
}

// runSeq sends every message and then receives them on the calling goroutine.
func runSeq(send SendFunc, recv RecvFunc, messages int) ([][]uintptr, error) {
	if err := send(0, messages); err != nil {
		return nil, err
	}
	got := make([][]uintptr, 1)
	if err := recv(messages, &got[0]); err != nil {
		return nil, err
	}
	return got, nil
}

// runConcurrent splits messages evenly over senders sender goroutines and receivers receivers.
// A single receiver runs on the calling goroutine, as upstream's does.
func runConcurrent(
	send SendFunc,
	recv RecvFunc,
	messages, senders, receivers int,
) ([][]uintptr, error) {
	// Each goroutine owns one slot, senders first, so the join needs no other synchronization.
	errs := make([]error, senders+receivers)
	got := make([][]uintptr, receivers)
	var wg sync.WaitGroup
	for k := range senders {
		wg.Go(func() { errs[k] = send(k, messages/senders) })
	}
	if receivers == 1 {
		errs[senders] = recv(messages, &got[0])
	} else {
		for k := range receivers {
			wg.Go(func() { errs[senders+k] = recv(messages/receivers, &got[k]) })
		}
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return got, nil
}
