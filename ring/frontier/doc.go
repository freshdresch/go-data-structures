// Package frontier provides fixed-capacity FIFO queues whose concurrent callers never wait for one
// another to finish.
//
// Each caller on a concurrent side reserves an interval, copies, and records the interval's end.
// The other side advances a frontier through the contiguous run of finished intervals, and stops
// at the first one that has not finished. MPSCQueue applies this to its producers and SPMCQueue to
// its consumers.
//
// Compared with the queues in ring/dpdk, which are modeled on DPDK's rte_ring, finishing is
// decoupled from visibility. A returned MPSCQueue enqueue does not guarantee its value is visible
// yet, and a returned SPMCQueue dequeue does not guarantee its slots are reusable yet, while an
// earlier caller on that side is unfinished. In exchange, no caller waits for earlier callers to
// publish, which makes single-element transfers much faster under contention.
//
// Count means something different for each queue. MPSCQueue counts values from the consumer's
// released position through the completed frontier, and keeps the furthest frontier any call has
// proven so repeated calls stay cheap. SPMCQueue counts published values no consumer has reserved.
//
// There is no SPSCQueue. With one caller on each side there is no wait to remove, so use
// dpdk.SPSCQueue. The errors are the ring package's sentinels, which ring/dpdk also returns, so
// errors.Is matches either package.
package frontier
