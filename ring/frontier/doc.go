// Package frontier provides fixed-capacity FIFO queues whose concurrent callers never wait for one
// another to finish.
//
// Each caller on a concurrent side reserves an interval, copies, and records the interval's end.
// The other side advances a frontier through the contiguous run of finished intervals, and stops
// at the first one that has not finished. MPSCQueue applies this to its producers, SPMCQueue to
// its consumers, and MPMCQueue to both, where each frontier is a shared atomic that any caller on
// the side that reads it can advance.
//
// Compared with the queues in ring/dpdk, which are modeled on DPDK's rte_ring, finishing is
// decoupled from visibility. While an earlier caller on a concurrent side is unfinished, a returned
// enqueue on a concurrent producer side does not guarantee its value is visible yet, and a returned
// dequeue on a concurrent consumer side does not guarantee its slots are reusable yet. In exchange,
// no caller waits for earlier callers to publish, which makes single-element transfers much faster
// under contention.
//
// Count means something different for each queue. MPSCQueue counts values from the consumer's
// released position through the completed frontier, and keeps the furthest frontier any call has
// proven so repeated calls stay cheap. SPMCQueue and MPMCQueue count published values no consumer
// has reserved. MPMCQueue's Count and dequeue methods can advance the shared completed frontier,
// and its enqueue methods the shared released frontier, including zero-length and invalid-length
// calls.
//
// There is no SPSCQueue. With one caller on each side there is no wait to remove, so use
// dpdk.SPSCQueue. The errors are the ring package's sentinels, which ring/dpdk also returns, so
// errors.Is matches either package.
package frontier
