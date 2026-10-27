// Package frontier provides fixed-capacity FIFO queues whose producers publish completed ranges
// independently, so no producer waits for another to finish.
//
// Each producer reserves an interval, copies into it, and records the interval's end. The
// consumer advances a frontier through the contiguous run of completed intervals, and sees every
// value up to the first reservation that has not finished. Count keeps the furthest frontier any
// call has proven, so repeated calls on a stable backlog stay cheap.
//
// Compared with the queues in package ring, which are modeled on DPDK's rte_ring, a returned
// enqueue does not guarantee its value is visible yet: a producer stalled mid-enqueue holds back
// the values reserved after it. In exchange, producers skip the wait for earlier producers to
// publish, which makes single-element transfers several times faster under producer contention.
//
// The errors are the ring package's sentinels, which its queues also return, so errors.Is matches
// the same sentinels whichever package returned them.
package frontier
