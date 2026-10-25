// Package ring provides four fixed-capacity FIFO queue topologies modeled on DPDK's rte_ring.
package ring

import (
	"runtime"
	"sync/atomic"
)

// cacheLine is the size each part of a queue is padded to. It is 128 bytes because that is a full
// cache line on Apple silicon, and on x86 it covers a 64-byte line plus the neighbor that an
// adjacent-line prefetcher may pull in.
const cacheLine = 128

// pack returns the relaxed index word that holds pos in its high 32 bits and cnt in its low 32.
// Every relaxed index is built with pack and read with unpackPos and unpackCnt, and all
// arithmetic happens on the 32-bit fields, so a counter that wraps never carries into the
// position.
func pack(pos, cnt uint32) uint64 {
	return uint64(pos)<<32 | uint64(cnt)
}

// unpackPos returns the position in a relaxed index word.
func unpackPos(word uint64) uint32 {
	return uint32(word >> 32)
}

// unpackCnt returns the counter in a relaxed index word.
func unpackCnt(word uint64) uint32 {
	return uint32(word)
}

// defaultSpinsBeforeYield is the bound a ring uses when WithSpinsBeforeYield is not given. Against
// 4096, 8192 was faster on an idle Apple M3 Pro and on a loaded AMD Ryzen 7 5800X, tied on an
// idle Ryzen, and 3% slower on a loaded M3 Pro, the smallest worst case of the longer bounds
// measured. scripts/ring-spin-bound.sh rewrites this line to measure other bounds, so it must
// stay a standalone untyped constant.
const defaultSpinsBeforeYield = 8192

// spinForTail checks the tail up to spins times, each with an atomic load, and reports whether
// one of them saw want. A true result synchronizes with the store that wrote want. It returns
// false when spins is zero or less. Each check is an atomic load, so the compiler cannot hoist
// it out of the loop.
func spinForTail(tail *atomic.Uint32, want uint32, spins int) bool {
	for range spins {
		if tail.Load() == want {
			return true
		}
	}
	return false
}

// waitForTail returns once the tail equals want. A producer or consumer that has finished copying
// calls it to wait for every earlier one on its side to publish, so publication stays in order.
// Whichever load sees want synchronizes with the earlier goroutine's tail store, so that
// goroutine's slot writes are published along with the caller's own tail store that follows.
//
// It first calls spinForTail with spins, since the earlier goroutine is usually nanoseconds from
// publishing, and then yields with runtime.Gosched between checks. This departs from DPDK, which
// spins without a bound because each lcore is a pinned thread that is not expected to be
// preempted. DPDK answers preemption on the tail update with its RTS and HTS modes, which still
// spin. Goroutines are preempted and share threads, so a waiter that never yielded could hold
// the thread the goroutine it waits for needs, and with GOMAXPROCS=1 stall until the runtime
// preempts it. The relaxed options are the DPDK-style answer for callers who expect heavy
// preemption. Gosched yields only to other goroutines and keeps the thread, so yielding is no
// cheaper than spinning when nothing else is ready to run.
func waitForTail(tail *atomic.Uint32, want uint32, spins int) {
	if spinForTail(tail, want, spins) {
		return
	}
	for tail.Load() != want {
		runtime.Gosched()
	}
}

// waitHeadTailDistance returns the head word once the side's head position is at most htdMax past
// its tail position, which bounds how many slots are reserved but not yet published. Without the
// bound, a steady stream of operations could keep one always in flight, and the tail would never
// move. It checks up to spins times, then yields between checks, as waitForTail does, with the
// same bound. The two phases keep a counter out of the yield loop, so no counter can wrap however
// long the wait lasts.
func waitHeadTailDistance(head, tail *atomic.Uint64, htdMax uint32, spins int) uint64 {
	for range spins {
		if word, ok := headWithinDistance(head, tail, htdMax); ok {
			return word
		}
	}
	for {
		if word, ok := headWithinDistance(head, tail, htdMax); ok {
			return word
		}
		runtime.Gosched()
	}
}

// headWithinDistance loads the side's tail, then its head, and returns the head word and whether
// its position is at most htdMax past the tail's.
//
// It loads the tail before the head, unlike DPDK, so the tail is never past the loaded head and
// the distance cannot wrap into a spurious wait. The head is still loaded before the caller reads
// the other side's tail.
func headWithinDistance(head, tail *atomic.Uint64, htdMax uint32) (uint64, bool) {
	tailPos := unpackPos(tail.Load())
	word := head.Load()
	return word, unpackPos(word)-tailPos <= htdMax
}

// nextTail returns the tail word after one more operation finishes, given a head word loaded
// after tail. The tail's counter counts finished operations. When it then equals the head's
// counter, every reservation made so far has finished, so the tail position moves to the head's.
func nextTail(tail, head uint64) uint64 {
	cnt := unpackCnt(tail) + 1
	pos := unpackPos(tail)
	if cnt == unpackCnt(head) {
		pos = unpackPos(head)
	}
	return pack(pos, cnt)
}

// publishRelaxed records that one operation on a relaxed side has finished, and reports whether
// that moved the tail position, in which case the caller raises the mirror.
func publishRelaxed(head, tail *atomic.Uint64) (moved bool) {
	for {
		// The tail is loaded before the head. Loaded the other way, a reservation made after the
		// head load could finish and be counted in the tail, and this call's count could then
		// match the stale head's, moving the tail past a reservation that has not finished.
		old := tail.Load()
		next := nextTail(old, head.Load())
		if tail.CompareAndSwap(old, next) {
			return unpackPos(next) != unpackPos(old)
		}
	}
}

// raiseMirror raises the 32-bit copy of a relaxed tail, which the other side reads, to the
// relaxed tail's position. It loads that position fresh on every attempt and never uses one the
// caller saw earlier, because a caller descheduled long enough could otherwise move the copy
// backward and let the other side read unpublished slots or overwrite live ones. A fresh position
// is at most the capacity ahead of the copy, so the signed comparison is sound. A compare-and-swap
// rather than a store keeps two raisers that finish out of order from moving it backward.
func raiseMirror(mirror *atomic.Uint32, tail *atomic.Uint64) {
	for {
		mirrorPos := mirror.Load()
		tailPos := unpackPos(tail.Load())
		if int32(tailPos-mirrorPos) <= 0 {
			return
		}
		if mirror.CompareAndSwap(mirrorPos, tailPos) {
			return
		}
	}
}
