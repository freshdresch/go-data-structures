// This generalized implementation is the benchmark and parity reference.
// It compiles only during tests.
package ring

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// admission is how a head move decides whether a request fits: by the cached copy of the other
// side's tail, or by a fresh load of it. The cache saves reading the other side's line, but a
// count computed from it can be stale. A batch call admits by a fresh load when it must return a
// fresh count, which is when it is zero-length or the ring was built with WithExactBatchCounts.
// Every other call admits by the cache. It is a named type so it cannot be swapped with the fixed
// bool beside it.
type admission uint8

const (
	admitByCache admission = iota
	admitByLoad
)

// readOnly holds the fields New sets and nothing writes afterward, so every core can keep the
// line holding them in its cache. On 64-bit platforms its fields take 52 bytes, which the slice
// header's alignment rounds up to 56. The allocator can place a ring 72 bytes past a cache line,
// where readOnly then ends exactly at the line's end, so it has room for at most 4 more one-byte
// fields before it crosses into the next line. TestRingAlignment checks this.
type readOnly[T any] struct {
	entries          []T
	mask             uint32
	maxEnqueueBatch  uint32
	maxDequeueBatch  uint32
	prodHTDMax       uint32
	consHTDMax       uint32
	spinsBeforeYield uint32
	prodMode         syncMode
	consMode         syncMode
	clearOnDequeue   bool
	exactBatchCounts bool
}

// prodHeadLine holds the producer side's heads and its cached copy of the consumer tail, and
// prodTailLine holds the producer side's tails. consHeadLine and consTailLine hold the consumer
// side's counterparts.
//
// The caches depart from DPDK, which loads the other side's tail on every head move. Here a head
// move first checks its request against the cached copy, and loads the real tail only when the
// cache says the request does not fit, or when the call admits by a fresh load. The cache only
// ever holds a value loaded from the tail it copies, and that tail only grows, so the cache can
// make a side see less room or data than there is, which costs a reload, but never more.
//
// That holds only while each cache is less than 2^32 - capacity positions behind the real tail it
// copies. A cache that lags further can wrap into the range the admission check accepts, and let
// a producer overwrite live slots or a consumer read unpublished ones. A single side keeps the
// invariant by construction, because its cache never lags by more than the capacity. A multi or
// relaxed side can store an older tail over a newer one, but breaking the invariant would need a
// goroutine stalled while about 4 billion elements pass. Reset moves every index back to 0, so it
// zeroes the caches too, and anything else that sets the indices must set the caches to match.
//
// A reload stores the loaded tail into the cache only when it differs from the cache value the
// call holds, compared by plain inequality, so a reload after the other side stood still writes
// nothing.
//
// Each side's head and its cache share a line, because that side writes both. Its tail has a
// line of its own, so a multi side's head CAS does not disturb the goroutines spinning on that
// side's tail in the publish wait. Every line is written by one side only, and the other side's
// head moves read a tail line only on a reload. This departs from DPDK, which keeps each side's
// head and tail together.
//
// Go's allocator puts an 8-byte header in front of a pointer-holding object this size, so a ring
// starts 8 or 72 bytes past a 128-byte boundary. Each group of fields still falls within one line
// of its own, and only the padding straddles lines. At an offset of 72, the read-only fields' line
// also holds the last bytes of the object before the ring, which the ring accepts because it never
// writes that line. None of the index lines share a line with another object. Moving the index
// lines to a separate pointer-free allocation avoided the offset but measured slower, from the
// extra pointer load and the second allocation's placement.
//
// A relaxed side uses the 64-bit indices, each a pos and cnt word built with pack. It also keeps
// its 32-bit tail as a copy of its 64-bit tail's position, raised by raiseMirror, so the other
// side reads the 32-bit tails in every mode. Go has no sound way to load half of a 64-bit atomic
// word, which is how DPDK lets the other side read the position directly. A relaxed side's
// 32-bit head stays unused.
//
// The 64-bit words come first, so the 32-bit fields pack after them without padding.
type prodHeadLine struct {
	prodHeadRTS          atomic.Uint64
	prodHead             atomic.Uint32
	cachedConsTail       atomic.Uint32
	singleCachedConsTail uint32
}

// prodTailLine holds the producer side's tails. The prodHeadLine comment covers it.
type prodTailLine struct {
	prodTailRTS atomic.Uint64
	prodTail    atomic.Uint32
}

// consHeadLine holds the consumer side's heads and its cached copy of the producer tail. The
// prodHeadLine comment covers it.
type consHeadLine struct {
	consHeadRTS          atomic.Uint64
	consHead             atomic.Uint32
	cachedProdTail       atomic.Uint32
	singleCachedProdTail uint32
}

// consTailLine holds the consumer side's tails. The prodHeadLine comment covers it.
type consTailLine struct {
	consTailRTS atomic.Uint64
	consTail    atomic.Uint32
}

// Ring is a fixed-size, lock-free ring buffer modeled on DPDK's rte_ring. It works as a FIFO queue
// anywhere between single-producer single-consumer and multi-producer multi-consumer.
//
// Without options, a ring allows one producer and one consumer at a time. WithMultiProdEnqueue
// makes the enqueue methods safe for concurrent producers, and WithMultiConsDequeue does the same
// for the dequeue methods and consumers. WithRelaxedMultiProdEnqueue and
// WithRelaxedMultiConsDequeue do the same with relaxed tail sync, so the goroutines on a side do
// not wait for each other to publish. WithMaxEnqueueBatch, WithMaxDequeueBatch, and WithMaxBatch
// limit how many elements one batch call moves. WithoutDequeueClearing leaves dequeued slots
// uncleared. WithExactBatchCounts makes the batch calls return exact counts. WithSpinsBeforeYield
// sets how long a waiting goroutine spins before it yields.
//
// The ring holds exactly count elements, where count is the count given to New.
//
// Each single side permits one operation at a time across all its methods, including failed
// and zero-length calls. Owners may change goroutines, but callers must synchronize every
// handoff with a happens-before edge, such as a mutex or channel. Opposite-side operations and
// Count, Free, Full, and Empty may run concurrently.
//
// A Ring must not be copied once in use.
type Ring[T any] struct {
	readOnly[T]
	// An array length must be a constant, so the padding is sized from readOnly[byte]. That holds
	// for every T because entries is always a slice header, so readOnly's size never depends on T.
	_ [cacheLine - unsafe.Sizeof(readOnly[byte]{})]byte
	prodHeadLine
	_ [cacheLine - unsafe.Sizeof(prodHeadLine{})]byte
	prodTailLine
	_ [cacheLine - unsafe.Sizeof(prodTailLine{})]byte
	consHeadLine
	_ [cacheLine - unsafe.Sizeof(consHeadLine{})]byte
	consTailLine
	_ [cacheLine - unsafe.Sizeof(consTailLine{})]byte
}

// A layout change that breaks the Ring's size or any index line's offset fails the build here. The
// [64]byte instance catches a field whose size depends on T, which the byte instance would miss.
var (
	_ [unsafe.Sizeof(Ring[byte]{}) - 5*cacheLine]struct{}
	_ [5*cacheLine - unsafe.Sizeof(Ring[byte]{})]struct{}
	_ [unsafe.Offsetof(Ring[byte]{}.prodHeadLine) - cacheLine]struct{}
	_ [cacheLine - unsafe.Offsetof(Ring[byte]{}.prodHeadLine)]struct{}
	_ [unsafe.Offsetof(Ring[byte]{}.prodTailLine) - 2*cacheLine]struct{}
	_ [2*cacheLine - unsafe.Offsetof(Ring[byte]{}.prodTailLine)]struct{}
	_ [unsafe.Offsetof(Ring[byte]{}.consHeadLine) - 3*cacheLine]struct{}
	_ [3*cacheLine - unsafe.Offsetof(Ring[byte]{}.consHeadLine)]struct{}
	_ [unsafe.Offsetof(Ring[byte]{}.consTailLine) - 4*cacheLine]struct{}
	_ [4*cacheLine - unsafe.Offsetof(Ring[byte]{}.consTailLine)]struct{}
	_ [unsafe.Sizeof(Ring[[64]byte]{}) - 5*cacheLine]struct{}
	_ [5*cacheLine - unsafe.Sizeof(Ring[[64]byte]{})]struct{}
	_ [unsafe.Offsetof(Ring[[64]byte]{}.prodHeadLine) - cacheLine]struct{}
	_ [cacheLine - unsafe.Offsetof(Ring[[64]byte]{}.prodHeadLine)]struct{}
	_ [unsafe.Offsetof(Ring[[64]byte]{}.prodTailLine) - 2*cacheLine]struct{}
	_ [2*cacheLine - unsafe.Offsetof(Ring[[64]byte]{}.prodTailLine)]struct{}
	_ [unsafe.Offsetof(Ring[[64]byte]{}.consHeadLine) - 3*cacheLine]struct{}
	_ [3*cacheLine - unsafe.Offsetof(Ring[[64]byte]{}.consHeadLine)]struct{}
	_ [unsafe.Offsetof(Ring[[64]byte]{}.consTailLine) - 4*cacheLine]struct{}
	_ [4*cacheLine - unsafe.Offsetof(Ring[[64]byte]{}.consTailLine)]struct{}
)

// New returns an empty Ring with count slots. A ring of count n holds n elements. Without options,
// the ring allows one producer and one consumer at a time, and a batch call may move up to the
// whole capacity. It returns an error wrapping ErrInvalidCount when count is not a power of two
// between 1 and 2^30, and one wrapping ErrInvalidMaxBatch when a maximum enqueue or dequeue batch
// is below 1 or above count. It returns one wrapping ErrInvalidSpinsBeforeYield when the bound set
// with WithSpinsBeforeYield is below 0 or above math.MaxInt32. Options apply in order and only
// final values are validated. Each RTS side defaults to count / 8 element positions. An explicit
// distance requires that side's relaxed selector or wraps ErrHeadTailDistanceRequiresRelaxed.
// Numeric distance errors wrap ErrInvalidMaxHeadTailDistance.
// Each single side permits one operation at a time across all its methods, including failed
// and zero-length calls. Owners may change goroutines, but callers must synchronize every
// handoff with a happens-before edge, such as a mutex or channel. Opposite-side operations and
// Count, Free, Full, and Empty may run concurrently.
func New[T any](count int, options ...Option) (*Ring[T], error) {
	cfg, err := resolveConfig("New", count, modeSingle, modeSingle, false, options)
	if err != nil {
		return nil, err
	}

	ring := &Ring[T]{
		readOnly: readOnly[T]{
			entries:         make([]T, count),
			mask:            uint32(count - 1),
			maxEnqueueBatch: uint32(cfg.maxEnqueueBatch),
			maxDequeueBatch: uint32(cfg.maxDequeueBatch),
			prodHTDMax: resolveHeadTailDistance(
				cfg.prodMode,
				count,
				cfg.maxProducerHeadTailDistance,
				cfg.producerHeadTailDistanceSet,
			),
			consHTDMax: resolveHeadTailDistance(
				cfg.consMode,
				count,
				cfg.maxConsumerHeadTailDistance,
				cfg.consumerHeadTailDistanceSet,
			),
			spinsBeforeYield: uint32(cfg.spinsBeforeYield),
			prodMode:         cfg.prodMode,
			consMode:         cfg.consMode,
			clearOnDequeue:   !cfg.keepDequeued,
			exactBatchCounts: cfg.exactBatchCounts,
		},
	}
	return ring, nil
}

// Capacity returns the number of elements the ring holds, which is the count given to New.
func (r *Ring[T]) Capacity() int {
	return int(r.capacity())
}

// MaxEnqueueBatch returns the most elements one enqueue batch call moves: the limit set by
// WithMaxEnqueueBatch or WithMaxBatch, or the capacity without one.
func (r *Ring[T]) MaxEnqueueBatch() int {
	return int(r.maxEnqueueBatch)
}

// MaxDequeueBatch returns the most elements one dequeue batch call moves: the limit set by
// WithMaxDequeueBatch or WithMaxBatch, or the capacity without one.
func (r *Ring[T]) MaxDequeueBatch() int {
	return int(r.maxDequeueBatch)
}

// capacity returns the number of elements the ring holds as a uint32, for the index arithmetic.
// The count is a power of two, so it is one more than the mask.
func (r *Ring[T]) capacity() uint32 {
	return r.mask + 1
}

// Reset empties the ring and clears every slot.
//
// WARNING: Make sure to only do this while the ring is not in use!
//
// Callers must give Reset exclusive access, synchronize completion of all prior use, and
// synchronize publication of the reset ring to future users.
//
// Reset does not change how the ring was configured. Elements that were enqueued but never
// dequeued are released. It clears every slot even on a ring created with WithoutDequeueClearing.
// It also zeroes each side's cached copy of the other side's tail, because a cache left at its old
// value would let a dequeue after Reset return cleared slots.
func (r *Ring[T]) Reset() {
	clear(r.entries)
	r.prodHead.Store(0)
	r.prodTail.Store(0)
	r.consHead.Store(0)
	r.consTail.Store(0)
	r.prodHeadRTS.Store(0)
	r.prodTailRTS.Store(0)
	r.consHeadRTS.Store(0)
	r.consTailRTS.Store(0)
	r.cachedConsTail.Store(0)
	r.cachedProdTail.Store(0)
	r.singleCachedConsTail = 0
	r.singleCachedProdTail = 0
}

// Count returns the number of occupied entries in the ring.
//
// Unlike DPDK's rte_ring_count, it does not mask the difference of the tails. The ring holds
// exactly Capacity elements, so a full ring's difference equals the capacity, and masking it would
// report a full ring as empty.
//
// It loads the consumer tail before the producer tail. In every mode, the 32-bit consumer tail,
// which trails the 64-bit one on a relaxed side, is never past the consumer head, which is never
// past the 32-bit producer tail, and both tails only grow. So the producer tail loaded second is
// never behind the consumer tail loaded first, and the difference cannot underflow. Loading them
// in the other order lets consumers pass the producer tail that was read, and the underflow
// would report a nearly empty ring as full.
//
// Under concurrency the result is a snapshot. Dequeues that complete between the two loads, with
// the enqueues that refill the ring, can make it report more entries than the ring holds. It never
// exceeds Capacity. With relaxed consumers it can also count elements that a dequeue has already
// returned, until the consumer tail moves past them. With relaxed producers it can miss elements
// whose enqueue has already returned, until the producer tail moves past them.
func (r *Ring[T]) Count() int {
	consTail := r.consTail.Load()
	prodTail := r.prodTail.Load()
	return int(min(prodTail-consTail, r.capacity()))
}

// Free returns the number of empty slots in the ring. It derives from Count, so under concurrency
// it can report the ring as fuller than it is.
func (r *Ring[T]) Free() int {
	return r.Capacity() - r.Count()
}

// Full reports whether the ring is full. It derives from Count, so under concurrency it can report
// the ring as fuller than it is.
func (r *Ring[T]) Full() bool {
	return r.Free() == 0
}

// Empty reports whether the ring is empty, which is whether Count reports 0.
func (r *Ring[T]) Empty() bool {
	return r.Count() == 0
}

// Enqueue places item in the ring. It returns ErrFull, unwrapped, when the ring has no free slot.
//
// Enqueue is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue or WithRelaxedMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) Enqueue(item T) error {
	items := [1]T{item}
	enqueued, _ := r.doEnqueue(items[:], true, admitByCache)
	if enqueued == 0 {
		return ErrFull
	}
	return nil
}

// EnqueueBulk places all of items in the ring, or none of them. It returns nil when all are
// enqueued. A zero-length items succeeds and enqueues nothing.
//
// It checks, in order, that len(items) is at most Capacity, that it is at most MaxEnqueueBatch,
// and that the ring has len(items) free slots. The first failing check decides the error. These
// return an error wrapping ErrExceedsCapacity, an error wrapping ErrExceedsMaxBatch, and ErrFull
// unwrapped. A failed call leaves the ring unchanged.
//
// free is returned on every path, errors included. It is the number of free slots after the call,
// between 0 and Capacity inclusive. By default it can be stale when the call succeeds. With a
// single producer side it is never more than the real value, and with a multi or relaxed one it
// is a snapshot. Do not use it to block, to decide wakeups, or to size the next request.
// WithExactBatchCounts explains this in full.
//
// EnqueueBulk is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue or WithRelaxedMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBulk(items []T) (free int, err error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path. It is in int, so a length beyond the uint32 range cannot truncate.
	if len(items) > int(r.maxEnqueueBatch) {
		return int(r.freeEntries()), r.bulkLengthError("enqueue", uint64(len(items)), r.maxEnqueueBatch)
	}

	enqueued, freeAfter := r.doEnqueue(items, true, r.batchAdmission(len(items)))
	if int(enqueued) != len(items) {
		return int(freeAfter), ErrFull
	}
	return int(freeAfter), nil
}

// EnqueueBurst places as many elements from the front of items in the ring as it has free slots
// for, and never more than MaxEnqueueBatch. It returns the number enqueued, between 0 and
// len(items) inclusive. A zero-length items enqueues nothing.
//
// free is returned on every path. It is the number of free slots after the call, between 0 and
// Capacity inclusive. By default it can be stale when the call enqueued all of its request after
// the MaxEnqueueBatch clamp. With a single producer side it is never more than the real value, and
// with a multi or relaxed one it is a snapshot. Do not use it to block, to decide wakeups, or to
// size the next request. WithExactBatchCounts explains this in full.
//
// EnqueueBurst is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue or WithRelaxedMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBurst(items []T) (enqueued, free int) {
	// Clamping in int keeps a slice longer than 2^32 from truncating when its length becomes a
	// uint32 in doEnqueue. The limit is at most the capacity, so it bounds both.
	clamped := min(len(items), int(r.maxEnqueueBatch))
	moved, freeAfter := r.doEnqueue(items[:clamped], false, r.batchAdmission(clamped))
	return int(moved), int(freeAfter)
}

// Dequeue removes and returns the oldest element in the ring. It returns the zero value and
// ErrEmpty, unwrapped, when the ring is empty.
//
// Dequeue is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) Dequeue() (T, error) {
	var dst [1]T
	dequeued, _ := r.doDequeue(dst[:], true, admitByCache)
	if dequeued == 0 {
		var zero T
		return zero, ErrEmpty
	}
	return dst[0], nil
}

// DequeueBulk removes the len(dst) oldest elements in the ring and copies them into dst, oldest
// first, or removes none of them. It returns nil when all are dequeued. A zero-length dst succeeds
// and dequeues nothing.
//
// It checks, in order, that len(dst) is at most Capacity, that it is at most MaxDequeueBatch, and
// that the ring has len(dst) occupied entries. The first failing check decides the error. These
// return an error wrapping ErrExceedsCapacity, an error wrapping ErrExceedsMaxBatch, and ErrEmpty
// unwrapped. A failed call leaves the ring and all of dst unchanged.
//
// available is returned on every path, errors included. It is the number of occupied entries after
// the call, between 0 and Capacity inclusive. By default it can be stale when the call succeeds.
// With a single consumer side it is never more than the real value, and with a multi or relaxed
// one it is a snapshot. Do not use it to block, to decide wakeups, or to size the next request.
// WithExactBatchCounts explains this in full.
//
// DequeueBulk is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBulk(dst []T) (available int, err error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path. It is in int, so a length beyond the uint32 range cannot truncate.
	if len(dst) > int(r.maxDequeueBatch) {
		return int(r.occupiedEntries()), r.bulkLengthError("dequeue", uint64(len(dst)), r.maxDequeueBatch)
	}

	dequeued, availableAfter := r.doDequeue(dst, true, r.batchAdmission(len(dst)))
	if int(dequeued) != len(dst) {
		return int(availableAfter), ErrEmpty
	}
	return int(availableAfter), nil
}

// DequeueBurst removes up to len(dst) of the oldest elements, as many as the ring holds, and never
// more than MaxDequeueBatch, and copies them into the front of dst, oldest first. It returns the
// number dequeued, between 0 and len(dst) inclusive. It writes only dst[:dequeued] and leaves the
// rest of dst unchanged. A zero-length dst dequeues nothing.
//
// available is returned on every path. It is the number of occupied entries after the call,
// between 0 and Capacity inclusive. By default it can be stale when the call dequeued all of its
// request after the MaxDequeueBatch clamp. With a single consumer side it is never more than the
// real value, and with a multi or relaxed one it is a snapshot. Do not use it to block, to decide
// wakeups, or to size the next request. WithExactBatchCounts explains this in full.
//
// DequeueBurst is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBurst(dst []T) (dequeued, available int) {
	// Clamping in int keeps a slice longer than 2^32 from truncating when its length becomes a
	// uint32 in doDequeue. The limit is at most the capacity, so it bounds both.
	clamped := min(len(dst), int(r.maxDequeueBatch))
	moved, availableAfter := r.doDequeue(dst[:clamped], false, r.batchAdmission(clamped))
	return int(moved), int(availableAfter)
}

// bulkLengthError returns the error for a bulk call that asked to move length elements, more than
// maxBatch, its direction's limit. It wraps ErrExceedsCapacity when length also exceeds the
// capacity, and ErrExceedsMaxBatch otherwise. op names the call, enqueue or dequeue, in the
// message. Callers check the limit first, so this runs only on the error path. length is a uint64
// so that neither a slice length nor a uint32 count can wrap negative on a 32-bit target.
func (r *Ring[T]) bulkLengthError(op string, length uint64, maxBatch uint32) error {
	capacity := uint64(r.capacity())
	if length > capacity {
		return fmt.Errorf("%s %d elements, capacity %d: %w", op, length, capacity, ErrExceedsCapacity)
	}
	return fmt.Errorf(
		"%s %d elements, max %s batch %d: %w",
		op, length, op, maxBatch, ErrExceedsMaxBatch,
	)
}

// batchAdmission returns how a batch call of n elements admits: by a fresh load when the ring was
// created with WithExactBatchCounts or n is 0, and by the cache otherwise. A zero-length request
// always fits the cache, so admitting it by the cache would return the same stale count forever.
func (r *Ring[T]) batchAdmission(n int) admission {
	if r.exactBatchCounts || n == 0 {
		return admitByLoad
	}
	return admitByCache
}

// doEnqueue places elements from the front of items in the ring. fixed is true for a bulk
// enqueue, which enqueues all of items or nothing. admit is how the head move decides whether
// the request fits. It returns the number enqueued and the number of free slots after the call,
// which is at most the capacity.
//
// Callers pass a slice whose length is already at most the capacity, so converting len(items) to
// uint32 cannot truncate.
func (r *Ring[T]) doEnqueue(items []T, fixed bool, admit admission) (enqueued, free uint32) {
	if r.prodMode == modeRelaxed {
		return r.doEnqueueRelaxed(items, fixed, admit)
	}

	var (
		head uint32
		next uint32
	)

	numEntries, freeBefore := r.moveProdHead(uint32(len(items)), &head, &next, fixed, admit)
	if numEntries == 0 {
		// Without a reservation the producer head moveProdHead read may be stale, so the count
		// can overshoot the capacity. With a reservation the head was current, because the single
		// producer owns the head or the multi-producer CAS succeeded on it. The consumer tail the
		// count came from, cached or loaded, is never ahead of the real tail, the real tail is
		// never ahead of the head, and the cache invariant keeps a lagging cache out of the range
		// the admission check accepts, so capacity + tail - head is already between 0 and the
		// capacity.
		return 0, min(freeBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.writeSlots(base, end, items[:numEntries])

	if r.prodMode == modeMulti {
		waitForTail(&r.prodTail, head, int(r.spinsBeforeYield))
	}

	// the tail store publishes the slots written above to consumers
	r.prodTail.Store(next)

	return numEntries, freeBefore - numEntries
}

// doEnqueueRelaxed is doEnqueue for a relaxed producer side. It publishes without waiting for the
// producers that reserved before it.
func (r *Ring[T]) doEnqueueRelaxed(
	items []T,
	fixed bool,
	admit admission,
) (enqueued, free uint32) {
	head, numEntries, freeBefore := r.moveProdHeadRelaxed(uint32(len(items)), fixed, admit)
	if numEntries == 0 {
		// As in doEnqueue, only a reservation guarantees the head was current.
		return 0, min(freeBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.writeSlots(base, end, items[:numEntries])

	if publishRelaxed(&r.prodHeadRTS, &r.prodTailRTS) {
		raiseMirror(&r.prodTail, &r.prodTailRTS)
	}
	return numEntries, freeBefore - numEntries
}

// doDequeue removes elements from the ring and copies them into the front of dst, oldest first.
// fixed is true for a bulk dequeue, which fills all of dst or nothing. admit is how the head move
// decides whether the request fits. It returns the number dequeued and the number of occupied
// entries after the call, which is at most the capacity. It writes only dst[:dequeued] and never
// allocates.
//
// Callers pass a slice whose length is already at most the capacity, so converting len(dst) to
// uint32 cannot truncate.
func (r *Ring[T]) doDequeue(dst []T, fixed bool, admit admission) (dequeued, available uint32) {
	if r.consMode == modeRelaxed {
		return r.doDequeueRelaxed(dst, fixed, admit)
	}

	var (
		head uint32
		next uint32
	)

	numEntries, availableBefore := r.moveConsHead(uint32(len(dst)), &head, &next, fixed, admit)
	if numEntries == 0 {
		// Without a reservation the consumer head moveConsHead read may be stale, so the count
		// can overshoot the capacity. With a reservation the head was current, because the single
		// consumer owns the head or the multi-consumer CAS succeeded on it. The producer tail the
		// count came from, cached or loaded, is never ahead of the real tail, the real tail is
		// never more than the capacity past the head, and the cache invariant keeps a lagging
		// cache out of the range the admission check accepts, so tail - head is already between 0
		// and the capacity.
		return 0, min(availableBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.readSlots(base, end, dst[:numEntries])

	if r.clearOnDequeue {
		r.clearSlots(base, end, numEntries)
	}

	if r.consMode == modeMulti {
		waitForTail(&r.consTail, head, int(r.spinsBeforeYield))
	}

	// the tail store hands the slots read above back to producers
	r.consTail.Store(next)

	return numEntries, availableBefore - numEntries
}

// doDequeueRelaxed is doDequeue for a relaxed consumer side. It releases its slots without waiting
// for the consumers that reserved before it.
func (r *Ring[T]) doDequeueRelaxed(
	dst []T,
	fixed bool,
	admit admission,
) (dequeued, available uint32) {
	head, numEntries, availableBefore := r.moveConsHeadRelaxed(uint32(len(dst)), fixed, admit)
	if numEntries == 0 {
		// As in doDequeue, only a reservation guarantees the head was current.
		return 0, min(availableBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.readSlots(base, end, dst[:numEntries])

	if r.clearOnDequeue {
		r.clearSlots(base, end, numEntries)
	}

	if publishRelaxed(&r.consHeadRTS, &r.consTailRTS) {
		raiseMirror(&r.consTail, &r.consTailRTS)
	}
	return numEntries, availableBefore - numEntries
}

// slotRange returns where in the backing array the count slots starting at index head begin, and
// how many of them come before the end of the array.
func (r *Ring[T]) slotRange(head, count uint32) (base, end uint32) {
	base = head & r.mask
	return base, min(r.capacity()-base, count)
}

// writeSlots copies items into the slots slotRange described with base and end.
func (r *Ring[T]) writeSlots(base, end uint32, items []T) {
	if len(items) == 1 {
		// avoid the memmove when we can, since it costs more than an assignment for one element
		r.entries[base] = items[0]
		return
	}
	copy(r.entries[base:base+end], items[:end])
	copy(r.entries[:uint32(len(items))-end], items[end:])
}

// readSlots copies the slots slotRange described with base and end into dst.
func (r *Ring[T]) readSlots(base, end uint32, dst []T) {
	if len(dst) == 1 {
		// avoid the memmove when we can, since it costs more than an assignment for one element
		dst[0] = r.entries[base]
		return
	}
	copy(dst[:end], r.entries[base:base+end])
	copy(dst[end:], r.entries[:uint32(len(dst))-end])
}

// clearSlots zeroes the count slots a dequeue read, which slotRange described with base and end.
// A slot that still holds a pointer keeps its target alive until a later enqueue overwrites it,
// so clearing it lets the garbage collector release the target. The caller must call it before
// its consumer tail store, so a producer never sees a slot as free while it is being cleared.
func (r *Ring[T]) clearSlots(base, end, count uint32) {
	if count == 1 {
		var zero T
		r.entries[base] = zero
		return
	}
	clear(r.entries[base : base+end])
	clear(r.entries[:count-end])
}

// moveProdHeadRelaxed reserves up to n slots for a relaxed enqueue, as moveProdHead does for the
// multi mode, after waitHeadTailDistance allows it. The head's counter counts reservations. It
// returns the position the reservation starts at, the number of slots reserved, between 0 and n
// inclusive and 0 or n when fixed, and the number of free slots before the reservation, from the
// cache or a fresh load as admit and admissionConsTail decide.
func (r *Ring[T]) moveProdHeadRelaxed(
	n uint32,
	fixed bool,
	admit admission,
) (oldPos, count, free uint32) {
	capacity := r.capacity()
	for {
		head := waitHeadTailDistance(
			&r.prodHeadRTS,
			&r.prodTailRTS,
			r.prodHTDMax,
			int(r.spinsBeforeYield),
		)
		oldPos = unpackPos(head)
		free = capacity + r.admissionConsTail(oldPos, n, admit) - oldPos
		count = n
		if count > free {
			if fixed {
				return oldPos, 0, free
			}
			count = free
		}
		if count == 0 {
			return oldPos, 0, free
		}
		next := pack(oldPos+count, unpackCnt(head)+1)
		if r.prodHeadRTS.CompareAndSwap(head, next) {
			return oldPos, count, free
		}
	}
}

// moveConsHeadRelaxed reserves up to n slots for a relaxed dequeue, as moveConsHead does for the
// multi mode, after waitHeadTailDistance allows it. The head's counter counts reservations. It
// returns the position the reservation starts at, the number of slots reserved, between 0 and n
// inclusive and 0 or n when fixed, and the number of occupied entries before the reservation, from
// the cache or a fresh load as admit and admissionProdTail decide.
func (r *Ring[T]) moveConsHeadRelaxed(
	n uint32,
	fixed bool,
	admit admission,
) (oldPos, count, available uint32) {
	for {
		head := waitHeadTailDistance(
			&r.consHeadRTS,
			&r.consTailRTS,
			r.consHTDMax,
			int(r.spinsBeforeYield),
		)
		oldPos = unpackPos(head)
		available = r.admissionProdTail(oldPos, n, admit) - oldPos
		count = n
		if count > available {
			if fixed {
				return oldPos, 0, available
			}
			count = available
		}
		if count == 0 {
			return oldPos, 0, available
		}
		next := pack(oldPos+count, unpackCnt(head)+1)
		if r.consHeadRTS.CompareAndSwap(head, next) {
			return oldPos, count, available
		}
	}
}

// moveProdHead moves the ring's producer head for an enqueue operation.
//
// numItems is the number of items to enqueue, which is how far the head should move. oldHead
// receives the producer head from before the move, where the enqueue starts, and newHead receives
// it from after the move, where the enqueue ends. fixed is true for a bulk enqueue, which enqueues
// the entire batch or nothing. admit is how it decides whether the request fits.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed enqueue. It also returns the number of free entries in the ring before the
// head was moved, computed from the consumer tail admissionConsTail returned, which is either the
// cached copy or a fresh load.
func (r *Ring[T]) moveProdHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
	admit admission,
) (uint32, uint32) {
	capacity := r.capacity()
	maxItems := numItems

	for {
		head := r.prodHead.Load()
		tail := r.admissionConsTail(head, maxItems, admit)

		freeEntries := capacity + tail - head
		numItems = maxItems
		if numItems > freeEntries {
			if fixed {
				return 0, freeEntries
			}
			numItems = freeEntries
		}
		if numItems == 0 {
			return 0, freeEntries
		}

		next := head + numItems

		if r.prodMode == modeMulti {
			if r.prodHead.CompareAndSwap(head, next) {
				*oldHead = head
				*newHead = next
				return numItems, freeEntries
			}
			continue
		}

		r.prodHead.Store(next)
		*oldHead = head
		*newHead = next
		return numItems, freeEntries
	}
}

// moveConsHead moves the ring's consumer head for a dequeue operation.
//
// numItems is the number of items to dequeue, which is how far the head should move. oldHead
// receives the consumer head from before the move, where the dequeue starts, and newHead receives
// it from after the move, where the dequeue ends. fixed is true for a bulk dequeue, which dequeues
// the entire batch or nothing. admit is how it decides whether the request fits.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed dequeue. It also returns the number of occupied entries in the ring before
// the head was moved, computed from the producer tail admissionProdTail returned, which is either
// the cached copy or a fresh load.
func (r *Ring[T]) moveConsHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
	admit admission,
) (uint32, uint32) {
	var (
		tail   uint32
		head   uint32
		filled uint32
	)

	maxItems := numItems

	for {
		head = r.consHead.Load()
		tail = r.admissionProdTail(head, maxItems, admit)

		filled = tail - head
		numItems = maxItems
		if numItems > filled {
			if fixed {
				return 0, filled
			}
			numItems = filled
		}
		if numItems == 0 {
			return 0, filled
		}

		next := head + numItems

		if r.consMode == modeMulti {
			if r.consHead.CompareAndSwap(head, next) {
				*oldHead = head
				*newHead = next
				return numItems, filled
			}
			continue
		}

		r.consHead.Store(next)
		*oldHead = head
		*newHead = next
		return numItems, filled
	}
}

// admissionConsTail returns the consumer tail an enqueue of n slots from the producer position
// head decides by. With admitByCache, when the cached copy alone shows room for all n, it returns
// the cache without reading the consumer's line. Otherwise it loads the real consumer tail, stores
// it into the cache if it differs from the cache value loaded here, and returns it.
//
// The check used <= capacity - n also rejects used > capacity, where capacity - used would wrap
// to a huge free count. A multi or relaxed side reaches that when another producer stored the
// cache backward, or when the head loaded here is older than the cache, and then it reloads. n is
// at least 1 with admitByCache, because batchAdmission sends a zero-length call to the load.
func (r *Ring[T]) admissionConsTail(head, n uint32, admit admission) uint32 {
	cached := r.cachedConsTail.Load()
	if admit == admitByCache {
		used := head - cached
		if used <= r.capacity()-n {
			return cached
		}
	}
	tail := r.consTail.Load()
	if tail != cached {
		r.cachedConsTail.Store(tail)
	}
	return tail
}

// admissionProdTail returns the producer tail a dequeue of n entries from the consumer position
// head decides by. With admitByCache, when the cached copy alone shows n entries, it returns the
// cache without reading the producer's line. Otherwise it loads the real producer tail, stores it
// into the cache if it differs from the cache value loaded here, and returns it.
//
// The check avail - n <= capacity - n rejects both avail < n, where avail - n wraps, and
// avail > capacity. A multi or relaxed side reaches either when another consumer stored the cache
// backward, or when the head loaded here is older than the cache, and then it reloads. n is at
// least 1 with admitByCache, because batchAdmission sends a zero-length call to the load.
func (r *Ring[T]) admissionProdTail(head, n uint32, admit admission) uint32 {
	cached := r.cachedProdTail.Load()
	if admit == admitByCache {
		avail := cached - head
		if avail-n <= r.capacity()-n {
			return cached
		}
	}
	tail := r.prodTail.Load()
	if tail != cached {
		r.cachedProdTail.Store(tail)
	}
	return tail
}

// freeEntries returns the number of slots a producer could reserve right now. It loads the
// producer head before the consumer tail, as moveProdHead does. It clamps the result to the
// capacity, because a consumer tail that advances after the head was loaded can make the value
// overshoot.
func (r *Ring[T]) freeEntries() uint32 {
	head := r.prodHeadPos()
	capacity := r.capacity()
	free := capacity + r.consTail.Load() - head
	return min(free, capacity)
}

// occupiedEntries returns the number of entries a consumer could reserve right now. It loads the
// consumer head before the producer tail, as moveConsHead does. It clamps the result to the
// capacity, because a producer tail that advances after the head was loaded can make the value
// overshoot.
func (r *Ring[T]) occupiedEntries() uint32 {
	head := r.consHeadPos()
	occupied := r.prodTail.Load() - head
	return min(occupied, r.capacity())
}

// prodHeadPos returns the producer head's position from whichever index the producer side's
// mode uses.
func (r *Ring[T]) prodHeadPos() uint32 {
	if r.prodMode == modeRelaxed {
		return unpackPos(r.prodHeadRTS.Load())
	}
	return r.prodHead.Load()
}

// consHeadPos returns the consumer head's position from whichever index the consumer side's
// mode uses.
func (r *Ring[T]) consHeadPos() uint32 {
	if r.consMode == modeRelaxed {
		return unpackPos(r.consHeadRTS.Load())
	}
	return r.consHead.Load()
}
