// Package ring provides a fixed-size, lock-free ring buffer modeled on DPDK's rte_ring.
package ring

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// ErrInvalidCount is returned by New, wrapped, when the count is not a power of two, is less
// than 1, or exceeds 2^30.
var ErrInvalidCount = errors.New("ring: count must be a power of two between 1 and 2^30")

// ErrFull is returned unwrapped by Enqueue and EnqueueBulk when the ring does not have enough
// free slots for the request. The caller may retry.
var ErrFull = errors.New("ring: not enough free slots")

// ErrEmpty is returned unwrapped by Dequeue and DequeueBulk when the ring does not have enough
// occupied entries for the request. The caller may retry.
var ErrEmpty = errors.New("ring: not enough occupied entries")

// ErrExceedsCapacity is returned wrapped by EnqueueBulk when len(items) exceeds the ring's
// capacity, and by DequeueBulk when len(dst) does. Such a request can never succeed, so retrying
// it is pointless.
var ErrExceedsCapacity = errors.New("ring: requested count exceeds the ring's capacity")

// ErrInvalidMaxBatch is returned by New, wrapped, when a maximum enqueue or dequeue batch size,
// set with WithMaxBatch, WithMaxEnqueueBatch, or WithMaxDequeueBatch, is below 1 or above the
// count.
var ErrInvalidMaxBatch = errors.New("ring: max batch must be between 1 and the ring's count")

// ErrExceedsMaxBatch is returned wrapped by EnqueueBulk when len(items) exceeds the ring's maximum
// enqueue batch size, and by DequeueBulk when len(dst) exceeds its maximum dequeue batch size,
// while still fitting the capacity. Such a request can never succeed, so retrying it is
// pointless.
var ErrExceedsMaxBatch = errors.New("ring: requested count exceeds the ring's maximum batch size")

// cacheLine is the size each part of a Ring is padded to. It is 128 bytes because that is a full
// cache line on Apple silicon, and on x86 it covers a 64-byte line plus the neighbor that an
// adjacent-line prefetcher may pull in.
const cacheLine = 128

// syncMode is how the goroutines on one side of a ring synchronize: one goroutine at a time,
// many that publish in reservation order, or many that publish with relaxed tail sync.
// modeSingle must stay the first constant, because the zero config means one goroutine per side.
type syncMode uint8

const (
	modeSingle syncMode = iota
	modeMulti
	modeRelaxed
)

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

// readOnly holds the fields New sets and nothing writes afterward, so every core can keep the
// line holding them in its cache.
type readOnly[T any] struct {
	entries         []T
	mask            uint32
	maxEnqueueBatch uint32
	maxDequeueBatch uint32
	prodHTDMax      uint32
	consHTDMax      uint32
	prodMode        syncMode
	consMode        syncMode
	clearOnDequeue  bool
}

// indices holds all four indices on one line, the layout that held up on both benchmarked
// platforms. A line per side was far slower on Apple silicon. The likely reason is that each
// side reads the other side's tail on every call, so one line moves between cores instead of two.
//
// A relaxed side uses the 64-bit indices, each a pos and cnt word built with pack. It also keeps
// its 32-bit tail as a copy of its 64-bit tail's position, raised by raiseMirror, so the other
// side reads the 32-bit tails in every mode. Go has no sound way to load half of a 64-bit atomic
// word, which is how DPDK lets the other side read the position directly. A relaxed side's
// 32-bit head stays unused.
type indices struct {
	prodHead    atomic.Uint32
	prodTail    atomic.Uint32
	consHead    atomic.Uint32
	consTail    atomic.Uint32
	prodHeadRTS atomic.Uint64
	prodTailRTS atomic.Uint64
	consHeadRTS atomic.Uint64
	consTailRTS atomic.Uint64
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
// uncleared.
//
// The ring holds exactly count elements, where count is the count given to New.
//
// A Ring must not be copied once in use.
type Ring[T any] struct {
	readOnly[T]
	// An array length must be a constant, so the padding is sized from readOnly[byte]. That holds
	// for every T because entries is always a slice header, so readOnly's size never depends on T.
	_ [cacheLine - unsafe.Sizeof(readOnly[byte]{})]byte
	indices
	_ [cacheLine - unsafe.Sizeof(indices{})]byte
}

// A layout change that breaks the Ring's size or the indices' offset fails the build here. The
// [64]byte instance catches a field whose size depends on T, which the byte instance would miss.
var (
	_ [unsafe.Sizeof(Ring[byte]{}) - 2*cacheLine]struct{}
	_ [2*cacheLine - unsafe.Sizeof(Ring[byte]{})]struct{}
	_ [unsafe.Offsetof(Ring[byte]{}.indices) - cacheLine]struct{}
	_ [cacheLine - unsafe.Offsetof(Ring[byte]{}.indices)]struct{}
	_ [unsafe.Sizeof(Ring[[64]byte]{}) - 2*cacheLine]struct{}
	_ [2*cacheLine - unsafe.Sizeof(Ring[[64]byte]{})]struct{}
	_ [unsafe.Offsetof(Ring[[64]byte]{}.indices) - cacheLine]struct{}
	_ [cacheLine - unsafe.Offsetof(Ring[[64]byte]{}.indices)]struct{}
)

type config struct {
	prodMode        syncMode
	consMode        syncMode
	maxEnqueueBatch int
	maxDequeueBatch int
	keepDequeued    bool
}

// Option configures a Ring at construction.
type Option func(*config)

// WithMultiProdEnqueue returns an Option that makes the enqueue methods safe for concurrent use by
// multiple producers. When it and WithRelaxedMultiProdEnqueue are both given, the last one
// applies.
//
// A producer stalled between loading the producer head and its compare-and-swap, while a multiple
// of 2^32 elements pass through, would reserve with a stale count and could overwrite unconsumed
// elements. DPDK has the same exposure, and that many operations during one stall is far outside
// practical use.
func WithMultiProdEnqueue() Option {
	return func(cfg *config) {
		cfg.prodMode = modeMulti
	}
}

// WithMultiConsDequeue returns an Option that makes the dequeue methods safe for concurrent use by
// multiple consumers. When it and WithRelaxedMultiConsDequeue are both given, the last one
// applies.
//
// A consumer stalled between loading the consumer head and its compare-and-swap, while a multiple
// of 2^32 elements pass through, would reserve with a stale count and could return elements
// twice. DPDK has the same exposure, and that many operations during one stall is far outside
// practical use.
func WithMultiConsDequeue() Option {
	return func(cfg *config) {
		cfg.consMode = modeMulti
	}
}

// WithRelaxedMultiProdEnqueue returns an Option that makes the enqueue methods safe for concurrent
// use by multiple producers, with relaxed tail sync, DPDK's RTS mode. When it and
// WithMultiProdEnqueue are both given, the last one applies.
//
// With WithMultiProdEnqueue, a producer publishes only after every producer that reserved before
// it has published. With this option a producer finishes without waiting for the others, and
// consumers see the producers' elements once every reservation made so far has finished. A
// producer descheduled at any point from its reservation to the end of its call still holds back
// every element reserved after it, and an enqueue can return before consumers can see its
// elements. Other producers keep enqueuing until the producer head is an eighth of the ring,
// rounded down, past the producer tail, and then wait. That distance includes finished enqueues
// still held back by an unfinished one, and a call checks it before reserving, so its own batch
// can take the head further. On a ring of fewer than 8 slots that bound is 0, so producers take
// turns. The wait comes before the check for free slots, so a call that finds the ring full, or a
// zero-length call, can wait before it returns.
//
// A producer stalled between loading the producer head and its compare-and-swap, while a multiple
// of 2^32 reservations pass through, would succeed with a stale view. The head holds both a
// position and a count of reservations, and both must match. The publish and the update of the
// tail that consumers read are compare-and-swaps with the same exposure. As with
// WithMultiProdEnqueue, that many operations during one stall is far outside practical use.
//
// On platforms that implement 64-bit atomics with a lock, such as mips, this mode is correct but
// not lock-free.
func WithRelaxedMultiProdEnqueue() Option {
	return func(cfg *config) {
		cfg.prodMode = modeRelaxed
	}
}

// WithRelaxedMultiConsDequeue returns an Option that makes the dequeue methods safe for concurrent
// use by multiple consumers, with relaxed tail sync, DPDK's RTS mode. When it and
// WithMultiConsDequeue are both given, the last one applies.
//
// With WithMultiConsDequeue, a consumer releases its slots only after every consumer that reserved
// before it has released theirs. With this option a consumer finishes without waiting for the
// others, and producers can reuse the consumers' slots once every reservation made so far has
// finished. A consumer descheduled at any point from its reservation to the end of its call still
// holds back the release of every slot reserved after it, and Count can include elements a
// dequeue has already returned. Other consumers keep dequeuing until the consumer head is an
// eighth of the ring, rounded down, past the consumer tail, and then wait. That distance includes
// finished dequeues still held back by an unfinished one, and a call checks it before reserving,
// so its own batch can take the head further. On a ring of fewer than 8 slots that bound is 0, so
// consumers take turns. The wait comes before the check for occupied entries, so a call that finds
// the ring empty, or a zero-length call, can wait before it returns.
//
// A consumer stalled between loading the consumer head and its compare-and-swap, while a multiple
// of 2^32 reservations pass through, would succeed with a stale view. The head holds both a
// position and a count of reservations, and both must match. The release and the update of the
// tail that producers read are compare-and-swaps with the same exposure. As with
// WithMultiConsDequeue, that many operations during one stall is far outside practical use.
//
// On platforms that implement 64-bit atomics with a lock, such as mips, this mode is correct but
// not lock-free.
func WithRelaxedMultiConsDequeue() Option {
	return func(cfg *config) {
		cfg.consMode = modeRelaxed
	}
}

// WithMaxEnqueueBatch returns an Option that limits every enqueue batch call to at most n
// elements. EnqueueBurst moves at most n, and EnqueueBulk rejects a request for more than n with
// ErrExceedsMaxBatch. Enqueue moves one element, so the limit never affects it. Without a limit
// the enqueue batch may be as large as the capacity. New returns an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one enqueue can make others wait. With WithMultiProdEnqueue, a
// producer publishes only after every producer that reserved before it has published, so a long
// batch delays every producer that reserved after it until its copy finishes. With
// WithRelaxedMultiProdEnqueue, it delays consumers' view of every element reserved after it
// instead. With one producer or many, a batch is published all at once, so consumers see none of
// it until its whole copy is done. Choose n from the element size and the delay the ring's users
// can tolerate.
//
// When options set the same limit more than once, the last one given applies. New checks only the
// limit that applies.
func WithMaxEnqueueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
	}
}

// WithMaxDequeueBatch returns an Option that limits every dequeue batch call to at most n
// elements. DequeueBurst moves at most n, and DequeueBulk rejects a request for more than n with
// ErrExceedsMaxBatch. Dequeue moves one element, so the limit never affects it. Without a limit
// the dequeue batch may be as large as the capacity. New returns an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one dequeue can make others wait. With WithMultiConsDequeue, a
// consumer releases its slots only after every consumer that reserved before it has released
// theirs, so a long batch delays every consumer that reserved after it until its copy finishes.
// With WithRelaxedMultiConsDequeue, it delays the release of every slot reserved after it
// instead. With one consumer or many, a batch's slots are released all at once, so producers
// cannot reuse any of them until its whole copy is done. Choose n from the element size and the
// delay the ring's users can tolerate.
//
// When options set the same limit more than once, the last one given applies. New checks only the
// limit that applies.
func WithMaxDequeueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxDequeueBatch = n
	}
}

// WithMaxBatch returns an Option that sets both the maximum enqueue batch and the maximum dequeue
// batch to n. It is shorthand for WithMaxEnqueueBatch(n) and WithMaxDequeueBatch(n) together. When
// options set the same limit more than once, the last one given applies, so a later
// WithMaxEnqueueBatch or WithMaxDequeueBatch overrides this for its side.
func WithMaxBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
		cfg.maxDequeueBatch = n
	}
}

// WithoutDequeueClearing returns an Option that makes the dequeue methods leave the slots they read
// as they are, instead of clearing them.
//
// It can make SPSC dequeues faster for element types without pointers, where clearing releases
// nothing. With pointer elements, up to Capacity stale elements stay reachable until their slots
// are reused, which may be never on a ring that drains and goes quiet. Clearing measured faster
// for MPMC, and for pointer SPSC batches on some machines, so measure before relying on this
// option.
//
// Reset clears every slot whether or not this option is given.
func WithoutDequeueClearing() Option {
	return func(cfg *config) {
		cfg.keepDequeued = true
	}
}

// maxCount is the largest count New accepts, the largest power of two DPDK accepts.
const maxCount = 1 << 30

// relaxedHTDMax returns the head-to-tail bound for a side in mode on a ring of count slots: an
// eighth of the ring, DPDK's default, for a relaxed side, and 0 for any other, which never reads
// it.
func relaxedHTDMax(mode syncMode, count int) uint32 {
	if mode != modeRelaxed {
		return 0
	}
	return uint32(count / 8)
}

// New returns an empty Ring with count slots. A ring of count n holds n elements. Without options,
// the ring allows one producer and one consumer at a time, and a batch call may move up to the
// whole capacity. It returns an error wrapping ErrInvalidCount when count is not a power of two
// between 1 and 2^30, and one wrapping ErrInvalidMaxBatch when a maximum enqueue or dequeue batch
// is below 1 or above count.
func New[T any](count int, options ...Option) (*Ring[T], error) {
	inRange := count >= 1 && count <= maxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return nil, fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}

	cfg := config{maxEnqueueBatch: count, maxDequeueBatch: count}
	for _, option := range options {
		option(&cfg)
	}
	if err := checkMaxBatch("enqueue", cfg.maxEnqueueBatch, count); err != nil {
		return nil, err
	}
	if err := checkMaxBatch("dequeue", cfg.maxDequeueBatch, count); err != nil {
		return nil, err
	}

	ring := &Ring[T]{
		readOnly: readOnly[T]{
			entries:         make([]T, count),
			mask:            uint32(count - 1),
			maxEnqueueBatch: uint32(cfg.maxEnqueueBatch),
			maxDequeueBatch: uint32(cfg.maxDequeueBatch),
			prodHTDMax:      relaxedHTDMax(cfg.prodMode, count),
			consHTDMax:      relaxedHTDMax(cfg.consMode, count),
			prodMode:        cfg.prodMode,
			consMode:        cfg.consMode,
			clearOnDequeue:  !cfg.keepDequeued,
		},
	}
	return ring, nil
}

// checkMaxBatch returns an error wrapping ErrInvalidMaxBatch when maxBatch, the limit for the
// direction op names, is below 1 or above count.
func checkMaxBatch(op string, maxBatch, count int) error {
	if maxBatch >= 1 && maxBatch <= count {
		return nil
	}
	return fmt.Errorf(
		"new ring with count %d and max %s batch %d: %w",
		count, op, maxBatch, ErrInvalidMaxBatch,
	)
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
// Reset does not change how the ring was configured. Elements that were enqueued but never
// dequeued are released. It clears every slot even on a ring created with WithoutDequeueClearing.
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
	enqueued, _ := r.doEnqueue(items[:], true)
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
// between 0 and Capacity inclusive.
//
// EnqueueBulk is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue or WithRelaxedMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBulk(items []T) (free int, err error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path. It is in int, so a length beyond the uint32 range cannot truncate.
	if len(items) > int(r.maxEnqueueBatch) {
		return int(r.freeEntries()), r.bulkLengthError("enqueue", uint64(len(items)), r.maxEnqueueBatch)
	}

	enqueued, freeAfter := r.doEnqueue(items, true)
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
// Capacity inclusive.
//
// EnqueueBurst is safe for concurrent use with other enqueues only when the ring was created with
// WithMultiProdEnqueue or WithRelaxedMultiProdEnqueue. It is always safe to run alongside dequeues.
func (r *Ring[T]) EnqueueBurst(items []T) (enqueued, free int) {
	// Clamping in int keeps a slice longer than 2^32 from truncating when its length becomes a
	// uint32 in doEnqueue. The limit is at most the capacity, so it bounds both.
	clamped := min(len(items), int(r.maxEnqueueBatch))
	moved, freeAfter := r.doEnqueue(items[:clamped], false)
	return int(moved), int(freeAfter)
}

// Dequeue removes and returns the oldest element in the ring. It returns the zero value and
// ErrEmpty, unwrapped, when the ring is empty.
//
// Dequeue is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) Dequeue() (T, error) {
	var dst [1]T
	dequeued, _ := r.doDequeue(dst[:], true)
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
// the call, between 0 and Capacity inclusive.
//
// DequeueBulk is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBulk(dst []T) (available int, err error) {
	// The limit is at most the capacity, so one comparison guards both checks on the success
	// path. It is in int, so a length beyond the uint32 range cannot truncate.
	if len(dst) > int(r.maxDequeueBatch) {
		return int(r.occupiedEntries()), r.bulkLengthError("dequeue", uint64(len(dst)), r.maxDequeueBatch)
	}

	dequeued, availableAfter := r.doDequeue(dst, true)
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
// between 0 and Capacity inclusive.
//
// DequeueBurst is safe for concurrent use with other dequeues only when the ring was created with
// WithMultiConsDequeue or WithRelaxedMultiConsDequeue. It is always safe to run alongside enqueues.
func (r *Ring[T]) DequeueBurst(dst []T) (dequeued, available int) {
	// Clamping in int keeps a slice longer than 2^32 from truncating when its length becomes a
	// uint32 in doDequeue. The limit is at most the capacity, so it bounds both.
	clamped := min(len(dst), int(r.maxDequeueBatch))
	moved, availableAfter := r.doDequeue(dst[:clamped], false)
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

// doEnqueue places elements from the front of items in the ring. fixed is true for a bulk
// enqueue, which enqueues all of items or nothing. It returns the number enqueued and the number
// of free slots after the call, which is at most the capacity.
//
// Callers pass a slice whose length is already at most the capacity, so converting len(items) to
// uint32 cannot truncate.
func (r *Ring[T]) doEnqueue(items []T, fixed bool) (enqueued, free uint32) {
	if r.prodMode == modeRelaxed {
		return r.doEnqueueRelaxed(items, fixed)
	}

	var (
		head uint32
		next uint32
	)

	numEntries, freeBefore := r.moveProdHead(uint32(len(items)), &head, &next, fixed)
	if numEntries == 0 {
		// Without a reservation the producer head moveProdHead read may be stale, so the count
		// can overshoot the capacity. With a reservation the head was current when the consumer
		// tail was loaded, because the single producer owns the head or the multi-producer CAS
		// succeeded on it, so capacity + tail - head is already between 0 and the capacity.
		return 0, min(freeBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.writeSlots(base, end, items[:numEntries])

	if r.prodMode == modeMulti {
		waitForTail(&r.prodTail, head)
	}

	// the tail store publishes the slots written above to consumers
	r.prodTail.Store(next)

	return numEntries, freeBefore - numEntries
}

// doEnqueueRelaxed is doEnqueue for a relaxed producer side. It publishes without waiting for the
// producers that reserved before it.
func (r *Ring[T]) doEnqueueRelaxed(items []T, fixed bool) (enqueued, free uint32) {
	head, numEntries, freeBefore := r.moveProdHeadRelaxed(uint32(len(items)), fixed)
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
// fixed is true for a bulk dequeue, which fills all of dst or nothing. It returns the number
// dequeued and the number of occupied entries after the call, which is at most the capacity. It
// writes only dst[:dequeued] and never allocates.
//
// Callers pass a slice whose length is already at most the capacity, so converting len(dst) to
// uint32 cannot truncate.
func (r *Ring[T]) doDequeue(dst []T, fixed bool) (dequeued, available uint32) {
	if r.consMode == modeRelaxed {
		return r.doDequeueRelaxed(dst, fixed)
	}

	var (
		head uint32
		next uint32
	)

	numEntries, availableBefore := r.moveConsHead(uint32(len(dst)), &head, &next, fixed)
	if numEntries == 0 {
		// Without a reservation the consumer head moveConsHead read may be stale, so the count
		// can overshoot the capacity. With a reservation the head was current when the producer
		// tail was loaded, because the single consumer owns the head or the multi-consumer CAS
		// succeeded on it, so tail - head is already between 0 and the capacity.
		return 0, min(availableBefore, r.capacity())
	}

	base, end := r.slotRange(head, numEntries)
	r.readSlots(base, end, dst[:numEntries])

	if r.clearOnDequeue {
		r.clearSlots(base, end, numEntries)
	}

	if r.consMode == modeMulti {
		waitForTail(&r.consTail, head)
	}

	// the tail store hands the slots read above back to producers
	r.consTail.Store(next)

	return numEntries, availableBefore - numEntries
}

// doDequeueRelaxed is doDequeue for a relaxed consumer side. It releases its slots without waiting
// for the consumers that reserved before it.
func (r *Ring[T]) doDequeueRelaxed(dst []T, fixed bool) (dequeued, available uint32) {
	head, numEntries, availableBefore := r.moveConsHeadRelaxed(uint32(len(dst)), fixed)
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

// tailSpins is how many times waitForTail checks the tail in a plain loop before it starts
// yielding. On an Apple M3 Pro and an AMD Ryzen, 4096 beat 1024 by 6 to 23% in geometric mean over
// the MPSC, SPMC, and MPMC rows, most of what 16384 gained, and cost nothing with one CPU. When the
// goroutine being waited for is descheduled, one exhausted spin wastes 1 to 3 µs.
const tailSpins = 4096

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
//
// It first calls spinForTail with tailSpins, since the earlier goroutine is usually nanoseconds
// from publishing. The bound keeps it from burning a whole time slice when that goroutine has
// been descheduled, and after it waitForTail yields with runtime.Gosched between checks, so the
// goroutine it waits for gets to run and publish, even with GOMAXPROCS=1. Whichever load sees
// want synchronizes with the earlier goroutine's tail store, so that goroutine's slot writes are
// published along with the caller's own tail store that follows.
func waitForTail(tail *atomic.Uint32, want uint32) {
	if spinForTail(tail, want, tailSpins) {
		return
	}
	for tail.Load() != want {
		runtime.Gosched()
	}
}

// waitHeadTailDistance returns the head word once the side's head position is at most htdMax past
// its tail position, which bounds how many slots are reserved but not yet published. Without the
// bound, a steady stream of operations could keep one always in flight, and the tail would never
// move. It spins like waitForTail, then yields between checks.
//
// It loads the tail before the head, unlike DPDK, so the tail is never past the loaded head and
// the distance cannot wrap into a spurious wait. The head is still loaded before the caller reads
// the other side's tail.
func waitHeadTailDistance(head, tail *atomic.Uint64, htdMax uint32) uint64 {
	for spins := 0; ; spins++ {
		tailPos := unpackPos(tail.Load())
		word := head.Load()
		if unpackPos(word)-tailPos <= htdMax {
			return word
		}
		if spins >= tailSpins {
			runtime.Gosched()
		}
	}
}

// moveProdHeadRelaxed reserves up to n slots for a relaxed enqueue, as moveProdHead does for the
// multi mode, after waitHeadTailDistance allows it. The head's counter counts reservations. It
// returns the position the reservation starts at, the number of slots reserved, between 0 and n
// inclusive and 0 or n when fixed, and the number of free slots before the reservation.
func (r *Ring[T]) moveProdHeadRelaxed(n uint32, fixed bool) (oldPos, count, free uint32) {
	capacity := r.capacity()
	for {
		head := waitHeadTailDistance(&r.prodHeadRTS, &r.prodTailRTS, r.prodHTDMax)
		oldPos = unpackPos(head)
		free = capacity + r.consTail.Load() - oldPos
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
// inclusive and 0 or n when fixed, and the number of occupied entries before the reservation.
func (r *Ring[T]) moveConsHeadRelaxed(n uint32, fixed bool) (oldPos, count, available uint32) {
	for {
		head := waitHeadTailDistance(&r.consHeadRTS, &r.consTailRTS, r.consHTDMax)
		oldPos = unpackPos(head)
		available = r.prodTail.Load() - oldPos
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

// moveProdHead moves the ring's producer head for an enqueue operation.
//
// numItems is the number of items to enqueue, which is how far the head should move. oldHead
// receives the producer head from before the move, where the enqueue starts, and newHead receives
// it from after the move, where the enqueue ends. fixed is true for a bulk enqueue, which enqueues
// the entire batch or nothing.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed enqueue. It also returns the number of free entries in the ring before the
// head was moved.
func (r *Ring[T]) moveProdHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
) (uint32, uint32) {
	capacity := r.capacity()
	maxItems := numItems

	for {
		head := r.prodHead.Load()
		tail := r.consTail.Load()

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
// the entire batch or nothing.
//
// It returns the number of items reserved, between 0 and numItems inclusive, which is 0 or
// numItems for a fixed dequeue. It also returns the number of occupied entries in the ring before
// the head was moved.
func (r *Ring[T]) moveConsHead(
	numItems uint32,
	oldHead *uint32,
	newHead *uint32,
	fixed bool,
) (uint32, uint32) {
	var (
		tail   uint32
		head   uint32
		filled uint32
	)

	maxItems := numItems

	for {
		head = r.consHead.Load()
		tail = r.prodTail.Load()

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
