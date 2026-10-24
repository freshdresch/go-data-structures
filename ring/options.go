package ring

import (
	"fmt"
	"math"
)

// syncMode is how the goroutines on one side of a ring synchronize: one goroutine at a time,
// many that publish in reservation order, or many that publish with relaxed tail sync.
// modeSingle must stay the first constant, because the zero config means one goroutine per side.
type syncMode uint8

const (
	modeSingle syncMode = iota
	modeMulti
	modeRelaxed
)

type config struct {
	prodMode                    syncMode
	consMode                    syncMode
	maxEnqueueBatch             int
	maxDequeueBatch             int
	keepDequeued                bool
	exactBatchCounts            bool
	spinsBeforeYield            int
	spinsBeforeYieldSet         bool
	maxProducerHeadTailDistance int
	maxConsumerHeadTailDistance int
	producerHeadTailDistanceSet bool
	consumerHeadTailDistanceSet bool
}

// Option configures an SPSCQueue, MPSCQueue, SPMCQueue, or MPMCQueue at construction.
// Options apply in argument order. Constructors validate only the final values, after checking
// capacity. Options never change a queue's fixed producer/consumer topology.
// Incompatible modes, serialized-side distances, and explicit SPSCQueue spins wrap
// ErrUnsupportedOption. A distance on an ordered side wraps ErrHeadTailDistanceRequiresRelaxed.
// Numeric distance errors precede applicability. Concurrent sides use ordered publication by
// default. WithRelaxedMultiProdEnqueue and WithRelaxedMultiConsDequeue select RTS at construction.
type Option func(*config)

// WithRelaxedMultiProdEnqueue returns an Option that selects relaxed tail sync, DPDK's RTS mode,
// for producers at construction. MPSCQueue and MPMCQueue accept this option. SPSCQueue and
// SPMCQueue reject it.
//
// By default, a producer on a concurrent side publishes only after every producer that reserved
// before it has published. With this option a producer finishes without waiting for the others, and
// consumers see the producers' elements once every reservation made so far has finished. A
// producer descheduled at any point from its reservation to the end of its call still holds back
// every element reserved after it, and an enqueue can return before consumers can see its
// elements. Before every reservation attempt, producers wait while head minus tail exceeds the
// configured limit. The default is count / 8, rounded down. WithMaxProducerHeadTailDistance
// overrides it. That distance includes finished enqueues held back by an unfinished one.
// Equality admits an attempt, and its own batch can take the head further. Below 8 slots the
// default is zero, so reservations take turns. The wait precedes the free-slot check, so full
// and zero-length calls can wait. Bulk length errors return before this wait.
//
// Whether this mode is faster depends on the machine and the batch size. Each call does two more
// atomic read-modify-writes than ordered publication, one to publish and one to update the tail
// that consumers read, in exchange for not waiting for the producers that reserved before it. In
// the ring's MPSC, SPMC, and MPMC throughput benchmarks, it made batches of 32 or 64 elements 14 to
// 36% faster per element on an Apple M3 Pro, and batches of 1 or 8 elements 3 to 12% slower. On an
// AMD Ryzen 7 5800X it was 6 to 29% slower in most cases. Measure before choosing it.
//
// A producer stalled between loading the producer head and its compare-and-swap, while a multiple
// of 2^32 reservations pass through, would succeed with a stale view. The head holds both a
// position and a count of reservations, and both must match. The publish and the update of the
// tail that consumers read are compare-and-swaps with the same exposure. As with ordered
// publication, that many operations during one stall is far outside practical use.
//
// On platforms that implement 64-bit atomics with a lock, such as mips, this mode is correct but
// not lock-free.
func WithRelaxedMultiProdEnqueue() Option {
	return func(cfg *config) {
		cfg.prodMode = modeRelaxed
	}
}

// WithRelaxedMultiConsDequeue returns an Option that selects relaxed tail sync, DPDK's RTS mode,
// for consumers at construction. SPMCQueue and MPMCQueue accept this option. SPSCQueue and
// MPSCQueue reject it.
//
// By default, a consumer on a concurrent side releases its slots only after every consumer that
// reserved before it has released theirs. With this option a consumer finishes without waiting for
// the others, and producers can reuse the consumers' slots once every reservation made so far has
// finished. A consumer descheduled at any point from its reservation to the end of its call still
// holds back the release of every slot reserved after it, and Count can include elements a
// dequeue has already returned. Before every reservation attempt, consumers wait while head
// minus tail exceeds the configured limit. The default is count / 8, rounded down.
// WithMaxConsumerHeadTailDistance overrides it. That distance includes finished dequeues held
// back by an unfinished one. Equality admits an attempt, and its own batch can take the head
// further. Below 8 slots the default is zero, so reservations take turns. The wait precedes the
// occupied-entry check, so empty and zero-length calls can wait. Bulk length errors return
// before this wait.
//
// Whether this mode is faster depends on the machine and the batch size. Each call does two more
// atomic read-modify-writes than ordered publication, one to release its slots and one to update
// the tail that producers read, in exchange for not waiting for the consumers that reserved before
// it. In the ring's MPSC, SPMC, and MPMC throughput benchmarks, it made batches of 32 or 64
// elements 14 to 36% faster per element on an Apple M3 Pro, and batches of 1 or 8 elements 3 to 12%
// slower. On an AMD Ryzen 7 5800X it was 6 to 29% slower in most cases. Measure before choosing it.
//
// A consumer stalled between loading the consumer head and its compare-and-swap, while a multiple
// of 2^32 reservations pass through, would succeed with a stale view. The head holds both a
// position and a count of reservations, and both must match. The release and the update of the
// tail that producers read are compare-and-swaps with the same exposure. As with ordered
// publication, that many operations during one stall is far outside practical use.
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
// the enqueue batch may be as large as the capacity. Constructors return an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one enqueue can make others wait. With default ordered publication,
// a producer publishes only after every producer that reserved before it has published, so a long
// batch delays every producer that reserved after it until its copy finishes. With
// WithRelaxedMultiProdEnqueue, it delays consumers' view of every element reserved after it
// instead. With one producer or many, a batch is published all at once, so consumers see none of
// it until its whole copy is done. Choose n from the element size and the delay the ring's users
// can tolerate.
//
// When options set the same limit more than once, the last one given applies. Constructors check
// only the final limit.
func WithMaxEnqueueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
	}
}

// WithMaxDequeueBatch returns an Option that limits every dequeue batch call to at most n
// elements. DequeueBurst moves at most n, and DequeueBulk rejects a request for more than n with
// ErrExceedsMaxBatch. Dequeue moves one element, so the limit never affects it. Without a limit
// the dequeue batch may be as large as the capacity. Constructors return an error wrapping
// ErrInvalidMaxBatch when the limit is below 1 or above the count.
//
// The limit bounds how long one dequeue can make others wait. With default ordered publication,
// a consumer releases its slots only after every consumer that reserved before it has released
// theirs, so a long batch delays every consumer that reserved after it until its copy finishes.
// With WithRelaxedMultiConsDequeue, it delays the release of every slot reserved after it
// instead. With one consumer or many, a batch's slots are released all at once, so producers
// cannot reuse any of them until its whole copy is done. Choose n from the element size and the
// delay the ring's users can tolerate.
//
// When options set the same limit more than once, the last one given applies. Constructors check
// only the final limit.
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

// WithExactBatchCounts returns an Option that makes EnqueueBulk, EnqueueBurst, DequeueBulk, and
// DequeueBurst return exact counts, by loading the other side's tail on every call as Count does.
//
// By default a batch call first checks its request against a cached copy of the other side's
// tail, and loads the real tail only when the cache says the request does not fit. A call never
// fails or moves fewer elements because of the cache, but when the cache admits it, its returned
// free or available count comes from the cache and can be stale. For example, enqueue 1000
// elements into a 1024-slot ring, dequeue all 1000, then call EnqueueBurst with one element. A
// fresh load reports 1023 free, while a cache that still holds the consumer at 0 reports 23.
//
// With one goroutine on the caller's side, a stale count is never more than the real value at the
// time of the call. With concurrent callers on the same side, under ordered publication or RTS,
// the count is a snapshot, as an exact one is, because the side's other goroutines reserve at the
// same time, so it can be higher or lower than the real value by the time the caller reads it.
//
// So by default the counts can under-report, and must not drive blocking or wakeup decisions, or
// size the next request. Two examples:
//
//   - A DequeueBurst that dequeued some elements on the cache's say-so can report 0 available
//     while data published before the call is waiting, so a consumer that parks on that 0 can park
//     with data in the ring.
//   - A producer that signals a parked consumer only when its EnqueueBurst or EnqueueBulk saw the
//     ring empty, that is free + n == Capacity, can see a stale, low free, decide the ring was not
//     empty, and skip the signal.
//
// A call that loaded the real tail returns a fresh count, which includes every call that returned
// ErrFull or ErrEmpty and every Burst that moved fewer elements than its request after the
// MaxEnqueueBatch or MaxDequeueBatch clamp. A Burst that moved all of its clamped request can
// return a stale count, even when the clamp left the request shorter than its slice. A zero-length
// call always loads the real tail, so it returns a fresh count under either setting, as do Count
// and Free.
//
// With this option, the batch calls load the real tail on every call, which reads the other side's
// cache line. Enqueue and Dequeue return no count, so they use the cache under either setting.
func WithExactBatchCounts() Option {
	return func(cfg *config) {
		cfg.exactBatchCounts = true
	}
}

// WithSpinsBeforeYield returns an Option that sets how many times a waiting goroutine checks
// before it starts yielding with runtime.Gosched. It affects only concurrent sides, under default
// ordered publication or RTS, because a single side never waits. When it is given more than once,
// the last one applies. The default is defaultSpinsBeforeYield, 8192. Constructors wrap
// ErrInvalidSpinsBeforeYield when n is below 0
// or above math.MaxInt32. SPSCQueue rejects every explicit spin option with ErrUnsupportedOption
// after numeric and topology validation.
//
// A goroutine on a multi side waits for every earlier goroutine on its side to publish, and one
// on a relaxed side waits while too many reservations are in flight. A longer spin helps when the
// goroutine being waited for is running but slow to publish, because the waiter skips the
// scheduler. It wastes CPU when that goroutine has been preempted, which happens most when the
// machine runs more threads than cores. Yielding does not give the thread back to the operating
// system, so the process keeps the core either way.
//
// The best value depends on the machine and the load. On an Apple M3 Pro, a bound of 65536 was
// up to 34% faster than 4096 at GOMAXPROCS 10 on an idle machine, and up to 49% slower at
// GOMAXPROCS 8 when every core also ran a CPU-bound process. On an AMD Ryzen 7 5800X under WSL2,
// longer bounds made no measurable difference on an idle machine, and 65536 was up to 25% faster
// under the same load.
//
// With GOMAXPROCS=1, any spin is wasted, because the goroutine being waited for cannot run until
// the waiter yields, so 0 suits that setting. A very large n effectively removes the yield. At
// math.MaxInt32 a waiter can hold its P for up to about a second and a half, and with
// GOMAXPROCS=1 it then relies on the Go runtime's asynchronous preemption.
func WithSpinsBeforeYield(n int) Option {
	return func(cfg *config) {
		cfg.spinsBeforeYield = n
		cfg.spinsBeforeYieldSet = true
	}
}

// maxCount is the largest count queue constructors accept, the largest power of two DPDK accepts.
const maxCount = 1 << 30

// WithMaxProducerHeadTailDistance sets the producer RTS pre-admission distance limit.
// The final producer mode must be relaxed and support concurrent producers. Omission uses
// count / 8. Distances count element positions, not reservations, bytes, or goroutines.
// Explicit zero permits admission only when earlier reservations have published.
// Values in [0, math.MaxUint32] are accepted when representable as int. A limit at or above
// capacity removes distance throttling, but capacity admission still applies. Each attempt
// waits while head minus tail exceeds the limit, so its own batch can cross the limit.
// Full and zero-length calls can wait. Bulk length errors return before this wait.
// This option never selects RTS. Use WithRelaxedMultiProdEnqueue on MPSCQueue or MPMCQueue.
// Numeric errors wrap ErrInvalidMaxHeadTailDistance. A serialized side wraps ErrUnsupportedOption.
// An ordered side wraps ErrHeadTailDistanceRequiresRelaxed.
func WithMaxProducerHeadTailDistance(n int) Option {
	return func(cfg *config) {
		cfg.maxProducerHeadTailDistance = n
		cfg.producerHeadTailDistanceSet = true
	}
}

// WithMaxConsumerHeadTailDistance sets the consumer RTS pre-admission distance limit.
// It has the same numeric and waiting contract as WithMaxProducerHeadTailDistance, but
// requires a final relaxed consumer mode that supports concurrent consumers. Empty and
// zero-length calls can wait. Use WithRelaxedMultiConsDequeue on SPMCQueue or MPMCQueue.
// Distances never select relaxed mode implicitly. The producer option documents distance errors.
func WithMaxConsumerHeadTailDistance(n int) Option {
	return func(cfg *config) {
		cfg.maxConsumerHeadTailDistance = n
		cfg.consumerHeadTailDistanceSet = true
	}
}

func checkCapacity(count int) error {
	inRange := count >= 1 && count <= maxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return fmt.Errorf("new ring with count %d: %w", count, ErrInvalidCount)
	}
	return nil
}

func defaultConfig(count int, producerMode, consumerMode syncMode) config {
	return config{
		prodMode:         producerMode,
		consMode:         consumerMode,
		maxEnqueueBatch:  count,
		maxDequeueBatch:  count,
		spinsBeforeYield: defaultSpinsBeforeYield,
	}
}

// resolveConfig validates before allocation. Initial modes describe constructor topology,
// not mutable runtime state. fixedTopology enables restrictions for those initial modes.
func resolveConfig(
	constructor string,
	count int,
	producerMode, consumerMode syncMode,
	fixedTopology bool,
	options []Option,
) (config, error) {
	if err := checkCapacity(count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	cfg := defaultConfig(count, producerMode, consumerMode)
	for _, option := range options {
		option(&cfg)
	}
	if err := checkMaxBatch("enqueue", cfg.maxEnqueueBatch, count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	if err := checkMaxBatch("dequeue", cfg.maxDequeueBatch, count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	if err := checkSpinsBeforeYield(cfg.spinsBeforeYield, count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	if fixedTopology {
		if err := checkTopologyMode(
			constructor,
			count,
			"producer",
			cfg.prodMode,
			producerMode,
		); err != nil {
			return config{}, err
		}
		if err := checkTopologyMode(
			constructor,
			count,
			"consumer",
			cfg.consMode,
			consumerMode,
		); err != nil {
			return config{}, err
		}
		serialized := producerMode == modeSingle && consumerMode == modeSingle
		if serialized && cfg.spinsBeforeYieldSet {
			return config{}, fmt.Errorf(
				"%s with count %d and WithSpinsBeforeYield(%d): %w",
				constructor, count, cfg.spinsBeforeYield, ErrUnsupportedOption,
			)
		}
	}
	if err := checkHeadTailDistance(
		constructor,
		count,
		"producer",
		cfg.maxProducerHeadTailDistance,
		cfg.producerHeadTailDistanceSet,
		cfg.prodMode,
		fixedTopology && producerMode == modeSingle,
		"WithRelaxedMultiProdEnqueue",
	); err != nil {
		return config{}, err
	}
	if err := checkHeadTailDistance(
		constructor,
		count,
		"consumer",
		cfg.maxConsumerHeadTailDistance,
		cfg.consumerHeadTailDistanceSet,
		cfg.consMode,
		fixedTopology && consumerMode == modeSingle,
		"WithRelaxedMultiConsDequeue",
	); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func checkTopologyMode(
	constructor string,
	count int,
	side string,
	mode, initialMode syncMode,
) error {
	compatible := mode == modeSingle
	if initialMode != modeSingle {
		compatible = mode == modeMulti || mode == modeRelaxed
	}
	if compatible {
		return nil
	}
	return fmt.Errorf(
		"%s with count %d and %s mode %d: %w",
		constructor, count, side, mode, ErrUnsupportedOption,
	)
}

func checkHeadTailDistance(
	constructor string,
	count int,
	side string,
	distance int,
	explicit bool,
	mode syncMode,
	serialized bool,
	relaxedSelector string,
) error {
	if !explicit {
		return nil
	}
	if err := checkHeadTailDistanceNumber(distance); err != nil {
		return fmt.Errorf(
			"%s with count %d and %s head-to-tail distance %d: %w",
			constructor, count, side, distance, err,
		)
	}
	if serialized {
		return fmt.Errorf(
			"%s with count %d and %s head-to-tail distance %d: %w",
			constructor, count, side, distance, ErrUnsupportedOption,
		)
	}
	if mode != modeRelaxed {
		return fmt.Errorf(
			"%s with count %d and %s head-to-tail distance %d requires %s: %w",
			constructor, count, side, distance, relaxedSelector, ErrHeadTailDistanceRequiresRelaxed,
		)
	}
	return nil
}

func checkHeadTailDistanceNumber(distance int) error {
	if distance < 0 {
		return ErrInvalidMaxHeadTailDistance
	}
	if uint64(distance) > math.MaxUint32 {
		return ErrInvalidMaxHeadTailDistance
	}
	return nil
}

func resolveHeadTailDistance(mode syncMode, count, distance int, explicit bool) uint32 {
	if mode != modeRelaxed {
		return 0
	}
	if explicit {
		return uint32(distance)
	}
	return uint32(count / 8)
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

// checkSpinsBeforeYield returns an error wrapping ErrInvalidSpinsBeforeYield when spins, the
// bound set with WithSpinsBeforeYield, is below 0 or above math.MaxInt32.
func checkSpinsBeforeYield(spins, count int) error {
	if spins >= 0 && spins <= math.MaxInt32 {
		return nil
	}
	return fmt.Errorf(
		"new ring with count %d and spins before yield %d: %w",
		count, spins, ErrInvalidSpinsBeforeYield,
	)
}
