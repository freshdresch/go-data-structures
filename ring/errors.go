package ring

import "errors"

// ErrInvalidCount is returned by constructors, wrapped, when count is not a power of two, is less
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

// ErrInvalidMaxBatch is returned by constructors, wrapped, when an enqueue or dequeue batch size,
// set with WithMaxBatch, WithMaxEnqueueBatch, or WithMaxDequeueBatch, is below 1 or above the
// count.
var ErrInvalidMaxBatch = errors.New("ring: max batch must be between 1 and the ring's count")

// ErrInvalidSpinsBeforeYield is returned by constructors, wrapped, when the bound set with
// WithSpinsBeforeYield is below 0 or above math.MaxInt32.
var ErrInvalidSpinsBeforeYield = errors.New(
	"ring: spins before yield must be between 0 and math.MaxInt32",
)

// ErrExceedsMaxBatch is returned wrapped by EnqueueBulk when len(items) exceeds the ring's maximum
// enqueue batch size, and by DequeueBulk when len(dst) exceeds its maximum dequeue batch size,
// while still fitting the capacity. Such a request can never succeed, so retrying it is
// pointless.
var ErrExceedsMaxBatch = errors.New("ring: requested count exceeds the ring's maximum batch size")

// ErrInvalidMaxHeadTailDistance is wrapped when an explicit distance is negative or exceeds
// math.MaxUint32. Inputs must also be representable as int on the target platform.
var ErrInvalidMaxHeadTailDistance = errors.New("ring: invalid maximum head-to-tail distance")

// ErrHeadTailDistanceRequiresRelaxed is wrapped when an explicit distance targets a side
// whose final mode is not relaxed tail sync.
var ErrHeadTailDistanceRequiresRelaxed = errors.New("ring: head-to-tail distance requires RTS")

// ErrUnsupportedOption is wrapped when a final option conflicts with a queue's fixed topology,
// or when SPSCQueue receives an explicit spin option.
var ErrUnsupportedOption = errors.New("ring: option unsupported by queue topology")
