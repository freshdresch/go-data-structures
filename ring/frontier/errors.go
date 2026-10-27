package frontier

import "github.com/freshdresch/go-data-structures/ring"

// These errors are the ring package's sentinels, which the DPDK-style queues also return, so
// errors.Is matches them whichever queue family returned one.
var (
	// ErrInvalidCount is returned by constructors, wrapped, when count is not a power of two, is
	// less than 1, or exceeds 2^30.
	ErrInvalidCount = ring.ErrInvalidCount
	// ErrFull is returned unwrapped by Enqueue and EnqueueBulk when the ring does not have
	// enough free slots for the request. The caller may retry.
	ErrFull = ring.ErrFull
	// ErrEmpty is returned unwrapped by Dequeue and DequeueBulk when the ring does not have
	// enough published entries for the request. The caller may retry.
	ErrEmpty = ring.ErrEmpty
	// ErrExceedsCapacity is returned wrapped by EnqueueBulk or DequeueBulk when the request is
	// longer than the capacity, so it can never succeed.
	ErrExceedsCapacity = ring.ErrExceedsCapacity
	// ErrInvalidMaxBatch is returned by constructors, wrapped, when a final batch limit is below
	// 1 or above the count.
	ErrInvalidMaxBatch = ring.ErrInvalidMaxBatch
	// ErrExceedsMaxBatch is returned wrapped by EnqueueBulk or DequeueBulk when the request is
	// longer than the side's batch limit but fits the capacity, so it can never succeed.
	ErrExceedsMaxBatch = ring.ErrExceedsMaxBatch
)
