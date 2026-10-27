// Package shared holds the construction and length checks common to every ring queue family. The
// errors they wrap are the ring package's sentinels, so errors.Is matches the same value whichever
// family returned one.
package shared

import (
	"fmt"

	"github.com/freshdresch/go-data-structures/ring"
)

// MaxCount is the largest count queue constructors accept, the largest power of two DPDK accepts.
const MaxCount = 1 << 30

// CheckCapacity returns an error wrapping ring.ErrInvalidCount unless count is a power of two in
// [1, MaxCount].
func CheckCapacity(count int) error {
	inRange := count >= 1 && count <= MaxCount
	isPowerOfTwo := count&(count-1) == 0
	if !inRange || !isPowerOfTwo {
		return fmt.Errorf("new ring with count %d: %w", count, ring.ErrInvalidCount)
	}
	return nil
}

// CheckMaxBatch returns an error wrapping ring.ErrInvalidMaxBatch unless maxBatch is in [1, count].
// op names the side, "enqueue" or "dequeue".
func CheckMaxBatch(op string, maxBatch, count int) error {
	if maxBatch >= 1 && maxBatch <= count {
		return nil
	}
	return fmt.Errorf(
		"new ring with count %d and max %s batch %d: %w",
		count, op, maxBatch, ring.ErrInvalidMaxBatch,
	)
}

// BatchLengthError returns the error a Bulk call returns for a request of length elements that
// can never succeed: ring.ErrExceedsCapacity when it exceeds capacity, otherwise
// ring.ErrExceedsMaxBatch.
func BatchLengthError(op string, length int, maxBatch, capacity uint32) error {
	if length > int(capacity) {
		return fmt.Errorf(
			"%s %d elements, capacity %d: %w",
			op, length, capacity, ring.ErrExceedsCapacity,
		)
	}
	return fmt.Errorf(
		"%s %d elements, max %s batch %d: %w",
		op, length, op, maxBatch, ring.ErrExceedsMaxBatch,
	)
}
