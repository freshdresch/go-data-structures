package ring

import "fmt"

func batchLengthError(op string, length int, maxBatch, capacity uint32) error {
	if length > int(capacity) {
		return fmt.Errorf(
			"%s %d elements, capacity %d: %w",
			op, length, capacity, ErrExceedsCapacity,
		)
	}
	return fmt.Errorf(
		"%s %d elements, max %s batch %d: %w",
		op, length, op, maxBatch, ErrExceedsMaxBatch,
	)
}
