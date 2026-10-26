package dpdk

import "github.com/freshdresch/go-data-structures/ring/internal/shared"

func batchLengthError(op string, length int, maxBatch, capacity uint32) error {
	return shared.BatchLengthError(op, length, maxBatch, capacity)
}
