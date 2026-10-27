package frontier

import "sync/atomic"

// MPSCQueue is a fixed-capacity FIFO with concurrent producers and one serialized consumer.
// Construct it with NewMPSCQueue. Its zero value is unusable, and it must not be copied after use.
//
// Producers never wait for one another. Each reserves an interval with one compare-and-swap,
// copies into it, and publishes it on its own. The consumer sees every value up to the first
// reservation that has not finished, so FIFO follows reservation order, and a returned Enqueue
// does not mean its value is visible yet: a producer stalled mid-enqueue holds back every value
// reserved after it until it finishes. Producer compare-and-swap retries have no fixed bound, so
// the queue is neither wait-free nor formally lock-free.
//
// Every consumer call, including failed and zero-length calls, belongs to one serialized owner,
// and handing that role to another goroutine needs a happens-before edge such as a channel or
// mutex. Producers, Count, Free, Full, Empty, and the configuration queries may run concurrently
// with both sides. Count may record what it discovered, so later calls start from there. Reset
// needs exclusive access against every operation and observer, and synchronized publication to
// later users.
//
// Positions are 64-bit, and no position may overflow between construction or Resets. No producer
// attempt or Count call may span a complete 2^32 position cycle. Platforms may implement 64-bit
// atomics with locks.
type MPSCQueue[T any] struct {
	mpscBase[T]
	_                 [128]byte
	observerCompleted atomic.Uint64
}

// NewMPSCQueue returns an empty queue using all count slots, with clearing enabled and cached
// batch counts. Count must be a power of two in [1, 2^30], otherwise it wraps ErrInvalidCount
// before applying options. Batch limits default to count, and invalid final limits wrap
// ErrInvalidMaxBatch.
func NewMPSCQueue[T any](count int, options ...Option) (*MPSCQueue[T], error) {
	cfg, err := resolveConfig("NewMPSCQueue", count, options)
	if err != nil {
		return nil, err
	}
	queue := &MPSCQueue[T]{}
	queue.init(count, cfg)
	return queue, nil
}

type mpscCountObservation struct {
	result   uint64
	observed uint64
	end      uint64
	retain   bool
}

// Count discovers the completed prefix through a captured head or the first hole.
// Changed consumer state returns the final acquired prefix without retry. The bounded result
// is not a linearizable snapshot. An unchanged scan may retain historical completion proof.
func (r *MPSCQueue[T]) Count() int {
	before := r.consumerPublished.Load()
	observation := r.mpscScanCount(before)
	result, retain := r.mpscValidateCount(before, observation)
	if retain {
		r.mpscPublishProof(observation.observed, observation.end)
	}
	return result
}

func (r *MPSCQueue[T]) mpscScanCount(
	before uint64,
) mpscCountObservation {
	released := mpscReleased(before)
	acquired := mpscFrontier(before) - released
	capacity := r.producerConfig.mask + 1
	observation := mpscCountObservation{result: uint64(min(acquired, capacity))}
	if acquired == capacity {
		return observation
	}
	head := r.producerReservation.head.Load()
	distance := uint32(head) - released
	if uint64(distance) > head {
		return observation
	}
	cursor := head - uint64(distance)
	limit := min(uint64(distance), uint64(capacity))
	if uint64(acquired) > limit {
		return observation
	}
	end := cursor + uint64(acquired)
	if uint64(acquired) < limit {
		observation.observed = r.observerCompleted.Load()
		if observation.observed >= end {
			if observation.observed-cursor <= limit {
				end = observation.observed
			}
		}
		for end-cursor < limit {
			recordEnd := r.completion.records[uint32(end)&r.producerConfig.mask].Load()
			if recordEnd <= end {
				break
			}
			if recordEnd-end > limit-(end-cursor) {
				break
			}
			end = recordEnd
		}
		observation.retain = true
	}
	observation.end = end
	observation.result = end - cursor
	return observation
}

func (r *MPSCQueue[T]) mpscValidateCount(
	before uint64,
	observation mpscCountObservation,
) (result int, retain bool) {
	after := r.consumerPublished.Load()
	if before != after {
		acquired := mpscFrontier(after) - mpscReleased(after)
		return int(min(acquired, r.producerConfig.mask+1)), false
	}
	return int(observation.result), observation.retain
}

func (r *MPSCQueue[T]) mpscPublishProof(observed, end uint64) {
	if end > observed {
		r.observerCompleted.CompareAndSwap(observed, end)
	}
}

// Free returns Capacity minus Count and may retain historical completion proof.
// Unpublished reservations can make the result exceed immediately reservable space.
func (r *MPSCQueue[T]) Free() int { return r.Capacity() - r.Count() }

// Full reports whether Count equals Capacity and may retain historical completion proof.
func (r *MPSCQueue[T]) Full() bool { return r.Count() == r.Capacity() }

// Empty reports whether Count is zero and may retain historical completion proof.
func (r *MPSCQueue[T]) Empty() bool { return r.Count() == 0 }

// Reset clears storage and all positions and proof while preserving configuration.
// Callers must join all active calls, including delayed proof publication, before Reset,
// and synchronize publication to future users.
func (r *MPSCQueue[T]) Reset() {
	r.mpscBase.Reset()
	r.observerCompleted.Store(0)
}
