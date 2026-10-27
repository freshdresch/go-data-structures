package frontier

import (
	"fmt"

	"github.com/freshdresch/go-data-structures/ring/internal/shared"
)

type config struct {
	maxEnqueueBatch  int
	maxDequeueBatch  int
	keepDequeued     bool
	exactBatchCounts bool
}

// Option configures a queue at construction. Options apply in argument order, and constructors
// validate only the final values, after checking the count.
type Option func(*config)

// WithMaxEnqueueBatch returns an Option that limits every enqueue batch call to at most n
// elements. EnqueueBurst moves at most n, and EnqueueBulk rejects a longer request with
// ErrExceedsMaxBatch. Enqueue moves one element, so the limit never affects it. The default is
// the capacity. Constructors return an error wrapping ErrInvalidMaxBatch when the final limit is
// below 1 or above the count.
//
// A producer's batch becomes visible only when its whole copy finishes, and the consumer cannot
// see later reservations past it until then. The limit bounds that delay.
func WithMaxEnqueueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
	}
}

// WithMaxDequeueBatch returns an Option that limits every dequeue batch call to at most n
// elements. DequeueBurst moves at most n, and DequeueBulk rejects a longer request with
// ErrExceedsMaxBatch. Dequeue moves one element, so the limit never affects it. The default is
// the capacity. Constructors return an error wrapping ErrInvalidMaxBatch when the final limit is
// below 1 or above the count.
//
// A dequeue releases its slots for reuse only when its whole copy and clearing finish. The limit
// bounds that delay.
func WithMaxDequeueBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxDequeueBatch = n
	}
}

// WithMaxBatch returns an Option that sets both batch limits to n. A later WithMaxEnqueueBatch or
// WithMaxDequeueBatch overrides it for that side.
func WithMaxBatch(n int) Option {
	return func(cfg *config) {
		cfg.maxEnqueueBatch = n
		cfg.maxDequeueBatch = n
	}
}

// WithoutDequeueClearing returns an Option that makes the dequeue methods leave the slots they
// read as they are, instead of clearing them.
//
// With pointer elements, up to Capacity stale elements then stay reachable until their slots are
// reused, which may be never on a queue that drains and goes quiet. The cost of clearing shows
// mostly when producers keep the ring full: the consumer then sets the transfer rate, and
// clearing adds a store to every consumed slot. While the ring stays full, producers overwrite
// drained slots almost immediately, so little is retained in that state.
//
// Reset clears every slot whether or not this option is given.
func WithoutDequeueClearing() Option {
	return func(cfg *config) {
		cfg.keepDequeued = true
	}
}

// WithExactBatchCounts returns an Option that makes EnqueueBulk, EnqueueBurst, DequeueBulk, and
// DequeueBurst return fresh counts on every call.
//
// By default a batch call checks its request against the other side's last discovered progress,
// which is a cached copy or, in MPMCQueue, the shared frontier. It refreshes that progress only
// when the request does not fit. A call never fails or moves fewer elements because of a stale
// count, but its returned free or available count can be stale and low, so counts must not drive
// blocking or wakeup decisions. A call that returned ErrFull or ErrEmpty, a Burst that moved less
// than its clamped request, and a zero-length call always return a fresh count under either
// setting.
func WithExactBatchCounts() Option {
	return func(cfg *config) {
		cfg.exactBatchCounts = true
	}
}

// resolveConfig validates count, applies options in order, then validates the final limits.
func resolveConfig(constructor string, count int, options []Option) (config, error) {
	if err := shared.CheckCapacity(count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	cfg := config{maxEnqueueBatch: count, maxDequeueBatch: count}
	for _, option := range options {
		option(&cfg)
	}
	if err := shared.CheckMaxBatch("enqueue", cfg.maxEnqueueBatch, count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	if err := shared.CheckMaxBatch("dequeue", cfg.maxDequeueBatch, count); err != nil {
		return config{}, fmt.Errorf("%s: %w", constructor, err)
	}
	return cfg, nil
}
