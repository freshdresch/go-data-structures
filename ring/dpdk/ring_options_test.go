package dpdk

import (
	"errors"
	"math"
	"strings"
	"testing"
)

type ringConstructorCase struct {
	name          string
	multiProducer bool
	multiConsumer bool
	construct     func(*testing.T, int, ...Option) (ringTestQueue[int], error)
}

func ringConstructorCases() []ringConstructorCase {
	return []ringConstructorCase{
		{
			name: "SPSC", multiProducer: false, multiConsumer: false,
			construct: func(t *testing.T, capacity int, options ...Option) (ringTestQueue[int], error) {
				t.Helper()
				queue, err := NewSPSCQueue[int](capacity, options...)
				if err != nil {
					if queue != nil {
						t.Fatal("constructor returned nonnil queue on error", err)
					}
					return nil, err
				}
				return queue, nil
			},
		},
		{
			name: "MPSC", multiProducer: true, multiConsumer: false,
			construct: func(t *testing.T, capacity int, options ...Option) (ringTestQueue[int], error) {
				t.Helper()
				queue, err := NewMPSCQueue[int](capacity, options...)
				if err != nil {
					if queue != nil {
						t.Fatal("constructor returned nonnil queue on error", err)
					}
					return nil, err
				}
				return queue, nil
			},
		},
		{
			name: "SPMC", multiProducer: false, multiConsumer: true,
			construct: func(t *testing.T, capacity int, options ...Option) (ringTestQueue[int], error) {
				t.Helper()
				queue, err := NewSPMCQueue[int](capacity, options...)
				if err != nil {
					if queue != nil {
						t.Fatal("constructor returned nonnil queue on error", err)
					}
					return nil, err
				}
				return queue, nil
			},
		},
		{
			name: "MPMC", multiProducer: true, multiConsumer: true,
			construct: func(t *testing.T, capacity int, options ...Option) (ringTestQueue[int], error) {
				t.Helper()
				queue, err := NewMPMCQueue[int](capacity, options...)
				if err != nil {
					if queue != nil {
						t.Fatal("constructor returned nonnil queue on error", err)
					}
					return nil, err
				}
				return queue, nil
			},
		},
	}
}

type ringConstructorSuccessCase struct {
	name             string
	options          []Option
	enqueue, dequeue int
}

func ringConstructorSuccessCases() []ringConstructorSuccessCase {
	return []ringConstructorSuccessCase{
		{"defaults", nil, 8, 8},
		{"both", []Option{WithMaxBatch(4)}, 4, 4},
		{"invalid-overridden", []Option{WithMaxBatch(0), WithMaxBatch(4)}, 4, 4},
		{"directional-last", []Option{
			WithMaxBatch(4), WithMaxEnqueueBatch(2), WithMaxDequeueBatch(3),
		}, 2, 3},
		{"both-last", []Option{
			WithMaxEnqueueBatch(2), WithMaxDequeueBatch(3), WithMaxBatch(4),
		}, 4, 4},
		{"invalid-both-overridden", []Option{
			WithMaxBatch(0), WithMaxEnqueueBatch(2), WithMaxDequeueBatch(3),
		}, 2, 3},
		{"invalid-directions-overridden", []Option{
			WithMaxEnqueueBatch(-1), WithMaxDequeueBatch(9), WithMaxBatch(4),
		}, 4, 4},
	}
}

func TestRingConstructorValidation(t *testing.T) {
	for _, constructor := range ringConstructorCases() {
		t.Run(constructor.name, func(t *testing.T) {
			for _, count := range []int{-2, -1, 0, 3, 5, maxCount + 1, math.MaxInt} {
				called := false
				option := func(*config) { called = true }
				queue, err := constructor.construct(t, count, option)
				if queue != nil || !errors.Is(err, ErrInvalidCount) || err == ErrInvalidCount || called {
					t.Fatalf("count=%d queue=%v error=%v option called=%t", count, queue, err, called)
				}
			}
			for _, option := range []func(int) Option{
				WithMaxBatch, WithMaxEnqueueBatch, WithMaxDequeueBatch,
			} {
				for _, limit := range []int{-1, 0, 5, math.MaxInt} {
					queue, err := constructor.construct(t, 4, option(limit))
					if queue != nil || !errors.Is(err, ErrInvalidMaxBatch) || err == ErrInvalidMaxBatch {
						t.Fatalf("limit=%d queue=%v error=%v", limit, queue, err)
					}
				}
			}
			cases := ringConstructorSuccessCases()
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					queue, err := constructor.construct(t, 8, test.options...)
					if err != nil {
						t.Fatal(err)
					}
					if queue.MaxEnqueueBatch() != test.enqueue || queue.MaxDequeueBatch() != test.dequeue {
						t.Fatalf("batch limits=(%d,%d)", queue.MaxEnqueueBatch(), queue.MaxDequeueBatch())
					}
					if queue.Capacity() != 8 || queue.Count() != 0 || queue.Free() != 8 ||
						!queue.Empty() || queue.Full() {
						t.Fatal("initial capacity/count/free and flags")
					}
				})
			}
			for _, test := range []struct {
				name    string
				options []Option
				want    error
				context string
			}{
				{"enqueue-first", []Option{
					WithMaxEnqueueBatch(0), WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1),
				}, ErrInvalidMaxBatch, "max enqueue batch 0"},
				{"dequeue-second", []Option{
					WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1),
				}, ErrInvalidMaxBatch, "max dequeue batch 0"},
				{"spins-last", []Option{WithSpinsBeforeYield(-1)},
					ErrInvalidSpinsBeforeYield, "spins before yield -1"},
			} {
				t.Run(test.name, func(t *testing.T) {
					queue, err := constructor.construct(t, 4, test.options...)
					if queue != nil || !errors.Is(err, test.want) || err == test.want {
						t.Fatalf("queue=%v error=%v want wrapped %v", queue, err, test.want)
					}
					if !strings.Contains(err.Error(), test.context) {
						t.Fatalf("error=%q want context %q", err, test.context)
					}
				})
			}
			invalidSpins := []int{-1}
			if uint64(^uint(0)) > math.MaxUint32 {
				above := uint64(math.MaxInt32) + 1
				invalidSpins = append(invalidSpins, int(above))
			}
			for _, spins := range invalidSpins {
				queue, err := constructor.construct(t, 1, WithSpinsBeforeYield(spins))
				if queue != nil || !errors.Is(err, ErrInvalidSpinsBeforeYield) ||
					err == ErrInvalidSpinsBeforeYield {
					t.Fatalf("spins=%d queue=%v error=%v", spins, queue, err)
				}
			}

			queue, err := constructor.construct(t, 4)
			if err != nil {
				t.Fatal(err)
			}
			if queue.Capacity() != 4 || queue.Count() != 0 || queue.Free() != 4 ||
				!queue.Empty() || queue.Full() || queue.MaxEnqueueBatch() != 4 ||
				queue.MaxDequeueBatch() != 4 {
				t.Fatal("initial queries")
			}
			if !constructor.multiProducer && !constructor.multiConsumer {
				for _, spins := range []int{0, 1, defaultSpinsBeforeYield, math.MaxInt32} {
					queue, err := constructor.construct(t, 4, WithSpinsBeforeYield(spins))
					if queue != nil || !errors.Is(err, ErrUnsupportedOption) || err == ErrUnsupportedOption {
						t.Fatal("explicit SPSC spins", spins, queue, err)
					}
				}
			}
		})
	}
}

func TestRingMultiConstructorOptions(t *testing.T) {
	for _, constructor := range ringConstructorCases() {
		if !constructor.multiProducer || !constructor.multiConsumer {
			continue
		}
		t.Run(constructor.name, func(t *testing.T) {
			cases := []struct {
				options  []Option
				sentinel error
			}{
				{[]Option{WithMaxEnqueueBatch(0), WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1)},
					ErrInvalidMaxBatch},
				{[]Option{WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1)}, ErrInvalidMaxBatch},
				{[]Option{WithSpinsBeforeYield(-1)}, ErrInvalidSpinsBeforeYield},
				{[]Option{func(cfg *config) { cfg.prodMode = modeSingle }}, ErrUnsupportedOption},
				{[]Option{func(cfg *config) { cfg.consMode = modeSingle }}, ErrUnsupportedOption},
				{[]Option{WithMaxProducerHeadTailDistance(-1)}, ErrInvalidMaxHeadTailDistance},
				{[]Option{WithMaxConsumerHeadTailDistance(-1)}, ErrInvalidMaxHeadTailDistance},
				{[]Option{WithMaxProducerHeadTailDistance(0)}, ErrHeadTailDistanceRequiresRelaxed},
				{[]Option{WithMaxConsumerHeadTailDistance(0)}, ErrHeadTailDistanceRequiresRelaxed},
				{[]Option{WithRelaxedMultiProdEnqueue(), WithMultiProdEnqueue(),
					WithMaxProducerHeadTailDistance(1)}, ErrHeadTailDistanceRequiresRelaxed},
				{[]Option{WithRelaxedMultiConsDequeue(), WithMultiConsDequeue(),
					WithMaxConsumerHeadTailDistance(1)}, ErrHeadTailDistanceRequiresRelaxed},
			}
			for index, test := range cases {
				queue, err := constructor.construct(t, 8, test.options...)
				if queue != nil || !errors.Is(err, test.sentinel) || err == test.sentinel {
					t.Fatal("constructor error", index, queue, err, test.sentinel)
				}
			}

		})
	}
}
