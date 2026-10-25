package ring

import (
	"slices"
	"testing"
)

type ringTestQueue[T any] interface {
	Enqueue(T) error
	Dequeue() (T, error)
	EnqueueBulk([]T) (int, error)
	DequeueBulk([]T) (int, error)
	EnqueueBurst([]T) (int, int)
	DequeueBurst([]T) (int, int)
	Capacity() int
	MaxEnqueueBatch() int
	MaxDequeueBatch() int
	Count() int
	Free() int
	Full() bool
	Empty() bool
	Reset()
}

type ringTestFixture[T any] struct {
	queue   ringTestQueue[T]
	entries []T
	seed    func(position, counter uint32)
}

type ringTestCase[T any] struct {
	name            string
	multiProducer   bool
	multiConsumer   bool
	protocolOptions []Option
	newFixture      func(*testing.T, int, ...Option) ringTestFixture[T]
}

func ringTestCases[T any]() []ringTestCase[T] {
	cases := []ringTestCase[T]{
		{
			name:            "SPSC/producer=single/consumer=single",
			protocolOptions: nil,
			newFixture: func(t *testing.T, capacity int, options ...Option) ringTestFixture[T] {
				queue := newSPSCForTest[T](t, capacity, options...)
				return ringTestFixture[T]{queue, queue.producer.entries, func(position, _ uint32) {
					seedSPSC(queue, position)
				}}
			},
		},
	}
	for _, relaxed := range []bool{false, true} {
		producerProtocol, consumerProtocol := "ordered", "ordered"
		if relaxed {
			producerProtocol, consumerProtocol = "RTS", "RTS"
		}
		producerOptions := []Option{WithMultiProdEnqueue()}
		consumerOptions := []Option{WithMultiConsDequeue()}
		if relaxed {
			producerOptions = append(producerOptions, WithRelaxedMultiProdEnqueue())
			consumerOptions = append(consumerOptions, WithRelaxedMultiConsDequeue())
		}
		cases = append(cases, ringTestCase[T]{
			name:            "MPSC/producer=" + producerProtocol + "/consumer=single",
			multiProducer:   true,
			protocolOptions: producerOptions,
			newFixture: func(t *testing.T, capacity int, options ...Option) ringTestFixture[T] {
				queue := newMPSCProtocolForTest[T](t, relaxed, capacity, options...)
				return ringTestFixture[T]{queue, queue.producerConfig.entries, func(position, _ uint32) {
					seedMPSC(queue, position)
				}}
			},
		}, ringTestCase[T]{
			name:            "SPMC/producer=single/consumer=" + consumerProtocol,
			multiConsumer:   true,
			protocolOptions: consumerOptions,
			newFixture: func(t *testing.T, capacity int, options ...Option) ringTestFixture[T] {
				protocolOptions := spmcProtocolOptions(relaxed, slices.Clone(options))
				queue := newSPMCForTest[T](t, capacity, protocolOptions...)
				return ringTestFixture[T]{queue, queue.producer.entries, func(position, _ uint32) {
					seedSPMC(queue, position)
				}}
			},
		})
	}
	for mode := 0; mode < 4; mode++ {
		producerProtocol, consumerProtocol := "ordered", "ordered"
		if mode&1 != 0 {
			producerProtocol = "RTS"
		}
		if mode&2 != 0 {
			consumerProtocol = "RTS"
		}
		cases = append(cases, ringTestCase[T]{
			name:            "MPMC/producer=" + producerProtocol + "/consumer=" + consumerProtocol,
			multiProducer:   true,
			multiConsumer:   true,
			protocolOptions: mpmcTestOptions(mode, nil),
			newFixture: func(t *testing.T, capacity int, options ...Option) ringTestFixture[T] {
				protocolOptions := mpmcTestOptions(mode, slices.Clone(options))
				queue := mpmcNewForTest[T](t, capacity, protocolOptions...)
				return ringTestFixture[T]{queue, queue.config.entries, func(position, counter uint32) {
					mpmcSeed(queue, position, counter)
				}}
			},
		})
	}
	return cases
}

func ringBehaviorOptions(exact, retain bool) []Option {
	var options []Option
	if exact {
		options = append(options, WithExactBatchCounts())
	}
	if retain {
		options = append(options, WithoutDequeueClearing())
	}
	return options
}
