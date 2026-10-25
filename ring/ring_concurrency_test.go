package ring

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const ringConcurrentTimeout = 10 * time.Second

type ringConcurrentTag struct {
	producer, sequence int
}

type ringConcurrentScenario struct {
	producerCount, consumerCount int
	firstProduced, firstExpected int
	lastSequence                 int
	method                       string
	ownership                    <-chan ringTestQueue[ringConcurrentTag]
}

func TestRingConcurrentTransfersAndObservers(t *testing.T) {
	for _, testCase := range ringTestCases[ringConcurrentTag]() {
		t.Run(testCase.name, func(t *testing.T) {
			producerCount, lastSequence := 1, 384
			consumerCounts := []int{1}
			if testCase.multiProducer {
				producerCount, lastSequence = 4, 96
			}
			if testCase.multiConsumer {
				consumerCounts = []int{4}
			}
			if testCase.multiProducer && testCase.multiConsumer {
				lastSequence, consumerCounts = 48, []int{1, 4}
			}
			for _, capacity := range []int{1, 2, 8, 64} {
				for _, exact := range []bool{false, true} {
					for _, retain := range []bool{false, true} {
						for _, consumers := range consumerCounts {
							for _, method := range []string{"single", "bulk", "burst", "mixed"} {
								name := fmt.Sprintf(
									"capacity=%d/exact=%t/retain=%t/consumers=%d/%s",
									capacity, exact, retain, consumers, method,
								)
								t.Run(name, func(t *testing.T) {
									options := ringBehaviorOptions(exact, retain)
									if testCase.multiProducer || testCase.multiConsumer {
										options = append(options, WithSpinsBeforeYield(0))
									}
									if testCase.multiProducer && testCase.multiConsumer {
										var selected config
										for _, option := range testCase.protocolOptions {
											option(&selected)
										}
										if selected.prodMode == modeRelaxed {
											options = append(options, WithMaxProducerHeadTailDistance(capacity))
										}
										if selected.consMode == modeRelaxed {
											options = append(options, WithMaxConsumerHeadTailDistance(capacity))
										}
									}
									fixture := testCase.newFixture(t, capacity, options...)
									fixture.seed(math.MaxUint32-3, math.MaxUint32-2)
									checkRingConcurrentTransfers(
										t,
										fixture.queue,
										ringConcurrentScenario{
											producerCount: producerCount, consumerCount: consumers,
											firstProduced: 1, firstExpected: 1, lastSequence: lastSequence,
											method: method,
										},
									)
								})
							}
						}
					}
				}
			}
		})
	}
}

func ringConcurrentMethod(method string, iteration int) string {
	if method != "mixed" {
		return method
	}
	return []string{"single", "bulk", "burst"}[iteration%3]
}

func ringConcurrentEnqueue(
	queue ringTestQueue[ringConcurrentTag],
	items []ringConcurrentTag,
	method string,
) (int, error) {
	moved, free := 0, 0
	switch method {
	case "single":
		err := queue.Enqueue(items[0])
		if err != nil && err != ErrFull {
			return 0, fmt.Errorf("single enqueue: %w", err)
		}
		if err == nil {
			moved = 1
		}
	case "bulk":
		var err error
		free, err = queue.EnqueueBulk(items)
		if err != nil && err != ErrFull {
			return 0, fmt.Errorf("bulk enqueue: %w", err)
		}
		if err == nil {
			moved = len(items)
		}
	case "burst":
		moved, free = queue.EnqueueBurst(items)
	}
	if moved < 0 || moved > len(items) || free < 0 || free > queue.Capacity() {
		return 0, fmt.Errorf("enqueue moved=%d free=%d", moved, free)
	}
	return moved, nil
}

func ringConcurrentDequeue(
	queue ringTestQueue[ringConcurrentTag],
	destination []ringConcurrentTag,
	method string,
) (int, error) {
	moved, available := 0, 0
	switch method {
	case "single":
		value, err := queue.Dequeue()
		if err != nil && err != ErrEmpty {
			return 0, fmt.Errorf("single dequeue: %w", err)
		}
		if err == nil {
			destination[0], moved = value, 1
		}
	case "bulk":
		var err error
		available, err = queue.DequeueBulk(destination)
		if err != nil && err != ErrEmpty {
			return 0, fmt.Errorf("bulk dequeue: %w", err)
		}
		if err == nil {
			moved = len(destination)
		}
	case "burst":
		moved, available = queue.DequeueBurst(destination)
	}
	if moved < 0 || moved > min(len(destination), queue.Capacity()) {
		return 0, fmt.Errorf("dequeue moved=%d", moved)
	}
	if available < 0 || available > queue.Capacity() {
		return 0, fmt.Errorf("dequeue available=%d", available)
	}
	for _, tag := range destination[moved:] {
		if tag != (ringConcurrentTag{-1, -1}) {
			return 0, fmt.Errorf("dequeue changed suffix: %v", destination)
		}
	}
	return moved, nil
}

func checkRingConcurrentTransfers(
	t *testing.T,
	queue ringTestQueue[ringConcurrentTag],
	scenario ringConcurrentScenario,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), ringConcurrentTimeout)
	defer cancel()
	capacity := queue.Capacity()
	request := min(3, capacity)
	total := scenario.producerCount * (scenario.lastSequence - scenario.firstExpected + 1)
	workerErrors := make(chan error, scenario.producerCount+scenario.consumerCount+1)
	report := func(err error) {
		workerErrors <- err
		cancel()
	}
	var delivered atomic.Int64
	producerIntervals := make([][][]ringConcurrentTag, scenario.producerCount)
	consumerIntervals := make([][][]ringConcurrentTag, scenario.consumerCount)
	firstPublication, observed := make(chan struct{}), make(chan struct{})
	var workers sync.WaitGroup
	for producer := range scenario.producerCount {
		workers.Go(func() {
			producerQueue := queue
			if scenario.ownership != nil {
				select {
				case producerQueue = <-scenario.ownership:
				case <-ctx.Done():
					return
				}
			}
			next := scenario.firstProduced
			for iteration := 0; next <= scenario.lastSequence && ctx.Err() == nil; iteration++ {
				length := min(request, scenario.lastSequence-next+1)
				items := make([]ringConcurrentTag, length)
				for index := range items {
					items[index] = ringConcurrentTag{producer, next + index}
				}
				method := ringConcurrentMethod(scenario.method, iteration)
				moved, err := ringConcurrentEnqueue(producerQueue, items, method)
				if err != nil {
					report(err)
					return
				}
				if moved > 0 {
					producerIntervals[producer] = append(producerIntervals[producer], items[:moved])
					if producer == 0 && next == scenario.firstProduced {
						close(firstPublication)
						select {
						case <-observed:
						case <-ctx.Done():
							return
						}
					}
				}
				next += moved
				runtime.Gosched()
			}
		})
	}
	for consumer := range scenario.consumerCount {
		workers.Go(func() {
			for iteration := 0; delivered.Load() < int64(total) && ctx.Err() == nil; iteration++ {
				method := ringConcurrentMethod(scenario.method, iteration)
				length := request
				if scenario.consumerCount == 1 {
					length = min(length, total-int(delivered.Load()))
				}
				if method == "burst" {
					length += 2
				}
				destination := make([]ringConcurrentTag, length)
				for index := range destination {
					destination[index] = ringConcurrentTag{-1, -1}
				}
				moved, err := ringConcurrentDequeue(queue, destination, method)
				if err != nil {
					report(err)
					return
				}
				if moved > 0 {
					consumerIntervals[consumer] = append(consumerIntervals[consumer], destination[:moved])
					delivered.Add(int64(moved))
				}
				runtime.Gosched()
			}
		})
	}
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		sampled := false
		for ctx.Err() == nil {
			count, free := queue.Count(), queue.Free()
			if count < 0 || count > capacity || free < 0 || free > capacity {
				report(fmt.Errorf("observer count=%d free=%d", count, free))
				return
			}
			queue.Full()
			queue.Empty()
			if queue.Capacity() != capacity || queue.MaxEnqueueBatch() != capacity {
				report(fmt.Errorf("immutable producer query changed"))
				return
			}
			if queue.MaxDequeueBatch() != capacity {
				report(fmt.Errorf("immutable consumer query changed"))
				return
			}
			if !sampled {
				select {
				case <-firstPublication:
					// Producer zero waits until this sample overlaps its transfer.
					queue.Count()
					close(observed)
					sampled = true
				default:
				}
			}
			runtime.Gosched()
		}
	}()
	joined := make(chan struct{})
	go func() {
		workers.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Error("concurrent transfers exceeded deadline or were canceled")
	}
	cancel()
	awaitRingConcurrentSignal(t, joined)
	awaitRingConcurrentSignal(t, observerDone)
	close(workerErrors)
	for err := range workerErrors {
		t.Error(err)
	}
	select {
	case <-observed:
	default:
		t.Error("observer did not sample during producer transfer")
	}
	checkRingConcurrentIntervals(t, scenario, producerIntervals, consumerIntervals)
	if delivered.Load() != int64(total) {
		t.Errorf("delivered=%d, want %d", delivered.Load(), total)
	}
	if queue.Count() != 0 || queue.Free() != capacity || !queue.Empty() || queue.Full() {
		t.Error("joined queue did not finish empty with all capacity free")
	}
}

func awaitRingConcurrentSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(ringConcurrentTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal("concurrent worker did not join")
	}
}

func checkRingConcurrentIntervals(
	t *testing.T,
	scenario ringConcurrentScenario,
	producerIntervals, consumerIntervals [][][]ringConcurrentTag,
) {
	t.Helper()
	seen := make(map[ringConcurrentTag]int)
	positions := make(map[ringConcurrentTag]int)
	nextSequence := make([]int, scenario.producerCount)
	for producer := range nextSequence {
		nextSequence[producer] = scenario.firstExpected
	}
	position := 0
	for _, intervals := range consumerIntervals {
		for _, interval := range intervals {
			for index, tag := range interval {
				if tag.producer < 0 || tag.producer >= scenario.producerCount {
					t.Fatalf("producer outside expected range: %v", tag)
				}
				if tag.sequence < scenario.firstExpected || tag.sequence > scenario.lastSequence {
					t.Fatalf("sequence outside expected range: %v", tag)
				}
				for _, earlier := range interval[:index] {
					if earlier.producer == tag.producer && earlier.sequence >= tag.sequence {
						t.Fatalf("batch producer order: %v", interval)
					}
				}
				if scenario.producerCount == 1 && index > 0 {
					if tag.sequence != interval[index-1].sequence+1 {
						t.Fatalf("nonconsecutive consumer interval: %v", interval)
					}
				}
				if scenario.consumerCount == 1 {
					if tag.sequence != nextSequence[tag.producer] {
						t.Fatalf("producer FIFO mismatch: %v, want %d", tag, nextSequence[tag.producer])
					}
					nextSequence[tag.producer]++
				}
				seen[tag]++
				positions[tag] = position
				position++
			}
		}
	}
	for producer := range scenario.producerCount {
		for sequence := scenario.firstExpected; sequence <= scenario.lastSequence; sequence++ {
			tag := ringConcurrentTag{producer, sequence}
			if seen[tag] != 1 {
				t.Errorf("tag %v occurred %d times", tag, seen[tag])
			}
		}
	}
	if scenario.consumerCount != 1 {
		return
	}
	// Completion order across consumers need not match reservation order.
	for _, intervals := range producerIntervals {
		for _, interval := range intervals {
			for index := 1; index < len(interval); index++ {
				if positions[interval[index]] != positions[interval[0]]+index {
					t.Fatalf("producer interval was not contiguous: %v", interval)
				}
			}
		}
	}
}
