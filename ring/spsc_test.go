package ring

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func newSPSCForTest[T any](t *testing.T, capacity int, options ...Option) *SPSCQueue[T] {
	t.Helper()
	queue, err := NewSPSCQueue[T](capacity, options...)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func TestSPSCConstruction(t *testing.T) {
	for _, test := range ringConstructorSuccessCases() {
		t.Run(test.name, func(t *testing.T) {
			queue := newSPSCForTest[int](t, 8, test.options...)

			if !queue.consumer.clearOnDequeue {
				t.Fatal("initial flags")
			}
			if queue.producer.exactBatchCounts || queue.consumer.exactBatchCounts {
				t.Fatal("default exact counts")
			}
			if &queue.producer.entries[0] != &queue.consumer.entries[0] {
				t.Fatal("entries do not share storage")
			}
		})
	}
}

func seedSPSC[T any](queue *SPSCQueue[T], position uint32) {
	queue.producer.cursor = position
	queue.producer.cachedConsumer = position
	queue.producer.published.Store(position)
	queue.consumer.cursor = position
	queue.consumer.cachedProducer = position
	queue.consumer.published.Store(position)
}

func TestSPSCPositionAndPhysicalWrap(t *testing.T) {
	for _, position := range []uint32{3, math.MaxUint32 - 2} {
		queue := newSPSCForTest[int](t, 4)
		seedSPSC(queue, position)
		if err := queue.Enqueue(10); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.EnqueueBulk([]int{11, 12}); err != nil {
			t.Fatal(err)
		}
		if moved, _ := queue.EnqueueBurst([]int{13, 99}); moved != 1 {
			t.Fatalf("partial enqueue moved=%d", moved)
		}
		value, err := queue.Dequeue()
		if err != nil || value != 10 {
			t.Fatalf("single value=%d error=%v", value, err)
		}
		dst := []int{-1, -1}
		if _, err := queue.DequeueBulk(dst); err != nil || !slices.Equal(dst, []int{11, 12}) {
			t.Fatalf("bulk dst=%v error=%v", dst, err)
		}
		dst = []int{-1, -1, -1}
		if moved, left := queue.DequeueBurst(dst); moved != 1 || left != 0 {
			t.Fatalf("partial dequeue moved=%d left=%d", moved, left)
		}
		if !slices.Equal(dst, []int{13, -1, -1}) {
			t.Fatalf("burst dst=%v", dst)
		}
		if queue.producer.cursor != position+4 || queue.consumer.cursor != position+4 {
			t.Fatal("cursor wrap")
		}
		if queue.producer.published.Load() != position+4 ||
			queue.consumer.published.Load() != position+4 {
			t.Fatal("publication wrap")
		}
	}
}

func TestSPSCFailedBatchKeepsProducerPosition(t *testing.T) {
	queue := newSPSCForTest[int](t, 4, WithMaxBatch(2))
	for _, item := range []int{1, 2, 3, 4} {
		if err := queue.Enqueue(item); err != nil {
			t.Fatal(err)
		}
	}
	for _, length := range []int{5, 3, 2} {
		_, err := queue.EnqueueBulk(make([]int, length))
		if err == nil {
			t.Fatal("expected failed enqueue")
		}
	}
	if queue.producer.cursor != 4 || queue.producer.published.Load() != 4 {
		t.Fatal("failed enqueue moved producer")
	}
}

func TestSPSCBatchCounts(t *testing.T) {
	for _, exact := range []bool{false, true} {
		for _, burst := range []bool{false, true} {
			t.Run(fmt.Sprintf("exact=%t/burst=%t", exact, burst), func(t *testing.T) {
				options := []Option{WithMaxBatch(2)}
				if exact {
					options = append(options, WithExactBatchCounts())
				}
				queue := newSPSCForTest[int](t, 8, options...)
				enqueue := func(items []int) (int, error) {
					if burst {
						_, free := queue.EnqueueBurst(items)
						return free, nil
					}
					return queue.EnqueueBulk(items)
				}
				dequeue := func(dst []int) (int, error) {
					if burst {
						_, available := queue.DequeueBurst(dst)
						return available, nil
					}
					return queue.DequeueBulk(dst)
				}
				for value := range 4 {
					if err := queue.Enqueue(value); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := dequeue(make([]int, 2)); err != nil {
					t.Fatal(err)
				}
				// Consumer released room while the producer's cache still names position zero.
				want := 2
				if exact {
					want = 4
				}
				if free, err := enqueue([]int{4, 5}); err != nil || free != want {
					t.Fatalf("cached free=%d want=%d error=%v", free, want, err)
				}
				// Producer added data beyond the consumer's previously acquired snapshot.
				want = 1
				if exact {
					want = 3
				}
				if left, err := dequeue(make([]int, 1)); err != nil || left != want {
					t.Fatalf("cached available=%d want=%d error=%v", left, want, err)
				}
				producerCache := queue.producer.cachedConsumer
				consumerCache := queue.consumer.cachedProducer
				free, err := queue.EnqueueBulk(make([]int, 9))
				if free != 5 || !errors.Is(err, ErrExceedsCapacity) {
					t.Fatalf("invalid free=%d error=%v", free, err)
				}
				left, err := queue.DequeueBulk(make([]int, 3))
				if left != 3 || !errors.Is(err, ErrExceedsMaxBatch) {
					t.Fatalf("invalid available=%d error=%v", left, err)
				}
				if queue.producer.cachedConsumer != producerCache ||
					queue.consumer.cachedProducer != consumerCache {
					t.Fatal("invalid request changed cache")
				}
				producerPublished := queue.producer.published.Load()
				consumerPublished := queue.consumer.published.Load()
				if free, err := enqueue(nil); err != nil || free != 5 {
					t.Fatalf("zero free=%d error=%v", free, err)
				}
				if left, err := dequeue(nil); err != nil || left != 3 {
					t.Fatalf("zero available=%d error=%v", left, err)
				}
				if queue.producer.cachedConsumer != 3 || queue.consumer.cachedProducer != 6 {
					t.Fatal("zero request did not refresh caches")
				}
				if queue.producer.published.Load() != producerPublished ||
					queue.consumer.published.Load() != consumerPublished {
					t.Fatal("zero request published")
				}
			})
		}
	}
}

func TestSPSCReloadBeforePartial(t *testing.T) {
	for _, exact := range []bool{false, true} {
		var options []Option
		if exact {
			options = append(options, WithExactBatchCounts())
		}
		queue := newSPSCForTest[int](t, 4, options...)
		if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
			t.Fatal(err)
		}
		if moved, free := queue.EnqueueBurst([]int{5, 6, 7}); moved != 2 || free != 0 {
			t.Fatalf("partial enqueue moved=%d free=%d", moved, free)
		}
		if queue.producer.cachedConsumer != 2 {
			t.Fatal("partial enqueue failed to refresh")
		}
		if moved, free := queue.EnqueueBurst([]int{99}); moved != 0 || free != 0 {
			t.Fatalf("full burst moved=%d free=%d", moved, free)
		}
		if queue.producer.cursor != 6 || queue.producer.published.Load() != 6 {
			t.Fatal("full burst changed position")
		}
		dst := make([]int, 4)
		if moved, left := queue.DequeueBurst(dst); moved != 4 || left != 0 {
			t.Fatalf("refresh dequeue moved=%d left=%d", moved, left)
		}
		if !slices.Equal(dst, []int{3, 4, 5, 6}) || queue.consumer.cachedProducer != 6 {
			t.Fatalf("refresh dst=%v cache=%d", dst, queue.consumer.cachedProducer)
		}
		if free, err := queue.EnqueueBulk([]int{7, 8, 9, 10}); err != nil || free != 0 {
			t.Fatalf("refresh full bulk free=%d error=%v", free, err)
		}
		if free, err := queue.EnqueueBulk([]int{11}); err != ErrFull || free != 0 {
			t.Fatalf("failed bulk free=%d error=%v", free, err)
		}
		if _, err := queue.DequeueBulk(dst); err != nil {
			t.Fatal(err)
		}
		if left, err := queue.DequeueBulk(dst); err != ErrEmpty || left != 0 {
			t.Fatalf("failed bulk available=%d error=%v", left, err)
		}
		if moved, left := queue.DequeueBurst(dst); moved != 0 || left != 0 {
			t.Fatalf("empty burst moved=%d available=%d", moved, left)
		}
		if err := queue.Enqueue(11); err != nil {
			t.Fatal(err)
		}
		dst = []int{-1, -1, -1, -1}
		if moved, left := queue.DequeueBurst(dst); moved != 1 || left != 0 {
			t.Fatalf("partial dequeue moved=%d available=%d", moved, left)
		}
		if !slices.Equal(dst, []int{11, -1, -1, -1}) {
			t.Fatalf("partial dequeue destination=%v", dst)
		}
	}
}

func TestSPSCPointerClearingAndReset(t *testing.T) {
	for _, keep := range []bool{false, true} {
		options := []Option{WithMaxBatch(3), WithExactBatchCounts()}
		if keep {
			options = append(options, WithoutDequeueClearing())
		}
		queue := newSPSCForTest[*int](t, 4, options...)
		seedSPSC(queue, math.MaxUint32-1)
		value := 42
		for index := range queue.producer.entries {
			queue.producer.entries[index] = &value
		}
		backing := &queue.producer.entries[0]
		queue.Reset()
		if !queue.Empty() || queue.Free() != 4 || queue.producer.cursor != 0 ||
			queue.consumer.cursor != 0 || queue.producer.cachedConsumer != 0 ||
			queue.consumer.cachedProducer != 0 || queue.producer.published.Load() != 0 ||
			queue.consumer.published.Load() != 0 {
			t.Fatal("reset positions")
		}
		if queue.MaxEnqueueBatch() != 3 || queue.MaxDequeueBatch() != 3 ||
			queue.consumer.clearOnDequeue == keep || !queue.producer.exactBatchCounts ||
			!queue.consumer.exactBatchCounts || queue.producer.mask != 3 ||
			queue.consumer.mask != 3 || &queue.producer.entries[0] != backing ||
			&queue.consumer.entries[0] != backing {
			t.Fatal("reset changed configuration or storage")
		}
		for _, value := range queue.producer.entries {
			if value != nil {
				t.Fatal("reset retained pointer")
			}
		}
	}
}

func TestSPSCConcurrentTransfersAndHandoffs(t *testing.T) {
	for _, capacity := range []int{1, 2, 4, 8} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/exact=%t", capacity, exact), func(t *testing.T) {
				runSPSCTransfers(t, capacity, exact)
			})
		}
	}
}

func runSPSCTransfers(t *testing.T, capacity int, exact bool) {
	t.Helper()
	var options []Option
	if exact {
		options = append(options, WithExactBatchCounts())
	}
	queue := newSPSCForTest[int](t, capacity, options...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const total = 3000
	errorsFound := make(chan error, 4)
	producerHandoff := make(chan int, 1)
	consumerHandoff := make(chan int, 1)
	var workers sync.WaitGroup
	producer := func(start, end int, handoff chan<- int) {
		defer workers.Done()
		next := start
		for next < end {
			if ctx.Err() != nil {
				return
			}
			var moved int
			switch next % 3 {
			case 0:
				err := queue.Enqueue(next)
				if err == nil {
					moved = 1
				} else if err != ErrFull {
					errorsFound <- err
					return
				}
			case 1:
				length := min(2, capacity, end-next)
				items := [2]int{next, next + 1}
				_, err := queue.EnqueueBulk(items[:length])
				if err == nil {
					moved = length
				} else if err != ErrFull {
					errorsFound <- err
					return
				}
			case 2:
				items := [2]int{next, next + 1}
				moved, _ = queue.EnqueueBurst(items[:min(2, end-next)])
			}
			next += moved
			if moved == 0 {
				runtime.Gosched()
			}
		}
		// These calls refresh ordinary caches immediately before a synchronized handoff.
		_, err := queue.EnqueueBulk(make([]int, capacity+1))
		if !errors.Is(err, ErrExceedsCapacity) {
			errorsFound <- fmt.Errorf("handoff invalid enqueue: %v", err)
			return
		}
		queue.EnqueueBurst(nil)
		if handoff != nil {
			handoff <- next
		}
	}
	consumer := func(start, end int, handoff chan<- int) {
		defer workers.Done()
		next := start
		for next < end {
			if ctx.Err() != nil {
				return
			}
			var dst [2]int
			var moved int
			switch next % 3 {
			case 0:
				value, err := queue.Dequeue()
				if err == nil {
					dst[0] = value
					moved = 1
				} else if err != ErrEmpty {
					errorsFound <- err
					return
				}
			case 1:
				length := min(2, capacity, end-next)
				_, err := queue.DequeueBulk(dst[:length])
				if err == nil {
					moved = length
				} else if err != ErrEmpty {
					errorsFound <- err
					return
				}
			case 2:
				moved, _ = queue.DequeueBurst(dst[:min(2, end-next)])
			}
			for index := range moved {
				if dst[index] != next+index {
					errorsFound <- fmt.Errorf("FIFO got %d want %d", dst[index], next+index)
					return
				}
			}
			next += moved
			if moved == 0 {
				runtime.Gosched()
			}
		}
		_, err := queue.DequeueBulk(make([]int, capacity+1))
		if !errors.Is(err, ErrExceedsCapacity) {
			errorsFound <- fmt.Errorf("handoff invalid dequeue: %v", err)
			return
		}
		queue.DequeueBurst(nil)
		if handoff != nil {
			handoff <- next
		}
	}
	workers.Add(4)
	go producer(0, total/3, producerHandoff)
	go consumer(0, total/2, consumerHandoff)
	go func() {
		select {
		case next := <-producerHandoff:
			producer(next, total, nil)
		case <-ctx.Done():
			workers.Done()
		}
	}()
	go func() {
		select {
		case next := <-consumerHandoff:
			consumer(next, total, nil)
		case <-ctx.Done():
			workers.Done()
		}
	}()
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		for ctx.Err() == nil {
			count := queue.Count()
			free := queue.Free()
			queue.Full()
			queue.Empty()
			if count < 0 || count > capacity || free < 0 || free > capacity {
				errorsFound <- fmt.Errorf("observer count=%d free=%d", count, free)
				return
			}
			runtime.Gosched()
		}
	}()
	workers.Wait()
	timedOut := ctx.Err()
	cancel()
	<-observerDone
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if timedOut != nil {
		t.Fatal(timedOut)
	}
	if queue.Count() != 0 || queue.Free() != capacity {
		t.Fatal("final count")
	}
}

func TestSPSCFullClampedBurstCachedCounts(t *testing.T) {
	queue := newSPSCForTest[int](t, 8, WithMaxBatch(2))
	for value := range 4 {
		if err := queue.Enqueue(value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
		t.Fatal(err)
	}
	if moved, free := queue.EnqueueBurst([]int{4, 5, 6}); moved != 2 || free != 2 {
		t.Fatalf("clamped cached enqueue moved=%d free=%d", moved, free)
	}
	if queue.Free() != 4 || queue.producer.cachedConsumer != 0 {
		t.Fatal("enqueue did not retain conservative snapshot")
	}
	dst := []int{-1, -1, -1}
	if moved, left := queue.DequeueBurst(dst); moved != 2 || left != 0 {
		t.Fatalf("clamped cached dequeue moved=%d available=%d", moved, left)
	}
	if queue.Count() != 2 || queue.consumer.cachedProducer != 4 ||
		!slices.Equal(dst, []int{2, 3, -1}) {
		t.Fatal("dequeue did not retain conservative snapshot or destination suffix")
	}
}

func TestSPSCHandoffAfterFailedAdmission(t *testing.T) {
	for _, producerSide := range []bool{false, true} {
		t.Run(fmt.Sprintf("producer=%t", producerSide), func(t *testing.T) {
			queue := newSPSCForTest[int](t, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if producerSide {
				if _, err := queue.EnqueueBulk([]int{1, 2}); err != nil {
					t.Fatal(err)
				}
			}
			handoff := make(chan struct{})
			results := make(chan error, 2)
			var workers sync.WaitGroup
			workers.Add(2)
			go func() {
				defer workers.Done()
				defer close(handoff)
				if producerSide {
					if err := queue.Enqueue(3); err != ErrFull {
						results <- fmt.Errorf("full handoff: %v", err)
						return
					}
					queue.EnqueueBurst(nil)
					return
				}
				if _, err := queue.Dequeue(); err != ErrEmpty {
					results <- fmt.Errorf("empty handoff: %v", err)
					return
				}
				queue.DequeueBurst(nil)
			}()
			go func() {
				defer workers.Done()
				select {
				case <-handoff:
				case <-ctx.Done():
					return
				}
				for ctx.Err() == nil {
					if producerSide {
						err := queue.Enqueue(3)
						if err == nil {
							return
						}
						if err != ErrFull {
							results <- err
							return
						}
					} else {
						value, err := queue.Dequeue()
						if err == nil {
							if value != 3 {
								results <- fmt.Errorf("handoff FIFO got %d want 3", value)
							}
							return
						}
						if err != ErrEmpty {
							results <- err
							return
						}
					}
					runtime.Gosched()
				}
			}()
			select {
			case <-handoff:
				if producerSide {
					if value, err := queue.Dequeue(); err != nil || value != 1 {
						t.Errorf("opposing dequeue value=%d error=%v", value, err)
					}
				} else if err := queue.Enqueue(3); err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
			}
			workers.Wait()
			close(results)
			for err := range results {
				t.Error(err)
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
		})
	}
}
