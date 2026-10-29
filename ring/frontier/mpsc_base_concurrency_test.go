package frontier

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestMPSCBaseConcurrentAccounting(t *testing.T) {
	baseTestConcurrent(
		t,
		func(value int64) int64 { return value },
		func(value int64) int64 { return value },
	)
	for _, retain := range []bool{false, true} {
		var options []Option
		if retain {
			options = append(options, WithoutDequeueClearing())
		}
		baseTestConcurrent(
			t,
			func(value int64) *int64 { return &value },
			func(value *int64) int64 { return *value },
			options...,
		)
	}
}

func baseTestConcurrent[T any](
	t *testing.T,
	encode func(int64) T,
	decode func(T) int64,
	options ...Option,
) {
	t.Helper()
	const producers, perProducer = 4, 300
	queue := baseTestQueue[T](t, 64, options...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	for producer := 0; producer < producers; producer++ {
		workers.Add(1)
		go func(producer int) {
			defer workers.Done()
			for sequence := 0; sequence < perProducer; {
				select {
				case <-ctx.Done():
					return
				default:
				}
				count := min(1+sequence%5, perProducer-sequence)
				items := make([]T, count)
				for index := range items {
					items[index] = encode(int64(producer*perProducer + sequence + index))
				}
				moved := 0
				switch sequence % 3 {
				case 0:
					if err := queue.Enqueue(items[0]); err == nil {
						moved = 1
					}
				case 1:
					if _, err := queue.EnqueueBulk(items); err == nil {
						moved = count
					}
				default:
					moved, _ = queue.EnqueueBurst(items)
				}
				sequence += moved
				if moved == 0 {
					runtime.Gosched()
				}
			}
		}(producer)
	}
	observerErrors := make(chan string, 1)
	for observer := 0; observer < 2; observer++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				count, free := queue.Count(), queue.Free()
				_ = queue.Full()
				_ = queue.Empty()
				if count < 0 || count > 64 || free < 0 || free > 64 {
					select {
					case observerErrors <- "observer bounds":
					default:
					}
					return
				}
			}
		}()
	}
	next := make([]int, producers)
	seen := make([]bool, producers*perProducer)
	for total := 0; total < len(seen); {
		select {
		case <-ctx.Done():
			t.Fatal("accounting deadline", total)
		default:
		}
		dst := make([]T, min(1+total%7, len(seen)-total))
		moved := 0
		switch total % 3 {
		case 0:
			value, err := queue.Dequeue()
			if err == nil {
				dst[0], moved = value, 1
			}
		case 1:
			if _, err := queue.DequeueBulk(dst); err == nil {
				moved = len(dst)
			}
		default:
			moved, _ = queue.DequeueBurst(dst)
		}
		for _, value := range dst[:moved] {
			tag := int(decode(value))
			if tag < 0 || tag >= len(seen) || seen[tag] {
				t.Fatal("corrupt or duplicate tag", tag)
			}
			producer, sequence := tag/perProducer, tag%perProducer
			if sequence != next[producer] {
				t.Fatal("producer sequence", producer, sequence, next[producer])
			}
			seen[tag] = true
			next[producer]++
		}
		total += moved
		if moved == 0 {
			runtime.Gosched()
		}
	}
	cancel()
	workers.Wait()
	select {
	case message := <-observerErrors:
		t.Fatal(message)
	default:
	}
	if queue.Count() != 0 {
		t.Fatal("final count")
	}
	if queue.consumer.clearOnDequeue {
		for _, value := range queue.producerConfig.entries {
			if !reflect.ValueOf(&value).Elem().IsZero() {
				t.Fatal("drained payload retained")
			}
		}
	}
	queue.Reset()
}

func TestMPSCBaseStalledProducerGates(t *testing.T) {
	queue := baseTestQueue[int64](t, 8)
	reserved := make(chan uint64, 1)
	allowCopy := make(chan struct{})
	completed := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		start, count, _ := queue.mpscReserveProducer(4, true, false)
		if count != 4 {
			return
		}
		reserved <- start
		select {
		case <-allowCopy:
		case <-ctx.Done():
			return
		}
		baseTestComplete(queue, start, []int64{1, 2, 3, 4})
		close(completed)
	}()
	defer func() { cancel(); workers.Wait() }()
	select {
	case start := <-reserved:
		if start != 0 {
			t.Fatal(start)
		}
	case <-ctx.Done():
		t.Fatal("reservation deadline")
	}
	secondDone := make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		start, count, _ := queue.mpscReserveProducer(3, true, false)
		if start == 4 && count == 3 {
			baseTestComplete(queue, start, []int64{5, 6, 7})
			close(secondDone)
		}
	}()
	select {
	case <-secondDone:
	case <-ctx.Done():
		t.Fatal("later producer did not return through hole")
	}
	if queue.Count() != 0 {
		t.Fatal("hole")
	}
	close(allowCopy)
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("completion deadline")
	}
	dst := make([]int64, 7)
	if _, err := queue.DequeueBulk(dst); err != nil ||
		!slices.Equal(dst, []int64{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatal(dst, err)
	}
}

func TestMPSCBaseReverseCompletion(t *testing.T) {
	baseTestReverse(t, []int64{1, 2, 3, 4, 5, 6, 7})
	values := []int{1, 2, 3, 4, 5, 6, 7}
	pointers := make([]*int, len(values))
	for index := range pointers {
		pointers[index] = &values[index]
	}
	baseTestReverse(t, pointers)
}

func baseTestReverse[T comparable](t *testing.T, values []T) {
	t.Helper()
	for _, position := range []uint64{0, math.MaxUint32 - 2} {
		queue := baseTestQueue[T](t, 8)
		baseTestSeed(queue, position)
		first, count, _ := queue.mpscReserveProducer(4, true, false)
		if first != position || count != 4 {
			t.Fatal("first admission")
		}
		second, count, _ := queue.mpscReserveProducer(3, true, false)
		if second != position+4 || count != 3 {
			t.Fatal("second admission")
		}
		baseTestComplete(queue, second, values[4:])
		if queue.Count() != 0 {
			t.Fatal("observer bridged hole")
		}
		if _, err := queue.Dequeue(); err != ErrEmpty {
			t.Fatal(err)
		}
		dst := slices.Clone(values[:2])
		before := slices.Clone(dst)
		if _, err := queue.DequeueBulk(dst); err != ErrEmpty || !slices.Equal(dst, before) {
			t.Fatal("Bulk bridged hole or changed destination")
		}
		for _, buffer := range [][]T{nil, make([]T, 2)} {
			if moved, available := queue.DequeueBurst(buffer); moved != 0 || available != 0 {
				t.Fatalf("publication hole: moved=%d available=%d", moved, available)
			}
		}
		if moved, reusable := queue.EnqueueBurst(nil); moved != 0 || reusable != 1 {
			t.Fatalf("reserved capacity: moved=%d reusable=%d", moved, reusable)
		}
		baseTestComplete(queue, first, values[:4])
		if moved, available := queue.DequeueBurst(nil); moved != 0 || available != 7 {
			t.Fatalf("completed publication: moved=%d available=%d", moved, available)
		}
		value, err := queue.Dequeue()
		if err != nil || value != values[0] {
			t.Fatal("scalar FIFO", err)
		}
		dst = make([]T, 6)
		if _, err := queue.DequeueBulk(dst); err != nil || !slices.Equal(dst, values[1:]) {
			t.Fatal("Bulk FIFO", err)
		}
	}
}

func TestMPSCBaseBlockedBeforeRelease(t *testing.T) {
	for _, capacity := range []int{1, 8} {
		queue := baseTestQueue[*int](t, capacity)
		values := make([]int, capacity)
		pointers := make([]*int, capacity)
		for index := range values {
			values[index] = index + 1
			pointers[index] = &values[index]
		}
		start, count, _ := queue.mpscReserveProducer(uint32(capacity), true, false)
		if count != uint32(capacity) {
			t.Fatal("A admission")
		}
		blocked := func() {
			t.Helper()
			for _, items := range [][]*int{nil, pointers[:1]} {
				if moved, reusable := queue.EnqueueBurst(items); moved != 0 || reusable != 0 {
					t.Fatalf("before release: moved=%d reusable=%d", moved, reusable)
				}
			}
			if _, moved, _ := queue.mpscReserveProducer(1, true, false); moved != 0 {
				t.Fatal("B reused unreleased slots")
			}
		}
		blocked()
		baseTestComplete(queue, start, pointers)
		queue.consumer.frontier = queue.mpscDiscover(0, 0, false, 0)
		blocked()
		dst := make([]*int, capacity)
		queue.mpscReadConsumer(0, dst)
		if !slices.Equal(dst, pointers) {
			t.Fatal("read visibility")
		}
		for _, pointer := range queue.producerConfig.entries {
			if pointer != nil {
				t.Fatal("clear before release")
			}
		}
		blocked()
		queue.mpscReleaseConsumer(uint32(capacity))
		if moved, reusable := queue.EnqueueBurst(nil); moved != 0 || reusable != capacity {
			t.Fatalf("completed release: moved=%d reusable=%d", moved, reusable)
		}
		next, moved, _ := queue.mpscReserveProducer(1, true, false)
		if next != uint64(capacity) || moved != 1 {
			t.Fatal("B admission after release")
		}
		baseTestComplete(queue, next, pointers[:1])
		if got, err := queue.Dequeue(); got != pointers[0] || err != nil {
			t.Fatal("B FIFO", err)
		}
	}
}

func TestMPSCBasePartialReuseAndObserverFallback(t *testing.T) {
	queue := baseTestQueue[int](t, 64)
	values := make([]int, 96)
	for index := range values {
		values[index] = index + 1
	}
	if _, err := queue.EnqueueBulk(values[:64]); err != nil {
		t.Fatal(err)
	}
	old := queue.consumerPublished.Load()
	queue.consumer.frontier = queue.mpscDiscover(0, 0, false, 0)
	if queue.consumer.frontier != 64 || queue.completion.records[0].Load() != 64 {
		t.Fatal("acquired proof or retained record")
	}
	dst := make([]int, 32)
	queue.mpscReadConsumer(0, dst)
	if !slices.Equal(dst, values[:32]) {
		t.Fatal(dst)
	}
	if _, moved, _ := queue.mpscReserveProducer(1, true, false); moved != 0 {
		t.Fatal("reuse before final release")
	}
	queue.mpscReleaseConsumer(32)
	if _, err := queue.EnqueueBulk(values[64:]); err != nil {
		t.Fatal(err)
	}
	if queue.consumer.frontier-queue.consumer.cursor != 32 || queue.Count() != 64 ||
		queue.mpscCountFromSnapshot(old) != 32 ||
		queue.completion.records[0].Load() != 96 {
		t.Fatal("partial overwrite proof")
	}
	dst = make([]int, 64)
	if _, err := queue.DequeueBulk(dst); err != nil || !slices.Equal(dst, values[32:]) {
		t.Fatal("remainder FIFO", err)
	}
	if queue.completion.records[0].Load() != 96 {
		t.Fatal("completion retired")
	}
}

func TestMPSCBasePointerResetAndHandoff(t *testing.T) {
	for _, retain := range []bool{false, true} {
		for _, position := range []uint64{0, math.MaxUint32} {
			var options []Option
			if retain {
				options = append(options, WithoutDequeueClearing())
			}
			options = append(options, WithMaxBatch(6), WithExactBatchCounts())
			queue := baseTestQueue[*int](t, 8, options...)
			baseTestSeed(queue, position)
			values := []int{1, 2, 3, 4}
			pointers := []*int{&values[0], &values[1], &values[2], &values[3]}
			if _, err := queue.EnqueueBulk(pointers); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var workers sync.WaitGroup
			done := make(chan []error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, success := queue.Dequeue()
				_, failed := queue.DequeueBulk(make([]*int, 5))
				_, zero := queue.DequeueBulk(nil)
				_, invalid := queue.DequeueBulk(make([]*int, 9))
				done <- []error{success, failed, zero, invalid}
			}()
			select {
			case results := <-done:
				cancel()
				workers.Wait()
				if results[0] != nil || results[1] != ErrEmpty || results[2] != nil ||
					!errors.Is(results[3], ErrExceedsCapacity) {
					t.Fatal(results)
				}
			case <-ctx.Done():
				cancel()
				workers.Wait()
				t.Fatal("handoff deadline")
			}
			if queue.consumer.cursor != position+1 || queue.consumer.frontier != position+4 {
				t.Fatal("partial proof handoff")
			}
			if (queue.producerConfig.entries[uint32(position)&7] != nil) != retain {
				t.Fatal("clearing option")
			}
			if got, err := queue.Dequeue(); got != pointers[1] || err != nil {
				t.Fatal(got, err)
			}
			payload, record := &queue.producerConfig.entries[0], &queue.completion.records[0]
			queue.Reset()
			if payload != &queue.producerConfig.entries[0] || record != &queue.completion.records[0] ||
				queue.MaxEnqueueBatch() != 6 || queue.MaxDequeueBatch() != 6 ||
				!queue.consumer.exactBatchCounts || queue.consumer.clearOnDequeue == retain {
				t.Fatal("Reset identity/options")
			}
			for index, pointer := range queue.producerConfig.entries {
				if pointer != nil || queue.completion.records[index].Load() != 0 {
					t.Fatal("Reset retained storage")
				}
			}
			if err := queue.Enqueue(pointers[3]); err != nil {
				t.Fatal(err)
			}
			if got, err := queue.Dequeue(); got != pointers[3] || err != nil {
				t.Fatal("synchronized reuse", err)
			}
		}
	}
}

func TestMPSCBaseZeroAllocations(t *testing.T) {
	baseTestAllocations(t, int64(7))
	value := 7
	baseTestAllocations(t, &value)
}

func baseTestAllocations[T any](t *testing.T, value T) {
	t.Helper()
	for _, api := range []string{"scalar", "bulk", "burst"} {
		queue := baseTestQueue[T](t, 8)
		baseTestSeed(queue, math.MaxUint32-3)
		items, dst := []T{value}, make([]T, 1)
		failed := false
		allocations := testing.AllocsPerRun(100, func() {
			for pair := 0; pair < 8; pair++ {
				switch api {
				case "scalar":
					enqueueErr := queue.Enqueue(value)
					_, dequeueErr := queue.Dequeue()
					failed = failed || enqueueErr != nil || dequeueErr != nil
				case "bulk":
					_, enqueueErr := queue.EnqueueBulk(items)
					_, dequeueErr := queue.DequeueBulk(dst)
					failed = failed || enqueueErr != nil || dequeueErr != nil
				case "burst":
					enqueued, _ := queue.EnqueueBurst(items)
					dequeued, _ := queue.DequeueBurst(dst)
					failed = failed || enqueued != 1 || dequeued != 1
				}
			}
		})
		if failed || allocations != 0 {
			t.Fatalf("api=%s allocations=%g failed=%t", api, allocations, failed)
		}
	}
}
