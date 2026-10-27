package frontier

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func cacheTestQueue[T any](t *testing.T, capacity int) *MPSCQueue[T] {
	t.Helper()
	queue, err := NewMPSCQueue[T](capacity)
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func cacheTestBaseQueue[T any](
	t *testing.T,
	variant string,
	capacity int,
	options ...Option,
) *mpscBase[T] {
	t.Helper()
	switch variant {
	case "cached":
		queue, err := NewMPSCQueue[T](capacity, options...)
		if err != nil {
			t.Fatal(err)
		}
		return &queue.mpscBase
	default:
		return baseTestQueue[T](t, capacity, options...)
	}
}

func TestMPSCCacheCapacityReuseFIFO(t *testing.T) {
	for _, variant := range []string{"base", "cached"} {
		t.Run(variant, func(t *testing.T) {

			for _, capacity := range []int{1, 8, 1024, 16384} {
				t.Run(fmt.Sprint(capacity), func(t *testing.T) {
					queue := cacheTestBaseQueue[int](t, variant, capacity)
					items := make([]int, capacity)
					for lap := 0; lap < 3; lap++ {
						for index := range items {
							items[index] = lap*capacity + index + 1
						}
						if !queue.Empty() || queue.Free() != capacity {
							t.Fatal("initial observation")
						}
						if free, err := queue.EnqueueBulk(items); free != 0 || err != nil {
							t.Fatalf("full reservation free=%d err=%v", free, err)
						}
						if !queue.Full() || queue.Count() != capacity || queue.Enqueue(0) != ErrFull {
							t.Fatal("full observation")
						}
						for _, want := range items {
							got, err := queue.Dequeue()
							if got != want || err != nil {
								t.Fatalf("got=%d want=%d err=%v", got, want, err)
							}
						}
						if _, err := queue.Dequeue(); err != ErrEmpty {
							t.Fatalf("empty=%v", err)
						}
					}
				})
			}

		})
	}
}

func TestMPSCCacheMixedBoundaries(t *testing.T) {
	for _, variant := range []string{"base", "cached"} {
		t.Run(variant, func(t *testing.T) {

			queue := cacheTestBaseQueue[int](t, variant, 8)
			if err := queue.Enqueue(1); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.EnqueueBulk([]int{2, 3, 4}); err != nil {
				t.Fatal(err)
			}
			if moved, _ := queue.EnqueueBurst([]int{5, 6, 7, 8, 9}); moved != 4 {
				t.Fatalf("moved=%d", moved)
			}
			dst := []int{-1, -1, -1, -1, -1}
			if left, err := queue.DequeueBulk(dst); left != 3 || err != nil {
				t.Fatalf("left=%d err=%v", left, err)
			}
			if !slices.Equal(dst, []int{1, 2, 3, 4, 5}) || queue.consumer.frontier != 8 {
				t.Fatal(dst)
			}
			if _, err := queue.DequeueBulk(dst[:4]); err != ErrEmpty {
				t.Fatal(err)
			}
			if !slices.Equal(dst, []int{1, 2, 3, 4, 5}) {
				t.Fatal("failed Bulk touched destination")
			}
			if moved, left := queue.DequeueBurst(dst); moved != 3 || left != 0 {
				t.Fatalf("moved=%d left=%d", moved, left)
			}
			if !slices.Equal(dst, []int{6, 7, 8, 4, 5}) {
				t.Fatal(dst)
			}

		})
	}
}

func TestMPSCCacheCountsInvalidAndZero(t *testing.T) {
	for _, variant := range []string{"base", "cached"} {
		t.Run(variant, func(t *testing.T) {

			for _, exact := range []bool{false, true} {
				t.Run(fmt.Sprint(exact), func(t *testing.T) {
					var options []Option
					if exact {
						options = append(options, WithExactBatchCounts())
					}
					queue := cacheTestBaseQueue[int](t, variant, 8, options...)
					if _, err := queue.EnqueueBulk([]int{1, 2, 3, 4}); err != nil {
						t.Fatal(err)
					}
					if _, err := queue.DequeueBulk(make([]int, 2)); err != nil {
						t.Fatal(err)
					}
					if free, err := queue.EnqueueBulk([]int{5}); err != nil ||
						free != map[bool]int{false: 3, true: 5}[exact] {
						t.Fatalf("cached enqueue free=%d err=%v", free, err)
					}
					dst := []int{-1}
					want := map[bool]int{false: 1, true: 2}[exact]
					if left, err := queue.DequeueBulk(dst); err != nil || left != want {
						t.Fatalf("cached dequeue left=%d err=%v", left, err)
					}
					for _, direction := range []string{"enqueue", "dequeue"} {
						slots := slices.Clone(queue.producerConfig.entries)
						records := baseTestRecords(queue)
						state := queue.consumerPublished.Load()
						cursor, frontier := queue.consumer.cursor, queue.consumer.frontier
						cache := queue.producerReservation.cachedConsumer.Load()
						head := queue.producerReservation.head.Load()
						dst := make([]int, 9)
						var left int
						var err error
						if direction == "enqueue" {
							left, err = queue.EnqueueBulk(dst)
							if left != 6 {
								t.Fatal(left)
							}
						} else {
							left, err = queue.DequeueBulk(dst)
							if left != 2 {
								t.Fatal(left)
							}
						}
						if !errors.Is(err, ErrExceedsCapacity) || err == ErrExceedsCapacity {
							t.Fatal(err)
						}
						if !slices.Equal(slots, queue.producerConfig.entries) ||
							!slices.Equal(records, baseTestRecords(queue)) ||
							state != queue.consumerPublished.Load() ||
							cursor != queue.consumer.cursor || frontier != queue.consumer.frontier ||
							cache != queue.producerReservation.cachedConsumer.Load() ||
							head != queue.producerReservation.head.Load() ||
							!slices.Equal(dst, make([]int, 9)) {
							t.Fatal("invalid request mutated state")
						}
					}
					if left, err := queue.DequeueBulk(nil); left != 2 || err != nil ||
						queue.consumer.frontier != 5 || queue.consumer.cursor != 3 {
						t.Fatalf("zero refresh left=%d err=%v", left, err)
					}
					if free, err := queue.EnqueueBulk(nil); free != 6 || err != nil {
						t.Fatalf("zero producer free=%d err=%v", free, err)
					}
					if left, err := queue.DequeueBulk(make([]int, 3)); left != 2 || err != ErrEmpty {
						t.Fatalf("failed fresh left=%d err=%v", left, err)
					}
					if moved, left := queue.DequeueBurst(make([]int, 3)); moved != 2 || left != 0 {
						t.Fatalf("partial moved=%d left=%d", moved, left)
					}
				})
			}

		})
	}
}

func TestMPSCCacheOptions(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		options  []Option
		want     error
	}{
		{"capacity-first", 3, []Option{WithMaxBatch(0)}, ErrInvalidCount},
		{"enqueue-first", 8, []Option{WithMaxBatch(0)}, ErrInvalidMaxBatch},
		{"dequeue", 8, []Option{WithMaxDequeueBatch(0)}, ErrInvalidMaxBatch},
		{"enqueue-above-count", 8, []Option{WithMaxEnqueueBatch(9)}, ErrInvalidMaxBatch},
		{"zero-count", 0, nil, ErrInvalidCount},
		{"last-limit", 8, []Option{WithMaxBatch(0), WithMaxBatch(4)}, nil},
	}
	for _, constructor := range []struct {
		name     string
		newQueue func(int, ...Option) (bool, error)
	}{
		{"NewMPSCQueue", func(capacity int, options ...Option) (bool, error) {
			queue, err := NewMPSCQueue[int](capacity, options...)
			return queue == nil, err
		}},
	} {
		for _, test := range tests {
			t.Run(constructor.name+"/"+test.name, func(t *testing.T) {
				isNil, err := constructor.newQueue(test.capacity, test.options...)
				if !errors.Is(err, test.want) {
					t.Fatalf("err=%v want=%v", err, test.want)
				}
				if test.want != nil {
					if !isNil || err == test.want || !strings.Contains(err.Error(), constructor.name) {
						t.Fatalf("nil=%t err=%v", isNil, err)
					}
				}
			})
		}
	}
	for _, variant := range []string{"base", "cached"} {
		queue := cacheTestBaseQueue[int](
			t,
			variant,
			8,
			WithMaxBatch(4),
			WithMaxEnqueueBatch(2),
			WithMaxDequeueBatch(3),
			WithExactBatchCounts(),
			WithoutDequeueClearing(),
		)
		if queue.Capacity() != 8 || queue.MaxEnqueueBatch() != 2 || queue.MaxDequeueBatch() != 3 ||
			!queue.consumer.exactBatchCounts || !queue.producerConfig.exactBatchCounts ||
			queue.consumer.clearOnDequeue {
			t.Fatal("configuration", variant)
		}
	}
}

func TestMPSCCacheClearingAndReset(t *testing.T) {
	for _, variant := range []string{"cached"} {
		for _, keep := range []bool{false, true} {
			var options []Option
			if keep {
				options = append(options, WithoutDequeueClearing())
			}
			var queue *mpscBase[*int]
			var reset func()
			var cacheLoad func() uint64
			value := 42
			if variant == "cached" {
				outer, err := NewMPSCQueue[*int](8, options...)
				if err != nil {
					t.Fatal(err)
				}
				queue = &outer.mpscBase
				reset = outer.Reset
				outer.observerCompleted.Store(7)
				cacheLoad = outer.observerCompleted.Load
			}
			if err := queue.Enqueue(&value); err != nil {
				t.Fatal(err)
			}
			got, err := queue.Dequeue()
			if err != nil || got != &value {
				t.Fatal(got, err)
			}
			if (queue.producerConfig.entries[0] != nil) != keep {
				t.Fatal("clearing", variant, keep)
			}
			if cacheLoad() != 7 {
				t.Fatal("transfer touched observer proof")
			}
			reset()
			if cacheLoad() != 0 || queue.Count() != 0 || queue.consumer.cursor != 0 ||
				queue.consumer.frontier != 0 ||
				queue.consumerPublished.Load() != 0 || queue.producerReservation.head.Load() != 0 ||
				queue.producerReservation.cachedConsumer.Load() != 0 || queue.consumer.clearOnDequeue == keep {
				t.Fatal("reset state", variant)
			}
			for index, payload := range queue.producerConfig.entries {
				if payload != nil || queue.completion.records[index].Load() != 0 {
					t.Fatal("reset storage")
				}
			}
		}
	}
}

type cacheTestAPIs[T any] interface {
	Enqueue(T) error
	Dequeue() (T, error)
	EnqueueBulk([]T) (int, error)
	DequeueBulk([]T) (int, error)
	EnqueueBurst([]T) (int, int)
	DequeueBurst([]T) (int, int)
	Count() int
	Free() int
	Full() bool
	Empty() bool
}

func TestMPSCCacheAllocations(t *testing.T) {
	cacheTestAllocations[int64](t, 17)
	value := 17
	cacheTestAllocations[*int](t, &value)
}

func cacheTestAllocations[T any](t *testing.T, value T) {
	t.Helper()
	for _, variant := range []string{"base", "cached"} {
		var queue cacheTestAPIs[T]
		switch variant {
		case "base":
			queue = baseTestQueue[T](t, 8)
		case "cached":
			queue = cacheTestQueue[T](t, 8)
		}
		source := []T{value, value}
		destination := make([]T, 2)
		for _, api := range []string{"scalar", "bulk", "burst", "observers"} {
			t.Run(fmt.Sprintf("%T/%s/%s", value, variant, api), func(t *testing.T) {
				var operation func()
				switch api {
				case "scalar":
					operation = func() {
						if err := queue.Enqueue(value); err != nil {
							panic(err)
						}
						if _, err := queue.Dequeue(); err != nil {
							panic(err)
						}
					}
				case "bulk":
					operation = func() {
						if _, err := queue.EnqueueBulk(source); err != nil {
							panic(err)
						}
						if _, err := queue.DequeueBulk(destination); err != nil {
							panic(err)
						}
					}
				case "burst":
					operation = func() {
						if moved, _ := queue.EnqueueBurst(source); moved != 2 {
							panic("enqueue")
						}
						if moved, _ := queue.DequeueBurst(destination); moved != 2 {
							panic("dequeue")
						}
					}
				case "observers":
					operation = func() {
						// Each iteration includes a newly completed prefix and repeated proof observation.
						if err := queue.Enqueue(value); err != nil {
							panic(err)
						}
						if queue.Count() != 1 {
							panic("first proof observation")
						}
						if queue.Count() != 1 || queue.Free() != 7 || queue.Full() || queue.Empty() {
							panic("observation")
						}
						if _, err := queue.Dequeue(); err != nil {
							panic(err)
						}
					}
				}
				if allocations := testing.AllocsPerRun(100, operation); allocations != 0 {
					t.Fatalf("allocations=%v", allocations)
				}
			})
		}
	}
}

func TestMPSCCacheResetProofLifetime(t *testing.T) {
	for _, variant := range []string{"cached"} {
		t.Run(variant, func(t *testing.T) {
			cached := cacheTestQueue[int](t, 4)
			queue := &cached.mpscBase
			reset := cached.Reset
			count := cached.Count
			free := cached.Free
			full := cached.Full
			empty := cached.Empty
			proof := &cached.observerCompleted
			for value := 1; value <= 4; value++ {
				if err := queue.Enqueue(value); err != nil {
					t.Fatal(err)
				}
			}
			if count() != 4 {
				t.Fatal("initial full prefix")
			}
			if variant == "cached" && proof.Load() != 4 {
				t.Fatal("initial cached proof")
			}
			reset()
			if proof.Load() != 0 {
				t.Fatal("Reset retained old proof")
			}
			start, reserved, _ := queue.mpscReserveProducer(4, true, false)
			if start != 0 || reserved != 4 || queue.producerReservation.head.Load() != 4 {
				t.Fatal("new full reservation")
			}
			for _, record := range baseTestRecords(queue) {
				if record != 0 {
					t.Fatal("Reset retained completion")
				}
			}
			if count() != 0 || free() != 4 || full() || !empty() || proof.Load() != 0 {
				t.Fatal("new lifetime hole appeared complete")
			}
			items := []int{11, 12, 13, 14}
			queue.mpscWriteProducer(start, items)
			queue.mpscPublishProducer(start, reserved)
			if count() != 4 {
				t.Fatal("completed new lifetime prefix")
			}
			for _, want := range items {
				got, err := queue.Dequeue()
				if err != nil || got != want {
					t.Fatal("new lifetime FIFO", got, want, err)
				}
			}
		})
	}
}

func TestMPSCCachePrivateBaseExposure(t *testing.T) {
	expected := []string{
		"Capacity", "Count", "Dequeue", "DequeueBulk", "DequeueBurst", "Empty", "Enqueue",
		"EnqueueBulk", "EnqueueBurst", "Free", "Full", "MaxDequeueBatch", "MaxEnqueueBatch", "Reset",
	}
	for _, pointer := range []reflect.Type{
		reflect.TypeOf((*MPSCQueue[int])(nil)),
	} {
		structure := pointer.Elem()
		field, exists := structure.FieldByName("mpscBase")
		if !exists || !field.Anonymous || field.IsExported() || field.PkgPath == "" {
			t.Fatal("base field must be private and anonymous", structure)
		}
		methods := make([]string, pointer.NumMethod())
		for index := range methods {
			methods[index] = pointer.Method(index).Name
		}
		if !slices.Equal(methods, expected) {
			t.Fatal("public methods", structure, methods)
		}
	}
}
