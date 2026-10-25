package ring

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
)

func TestSPSCEnqueueBulkOneSnapshots(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		testSPSCEnqueueBulkOneSnapshots(t, int64(77), int64(-10))
	})
	value, sentinel := int64(77), int64(-10)
	t.Run("ptr", func(t *testing.T) { testSPSCEnqueueBulkOneSnapshots(t, &value, &sentinel) })
}

func testSPSCEnqueueBulkOneSnapshots[T comparable](t *testing.T, value, sentinel T) {
	t.Helper()
	for _, test := range []struct {
		name                  string
		exact                 bool
		cursor, cached, fresh uint32
		success               bool
		remaining             int
		wantCache             uint32
	}{
		{"cached", false, 4, 0, 2, true, 3, 0},
		{"exact", true, 4, 0, 2, true, 5, 2},
		{"full", false, 8, 0, 0, false, 0, 0},
		{"exhausted-cache", false, 8, 0, 2, true, 1, 2},
		{"invalid-cache-recovers", false, 4, 5, 2, true, 5, 2},
		{"invalid-fresh", false, 4, 5, 5, false, 8, 5},
		{"exact-invalid-fresh", true, 4, 0, 5, false, 8, 5},
	} {
		for _, clearing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%t", test.name, clearing), func(t *testing.T) {
				var queues [2]*SPSCQueue[T]
				for index := range queues {
					queue := newSPSCForTest[T](t, 8)
					queue.consumer.clearOnDequeue = clearing
					queue.producer.exactBatchCounts = test.exact
					queue.producer.cursor = test.cursor
					queue.producer.cachedConsumer = test.cached
					queue.producer.published.Store(test.cursor)
					queue.consumer.cursor = test.fresh
					queue.consumer.published.Store(test.fresh)
					for slot := range queue.producer.entries {
						queue.producer.entries[slot] = sentinel
					}
					queues[index] = queue
				}
				want := snapshotSPSCBurstOne(queues[0])
				want.positions[1] = test.wantCache
				var wantErr error
				if !test.success {
					wantErr = ErrFull
				}
				if test.success {
					want.positions[0]++
					want.positions[2]++
					want.entries[test.cursor&7] = value
				}
				remaining, err := queues[0].EnqueueBulk([]T{value})
				moved, referenceRemaining := queues[1].enqueueBatch([]T{value}, true)
				var referenceErr error
				if moved != 1 {
					referenceErr = ErrFull
				}
				if remaining != test.remaining || err != wantErr {
					t.Fatalf("count/error=%d/%v want=%d/%v", remaining, err, test.remaining, wantErr)
				}
				if remaining != referenceRemaining || err != referenceErr {
					t.Fatalf("helper count/error=%d/%v", referenceRemaining, referenceErr)
				}
				for _, queue := range queues {
					if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
						t.Fatalf("state=%+v want=%+v", got, want)
					}
				}
			})
		}
	}
}

func TestSPSCDequeueBulkOneSnapshots(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		testSPSCDequeueBulkOneSnapshots(t, int64(-10), int64(-1))
	})
	sentinel, tail := int64(-10), int64(-1)
	t.Run("ptr", func(t *testing.T) { testSPSCDequeueBulkOneSnapshots(t, &sentinel, &tail) })
}

func testSPSCDequeueBulkOneSnapshots[T comparable](t *testing.T, sentinel, tail T) {
	t.Helper()
	for _, test := range []struct {
		name                  string
		exact                 bool
		cursor, cached, fresh uint32
		success               bool
		remaining             int
		wantCache             uint32
	}{
		{"cached", false, 2, 4, 6, true, 1, 4},
		{"exact", true, 2, 4, 6, true, 3, 6},
		{"empty", false, 2, 2, 2, false, 0, 2},
		{"exhausted-cache", false, 2, 2, 6, true, 3, 6},
		{"invalid-cache-recovers", false, 2, 11, 6, true, 3, 6},
		{"invalid-fresh", false, 2, 11, 11, false, 8, 11},
		{"exact-invalid-fresh", true, 2, 4, 11, false, 8, 11},
	} {
		for _, clearing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%t", test.name, clearing), func(t *testing.T) {
				var queues [2]*SPSCQueue[T]
				for index := range queues {
					queue := newSPSCForTest[T](t, 8)
					queue.consumer.clearOnDequeue = clearing
					queue.consumer.exactBatchCounts = test.exact
					queue.consumer.cursor = test.cursor
					queue.consumer.cachedProducer = test.cached
					queue.consumer.published.Store(test.cursor)
					queue.producer.cursor = test.fresh
					queue.producer.published.Store(test.fresh)
					for slot := range queue.producer.entries {
						queue.producer.entries[slot] = sentinel
					}
					queues[index] = queue
				}
				want := snapshotSPSCBurstOne(queues[0])
				want.positions[4] = test.wantCache
				var wantErr error
				if !test.success {
					wantErr = ErrEmpty
				}
				dst := []T{tail, tail, tail}
				referenceDst := slices.Clone(dst)
				wantDst := slices.Clone(dst)
				if test.success {
					want.positions[3]++
					want.positions[5]++
					if clearing {
						var zero T
						want.entries[test.cursor&7] = zero
					}
					wantDst[0] = sentinel
				}
				remaining, err := queues[0].DequeueBulk(dst[:1])
				moved, referenceRemaining := queues[1].dequeueBatch(referenceDst[:1], true)
				var referenceErr error
				if moved != 1 {
					referenceErr = ErrEmpty
				}
				if !slices.Equal(dst, wantDst) || !slices.Equal(referenceDst, wantDst) {
					t.Fatalf("destination=%v helper=%v want=%v", dst, referenceDst, wantDst)
				}
				if remaining != test.remaining || err != wantErr {
					t.Fatalf("count/error=%d/%v want=%d/%v", remaining, err, test.remaining, wantErr)
				}
				if remaining != referenceRemaining || err != referenceErr {
					t.Fatalf("helper count/error=%d/%v", referenceRemaining, referenceErr)
				}
				for _, queue := range queues {
					if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
						t.Fatalf("state=%+v want=%+v", got, want)
					}
				}
			})
		}
	}
}

func TestSPSCBulkOnePointersAndWrap(t *testing.T) {
	t.Run("int64", func(t *testing.T) { testSPSCBulkOneWrap(t, int64(42), int64(-1)) })
	value, sentinel := int64(42), int64(-1)
	t.Run("ptr", func(t *testing.T) { testSPSCBulkOneWrap(t, &value, &sentinel) })
}

func testSPSCBulkOneWrap[T comparable](t *testing.T, value, sentinel T) {
	t.Helper()
	for _, capacity := range []int{1, 8} {
		for _, exact := range []bool{false, true} {
			for _, clearing := range []bool{false, true} {
				name := fmt.Sprintf("capacity=%d/exact=%t/clear=%t", capacity, exact, clearing)
				t.Run(name, func(t *testing.T) {
					var queues [2]*SPSCQueue[T]
					for index := range queues {
						queue := newSPSCForTest[T](t, capacity)
						queue.producer.cursor = math.MaxUint32
						queue.producer.cachedConsumer = math.MaxUint32
						queue.producer.published.Store(math.MaxUint32)
						queue.consumer.cursor = math.MaxUint32
						queue.consumer.cachedProducer = math.MaxUint32
						queue.consumer.published.Store(math.MaxUint32)
						queue.producer.exactBatchCounts = exact
						queue.consumer.exactBatchCounts = exact
						queue.consumer.clearOnDequeue = clearing
						queues[index] = queue
					}
					want := snapshotSPSCBurstOne(queues[0])
					want.positions[0], want.positions[2] = 0, 0
					want.entries[capacity-1] = value
					count, err := queues[0].EnqueueBulk([]T{value})
					moved, referenceCount := queues[1].enqueueBatch([]T{value}, true)
					if err != nil || count != capacity-1 || moved != 1 || referenceCount != capacity-1 {
						t.Fatal("enqueue counts")
					}
					for _, queue := range queues {
						if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
							t.Fatalf("enqueue state=%+v want=%+v", got, want)
						}
					}
					dst, referenceDst := []T{sentinel, sentinel}, []T{sentinel, sentinel}
					want.positions[3], want.positions[4], want.positions[5] = 0, 0, 0
					if clearing {
						var zero T
						want.entries[capacity-1] = zero
					}
					count, err = queues[0].DequeueBulk(dst[:1])
					moved, referenceCount = queues[1].dequeueBatch(referenceDst[:1], true)
					if err != nil || count != 0 || moved != 1 || referenceCount != 0 {
						t.Fatal("dequeue counts")
					}
					if !slices.Equal(dst, []T{value, sentinel}) || !slices.Equal(dst, referenceDst) {
						t.Fatal("destination or suffix")
					}
					for _, queue := range queues {
						if got := snapshotSPSCBurstOne(queue); !reflect.DeepEqual(got, want) {
							t.Fatalf("dequeue state=%+v want=%+v", got, want)
						}
					}
				})
			}
		}
	}
}
