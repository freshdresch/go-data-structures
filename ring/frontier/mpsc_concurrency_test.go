package frontier

import (
	"context"
	"math"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestMPSCCacheDelayedPublication(t *testing.T) {
	for _, newer := range []bool{false, true} {
		queue := cacheTestQueue[int](t, 8)
		for value := 0; value < 4; value++ {
			if err := queue.Enqueue(value); err != nil {
				t.Fatal(err)
			}
		}
		before := queue.consumerPublished.Load()
		observation := queue.mpscScanCount(before)
		result, retain := queue.mpscValidateCount(before, observation)
		if result != 4 || !retain || observation.observed != 0 || observation.end != 4 {
			t.Fatal(result, retain, observation)
		}
		competing := queue.mpscScanCount(before)
		competingResult, competingRetain := queue.mpscValidateCount(before, competing)
		if competingResult != 4 || !competingRetain {
			t.Fatal("competing validation")
		}
		for want := 0; want < 4; want++ {
			if value, err := queue.Dequeue(); err != nil || value != want {
				t.Fatal(value, err)
			}
		}
		for value := 4; value < 12; value++ {
			if err := queue.Enqueue(value); err != nil {
				t.Fatal(err)
			}
		}
		if queue.completion.records[0].Load() != 9 {
			t.Fatal("start not reused")
		}
		expected := uint64(4)
		if newer {
			if queue.Count() != 8 {
				t.Fatal("new observer")
			}
			expected = 12
		}
		queue.mpscPublishProof(observation.observed, observation.end)
		queue.mpscPublishProof(competing.observed, competing.end)
		if queue.observerCompleted.Load() != expected || result != 4 || competingResult != 4 {
			t.Fatal("delayed CAS", queue.observerCompleted.Load(), result, competingResult)
		}
		if queue.Count() != 8 || queue.observerCompleted.Load() != 12 {
			t.Fatal("fresh prefix")
		}
		if _, err := queue.DequeueBulk(nil); err != nil {
			t.Fatal(err)
		}
		word := queue.consumerPublished.Load()
		if mpscReleased(word) != 4 || mpscFrontier(word) != 12 {
			t.Fatal("acquired state", word)
		}
		for want := 4; want < 12; want++ {
			if value, err := queue.Dequeue(); err != nil || value != want {
				t.Fatal(value, err)
			}
		}
	}
}

func TestMPSCCacheHeadBeforeCache(t *testing.T) {
	// This defensive snapshot isolates the real guard, not a legal stable lifetime.
	queue := cacheTestQueue[int](t, 8)
	queue.producerReservation.head.Store(4)
	queue.observerCompleted.Store(6)
	observation := queue.mpscScanCount(queue.consumerPublished.Load())
	if observation.observed != 6 || observation.result != 0 || observation.end != 0 {
		t.Fatal("cache beyond captured head accepted", observation)
	}
	const source = "mpsc.go"
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	start := strings.Index(body, "func (r *MPSCQueue[T]) mpscScanCount(")
	if start < 0 {
		t.Fatal("scan method absent")
	}
	body = body[start:]
	head := strings.Index(body, "head := r.producerReservation.head.Load()")
	cache := strings.Index(body, "observation.observed = r.observerCompleted.Load()")
	if head < 0 || cache < head {
		t.Fatal("head/cache load order")
	}
	// The SC model covers a legal observer pause between these atomic loads.
}

func TestMPSCCacheAbsoluteBoundaries(t *testing.T) {
	for _, capacity := range []int{1, 8} {
		for _, start := range []uint64{math.MaxUint32 - 2, math.MaxUint64 - uint64(capacity)} {
			queue := cacheTestQueue[int](t, capacity)
			baseTestSeed(&queue.mpscBase, start)
			items := make([]int, capacity)
			for index := range items {
				items[index] = index + 1
			}
			if _, err := queue.EnqueueBulk(items); err != nil {
				t.Fatal(err)
			}
			if queue.Count() != capacity || queue.observerCompleted.Load() != start+uint64(capacity) {
				t.Fatal("absolute frontier", start, queue.observerCompleted.Load())
			}
			for index, want := range items {
				if value, err := queue.Dequeue(); err != nil || value != want {
					t.Fatal(value, err)
				}
				if queue.Count() != capacity-index-1 {
					t.Fatal("wrapped acquired remainder")
				}
			}
			if start == math.MaxUint32-2 {
				if queue.Count() != 0 {
					t.Fatal("wrapped empty")
				}
			}
			// Exclusive Reset precedes any reservation beyond the final legal endpoint.
			queue.Reset()
			if err := queue.Enqueue(9); err != nil {
				t.Fatal(err)
			}
			if queue.Count() != 1 {
				t.Fatal("new lifetime")
			}
		}
	}
}

func TestMPSCCacheJoinedResetHandoff(t *testing.T) {
	queue, err := NewMPSCQueue[*int](
		8,
		WithMaxBatch(4),
		WithExactBatchCounts(),
		WithoutDequeueClearing(),
	)
	if err != nil {
		t.Fatal(err)
	}
	value := 17
	if err := queue.Enqueue(&value); err != nil {
		t.Fatal(err)
	}
	validated := make(chan struct{})
	resume := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	workers.Add(1)
	go func() {
		defer workers.Done()
		before := queue.consumerPublished.Load()
		observation := queue.mpscScanCount(before)
		_, retain := queue.mpscValidateCount(before, observation)
		close(validated)
		select {
		case <-resume:
			if retain {
				queue.mpscPublishProof(observation.observed, observation.end)
			}
		case <-ctx.Done():
		}
	}()
	<-validated
	close(resume)
	workers.Wait()
	if queue.observerCompleted.Load() != 1 {
		t.Fatal("observer was not joined after CAS")
	}
	if _, err := queue.Dequeue(); err != nil {
		t.Fatal(err)
	}
	queue.Reset()
	if queue.producerReservation.head.Load() != 0 ||
		queue.producerReservation.cachedConsumer.Load() != 0 || queue.consumer.cursor != 0 ||
		queue.consumer.frontier != 0 || queue.consumerPublished.Load() != 0 ||
		queue.observerCompleted.Load() != 0 {
		t.Fatal("positions survived Reset")
	}
	for _, pointer := range queue.producerConfig.entries {
		if pointer != nil {
			t.Fatal("payload survived Reset")
		}
	}
	for index := range queue.completion.records {
		if queue.completion.records[index].Load() != 0 {
			t.Fatal("record survived Reset")
		}
	}
	if queue.MaxEnqueueBatch() != 4 || queue.MaxDequeueBatch() != 4 ||
		!queue.consumer.exactBatchCounts || queue.consumer.clearOnDequeue {
		t.Fatal("configuration changed")
	}
	if err := queue.Enqueue(&value); err != nil {
		t.Fatal(err)
	}
	handed := make(chan *MPSCQueue[*int], 1)
	type handoffResult struct {
		value *int
		err   error
	}
	done := make(chan handoffResult, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		owned := <-handed
		got, err := owned.Dequeue()
		done <- handoffResult{value: got, err: err}
	}()
	handed <- queue
	result := <-done
	workers.Wait()
	if result.value != &value || result.err != nil {
		t.Fatal(result.value, result.err)
	}
}

func TestMPSCCacheMultiobserver(t *testing.T) {
	cacheTestConcurrent(t, func(value int64) int64 { return value })
	values := make([]int64, 1200)
	pointers := make([]*int64, len(values))
	for index := range values {
		values[index] = int64(index)
		pointers[index] = &values[index]
	}
	cacheTestConcurrent(t, func(value int64) *int64 { return pointers[value] })
}

func cacheTestConcurrent[T comparable](t *testing.T, encode func(int64) T) {
	t.Helper()
	const producers, perProducer, capacity = 4, 300, 64
	queue := cacheTestQueue[T](t, capacity)
	expected := make(map[T]int, producers*perProducer)
	for tag := 0; tag < producers*perProducer; tag++ {
		expected[encode(int64(tag))] = tag
	}
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
				if err := queue.Enqueue(encode(int64(producer*perProducer + sequence))); err == nil {
					sequence++
				} else {
					runtime.Gosched()
				}
			}
		}(producer)
	}
	observerErrors := make(chan string, 2)
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
				if count < 0 || count > capacity || free < 0 || free > capacity {
					observerErrors <- "observer bounds"
					return
				}
			}
		}()
	}
	next := [producers]int{}
	seen := make([]bool, producers*perProducer)
	for total := 0; total < len(seen); {
		select {
		case <-ctx.Done():
			t.Fatal("accounting deadline", total)
		default:
		}
		value, err := queue.Dequeue()
		if err != nil {
			runtime.Gosched()
			continue
		}
		tag, exists := expected[value]
		if !exists || seen[tag] {
			t.Fatal("unknown identity or duplicate", value)
		}
		producer, sequence := tag/perProducer, tag%perProducer
		if sequence != next[producer] {
			t.Fatal("producer FIFO", producer, sequence)
		}
		seen[tag] = true
		next[producer]++
		total++
	}
	cancel()
	workers.Wait()
	select {
	case message := <-observerErrors:
		t.Fatal(message)
	default:
	}
	if queue.Count() != 0 {
		t.Fatal("drained count")
	}
}

func TestMPSCCacheEnclosingLayout(t *testing.T) {
	cacheTestLayout[int64](t)
	cacheTestLayout[*int64](t)
}

func cacheTestLayout[T any](t *testing.T) {
	t.Helper()
	cached := cacheTestQueue[T](t, 8)
	cachedType := reflect.TypeOf(cached).Elem()
	baseType := reflect.TypeOf(mpscBase[T]{})
	baseSize, cacheOffset, size := uintptr(664), uintptr(792), uintptr(800)
	offsets := []uintptr{0, 168, 312, 464, 656}
	if runtime.GOARCH == "386" {
		baseSize, cacheOffset, size = 616, 744, 752
		offsets = []uintptr{0, 152, 296, 436, 608}
	}
	baseTestLayout[T](t)
	if baseType.Size() != baseSize || cachedType.Size() != size || cachedType.Align() != 8 {
		t.Fatal("enclosing size/alignment")
	}
	if cachedType.Field(0).Name != "mpscBase" ||
		cachedType.Field(0).PkgPath == "" ||
		cachedType.Field(0).Offset != 0 || cachedType.Field(0).Type != baseType ||
		cachedType.Field(1).Offset != baseSize ||
		cachedType.Field(1).Type != reflect.TypeOf([128]byte{}) ||
		cachedType.Field(2).Offset != cacheOffset ||
		cachedType.Field(2).Type != reflect.TypeOf(atomic.Uint64{}) {
		t.Fatal("new gap/cache")
	}
	if unsafe.Pointer(cached) != unsafe.Pointer(&cached.mpscBase) {
		t.Fatal("embedded receiver identity")
	}
	for index, offset := range offsets {
		if baseType.Field(index*2).Offset != offset {
			t.Fatal("base offset", index)
		}
	}
	nestedOffsets := [][]uintptr{{0, 24, 28, 32}, {0, 8}, {0}, {0, 24, 32, 40, 48, 52, 56}}
	if runtime.GOARCH == "386" {
		nestedOffsets = [][]uintptr{{0, 12, 16, 20}, {0, 8}, {0}, {0, 12, 16, 24, 32, 36, 40}}
	}
	// The frozen helper checks nested names/types. Check their offsets on the embedded value too.
	for index := 0; index < baseType.NumField(); index++ {
		field := baseType.Field(index)
		t.Logf("base field=%s type=%s offset=%d", field.Name, field.Type, field.Offset)
		if index < 8 && index%2 == 0 {
			for nested := 0; nested < field.Type.NumField(); nested++ {
				member := field.Type.Field(nested)
				if member.Offset != nestedOffsets[index/2][nested] {
					t.Fatal("nested offset", member)
				}
				t.Logf("nested=%s.%s type=%s offset=%d", field.Name, member.Name,
					member.Type, field.Offset+member.Offset)
			}
		}
	}
	for _, queue := range []*mpscBase[T]{&cached.mpscBase} {
		object := uintptr(unsafe.Pointer(queue))
		payload := uintptr(unsafe.Pointer(unsafe.SliceData(queue.producerConfig.entries)))
		records := uintptr(unsafe.Pointer(unsafe.SliceData(queue.completion.records)))
		head := uintptr(unsafe.Pointer(&queue.producerReservation.head))
		publication := uintptr(unsafe.Pointer(&queue.consumerPublished))
		if head%8 != 0 || publication%8 != 0 || (object+cacheOffset)%8 != 0 {
			t.Fatal("atomic alignment")
		}
		for index := range queue.completion.records {
			if uintptr(unsafe.Pointer(&queue.completion.records[index]))%8 != 0 {
				t.Fatal("record alignment", index)
			}
		}
		t.Logf("arch=%s elem=%s size=%d base=%d cacheOffset=%d align=%d",
			runtime.GOARCH, reflect.TypeOf((*T)(nil)).Elem(), size, baseSize, cacheOffset,
			cachedType.Align())
		for name, address := range map[string]uintptr{
			"object": object, "payload": payload, "records": records, "cache": object + cacheOffset,
		} {
			t.Logf("%s=%#x mod64=%d mod128=%d", name, address, address%64, address%128)
		}
	}
	runtime.KeepAlive(cached)
}
