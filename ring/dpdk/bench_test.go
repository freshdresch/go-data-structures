package dpdk_test

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/freshdresch/go-data-structures/ring/dpdk"
)

// spinPolls is how many dequeue attempts a spinThenYield wait cycle makes, the first included,
// before it yields the processor.
const spinPolls = 64

// ringCapacity is the number of slots every implementation is built with, and the number of
// elements each holds.
const ringCapacity = 1024

// multiWorkers is the number of goroutines on each multi side of a mode.
const multiWorkers = 4

// ptrPoolSize is the number of values the ptr element type points into, in rotation.
const ptrPoolSize = 1024

// lateThreshold is how far a loaded producer may fall behind its schedule before the time it spends
// further behind counts as late time.
const lateThreshold = time.Millisecond

// paceSpinWindow is how close to a batch's due time a loaded producer spins instead of yielding,
// so it sends on time without holding a CPU for a whole time slice while it waits.
const paceSpinWindow = 5 * time.Microsecond

// seqBits is how many low bits of a packed int64 element hold the sequence number. The producer
// ID is in the bits above them.
const seqBits = 56

// queue is the shape the harness drives. Each method moves as many elements as it can right
// now, without blocking, and returns how many it moved.
type queue[T any] interface {
	enqueue(items []T) int
	dequeue(dst []T) int
}

// pollPolicy decides what a consumer does after a dequeue attempt that moved nothing.
type pollPolicy int

const (
	// yieldOnly yields with runtime.Gosched after every empty poll, so a waiting goroutine never
	// burns its time slice. The throughput and RingAPI benchmarks use it.
	yieldOnly pollPolicy = iota
	// spinThenYield polls up to spinPolls times before it yields, so a waiting goroutine sees a
	// new element without paying for a yield. The latency benchmarks use it, because yielding on
	// every empty poll mostly measures the yield.
	spinThenYield
)

// waiter holds the bounded spin that every polling loop shares. yieldOnly makes one dequeue
// attempt per wait cycle, and yields when that attempt moved nothing. spinThenYield makes up to
// spinPolls attempts per wait cycle, the first included, and yields when the last of them moved
// nothing. A successful dequeue starts a new cycle. yield is injectable, so tests can observe the
// wait cycle.
type waiter struct {
	policy     pollPolicy
	emptyPolls int
	yield      func()
}

// newWaiter returns a waiter that follows policy and yields with runtime.Gosched.
func newWaiter(policy pollPolicy) waiter {
	return waiter{policy: policy, yield: runtime.Gosched}
}

// afterEmptyPoll counts a dequeue attempt that moved nothing, and yields when that attempt was
// the last of the wait cycle, so the next attempt starts a new cycle.
func (w *waiter) afterEmptyPoll() {
	w.emptyPolls++
	if w.policy == spinThenYield && w.emptyPolls < spinPolls {
		return
	}
	w.emptyPolls = 0
	w.yield()
}

// restart starts a new wait cycle after a dequeue attempt that moved elements.
func (w *waiter) restart() {
	w.emptyPolls = 0
}

// sink receives each batch a consumer dequeues. The batch is only valid during the call, because
// the consumer reuses its buffer.
type sink[T any] interface {
	record(batch []T)
}

// recordSink keeps a copy of every element it receives, so tests can check what was dequeued.
type recordSink[T any] struct {
	items []T
}

func (r *recordSink[T]) record(batch []T) {
	r.items = append(r.items, batch...)
}

// consume dequeues from source into dst until the producers are done and source is empty, passes
// each dequeued batch to recorder, and returns how many elements it received. dst must not be
// empty. The caller supplies wait, built with newWaiter, so tests can observe the wait cycle.
//
// Important: it loads producersDone before each dequeue attempt, and returns only when the flag
// was already set before an attempt that moved nothing. The reverse order, dequeue and then load
// the flag, can return while a producer's last elements are still in flight. The dequeue finds the
// queue empty, then the producer enqueues its last elements and finishes, and then the flag load
// sees the flag set.
func consume[T any, S sink[T]](
	source queue[T],
	producersDone *atomic.Bool,
	dst []T,
	recorder S,
	wait waiter,
) int {
	received := 0
	for {
		done := producersDone.Load()
		moved := source.dequeue(dst)
		if moved > 0 {
			recorder.record(dst[:moved])
			received += moved
			wait.restart()
			continue
		}
		if done {
			return received
		}
		wait.afterEmptyPoll()
	}
}

// noopSink discards every batch. Benchmarks use it, so recording costs nothing.
type noopSink[T any] struct{}

func (noopSink[T]) record(_ []T) {}

// noopSinks returns a noopSink for any consumer, so benchmarks can pass it to the runs.
func noopSinks[T any](_ int) noopSink[T] {
	return noopSink[T]{}
}

// int64x8 is a 64-byte element without pointers.
type int64x8 [8]int64

// item is what the ptr element type points to.
type item struct {
	value int64
}

// newItemPool returns the ptrPoolSize items that ptr elements point into, each holding its
// index.
func newItemPool() []item {
	pool := make([]item, ptrPoolSize)
	for k := range pool {
		pool[k].value = int64(k)
	}
	return pool
}

// fillFunc writes len(buf) elements for producer, where element k carries sequence nextSeq+k.
type fillFunc[T any] func(buf []T, producer int, nextSeq int64)

// packInt64 combines a producer ID and a sequence number into one element.
func packInt64(producer int, seq int64) int64 {
	return int64(producer)<<seqBits | seq
}

// unpackInt64 returns the producer ID and sequence number packInt64 combined into v.
func unpackInt64(v int64) (producer int, seq int64) {
	return int(v >> seqBits), v & (1<<seqBits - 1)
}

func fillInt64(buf []int64, producer int, nextSeq int64) {
	for k := range buf {
		buf[k] = packInt64(producer, nextSeq+int64(k))
	}
}

// fillInt64x8 writes the packed producer and sequence into element 0 and leaves the rest zero.
func fillInt64x8(buf []int64x8, producer int, nextSeq int64) {
	for k := range buf {
		buf[k] = int64x8{packInt64(producer, nextSeq+int64(k))}
	}
}

// fillPtr returns a fillFunc that points each element into pool, in rotation by sequence number,
// so producers never allocate. Pointer elements carry no order information.
func fillPtr(pool []item) fillFunc[*item] {
	return func(buf []*item, _ int, nextSeq int64) {
		for k := range buf {
			buf[k] = &pool[(nextSeq+int64(k))%int64(len(pool))]
		}
	}
}

// produce sends total elements to target in batches of len(buf), the last one shorter when total
// does not divide evenly. It refills buf before each batch and retries the unsent remainder,
// yielding whenever an attempt moves nothing.
func produce[T any](target queue[T], buf []T, total, producer int, fill fillFunc[T]) {
	for sent := 0; sent < total; {
		batch := buf[:min(len(buf), total-sent)]
		fill(batch, producer, int64(sent))
		enqueueAll(target, batch)
		sent += len(batch)
	}
}

// enqueueAll enqueues every element of items on target, retrying the unsent remainder and
// yielding whenever an attempt moves nothing. A full queue is normal backpressure, so it retries.
func enqueueAll[T any](target queue[T], items []T) {
	for unsent := items; len(unsent) > 0; {
		moved := target.enqueue(unsent)
		if moved == 0 {
			runtime.Gosched()
		}
		unsent = unsent[moved:]
	}
}

// producerShare returns how many of total elements producer sends when they are split across
// producers as evenly as possible. The first total % producers producers send one more.
func producerShare(total, producers, producer int) int {
	share := total / producers
	if producer < total%producers {
		share++
	}
	return share
}

// setElemBytes sets the benchmark's bytes per operation to the size of one T, so throughput
// reports MB/s for the element type.
func setElemBytes[T any](b *testing.B) {
	b.Helper()
	b.SetBytes(int64(unsafe.Sizeof(*new(T))))
}

// benchTimer is the part of *testing.B the runs use, so tests can run them without a benchmark.
type benchTimer interface {
	ResetTimer()
	StopTimer()
}

// readGCCycles returns the number of completed garbage-collection cycles. It reads
// runtime/metrics, which does not stop the world as runtime.ReadMemStats does.
func readGCCycles() uint64 {
	samples := []metrics.Sample{{Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

// gcPerMillion returns cycles per million elements. It returns 0 when elements is not positive.
func gcPerMillion(cycles uint64, elements int) float64 {
	if elements <= 0 {
		return 0
	}
	return float64(cycles) * 1e6 / float64(elements)
}

// runTimed runs work as the timed region of a run and returns how many garbage-collection cycles
// completed during it. It collects garbage first, so no collection left over from setup runs
// inside the timed region.
func runTimed(timer benchTimer, work func()) uint64 {
	runtime.GC()
	before := readGCCycles()
	timer.ResetTimer()
	work()
	timer.StopTimer()
	return readGCCycles() - before
}

// throughputResult is what one throughput run measured.
type throughputResult struct {
	received   int
	gcCycles   uint64
	gcPerMelem float64
}

// newThroughputResult sums the consumers' counts and derives the GC rate per million elements.
func newThroughputResult(counts []int, gcCycles uint64, total int) throughputResult {
	received := 0
	for _, count := range counts {
		received += count
	}
	return throughputResult{
		received:   received,
		gcCycles:   gcCycles,
		gcPerMelem: gcPerMillion(gcCycles, total),
	}
}

// newBuffers isolates occupied worker buffers with at least 128 bytes of exterior guards on
// each side. Separate small allocations alone can share a cache line with neighboring objects.
func newBuffers[T any](count, size int) [][]T {
	const benchmarkBufferGuardBytes = 128
	buffers := make([][]T, count)
	for i := range buffers {
		guard := 0
		elementSize := unsafe.Sizeof(*new(T))
		if size > 0 && elementSize > 0 {
			guard = int(1 + (benchmarkBufferGuardBytes-1)/elementSize)
		}
		backing := make([]T, guard+size+guard)
		buffers[i] = backing[guard : guard+size : guard+size]
	}
	return buffers
}

// runThroughput moves total elements from producers to consumers through the queue newQueue
// builds, in batches of batch, and returns what it measured. Consumers yield on every empty
// poll. The only error it returns is from newQueue.
func runThroughput[T any, S sink[T]](
	timer benchTimer,
	newQueue func() (queue[T], error),
	producers, consumers, batch, total int,
	fill fillFunc[T],
	sinks func(consumer int) S,
) (throughputResult, error) {
	source, err := newQueue()
	if err != nil {
		return throughputResult{}, err
	}
	bufs := newBuffers[T](producers, batch)
	dsts := newBuffers[T](consumers, batch)
	recorders := make([]S, consumers)
	for c := range recorders {
		recorders[c] = sinks(c)
	}
	counts := make([]int, consumers)

	gcCycles := runTimed(timer, func() {
		var producersDone atomic.Bool
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range consumers {
			consumerGroup.Go(func() {
				counts[c] = consume(source, &producersDone, dsts[c], recorders[c], newWaiter(yieldOnly))
			})
		}
		for p := range producers {
			producerGroup.Go(func() {
				produce(source, bufs[p], producerShare(total, producers, p), p, fill)
			})
		}
		producerGroup.Wait()
		producersDone.Store(true)
		consumerGroup.Wait()
	})
	return newThroughputResult(counts, gcCycles, total), nil
}

// runChanThroughput moves total elements from producers to consumers through a channel of
// ringCapacity slots, with blocking sends and range loops, and returns what it measured. The
// channel is closed once every producer is done.
func runChanThroughput[T any, S sink[T]](
	timer benchTimer,
	producers, consumers, total int,
	fill fillFunc[T],
	sinks func(consumer int) S,
) throughputResult {
	ch := make(chan T, ringCapacity)
	bufs := newBuffers[T](producers, 1)
	dsts := newBuffers[T](consumers, 1)
	recorders := make([]S, consumers)
	for c := range recorders {
		recorders[c] = sinks(c)
	}
	counts := make([]int, consumers)

	gcCycles := runTimed(timer, func() {
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range consumers {
			consumerGroup.Go(func() {
				counts[c] = consumeChan(ch, dsts[c], recorders[c])
			})
		}
		for p := range producers {
			producerGroup.Go(func() {
				produceChan(ch, bufs[p], producerShare(total, producers, p), p, fill)
			})
		}
		producerGroup.Wait()
		close(ch)
		consumerGroup.Wait()
	})
	return newThroughputResult(counts, gcCycles, total)
}

// produceChan sends total elements to ch one at a time, refilling buf, which holds one element,
// before each send.
func produceChan[T any](ch chan<- T, buf []T, total, producer int, fill fillFunc[T]) {
	for seq := range total {
		fill(buf, producer, int64(seq))
		ch <- buf[0]
	}
}

// consumeChan receives from ch until it is closed and drained, passes each element to recorder
// through dst, which holds one element, and returns how many it received. Recording through dst
// keeps the consumer from allocating, where a slice literal would escape to the heap.
func consumeChan[T any, S sink[T]](ch <-chan T, dst []T, recorder S) int {
	received := 0
	for v := range ch {
		dst[0] = v
		recorder.record(dst)
		received++
	}
	return received
}

// reportThroughput reports a throughput run's GC metrics, and fails the benchmark when the run
// did not receive every one of the total elements it sent.
func reportThroughput(b *testing.B, result throughputResult, total int) {
	b.Helper()
	if result.received != total {
		b.Fatalf("received %d elements, want %d", result.received, total)
	}
	b.ReportMetric(float64(result.gcCycles), "gc-cycles")
	b.ReportMetric(result.gcPerMelem, "gc/Melem")
}

// benchMode is how many producers and consumers a run has.
type benchMode struct {
	name      string
	producers int
	consumers int
	// relaxed is whether the mode's multi sides use relaxed tail sync.
	relaxed bool
}

// The named modes below use multiWorkers goroutines on each multi side.
var (
	// spscMode has one producer and one consumer.
	spscMode = benchMode{name: "SPSC", producers: 1, consumers: 1}
	// mpscMode has many producers and one consumer.
	mpscMode = benchMode{name: "MPSC", producers: multiWorkers, consumers: 1}
	// spmcMode has one producer and many consumers.
	spmcMode = benchMode{name: "SPMC", producers: 1, consumers: multiWorkers}
	// mpmcMode has many producers and many consumers.
	mpmcMode = benchMode{name: "MPMC", producers: multiWorkers, consumers: multiWorkers}
	// mpmcRelaxedMode has many producers and many consumers, and both sides use relaxed tail
	// sync.
	mpmcRelaxedMode = benchMode{
		name:      "MPMC-RTS",
		producers: multiWorkers,
		consumers: multiWorkers,
		relaxed:   true,
	}
)

// benchModes lists every mode, in the order the throughput benchmarks run them.
var benchModes = []benchMode{spscMode, mpscMode, spmcMode, mpmcMode}

// benchBatches lists the batch sizes the throughput benchmarks run at.
var benchBatches = []int{1, 8, 32, 64}

// BenchmarkThroughput measures moving b.N elements from producers to consumers, so ns/op is
// nanoseconds per element transferred end to end.
func BenchmarkThroughput(b *testing.B) {
	for _, mode := range benchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			for _, batch := range benchBatches {
				b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
					b.Run("elem=int64", func(b *testing.B) {
						benchmarkThroughputImpls(b, mode, batch, fillInt64)
					})
					b.Run("elem=int64x8", func(b *testing.B) {
						benchmarkThroughputImpls(b, mode, batch, fillInt64x8)
					})
					b.Run("elem=ptr", func(b *testing.B) {
						benchmarkThroughputImpls(b, mode, batch, fillPtr(newItemPool()))
					})
				})
			}
		})
	}
}

// benchmarkThroughputImpls runs one impl sub-benchmark for each adapter, and one for chan at
// batch size 1, because chan has no batch operation.
func benchmarkThroughputImpls[T any](b *testing.B, mode benchMode, batch int, fill fillFunc[T]) {
	for _, impl := range throughputImpls[T](mode) {
		b.Run("impl="+impl.name, func(b *testing.B) {
			setElemBytes[T](b)
			result, err := runThroughput(
				b,
				impl.newQueue,
				mode.producers,
				mode.consumers,
				batch,
				b.N,
				fill,
				noopSinks[T],
			)
			if err != nil {
				b.Fatal(err)
			}
			reportThroughput(b, result, b.N)
		})
	}
	if batch != 1 {
		return
	}
	b.Run("impl=chan", func(b *testing.B) {
		setElemBytes[T](b)
		result := runChanThroughput(b, mode.producers, mode.consumers, b.N, fill, noopSinks[T])
		reportThroughput(b, result, b.N)
	})
}

// ringAPI is a style of ring call that BenchmarkRingAPI measures.
type ringAPI int

const (
	// apiSingle is Enqueue and Dequeue, one element per call.
	apiSingle ringAPI = iota
	// apiBurst is EnqueueBurst and DequeueBurst, which move as many elements as they can.
	apiBurst
	// apiBulk is EnqueueBulk and DequeueBulk, which move all of the elements or none.
	apiBulk
)

// ringAPIs lists every style BenchmarkRingAPI runs through the queue harness.
var ringAPIs = []ringAPI{apiSingle, apiBurst, apiBulk}

// ringAPIModes are the modes BenchmarkRingAPI runs in, SPSC, MPMC, and MPMC with relaxed tail
// sync.
var ringAPIModes = []benchMode{spscMode, mpmcMode, mpmcRelaxedMode}

// String returns the name api has in BenchmarkRingAPI's sub-benchmark names.
func (api ringAPI) String() string {
	switch api {
	case apiSingle:
		return "single"
	case apiBurst:
		return "burst"
	case apiBulk:
		return "bulk"
	default:
		return fmt.Sprintf("ringAPI(%d)", int(api))
	}
}

// batches returns the batch sizes api runs at. apiSingle moves one element per call, so it runs
// at batch size 1 only.
func (api ringAPI) batches() []int {
	if api == apiSingle {
		return []int{1}
	}
	return benchBatches
}

// total returns how many elements a run of api sends when n are requested. For apiBulk it rounds
// n up to a multiple of batch times producers. Rounding to the batch keeps the total in whole
// batches, because an all-or-nothing dequeue cannot take a smaller remainder. The factor of the
// producer count makes each producer's share whole batches too, so no producer issues a short
// bulk enqueue and every call is the measured operation.
func (api ringAPI) total(n, batch, producers int) int {
	if api != apiBulk {
		return n
	}
	multiple := batch * producers
	return (n + multiple - 1) / multiple * multiple
}

// BenchmarkRingAPI reports ns/elem per verified concurrent transfer. Raw ns/op retains the
// Go iteration denominator. api=seq measures one enqueue/dequeue pair in one goroutine.
func BenchmarkRingAPI(b *testing.B) {
	for _, mode := range ringAPIModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			for _, api := range ringAPIs {
				b.Run("api="+api.String(), func(b *testing.B) {
					benchmarkRingAPIBatches(b, mode, api)
				})
			}
		})
	}
	b.Run("api=seq", func(b *testing.B) {
		b.Run("elem=int64", func(b *testing.B) {
			benchmarkRingAPISeq(b, fillInt64)
		})
		b.Run("elem=ptr", func(b *testing.B) {
			benchmarkRingAPISeq(b, fillPtr(newItemPool()))
		})
	})
}

// benchmarkRingAPIBatches runs one batch sub-benchmark for each batch size api runs at. apiSingle
// has no batch part in its name, so its batch-1 case runs directly under the api level.
func benchmarkRingAPIBatches(b *testing.B, mode benchMode, api ringAPI) {
	for _, batch := range api.batches() {
		if api == apiSingle {
			benchmarkRingAPIElems(b, mode, api, batch)
			continue
		}
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			benchmarkRingAPIElems(b, mode, api, batch)
		})
	}
}

// benchmarkRingAPIElems runs one elem sub-benchmark for each element type.
func benchmarkRingAPIElems(b *testing.B, mode benchMode, api ringAPI, batch int) {
	b.Run("elem=int64", func(b *testing.B) {
		benchmarkRingAPIRun(b, mode, api, batch, fillInt64)
	})
	b.Run("elem=ptr", func(b *testing.B) {
		benchmarkRingAPIRun(b, mode, api, batch, fillPtr(newItemPool()))
	})
}

// benchmarkRingAPIRun normalizes the stopped timed interval by verified transfers.
// Bulk rounds the transfer total, so elem/op records its ratio to Go iterations.
func benchmarkRingAPIRun[T any](
	b *testing.B,
	mode benchMode,
	api ringAPI,
	batch int,
	fill fillFunc[T],
) {
	setElemBytes[T](b)
	total := api.total(b.N, batch, mode.producers)
	newQueue := func() (queue[T], error) {
		return newRingAPIQueue[T](api, mode)
	}
	result, err := runThroughput(
		b,
		newQueue,
		mode.producers,
		mode.consumers,
		batch,
		total,
		fill,
		noopSinks[T],
	)
	if err != nil {
		b.Fatal(err)
	}
	reportConcurrentAPIMetrics(b, api, result, total)
}

// benchmarkRingAPISeq calls Enqueue and then Dequeue on one element per b.Loop iteration, in one
// goroutine, on a ring with no options, and reports the GC metrics like every throughput run.
func benchmarkRingAPISeq[T any](b *testing.B, fill fillFunc[T]) {
	setElemBytes[T](b)
	created, err := dpdk.New[T](ringCapacity)
	if err != nil {
		b.Fatal(err)
	}
	elems := make([]T, 1)
	fill(elems, 0, 0)
	elem := elems[0]
	iterations := 0
	gcCycles := runTimed(b, func() {
		for b.Loop() {
			if err := created.Enqueue(elem); err != nil {
				b.Fatal(err)
			}
			if _, err := created.Dequeue(); err != nil {
				b.Fatal(err)
			}
			iterations++
		}
	})
	result := newThroughputResult([]int{iterations}, gcCycles, iterations)
	reportThroughput(b, result, iterations)
}

func ringAPIMetrics(
	elapsed time.Duration,
	iterations, expected, received int,
) (nsPerElem, elemPerOp float64, err error) {
	if elapsed < 0 || iterations <= 0 || received <= 0 {
		return 0, 0, fmt.Errorf("invalid metric duration or denominator")
	}
	if received != expected {
		return 0, 0, fmt.Errorf("received %d elements, want %d", received, expected)
	}
	return float64(elapsed.Nanoseconds()) / float64(received),
		float64(received) / float64(iterations), nil
}
