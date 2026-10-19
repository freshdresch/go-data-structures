package ring_test

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// spinPolls is how many dequeue attempts a spinThenYield wait cycle makes, the first included,
// before it yields the processor.
const spinPolls = 64

// ringCapacity is the number of slots every implementation is built with. Today's ring holds one
// fewer, 1023, until the full-capacity change.
const ringCapacity = 1024

// multiWorkers is the number of goroutines on each multi side of a mode.
const multiWorkers = 4

// ptrPoolSize is the number of values the ptr element type points into, in rotation.
const ptrPoolSize = 1024

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
func consume[T any, S sink[T]](source queue[T], producersDone *atomic.Bool, dst []T,
	recorder S, wait waiter,
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
		for unsent := batch; len(unsent) > 0; {
			moved := target.enqueue(unsent)
			if moved == 0 {
				runtime.Gosched()
			}
			unsent = unsent[moved:]
		}
		sent += len(batch)
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

// runThroughput moves total elements from producers to consumers through the queue newQueue
// builds, in batches of batch, and returns what it measured. Consumers yield on every empty
// poll. The only error it returns is from newQueue.
func runThroughput[T any, S sink[T]](timer benchTimer, newQueue func() (queue[T], error),
	producers, consumers, batch, total int, fill fillFunc[T], sinks func(consumer int) S,
) (throughputResult, error) {
	source, err := newQueue()
	if err != nil {
		return throughputResult{}, err
	}
	bufs := make([][]T, producers)
	for p := range bufs {
		bufs[p] = make([]T, batch)
	}
	dsts := make([][]T, consumers)
	recorders := make([]S, consumers)
	for c := range dsts {
		dsts[c] = make([]T, batch)
		recorders[c] = sinks(c)
	}
	counts := make([]int, consumers)

	gcCycles := runTimed(timer, func() {
		var producersDone atomic.Bool
		var producerGroup, consumerGroup sync.WaitGroup
		for c := range consumers {
			consumerGroup.Go(func() {
				counts[c] = consume(source, &producersDone, dsts[c], recorders[c],
					newWaiter(yieldOnly))
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
func runChanThroughput[T any, S sink[T]](timer benchTimer, producers, consumers, total int,
	fill fillFunc[T], sinks func(consumer int) S,
) throughputResult {
	ch := make(chan T, ringCapacity)
	bufs := make([][]T, producers)
	for p := range bufs {
		bufs[p] = make([]T, 1)
	}
	dsts := make([][]T, consumers)
	recorders := make([]S, consumers)
	for c := range dsts {
		dsts[c] = make([]T, 1)
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
}

// benchModes lists every mode, with multiWorkers goroutines on each multi side.
var benchModes = []benchMode{
	{name: "SPSC", producers: 1, consumers: 1},
	{name: "MPSC", producers: multiWorkers, consumers: 1},
	{name: "SPMC", producers: 1, consumers: multiWorkers},
	{name: "MPMC", producers: multiWorkers, consumers: multiWorkers},
}

// BenchmarkThroughput measures moving b.N elements from producers to consumers, so ns/op is
// nanoseconds per element transferred end to end.
func BenchmarkThroughput(b *testing.B) {
	for _, mode := range benchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			for _, batch := range []int{1, 8, 32, 64} {
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
func benchmarkThroughputImpls[T any](b *testing.B, mode benchMode, batch int,
	fill fillFunc[T],
) {
	elemBytes := int64(unsafe.Sizeof(*new(T)))
	for _, impl := range adapterImpls[T](mode) {
		b.Run("impl="+impl.name, func(b *testing.B) {
			b.SetBytes(elemBytes)
			result, err := runThroughput(b, impl.newQueue, mode.producers, mode.consumers,
				batch, b.N, fill, noopSinks[T])
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
		b.SetBytes(elemBytes)
		result := runChanThroughput(b, mode.producers, mode.consumers, b.N, fill,
			noopSinks[T])
		reportThroughput(b, result, b.N)
	})
}
