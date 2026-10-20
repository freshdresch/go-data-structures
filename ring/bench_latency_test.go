package ring_test

import (
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// subBucketBits is log2 of subBuckets.
const subBucketBits = 4

// subBuckets is how many linear sub-buckets the histogram splits each power of two into.
const subBuckets = 1 << subBucketBits

// durationValueBits is the number of value bits in a non-negative time.Duration, which is an
// int64 without its sign bit.
const durationValueBits = 63

// histogramGroups is how many groups of subBuckets buckets the histogram has. A time.Duration
// that is not negative has durationValueBits value bits, so there is one group for each power of
// two from 2^subBucketBits to 2^(durationValueBits-1), plus group 0 for the exact values below
// subBuckets.
const histogramGroups = 1 + durationValueBits - subBucketBits

// histogramBuckets is the number of buckets in a histogram.
const histogramBuckets = histogramGroups * subBuckets

// histogram is a log-linear latency histogram in the style of HdrHistogram. It splits each power
// of two into subBuckets linear sub-buckets. It records any time.Duration, from 0 to math.MaxInt64
// nanoseconds, and records a negative duration as 0. Values below 2*subBuckets ns are kept
// exactly. A percentile is the lowest value of the bucket it falls in, so it is never above the
// true value. A bucket that starts at lowest is lowest/subBuckets wide or narrower, so a value in
// it is less than (subBuckets+1)/subBuckets times lowest. For example, value < (17/16)*lowest
// gives a relative error of 1 - lowest/value < 1 - 16/17 = 1/17. So the percentile is below the
// true value by less than 1/(subBuckets+1), about 5.9%. The maximum is kept exactly.
//
// The zero value is empty and ready to use. record does not allocate, so it can run in timed
// code. A histogram is not safe for concurrent use, so each goroutine records into its own and
// the owner merges them after the goroutines stop.
type histogram struct {
	counts  [histogramBuckets]int
	total   int
	largest time.Duration
}

// record adds one value to the histogram.
func (h *histogram) record(value time.Duration) {
	value = max(value, 0)
	h.counts[histogramIndex(value)]++
	h.total++
	h.largest = max(h.largest, value)
}

// merge adds every value recorded in other to h. It does not change other.
func (h *histogram) merge(other *histogram) {
	for k, n := range other.counts {
		h.counts[k] += n
	}
	h.total += other.total
	h.largest = max(h.largest, other.largest)
}

// count returns how many values the histogram has recorded.
func (h *histogram) count() int {
	return h.total
}

// max returns the largest value recorded, or 0 when the histogram is empty.
func (h *histogram) max() time.Duration {
	return h.largest
}

// percentile returns the lowest value of the bucket that holds the rank-th smallest value, where
// rank is ceil(p/100 * count), at least 1. p is a percent from 0 to 100. Values outside that range
// are clamped to the nearest end, and NaN is treated as 0. An empty histogram returns 0.
func (h *histogram) percentile(p float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	if math.IsNaN(p) {
		p = 0
	}
	p = min(max(p, 0), 100)
	// The relative epsilon keeps float rounding from pushing the rank one too high. It is exact
	// below about 9.8e8 samples for any p with at most three decimal places, and far higher for p99.9.
	exact := p * float64(h.total) / 100
	rank := int(math.Ceil(exact * (1 - 1e-14)))
	rank = min(max(rank, 1), h.total)
	seen := 0
	for k, n := range h.counts {
		seen += n
		if seen >= rank {
			return histogramLowest(k)
		}
	}
	// A defensive fallback. It is unreachable because total is the sum of the bucket counts, so
	// some bucket always reaches rank.
	return h.largest
}

// histogramIndex returns the bucket that holds value. value must not be negative, because
// record clamps negative durations to 0 before calling it.
func histogramIndex(value time.Duration) int {
	unsigned := uint64(value)
	if unsigned < subBuckets {
		return int(unsigned)
	}
	exponent := bits.Len64(unsigned) - 1
	shift := exponent - subBucketBits
	sub := int(unsigned>>shift) - subBuckets
	return (shift+1)*subBuckets + sub
}

// histogramLowest returns the lowest value that falls in bucket index, the inverse of
// histogramIndex.
func histogramLowest(index int) time.Duration {
	group := index / subBuckets
	sub := index % subBuckets
	if group == 0 {
		return time.Duration(sub)
	}
	return time.Duration(subBuckets+sub) << (group - 1)
}

// BenchmarkClock measures the clock every latency benchmark reads. read-ns is the cost of one
// time.Since call. resolution-ns is the smallest non-zero difference between consecutive
// readings. On a clock finer than one read, it is about read-ns, because each step is one read.
// Latency differences below the larger of the two are not meaningful.
func BenchmarkClock(b *testing.B) {
	// clockReadings is how many consecutive readings the resolution is the minimum over.
	const clockReadings = 1_000_000
	base := time.Now()
	for b.Loop() {
		time.Since(base)
	}
	readNanos := float64(b.Elapsed().Nanoseconds()) / float64(b.N)

	resolution := time.Duration(math.MaxInt64)
	previous := time.Since(base)
	for range clockReadings {
		current := time.Since(base)
		if step := current - previous; step > 0 {
			resolution = min(resolution, step)
		}
		previous = current
	}
	if resolution == math.MaxInt64 {
		b.Fatalf("the clock did not advance over %d readings", clockReadings)
	}
	b.ReportMetric(readNanos, "read-ns")
	b.ReportMetric(float64(resolution.Nanoseconds()), "resolution-ns")
}

// reportLatency reports h's percentiles and maximum in nanoseconds, as prefix-p50, prefix-p90,
// prefix-p99, prefix-p99.9, and prefix-max.
func reportLatency(b *testing.B, prefix string, h *histogram) {
	b.Helper()
	b.ReportMetric(float64(h.percentile(50).Nanoseconds()), prefix+"-p50")
	b.ReportMetric(float64(h.percentile(90).Nanoseconds()), prefix+"-p90")
	b.ReportMetric(float64(h.percentile(99).Nanoseconds()), prefix+"-p99")
	b.ReportMetric(float64(h.percentile(99.9).Nanoseconds()), prefix+"-p99.9")
	b.ReportMetric(float64(h.max().Nanoseconds()), prefix+"-max")
}

// handoffResult is what one handoff run measured. latency holds one handoff, half a round trip,
// for each round trip the pinger recorded.
type handoffResult struct {
	latency  *histogram
	recorded int
}

// handoffLatency returns one handoff's latency, half of roundTrip, since a round trip is two
// handoffs.
func handoffLatency(roundTrip time.Duration) time.Duration {
	return roundTrip / 2
}

// warmUpPing is the element the unrecorded warm-up round trip sends. It is negative because the
// recorded round trips send 0, 1, 2, and so on, so the warm-up reply can never pass for one of
// theirs.
const warmUpPing int64 = -1

// runHandoff sends roundTrips elements one at a time from a pinger to a ponger on pings, a queue
// newQueue builds, and back on pongs, a second one. The pinger waits for each reply before it
// sends the next element, and both sides poll with a bounded spin. The only error it returns is
// from newQueue.
func runHandoff(
	timer benchTimer,
	newQueue func() (queue[int64], error),
	roundTrips int,
) (handoffResult, error) {
	pings, err := newQueue()
	if err != nil {
		return handoffResult{}, err
	}
	pongs, err := newQueue()
	if err != nil {
		return handoffResult{}, err
	}
	sent := make([]int64, 1)
	reply := make([]int64, 1)
	forwarded := make([]int64, 1)
	latency := &histogram{}
	recorded := 0
	base := time.Now()

	// The pinger sets pingerDone after its last round trip. The ponger, through consume,
	// returns once it saw the flag set before a poll that found pings empty, so it never
	// leaves an element unanswered.
	var pingerDone atomic.Bool
	var ponger sync.WaitGroup
	ponger.Go(func() {
		consume(pings, &pingerDone, forwarded, forwardSink{target: pongs}, newWaiter(spinThenYield))
	})
	wait := newWaiter(spinThenYield)
	roundTrip := func(k int64) time.Duration {
		sent[0] = k
		start := time.Since(base)
		enqueueAll(pings, sent)
		for pongs.dequeue(reply) == 0 {
			wait.afterEmptyPoll()
		}
		wait.restart()
		return time.Since(base) - start
	}
	// The warm-up round trip is not recorded. It runs before the timer starts, so the
	// ponger's start-up stays out of the measurement.
	roundTrip(warmUpPing)

	runTimed(timer, func() {
		for k := range roundTrips {
			latency.record(handoffLatency(roundTrip(int64(k))))
			recorded++
		}
	})
	pingerDone.Store(true)
	ponger.Wait()
	return handoffResult{latency: latency, recorded: recorded}, nil
}

// runChanHandoff is runHandoff over two channels of ringCapacity slots, with blocking sends and
// receives, so each handoff includes waking the receiving goroutine. The pinger closes pings
// after its last round trip, which ends the ponger's range loop.
func runChanHandoff(timer benchTimer, roundTrips int) handoffResult {
	pings := make(chan int64, ringCapacity)
	pongs := make(chan int64, ringCapacity)
	latency := &histogram{}
	recorded := 0
	base := time.Now()

	var ponger sync.WaitGroup
	ponger.Go(func() {
		for v := range pings {
			pongs <- v
		}
	})
	roundTrip := func(k int64) time.Duration {
		start := time.Since(base)
		pings <- k
		<-pongs
		return time.Since(base) - start
	}
	// The warm-up round trip is not recorded. It runs before the timer starts, so the
	// ponger's start-up stays out of the measurement.
	roundTrip(warmUpPing)

	runTimed(timer, func() {
		for k := range roundTrips {
			latency.record(handoffLatency(roundTrip(int64(k))))
			recorded++
		}
	})
	close(pings)
	ponger.Wait()
	return handoffResult{latency: latency, recorded: recorded}
}

// forwardSink enqueues every batch it receives on target, so the ponger can answer each ping
// through consume.
type forwardSink struct {
	target queue[int64]
}

func (s forwardSink) record(batch []int64) {
	enqueueAll(s.target, batch)
}

// handoffImpl names an implementation BenchmarkHandoff runs. run makes roundTrips round trips.
type handoffImpl struct {
	name string
	run  func(timer benchTimer, roundTrips int) (handoffResult, error)
}

// handoffImpls returns every implementation BenchmarkHandoff runs, the SPSC adapters and chan.
func handoffImpls() []handoffImpl {
	var impls []handoffImpl
	for _, impl := range adapterImpls[int64](spscMode) {
		impls = append(impls, handoffImpl{
			name: impl.name,
			run: func(timer benchTimer, roundTrips int) (handoffResult, error) {
				return runHandoff(timer, impl.newQueue, roundTrips)
			},
		})
	}
	return append(impls, handoffImpl{
		name: "chan",
		run: func(timer benchTimer, roundTrips int) (handoffResult, error) {
			return runChanHandoff(timer, roundTrips), nil
		},
	})
}

// BenchmarkHandoff measures b.N round trips of one int64 between two goroutines over two SPSC
// queues, with nothing else queued. ns/op is one full round trip, including the pinger's two
// clock reads and recording. A warm-up round trip runs before the timer, so handoff-max does not
// include ponger start-up. The handoff percentiles are half a round trip, one handoff. ring,
// chanpoll, and mutex poll with a bounded spin. chan blocks, so its handoff includes waking a
// parked goroutine.
func BenchmarkHandoff(b *testing.B) {
	for _, impl := range handoffImpls() {
		b.Run("impl="+impl.name, func(b *testing.B) {
			result, err := impl.run(b, b.N)
			if err != nil {
				b.Fatal(err)
			}
			if result.recorded != b.N {
				b.Fatalf("recorded %d round trips, want %d", result.recorded, b.N)
			}
			reportLatency(b, "handoff", result.latency)
		})
	}
}

// dueTime returns when the last element of a loaded producer's batch is due, as an offset from the
// start barrier. rate is the offered rate of all producers together, in elements per second, and
// must be positive. Each producer sends at rate/producers, so one producer's elements are
// interval = producers/rate apart, and producer p is offset by p*interval/producers. The element
// at index k of producer p's schedule is therefore due at (k*producers + p)/rate, which spaces
// the combined arrivals of all producers evenly, 1/rate apart. The batch at batchIndex ends with
// element index (batchIndex+1)*batch - 1.
//
// The due time is computed from the element's position, never by adding up intervals, so float
// error does not accumulate over a run. It is rounded to the nearest nanosecond, and is within a
// nanosecond of exact for any run shorter than about a day.
func dueTime(producer, producers, batchIndex, batch int, rate float64) time.Duration {
	last := (batchIndex+1)*batch - 1
	arrival := last*producers + producer
	return time.Duration(math.Round(float64(arrival) * float64(time.Second) / rate))
}

// lateTracker follows one loaded producer's lateness. The zero value is ready to use, and treats
// the start barrier, offset 0, as the producer's previous attempt. A lateTracker is not safe for
// concurrent use, so each producer feeds its own.
type lateTracker struct {
	// lag holds how far behind its due time each batch's first enqueue attempt started, for the
	// lag-* metrics.
	lag histogram
	// lateTime is the producer's total time more than lateThreshold behind its schedule.
	lateTime time.Duration
	// previousAttempt is the attempt stamp of the batch observe saw last.
	previousAttempt time.Duration
}

// observe records one batch. attempt is when the batch's first enqueue attempt started, the
// batch's attempt stamp, and due is its dueTime. Both are offsets from the start barrier. The lag
// is attempt - due, or 0 when the batch started early, a clamp the histogram applies. Late time
// counts only the time the producer spent more than lateThreshold behind, so observe adds
// attempt - max(previousAttempt, due+lateThreshold) to lateTime when that is positive. The
// producer is more than lateThreshold behind on this batch from due+lateThreshold until it
// attempts the batch, and any part of that stretch before its previous attempt was already
// counted for an earlier batch, so each late instant is counted once. For the first batch, the
// previous attempt is the start barrier, before which the run had not begun. observe assumes a
// producer's attempts never go backward. Each producer reads the monotonic clock in order, so
// previousAttempt only moves forward.
func (l *lateTracker) observe(attempt, due time.Duration) {
	l.lag.record(attempt - due)
	lateSince := max(l.previousAttempt, due+lateThreshold)
	l.lateTime += max(attempt-lateSince, 0)
	l.previousAttempt = attempt
}

// achievedRate returns the elements per second a loaded run moved, dequeued over the time from
// the start barrier, offset 0, to lastDequeue. It returns 0 when lastDequeue is not positive,
// since no time passed to move anything in.
func achievedRate(dequeued int, lastDequeue time.Duration) float64 {
	if lastDequeue <= 0 {
		return 0
	}
	return float64(dequeued) / lastDequeue.Seconds()
}

// sustained reports whether a loaded run kept up with its schedule. achieved and offered are
// rates in elements per second. lateTimes holds each producer's lateTime, and runDuration runs
// from the start barrier to the last dequeue. The run is sustained when achieved is at least 98%
// of offered and every producer, judged on its own, was late for less than 1% of runDuration.
func sustained(
	achieved, offered float64,
	lateTimes []time.Duration,
	runDuration time.Duration,
) bool {
	const minAchievedFraction = 0.98
	const maxLateFraction = 0.01
	if achieved < minAchievedFraction*offered {
		return false
	}
	lateBudget := maxLateFraction * float64(runDuration)
	for _, late := range lateTimes {
		if float64(late) >= lateBudget {
			return false
		}
	}
	return true
}

// capReached reports whether a loaded run has gone on for twice its nominal length, the point at
// which producers stop offering load. elapsed is measured from the start barrier.
func capReached(elapsed, nominal time.Duration) bool {
	return elapsed >= 2*nominal
}

// stamped is the element a loaded run sends. attempt is when its batch's first enqueue attempt
// started, and due is when its batch's last element was due, both in nanoseconds from the start
// barrier.
type stamped struct{ attempt, due int64 }

// loadedResult is what one loaded run measured. wait holds each element's queue wait, from its
// attempt stamp to its dequeue, and sched its schedule latency, from its due time to its dequeue.
// lag holds how far behind its due time each batch's first enqueue attempt started. lateTimes
// holds each producer's late time. lastDequeue is the offset from the start barrier of the last
// dequeue of any consumer. capped reports that the 2× cap stopped at least one producer before it
// sent its share.
type loadedResult struct {
	wait, sched, lag *histogram
	lateTimes        []time.Duration
	sent, dequeued   int
	lastDequeue      time.Duration
	capped           bool
	gcCycles         uint64
	gcPerMelem       float64
}

// loadedSink records the queue wait and schedule latency of every element one loaded consumer
// dequeues, and when it last dequeued. It reads the clock once per batch, so later elements are
// not charged for the clock reads of earlier ones. base is the run's start barrier, set before the
// consumer's first dequeue.
type loadedSink struct {
	base        time.Time
	wait, sched histogram
	lastDequeue time.Duration
}

func (s *loadedSink) record(batch []stamped) {
	now := time.Since(s.base)
	for _, elem := range batch {
		s.wait.record(now - time.Duration(elem.attempt))
		s.sched.record(now - time.Duration(elem.due))
	}
	s.lastDequeue = now
}

// loadedRun is one loaded run's schedule, plus the state its goroutines write, each producer and
// consumer into its own slots, for the owner to merge after they all stop. The trackers and sinks
// are allocated separately, so goroutines writing to neighbors are unlikely to share a cache line,
// which today follows from their size classes.
type loadedRun struct {
	producers, batch int
	rate             float64
	// batches is how many batches each producer sends.
	batches int
	// nominal is how long the run lasts when every producer keeps to its schedule.
	nominal  time.Duration
	trackers []*lateTracker
	sinks    []*loadedSink
	sent     []int
	dequeued []int
	// capped records, for each producer, that it stopped at the 2× cap before sending its share.
	capped []bool
}

// newLoadedRun returns the state for a loaded run of total elements, in which each of producers
// sends total/(producers*batch) whole batches of batch elements, at rate elements per second in
// total. The caller rounds total to a multiple of producers*batch. It returns an error when rate
// is not positive and finite, because the schedule divides by it, when producers, consumers, or
// batch is less than 1, or when total is negative or not a multiple of producers*batch.
func newLoadedRun(producers, consumers, batch int, rate float64, total int) (*loadedRun, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 1) || rate <= 0 {
		return nil, fmt.Errorf("loaded rate %v elements per second is not positive and finite", rate)
	}
	if producers < 1 || consumers < 1 || batch < 1 {
		return nil, fmt.Errorf(
			"loaded run needs at least 1 producer, consumer, and element per "+
				"batch, got %d producers, %d consumers, and batch %d",
			producers, consumers, batch,
		)
	}
	if total < 0 || total%(producers*batch) != 0 {
		return nil, fmt.Errorf(
			"loaded total %d is not a non-negative multiple of %d producers "+
				"times batch %d",
			total, producers, batch,
		)
	}
	run := &loadedRun{
		producers: producers,
		batch:     batch,
		rate:      rate,
		batches:   total / (producers * batch),
		nominal:   time.Duration(float64(total) / rate * float64(time.Second)),
		trackers:  make([]*lateTracker, producers),
		sinks:     make([]*loadedSink, consumers),
		sent:      make([]int, producers),
		dequeued:  make([]int, consumers),
		capped:    make([]bool, producers),
	}
	for p := range run.trackers {
		run.trackers[p] = &lateTracker{}
	}
	for c := range run.sinks {
		run.sinks[c] = &loadedSink{}
	}
	return run, nil
}

// loadedTotal returns how many elements a loaded run sends to last length at rate elements per
// second in total, rounded up to whole batches of batch for each of producers.
func loadedTotal(rate float64, length time.Duration, producers, batch int) int {
	elements := int(math.Ceil(rate * length.Seconds()))
	return wholeBatches(elements, producers, batch)
}

// wholeBatches returns elements rounded up to a multiple of producers*batch, so every producer of
// a loaded run sends whole batches.
func wholeBatches(elements, producers, batch int) int {
	multiple := producers * batch
	return (elements + multiple - 1) / multiple * multiple
}

// pace sends producer's batches through send, each at its dueTime, and returns how many elements
// it sent. Before each batch it reads the clock and stops once capReached says the run has gone
// on for twice its nominal length, and then records that the producer was capped. It stamps each
// batch once, refilling buf, which holds one batch, with the due time and an attempt stamp read
// from the clock before send makes its first enqueue attempt. Only after the send does it feed
// the producer's lateTracker that attempt stamp, never the time the send completed, so the
// histogram write is not part of any element's queue wait.
func (r *loadedRun) pace(base time.Time, producer int, buf []stamped, send func([]stamped)) int {
	tracker := r.trackers[producer]
	sent := 0
	for k := range r.batches {
		now := time.Since(base)
		if capReached(now, r.nominal) {
			r.capped[producer] = true
			break
		}
		due := dueTime(producer, r.producers, k, r.batch, r.rate)
		attempt := waitUntil(base, due, now)
		for i := range buf {
			buf[i] = stamped{attempt: int64(attempt), due: int64(due)}
		}
		send(buf)
		tracker.observe(attempt, due)
		sent += len(buf)
	}
	return sent
}

// waitUntil waits until due, an offset from base, and returns the clock reading at which it
// stopped, which is due or later. now is the current offset. It spins while due is within
// paceSpinWindow, and otherwise yields, so pacing never holds a CPU for a whole time slice.
func waitUntil(base time.Time, due, now time.Duration) time.Duration {
	for now < due {
		if due-now > paceSpinWindow {
			runtime.Gosched()
		}
		now = time.Since(base)
	}
	return now
}

// execute runs one loaded run as the timed region and returns what it measured. Every producer
// and consumer starts, calls ready.Done, and blocks on start. The owner waits for all of them,
// takes base, the start barrier, and then closes start, so goroutine startup never counts as
// latency. runProducer runs producer p and returns how many elements it sent. runConsumer runs
// consumer c, whose sink already has base, and returns how many elements it received. Once every
// producer returns, the owner calls stopConsumers, after which each consumer drains the queue and
// returns.
func (r *loadedRun) execute(
	timer benchTimer,
	runProducer func(p int, base time.Time) int,
	runConsumer func(c int) int,
	stopConsumers func(),
) loadedResult {
	gcCycles := runTimed(timer, func() {
		var ready, producerGroup, consumerGroup sync.WaitGroup
		start := make(chan struct{})
		var base time.Time
		ready.Add(len(r.sinks) + len(r.trackers))
		for c := range r.sinks {
			consumerGroup.Go(func() {
				ready.Done()
				<-start
				r.sinks[c].base = base
				r.dequeued[c] = runConsumer(c)
			})
		}
		for p := range r.trackers {
			producerGroup.Go(func() {
				ready.Done()
				<-start
				r.sent[p] = runProducer(p, base)
			})
		}
		ready.Wait()
		base = time.Now()
		close(start)
		producerGroup.Wait()
		stopConsumers()
		consumerGroup.Wait()
	})
	return r.result(gcCycles)
}

// result merges what the producers and consumers recorded, once they have all stopped.
func (r *loadedRun) result(gcCycles uint64) loadedResult {
	result := loadedResult{
		wait:     &histogram{},
		sched:    &histogram{},
		lag:      &histogram{},
		gcCycles: gcCycles,
	}
	for p, tracker := range r.trackers {
		result.lag.merge(&tracker.lag)
		result.lateTimes = append(result.lateTimes, tracker.lateTime)
		result.sent += r.sent[p]
		result.capped = result.capped || r.capped[p]
	}
	for c, recorded := range r.sinks {
		result.wait.merge(&recorded.wait)
		result.sched.merge(&recorded.sched)
		result.lastDequeue = max(result.lastDequeue, recorded.lastDequeue)
		result.dequeued += r.dequeued[c]
	}
	result.gcPerMelem = gcPerMillion(gcCycles, result.dequeued)
	return result
}

// runLoaded sends total elements from producers to consumers through the queue newQueue builds,
// in batches of batch, at rate elements per second in total, and returns what it measured. The
// caller rounds total to a multiple of producers*batch. Producers retry a full queue, and
// consumers poll with a bounded spin. It returns an error when newLoadedRun rejects the run's
// parameters, or from newQueue.
func runLoaded(
	timer benchTimer,
	newQueue func() (queue[stamped], error),
	producers, consumers, batch int,
	rate float64,
	total int,
) (loadedResult, error) {
	run, err := newLoadedRun(producers, consumers, batch, rate, total)
	if err != nil {
		return loadedResult{}, err
	}
	source, err := newQueue()
	if err != nil {
		return loadedResult{}, err
	}
	bufs := newBuffers[stamped](producers, batch)
	dsts := newBuffers[stamped](consumers, batch)
	var producersDone atomic.Bool
	produceTo := func(p int, base time.Time) int {
		return run.pace(base, p, bufs[p], func(items []stamped) {
			enqueueAll(source, items)
		})
	}
	consumeFrom := func(c int) int {
		return consume(source, &producersDone, dsts[c], run.sinks[c], newWaiter(spinThenYield))
	}
	stop := func() { producersDone.Store(true) }
	return run.execute(timer, produceTo, consumeFrom, stop), nil
}

// runChanLoaded is runLoaded over a channel of ringCapacity slots. Each producer sends a batch as
// that many blocking sends back to back, all under the batch's one attempt stamp, and consumers
// block in a range loop, so chan wait includes waking a parked consumer. The channel is closed
// once every producer is done. It returns an error when newLoadedRun rejects the run's
// parameters.
func runChanLoaded(
	timer benchTimer,
	producers, consumers, batch int,
	rate float64,
	total int,
) (loadedResult, error) {
	run, err := newLoadedRun(producers, consumers, batch, rate, total)
	if err != nil {
		return loadedResult{}, err
	}
	ch := make(chan stamped, ringCapacity)
	bufs := newBuffers[stamped](producers, batch)
	dsts := newBuffers[stamped](consumers, 1)
	produceTo := func(p int, base time.Time) int {
		return run.pace(base, p, bufs[p], func(items []stamped) {
			for _, v := range items {
				ch <- v
			}
		})
	}
	consumeFrom := func(c int) int {
		return consumeChan(ch, dsts[c], run.sinks[c])
	}
	stop := func() { close(ch) }
	return run.execute(timer, produceTo, consumeFrom, stop), nil
}

// reportLoaded reports a loaded run's wait-*, sched-*, and lag-* latencies, its achieved-rate in
// elements per second, sustained as 1 or 0, and its GC metrics. offered is the offered rate in
// elements per second. A run the 2× cap stopped is never sustained.
func reportLoaded(b *testing.B, result loadedResult, offered float64) {
	b.Helper()
	if result.dequeued != result.sent {
		b.Fatalf("dequeued %d elements, want %d", result.dequeued, result.sent)
	}
	reportLatency(b, "wait", result.wait)
	reportLatency(b, "sched", result.sched)
	reportLatency(b, "lag", result.lag)
	achieved := achievedRate(result.dequeued, result.lastDequeue)
	keptUp := !result.capped && sustained(achieved, offered, result.lateTimes, result.lastDequeue)
	sustainedMetric := 0.0
	if keptUp {
		sustainedMetric = 1
	}
	b.ReportMetric(achieved, "achieved-rate")
	b.ReportMetric(sustainedMetric, "sustained")
	b.ReportMetric(float64(result.gcCycles), "gc-cycles")
	b.ReportMetric(result.gcPerMelem, "gc/Melem")
}

// loadedImpl names an implementation BenchmarkLoaded runs. run sends total elements in batches of
// batch at rate elements per second in total.
type loadedImpl struct {
	name string
	run  func(timer benchTimer, batch int, rate float64, total int) (loadedResult, error)
}

// loadedImpls returns every implementation BenchmarkLoaded runs in mode, the adapters and chan.
func loadedImpls(mode benchMode) []loadedImpl {
	var impls []loadedImpl
	for _, impl := range adapterImpls[stamped](mode) {
		impls = append(impls, loadedImpl{
			name: impl.name,
			run: func(timer benchTimer, batch int, rate float64, total int) (loadedResult, error) {
				return runLoaded(timer, impl.newQueue, mode.producers, mode.consumers, batch, rate, total)
			},
		})
	}
	return append(impls, loadedImpl{
		name: "chan",
		run: func(timer benchTimer, batch int, rate float64, total int) (loadedResult, error) {
			return runChanLoaded(timer, mode.producers, mode.consumers, batch, rate, total)
		},
	})
}

// loadedRate is an offered rate BenchmarkLoaded runs at, in elements per second in total, and its
// name in the sub-benchmark names.
type loadedRate struct {
	name string
	rate float64
}

// loadedRates lists the offered rates BenchmarkLoaded runs at.
var loadedRates = []loadedRate{
	{name: "100k", rate: 100_000},
	{name: "1M", rate: 1_000_000},
	{name: "10M", rate: 10_000_000},
}

// loadedBatches lists the batch sizes BenchmarkLoaded runs at.
var loadedBatches = []int{1, 32}

// BenchmarkLoaded measures how long elements wait when producers offer a fixed rate, with every
// producer and consumer started behind a common start barrier. Each run sends two seconds' worth
// of elements at the offered rate, rounded up to whole batches per producer, and ignores b.N, so
// it is meant for -benchtime 1x and ns/op is meaningless. Producers stop offering load at twice
// the nominal length, and such a run reports sustained 0. ring, chanpoll, and mutex consumers poll
// with a bounded spin. chan consumers block, so chan wait includes waking a parked goroutine.
func BenchmarkLoaded(b *testing.B) {
	// runLength is how long a run lasts at the offered rate when it keeps up.
	const runLength = 2 * time.Second
	for _, mode := range benchModes {
		b.Run("mode="+mode.name, func(b *testing.B) {
			if runtime.GOMAXPROCS(0) < mode.producers+mode.consumers {
				b.Skipf(
					"GOMAXPROCS %d is below the %d goroutines of mode %s, and the design "+
						"excludes configurations with fewer CPUs than goroutines",
					runtime.GOMAXPROCS(0), mode.producers+mode.consumers, mode.name,
				)
			}
			for _, batch := range loadedBatches {
				b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
					for _, offered := range loadedRates {
						b.Run("rate="+offered.name, func(b *testing.B) {
							total := loadedTotal(offered.rate, runLength, mode.producers, batch)
							benchmarkLoadedImpls(b, mode, batch, offered.rate, total)
						})
					}
				})
			}
		})
	}
}

// benchmarkLoadedImpls runs one impl sub-benchmark for each implementation loadedImpls returns.
func benchmarkLoadedImpls(b *testing.B, mode benchMode, batch int, rate float64, total int) {
	for _, impl := range loadedImpls(mode) {
		b.Run("impl="+impl.name, func(b *testing.B) {
			result, err := impl.run(b, batch, rate, total)
			if err != nil {
				b.Fatal(err)
			}
			reportLoaded(b, result, rate)
		})
	}
}
