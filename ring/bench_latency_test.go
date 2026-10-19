package ring_test

import (
	"math"
	"math/bits"
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
func runHandoff(timer benchTimer, newQueue func() (queue[int64], error),
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
		consume(pings, &pingerDone, forwarded, forwardSink{target: pongs},
			newWaiter(spinThenYield))
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
