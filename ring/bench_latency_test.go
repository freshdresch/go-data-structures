package ring_test

import (
	"math"
	"math/bits"
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
