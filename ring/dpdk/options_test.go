package dpdk

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestOptionsCapacityBeforeApplication(t *testing.T) {
	for _, constructor := range []string{"New", "NewSPSCQueue"} {
		for _, count := range []int{-1, 0, 3, maxCount + 1} {
			applied := false
			option := func(*config) { applied = true }
			var err error
			var nonnil bool
			if constructor == "New" {
				queue, result := New[int](count, option)
				err, nonnil = result, queue != nil
			} else {
				queue, result := NewSPSCQueue[int](count, option)
				err, nonnil = result, queue != nil
			}
			if applied || nonnil || !errors.Is(err, ErrInvalidCount) {
				t.Fatalf("%s(%d): applied=%v nonnil=%v err=%v", constructor, count, applied, nonnil, err)
			}
		}
	}
}

func TestOptionsValidationPrecedence(t *testing.T) {
	cases := []struct {
		name    string
		options []Option
		want    error
		context []string
	}{
		{"enqueue", []Option{WithMaxBatch(0), WithSpinsBeforeYield(-1)},
			ErrInvalidMaxBatch, []string{"enqueue", "0"}},
		{"dequeue", []Option{WithMaxDequeueBatch(0), WithSpinsBeforeYield(-1)},
			ErrInvalidMaxBatch, []string{"dequeue", "0"}},
		{"spin-number", []Option{WithSpinsBeforeYield(-1), WithMultiProdEnqueue()},
			ErrInvalidSpinsBeforeYield, []string{"spins", "-1"}},
		{"producer-mode", []Option{WithMultiProdEnqueue(), WithMultiConsDequeue()},
			ErrUnsupportedOption, []string{"producer", "1"}},
		{"consumer-mode", []Option{WithMultiConsDequeue(), WithSpinsBeforeYield(0)},
			ErrUnsupportedOption, []string{"consumer", "1"}},
		{"explicit-spin", []Option{WithSpinsBeforeYield(0),
			WithMaxProducerHeadTailDistance(-1)}, ErrUnsupportedOption,
			[]string{"WithSpinsBeforeYield", "0"}},
		{"producer-number", []Option{WithMaxProducerHeadTailDistance(-1),
			WithMaxConsumerHeadTailDistance(-2)}, ErrInvalidMaxHeadTailDistance,
			[]string{"producer", "-1"}},
		{"producer-applicability", []Option{WithMaxProducerHeadTailDistance(0),
			WithMaxConsumerHeadTailDistance(-2)}, ErrUnsupportedOption,
			[]string{"producer", "0"}},
		{"consumer-number", []Option{WithMaxConsumerHeadTailDistance(-2)},
			ErrInvalidMaxHeadTailDistance, []string{"consumer", "-2"}},
		{"consumer-applicability", []Option{WithMaxConsumerHeadTailDistance(0)},
			ErrUnsupportedOption, []string{"consumer", "0"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			queue, err := NewSPSCQueue[int](8, test.options...)
			if queue != nil {
				t.Fatal("non-nil queue on error")
			}
			checkOptionError(t, err, test.want, "NewSPSCQueue", "8")
			checkOptionError(t, err, test.want, test.context...)
		})
	}
}

func checkOptionError(t *testing.T, err, want error, context ...string) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
	for _, part := range context {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q lacks %q", err, part)
		}
	}
}

func TestOptionsFinalValuesAndDefaults(t *testing.T) {
	for _, count := range []int{1, 4, 8, 16} {
		queue, err := NewSPSCQueue[int](count, WithMaxBatch(0), WithMaxBatch(count))
		if err != nil {
			t.Fatal(err)
		}
		if queue.MaxEnqueueBatch() != count || queue.MaxDequeueBatch() != count {
			t.Fatal("SPSC default limits")
		}
		if !queue.consumer.clearOnDequeue || queue.producer.exactBatchCounts {
			t.Fatal("SPSC default policies")
		}
		for _, options := range [][]Option{
			nil,
			{WithRelaxedMultiProdEnqueue()},
			{WithRelaxedMultiConsDequeue()},
			{WithRelaxedMultiProdEnqueue(), WithRelaxedMultiConsDequeue()},
		} {
			queue, err := New[int](count, options...)
			if err != nil {
				t.Fatal(err)
			}
			producerWant, consumerWant := uint32(0), uint32(0)
			if queue.prodMode == modeRelaxed {
				producerWant = uint32(count / 8)
			}
			if queue.consMode == modeRelaxed {
				consumerWant = uint32(count / 8)
			}
			if queue.prodHTDMax != producerWant || queue.consHTDMax != consumerWant {
				t.Fatalf("count %d: distances %d/%d", count, queue.prodHTDMax, queue.consHTDMax)
			}
			if queue.spinsBeforeYield != defaultSpinsBeforeYield {
				t.Fatal("Ring default spins")
			}
		}
	}
	queue, err := New[int](
		8,
		WithMaxBatch(0),
		WithMaxBatch(4),
		WithMaxEnqueueBatch(2),
		WithSpinsBeforeYield(-1),
		WithSpinsBeforeYield(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if queue.MaxEnqueueBatch() != 2 || queue.MaxDequeueBatch() != 4 || queue.spinsBeforeYield != 0 {
		t.Fatal("final numeric values")
	}
	for _, option := range []Option{WithSpinsBeforeYield(0),
		WithSpinsBeforeYield(defaultSpinsBeforeYield)} {
		queue, err := NewSPSCQueue[int](8, option)
		if queue != nil {
			t.Fatal("explicit spin constructed SPSC")
		}
		checkOptionError(t, err, ErrUnsupportedOption, "NewSPSCQueue", "WithSpinsBeforeYield")
	}
}

func TestOptionsModeOrder(t *testing.T) {
	for _, producer := range []bool{false, true} {
		ordered, relaxed := WithMultiConsDequeue(), WithRelaxedMultiConsDequeue()
		distance := WithMaxConsumerHeadTailDistance(0)
		side, selector := "consumer", "WithRelaxedMultiConsDequeue"
		if producer {
			ordered, relaxed = WithMultiProdEnqueue(), WithRelaxedMultiProdEnqueue()
			distance = WithMaxProducerHeadTailDistance(0)
			side, selector = "producer", "WithRelaxedMultiProdEnqueue"
		}
		for _, test := range []struct {
			options []Option
			want    syncMode
		}{
			{[]Option{ordered, ordered}, modeMulti},
			{[]Option{relaxed, ordered}, modeMulti},
			{[]Option{ordered, relaxed}, modeRelaxed},
		} {
			options := test.options
			queue, err := New[int](8, options...)
			if err != nil {
				t.Fatal(err)
			}
			got := queue.consMode
			if producer {
				got = queue.prodMode
			}
			if got != test.want {
				t.Fatalf("%s mode %d, want %d", side, got, test.want)
			}
			spsc, err := NewSPSCQueue[int](8, options...)
			if spsc != nil {
				t.Fatal("mode constructed SPSC")
			}
			checkOptionError(t, err, ErrUnsupportedOption, "NewSPSCQueue", side)
		}
		for _, options := range [][]Option{{distance}, {relaxed, ordered, distance}} {
			queue, err := New[int](8, options...)
			if queue != nil {
				t.Fatal("ordered distance constructed Ring")
			}
			checkOptionError(t, err, ErrHeadTailDistanceRequiresRelaxed, "New", "8", side, "0", selector)
		}
	}
}

func TestOptionsDistanceDomain(t *testing.T) {
	largest := uint64(math.MaxInt)
	if largest > math.MaxUint32 {
		largest = math.MaxUint32
	}
	values := []int{0, 1, 7, 8, 9, int(largest)}
	for _, producer := range []bool{false, true} {
		for _, value := range values {
			for _, distanceFirst := range []bool{false, true} {
				distance, selector := WithMaxConsumerHeadTailDistance(value), WithRelaxedMultiConsDequeue()
				if producer {
					distance, selector = WithMaxProducerHeadTailDistance(value), WithRelaxedMultiProdEnqueue()
				}
				options := []Option{selector, distance}
				if distanceFirst {
					options = []Option{distance, selector}
				}
				queue, err := New[int](8, options...)
				if err != nil {
					t.Fatal(err)
				}
				got := queue.consHTDMax
				if producer {
					got = queue.prodHTDMax
				}
				if got != uint32(value) {
					t.Fatalf("distance %d, want %d", got, value)
				}
			}
		}
	}
	invalid := []int{-1}
	if strconv.IntSize == 64 {
		oversized := uint64(math.MaxUint32) + 1
		invalid = append(invalid, int(oversized))
	}
	for _, value := range invalid {
		for _, producer := range []bool{false, true} {
			distance := WithMaxConsumerHeadTailDistance(value)
			side := "consumer"
			if producer {
				distance, side = WithMaxProducerHeadTailDistance(value), "producer"
			}
			queue, err := New[int](8, distance)
			if queue != nil {
				t.Fatal("invalid distance constructed Ring")
			}
			checkOptionError(t, err, ErrInvalidMaxHeadTailDistance, "New", "8", side, fmt.Sprint(value))
			spsc, err := NewSPSCQueue[int](8, distance)
			if spsc != nil {
				t.Fatal("invalid distance constructed SPSC")
			}
			checkOptionError(t, err, ErrInvalidMaxHeadTailDistance, "NewSPSCQueue", side)
		}
	}
	queue, err := New[int](
		8,
		WithRelaxedMultiProdEnqueue(),
		WithMaxProducerHeadTailDistance(-1),
		WithMaxProducerHeadTailDistance(3),
		WithRelaxedMultiConsDequeue(),
		WithMaxConsumerHeadTailDistance(-1),
		WithMaxConsumerHeadTailDistance(5),
	)
	if err != nil {
		t.Fatal(err)
	}
	if queue.prodHTDMax != 3 || queue.consHTDMax != 5 {
		t.Fatal("final independent distances")
	}
}
