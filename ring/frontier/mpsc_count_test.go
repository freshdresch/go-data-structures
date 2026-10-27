package frontier

import "testing"

func TestMPSCCacheColdWarm(t *testing.T) {
	for _, capacity := range []int{1, 8} {
		queue := cacheTestQueue[int](t, capacity)
		for index := 0; index < capacity; index++ {
			if err := queue.Enqueue(index); err != nil {
				t.Fatal(err)
			}
		}
		if queue.observerCompleted.Load() != 0 {
			t.Fatal("transfer retained proof")
		}
		if queue.Count() != capacity || queue.observerCompleted.Load() != uint64(capacity) {
			t.Fatal("cold full")
		}
		if queue.Count() != capacity || queue.observerCompleted.Load() != uint64(capacity) {
			t.Fatal("warm full")
		}
		if left, err := queue.DequeueBulk(nil); left != capacity || err != nil {
			t.Fatal(left, err)
		}
		before := queue.consumerPublished.Load()
		observation := queue.mpscScanCount(before)
		if observation.result != uint64(capacity) || observation.retain || observation.observed != 0 {
			t.Fatal("acquired full shortcut", observation)
		}
		if queue.Count() != capacity {
			t.Fatal("public acquired full")
		}
	}
	queue := cacheTestQueue[int](t, 8)
	if queue.Count() != 0 || queue.observerCompleted.Load() != 0 {
		t.Fatal("empty")
	}
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.DequeueBulk(nil); err != nil {
		t.Fatal(err)
	}
	observation := queue.mpscScanCount(queue.consumerPublished.Load())
	if observation.result != 1 || observation.retain || observation.observed != 0 {
		t.Fatal("acquired-only window", observation)
	}
}

func TestMPSCCacheConvenienceObservers(t *testing.T) {
	for _, observer := range []string{"Free", "Full", "Empty"} {
		queue := cacheTestQueue[int](t, 8)
		if err := queue.Enqueue(1); err != nil {
			t.Fatal(err)
		}
		switch observer {
		case "Free":
			if queue.Free() != 7 {
				t.Fatal("free")
			}
		case "Full":
			if queue.Full() {
				t.Fatal("full")
			}
		case "Empty":
			if queue.Empty() {
				t.Fatal("empty")
			}
		}
		if queue.observerCompleted.Load() != 1 {
			t.Fatal(observer, "did not use cached Count")
		}
	}
}

func TestMPSCCacheCompletionHole(t *testing.T) {
	queue := cacheTestQueue[int](t, 8)
	first, firstCount, _ := queue.mpscReserveProducer(4, true, false)
	second, secondCount, _ := queue.mpscReserveProducer(3, true, false)
	if first != 0 || firstCount != 4 || second != 4 || secondCount != 3 {
		t.Fatal("admissible reservations")
	}
	copy(queue.producerConfig.entries[4:], []int{5, 6, 7})
	queue.mpscPublishProducer(second, secondCount)
	if queue.Count() != 0 || queue.observerCompleted.Load() != 0 {
		t.Fatal("crossed real head hole")
	}
	copy(queue.producerConfig.entries, []int{1, 2, 3, 4})
	queue.mpscPublishProducer(first, firstCount)
	if queue.Count() != 7 || queue.observerCompleted.Load() != 7 {
		t.Fatal("reverse completion prefix")
	}
	for want := 1; want <= 7; want++ {
		got, err := queue.Dequeue()
		if err != nil || got != want {
			t.Fatal(got, want, err)
		}
	}
}

func TestMPSCCacheHistoricalWindowBoundaries(t *testing.T) {
	for _, proof := range []uint64{0, 2, 6, 8, 10, 12} {
		queue := cacheTestQueue[int](t, 8)
		// These endpoints arise through legal public Count histories, rather than cache seeding.
		for index := 0; index < 8; index++ {
			if err := queue.Enqueue(index); err != nil {
				t.Fatal(err)
			}
			if uint64(index+1) == proof {
				if queue.Count() != index+1 {
					t.Fatal("initial proof")
				}
			}
		}
		dst := make([]int, 4)
		if _, err := queue.DequeueBulk(dst); err != nil {
			t.Fatal(err)
		}
		for index := 8; index < 12; index++ {
			if err := queue.Enqueue(index); err != nil {
				t.Fatal(err)
			}
			if uint64(index+1) == proof {
				if queue.Count() != index-3 {
					t.Fatal("reused proof")
				}
			}
		}
		if queue.observerCompleted.Load() != proof {
			t.Fatal("history endpoint", proof)
		}
		observation := queue.mpscScanCount(queue.consumerPublished.Load())
		if observation.observed != proof || observation.end != 12 || observation.result != 8 ||
			!observation.retain {
			t.Fatal("window", proof, observation)
		}
		if queue.Count() != 8 || queue.observerCompleted.Load() != 12 {
			t.Fatal("public boundary", proof)
		}
		for want := 4; want < 12; want++ {
			got, err := queue.Dequeue()
			if err != nil || got != want {
				t.Fatal(got, want, err)
			}
		}
	}
	// A full-capacity range retains proof after partial release overwrites its start record.
	queue := cacheTestQueue[int](t, 8)
	if _, err := queue.EnqueueBulk([]int{0, 1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	if queue.Count() != 8 {
		t.Fatal("range proof")
	}
	if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.EnqueueBulk([]int{8, 9, 10, 11}); err != nil {
		t.Fatal(err)
	}
	if queue.completion.records[0].Load() != 12 || queue.observerCompleted.Load() != 8 ||
		queue.Count() != 8 {
		t.Fatal("partial release with overwritten start")
	}
}

func TestMPSCCacheDefensiveSnapshots(t *testing.T) {
	// These synthetic snapshots isolate guards. They do not represent stable legal lifetimes.
	for _, test := range []struct {
		name     string
		head     uint64
		cursor   uint32
		frontier uint32
		cache    uint64
		want     uint64
		retain   bool
	}{
		{"distance-underflow", 0, 1, 1, 7, 0, false},
		{"acquired-beyond-window", 2, 0, 3, 7, 3, false},
		{"clamped-acquired", 2, 0, 9, 7, 8, false},
		{"cache-beyond-head", 4, 0, 0, 6, 0, true},
		{"record-equals-start", 4, 0, 0, 0, 0, true},
		{"record-beyond-window", 4, 0, 0, 0, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := cacheTestQueue[int](t, 8)
			queue.producerReservation.head.Store(test.head)
			queue.observerCompleted.Store(test.cache)
			if test.name == "record-beyond-window" {
				queue.completion.records[0].Store(5)
			}
			before := mpscConsumerWord(test.cursor, test.frontier)
			queue.consumerPublished.Store(before)
			observation := queue.mpscScanCount(before)
			if observation.result != test.want || observation.retain != test.retain {
				t.Fatal(observation)
			}
			if got := queue.Count(); got != int(test.want) {
				t.Fatal(got)
			}
		})
	}
}

func TestMPSCCacheEveryShortcutValidates(t *testing.T) {
	for _, state := range []string{"empty", "acquired-full", "acquired-only", "extension", "guard"} {
		queue := cacheTestQueue[int](t, 8)
		switch state {
		case "acquired-full":
			if _, err := queue.EnqueueBulk(make([]int, 8)); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.DequeueBulk(nil); err != nil {
				t.Fatal(err)
			}
		case "acquired-only":
			if err := queue.Enqueue(1); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.DequeueBulk(nil); err != nil {
				t.Fatal(err)
			}
		case "extension":
			if err := queue.Enqueue(1); err != nil {
				t.Fatal(err)
			}
		case "guard":
			// Defensive input only, as in TestMPSCCacheDefensiveSnapshots.
			queue.consumerPublished.Store(mpscConsumerWord(1, 1))
		}
		before := queue.consumerPublished.Load()
		observation := queue.mpscScanCount(before)
		queue.consumerPublished.Store(mpscConsumerWord(10, 13))
		result, retain := queue.mpscValidateCount(before, observation)
		if result != 3 || retain || queue.observerCompleted.Load() != 0 {
			t.Fatal(state, result, retain)
		}
		queue.consumerPublished.Store(mpscConsumerWord(10, 30))
		result, retain = queue.mpscValidateCount(before, observation)
		if result != 8 || retain {
			t.Fatal("clamped fallback", state, result, retain)
		}
	}
}

func TestMPSCCacheChangedStateAfterReuse(t *testing.T) {
	queue := cacheTestQueue[int](t, 8)
	for index := 0; index < 4; index++ {
		if err := queue.Enqueue(index); err != nil {
			t.Fatal(err)
		}
	}
	before := queue.consumerPublished.Load()
	observation := queue.mpscScanCount(before)
	if _, err := queue.DequeueBulk(make([]int, 4)); err != nil {
		t.Fatal(err)
	}
	for index := 4; index < 12; index++ {
		if err := queue.Enqueue(index); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := queue.DequeueBulk(nil); err != nil {
		t.Fatal(err)
	}
	result, retain := queue.mpscValidateCount(before, observation)
	if result != 8 || retain || queue.observerCompleted.Load() != 0 {
		t.Fatal(result, retain)
	}
	if queue.Count() != 8 {
		t.Fatal("new acquired prefix")
	}
}
