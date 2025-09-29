package ring

import (
        "fmt"
        "sync"
        "testing"
        "time"

        "github.com/stretchr/testify/assert"
        "github.com/stretchr/testify/require"
)

func TestRingCreation(t *testing.T) {
        // not a power of 2, should error
        _, err := NewRing[uintptr](13)
        assert.Error(t, err)

        _, err = NewRing[uintptr](6)
        assert.Error(t, err)

        simple, err := NewRing[uintptr](64)
        assert.NoError(t, err)
        assertNewRing[uintptr](t, simple, 64)
        assert.False(t, simple.multiProdEnqueue)
        assert.False(t, simple.multiConsDequeue)

        mpsc, err := NewRing[uintptr](64, WithMultiProdEnqueue[uintptr]())
        assert.NoError(t, err)
        assertNewRing[uintptr](t, mpsc, 64)
        assert.True(t, mpsc.multiProdEnqueue)
        assert.False(t, mpsc.multiConsDequeue)

        spmc, err := NewRing[uintptr](64, WithMultiConsDequeue[uintptr]())
        assert.NoError(t, err)
        assertNewRing[uintptr](t, spmc, 64)
        assert.False(t, spmc.multiProdEnqueue)
        assert.True(t, spmc.multiConsDequeue)

        mpmc, err := NewRing[uintptr](
                64,
                WithMultiProdEnqueue[uintptr](),
                WithMultiConsDequeue[uintptr](),
        )
        assert.NoError(t, err)
        assertNewRing[uintptr](t, mpmc, 64)
        assert.True(t, mpmc.multiProdEnqueue)
        assert.True(t, mpmc.multiConsDequeue)

        large, err := NewRing[uintptr](1 << 16)
        assert.NoError(t, err)
        assertNewRing(t, large, 1<<16)
}

func TestRingCount(t *testing.T) {
        buflen := 32
        buffer, err := NewRing[string](uint32(buflen))
        require.NotNil(t, buffer)
        require.NoError(t, err)

        assert.Equal(t, buffer.Count(), uint32(0))

        item := "test"
        for i := 0; i < buflen-1; i++ {
                err = buffer.Enqueue(item)
                require.NoError(t, err)
                assert.Equal(t, buffer.Count(), uint32(i+1))
        }

        err = buffer.Enqueue(item)
        assert.ErrorContains(t, err, "no space in the ring")

        assert.Equal(t, buffer.Count(), uint32(buflen-1))

        for i := 0; i < buflen-1; i++ {
                _, err = buffer.Dequeue()
                require.NoError(t, err)
                assert.Equal(t, buffer.Count(), uint32(buflen - 2 - i))
        }

        _, err = buffer.Dequeue()
        assert.ErrorContains(t, err, "no elements in the ring")
}

func TestRingReset(t *testing.T) {
        buflen := 32
        buffer, err := NewRing[string](uint32(buflen))
        require.NotNil(t, buffer)
        require.NoError(t, err)

        // also test the other occupancy functions in flight, they don't need
        // their own tests
        assert.True(t, buffer.Empty())
        assert.False(t, buffer.Full())
        assert.Equal(t, buffer.Free(), uint32(buflen-1))
        assert.Equal(t, buffer.Count(), uint32(0))

        item := "test"
        for i := 0; i < buflen-1; i++ {
                err = buffer.Enqueue(item)
                require.NoError(t, err)

                assert.False(t, buffer.Empty())
                if i == buflen - 2 {
                        assert.True(t, buffer.Full())
                } else {
                        assert.False(t, buffer.Full())
                }

                assert.Equal(t, buffer.Free(), uint32(buflen-2-i))
                assert.Equal(t, buffer.Count(), uint32(i+1))
        }

        assert.Equal(t, buffer.Count(), uint32(buflen-1))
        assert.Equal(t, buffer.Free(), uint32(0))

        buffer.Reset()

        assert.True(t, buffer.Empty())
        assert.False(t, buffer.Full())
        assert.Equal(t, buffer.Free(), uint32(buflen-1))
        assert.Equal(t, buffer.Count(), uint32(0))

        _, err = buffer.Dequeue()
        assert.ErrorContains(t, err, "no elements in the ring")
}

func TestSPSCRingBuffer(t *testing.T) {
        buffer, err := NewRing[string](32)
        require.NotNil(t, buffer)
        require.NoError(t, err)

        // Test Enqueue and Dequeue
        item := "test"
        err = buffer.Enqueue(item)
        assert.NoError(t, err)

        dequeuedItem, err := buffer.Dequeue()
        assert.NoError(t, err)
        assert.Equal(t, item, dequeuedItem)

        // Test buffer full
        for i := 0; i < 31; i++ {
                err = buffer.Enqueue(item)
                assert.NoError(t, err)
        }
        err = buffer.Enqueue(item)
        assert.ErrorContains(t, err, "no space in the ring")

        // Test buffer empty
        for i := 0; i < 31; i++ {
                _, err = buffer.Dequeue()
                require.NoError(t, err)
        }

        _, err = buffer.Dequeue()
        assert.ErrorContains(t, err, "no elements in the ring")
}

func TestSPMCRingBuffer(t *testing.T) {
        dequeuedStrings := map[string]struct{}{}

        buffer, err := NewRing[string](
                32, WithMultiConsDequeue[string](),
        )
        require.NotNil(t, buffer)
        require.NoError(t, err)

        for i := 0; i < 31; i++ {
                item := fmt.Sprintf("test-%d", i)
                err = buffer.Enqueue(item)
                require.NoError(t, err)
        }

        // Test Dequeue from multiple consumers
        numDequeues := []int{8, 8, 8, 7}
        var mu sync.Mutex // to protect the `dequeuedStrings` map

        var wg sync.WaitGroup
        wg.Add(4)
        for i := 0; i < 4; i++ {
                go func(worker int) {
                        defer wg.Done()
                        for j := 0; j < numDequeues[worker]; j++ {
                                dequeuedItem, err := buffer.Dequeue()
                                assert.NoError(t, err)

                                fmt.Printf("Consumer %d dequeued %q\n", worker, dequeuedItem)

                                mu.Lock()
                                _, ok := dequeuedStrings[dequeuedItem]
                                assert.False(t, ok)
                                dequeuedStrings[dequeuedItem] = struct{}{}
                                mu.Unlock()

                                time.Sleep(1 * time.Millisecond)
                        }
                }(i)
        }
        wg.Wait()

        // make sure any future dequeues would have failed
        _, err = buffer.Dequeue()
        assert.ErrorContains(t, err, "no elements in the ring")
}

func TestMPSCRingBuffer(t *testing.T) {
        buffer, err := NewRing[string](
                32, WithMultiProdEnqueue[string](),
        )
        require.NotNil(t, buffer)
        require.NoError(t, err)

        numWorkers := 4
        numEnqueues := []int{8, 8, 8, 7}
        testStrings := []string{}
        for i := 0; i < 31; i++ {
                testStrings = append(testStrings, fmt.Sprintf("test-%d", i))
        }

        // Test Enqueue from multiple producers
        var wg sync.WaitGroup
        wg.Add(numWorkers)

        for i := 0; i < numWorkers; i++ {
                go func(worker int) {
                        defer wg.Done()

                        stride := numEnqueues[worker]
                        for j := 0; j < stride; j++ {
                                idx := (worker * 8) + j
                                err := buffer.Enqueue(testStrings[idx])
                                assert.NoError(t, err)

                                fmt.Printf("Consumer %d enqueued %q\n", worker, testStrings[idx])
                                time.Sleep(1 * time.Millisecond)
                        }
                }(i)
        }
        wg.Wait()

        dequeuedStrings := map[string]struct{}{}
        for i := 0; i < 31; i++ {
                dequeuedItem, err := buffer.Dequeue()
                require.NoError(t, err)

                _, ok := dequeuedStrings[dequeuedItem]
                assert.False(t, ok)
                dequeuedStrings[dequeuedItem] = struct{}{}
        }

        for _, testString := range testStrings {
                _, ok := dequeuedStrings[testString]
                assert.True(t, ok)
        }
}

func TestMPMCRingBuffer(t *testing.T) {
        buffer, err := NewRing[string](
                32,
                WithMultiProdEnqueue[string](),
                WithMultiConsDequeue[string](),
        )
        require.NotNil(t, buffer)
        require.NoError(t, err)

        strides := []int{8, 8, 8, 7}
        numWorkers := 4

        testStrings := []string{}
        for i := 0; i < 31; i++ {
                testStrings = append(testStrings, fmt.Sprintf("test-%d", i))
        }

        var (
                producerWG sync.WaitGroup
                consumerWG sync.WaitGroup
        )

        producerWG.Add(numWorkers)
        for i := 0; i < numWorkers; i++ {
                go func(worker int) {
                        defer producerWG.Done()

                        for j := 0; j < strides[worker]; j++ {
                                idx := (worker * 8) + j
                                err := buffer.Enqueue(testStrings[idx])
                                assert.NoError(t, err)

                                fmt.Printf("Consumer %d enqueued %q\n", worker, testStrings[idx])
                                time.Sleep(10 * time.Millisecond)
                        }
                }(i)
        }

        // offset the consumer goroutines by 20 ms to give the chance for
        // the producers to start filling the queue, but also make sure that
        // the two passes overlap
        time.Sleep(20 * time.Millisecond)

        dequeuedStrings := map[string]struct{}{}
        var mu sync.Mutex // to protect the `dequeuedStrings` map

        consumerWG.Add(numWorkers)
        for i := 0; i < 4; i++ {
                go func(worker int) {
                        defer consumerWG.Done()
                        for j := 0; j < strides[worker]; j++ {
                                dequeuedItem, err := buffer.Dequeue()
                                assert.NoError(t, err)

                                fmt.Printf("Consumer %d dequeued %q\n", worker, dequeuedItem)

                                mu.Lock()
                                _, ok := dequeuedStrings[dequeuedItem]
                                assert.False(t, ok)
                                dequeuedStrings[dequeuedItem] = struct{}{}
                                mu.Unlock()

                                time.Sleep(10 * time.Millisecond)
                        }
                }(i)
        }

        producerWG.Wait()
        consumerWG.Wait()

        for _, testString := range testStrings {
                _, ok := dequeuedStrings[testString]
                assert.True(t, ok)
        }
}

func TestRingBufferWraparound(t *testing.T) {
        buflen := 32
        buffer, err := NewRing[uint32](uint32(buflen))
        require.NoError(t, err)
        require.NotNil(t, buffer)

        // Fill the buffer to its capacity
        for i := 0; i < buflen-1; i++ {
                err = buffer.Enqueue(uint32(i))
                require.NoError(t, err)
        }

        // Verify that the buffer is full
        assert.True(t, buffer.Full())

        err = buffer.Enqueue(uint32(buflen-1))
        assert.ErrorContains(t, err, "no space in the ring")

        // Make room at the start of the queue
        _, err = buffer.Dequeue()
        require.NoError(t, err)
        assert.False(t, buffer.Full())

        // Now we should be able to enqueue one more element
        err = buffer.Enqueue(uint32(buflen-1))
        assert.NoError(t, err)
        assert.True(t, buffer.Full())
}

func TestRingBufferMultipleWraparounds(t *testing.T) {
        buflen := 8
        buffer, err := NewRing[uint32](uint32(buflen))
        require.NoError(t, err)
        require.NotNil(t, buffer)

        numIterations := 5
        for i := 0; i < numIterations; i++ {
                for j := 0; j < buflen-1; j++ {
                        fmt.Printf("Enqueueing %d\n", i * buflen + j)
                        err = buffer.Enqueue(uint32(i * buflen + j))
                        require.NoError(t, err)
                }

                assert.False(t, buffer.Empty())
                assert.True(t, buffer.Full())
                assert.Equal(t, buffer.Free(), uint32(0))
                assert.Equal(t, buffer.Count(), uint32(buflen-1))

                for j := 0; j < buflen-1; j++ {
                        item, err := buffer.Dequeue()
                        fmt.Printf("Dequeueing %d\n", int(item))
                        require.NoError(t, err)
                        assert.Equal(t, i * buflen + j, int(item))
                }

                assert.True(t, buffer.Empty())
                assert.False(t, buffer.Full())
                assert.Equal(t, buffer.Free(), uint32(buflen-1))
                assert.Equal(t, buffer.Count(), uint32(0))
        }
}

func BenchmarkSPSCRingBufferSeq(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](uint32(buflen))

        item := "test"
        b.SetBytes(int64(len(item)))
        b.ResetTimer()

        for i := 0; i < b.N; i++ {
                _ = buffer.Enqueue(item)
                _, _ = buffer.Dequeue()
        }
}

func BenchmarkSPSCRingBufferSeqBatch(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](uint32(buflen))

        item := "test"
        batchSize := 10
        b.SetBytes(int64(len(item) * batchSize))
        b.ResetTimer()

        for i := 0; i < b.N; i++ {
                for j := 0; j < batchSize; j++ {
                        _ = buffer.Enqueue(item)
                }
                for j := 0; j < batchSize; j++ {
                        _, _ = buffer.Dequeue()
                }
        }
}

func BenchmarkSPSCRingBufferContention(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](uint32(buflen))

        item := "test"
        b.SetBytes(int64(len(item) * 2))
        b.ResetTimer()

        var wg sync.WaitGroup
        wg.Add(2)

        go func() {
                defer wg.Done()
                for i := 0; i < b.N; i++ {
                        _ = buffer.Enqueue(item)
                }
        }()
        go func() {
                defer wg.Done()
                for i := 0; i < b.N; i++ {
                        _, _ = buffer.Dequeue()
                }
        }()
        wg.Wait()
}

func BenchmarkSPMCRingBufferSeq(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiConsDequeue[string](),
        )

        item := "test"
        b.SetBytes(int64(len(item)))
        b.ResetTimer()

        for i := 0; i < b.N; i++ {
                _ = buffer.Enqueue(item)
                _, _ = buffer.Dequeue()
        }
}

func BenchmarkSPMCRingBufferParallel(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiConsDequeue[string](),
        )

        numConsumers := 4
        item := "test"
        b.SetBytes(int64(len(item) * numConsumers))
        b.ResetTimer()

        var wg sync.WaitGroup
        wg.Add(numConsumers)

        for i := 0; i < numConsumers; i++ {
                go func() {
                        defer wg.Done()
                        for j := 0; j < b.N; j++ {
                                _, _ = buffer.Dequeue()
                        }
                }()
        }

        for i := 0; i < b.N*numConsumers; i++ {
                _ = buffer.Enqueue(item)
        }
        wg.Wait()
}

func BenchmarkMPSCRingBufferSeq(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiProdEnqueue[string](),
        )

        item := "test"
        b.SetBytes(int64(len(item)))
        b.ResetTimer()

        for i := 0; i < b.N; i++ {
                _ = buffer.Enqueue(item)
                _, _ = buffer.Dequeue()
        }
}

func BenchmarkMPSCRingBufferParallel(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiProdEnqueue[string](),
        )

        numProducers := 4
        item := "test"
        b.SetBytes(int64(len(item) * numProducers))
        b.ResetTimer()

        var wg sync.WaitGroup
        wg.Add(numProducers)

        for i := 0; i < numProducers; i++ {
                go func() {
                        defer wg.Done()
                        for j := 0; j < b.N; j++ {
                                _ = buffer.Enqueue(item)
                        }
                }()
        }

        for i := 0; i < b.N*numProducers; i++ {
                _, _ = buffer.Dequeue()
        }
        wg.Wait()
}

func BenchmarkMPMCRingBufferSeq(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiProdEnqueue[string](),
                WithMultiConsDequeue[string](),
        )

        item := "test"
        b.SetBytes(int64(len(item)))
        b.ResetTimer()

        for i := 0; i < b.N; i++ {
                _ = buffer.Enqueue(item)
                _, _ = buffer.Dequeue()
        }
}

func BenchmarkMPMCRingBufferParallel(b *testing.B) {
        buflen := 1024
        buffer, _ := NewRing[string](
                uint32(buflen),
                WithMultiProdEnqueue[string](),
                WithMultiConsDequeue[string](),
        )

        numProducers := 4
        numConsumers := 4
        item := "test"
        b.SetBytes(int64(len(item) * numProducers))
        b.ResetTimer()

        var producerWG sync.WaitGroup
        producerWG.Add(numProducers)

        for i := 0; i < numProducers; i++ {
                go func() {
                        defer producerWG.Done()
                        for j := 0; j < b.N; j++ {
                                _ = buffer.Enqueue(item)
                        }
                }()
        }

        var consumerWG sync.WaitGroup
        consumerWG.Add(numConsumers)

        for i := 0; i < numConsumers; i++ {
                go func() {
                        defer consumerWG.Done()
                        for j := 0; j < b.N; j++ {
                                _, _ = buffer.Dequeue()
                        }
                }()
        }

        producerWG.Wait()
        consumerWG.Wait()
}

func assertNewRing[T any](t *testing.T, ring *Ring[T], count uint32) {
        assert.NotNil(t, ring.entries)
        assert.Equal(t, ring.size, count)
        assert.Equal(t, ring.mask, count-1)
        assert.Equal(t, ring.capacity, count-1)

        assert.Zero(t, *ring.prodHead)
        assert.Zero(t, *ring.prodTail)
        assert.Zero(t, *ring.consHead)
        assert.Zero(t, *ring.consTail)
}
