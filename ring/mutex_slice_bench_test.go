package ring_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// A simple bounded FIFO implemented as a slice protected by a mutex.
// This represents the most common "roll-your-own" queue alternative.
type mutexSliceQueue[T any] struct {
	mu   sync.Mutex
	buf  []T
	head int
	tail int
	mask int
	used int
}

func newMutexSliceQueue[T any](size int) *mutexSliceQueue[T] {
	// size must be power of two for mask
	return &mutexSliceQueue[T]{
		buf: make([]T, size),
		mask: size - 1,
	}
}

func (q *mutexSliceQueue[T]) Enqueue(v T) {
	for {
		q.mu.Lock()
		if q.used < len(q.buf)-1 {
			q.buf[q.tail&q.mask] = v
			q.tail++
			q.used++
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		runtime.Gosched()
	}
}

func (q *mutexSliceQueue[T]) Dequeue() T {
	var zero T
	for {
		q.mu.Lock()
		if q.used > 0 {
			v := q.buf[q.head&q.mask]
			q.buf[q.head&q.mask] = zero
			q.head++
			q.used--
			q.mu.Unlock()
			return v
		}
		q.mu.Unlock()
		runtime.Gosched()
	}
}

func (q *mutexSliceQueue[T]) EnqueueBatch(items []T) {
	n := len(items)
	for {
		q.mu.Lock()
		if q.used+n <= int(q.mask)+1 {
			for i := 0; i < n; i++ {
				baseIdx := q.tail & q.mask
				q.buf[baseIdx] = items[i]
				q.tail++
			}
			q.used += n
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		runtime.Gosched()
	}
}

func (q *mutexSliceQueue[T]) DequeueBatch(n int) []T {
	var zero T
	items := make([]T, 0, n)

	for {
		q.mu.Lock()
		if q.used > 0 {
			if n > q.used {
				n = q.used
			}
			for i := 0; i < n; i++ {
				baseIdx := q.head & q.mask
				items = append(items, q.buf[baseIdx])
				q.buf[baseIdx] = zero
				q.head++
			}
			q.used -= n
			q.mu.Unlock()
			return items
		}
		q.mu.Unlock()
		runtime.Gosched()
	}
}

func BenchmarkMutexSlice_SPSC_Seq(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			q := newMutexSliceQueue[[]byte](ringSize)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				q.Enqueue(payload)
				_ = q.Dequeue()
			}
		})
	}
}

func BenchmarkMutexSlice_MPSC_Parallel(b *testing.B) {
	const producers = 4
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			q := newMutexSliceQueue[[]byte](ringSize)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(producers)

			for i := 0; i < producers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						q.Enqueue(payload)
					}
				}()
			}

			for i := 0; i < b.N*producers; i++ {
				_ = q.Dequeue()
			}
			wg.Wait()
		})
	}
}

func BenchmarkMutexSlice_SPMC_Parallel(b *testing.B) {
	const consumers = 4
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			q := newMutexSliceQueue[[]byte](ringSize)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(consumers)

			for i := 0; i < consumers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						_ = q.Dequeue()
					}
				}()
			}

			for i := 0; i < b.N*consumers; i++ {
				q.Enqueue(payload)
			}
			wg.Wait()
		})
	}
}

func BenchmarkMutexSlice_MPMC_Parallel(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			q := newMutexSliceQueue[[]byte](ringSize)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			var prodWG, consWG sync.WaitGroup
			prodWG.Add(producers)
			consWG.Add(consumers)

			for i := 0; i < producers; i++ {
				go func() {
					defer prodWG.Done()
					for j := 0; j < b.N; j++ {
						q.Enqueue(payload)
					}
				}()
			}

			for i := 0; i < consumers; i++ {
				go func() {
					defer consWG.Done()
					for j := 0; j < b.N; j++ {
						_ = q.Dequeue()
					}
				}()
			}

			prodWG.Wait()
			consWG.Wait()
		})
	}
}

func BenchmarkMutexSlice_SPSC_Seq_Batch(b *testing.B) {
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					q := newMutexSliceQueue[[]byte](ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						q.EnqueueBatch(payload)
						_ = q.DequeueBatch(batchSize)
					}
				},
			)
		}
	}
}

func BenchmarkMutexSlice_MPSC_Parallel_Batch(b *testing.B) {
	const producers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					q := newMutexSliceQueue[[]byte](ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					var wg sync.WaitGroup
					wg.Add(producers)

					for i := 0; i < producers; i++ {
						go func() {
							defer wg.Done()
							for j := 0; j < b.N; j++ {
								q.EnqueueBatch(payload)
							}
						}()
					}

					for i := 0; i < b.N*producers; i++ {
						_ = q.DequeueBatch(batchSize)
					}
					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkMutexSlice_SPMC_Parallel_Batch(b *testing.B) {
	const consumers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					q := newMutexSliceQueue[[]byte](ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					var wg sync.WaitGroup
					wg.Add(consumers)

					for i := 0; i < consumers; i++ {
						go func() {
							defer wg.Done()
							for j := 0; j < b.N; j++ {
								_ = q.DequeueBatch(batchSize)
							}
						}()
					}

					for i := 0; i < b.N*consumers; i++ {
						q.EnqueueBatch(payload)
					}
					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkMutexSlice_MPMC_Parallel_Batch(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					q := newMutexSliceQueue[[]byte](ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					var prodWG, consWG sync.WaitGroup
					prodWG.Add(producers)
					consWG.Add(consumers)

					for i := 0; i < producers; i++ {
						go func() {
							defer prodWG.Done()
							for j := 0; j < b.N; j++ {
								q.EnqueueBatch(payload)
							}
						}()
					}

					for i := 0; i < consumers; i++ {
						go func() {
							defer consWG.Done()
							for j := 0; j < b.N; j++ {
								_ = q.DequeueBatch(batchSize)
							}
						}()
					}

					prodWG.Wait()
					consWG.Wait()
				},
			)
		}
	}
}
