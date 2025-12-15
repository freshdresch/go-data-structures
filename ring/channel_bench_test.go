package ring_test

import (
	"fmt"
	"sync"
	"testing"
)

type ChanWrapper[T any] struct {
	ch chan T
}

func NewChanWrapper[T any](capacity int) *ChanWrapper[T] {
	return &ChanWrapper[T]{ch: make(chan T, capacity)}
}

func (c *ChanWrapper[T]) EnqueueBurst(batch []T) {
	for _, v := range batch {
		c.ch <- v
	}
}

func (c *ChanWrapper[T]) DequeueBurst(n int) []T {
	res := make([]T, 0, n)
	for i := 0; i < n; i++ {
		select {
		case v := <-c.ch:
			res = append(res, v)
		default:
			return res
		}
	}
	return res
}

func BenchmarkChan_SPSC_Seq(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			ch := make(chan []byte, ringSize)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ch <- payload
				<-ch
			}
		})
	}
}

func BenchmarkChan_MPSC_Parallel(b *testing.B) {
	const producers = 4
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			ch := make(chan []byte, 1024)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(producers)

			for i := 0; i < producers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						ch <- payload
					}
				}()
			}

			for i := 0; i < b.N*producers; i++ {
				<-ch
			}
			wg.Wait()
		})
	}
}

func BenchmarkChan_SPMC_Parallel(b *testing.B) {
	const consumers = 4
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			ch := make(chan []byte, 1024)
			payload := make([]byte, size)

			b.SetBytes(int64(size))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(consumers)

			for i := 0; i < consumers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						<-ch
					}
				}()
			}

			for i := 0; i < b.N*consumers; i++ {
				ch <- payload
			}
			wg.Wait()
		})
	}
}

func BenchmarkChan_MPMC_Parallel(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			ch := make(chan []byte, 1024)
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
						ch <- payload
					}
				}()
			}

			for i := 0; i < consumers; i++ {
				go func() {
					defer consWG.Done()
					for j := 0; j < b.N; j++ {
						<-ch
					}
				}()
			}

			prodWG.Wait()
			consWG.Wait()
		})
	}
}

func BenchmarkChan_SPSC_Seq_Batch(b *testing.B) {
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					ch := make(chan [][]byte, ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						ch <- payload
						<-ch
					}
				},
			)
		}
	}
}

func BenchmarkChan_MPSC_Parallel_Batch(b *testing.B) {
	const producers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					ch := make(chan [][]byte, ringSize)
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
								ch <- payload
							}
						}()
					}

					for i := 0; i < b.N*producers; i++ {
						<-ch
					}
					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkChan_SPMC_Parallel_Batch(b *testing.B) {
	const consumers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					ch := make(chan [][]byte, ringSize)
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
								<-ch
							}
						}()
					}

					for i := 0; i < b.N*consumers; i++ {
						ch <- payload
					}
					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkChan_MPMC_Parallel_Batch(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					ch := make(chan [][]byte, ringSize)
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
								ch <- payload
							}
						}()
					}

					for i := 0; i < consumers; i++ {
						go func() {
							defer consWG.Done()
							for j := 0; j < b.N; j++ {
								<-ch
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
