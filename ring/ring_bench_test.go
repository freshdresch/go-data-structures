package ring_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/freshdresch/go-data-structures/ring"
)

func BenchmarkRing_SPSC_Seq(b *testing.B) {
	for _, sz := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", sz), func(b *testing.B) {
			rb, _ := ring.NewRing[[]byte](ringSize)
			p := make([]byte, sz)

			b.SetBytes(int64(sz))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = rb.Enqueue(p)
				_, _ = rb.Dequeue()
			}
		})
	}
}

func BenchmarkRing_SPMC_Parallel(b *testing.B) {
	const consumers = 4
	for _, sz := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", sz), func(b *testing.B) {
			rb, _ := ring.NewRing[[]byte](
				ringSize,
				ring.WithMultiConsDequeue[[]byte](),
			)
			p := make([]byte, sz)

			b.SetBytes(int64(sz))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(consumers)

			for i := 0; i < consumers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						_, _ = rb.Dequeue()
					}
				}()
			}

			for i := 0; i < b.N*consumers; i++ {
				_ = rb.Enqueue(p)
			}
			wg.Wait()
		})
	}
}

func BenchmarkRing_MPSC_Parallel(b *testing.B) {
	const producers = 4
	for _, sz := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", sz), func(b *testing.B) {
			rb, _ := ring.NewRing[[]byte](
				ringSize,
				ring.WithMultiProdEnqueue[[]byte](),
			)
			p := make([]byte, sz)

			b.SetBytes(int64(sz))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(producers)

			for i := 0; i < producers; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						_ = rb.Enqueue(p)
					}
				}()
			}

			for i := 0; i < b.N*producers; i++ {
				_, _ = rb.Dequeue()
			}
			wg.Wait()
		})
	}
}

func BenchmarkRing_MPMC_Parallel(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, sz := range payloadSizes {
		b.Run(fmt.Sprintf("%dB", sz), func(b *testing.B) {
			rb, _ := ring.NewRing[[]byte](
				ringSize,
				ring.WithMultiProdEnqueue[[]byte](),
				ring.WithMultiConsDequeue[[]byte](),
			)

			p := make([]byte, sz)

			b.SetBytes(int64(sz))
			b.ResetTimer()

			var (
				prodWG sync.WaitGroup
				consWG sync.WaitGroup
			)
			prodWG.Add(producers)
			consWG.Add(consumers)

			for i := 0; i < producers; i++ {
				go func() {
					defer prodWG.Done()
					for j := 0; j < b.N; j++ {
						_ = rb.Enqueue(p)
					}
				}()
			}

			for i := 0; i < consumers; i++ {
				go func() {
					defer consWG.Done()
					for j := 0; j < b.N; j++ {
						_, _ = rb.Dequeue()
					}
				}()
			}

			prodWG.Wait()
			consWG.Wait()
		})
	}
}

func BenchmarkRing_SPSC_Seq_Batch(b *testing.B) {
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					rb, _ := ring.NewRing[[]byte](ringSize)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						_ = rb.EnqueueBurst(payload, uint32(batchSize), nil)
						_, _ = rb.DequeueBurst(uint32(batchSize), nil)
					}
				},
			)
		}
	}
}

func BenchmarkRing_SPMC_Parallel_Batch(b *testing.B) {
	const consumers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					rb, _ := ring.NewRing[[]byte](
						ringSize,
						ring.WithMultiConsDequeue[[]byte](),
					)
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
								_, _ = rb.DequeueBurst(uint32(batchSize), nil)
							}
						}()
					}

					for i := 0; i < b.N*consumers; i++ {
						_ = rb.EnqueueBurst(payload, uint32(batchSize), nil)
					}
					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkRing_MPSC_Parallel_Batch(b *testing.B) {
	const producers = 4
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					rb, _ := ring.NewRing[[]byte](
						ringSize,
						ring.WithMultiProdEnqueue[[]byte](),
					)
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
								_ = rb.EnqueueBurst(payload, uint32(batchSize), nil)
							}
						}()
					}

					for i := 0; i < b.N*producers; i++ {
						_, _ = rb.DequeueBurst(uint32(batchSize), nil)
					}

					wg.Wait()
				},
			)
		}
	}
}

func BenchmarkRing_MPMC_Parallel_Batch(b *testing.B) {
	const (
		producers = 4
		consumers = 4
	)
	for _, batchSize := range batchSizes {
		for _, payloadSize := range payloadSizes {
			b.Run(
				fmt.Sprintf("batch%d_%dB", batchSize, payloadSize),
				func(b *testing.B) {
					rb, _ := ring.NewRing[[]byte](
						ringSize,
						ring.WithMultiProdEnqueue[[]byte](),
						ring.WithMultiConsDequeue[[]byte](),
					)
					payload := make([][]byte, batchSize)
					for i := range payload {
						payload[i] = make([]byte, payloadSize)
					}

					b.SetBytes(int64(payloadSize))
					b.ResetTimer()

					var (
						prodWG sync.WaitGroup
						consWG sync.WaitGroup
					)
					prodWG.Add(producers)
					consWG.Add(consumers)

					for i := 0; i < producers; i++ {
						go func() {
							defer prodWG.Done()
							for j := 0; j < b.N; j++ {
								_ = rb.EnqueueBurst(payload, uint32(batchSize), nil)
							}
						}()
					}

					for i := 0; i < consumers; i++ {
						go func() {
							defer consWG.Done()
							for j := 0; j < b.N; j++ {
								_, _ = rb.DequeueBurst(uint32(batchSize), nil)
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
