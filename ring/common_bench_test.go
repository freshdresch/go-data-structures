package ring_test

const ringSize = 1024

var (
	batchSizes = []int{1, 8, 16, 32, 64}
	payloadSizes = []int{8, 32, 64, 256, 512} // bytes
)
