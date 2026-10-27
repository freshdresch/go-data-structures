//go:build !race

package dpdk_test

// raceEnabled reports whether the test binary was built with -race, so tests can scale their
// work down or skip under it.
const raceEnabled = false
