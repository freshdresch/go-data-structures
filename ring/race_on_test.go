//go:build race

package ring_test

// raceEnabled reports whether the test binary was built with -race, so tests can scale their
// repetitions down under it.
const raceEnabled = true
