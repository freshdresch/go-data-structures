//go:build !crossbeamcheck

package workloads

// recording is off in timed builds, so the compiler removes the sender tag and the appends.
const recording = false
