package ring

// ResetWaitCounts and ReadWaitCounts expose the publish-wait counters to the external benchmarks.
var (
	ResetWaitCounts = resetWaitCounts
	ReadWaitCounts  = readWaitCounts
)
