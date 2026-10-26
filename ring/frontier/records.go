package frontier

import "sync/atomic"

// recordLookahead is how many predicted records discovery loads at once.
const recordLookahead = 4

// recordLookaheadMinStride is the smallest record spacing, in positions, at which predicted
// records lie on different 64-byte lines. Closer records share a line that the chained load
// already brings in, and loading ahead of them only reads records still being written, so
// discovery predicts only from runs at least this far apart.
const recordLookaheadMinStride = 64 / 8

// discoverRecords extends frontier through contiguous finished intervals. Each interval
// [s, e) is recorded by storing its absolute end e into records[s & mask]. Discovery accepts the
// record at boundary b when b < e and e lies within (base, base+window], and continues from e. It
// stops at the first record that fails, at the window's end, or once frontier-base reaches stopAt.
// The caller guarantees base <= frontier <= base+window and window <= len(records).
//
// A record left from an earlier lap ends at or before its boundary, and one from a later lap ends
// more than len(records) past it, so neither passes, and records never need clearing.
func discoverRecords(
	records []atomic.Uint64,
	mask uint32,
	base, window, frontier, stopAt uint64,
) uint64 {
	end := frontier
	var stride uint64
	for end-base < window {
		if end-base >= stopAt {
			break
		}
		if stride >= recordLookaheadMinStride {
			end = discoverRecordsAhead(records, mask, base, window, end, stride, stopAt)
			if end-base >= window || end-base >= stopAt {
				break
			}
		}
		recordEnd := records[uint32(end)&mask].Load()
		if recordEnd <= end || recordEnd-end > window-(end-base) {
			break
		}
		stride = recordEnd - end
		end = recordEnd
	}
	return end
}

// discoverRecordsAhead predicts that the records after end continue a run of stride-length
// intervals, and loads the records at those predicted boundaries before checking any of them.
// Each address is then known up front, so their cache misses overlap instead of each load waiting
// for the previous one to supply its address. It accepts records in order while each one is
// finished and exactly stride long, and returns the first boundary it did not accept for the
// caller to load normally.
//
// An early load is as good as a chained one. It is an atomic load of the same record, and a value
// counts only if it ends past its boundary and within the window, the same check chained discovery
// applies, so a wrong prediction or an unfinished interval costs only the wasted load.
func discoverRecordsAhead(
	records []atomic.Uint64,
	mask uint32,
	base, window, end, stride, stopAt uint64,
) uint64 {
	var ahead [recordLookahead]uint64
	lanes := 0
	for ; lanes < len(ahead); lanes++ {
		position := end + uint64(lanes)*stride
		if position-base >= window {
			break
		}
		ahead[lanes] = records[uint32(position)&mask].Load()
	}
	for lane := range lanes {
		if end-base >= stopAt {
			break
		}
		recordEnd := ahead[lane]
		if recordEnd <= end || recordEnd-end != stride || stride > window-(end-base) {
			break
		}
		end = recordEnd
	}
	return end
}
