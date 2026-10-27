package frontier

import "testing"

// Absolute coordinates use six bits. Completion history survives reusable record storage.
type cacheModelObserver struct {
	phase, released, frontier, head, cursor, end, limit, expected, result uint8
	retain                                                                bool
}

type cacheModel struct {
	head, cursor, frontier, released, published, cache uint8
	records, payload                                   [4]uint8
	producers, starts                                  [2]uint8
	ownerPhase, ownerEnd, ownerCount, ownerCalls       uint8
	observers                                          [2]cacheModelObserver
	history                                            uint64
}

func cacheModelCompleted(state cacheModel, start, end uint8) bool {
	for position := start; position < end; position++ {
		if state.history&(uint64(1)<<position) == 0 {
			return false
		}
	}
	return true
}

func cacheModelDiscover(state cacheModel, capacity uint8) uint8 {
	end := state.frontier
	for end-state.cursor < capacity {
		record := state.records[end%capacity]
		if record <= end || record-end > capacity-(end-state.cursor) {
			break
		}
		end = record
	}
	return end
}

func cacheModelTransitions(
	t *testing.T,
	state cacheModel,
	capacity uint8,
	lengths [2]uint8,
) []cacheModel {
	t.Helper()
	next := make([]cacheModel, 0, 12)
	for producer, length := range lengths {
		candidate := state
		switch state.producers[producer] {
		case 0:
			if state.head-state.released+length > capacity || int(state.head)+int(length) > 63 {
				continue
			}
			candidate.starts[producer] = state.head
			candidate.head += length
			candidate.producers[producer] = 1
		case 1:
			start := state.starts[producer]
			for position := start; position < start+length; position++ {
				candidate.payload[position%capacity] = position + 1
			}
			candidate.producers[producer] = 2
		case 2:
			start := state.starts[producer]
			for position := start; position < start+length; position++ {
				if state.payload[position%capacity] != position+1 {
					t.Fatal("completion before payload")
				}
				candidate.history |= uint64(1) << position
			}
			candidate.records[start%capacity] = start + length
			candidate.producers[producer] = 3
		default:
			continue
		}
		next = append(next, candidate)
	}
	if state.ownerCalls < 2 {
		candidate := state
		switch state.ownerPhase {
		case 0:
			candidate.ownerEnd = state.frontier
			candidate.ownerPhase = 1
			next = append(next, candidate)
		case 1:
			end := state.ownerEnd
			record := state.records[end%capacity]
			if end-state.cursor < capacity && record > end &&
				record-end <= capacity-(end-state.cursor) {
				candidate.ownerEnd = record
				next = append(next, candidate)
			} else {
				for count := uint8(0); count <= min(uint8(2), end-state.cursor); count++ {
					candidate := state
					candidate.frontier = end
					candidate.ownerCount = count
					candidate.ownerPhase = 2
					// Acquiring without movement publishes new proof before returning.
					if count == 0 {
						candidate.published = end % 8
						candidate.ownerCalls++
						candidate.ownerPhase = 0
					}
					next = append(next, candidate)
				}
			}
		case 2:
			end := state.cursor + state.ownerCount
			if !cacheModelCompleted(state, state.cursor, end) {
				t.Fatal("uncompleted acquisition")
			}
			for position := state.cursor; position < end; position++ {
				if state.payload[position%capacity] != position+1 {
					t.Fatal("model FIFO", state)
				}
				candidate.payload[position%capacity] = 0
			}
			candidate.ownerPhase = 3
			next = append(next, candidate)
		case 3:
			candidate.cursor += state.ownerCount
			candidate.released = candidate.cursor
			candidate.published = candidate.frontier % 8
			candidate.ownerPhase = 0
			candidate.ownerCount = 0
			candidate.ownerCalls++
			next = append(next, candidate)
		}
	}
	for observer, observed := range state.observers {
		candidate := state
		current := observed
		switch observed.phase {
		case 0:
			current.released, current.frontier = state.released%8, state.published
			current.result = (current.frontier - current.released) & 7
			current.result = min(current.result, capacity)
			current.phase = 1
			if (current.frontier-current.released)&7 == capacity {
				current.phase = 4
			}
		case 1:
			current.head = state.head
			distance := (state.head - current.released) & 7
			acquired := (current.frontier - current.released) & 7
			current.phase = 4
			if distance <= state.head && acquired <= min(distance, capacity) {
				current.cursor = state.head - distance
				current.limit = min(distance, capacity)
				current.end = current.cursor + acquired
				current.result = acquired
				if acquired < current.limit {
					current.phase = 2
				}
			}
		case 2:
			current.expected = state.cache
			if state.cache >= current.end && state.cache-current.cursor <= current.limit {
				if !cacheModelCompleted(state, current.end, state.cache) {
					t.Fatal("invalid cache skip")
				}
				current.end = state.cache
			}
			current.retain = true
			current.phase = 3
		case 3:
			current.phase = 4
			if observed.end-observed.cursor < observed.limit {
				record := state.records[observed.end%capacity]
				if record > observed.end &&
					record-observed.end <= observed.limit-(observed.end-observed.cursor) {
					current.end = record
					current.phase = 3
				} else if observed.released == state.released%8 && observed.frontier == state.published {
					if cacheModelCompleted(state, observed.end, observed.end+1) {
						t.Fatal("stable scan stopped before completed prefix", state, observed)
					}
				}
			}
			current.result = current.end - current.cursor
		case 4:
			if observed.released != state.released%8 || observed.frontier != state.published {
				current.result = min((state.published-state.released)&7, capacity)
				current.retain = false
				if current.result != min((state.published-state.released)&7, capacity) {
					t.Fatal("fallback differs from final acquired prefix")
				}
			} else if current.retain {
				if !cacheModelCompleted(state, current.cursor, current.end) {
					t.Fatal("validated incomplete prefix", state, current)
				}
			}
			if current.result > capacity || current.end-current.cursor > capacity {
				t.Fatal("unbounded observation", current)
			}
			current.phase = 5
		case 5:
			if observed.retain && observed.end > observed.expected && state.cache == observed.expected {
				if observed.end < state.cache || !cacheModelCompleted(state, 0, observed.end) {
					t.Fatal("nonhistorical or decreasing successful proof", state, observed)
				}
				candidate.cache = observed.end
			}
			current.phase = 6
		default:
			continue
		}
		candidate.observers[observer] = current
		next = append(next, candidate)
	}
	return next
}

func TestMPSCCacheReducedSCSchedules(t *testing.T) {
	total := 0
	coverage := make(map[string]bool)
	for _, capacity := range []uint8{1, 2, 4} {
		for _, cursor := range []uint8{0, 6} {
			for _, first := range []uint8{1, min(2, capacity)} {
				for _, second := range []uint8{1, min(2, capacity)} {
					initial := cacheModel{
						head: cursor, cursor: cursor, frontier: cursor, released: cursor, published: cursor % 8,
						history: (uint64(1) << cursor) - 1,
					}
					seen := map[cacheModel]uint8{initial: 0}
					layer := []cacheModel{initial}
					for depth := uint8(0); depth < 32 && len(layer) > 0; depth++ {
						next := make([]cacheModel, 0)
						for _, state := range layer {
							if state.head > state.cursor &&
								!cacheModelCompleted(state, state.cursor, state.cursor+1) {
								coverage["hole"] = true
							}
							if state.ownerCalls > 0 && state.frontier > state.cursor {
								coverage["partial-release"] = true
							}
							for _, observed := range state.observers {
								if observed.phase == 2 && state.cache > observed.head {
									coverage["cache-beyond-head"] = true
								}
								if observed.phase == 5 && observed.retain && observed.end > observed.expected {
									if state.cache == observed.expected && state.cursor >= observed.end &&
										state.head > observed.end {
										coverage["delayed-success-after-reuse"] = true
									}
									if state.cache > observed.expected {
										coverage["delayed-failure-new-proof"] = true
									}
								}
								if observed.phase == 5 && !observed.retain &&
									(observed.released != state.released%8 || observed.frontier != state.published) {
									coverage["changed-fallback"] = true
								}
							}
							candidates := cacheModelTransitions(t, state, capacity, [2]uint8{first, second})
							for _, candidate := range candidates {
								if _, exists := seen[candidate]; !exists {
									seen[candidate] = depth + 1
									next = append(next, candidate)
								}
							}
						}
						layer = next
					}
					total += len(seen)
					t.Logf("SC low=3 absolute=6 C=%d initial=%d lengths=%d,%d depth=32 states=%d",
						capacity, cursor, first, second, len(seen))
				}
			}
		}
	}
	for _, name := range []string{
		"hole", "partial-release", "cache-beyond-head", "delayed-success-after-reuse",
		"delayed-failure-new-proof", "changed-fallback",
	} {
		if !coverage[name] {
			t.Fatal("missing SC schedule coverage", name)
		}
	}
	t.Logf("SC total states=%d coverage=%v", total, coverage)
}

type cacheLifetimeAudit struct {
	validations, publications, advances, recordLoads int
	behind, warm, acquiredFull, calls                int
}

func cacheModelCompleteRange(
	t *testing.T,
	state cacheModel,
	capacity, count uint8,
) cacheModel {
	t.Helper()
	if count == 0 || count > capacity {
		t.Fatal("driver invalid reservation length", count)
	}
	if state.head-state.released+count > capacity || int(state.head)+int(count) > 63 {
		t.Fatal("driver violated lifetime or released room", state, count)
	}
	start := state.head
	state.head += count
	for position := start; position < state.head; position++ {
		state.payload[position%capacity] = position + 1
		state.history |= uint64(1) << position
	}
	state.records[start%capacity] = state.head
	return state
}

func cacheModelAcquire(t *testing.T, state cacheModel, capacity uint8) cacheModel {
	t.Helper()
	end := cacheModelDiscover(state, capacity)
	if !cacheModelCompleted(state, state.cursor, end) {
		t.Fatal("driver discovered incomplete prefix", state, end)
	}
	prefix := state.frontier
	for prefix < state.head && cacheModelCompleted(state, prefix, prefix+1) {
		prefix++
	}
	if end != prefix {
		t.Fatal("driver discovery differs from history", end, prefix)
	}
	state.frontier = end
	state.published = end % 8
	return state
}

func cacheModelDrain(t *testing.T, state cacheModel, capacity uint8) cacheModel {
	t.Helper()
	for position := state.cursor; position < state.frontier; position++ {
		if state.payload[position%capacity] != position+1 {
			t.Fatal("driver FIFO", state, position)
		}
		state.payload[position%capacity] = 0
	}
	state.cursor = state.frontier
	state.released = state.cursor
	state.published = state.frontier % 8
	return state
}

func cacheModelObserveLifetime(
	t *testing.T,
	state cacheModel,
	capacity uint8,
	audit cacheLifetimeAudit,
) (cacheModel, cacheLifetimeAudit) {
	t.Helper()
	if state.observers[0].phase != 0 && state.observers[0].phase != 6 {
		t.Fatal("reinitializing active observer", state)
	}
	startHead, startReleased := state.head, state.released
	startLoads := audit.recordLoads
	state.observers[0] = cacheModelObserver{}
	for state.observers[0].phase != 6 {
		before := state
		observed := before.observers[0]
		state = cacheModelStepObserver(t, state, capacity, 0)
		switch observed.phase {
		case 0:
			if (before.published-before.released)&7 == capacity {
				audit.acquiredFull++
			}
		case 2:
			if before.cache < observed.end {
				if state.observers[0].end != observed.end {
					t.Fatal("behind-cursor cache changed scan start")
				}
				audit.behind++
			}
			if state.observers[0].end-observed.cursor == observed.limit {
				audit.warm++
			}
		case 3:
			if observed.end-observed.cursor < observed.limit {
				audit.recordLoads++
			}
		case 4:
			audit.validations++
		case 5:
			audit.publications++
		}
		if state.cache < before.cache {
			t.Fatal("lifetime cache decreased")
		}
		if state.cache != before.cache {
			casAllowed := observed.phase == 5 && observed.retain &&
				observed.end > observed.expected && before.cache == observed.expected
			if !casAllowed || state.cache != observed.end {
				t.Fatal("cache changed without modeled CAS", before, state)
			}
			if !cacheModelCompleted(before, 0, state.cache) {
				t.Fatal("CAS lacks independent historical completion")
			}
			audit.advances++
		}
	}
	prefix := startReleased
	for prefix < state.head && cacheModelCompleted(state, prefix, prefix+1) {
		prefix++
	}
	if state.observers[0].result != prefix-startReleased {
		t.Fatal("stable Count differs from independent history", state, prefix)
	}
	if state.head-startHead >= 8 {
		t.Fatal("observer crossed full low-word cycle")
	}
	audit.calls++
	t.Logf("C=%d observer head=%d..%d released=%d..%d result=%d cache=%d loads=%d",
		capacity, startHead, state.head, startReleased, state.released,
		state.observers[0].result, state.cache, audit.recordLoads-startLoads)
	return state, audit
}

func cacheModelDriveRange(
	t *testing.T,
	state cacheModel,
	capacity, count uint8,
) cacheModel {
	t.Helper()
	state = cacheModelCompleteRange(t, state, capacity, count)
	state = cacheModelAcquire(t, state, capacity)
	state, _ = cacheModelObserveLifetime(t, state, capacity, cacheLifetimeAudit{})
	return cacheModelDrain(t, state, capacity)
}

func TestMPSCCacheReducedLifetimeDrivers(t *testing.T) {
	for _, capacity := range []uint8{1, 2, 4} {
		state := cacheModel{}
		state = cacheModelDriveRange(t, state, capacity, 1)
		for state.head < 24 {
			state = cacheModelDriveRange(t, state, capacity, min(capacity, 24-state.head))
		}
		if capacity == 4 && state.records[0] != 1 {
			t.Fatal("untouched stale record missing")
		}
		for state.head < 63 {
			state = cacheModelDriveRange(t, state, capacity, min(capacity, 63-state.head))
		}
		// The enforced guard excludes the first reservation whose exclusive end would wrap.
		if int(state.head)+1 <= 63 {
			t.Fatal("rollover guard did not exclude next reservation")
		}
		start, wrappedEnd := state.head, (state.head+1)&63
		state.records[start%capacity] = wrappedEnd
		state.head = wrappedEnd
		if cacheModelDiscover(state, capacity) != 63 {
			t.Fatal("missing rollover counterexample")
		}
		t.Logf("counterexample C=%d cursor=63 start=63 length=1 wrapped end=0 record=0 prefix=63",
			capacity)
		// No active protocol calls or delayed publication exist at this exclusive Reset.
		state = cacheModel{}
		state = cacheModelCompleteRange(t, state, capacity, capacity)
		state, _ = cacheModelObserveLifetime(t, state, capacity, cacheLifetimeAudit{})
		state = cacheModelAcquire(t, state, capacity)
		state = cacheModelDrain(t, state, capacity)
		if state.cache != capacity || state.cursor != capacity {
			t.Fatal("reset lifetime")
		}
	}
}

func cacheModelStepObserver(
	t *testing.T,
	state cacheModel,
	capacity uint8,
	observer int,
) cacheModel {
	t.Helper()
	for _, candidate := range cacheModelTransitions(t, state, capacity, [2]uint8{1, 1}) {
		if candidate.observers[observer] != state.observers[observer] {
			return candidate
		}
	}
	t.Fatal("no observer transition", observer, state)
	return state
}

func TestMPSCCacheReducedHeadCacheInterleaving(t *testing.T) {
	// This legal SC schedule pauses A after its head load.
	// B retains newer proof before A's cache load.
	state := cacheModel{
		head: 1, records: [4]uint8{1}, payload: [4]uint8{1}, history: 1,
		producers: [2]uint8{3, 3}, ownerCalls: 2,
	}
	state = cacheModelStepObserver(t, state, 2, 0)
	state = cacheModelStepObserver(t, state, 2, 0)
	if state.observers[0].head != 1 || state.observers[0].phase != 2 {
		t.Fatal("captured head boundary")
	}
	// A second legal reservation completes while both old slots are still live.
	state.head, state.records[1], state.payload[1], state.history = 2, 2, 2, 3
	for state.observers[1].phase != 6 {
		state = cacheModelStepObserver(t, state, 2, 1)
	}
	if state.cache != 2 {
		t.Fatal("new proof")
	}
	for state.observers[0].phase != 6 {
		state = cacheModelStepObserver(t, state, 2, 0)
	}
	if state.observers[0].expected != 2 || state.observers[0].result != 1 || state.cache != 2 {
		t.Fatal("captured window was extended", state)
	}
	t.Log("legal SC boundary: A head=1, B cache=2, A cache=2 rejected, A result=1")
}

func TestMPSCCacheReducedLifetimeObserverProof(t *testing.T) {
	for _, capacity := range []uint8{1, 2, 4} {
		driven := cacheModelDriveRange(t, cacheModel{}, capacity, 1)
		if driven.observers[0].phase != 6 {
			t.Fatal("range driver omitted observer validation/publication phases", capacity, driven)
		}
		state := cacheModelCompleteRange(t, cacheModel{}, capacity, 1)
		state, audit := cacheModelObserveLifetime(t, state, capacity, cacheLifetimeAudit{})
		if state.observers[0].result != 1 || state.cache != 1 {
			t.Fatal("first scalar proof", state)
		}
		state = cacheModelAcquire(t, state, capacity)
		state = cacheModelDrain(t, state, capacity)
		firstEndpoint := state.cache
		for state.released-firstEndpoint < 24 {
			state = cacheModelCompleteRange(t, state, capacity, capacity)
			state = cacheModelAcquire(t, state, capacity)
			state, audit = cacheModelObserveLifetime(t, state, capacity, audit)
			if state.cache != firstEndpoint || state.observers[0].retain {
				t.Fatal("acquired-full changed historical cache", state)
			}
			state = cacheModelDrain(t, state, capacity)
		}
		state = cacheModelCompleteRange(t, state, capacity, capacity)
		state, audit = cacheModelObserveLifetime(t, state, capacity, audit)
		if state.observers[0].expected != firstEndpoint || state.cache != state.head {
			t.Fatal("fresh scan failed to replace behind-cursor proof", state)
		}
		loads, validations := audit.recordLoads, audit.validations
		publications, advances, cache := audit.publications, audit.advances, state.cache
		state, audit = cacheModelObserveLifetime(t, state, capacity, audit)
		if audit.recordLoads != loads || audit.validations != validations+1 {
			t.Fatal("warm cache failed validation or skipped-record proof", state, audit)
		}
		if audit.publications != publications+1 || audit.advances != advances {
			t.Fatal("warm cache attempted an advancing CAS", state, audit)
		}
		if state.observers[0].result != capacity || state.cache != cache {
			t.Fatal("warm Count result", state)
		}
		state = cacheModelAcquire(t, state, capacity)
		state = cacheModelDrain(t, state, capacity)
		for state.head < 63 {
			count := min(capacity, 63-state.head)
			state = cacheModelCompleteRange(t, state, capacity, count)
			state = cacheModelAcquire(t, state, capacity)
			state, audit = cacheModelObserveLifetime(t, state, capacity, audit)
			state = cacheModelDrain(t, state, capacity)
		}
		if state.head != 63 || int(state.head)+1 <= 63 {
			t.Fatal("missing Max63 boundary")
		}
		if state.observers[0].phase != 6 || state.observers[1].phase != 0 {
			t.Fatal("Reset with active observer")
		}
		// All synchronous producer and owner operations and observer CAS have finished.
		state = cacheModel{}
		state = cacheModelCompleteRange(t, state, capacity, capacity)
		state, resetAudit := cacheModelObserveLifetime(
			t, state, capacity, cacheLifetimeAudit{},
		)
		if state.cache != capacity || resetAudit.advances != 1 {
			t.Fatal("new lifetime lacks real CAS", state, resetAudit)
		}
		state = cacheModelAcquire(t, state, capacity)
		state = cacheModelDrain(t, state, capacity)
		if state.cursor != capacity {
			t.Fatal("Reset lifetime drain", state)
		}
		if audit.validations == 0 || audit.publications == 0 || audit.advances == 0 {
			t.Fatal("lifetime lacks observer validation/publication/CAS", capacity, audit)
		}
		if audit.behind == 0 || audit.warm == 0 || audit.acquiredFull == 0 {
			t.Fatal("lifetime lacks behind/warm/acquired-full coverage", capacity, audit)
		}
		t.Logf("lifetime C=%d audit=%+v Reset=%+v", capacity, audit, resetAudit)
	}
}
