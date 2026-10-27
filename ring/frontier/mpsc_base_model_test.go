package frontier

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The model uses SC events. Its payload generation is absolute position plus one.
// Two one-shot producers cannot exercise later start-slot reuse. The driven test does that.
type baseTestModel struct {
	head, cursor, frontier                                 int
	released, published                                    int
	records, payload                                       [4]int
	producerPhase, start                                   [2]int
	ownerPhase, ownerCount, ownerCalls                     int
	noMoveDone, resetDone                                  bool
	observerPhase, beforeCursor, beforeFrontier            int
	capturedHead, scanCursor, scanEnd, scanLimit, observed int
}

func baseTestModelDiscover(state baseTestModel) int {
	end := state.frontier
	for end-state.cursor < 4 {
		record := state.records[end%4]
		if record <= end || record-end > 4-(end-state.cursor) {
			break
		}
		end = record
	}
	return end
}

func baseTestModelPrefix(t *testing.T, state baseTestModel, end int) {
	t.Helper()
	if end < state.cursor || end-state.cursor > 4 || end > state.head {
		t.Fatal("model acquisition bounds", state, end)
	}
	for position := state.cursor; position < end; position++ {
		if state.payload[position%4] != position+1 {
			t.Fatal("model acquired uninitialized generation", state, position)
		}
	}
}

func TestMPSCBaseReducedSCSchedules(t *testing.T) {
	totalStates := 0
	holes, reverse, partial, changed, resets := false, false, false, false, false
	for firstLength := 1; firstLength <= 2; firstLength++ {
		for secondLength := 1; secondLength <= 2; secondLength++ {
			lengths := [2]int{firstLength, secondLength}
			seen := map[baseTestModel]bool{{}: true}
			layer := []baseTestModel{{}}
			for depth := 0; depth < 24 && len(layer) != 0; depth++ {
				next := make([]baseTestModel, 0)
				add := func(state baseTestModel) {
					if !seen[state] {
						seen[state] = true
						next = append(next, state)
					}
				}
				for _, state := range layer {
					for producer, length := range lengths {
						candidate := state
						switch state.producerPhase[producer] {
						case 0:
							if state.head-state.released+length > 4 {
								continue
							}
							candidate.start[producer] = state.head
							candidate.head += length
							candidate.producerPhase[producer] = 1
						case 1:
							for position := state.start[producer]; position < state.start[producer]+length; position++ {
								if position < state.released || position >= state.released+4 {
									t.Fatal("model reused before release", state)
								}
								candidate.payload[position%4] = position + 1
							}
							candidate.producerPhase[producer] = 2
						case 2:
							start := state.start[producer]
							for position := start; position < start+length; position++ {
								if state.payload[position%4] != position+1 {
									t.Fatal("model completion before write")
								}
							}
							candidate.records[start%4] = start + length
							candidate.producerPhase[producer] = 3
							if start > 0 && state.records[0] == 0 {
								reverse = true
							}
						default:
							continue
						}
						add(candidate)
					}
					if state.ownerPhase == 0 {
						end := baseTestModelDiscover(state)
						baseTestModelPrefix(t, state, end)
						if end == state.cursor && state.head > end {
							holes = true
						}
						if !state.noMoveDone {
							// Zero and failed Bulk share acquisition/publication when no payload moves.
							for _, requested := range []int{0, 4} {
								if requested != 0 && end-state.cursor >= requested {
									continue
								}
								candidate := state
								candidate.noMoveDone = true
								candidate.frontier = end
								if end > state.frontier {
									candidate.published = end % 16
									candidate.released = state.cursor
								}
								add(candidate)
							}
						}
						if state.ownerCalls < 2 {
							for count := 1; count <= 2 && count <= end-state.cursor; count++ {
								candidate := state
								candidate.frontier = end
								candidate.ownerCount = count
								candidate.ownerPhase = 1
								if count < end-state.cursor {
									partial = true
								}
								add(candidate)
							}
						}
					} else if state.ownerPhase == 1 {
						candidate := state
						if state.cursor+state.ownerCount > state.frontier {
							t.Fatal("model read before acquisition")
						}
						for position := state.cursor; position < state.cursor+state.ownerCount; position++ {
							if state.payload[position%4] != position+1 {
								t.Fatal("model FIFO payload", state)
							}
							candidate.payload[position%4] = 0
						}
						candidate.ownerPhase = 2
						add(candidate)
					} else {
						candidate := state
						candidate.cursor += state.ownerCount
						candidate.released = candidate.cursor
						candidate.published = state.frontier % 16
						candidate.ownerPhase = 0
						candidate.ownerCount = 0
						candidate.ownerCalls++
						add(candidate)
					}
					candidate := state
					switch state.observerPhase {
					case 0:
						candidate.beforeCursor = state.released % 16
						candidate.beforeFrontier = state.published
						candidate.observerPhase = 1
					case 1:
						candidate.capturedHead = state.head
						distance := (state.head%16 - state.beforeCursor + 16) % 16
						acquired := (state.beforeFrontier - state.beforeCursor + 16) % 16
						candidate.observed = min(acquired, 4)
						candidate.observerPhase = 3
						if distance <= state.head && acquired <= min(distance, 4) {
							candidate.scanCursor = state.head - distance
							candidate.scanEnd = candidate.scanCursor + acquired
							candidate.scanLimit = min(distance, 4)
							candidate.observerPhase = 2
						}
					case 2:
						end := state.scanEnd
						candidate.observerPhase = 3
						if end-state.scanCursor < state.scanLimit {
							record := state.records[end%4]
							if record > end && record-end <= state.scanLimit-(end-state.scanCursor) {
								candidate.scanEnd = record
								candidate.observerPhase = 2
							}
						}
						candidate.observed = candidate.scanEnd - state.scanCursor
					case 3:
						if state.beforeCursor != state.released%16 || state.beforeFrontier != state.published {
							candidate.observed = (state.published - state.released%16 + 16) % 16
							changed = true
						}
						if candidate.observed < 0 || candidate.observed > 4 {
							t.Fatal("model observer bounds", candidate)
						}
						if state.beforeCursor == state.released%16 && state.beforeFrontier == state.published {
							for position := state.released; position < state.released+candidate.observed; position++ {
								// An owner may have read/cleared before final release. Completion is its proof.
								initialized := false
								for producer, length := range lengths {
									start := state.start[producer]
									if state.producerPhase[producer] == 3 && position >= start && position < start+length {
										initialized = true
									}
								}
								if !initialized {
									t.Fatal("model observer counted unwritten generation", state)
								}
							}
						}
						candidate.observerPhase = 4
					}
					if state.observerPhase < 4 {
						add(candidate)
					}
					quiescent := state.producerPhase == [2]int{3, 3} && state.observerPhase == 4 &&
						state.ownerPhase == 0 && state.cursor == state.head
					if quiescent && !state.resetDone {
						candidate := baseTestModel{resetDone: true}
						candidate.producerPhase = [2]int{3, 3}
						candidate.observerPhase = 4
						candidate.ownerCalls = 2
						candidate.noMoveDone = true
						add(candidate)
						resets = true
					}
				}
				layer = next
			}
			totalStates += len(seen)
		}
	}
	if !holes || !reverse || !partial || !changed || !resets {
		t.Fatal("missing model schedule coverage", holes, reverse, partial, changed, resets)
	}
	t.Logf("SC states=%d depth=24 low modulus=16 C=4 absolute modulus=256", totalStates)
}

func TestMPSCBaseReducedAbsoluteLifetime(t *testing.T) {
	const absoluteModulus = 256
	state := baseTestModel{}
	reserve := func(count int) int {
		t.Helper()
		if state.head != state.cursor || state.head+count >= absoluteModulus {
			t.Fatal("valid driver exceeded lifetime or reused active work")
		}
		start := state.head
		state.head += count
		return start
	}
	writePublishDrain := func(start, count int) {
		t.Helper()
		for position := start; position < start+count; position++ {
			state.payload[position%4] = position + 1
		}
		state.records[start%4] = start + count
		state.frontier = baseTestModelDiscover(state)
		if state.frontier != state.head {
			t.Fatal("real completion rejected", state)
		}
		baseTestModelPrefix(t, state, state.frontier)
		for position := state.cursor; position < state.frontier; position++ {
			if state.payload[position%4] != position+1 {
				t.Fatal("driver FIFO generation", state)
			}
			state.payload[position%4] = 0
		}
		state.cursor = state.frontier
		state.released = state.cursor
		state.published = state.frontier % 16
	}
	completeAndDrain := func(count int) {
		t.Helper()
		start := reserve(count)
		writePublishDrain(start, count)
	}
	completeAndDrain(1)
	for lap := 0; lap < 11; lap++ {
		completeAndDrain(4)
	}
	completeAndDrain(3)
	if state.head != 48 || state.records[0] != 1 {
		t.Fatal("idle record fixture", state)
	}
	start := reserve(1)
	if baseTestModelDiscover(state) != state.cursor {
		t.Fatal("ancient record accepted within lifetime")
	}
	writePublishDrain(start, 1)
	if state.head != state.cursor || state.cursor != state.frontier {
		t.Fatal("Reset with active work")
	}
	state = baseTestModel{}
	start = reserve(1)
	if baseTestModelDiscover(state) != 0 {
		t.Fatal("Reset admitted an incomplete range")
	}
	writePublishDrain(start, 1)
	// Starts 1 modulo 4 leave record zero idle through fifteen complete low-word cycles.
	for lap := 0; lap < 63; lap++ {
		completeAndDrain(4)
	}
	completeAndDrain(2)
	if state.head != 255 || state.records[0] != 1 || state.cursor != state.head {
		t.Fatal("last valid coordinate", state)
	}
	// This first violating reservation wraps its exclusive end. Discovery cannot cross it.
	start = state.head
	state.head = (state.head + 2) % absoluteModulus
	for offset := 0; offset < 2; offset++ {
		state.payload[(start+offset)%4] = start + offset + 1
	}
	state.records[start%4] = state.head
	if state.head != 1 || state.records[3] != 1 ||
		baseTestModelDiscover(state) != 255 {
		t.Fatal("missing out-of-contract wrapped-completion liveness failure", state)
	}
	// This arithmetic witness is staged outside the contract. It is not a reachable valid trace.
	ancientEnd, stagedStart, stagedHead := 1, 0, 1
	if stagedHead-stagedStart != 1 || ancientEnd <= stagedStart ||
		ancientEnd-stagedStart > 4 {
		t.Fatal("missing staged false-acquisition arithmetic witness")
	}
}

func baseTestNodeText(t *testing.T, node ast.Node) string {
	t.Helper()
	var output bytes.Buffer
	if err := format.Node(&output, token.NewFileSet(), node); err != nil {
		t.Fatal(err)
	}
	return output.String()
}

func TestMPSCBaseSourceOrdering(t *testing.T) {
	const source = "mpsc_base.go"
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), source, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	methods := make(map[string]*ast.FuncDecl)
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		methods[function.Name.Name] = function
		name := function.Name.Name
		if strings.Contains(name, "Retire") || strings.Contains(name, "Anchor") {
			t.Fatal("retirement/anchor helper", name)
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Store" {
				return true
			}
			receiver := baseTestNodeText(t, selector.X)
			if strings.Contains(receiver, "completion.records") &&
				name != "Reset" && name != "mpscPublishProducer" {
				t.Fatal("completion store outside publication/Reset", name)
			}
			if strings.Contains(receiver, "consumerPublished") && name != "Reset" &&
				name != "mpscReleaseConsumer" && name != "mpscDequeueBatch" {
				t.Fatal("unexpected consumer store", name)
			}
			return true
		})
	}
	requireOrder := func(name string, fragments ...string) {
		t.Helper()
		function := methods[name]
		if function == nil {
			t.Fatal("missing function", name)
		}
		text := baseTestNodeText(t, function.Body)
		offset := 0
		for _, fragment := range fragments {
			index := strings.Index(text[offset:], fragment)
			if index < 0 {
				t.Fatalf("%s missing ordered fragment %q", name, fragment)
			}
			offset += index + len(fragment)
		}
	}
	requireOrder("mpscReleaseConsumer", "cursor += uint64(count)",
		"consumerPublished.Store(")
	requireOrder("Dequeue", "value := consumer.entries[index]", "consumer.entries[index] = zero",
		"mpscReleaseConsumer(1)")
	requireOrder("mpscDequeueBatch", "previousFrontier := consumer.frontier",
		"if consumer.frontier > previousFrontier", "consumerPublished.Store(", "return 0,",
		"mpscReadConsumer(", "mpscReleaseConsumer(length)")
	requireOrder("mpscAttemptProducer", "headLow := uint32(head)",
		"free := capacity + released - headLow", "CompareAndSwap(head, head+uint64(count))")
	requireOrder("mpscCountFromSnapshot", "head :=", "distance := uint32(head)",
		"if uint64(distance) <= head", "cursor := head - uint64(distance)",
		"if uint64(acquired) <= limit", "recordEnd :=", "after :=", "if before != after",
		"return int(result)")
	for _, name := range []string{"mpscDiscover", "mpscReadConsumer"} {
		text := baseTestNodeText(t, methods[name].Body)
		if strings.Contains(text, ".Store(") {
			t.Fatal("publication in read/discovery", name)
		}
	}
	invalid := methods["DequeueBulk"].Body.List[2].(*ast.IfStmt)
	invalidText := baseTestNodeText(t, invalid)
	if !strings.Contains(invalidText, "mpscDiscover(") ||
		strings.Contains(invalidText, ".Store(") || strings.Contains(invalidText, "consumer.frontier =") {
		t.Fatal("invalid Bulk mutated proof")
	}
	for _, name := range []string{"mpscReleaseConsumer",
		"mpscPublishProducer", "mpscDequeueBatch"} {
		stores := strings.Count(baseTestNodeText(t, methods[name].Body), ".Store(")
		if stores != 1 {
			t.Fatal("structural store count", name, stores)
		}
	}
}
