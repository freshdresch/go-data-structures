package ring_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

type benchmarkManifest struct {
	Schema     int                    `json:"schema"`
	GOMAXPROCS int                    `json:"gomaxprocs"`
	Rows       []benchmarkManifestRow `json:"rows"`
}

type benchmarkManifestRow struct {
	Name              string   `json:"name"`
	Family            string   `json:"family"`
	Implementation    string   `json:"implementation"`
	API               *string  `json:"api"`
	APIReason         string   `json:"api_reason"`
	Element           string   `json:"element"`
	Batch             int      `json:"batch"`
	UsableCapacity    int      `json:"usable_capacity"`
	Producers         *int     `json:"producers"`
	Consumers         *int     `json:"consumers"`
	WorkerReason      string   `json:"worker_reason"`
	Callers           int      `json:"callers"`
	Participants      int      `json:"participants"`
	QueueCount        int      `json:"queue_count"`
	RingOptions       []string `json:"ring_options"`
	RingOptionsReason string   `json:"ring_options_reason"`
	ProducerWait      *string  `json:"producer_wait"`
	ConsumerWait      *string  `json:"consumer_wait"`
	WaitReason        string   `json:"wait_reason"`
	PrimaryMetric     string   `json:"primary_metric"`
	TimingBoundary    string   `json:"timing_boundary"`
	PointerPoolSize   *int     `json:"pointer_pool_size"`
	PointerPoolReason string   `json:"pointer_pool_reason"`
	Storage           string   `json:"storage"`
}

func manifestPointer[T any](value T) *T { return &value }

func manifestRingOptions(mode benchMode) []string {
	options := []string{}
	if mode.producers > 1 {
		option := "WithMultiProdEnqueue"
		if mode.relaxed {
			option = "WithRelaxedMultiProdEnqueue"
		}
		options = append(options, option)
	}
	if mode.consumers > 1 {
		option := "WithMultiConsDequeue"
		if mode.relaxed {
			option = "WithRelaxedMultiConsDequeue"
		}
		options = append(options, option)
	}
	return options
}

func manifestRow(name, implementation string, mode benchMode, batch int) benchmarkManifestRow {
	return benchmarkManifestRow{
		Name: name, Family: "Throughput", Implementation: implementation,
		APIReason: "implementation adapter", Element: "int64", Batch: batch,
		UsableCapacity: ringCapacity, Producers: manifestPointer(mode.producers),
		Consumers: manifestPointer(mode.consumers), QueueCount: 1,
		RingOptions:   manifestRingOptions(mode),
		ProducerWait:  manifestPointer("runtime.Gosched after each unsuccessful enqueue attempt"),
		ConsumerWait:  manifestPointer("runtime.Gosched after each unsuccessful dequeue attempt"),
		PrimaryMetric: "ns/op",
		TimingBoundary: "excludes queue/buffer setup and pre-run GC; includes worker startup, " +
			"payload fill, operations, retries, producer completion, drain, and joins",
		PointerPoolReason: "int64 values do not use a pointer pool", Storage: "typed int64",
	}
}

var spscManifestSelection = flag.String(
	"spsc-manifest",
	"legacy",
	"legacy, concurrent, or sequential",
)

func selectedBenchmarkManifest(selection string) (benchmarkManifest, error) {
	switch selection {
	case "legacy":
		return buildBenchmarkManifest(), nil
	case "concurrent":
		return buildSPSCConcurrentManifest(), nil
	case "sequential":
		return buildSPSCSequentialManifest(), nil
	default:
		return benchmarkManifest{}, fmt.Errorf("unknown SPSC manifest %q", selection)
	}
}

func TestBenchmarkManifest(t *testing.T) {
	manifest, err := selectedBenchmarkManifest(*spscManifestSelection)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("BENCHMARK_MANIFEST " + string(encoded))
}

func TestBenchmarkManifestRows(t *testing.T) {
	first := buildBenchmarkManifest()
	second := buildBenchmarkManifest()
	firstBytes, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) {
		t.Fatal("manifest is not deterministic")
	}
	if first.GOMAXPROCS != runtime.GOMAXPROCS(0) {
		t.Fatal("incorrect GOMAXPROCS")
	}
	seen := map[string]bool{}
	for _, row := range first.Rows {
		if seen[row.Name] {
			t.Fatalf("duplicate row %s", row.Name)
		}
		seen[row.Name] = true
		if row.Name == "" || row.Family == "" || row.Implementation == "" ||
			row.Element == "" || row.Batch <= 0 || row.UsableCapacity != ringCapacity ||
			row.PrimaryMetric == "" || row.TimingBoundary == "" || row.Storage == "" {
			t.Fatalf("incomplete row %+v", row)
		}
		if row.API == nil && row.APIReason == "" {
			t.Fatal("missing API null reason")
		}
		if row.Producers == nil && row.WorkerReason == "" {
			t.Fatal("missing worker null reason")
		}
		if row.RingOptions == nil && row.RingOptionsReason == "" {
			t.Fatal("missing ring options null reason")
		}
		if row.ProducerWait == nil && row.WaitReason == "" {
			t.Fatal("missing wait null reason")
		}
		if row.PointerPoolSize == nil && row.PointerPoolReason == "" {
			t.Fatal("missing pointer pool null reason")
		}
	}
	checkManifestExamples(t, first.Rows)
}

func buildBenchmarkManifest() benchmarkManifest {
	manifest := benchmarkManifest{Schema: 1, GOMAXPROCS: runtime.GOMAXPROCS(0)}
	sequential := manifestRow("BenchmarkRingAPI/api=seq/elem=int64", "ring", spscMode, 1)
	sequential.Family = "RingAPISequential"
	sequential.API = manifestPointer("seq")
	sequential.APIReason = ""
	sequential.Producers, sequential.Consumers = nil, nil
	sequential.WorkerReason = "one sequential caller has no concurrent worker pair"
	sequential.Callers = 1
	sequential.ProducerWait, sequential.ConsumerWait = nil, nil
	sequential.WaitReason = "sequential enqueue/dequeue pairs do not wait"
	sequential.TimingBoundary = "enqueue/dequeue pairs in one goroutine; excludes setup and GC"
	manifest.Rows = append(manifest.Rows, sequential)
	for _, mode := range benchModes {
		if mode != spscMode && mode != mpmcMode {
			continue
		}
		for _, batch := range benchBatches {
			if batch != 1 && batch != 32 {
				continue
			}
			name := fmt.Sprintf("BenchmarkThroughput/mode=%s/batch=%d/elem=int64/impl=ring",
				mode.name, batch)
			row := manifestRow(name, "ring", mode, batch)
			row.API = manifestPointer("burst")
			row.APIReason = ""
			manifest.Rows = append(manifest.Rows, row)
		}
	}
	handoff := manifestRow("BenchmarkHandoff/impl=ring", "ring", spscMode, 1)
	handoff.Family = "Handoff"
	handoff.API = manifestPointer("burst")
	handoff.APIReason = ""
	handoff.Participants, handoff.QueueCount = 2, 2
	handoff.ProducerWait = manifestPointer("runtime.Gosched after each unsuccessful enqueue attempt")
	handoff.ConsumerWait = manifestPointer("64 unsuccessful dequeue attempts before runtime.Gosched")
	handoff.TimingBoundary = "round trips between two participants over two SPSC queues; " +
		"excludes setup, pre-run GC and warm-up; half-round-trip percentiles are estimates"
	manifest.Rows = append(manifest.Rows, handoff)
	for _, mode := range ringAPIModes {
		if mode != spscMode && mode != mpmcMode {
			continue
		}
		for _, api := range ringAPIs {
			for _, batch := range api.batches() {
				name := fmt.Sprintf("BenchmarkRingAPI/mode=%s/api=%s", mode.name, api)
				if api != apiSingle {
					name += fmt.Sprintf("/batch=%d", batch)
				}
				name += "/elem=int64"
				row := manifestRow(name, "ring", mode, batch)
				row.Family, row.PrimaryMetric = "RingAPI", "ns/elem"
				row.API, row.APIReason = manifestPointer(api.String()), ""
				manifest.Rows = append(manifest.Rows, row)
			}
		}
	}
	return manifest
}

func checkManifestExamples(t *testing.T, rows []benchmarkManifestRow) {
	t.Helper()
	if len(rows) != 24 {
		t.Fatalf("got %d rows, want 24", len(rows))
	}
	var bulk32, single bool
	for _, row := range rows {
		if strings.Contains(row.Name, "api=bulk/batch=32") {
			bulk32 = row.Batch == 32
		}
		if strings.Contains(row.Name, "api=single") {
			single = row.Batch == 1
		}
	}
	if !bulk32 || !single {
		t.Fatal("missing bulk32 or single contract")
	}
}

func spscElementContract(row benchmarkManifestRow, element string) benchmarkManifestRow {
	row.Element = element
	if element == "ptr" {
		row.PointerPoolSize = manifestPointer(ptrPoolSize)
		row.PointerPoolReason = ""
		row.Storage = "typed *item pointing into a rotating pool of 1024 items"
	}
	return row
}

func buildSPSCConcurrentManifest() benchmarkManifest {
	manifest := benchmarkManifest{Schema: 1, GOMAXPROCS: runtime.GOMAXPROCS(0)}
	for _, implementation := range []string{"ring", "spsc"} {
		root := "BenchmarkRingAPI"
		if implementation == "spsc" {
			root = "BenchmarkSPSCAPI"
		}
		for _, api := range []ringAPI{apiSingle, apiBulk, apiBurst} {
			for _, batch := range []int{1, 32} {
				if api == apiSingle && batch != 1 {
					continue
				}
				for _, element := range []string{"int64", "ptr"} {
					name := root + "/mode=SPSC/api=" + api.String()
					if api != apiSingle {
						name += fmt.Sprintf("/batch=%d", batch)
					}
					row := manifestRow(name+"/elem="+element, implementation, spscMode, batch)
					row.Family, row.PrimaryMetric = "SPSCSharedAPI", "ns/elem"
					row.API, row.APIReason = manifestPointer(api.String()), ""
					row.Participants = 2
					manifest.Rows = append(manifest.Rows, spscElementContract(row, element))
				}
			}
		}
		row := manifestRow(
			"BenchmarkSPSCComparisonHandoff/impl="+implementation,
			implementation,
			spscMode,
			1,
		)
		row.Family, row.API, row.APIReason = "SPSCSharedHandoff", manifestPointer("burst"), ""
		row.Participants, row.QueueCount = 2, 2
		row.ConsumerWait = manifestPointer("64 unsuccessful dequeue attempts before runtime.Gosched")
		row.TimingBoundary = "round trips between two participants over two SPSC queues; " +
			"excludes setup, pre-run GC and warm-up; half-round-trip percentiles are estimates"
		manifest.Rows = append(manifest.Rows, row)
	}
	return manifest
}

func buildSPSCSequentialManifest() benchmarkManifest {
	manifest := benchmarkManifest{Schema: 1, GOMAXPROCS: runtime.GOMAXPROCS(0)}
	for _, implementation := range []string{"ring", "spsc"} {
		for _, element := range []string{"int64", "ptr"} {
			name := "BenchmarkSPSCConcrete/impl=" + implementation + "/api=seq/elem=" + element
			row := manifestRow(name, implementation, spscMode, 1)
			row.Family, row.API, row.APIReason = "SPSCConcreteSequential", manifestPointer("seq"), ""
			row.Producers, row.Consumers = nil, nil
			row.WorkerReason = "one sequential caller has no concurrent worker pair"
			row.Callers, row.Participants = 1, 1
			row.ProducerWait, row.ConsumerWait = nil, nil
			row.WaitReason = "sequential enqueue/dequeue pairs do not wait"
			row.TimingBoundary = "enqueue/dequeue pairs in one goroutine; excludes setup and GC"
			manifest.Rows = append(manifest.Rows, spscElementContract(row, element))
		}
	}
	return manifest
}
