package benchmark

import (
	"encoding/json"
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

func TestBenchmarkManifest(t *testing.T) {
	manifest := buildBenchmarkManifest()
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
	for _, implementation := range compareRows {
		for _, mode := range implementation.modes {
			for _, batch := range implementation.batches {
				for _, element := range []string{"int64", "ptr"} {
					name := fmt.Sprintf("BenchmarkCompare/mode=%s/batch=%d/elem=%s/impl=%s",
						mode.name, batch, element, implementation.name)
					row := manifestRow(name, implementation.name, mode, batch)
					row.Family, row.Element = "Compare", element
					row.Storage = "typed " + element
					if strings.HasPrefix(implementation.name, "ring") {
						api := "burst"
						if implementation.name == "ring-single" {
							api = "single"
						}
						row.API, row.APIReason = manifestPointer(api), ""
						optionsMode := mode
						if implementation.name == "ring-rts" {
							optionsMode = rtsMode(mode)
						}
						row.RingOptions = manifestRingOptions(optionsMode)
						if implementation.name == "ring-exact" {
							row.RingOptions = append(row.RingOptions, "WithExactBatchCounts")
						}
					} else {
						row.RingOptions = nil
						row.RingOptionsReason = "implementation is not a ring"
					}
					if implementation.name == "chan" || implementation.name == "workiva" {
						row.ProducerWait = manifestPointer("blocking put; implementation owns internal waiting")
						row.ConsumerWait = manifestPointer("blocking get; implementation owns internal waiting")
						row.TimingBoundary += "; includes blocking runner stop and wake-up"
					}
					if element == "ptr" {
						row.PointerPoolSize = manifestPointer(ptrPoolSize)
						row.PointerPoolReason = ""
					}
					if implementation.name == "workiva" {
						row.Storage = "interface storage with " + element + " conversion; int64 boxing is intrinsic"
					}
					manifest.Rows = append(manifest.Rows, row)
				}
			}
		}
	}
	return manifest
}

func checkManifestExamples(t *testing.T, rows []benchmarkManifestRow) {
	t.Helper()
	if len(rows) != 84 {
		t.Fatalf("got %d rows, want 84", len(rows))
	}
	var mpsc, spmc, exact, single bool
	for _, row := range rows {
		if strings.Contains(row.Name, "mode=MPSC") {
			mpsc = *row.Producers == 4 && *row.Consumers == 1
		}
		if strings.Contains(row.Name, "mode=SPMC") {
			spmc = *row.Producers == 1 && *row.Consumers == 4
		}
		if row.Implementation == "ring-exact" {
			for _, option := range row.RingOptions {
				if option == "WithExactBatchCounts" {
					exact = true
				}
			}
		}
		if row.Implementation == "ring-single" {
			single = row.Batch == 1
		}
		if row.Implementation == "ring-rts" && strings.Contains(row.Name, "mode=SPSC") {
			t.Fatal("SPSC ring-rts row")
		}
	}
	if !mpsc || !spmc || !exact || !single {
		t.Fatal("missing mode or option examples")
	}
}
