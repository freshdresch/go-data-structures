package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tinyManifest(t *testing.T) workloadManifest {
	t.Helper()
	row := map[string]any{
		"name":   "BenchmarkRingAPI/mode=SPSC/api=bulk/batch=32/elem=int64",
		"family": "RingAPI", "implementation": "ring", "api": "bulk", "api_reason": "",
		"element": "int64", "batch": 32, "usable_capacity": 1024, "producers": 1,
		"consumers": 1, "worker_reason": "", "callers": 0, "participants": 0, "queue_count": 1,
		"ring_options": []string{}, "ring_options_reason": "", "producer_wait": "yield",
		"consumer_wait": "yield", "wait_reason": "", "primary_metric": "ns/elem",
		"timing_boundary": "fixture", "pointer_pool_size": nil, "pointer_pool_reason": "int64",
		"storage": "int64",
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	return workloadManifest{Schema: 1, GOMAXPROCS: 2, Rows: []map[string]json.RawMessage{fields}}
}

func TestRowsAndManifestLog(t *testing.T) {
	directory := t.TempDir()
	manifest := tinyManifest(t)
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := json.Unmarshal(manifest.Rows[0]["name"], &name); err != nil {
		t.Fatal(err)
	}
	// More than one row distinguishes concurrent selections from quick's sequential-only selection.
	second := map[string]json.RawMessage{}
	for key, value := range manifest.Rows[0] {
		second[key] = value
	}
	second["name"] = json.RawMessage(`"BenchmarkThroughput/mode=SPSC/batch=1/elem=int64/impl=ring"`)
	manifest.Rows = append(manifest.Rows, second)
	if err := writeJSON(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	expected := filepath.Join(directory, "expected")
	writeFixture(t, expected, name+"-2\n"+
		"BenchmarkThroughput/mode=SPSC/batch=1/elem=int64/impl=ring-2\n")
	if err := checkRows(manifestPath, expected, 2); err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{name + "-2", name + "-2\n" + name + "-2", name + "-2\nextra"} {
		writeFixture(t, expected, content)
		if err := checkRows(manifestPath, expected, 2); err == nil {
			t.Fatal("wrong rows accepted")
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	log := "    file.go:1: BENCHMARK_MANIFEST " + string(encoded) +
		"\n--- PASS: TestBenchmarkManifest (0.00s)\nPASS\n"
	if err := validateManifestLog([]byte(log), manifest); err != nil {
		t.Fatal(err)
	}
	badLogs := []string{"PASS\n", strings.ReplaceAll(log, "--- PASS:", "--- FAIL:"), log + log}
	for _, bad := range badLogs {
		if err := validateManifestLog([]byte(bad), manifest); err == nil {
			t.Fatal("invalid export accepted")
		}
	}
}

func TestProjectCanonicalMetrics(t *testing.T) {
	directory := t.TempDir()
	input, output := filepath.Join(directory, "raw"), filepath.Join(directory, "primary")
	rows := []string{
		"BenchmarkRingAPI/mode=SPSC/api=bulk/batch=32/elem=int64-2 " +
			"129 20 ns/op 10 ns/elem 2 elem/op 0 B/op",
		"BenchmarkRingAPI/mode=SPSC/api=single/elem=int64-2 128 20 ns/op 20 ns/elem",
		"BenchmarkRingAPI/api=seq/elem=int64 128 4 ns/op",
		"BenchmarkThroughput/mode=SPSC/batch=1/elem=int64/impl=ring-2 128 5 ns/op",
		"BenchmarkHandoff/impl=ring-2 128 8 ns/op 4 handoff-p50",
	}
	writeFixture(t, input, strings.Join(rows, "\n")+"\n")
	if err := projectResults(input, output); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	if strings.Contains(lines[0], "ns/op") || !strings.Contains(lines[0], "10\tns/elem") ||
		!strings.Contains(lines[0], "2\telem/op") {
		t.Fatalf("bad bulk projection %s", lines[0])
	}
	for index := 2; index < len(rows); index++ {
		if lines[index] != rows[index] {
			t.Fatalf("nonconcurrent row changed %s", lines[index])
		}
	}
	badMetrics := []string{"", "1 ns/elem 2 ns/elem", "NaN ns/elem", "-1 ns/elem", "Inf ns/elem"}
	for _, metric := range badMetrics {
		writeFixture(t, input, "BenchmarkRingAPI/mode=SPSC/api=single/elem=int64 1 2 ns/op "+metric+"\n")
		if err := projectResults(input, output); err == nil {
			t.Fatalf("accepted %q", metric)
		}
	}
}
