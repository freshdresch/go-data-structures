package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

type workloadManifest struct {
	Schema     int                          `json:"schema"`
	GOMAXPROCS int                          `json:"gomaxprocs"`
	Rows       []map[string]json.RawMessage `json:"rows"`
}

func validateManifest(manifest workloadManifest) error {
	if manifest.Schema != schemaVersion || manifest.GOMAXPROCS <= 0 || len(manifest.Rows) == 0 {
		return inadmissibleError{"unsupported or incomplete workload manifest"}
	}
	required := []string{
		"name", "family", "implementation", "api", "api_reason", "element", "batch",
		"usable_capacity", "producers", "consumers", "worker_reason", "callers", "participants",
		"queue_count", "ring_options", "ring_options_reason", "producer_wait", "consumer_wait",
		"wait_reason", "primary_metric", "timing_boundary", "pointer_pool_size",
		"pointer_pool_reason", "storage",
	}
	seen := map[string]bool{}
	for _, row := range manifest.Rows {
		for _, key := range required {
			if _, exists := row[key]; !exists {
				return inadmissibleError{"missing row field " + key}
			}
		}
		var name string
		if err := json.Unmarshal(row["name"], &name); err != nil {
			return err
		}
		if name == "" || seen[name] {
			return inadmissibleError{"empty or duplicate workload name " + name}
		}
		seen[name] = true
		for _, key := range []string{"family", "implementation", "element", "primary_metric",
			"timing_boundary", "storage"} {
			var value string
			if err := json.Unmarshal(row[key], &value); err != nil {
				return err
			}
			if value == "" {
				return inadmissibleError{"empty row field " + key}
			}
		}
		for _, key := range []string{"batch", "usable_capacity", "queue_count"} {
			var value int
			if err := json.Unmarshal(row[key], &value); err != nil {
				return err
			}
			if value <= 0 {
				return inadmissibleError{"invalid row field " + key}
			}
		}
		for _, pair := range [][2]string{
			{"api", "api_reason"}, {"producers", "worker_reason"}, {"consumers", "worker_reason"},
			{"ring_options", "ring_options_reason"}, {"producer_wait", "wait_reason"},
			{"consumer_wait", "wait_reason"}, {"pointer_pool_size", "pointer_pool_reason"},
		} {
			if string(row[pair[0]]) != "null" {
				continue
			}
			var reason string
			if err := json.Unmarshal(row[pair[1]], &reason); err != nil {
				return err
			}
			if reason == "" {
				return inadmissibleError{"missing null reason for " + pair[0]}
			}
		}
	}
	return nil
}

func validateManifestLog(logBytes []byte, manifest workloadManifest) error {
	var payloads []string
	passed := false
	for _, line := range strings.Split(string(logBytes), "\n") {
		if index := strings.Index(line, "BENCHMARK_MANIFEST "); index >= 0 {
			payloads = append(payloads, line[index+len("BENCHMARK_MANIFEST "):])
		}
		if strings.HasPrefix(line, "--- PASS: TestBenchmarkManifest (") {
			passed = true
		}
	}
	if len(payloads) != 1 || !passed {
		return inadmissibleError{"manifest requires one payload and explicit test PASS"}
	}
	var exported workloadManifest
	if err := json.Unmarshal([]byte(payloads[0]), &exported); err != nil {
		return err
	}
	if digestJSON(exported) != digestJSON(manifest) {
		return inadmissibleError{"manifest does not match export log"}
	}
	return nil
}

func checkRows(manifestPath, expectedPath string, cpu int) error {
	var manifest workloadManifest
	if err := readJSON(manifestPath, &manifest); err != nil {
		return err
	}
	if err := validateManifest(manifest); err != nil {
		return err
	}
	if manifest.GOMAXPROCS != cpu {
		return inadmissibleError{"manifest GOMAXPROCS mismatch"}
	}
	expectedBytes, err := os.ReadFile(expectedPath)
	if err != nil {
		return err
	}
	expected := strings.Fields(string(expectedBytes))
	var actual []string
	for _, row := range manifest.Rows {
		var name string
		if err := json.Unmarshal(row["name"], &name); err != nil {
			return err
		}
		// Quick manifests contain CPU-1 sequential and selected-CPU concurrent contracts.
		if len(expected) == 1 && !strings.Contains(name, "/api=seq/") {
			continue
		}
		if len(expected) > 1 && strings.Contains(name, "/api=seq/") {
			continue
		}
		if cpu != 1 {
			name += "-" + strconv.Itoa(cpu)
		}
		actual = append(actual, name)
	}
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		return inadmissibleError{fmt.Sprintf("manifest rows differ: got %v, want %v", actual, expected)}
	}
	return nil
}

var benchmarkLine = regexp.MustCompile(`^Benchmark\S+\s+[0-9]+\s+`)

func projectResults(input, output string) error {
	file, err := os.Open(input)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var projected strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if !benchmarkLine.MatchString(line) {
			projected.WriteString(line + "\n")
			continue
		}
		fields := strings.Fields(line)
		if !strings.HasPrefix(fields[0], "BenchmarkRingAPI/mode=") {
			projected.WriteString(line + "\n")
			continue
		}
		if (len(fields)-2)%2 != 0 {
			return inadmissibleError{"malformed benchmark metric row"}
		}
		metrics := append([]string{}, fields[:2]...)
		canonical := 0
		for index := 2; index < len(fields); index += 2 {
			value, unit := fields[index], fields[index+1]
			if unit == "ns/elem" {
				canonical++
				number, err := strconv.ParseFloat(value, 64)
				if err != nil || math.IsNaN(number) || math.IsInf(number, 0) || number < 0 {
					return inadmissibleError{"invalid canonical ns/elem metric"}
				}
			}
			if unit != "ns/op" {
				metrics = append(metrics, value, unit)
			}
		}
		if canonical != 1 {
			return inadmissibleError{"expected exactly one canonical ns/elem metric"}
		}
		projected.WriteString(strings.Join(metrics, "\t") + "\n")
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return os.WriteFile(output, []byte(projected.String()), 0600)
}

func runResultCommand(arguments []string) error {
	flags := flag.NewFlagSet(arguments[0], flag.ContinueOnError)
	switch arguments[0] {
	case "rows":
		manifest := flags.String("manifest", "", "workload manifest")
		expected := flags.String("expected", "", "expected row names")
		cpu := flags.Int("cpu", 0, "requested GOMAXPROCS")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		return checkRows(*manifest, *expected, *cpu)
	case "project":
		input := flags.String("input", "", "raw benchmark result")
		output := flags.String("output", "", "primary metrics")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		return projectResults(*input, *output)
	case "admit":
		before := flags.String("before", "", "before provenance")
		after := flags.String("after", "", "after provenance")
		runner := flags.String("current-runner", "", "current runner hash JSON")
		platform := flags.String("platform", "", "platform JSON")
		output := flags.String("output", "", "decision JSON")
		methodology := flags.Bool("methodology-comparison", false, "allow methodology differences")
		layout := flags.Bool("allow-layout-confounded", false, "allow Apple layout diagnostic")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		return admitComparison(*before, *after, *runner, *platform, *output, *methodology, *layout)
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}
