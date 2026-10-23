package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func writeFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func fixtureRequest(t *testing.T) (snapshotRequest, string) {
	t.Helper()
	root := t.TempDir()
	runner := filepath.Join(root, "runner.sh")
	writeFixture(t, runner, "runner bytes")
	writeFixture(t, filepath.Join(root, "go.mod"), "module fixture\n")
	writeFixture(t, filepath.Join(root, "ring", "ring.go"), "production")
	writeFixture(t, filepath.Join(root, "ring", "ring_test.go"), "measurement")
	request := snapshotRequest{
		Schema: 1, RunnerKind: "quick", Origin: "fixture", SourceRoot: root,
		PackageDir: root, PackageTarget: "./ring", Variant: "before", RunnerBundle: []string{runner},
		Settings: buildSettings{Environment: map[string]string{
			"GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly",
		}}, BuildCommand: []string{"go", "test", "-c", "./ring"},
	}
	output := filepath.Join(root, "provenance", "before.snapshot.json")
	if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		t.Fatal(err)
	}
	return request, output
}

func fixturePackages(request snapshotRequest) []listedPackage {
	packages := []listedPackage{{
		Dir: filepath.Join(request.SourceRoot, "ring"), ImportPath: ringPackage,
		GoFiles: []string{"ring.go"}, TestGoFiles: []string{"ring_test.go"}}}
	if request.Settings.Overlay != "" {
		packages[0].GoFiles = append(packages[0].GoFiles, "padding.go")
	}
	return packages
}

func fixtureSnapshot(t *testing.T, request snapshotRequest, output string) sourceSnapshot {
	t.Helper()
	records, generated, err := recordsFromPackages(request, fixturePackages(request),
		"/fixture/goroot", filepath.Join(filepath.Dir(output), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sourceSnapshot{Schema: 1, Phase: "snapshot", Request: request,
		EffectiveSettings: request.Settings, GoVersion: "go fixture",
		Toolchain: map[string]string{
			"compile": digest([]byte("compiler")), "link": digest([]byte("linker")),
		},
		ModuleGraph: json.RawMessage(`[{"Path":"fixture"}]`), Records: records,
		GeneratedInputs: generated}
	snapshot.EffectiveSettings.Environment = map[string]string{
		"GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly",
		"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0", "GOROOT": "/fixture/goroot",
	}
	snapshot.Hashes = computeHashes(snapshot)
	return snapshot
}

func TestOverlayOnlySourceDiscovery(t *testing.T) {
	request, output := fixtureRequest(t)
	request.PackageDir = filepath.Join(request.SourceRoot, "benchmark")
	request.PackageTarget = "."
	request.Settings.Environment["CGO_ENABLED"] = "0"
	writeFixture(t, filepath.Join(request.PackageDir, "go.mod"), "module fixture/benchmark\n")
	writeFixture(t, filepath.Join(request.PackageDir, "benchmark.go"), "package benchmark\n")
	original := filepath.Join(request.PackageDir, "layout_pad_test.go")
	replacement := filepath.Join(t.TempDir(), "padding.go")
	padding := "package benchmark\nvar layoutPad [4]uint64\n"
	writeFixture(t, replacement, padding)
	request.Settings.Overlay = filepath.Join(request.SourceRoot, "overlay.json")
	writeMapping := func(target string) {
		t.Helper()
		if err := writeJSON(request.Settings.Overlay, map[string]any{
			"Replace": map[string]string{original: target},
		}); err != nil {
			t.Fatal(err)
		}
	}
	writeMapping(replacement)
	capture := func(name string) sourceSnapshot {
		t.Helper()
		if _, err := os.Stat(original); !os.IsNotExist(err) {
			t.Fatalf("overlay original must remain absent: %v", err)
		}
		discovered, _, err := goCommand(request,
			"list", "-deps", "-test", "-json", "-overlay="+request.Settings.Overlay, ".")
		if err != nil {
			t.Fatal(err)
		}
		packages, err := decodePackages(discovered)
		if err != nil {
			t.Fatal(err)
		}
		selected := false
		for _, entry := range packages {
			if entry.Dir == request.PackageDir &&
				(slices.Contains(entry.TestGoFiles, filepath.Base(original)) ||
					slices.Contains(entry.GoFiles, original)) {
				selected = true
			}
		}
		if !selected {
			t.Fatal("real Go discovery omitted overlay-only padding original")
		}
		snapshot, err := captureSnapshot(request, output+"."+name)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	before := capture("before")
	if err := writeJSON(output, before); err != nil {
		t.Fatal(err)
	}
	originalSnapshot, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, record := range before.Records {
		if record.Path == "repo/benchmark/layout_pad_test.go" {
			t.Fatal("padding duplicated as an ordinary source record")
		}
		if record.Path != "overlay/benchmark/layout_pad_test.go" {
			continue
		}
		count++
		saved, err := os.ReadFile(filepath.Join(filepath.Dir(output), record.Snapshot))
		if err != nil {
			t.Fatal(err)
		}
		if record.Class != "layout" || string(saved) != padding {
			t.Fatalf("padding capture: class=%s bytes=%q", record.Class, saved)
		}
	}
	if count != 1 {
		t.Fatalf("padding records=%d, want 1", count)
	}
	unchanged := capture("unchanged")
	if before.Hashes != unchanged.Hashes || !reflect.DeepEqual(before.Records, unchanged.Records) {
		t.Fatal("unchanged overlay capture changed identity")
	}
	for _, mutation := range []string{"replacement-bytes", "mapping"} {
		t.Run(mutation, func(t *testing.T) {
			changedPath := "overlay/benchmark/layout_pad_test.go"
			if mutation == "replacement-bytes" {
				writeFixture(t, replacement, padding+"var extraPad uint64\n")
				t.Cleanup(func() { writeFixture(t, replacement, padding) })
			} else {
				other := filepath.Join(t.TempDir(), "padding.go")
				writeFixture(t, other, padding)
				writeMapping(other)
				t.Cleanup(func() { writeMapping(replacement) })
				changedPath = "overlay/original-mapping"
			}
			after := capture(mutation)
			report := compareSnapshots(before, after)
			found := false
			for _, change := range report.Changed {
				if change.Path == changedPath {
					found = true
				}
			}
			if !found {
				t.Fatalf("mutation missing from comparison: %+v", report.Changed)
			}
			if before.Hashes.Measurement != after.Hashes.Measurement {
				t.Fatal("padding mutation changed measurement identity")
			}
			if mutation == "replacement-bytes" && before.Hashes.Layout == after.Hashes.Layout {
				t.Fatal("replacement-byte mutation missing from layout identity")
			}
			saved, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if string(saved) != string(originalSnapshot) {
				t.Fatal("original snapshot bytes changed")
			}
			for _, record := range before.Records {
				checksum, err := fileDigest(filepath.Join(filepath.Dir(output), record.Snapshot))
				if err != nil || checksum != record.SHA256 {
					t.Fatalf("original source bytes changed for %s: %v", record.Path, err)
				}
			}
		})
	}
}

func TestSourceIdentities(t *testing.T) {
	request, output := fixtureRequest(t)
	before := fixtureSnapshot(t, request, output)
	writeFixture(t, filepath.Join(request.SourceRoot, "ring", "ring.go"), "new production")
	after := fixtureSnapshot(t, request, output)
	if before.Hashes.Implementation == after.Hashes.Implementation ||
		before.Hashes.Layout == after.Hashes.Layout ||
		before.Hashes.Measurement != after.Hashes.Measurement {
		t.Fatal("production change affected wrong hashes")
	}
	writeFixture(t, filepath.Join(request.SourceRoot, "ring", "ring_test.go"), "dirty test")
	tests := fixtureSnapshot(t, request, output)
	if tests.Hashes.Measurement == after.Hashes.Measurement ||
		tests.Hashes.Layout == after.Hashes.Layout {
		t.Fatal("compiled test change missing from identities")
	}
	writeFixture(t, request.RunnerBundle[0], "dirty runner")
	runner := fixtureSnapshot(t, request, output)
	if runner.Hashes.Runner == tests.Hashes.Runner ||
		runner.Hashes.Measurement == tests.Hashes.Measurement {
		t.Fatal("runner change missing from identities")
	}
	for _, record := range before.Records {
		checksum, err := fileDigest(filepath.Join(filepath.Dir(output), record.Snapshot))
		if err != nil || checksum != record.SHA256 {
			t.Fatalf("original bytes unavailable: %v", err)
		}
	}
}

func TestSourceMembership(t *testing.T) {
	request, output := fixtureRequest(t)
	before := fixtureSnapshot(t, request, output)
	packages := fixturePackages(request)
	packages = append(packages, packages[0])
	records, _, err := recordsFromPackages(request, packages, "/fixture/goroot",
		filepath.Join(filepath.Dir(output), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, before.Records) {
		t.Fatal("test variant was not deduplicated")
	}
	writeFixture(t, filepath.Join(request.SourceRoot, "ring", "untracked_test.go"), "untracked")
	packages[0].TestGoFiles = append(packages[0].TestGoFiles, "untracked_test.go")
	records, _, err = recordsFromPackages(request, packages[:1], "/fixture/goroot",
		filepath.Join(filepath.Dir(output), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	after := before
	after.Records = records
	report := compareSnapshots(before, after)
	if len(report.Added) != 1 || report.Added[0].Path != "repo/ring/untracked_test.go" {
		t.Fatalf("missing addition evidence: %+v", report.Added)
	}
	reverse := compareSnapshots(after, before)
	if len(reverse.Removed) != 1 {
		t.Fatalf("missing removal evidence: %+v", reverse.Removed)
	}
	packages[0].GoFiles = []string{"missing.go"}
	if _, _, err := recordsFromPackages(request, packages[:1], "/fixture/goroot",
		filepath.Join(filepath.Dir(output), "sources")); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestSourceRootsAndIgnoredFiles(t *testing.T) {
	firstRequest, firstOutput := fixtureRequest(t)
	secondRequest, secondOutput := fixtureRequest(t)
	first, second := fixtureSnapshot(t, firstRequest, firstOutput),
		fixtureSnapshot(t, secondRequest, secondOutput)
	if first.Hashes != second.Hashes {
		t.Fatal("checkout roots changed content identity")
	}
	packages := fixturePackages(firstRequest)
	packages[0].IgnoredGoFiles = []string{"ignored.go"}
	writeFixture(t, filepath.Join(firstRequest.SourceRoot, "ring", "ignored.go"), "//go:build ignored")
	records, _, err := recordsFromPackages(firstRequest, packages, "/fixture/goroot",
		filepath.Join(filepath.Dir(firstOutput), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	first.Records = records
	if computeHashes(first) != second.Hashes {
		t.Fatal("ignored input affected selected identity")
	}
	if _, err := decodePackages([]byte("{bad")); err == nil {
		t.Fatal("malformed stream accepted")
	}
	if _, err := decodePackages(nil); err == nil {
		t.Fatal("empty stream accepted")
	}
}

func installGoFixture(t *testing.T, request snapshotRequest) string {
	t.Helper()
	tools := filepath.Join(request.SourceRoot, "toolchain")
	writeFixture(t, filepath.Join(tools, "compile"), "compiler")
	writeFixture(t, filepath.Join(tools, "link"), "linker")
	environment := map[string]string{
		"GOROOT": "/fixture/goroot", "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0",
		"GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly",
	}
	environmentBytes, err := json.Marshal(environment)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(request.SourceRoot, "environment.json"), string(environmentBytes))
	packages, err := json.Marshal(fixturePackages(request)[0])
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(request.SourceRoot, "packages.json"), string(packages))
	script := `#!/bin/sh
case "$1 $2" in
 "env -json") cat "$FIXTURE_ROOT/environment.json" ;;
 "env GOTOOLDIR") printf '%s\n' "$FIXTURE_ROOT/toolchain" ;;
 "version -m") cat "$FIXTURE_ROOT/build-info.txt" ;;
 "version ") echo 'go version fixture linux/amd64' ;;
 "list -m") echo '{"Path":"fixture"}' ;;
 "list -deps") cat "$FIXTURE_ROOT/packages.json" ;;
 *) exit 9 ;;
esac
`
	path := filepath.Join(tools, "go")
	writeFixture(t, path, script)
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FIXTURE_ROOT", request.SourceRoot)
	t.Setenv("PATH", tools+string(os.PathListSeparator)+os.Getenv("PATH"))
	return filepath.Join(request.SourceRoot, "packages.json")
}

func TestFinalizeRejectsSourceChanges(t *testing.T) {
	for _, variant := range []string{"ordinary", "padded", "race"} {
		mutations := []string{"modify", "add", "remove", "overlay-source", "overlay-mapping"}
		for _, mutation := range mutations {
			if variant != "padded" && strings.HasPrefix(mutation, "overlay") {
				continue
			}
			t.Run(variant+"/"+mutation, func(t *testing.T) {
				request, output := fixtureRequest(t)
				request.Settings.Race = variant == "race"
				packagesPath := installGoFixture(t, request)
				if variant == "padded" {
					source := filepath.Join(request.SourceRoot, "padding.go")
					writeFixture(t, source, "padding baseline")
					request.Settings.Overlay = filepath.Join(request.SourceRoot, "overlay.json")
					if err := writeJSON(request.Settings.Overlay, map[string]any{"Replace": map[string]string{
						filepath.Join(request.SourceRoot, "ring", "padding.go"): source,
					}}); err != nil {
						t.Fatal(err)
					}
				}
				if request.Settings.Overlay != "" {
					if err := writeJSON(packagesPath, fixturePackages(request)[0]); err != nil {
						t.Fatal(err)
					}
				}
				snapshot, err := captureSnapshot(request, output)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeJSON(output, snapshot); err != nil {
					t.Fatal(err)
				}
				original, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}
				// The build stub changes the selected membership or bytes between the two phases.
				switch mutation {
				case "modify":
					writeFixture(t, filepath.Join(request.SourceRoot, "ring", "ring.go"), "modified during build")
				case "add":
					writeFixture(t, filepath.Join(request.SourceRoot, "ring", "new.go"), "new during build")
					packages := fixturePackages(request)[0]
					packages.GoFiles = append(packages.GoFiles, "new.go")
					if err := writeJSON(packagesPath, packages); err != nil {
						t.Fatal(err)
					}
				case "remove":
					if err := os.Remove(filepath.Join(request.SourceRoot, "ring", "ring.go")); err != nil {
						t.Fatal(err)
					}
					packages := fixturePackages(request)[0]
					packages.GoFiles = nil
					if err := writeJSON(packagesPath, packages); err != nil {
						t.Fatal(err)
					}
				case "overlay-source":
					writeFixture(t, filepath.Join(request.SourceRoot, "padding.go"), "changed padding")
				case "overlay-mapping":
					writeFixture(t, request.Settings.Overlay, `{"Replace":{}}`)
				}
				final := filepath.Join(filepath.Dir(output), "before.json")
				reportPath := final + ".report"
				if err := finalizeSnapshot(output, "unused-artifacts", final, reportPath, false); err == nil {
					t.Fatal("mutated build admitted")
				}
				saved, err := os.ReadFile(output)
				if err != nil {
					t.Fatal(err)
				}
				if string(saved) != string(original) {
					t.Fatal("original snapshot changed")
				}
				if _, err := os.Stat(final); !os.IsNotExist(err) {
					t.Fatal("complete provenance created")
				}
				var report verificationReport
				if err := readJSON(reportPath, &report); err != nil {
					t.Fatal(err)
				}
				if len(report.Added)+len(report.Removed)+len(report.Changed)+len(report.Errors) == 0 {
					t.Fatal("missing mutation evidence")
				}
			})
		}
	}
}

func TestFinalizeUnchangedVariants(t *testing.T) {
	for _, variant := range []string{"ordinary", "padded", "race"} {
		t.Run(variant, func(t *testing.T) {
			request, output := fixtureRequest(t)
			request.Settings.Race = variant == "race"
			packagesPath := installGoFixture(t, request)
			if variant == "padded" {
				padding := filepath.Join(request.SourceRoot, "padding.go")
				writeFixture(t, padding, "generated padding")
				request.Settings.Overlay = filepath.Join(request.SourceRoot, "overlay.json")
				if err := writeJSON(request.Settings.Overlay, map[string]any{
					"Replace": map[string]string{
						filepath.Join(request.SourceRoot, "ring", "padding.go"): padding,
					},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if request.Settings.Overlay != "" {
				if err := writeJSON(packagesPath, fixturePackages(request)[0]); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := captureSnapshot(request, output)
			if err != nil {
				t.Fatal(err)
			}
			if err := writeJSON(output, snapshot); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Dir(output)
			manifest := tinyManifest(t)
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			writeFixture(t, filepath.Join(directory, "manifest.json"), string(encoded))
			writeFixture(t, filepath.Join(directory, "manifest.log"),
				"BENCHMARK_MANIFEST "+string(encoded)+"\n--- PASS: TestBenchmarkManifest (0.00s)\n")
			writeFixture(t, filepath.Join(directory, "binary"), "fixture binary")
			info := "binary: go fixture\n\tbuild\tGOOS=linux\n\tbuild\tGOARCH=amd64\n" +
				"\tbuild\tCGO_ENABLED=0\n"
			if request.Settings.Race {
				info += "\tbuild\t-race=true\n"
			}
			writeFixture(t, filepath.Join(request.SourceRoot, "build-info.txt"), info)
			writeFixture(t, filepath.Join(directory, "info"), info)
			artifacts := artifactRequest{
				Binary: "binary", BuildInfo: "info", Manifest: "manifest.json",
				ManifestLog: "manifest.log", SchedulerOffset: "unknown",
			}
			artifactsPath := filepath.Join(directory, "artifacts.json")
			if err := writeJSON(artifactsPath, artifacts); err != nil {
				t.Fatal(err)
			}
			final := filepath.Join(directory, "final.json")
			if err := finalizeSnapshot(output, artifactsPath, final, final+".report", false); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(final)
			if err != nil {
				t.Fatal(err)
			}
			if err := finalizeSnapshot(output, artifactsPath, final, final+".check", true); err != nil {
				t.Fatal(err)
			}
			saved, err := os.ReadFile(final)
			if err != nil {
				t.Fatal(err)
			}
			if string(saved) != string(original) {
				t.Fatal("check-only rewrote finalized origin")
			}
			if _, err := verifyProvenance(final); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSourceModulesAndDependencies(t *testing.T) {
	request, output := fixtureRequest(t)
	before := fixtureSnapshot(t, request, output)
	writeFixture(t, filepath.Join(request.SourceRoot, "go.mod"), "module fixture\ngo 1.25\n")
	after := fixtureSnapshot(t, request, output)
	if before.Hashes.Measurement == after.Hashes.Measurement ||
		before.Hashes.Layout == after.Hashes.Layout {
		t.Fatal("module change missing from identities")
	}
	dependency := t.TempDir()
	writeFixture(t, filepath.Join(dependency, "dependency.go"), "dependency bytes")
	entry := listedPackage{
		Dir: dependency, ImportPath: "example.com/dependency", GoFiles: []string{"dependency.go"},
		Module: &listedModule{Path: "example.com/dependency", Version: "v1.0.0", Dir: dependency},
	}
	packages := append(fixturePackages(request), entry)
	records, _, err := recordsFromPackages(request, packages, "/fixture/goroot",
		filepath.Join(filepath.Dir(output), "sources"))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, record := range records {
		if record.Path == "module/example.com/dependency@v1.0.0/dependency.go" {
			found = record.Class == "measurement"
		}
	}
	if !found {
		t.Fatal("external dependency source missing")
	}
	entry.Module.Replace = &listedModule{Path: dependency, Dir: dependency}
	if _, err := logicalSourcePath(request, entry,
		filepath.Join(dependency, "dependency.go"), "/fixture/goroot"); err == nil {
		t.Fatal("external local replacement accepted")
	}
	request.Settings.Environment["GOFLAGS"] = "-tags=unsupported"
	if err := validateRequest(request); err == nil {
		t.Fatal("unsupported build flags accepted")
	}
}
