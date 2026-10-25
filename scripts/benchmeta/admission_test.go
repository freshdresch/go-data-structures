package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func finalizedFixture(t *testing.T) (finalizedProvenance, string) {
	t.Helper()
	request, snapshotPath := fixtureRequest(t)
	snapshot := fixtureSnapshot(t, request, snapshotPath)
	directory := filepath.Dir(snapshotPath)
	if err := writeJSON(snapshotPath, snapshot); err != nil {
		t.Fatal(err)
	}
	manifest := tinyManifest(t)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(directory, "manifest.json"), string(encoded))
	writeFixture(
		t,
		filepath.Join(directory, "manifest.log"),
		"BENCHMARK_MANIFEST "+string(encoded)+"\n--- PASS: TestBenchmarkManifest (0.00s)\n",
	)
	writeFixture(t, filepath.Join(directory, "binary"), "binary fixture")
	writeFixture(t, filepath.Join(directory, "info"), "binary: go fixture\nGOOS=linux\n")
	provenance := finalizedProvenance{Schema: 1, Phase: "finalized", Complete: true,
		Snapshot: snapshot, SnapshotFile: filepath.Base(snapshotPath),
		Artifacts: artifactRequest{Binary: "binary", BuildInfo: "info", Manifest: "manifest.json",
			ManifestLog: "manifest.log", SchedulerOffset: "0"}}
	for _, item := range []struct {
		path        string
		destination *string
	}{
		{snapshotPath, &provenance.SnapshotHash},
		{filepath.Join(directory, "binary"), &provenance.BinaryHash},
		{filepath.Join(directory, "info"), &provenance.BuildInfoHash},
		{filepath.Join(directory, "manifest.json"), &provenance.ManifestHash},
		{filepath.Join(directory, "manifest.log"), &provenance.ManifestLogHash},
	} {
		*item.destination, err = fileDigest(item.path)
		if err != nil {
			t.Fatal(err)
		}
	}
	provenance.MeasurementHash = digestJSON([]any{snapshot.Hashes.Measurement, manifest.Rows})
	output := filepath.Join(directory, "before.json")
	if err := writeJSON(output, provenance); err != nil {
		t.Fatal(err)
	}
	return provenance, output
}

func TestAdmissionPolicy(t *testing.T) {
	before, _ := finalizedFixture(t)
	for _, osName := range []string{"Linux", "Darwin"} {
		for _, architecture := range []string{"amd64", "arm64"} {
			for _, offset := range []string{"0", "96", "unknown"} {
				for _, methodology := range []bool{false, true} {
					for _, layout := range []bool{false, true} {
						after := before
						after.Artifacts.SchedulerOffset = offset
						decision := evaluateAdmission(
							before,
							after,
							before.Snapshot.Hashes.Runner,
							admissionPlatform{osName, architecture},
							methodology,
							layout,
						)
						want := osName != "Darwin" || architecture != "arm64" || offset == "0" || layout
						if decision.Accepted != want {
							t.Fatalf(
								"policy %s/%s %s %v %v: %+v",
								osName,
								architecture,
								offset,
								methodology,
								layout,
								decision,
							)
						}
					}
				}
			}
		}
	}
	after := before
	after.MeasurementHash = "changed"
	for _, layout := range []bool{false, true} {
		decision := evaluateAdmission(
			before,
			after,
			before.Snapshot.Hashes.Runner,
			admissionPlatform{"Linux", "amd64"},
			false,
			layout,
		)
		if decision.Accepted {
			t.Fatal("layout override bypassed methodology")
		}
	}
	if evaluateAdmission(
		before,
		before,
		"changed runner",
		admissionPlatform{"Linux", "amd64"},
		false,
		false,
	).Accepted {
		t.Fatal("current runner change admitted")
	}
	if !evaluateAdmission(
		before,
		after,
		"changed runner",
		admissionPlatform{"Linux", "amd64"},
		true,
		false,
	).Accepted {
		t.Fatal("explicit methodology comparison rejected")
	}
}

func TestVerifyProvenance(t *testing.T) {
	_, path := finalizedFixture(t)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyProvenance(path); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"binary", "info", "manifest.json", "manifest.log"} {
		t.Run(name, func(t *testing.T) {
			_, path := finalizedFixture(t)
			writeFixture(t, filepath.Join(filepath.Dir(path), name), "corrupt")
			if _, err := verifyProvenance(path); err == nil {
				t.Fatal("corruption admitted")
			}
		})
	}
	if _, err := verifyProvenance(filepath.Join(t.TempDir(), "legacy.json")); err == nil {
		t.Fatal("legacy provenance admitted")
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(saved) != string(original) {
		t.Fatal("verification changed immutable origin")
	}
	var provenance finalizedProvenance
	if err := json.Unmarshal(original, &provenance); err != nil {
		t.Fatal(err)
	}
	provenance.Schema = 2
	if err := writeJSON(path, provenance); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyProvenance(path); err == nil {
		t.Fatal("future schema admitted")
	}
}

func TestUnknownIdentityAndCorruptSource(t *testing.T) {
	provenance, _ := finalizedFixture(t)
	provenance.Snapshot.GoVersion = ""
	if err := validateSnapshotIdentity(provenance.Snapshot); err == nil {
		t.Fatal("unknown toolchain accepted")
	}
	provenance, path := finalizedFixture(t)
	record := provenance.Snapshot.Records[0]
	writeFixture(t, filepath.Join(filepath.Dir(path), record.Snapshot), "corrupted source")
	if _, err := verifyProvenance(path); err == nil {
		t.Fatal("corrupted source accepted")
	}
}

func TestVerifyProvenanceDigestErrors(t *testing.T) {
	for _, kind := range []string{"snapshot", "helper", "source", "artifact", "single export"} {
		for _, missing := range []bool{false, true} {
			name := kind + "/mismatch"
			if missing {
				name = kind + "/missing"
			}
			t.Run(name, func(t *testing.T) {
				provenance, path := finalizedFixture(t)
				directory := filepath.Dir(path)
				var target string
				switch kind {
				case "snapshot":
					target = provenance.SnapshotFile
				case "helper":
					target = "helper"
					writeFixture(t, filepath.Join(directory, target), "helper fixture")
					provenance.Snapshot.Request.HelperBinary = "fixture helper"
					provenance.Snapshot.HelperBinarySnapshot = target
					checksum, err := fileDigest(filepath.Join(directory, target))
					if err != nil {
						t.Fatal(err)
					}
					provenance.Snapshot.HelperBinaryHash = checksum
					provenance.Snapshot.Hashes = computeHashes(provenance.Snapshot)
					if err := writeJSON(
						filepath.Join(directory, provenance.SnapshotFile),
						provenance.Snapshot,
					); err != nil {
						t.Fatal(err)
					}
					provenance.SnapshotHash, err = fileDigest(
						filepath.Join(directory, provenance.SnapshotFile),
					)
					if err != nil {
						t.Fatal(err)
					}
				case "source":
					target = provenance.Snapshot.Records[0].Snapshot
				case "artifact":
					target = provenance.Artifacts.Binary
				case "single export":
					target = "single-manifest"
					writeFixture(t, filepath.Join(directory, target), "single fixture")
					provenance.Artifacts.SingleManifest = target
					checksum, err := fileDigest(filepath.Join(directory, target))
					if err != nil {
						t.Fatal(err)
					}
					provenance.SingleManifestHash = checksum
				}
				if err := writeJSON(path, provenance); err != nil {
					t.Fatal(err)
				}
				target = filepath.Join(directory, target)
				if missing {
					if err := os.Remove(target); err != nil {
						t.Fatal(err)
					}
				} else {
					writeFixture(t, target, "checksum mismatch")
				}
				_, err := verifyProvenance(path)
				if err == nil {
					t.Fatal("invalid digest accepted")
				}
				if missing && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing file cause lost: %v", err)
				}
				if !missing && strings.Contains(err.Error(), "<nil>") {
					t.Fatalf("checksum mismatch included a nil cause: %v", err)
				}
			})
		}
	}
}
