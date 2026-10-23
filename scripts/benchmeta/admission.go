package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
)

type admissionPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

type admissionDecision struct {
	Accepted bool               `json:"accepted"`
	Labels   []string           `json:"labels"`
	Reasons  []string           `json:"reasons"`
	Warnings []string           `json:"warnings"`
	Changes  []classifiedChange `json:"changes"`
}

type classifiedChange struct {
	Path   string `json:"path"`
	Class  string `json:"class"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

func artifactPath(directory, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(directory, path)
}

func verifyProvenance(path string) (finalizedProvenance, error) {
	var provenance finalizedProvenance
	if err := readJSON(path, &provenance); err != nil {
		return provenance, fmt.Errorf(
			"historical artifacts need complete provenance and a fresh build: %w",
			err,
		)
	}
	if provenance.Schema != schemaVersion || provenance.Phase != "finalized" || !provenance.Complete {
		return provenance, fmt.Errorf("unsupported or incomplete provenance")
	}
	snapshot := provenance.Snapshot
	if err := validateSnapshotIdentity(snapshot); err != nil {
		return provenance, err
	}
	if !reflect.DeepEqual(snapshot.Hashes, computeHashes(snapshot)) {
		return provenance, fmt.Errorf("source identity hashes do not match")
	}
	directory := filepath.Dir(path)
	snapshotPath := filepath.Join(directory, provenance.SnapshotFile)
	snapshotHash, err := fileDigest(snapshotPath)
	if err != nil || snapshotHash != provenance.SnapshotHash {
		return provenance, fmt.Errorf("saved snapshot hash mismatch: %v", err)
	}
	var original sourceSnapshot
	if err := readJSON(snapshotPath, &original); err != nil {
		return provenance, err
	}
	if digestJSON(snapshot) != digestJSON(original) {
		return provenance, fmt.Errorf("saved snapshot identity mismatch")
	}
	if snapshot.Request.HelperBinary != "" {
		checksum, err := fileDigest(filepath.Join(directory, snapshot.HelperBinarySnapshot))
		if err != nil || checksum != snapshot.HelperBinaryHash {
			return provenance, fmt.Errorf("metadata helper binary snapshot corrupted")
		}
	}
	for _, record := range snapshot.Records {
		if record.Path == "" || record.Class == "" || record.SHA256 == "" || record.Snapshot == "" {
			return provenance, fmt.Errorf("incomplete source record")
		}
		checksum, err := fileDigest(filepath.Join(directory, record.Snapshot))
		if err != nil || checksum != record.SHA256 {
			return provenance, fmt.Errorf("source snapshot corrupted: %s: %v", record.Path, err)
		}
	}
	artifacts := provenance.Artifacts
	for _, file := range []struct{ path, expected string }{
		{artifacts.Binary, provenance.BinaryHash}, {artifacts.BuildInfo, provenance.BuildInfoHash},
		{artifacts.Manifest, provenance.ManifestHash},
		{artifacts.ManifestLog, provenance.ManifestLogHash},
	} {
		checksum, err := fileDigest(artifactPath(directory, file.path))
		if err != nil || checksum != file.expected {
			return provenance, fmt.Errorf("artifact hash mismatch %s: %v", file.path, err)
		}
	}
	if artifacts.SingleManifest != "" || artifacts.SingleManifestLog != "" {
		for _, file := range []struct{ path, expected string }{
			{artifacts.SingleManifest, provenance.SingleManifestHash},
			{artifacts.SingleManifestLog, provenance.SingleManifestLogHash},
		} {
			checksum, err := fileDigest(artifactPath(directory, file.path))
			if err != nil || checksum != file.expected {
				return provenance, fmt.Errorf("single export hash mismatch")
			}
		}
	}
	var manifest workloadManifest
	if err := readJSON(artifactPath(directory, artifacts.Manifest), &manifest); err != nil {
		return provenance, err
	}
	if err := validateManifest(manifest); err != nil {
		return provenance, err
	}
	if provenance.MeasurementHash != digestJSON([]any{snapshot.Hashes.Measurement, manifest.Rows}) {
		return provenance, fmt.Errorf("measurement hash mismatch")
	}
	return provenance, nil
}

func sourceChanges(before, after sourceSnapshot) []classifiedChange {
	previous := map[string]sourceRecord{}
	for _, record := range before.Records {
		previous[record.Path] = record
	}
	var changes []classifiedChange
	for _, record := range after.Records {
		old, exists := previous[record.Path]
		if !exists || old.SHA256 != record.SHA256 || old.Class != record.Class {
			changes = append(changes, classifiedChange{record.Path, record.Class, old.SHA256, record.SHA256})
		}
		delete(previous, record.Path)
	}
	for _, record := range previous {
		changes = append(changes, classifiedChange{
			Path: record.Path, Class: record.Class, Before: record.SHA256,
		})
	}
	return changes
}

func evaluateAdmission(
	before, after finalizedProvenance,
	currentRunner string,
	platform admissionPlatform,
	methodology, layout bool,
) admissionDecision {
	decision := admissionDecision{Accepted: true, Labels: []string{"implementation-comparison"}}
	decision.Changes = sourceChanges(before.Snapshot, after.Snapshot)
	mismatch := before.MeasurementHash != after.MeasurementHash ||
		before.Snapshot.Hashes.Runner != after.Snapshot.Hashes.Runner ||
		before.Snapshot.Hashes.Runner != currentRunner || after.Snapshot.Hashes.Runner != currentRunner
	if methodology {
		decision.Labels = []string{"methodology-comparison"}
	} else if mismatch {
		decision.Accepted = false
		decision.Reasons = append(decision.Reasons, "measurement or current runner identity differs")
	}
	beforeOffset, afterOffset := before.Artifacts.SchedulerOffset, after.Artifacts.SchedulerOffset
	known := validOffset(beforeOffset) && validOffset(afterOffset)
	if !known || beforeOffset != afterOffset {
		warning := "runtime.sched offsets differ or are unknown: " + beforeOffset + "/" + afterOffset
		if platform.OS == "Darwin" && platform.Architecture == "arm64" {
			if layout {
				decision.Labels = append(decision.Labels, "layout-confounded")
				decision.Warnings = append(decision.Warnings, warning)
			} else {
				decision.Accepted = false
				decision.Reasons = append(decision.Reasons, warning)
			}
		} else {
			decision.Warnings = append(decision.Warnings, warning+"; offset sensitivity unestablished")
		}
	}
	decision.Warnings = append(decision.Warnings, "equal offsets do not establish equivalent layouts")
	return decision
}

func validOffset(value string) bool {
	offset, err := strconv.Atoi(value)
	return err == nil && offset >= 0 && offset < 128
}

func currentRunnerHash(paths []string) (string, error) {
	if len(paths) == 0 {
		return "", fmt.Errorf("missing current runner bundle")
	}
	records := []hashRecord{}
	for _, path := range paths {
		checksum, err := fileDigest(path)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		logical := "runner/" + filepath.Base(path)
		if filepath.Base(filepath.Dir(path)) == "benchmeta" {
			logical = "runner/benchmeta/" + filepath.Base(path)
		}
		records = append(records, hashRecord{logical, "runner", checksum, info.Mode()&0111 != 0})
	}
	sort.Slice(records, func(a, b int) bool { return records[a].Path < records[b].Path })
	return digestJSON(records), nil
}

func admitComparison(beforePath, afterPath, runnerPath, platformPath, output string,
	methodology, layout bool) error {
	decision := admissionDecision{Accepted: false}
	before, beforeErr := verifyProvenance(beforePath)
	after, afterErr := verifyProvenance(afterPath)
	for _, err := range []error{beforeErr, afterErr} {
		if err != nil {
			decision.Reasons = append(decision.Reasons, err.Error())
		}
	}
	var runner struct {
		RunnerHash   string   `json:"runner_hash"`
		RunnerBundle []string `json:"runner_bundle"`
	}
	var platform admissionPlatform
	for _, item := range []struct {
		path   string
		target any
	}{
		{runnerPath, &runner}, {platformPath, &platform},
	} {
		if err := readJSON(item.path, item.target); err != nil {
			decision.Reasons = append(decision.Reasons, err.Error())
		}
	}
	if len(runner.RunnerBundle) > 0 {
		hash, err := currentRunnerHash(runner.RunnerBundle)
		if err != nil {
			decision.Reasons = append(decision.Reasons, err.Error())
		}
		runner.RunnerHash = hash
	}
	if runner.RunnerHash == "" || platform.OS == "" || platform.Architecture == "" {
		decision.Reasons = append(decision.Reasons, "missing current runner or platform identity")
	}
	if len(decision.Reasons) == 0 {
		decision = evaluateAdmission(before, after, runner.RunnerHash, platform, methodology, layout)
	}
	if err := writeJSON(output, decision); err != nil {
		return err
	}
	if !decision.Accepted {
		return inadmissibleError{"comparison rejected; see " + output}
	}
	encoded, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}
