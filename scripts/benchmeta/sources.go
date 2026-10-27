package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
)

const schemaVersion = 1
const ringPackage = "github.com/freshdresch/go-data-structures/ring"

// isRingPackage reports whether importPath holds queue implementation code: the ring package of
// older commits, or any package under it, such as ring/dpdk, ring/frontier, and ring/internal.
func isRingPackage(importPath string) bool {
	return importPath == ringPackage || strings.HasPrefix(importPath, ringPackage+"/")
}

var environmentKeys = []string{
	"GOOS", "GOARCH", "GOAMD64", "GOARM64", "CGO_ENABLED", "CC", "CXX", "GOEXPERIMENT",
	"GOTOOLCHAIN", "GOROOT", "GOWORK", "GOFLAGS",
}

type buildSettings struct {
	Race        bool              `json:"race"`
	Overlay     string            `json:"overlay,omitempty"`
	Environment map[string]string `json:"environment"`
}

type snapshotRequest struct {
	HelperBinary  string        `json:"helper_binary,omitempty"`
	Schema        int           `json:"schema"`
	RunnerKind    string        `json:"runner_kind"`
	Origin        string        `json:"origin"`
	SourceRoot    string        `json:"source_root"`
	PackageDir    string        `json:"package_dir"`
	PackageTarget string        `json:"package_target"`
	Variant       string        `json:"variant"`
	RunnerBundle  []string      `json:"runner_bundle"`
	Settings      buildSettings `json:"settings"`
	BuildCommand  []string      `json:"build_command"`
}

type sourceRecord struct {
	Path         string `json:"path"`
	Class        string `json:"class"`
	SHA256       string `json:"sha256"`
	Executable   bool   `json:"executable"`
	OriginalPath string `json:"original_path"`
	Snapshot     string `json:"snapshot"`
}

type identityHashes struct {
	Implementation string `json:"implementation_hash"`
	Measurement    string `json:"measurement_inputs_hash"`
	Layout         string `json:"layout_input_hash"`
	Runner         string `json:"runner_hash"`
}

type sourceSnapshot struct {
	HelperBinaryHash     string            `json:"helper_binary_hash,omitempty"`
	HelperBinarySnapshot string            `json:"helper_binary_snapshot,omitempty"`
	Schema               int               `json:"schema"`
	Phase                string            `json:"phase"`
	Complete             bool              `json:"complete"`
	Request              snapshotRequest   `json:"request"`
	EffectiveSettings    buildSettings     `json:"effective_settings"`
	GoVersion            string            `json:"go_version"`
	Toolchain            map[string]string `json:"toolchain"`
	CompilerVersion      string            `json:"compiler_version"`
	ModuleGraph          json.RawMessage   `json:"module_graph"`
	Records              []sourceRecord    `json:"records"`
	GeneratedInputs      []string          `json:"generated_inputs"`
	Hashes               identityHashes    `json:"hashes"`
	DiscoveryCommands    [][]string        `json:"discovery_commands"`
}

type listedModule struct {
	Path    string
	Version string
	Dir     string
	GoMod   string
	Replace *listedModule
}

type listedPackage struct {
	Dir                                                                       string
	ImportPath                                                                string
	Name                                                                      string
	ForTest                                                                   string
	Goroot                                                                    bool
	Module                                                                    *listedModule
	GoFiles, CgoFiles, TestGoFiles, XTestGoFiles                              []string
	CFiles, CXXFiles, MFiles, HFiles, FFiles, SFiles, SwigFiles, SwigCXXFiles []string
	SysoFiles, EmbedFiles, TestEmbedFiles, XTestEmbedFiles, IgnoredGoFiles    []string
	Error                                                                     *struct{ Err string }
	DepsErrors                                                                []struct{ Err string }
}

type hashRecord struct {
	Path       string `json:"path"`
	Class      string `json:"class"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}

func digest(bytes []byte) string {
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func digestJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return digest(encoded)
}

func fileDigest(path string) (string, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return digest(bytes), nil
}

func validateRequest(request snapshotRequest) error {
	if request.Schema != schemaVersion {
		return fmt.Errorf("unsupported request schema %d", request.Schema)
	}
	if request.SourceRoot == "" || request.PackageDir == "" || request.Origin == "" ||
		request.Variant == "" || request.RunnerKind == "" || len(request.RunnerBundle) == 0 {
		return fmt.Errorf("missing required snapshot identity")
	}
	if !slices.Contains([]string{"./ring", "./ring/dpdk", "."}, request.PackageTarget) {
		return fmt.Errorf("unsupported package target %q", request.PackageTarget)
	}
	if !within(request.SourceRoot, request.PackageDir) {
		return fmt.Errorf("package outside source root")
	}
	for key := range request.Settings.Environment {
		if !slices.Contains(environmentKeys, key) {
			return fmt.Errorf("unsupported Go setting %s", key)
		}
	}
	for key, expected := range map[string]string{
		"GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly",
	} {
		if request.Settings.Environment[key] != expected {
			return fmt.Errorf("%s must equal %q", key, expected)
		}
	}
	if len(request.BuildCommand) == 0 {
		return fmt.Errorf("missing build command")
	}
	return nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func goCommand(request snapshotRequest, arguments ...string) ([]byte, []byte, error) {
	command := exec.Command("go", arguments...)
	command.Dir = request.PackageDir
	command.Env = os.Environ()
	for key, value := range request.Settings.Environment {
		command.Env = append(command.Env, key+"="+value)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if err != nil {
		err = fmt.Errorf("go %s: %w: %s", strings.Join(arguments, " "), err, stderr.String())
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func decodePackages(stream []byte) ([]listedPackage, error) {
	decoder := json.NewDecoder(bytes.NewReader(stream))
	var packages []listedPackage
	for {
		var entry listedPackage
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if entry.Error != nil {
			return nil, fmt.Errorf("%s: %s", entry.ImportPath, entry.Error.Err)
		}
		if len(entry.DepsErrors) != 0 {
			return nil, fmt.Errorf("%s", entry.DepsErrors[0].Err)
		}
		packages = append(packages, entry)
	}
	if len(packages) == 0 {
		return nil, fmt.Errorf("empty package discovery")
	}
	return packages, nil
}

func canonicalModuleGraph(stream []byte, root, goroot string) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(stream))
	var modules []map[string]any
	for {
		var module map[string]any
		if err := decoder.Decode(&module); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		normalizeModulePaths(module, root)
		modules = append(modules, module)
	}
	if len(modules) == 0 {
		return nil, fmt.Errorf("empty module graph")
	}
	encoded, err := json.Marshal(modules)
	if err != nil {
		return nil, err
	}
	normalized := strings.ReplaceAll(string(encoded), root, "<source-root>")
	normalized = strings.ReplaceAll(normalized, goroot, "<GOROOT>")
	return json.RawMessage(normalized), nil
}

// normalizeModulePaths rewrites module and its nested replacements in place to remove host paths.
func normalizeModulePaths(module map[string]any, root string) {
	path, _ := module["Path"].(string)
	version, _ := module["Version"].(string)
	logical := "module/" + path + "@" + version
	for _, key := range []string{"Dir", "GoMod"} {
		original, ok := module[key].(string)
		if !ok {
			continue
		}
		if within(root, original) {
			// within already confirmed that Rel succeeds for these unchanged paths.
			relative, _ := filepath.Rel(root, original)
			module[key] = "repo/" + filepath.ToSlash(relative)
		} else {
			module[key] = logical
			if key == "GoMod" {
				module[key] = logical + "/go.mod"
			}
		}
	}
	if replacement, ok := module["Replace"].(map[string]any); ok {
		normalizeModulePaths(replacement, root)
	}
}

func logicalSourcePath(
	request snapshotRequest,
	entry listedPackage,
	path, goroot string,
) (string, error) {
	if within(request.SourceRoot, path) {
		relative, err := filepath.Rel(request.SourceRoot, path)
		return "repo/" + filepath.ToSlash(relative), err
	}
	if within(goroot, path) {
		relative, err := filepath.Rel(goroot, path)
		return "GOROOT/" + filepath.ToSlash(relative), err
	}
	if entry.Module == nil {
		return "", fmt.Errorf("unresolvable source %s", path)
	}
	module := entry.Module
	if module.Replace != nil {
		if module.Replace.Version == "" {
			return "", fmt.Errorf("external local replacement %s", path)
		}
		module = module.Replace
	}
	if module.Dir == "" || !within(module.Dir, path) {
		return "", fmt.Errorf("source outside selected module %s", path)
	}
	relative, err := filepath.Rel(module.Dir, path)
	return "module/" + module.Path + "@" + module.Version + "/" + filepath.ToSlash(relative), err
}

func addSourceRecord(
	records map[string]sourceRecord,
	logical, class, path, snapshotDirectory string,
) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("source %s: %w", logical, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	checksum := digest(content)
	if err := os.MkdirAll(snapshotDirectory, 0700); err != nil {
		return err
	}
	saved := filepath.Join(snapshotDirectory, checksum)
	if err := os.WriteFile(saved, content, 0600); err != nil {
		return err
	}
	record := sourceRecord{
		Path: logical, Class: class, SHA256: checksum, Executable: info.Mode()&0111 != 0,
		OriginalPath: path, Snapshot: filepath.Join("sources", checksum),
	}
	if previous, ok := records[logical]; ok && previous.SHA256 != checksum {
		return fmt.Errorf("conflicting source identity %s", logical)
	}
	records[logical] = record
	return nil
}

func recordsFromPackages(
	request snapshotRequest,
	packages []listedPackage,
	goroot, snapshotDirectory string,
) ([]sourceRecord, []string, error) {
	records := map[string]sourceRecord{}
	var overlay struct{ Replace map[string]string }
	normalized := map[string]string{}
	if request.Settings.Overlay != "" {
		if err := readJSON(request.Settings.Overlay, &overlay); err != nil {
			return nil, nil, err
		}
		for original := range overlay.Replace {
			relative, err := filepath.Rel(request.SourceRoot, original)
			if err != nil || !within(request.SourceRoot, original) {
				return nil, nil, fmt.Errorf("overlay outside root")
			}
			normalized["repo/"+filepath.ToSlash(relative)] = "overlay/" + filepath.ToSlash(relative)
		}
	}
	var generated []string
	for _, entry := range packages {
		if entry.Name == "main" && strings.HasSuffix(entry.ImportPath, ".test") {
			generated = append(generated, "Go-generated _testmain.go: "+entry.ImportPath)
			continue
		}
		selected := [][]string{
			entry.GoFiles, entry.CgoFiles, entry.TestGoFiles, entry.XTestGoFiles, entry.CFiles,
			entry.CXXFiles, entry.MFiles, entry.HFiles, entry.FFiles, entry.SFiles, entry.SwigFiles,
			entry.SwigCXXFiles, entry.SysoFiles, entry.EmbedFiles, entry.TestEmbedFiles,
			entry.XTestEmbedFiles, entry.IgnoredGoFiles,
		}
		for group, files := range selected {
			for _, file := range files {
				path := file
				if !filepath.IsAbs(path) {
					path = filepath.Join(entry.Dir, file)
				}
				logical, err := logicalSourcePath(request, entry, path, goroot)
				if err != nil {
					return nil, nil, err
				}
				if replacement, mapped := overlay.Replace[path]; mapped {
					overlayLogical := normalized[logical]
					if _, captured := records[overlayLogical]; !captured {
						if err := addSourceRecord(
							records,
							overlayLogical,
							"layout",
							replacement,
							snapshotDirectory,
						); err != nil {
							return nil, nil, err
						}
					}
					continue
				}
				class := "measurement"
				canonicalPackage := strings.Split(entry.ImportPath, " [")[0]
				if isRingPackage(canonicalPackage) && !strings.HasSuffix(file, "_test.go") {
					class = "implementation"
				}
				if group == len(selected)-1 {
					class = "inventory"
				}
				if err := addSourceRecord(records, logical, class, path, snapshotDirectory); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	for _, directory := range []string{request.SourceRoot, request.PackageDir} {
		for _, name := range []string{"go.mod", "go.sum"} {
			path := filepath.Join(directory, name)
			if _, err := os.Stat(path); os.IsNotExist(err) && name == "go.sum" {
				continue
			}
			relative, err := filepath.Rel(request.SourceRoot, path)
			if err != nil {
				return nil, nil, err
			}
			if err := addSourceRecord(
				records,
				"repo/"+filepath.ToSlash(relative),
				"module/build",
				path,
				snapshotDirectory,
			); err != nil {
				return nil, nil, err
			}
		}
	}
	scriptsDirectory := filepath.Join(request.SourceRoot, "scripts")
	entries, err := os.ReadDir(scriptsDirectory)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sh") {
			continue
		}
		path := filepath.Join(scriptsDirectory, entry.Name())
		if err := addSourceRecord(
			records,
			"inventory/scripts/"+entry.Name(),
			"inventory",
			path,
			snapshotDirectory,
		); err != nil {
			return nil, nil, err
		}
	}
	for _, path := range request.RunnerBundle {
		logical := "runner/" + filepath.Base(path)
		if filepath.Base(filepath.Dir(path)) == "benchmeta" {
			logical = "runner/benchmeta/" + filepath.Base(path)
		}
		if err := addSourceRecord(records, logical, "runner", path, snapshotDirectory); err != nil {
			return nil, nil, err
		}
	}
	if request.Settings.Overlay != "" {
		for original, replacement := range overlay.Replace {
			relative, err := filepath.Rel(request.SourceRoot, original)
			if err != nil {
				return nil, nil, err
			}
			logical := normalized["repo/"+filepath.ToSlash(relative)]
			if _, captured := records[logical]; captured {
				continue
			}
			if err := addSourceRecord(
				records,
				logical,
				"layout",
				replacement,
				snapshotDirectory,
			); err != nil {
				return nil, nil, err
			}
		}
		normalizedBytes, err := json.Marshal(normalized)
		if err != nil {
			return nil, nil, err
		}
		checksum := digest(normalizedBytes)
		mappingPath := filepath.Join(snapshotDirectory, checksum)
		if err := os.WriteFile(mappingPath, normalizedBytes, 0600); err != nil {
			return nil, nil, err
		}
		records["overlay/mapping"] = sourceRecord{
			Path: "overlay/mapping", Class: "layout", SHA256: checksum,
			OriginalPath: request.Settings.Overlay, Snapshot: filepath.Join("sources", checksum),
		}
		// Preserve exact mapping bytes without putting temporary paths into content identities.
		if err := addSourceRecord(
			records,
			"overlay/original-mapping",
			"inventory",
			request.Settings.Overlay,
			snapshotDirectory,
		); err != nil {
			return nil, nil, err
		}
	}
	var result []sourceRecord
	for _, record := range records {
		result = append(result, record)
	}
	sort.Slice(result, func(a, b int) bool { return result[a].Path < result[b].Path })
	slices.Sort(generated)
	return result, slices.Compact(generated), nil
}

func validateSnapshotIdentity(snapshot sourceSnapshot) error {
	if err := validateRequest(snapshot.Request); err != nil {
		return err
	}
	if snapshot.Schema != schemaVersion || snapshot.Phase != "snapshot" || snapshot.Complete ||
		snapshot.GoVersion == "" || len(snapshot.Records) == 0 || len(snapshot.ModuleGraph) == 0 {
		return fmt.Errorf("incomplete source identity")
	}
	required := []string{
		"GOOS", "GOARCH", "CGO_ENABLED", "GOROOT", "GOTOOLCHAIN", "GOWORK", "GOFLAGS",
	}
	for _, key := range required {
		value := snapshot.EffectiveSettings.Environment[key]
		if value == "" || value == "unknown" {
			return fmt.Errorf("unknown required setting %s", key)
		}
	}
	for _, key := range []string{"compile", "link"} {
		decoded, err := hex.DecodeString(snapshot.Toolchain[key])
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("missing toolchain digest %s", key)
		}
	}
	return nil
}

func computeHashes(snapshot sourceSnapshot) identityHashes {
	var implementation, measurement, layout, runner []hashRecord
	for _, record := range snapshot.Records {
		entry := hashRecord{record.Path, record.Class, record.SHA256, record.Executable}
		switch record.Class {
		case "implementation":
			implementation = append(implementation, entry)
			layout = append(layout, entry)
		case "measurement", "module/build":
			measurement = append(measurement, entry)
			layout = append(layout, entry)
		case "runner":
			measurement = append(measurement, entry)
			runner = append(runner, entry)
		case "layout":
			layout = append(layout, entry)
		}
	}
	settings := normalizedSettings(snapshot)
	context := []any{
		settings, snapshot.GoVersion, snapshot.Toolchain, snapshot.CompilerVersion, snapshot.ModuleGraph,
	}
	return identityHashes{
		Implementation: digestJSON(implementation),
		Measurement:    digestJSON([]any{measurement, context}),
		Layout:         digestJSON([]any{layout, context}), Runner: digestJSON(runner),
	}
}

func normalizedSettings(snapshot sourceSnapshot) buildSettings {
	settings := snapshot.EffectiveSettings
	settings.Environment = map[string]string{}
	for key, value := range snapshot.EffectiveSettings.Environment {
		if key == "GOROOT" {
			value = "<GOROOT>"
		}
		settings.Environment[key] = value
	}
	if settings.Overlay != "" {
		settings.Overlay = "<overlay>"
	}
	return settings
}

func captureSnapshot(request snapshotRequest, output string) (sourceSnapshot, error) {
	if err := validateRequest(request); err != nil {
		return sourceSnapshot{}, err
	}
	snapshot := sourceSnapshot{Schema: schemaVersion, Phase: "snapshot", Request: request}
	arguments := append([]string{"env", "-json"}, environmentKeys...)
	environment, _, err := goCommand(request, arguments...)
	if err != nil {
		return snapshot, err
	}
	snapshot.EffectiveSettings = request.Settings
	snapshot.EffectiveSettings.Environment = map[string]string{}
	if err := json.Unmarshal(environment, &snapshot.EffectiveSettings.Environment); err != nil {
		return snapshot, err
	}
	goroot := snapshot.EffectiveSettings.Environment["GOROOT"]
	version, _, err := goCommand(request, "version")
	if err != nil {
		return snapshot, err
	}
	snapshot.GoVersion = strings.TrimSpace(string(version))
	tools, _, err := goCommand(request, "env", "GOTOOLDIR")
	if err != nil {
		return snapshot, err
	}
	snapshot.Toolchain = map[string]string{}
	for _, name := range []string{"compile", "link"} {
		checksum, err := fileDigest(filepath.Join(strings.TrimSpace(string(tools)), name))
		if err != nil {
			return snapshot, err
		}
		snapshot.Toolchain[name] = checksum
	}
	if snapshot.EffectiveSettings.Environment["CGO_ENABLED"] == "1" {
		compiler := strings.Fields(snapshot.EffectiveSettings.Environment["CC"])
		if len(compiler) != 1 {
			return snapshot, fmt.Errorf("unsupported CC command")
		}
		resolved, err := exec.LookPath(compiler[0])
		if err != nil {
			return snapshot, err
		}
		checksum, err := fileDigest(resolved)
		if err != nil {
			return snapshot, err
		}
		snapshot.Toolchain["cgo-compiler"] = checksum
		version, err := exec.Command(resolved, "--version").CombinedOutput()
		if err != nil {
			return snapshot, err
		}
		snapshot.CompilerVersion = string(version)
	}
	moduleBytes, moduleErrors, err := goCommand(request, "list", "-m", "-json", "all")
	if writeErr := saveDiscovery(output, "modules", moduleBytes, moduleErrors); writeErr != nil {
		return snapshot, writeErr
	}
	if err != nil {
		return snapshot, err
	}
	snapshot.ModuleGraph, err = canonicalModuleGraph(moduleBytes, request.SourceRoot, goroot)
	if err != nil {
		return snapshot, err
	}
	arguments = []string{"list", "-deps", "-test", "-json"}
	if request.Settings.Race {
		arguments = append(arguments, "-race")
	}
	if request.Settings.Overlay != "" {
		arguments = append(arguments, "-overlay", request.Settings.Overlay)
	}
	arguments = append(arguments, request.PackageTarget)
	snapshot.DiscoveryCommands = [][]string{append([]string{"go"}, arguments...),
		{"go", "list", "-m", "-json", "all"}}
	packageBytes, packageErrors, err := goCommand(request, arguments...)
	if writeErr := saveDiscovery(output, "packages", packageBytes, packageErrors); writeErr != nil {
		return snapshot, writeErr
	}
	if err != nil {
		return snapshot, err
	}
	packages, err := decodePackages(packageBytes)
	if err != nil {
		return snapshot, err
	}
	snapshot.Records, snapshot.GeneratedInputs, err = recordsFromPackages(
		request,
		packages,
		goroot,
		filepath.Join(filepath.Dir(output), "sources"),
	)
	if err != nil {
		return snapshot, err
	}
	if request.HelperBinary != "" {
		records := map[string]sourceRecord{}
		if err := addSourceRecord(
			records,
			"helper",
			"artifact",
			request.HelperBinary,
			filepath.Join(filepath.Dir(output), "sources"),
		); err != nil {
			return snapshot, err
		}
		snapshot.HelperBinaryHash = records["helper"].SHA256
		snapshot.HelperBinarySnapshot = records["helper"].Snapshot
	}
	snapshot.Hashes = computeHashes(snapshot)
	return snapshot, nil
}

func saveDiscovery(output, kind string, stdout, stderr []byte) error {
	if err := os.WriteFile(output+"."+kind+".stdout", stdout, 0600); err != nil {
		return err
	}
	return os.WriteFile(output+"."+kind+".stderr", stderr, 0600)
}

type artifactRequest struct {
	Binary            string `json:"binary"`
	BuildInfo         string `json:"build_info"`
	Manifest          string `json:"manifest"`
	ManifestLog       string `json:"manifest_log"`
	SchedulerOffset   string `json:"scheduler_offset"`
	SingleManifest    string `json:"single_manifest,omitempty"`
	SingleManifestLog string `json:"single_manifest_log,omitempty"`
}

type finalizedProvenance struct {
	Schema                int             `json:"schema"`
	Phase                 string          `json:"phase"`
	Complete              bool            `json:"complete"`
	Snapshot              sourceSnapshot  `json:"snapshot"`
	SnapshotHash          string          `json:"snapshot_hash"`
	SnapshotFile          string          `json:"snapshot_file"`
	Artifacts             artifactRequest `json:"artifacts"`
	BinaryHash            string          `json:"binary_hash"`
	BuildInfoHash         string          `json:"build_info_hash"`
	ManifestHash          string          `json:"manifest_hash"`
	ManifestLogHash       string          `json:"manifest_log_hash"`
	SingleManifestHash    string          `json:"single_manifest_hash,omitempty"`
	SingleManifestLogHash string          `json:"single_manifest_log_hash,omitempty"`
	MeasurementHash       string          `json:"measurement_hash"`
}

type inputChange struct {
	Path   string `json:"path"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

type verificationReport struct {
	Added   []inputChange  `json:"added"`
	Removed []inputChange  `json:"removed"`
	Changed []inputChange  `json:"changed"`
	Errors  []string       `json:"errors"`
	Before  sourceSnapshot `json:"before"`
	After   sourceSnapshot `json:"after"`
}

func compareSnapshots(before, after sourceSnapshot) verificationReport {
	report := verificationReport{Before: before, After: after}
	previous := map[string]sourceRecord{}
	current := map[string]sourceRecord{}
	for _, record := range before.Records {
		previous[record.Path] = record
	}
	for _, record := range after.Records {
		current[record.Path] = record
	}
	for path, record := range current {
		old, exists := previous[path]
		if !exists {
			report.Added = append(report.Added, inputChange{Path: path, After: record.SHA256})
			continue
		}
		if old.SHA256 != record.SHA256 || old.Executable != record.Executable ||
			old.Class != record.Class {
			report.Changed = append(report.Changed, inputChange{path, old.SHA256, record.SHA256})
		}
	}
	for path, record := range previous {
		if _, exists := current[path]; !exists {
			report.Removed = append(report.Removed, inputChange{Path: path, Before: record.SHA256})
		}
	}
	for _, changes := range [][]inputChange{report.Added, report.Removed, report.Changed} {
		sort.Slice(changes, func(a, b int) bool { return changes[a].Path < changes[b].Path })
	}
	if !reflect.DeepEqual(normalizedSettings(before), normalizedSettings(after)) ||
		before.GoVersion != after.GoVersion || !reflect.DeepEqual(before.Toolchain, after.Toolchain) ||
		digestJSON(before.ModuleGraph) != digestJSON(after.ModuleGraph) ||
		before.CompilerVersion != after.CompilerVersion ||
		before.HelperBinaryHash != after.HelperBinaryHash {
		report.Errors = append(report.Errors, "build environment, toolchain, or module graph changed")
	}
	return report
}

func finalizeSnapshot(
	snapshotPath, artifactsPath, output, reportPath string,
	checkOnly bool,
) error {
	var snapshot sourceSnapshot
	if err := readJSON(snapshotPath, &snapshot); err != nil {
		return err
	}
	if err := validateSnapshotIdentity(snapshot); err != nil {
		return err
	}
	after, discoveryErr := captureSnapshot(snapshot.Request, reportPath+".postbuild")
	report := compareSnapshots(snapshot, after)
	if discoveryErr != nil {
		report.Errors = append(report.Errors, discoveryErr.Error())
	}
	if err := writeJSON(reportPath, report); err != nil {
		return err
	}
	if len(report.Added)+len(report.Removed)+len(report.Changed)+len(report.Errors) != 0 {
		return inadmissibleError{"source verification failed; see " + reportPath}
	}
	var artifacts artifactRequest
	if err := readJSON(artifactsPath, &artifacts); err != nil {
		return err
	}
	provenance, err := attachArtifacts(snapshot, snapshotPath, artifacts)
	if err != nil {
		return err
	}
	if checkOnly {
		var previous finalizedProvenance
		if err := readJSON(output, &previous); err != nil {
			return err
		}
		if digestJSON(previous) != digestJSON(provenance) {
			return inadmissibleError{"finalized artifact changed"}
		}
		return nil
	}
	return writeJSON(output, provenance)
}

func attachArtifacts(
	snapshot sourceSnapshot,
	snapshotPath string,
	artifacts artifactRequest,
) (finalizedProvenance, error) {
	provenance := finalizedProvenance{
		Schema: schemaVersion, Phase: "finalized", Complete: true, Snapshot: snapshot,
		SnapshotFile: filepath.Base(snapshotPath), Artifacts: artifacts,
	}
	directory := filepath.Dir(snapshotPath)
	var err error
	for _, field := range []struct {
		path   string
		target *string
	}{
		{snapshotPath, &provenance.SnapshotHash}, {artifacts.Binary, &provenance.BinaryHash},
		{artifacts.BuildInfo, &provenance.BuildInfoHash}, {artifacts.Manifest, &provenance.ManifestHash},
		{artifacts.ManifestLog, &provenance.ManifestLogHash},
	} {
		*field.target, err = fileDigest(artifactPath(directory, field.path))
		if err != nil {
			return provenance, err
		}
	}
	if artifacts.SchedulerOffset == "" {
		return provenance, fmt.Errorf("scheduler offset must be explicit")
	}
	info, err := os.ReadFile(artifactPath(directory, artifacts.BuildInfo))
	if err != nil {
		return provenance, err
	}
	if snapshot.Request.Settings.Race != bytes.Contains(info, []byte("-race=true")) {
		return provenance, fmt.Errorf("binary race setting does not match snapshot")
	}
	for _, key := range []string{"GOOS", "GOARCH", "CGO_ENABLED"} {
		expected := snapshot.EffectiveSettings.Environment[key]
		if expected == "" || !bytes.Contains(info, []byte(key+"="+expected)) {
			return provenance, fmt.Errorf("binary build setting %s does not match snapshot", key)
		}
	}
	actualInfo, _, err := goCommand(
		snapshot.Request,
		"version",
		"-m",
		artifactPath(directory, artifacts.Binary),
	)
	if err != nil {
		return provenance, err
	}
	if !bytes.Equal(info, actualInfo) {
		return provenance, fmt.Errorf("binary build metadata mismatch")
	}
	var manifest workloadManifest
	if err := readJSON(artifactPath(directory, artifacts.Manifest), &manifest); err != nil {
		return provenance, err
	}
	if err := validateManifest(manifest); err != nil {
		return provenance, err
	}
	logBytes, err := os.ReadFile(artifactPath(directory, artifacts.ManifestLog))
	if err != nil {
		return provenance, err
	}
	if err := validateManifestLog(logBytes, manifest); err != nil {
		return provenance, err
	}
	if artifacts.SingleManifest != "" || artifacts.SingleManifestLog != "" {
		provenance.SingleManifestHash, err = fileDigest(artifactPath(directory, artifacts.SingleManifest))
		if err != nil {
			return provenance, err
		}
		singleLogPath := artifactPath(directory, artifacts.SingleManifestLog)
		provenance.SingleManifestLogHash, err = fileDigest(singleLogPath)
		if err != nil {
			return provenance, err
		}
		var single workloadManifest
		if err := readJSON(artifactPath(directory, artifacts.SingleManifest), &single); err != nil {
			return provenance, err
		}
		singleLog, err := os.ReadFile(artifactPath(directory, artifacts.SingleManifestLog))
		if err != nil {
			return provenance, err
		}
		if err := validateManifestLog(singleLog, single); err != nil {
			return provenance, err
		}
		if single.GOMAXPROCS != 1 || digestJSON(single.Rows) != digestJSON(manifest.Rows) {
			return provenance, fmt.Errorf("single and concurrent metadata contracts differ")
		}
	}
	provenance.MeasurementHash = digestJSON([]any{snapshot.Hashes.Measurement, manifest.Rows})
	return provenance, nil
}
