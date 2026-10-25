package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

type inadmissibleError struct{ message string }

func (err inadmissibleError) Error() string { return err.message }

func readJSON(path string, value any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%s: trailing JSON input", path)
	}
	return nil
}

func writeJSON(path string, value any) error {
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(bytes, '\n'), 0600)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		// A stderr failure cannot usefully report another error while printing this one.
		_, _ = fmt.Fprintln(os.Stderr, err)
		var rejected inadmissibleError
		if errors.As(err, &rejected) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("expected snapshot, finalize, admit, rows, or project")
	}
	flags := flag.NewFlagSet(arguments[0], flag.ContinueOnError)
	switch arguments[0] {
	case "snapshot":
		requestPath := flags.String("request", "", "snapshot request")
		output := flags.String("output", "", "snapshot output")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		var request snapshotRequest
		if err := readJSON(*requestPath, &request); err != nil {
			return err
		}
		snapshot, err := captureSnapshot(request, *output)
		if err != nil {
			return err
		}
		return writeJSON(*output, snapshot)
	case "finalize":
		snapshot := flags.String("snapshot", "", "snapshot input")
		artifacts := flags.String("artifacts", "", "artifact request")
		output := flags.String("output", "", "final provenance")
		report := flags.String("report", "", "verification report")
		checkOnly := flags.Bool("check-only", false, "verify without replacing provenance")
		if err := flags.Parse(arguments[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return fmt.Errorf("unexpected arguments")
		}
		return finalizeSnapshot(*snapshot, *artifacts, *output, *report, *checkOnly)
	default:
		return runResultCommand(arguments)
	}
}
