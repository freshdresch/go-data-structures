"""Bounded acceptance and provenance regressions. No campaign processes run."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import campaign
import provenance
from validation import check_snapshot, validate_report


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.report = json.loads((Path(__file__).parent / "testdata/valid-report.json").read_text())
        self.options = copy.deepcopy(self.report["options"])

    def test_real_smoke_fixture(self):
        validate_report(self.report, self.options)

    def test_rejects_missing_fields(self):
        # Every emitted field, except an empty optional build dependency replacement,
        # must survive the independent acceptance gate.
        paths = [
            (key,) for key in self.report
        ] + [
            ("options", key) for key in self.options
        ] + [
            ("measured", key) for key in self.report["measured"]
        ] + [("final", key) for key in self.report["final"]]
        for path in paths:
            with self.subTest(path=path):
                report = copy.deepcopy(self.report)
                parent = report
                for key in path[:-1]:
                    parent = parent[key]
                del parent[path[-1]]
                with self.assertRaises((ValueError, KeyError, TypeError)):
                    validate_report(report, self.options)

    def test_rejects_malformed_or_stale_values(self):
        changes = [
            (("valid",), 1), (("valid",), "true"), (("failure",), ""),
            (("options", "payload_bytes"), 4096), (("options", "submit_batch_limit"), True),
            (("options", "library_revision"), "stale"), (("options", "directory"), "old"),
            (("options", "policy"), "NORMAL"), (("options", "warmup_ns"), 1),
            (("options", "admission_ns"), 1), (("measured", "accepted"), True),
            (("measured", "acknowledged"), 0), (("measured", "waves"), False),
            (("measured", "submit_batch_latency", "total_ns"), 0),
            (("measured", "ack_batch_latency", "count"), 100000),
            (("measured", "receive_batch_sizes", 0), 1),
            (("measured", "accepted_by_producer", 0), 0),
            (("cpu_ns",), -1), (("elapsed_ns",), 0),
            (("records_per_second",), float("nan")), (("cpu_ns_per_record",), float("inf")),
            (("allocated_bytes_per_record",), 1), (("allocations_per_record",), False),
            (("final", "State"), 0), (("final", "Store", "Counts", "Receipts"), 1),
            (("inspection", "Store", "Epoch"), 1),
            (("inspection", "Store", "UUID", 0), 999),
            (("inspection", "Store", "Policy", "MaxReceipts"), 1),
            (("inspection", "Statuses", 0, "RetiredThrough"), 0),
            (("inspection", "Statuses", 0, "HasRetirement"), 1),
        ]
        for path, value in changes:
            with self.subTest(path=path, value=value):
                report = copy.deepcopy(self.report)
                parent = report
                for key in path[:-1]:
                    parent = parent[key]
                parent[path[-1]] = value
                with self.assertRaises((ValueError, KeyError, TypeError)):
                    validate_report(report, self.options)

    def test_rejects_contradictory_queue_metrics(self):
        changes = [
            (("CommitLatency", "Count"), 0), (("CommitLatency", "Total"), 0),
            (("CommitLatency", "Last"), 10**12), (("CommitLatency", "Max"), 10**12),
            (("AckLatency", "Count"), 1), (("AckLatency", "Total"), 0),
            (("Checkpoint", "Valid"), False), (("Checkpoint", "Busy"), 1),
            (("Checkpoint", "Completed"), 1), (("Outcomes", "Checkpoints"), 0),
            (("AdmissionStopped",), False), (("DeliveryStopped",), False),
            (("Store", "WALBytes"), 1),
        ]
        for group in ("Ordinary", "Progress", "Delivery", "CurrentGroup"):
            changes.extend(((group, key), 1) for key in self.report["final"][group])
        for path, value in changes:
            with self.subTest(path=path):
                report = copy.deepcopy(self.report)
                target = report["final"]
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                with self.assertRaises(ValueError):
                    validate_report(report, self.options)

    def test_rejects_decreasing_latency_aggregates(self):
        for group in ("CommitLatency", "AckLatency"):
            for key in ("Total", "Max"):
                with self.subTest(group=group, key=key):
                    report = copy.deepcopy(self.report)
                    before, final = report["before"][group], report["final"][group]
                    if key == "Total":
                        final["Total"] = before["Total"] - 1
                    else:
                        final["Max"] = before["Max"] - 1
                        final["Last"] = min(final["Last"], final["Max"])
                    with self.assertRaisesRegex(ValueError, "decreased"):
                        validate_report(report, self.options)

    def test_rejects_latency_increment_contradictions(self):
        for group in ("CommitLatency", "AckLatency"):
            with self.subTest(group=group):
                report = copy.deepcopy(self.report)
                before, final = report["before"][group], report["final"][group]
                final["Total"] = before["Total"]
                with self.assertRaisesRegex(ValueError, "exceeds increment"):
                    validate_report(report, self.options)

    def test_rejects_distinct_max_and_last_without_enough_total(self):
        for group in ("CommitLatency", "AckLatency"):
            with self.subTest(group=group):
                report = copy.deepcopy(self.report)
                report["before"][group].update(Total=100, Max=100, Last=99)
                report["final"][group].update(Total=200, Max=100, Last=99)
                with self.assertRaisesRegex(ValueError, "distinct maximum and last"):
                    validate_report(report, self.options)

    def test_rejects_single_latency_increment_not_equal_to_last(self):
        for group in ("CommitLatency", "AckLatency"):
            with self.subTest(group=group):
                report = copy.deepcopy(self.report)
                measured = report["measured"]
                measured.update(accepted=1, acknowledged=1, accepted_by_producer=[1, 0, 0, 0])
                for key in ("submit_batch_latency", "ack_batch_latency"):
                    measured[key] = {
                        "count": 1, "total_ns": 1, "max_ns": 1,
                        "base2_nanosecond_buckets": [1] + [0] * 63,
                    }
                for key in ("receive_batch_sizes", "ack_batch_sizes"):
                    measured[key] = [0, 1] + [0] * 255
                report["final"]["Outcomes"]["Acks"] = report["warmup"]["accepted"] + 1
                before, final = report["before"], report["final"]
                for latency in ("CommitLatency", "AckLatency"):
                    before[latency].update(Total=200, Max=100, Last=99)
                    final[latency].update(
                        Count=before[latency]["Count"] + 1, Total=300, Max=100, Last=100,
                    )
                final["Outcomes"]["Commits"] = final["CommitLatency"]["Count"]
                for producer, status in enumerate(report["inspection"]["Statuses"]):
                    frontier = report["warmup"]["accepted_by_producer"][producer]
                    frontier += measured["accepted_by_producer"][producer]
                    status["ID"]["Sequence"] = frontier
                    status["RetiredThrough"] = frontier
                report["records_per_second"] = 1e9 / report["elapsed_ns"]
                report["cpu_ns_per_record"] = report["cpu_ns"]
                report["allocated_bytes_per_record"] = report["total_alloc_bytes"]
                report["allocations_per_record"] = report["mallocs"]
                validate_report(report, self.options)
                final[group]["Last"] = 99
                with self.assertRaisesRegex(ValueError, "queue latency increment bounds"):
                    validate_report(report, self.options)

    def test_accepts_equal_max_last_and_zero_durations(self):
        for maximum in (0, 100):
            with self.subTest(maximum=maximum):
                report = copy.deepcopy(self.report)
                for name in ("before", "final"):
                    for group in ("CommitLatency", "AckLatency"):
                        report[name][group].update(Total=maximum, Max=maximum, Last=maximum)
                for group in ("CommitLatency", "AckLatency"):
                    report["final"][group]["Total"] += maximum
                validate_report(report, self.options)

    def test_queue_latency_upper_bound_includes_last(self):
        for group in ("CommitLatency", "AckLatency"):
            for total in (150, 250, 251):
                with self.subTest(group=group, total=total):
                    snapshot = copy.deepcopy(self.report["before"])
                    snapshot[group].update(Count=3, Total=total, Max=100, Last=50)
                    if group == "CommitLatency":
                        snapshot["Outcomes"]["Commits"] = 3
                    if total <= 250:
                        check_snapshot(snapshot)
                    else:
                        with self.assertRaises(ValueError):
                            check_snapshot(snapshot)

    def test_latency_increment_bounds_include_last_and_new_max(self):
        for group in ("CommitLatency", "AckLatency"):
            for case in ("upper", "new maximum"):
                with self.subTest(group=group, case=case):
                    report = copy.deepcopy(self.report)
                    before, final = report["before"][group], report["final"][group]
                    before.update(Total=200, Max=80, Last=50)
                    final.update(Max=80, Last=50)
                    added_count = final["Count"] - before["Count"]
                    final["Total"] = before["Total"] + (added_count - 1) * 80 + 50
                    validate_report(report, self.options)
                    if case == "upper":
                        final["Total"] += 1
                    else:
                        final.update(Total=350, Max=100, Last=99)
                    with self.assertRaises(ValueError):
                        validate_report(report, self.options)

    def test_queue_ack_latency_counts_calls_not_items(self):
        report = copy.deepcopy(self.report)
        options = dict(self.options, submit_batch_limit=256)
        report["options"] = options
        report["resolved_config"]["Runtime"]["Delivery"]["ReceiveItems"] = 256
        for phase in ("warmup", "measured"):
            stats = report[phase]
            stats["accepted"] *= 256
            stats["acknowledged"] *= 256
            stats["accepted_by_producer"] = [count * 256 for count in stats["accepted_by_producer"]]
            for key in ("receive_batch_sizes", "ack_batch_sizes"):
                stats[key][256], stats[key][1] = stats[key][1], 0
        report["before"]["Outcomes"]["Acks"] *= 256
        report["final"]["Outcomes"]["Acks"] *= 256
        for status in report["inspection"]["Statuses"]:
            status["ID"]["Sequence"] *= 256
            status["RetiredThrough"] *= 256
        report["records_per_second"] *= 256
        for key in ("cpu_ns_per_record", "allocated_bytes_per_record", "allocations_per_record"):
            report[key] /= 256
        validate_report(report, options)



class ProvenanceTest(unittest.TestCase):
    def test_compiler_overrides_stop_before_build(self):
        overrides = dict.fromkeys(provenance.COMPILER_OVERRIDES, "/external/input")
        overrides.update(dict.fromkeys(provenance.COMPILER_DEFAULTS, "-include /external/input.h"))
        overrides.update(CC="gcc -include /external/input.h", CXX="clang++", CGO_ENABLED="0",
                         CGO_CFLAGS_ALLOW=".*", CGO_LDFLAGS_DISALLOW=".*")
        for name, value in overrides.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch.dict(os.environ, {name: value}, clear=True):
                    with patch.object(provenance, "command_output") as command:
                        with self.assertRaisesRegex(ValueError, name):
                            provenance.build_binary(root, root, {})
                        command.assert_not_called()
                self.assertFalse((root / "build").exists())

    def test_default_environment_is_explicit_and_preserves_runtime_settings(self):
        inherited = dict(provenance.COMPILER_DEFAULTS, GOMAXPROCS="2", GOGC="80",
                         PATH="/custom/tool/bin", UNREVIEWED_NATIVE_INPUT="external")
        env = provenance.campaign_build_environment(inherited)
        self.assertEqual(env["CGO_CFLAGS"], "-O2 -g")
        self.assertEqual(env["CGO_CPPFLAGS"], "")
        self.assertEqual(env["CC"], "/usr/bin/gcc")
        self.assertEqual(env["PATH"], "/usr/local/go/bin:/usr/bin:/bin")
        self.assertEqual(env["GOMAXPROCS"], "2")
        self.assertEqual(env["GOGC"], "80")
        self.assertNotIn("UNREVIEWED_NATIVE_INPUT", env)


    def test_source_and_executable_replacement_stop_before_or_after_invocation(self):
        for kind in ("source", "binary"):
            for when in ("before", "after"):
                with self.subTest(kind=kind, when=when), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    binary = root / "owned-binary"
                    binary.write_bytes(b"original")
                    source = root / "input.go"
                    source.write_text("package main")
                    hashes = {str(source): provenance.digest(source)}
                    manifest = {
                        "runs": [], "revision": provenance.source_revision(hashes),
                        "source_sha256": hashes, "binary_sha256": provenance.digest(binary),
                    }
                    target = source if kind == "source" else binary
                    if when == "before":
                        target.write_text("replacement")

                    def invoke(command, stdout, stderr, check):
                        target.write_text("replacement")
                        stdout.write('{"valid": true}')
                        return SimpleNamespace(returncode=0)

                    def sources(*args):
                        return {str(source): provenance.digest(source)}

                    with patch.object(provenance, "source_manifest", side_effect=sources):
                        with patch.object(campaign.subprocess, "run", side_effect=invoke) as run:
                            self.assertFalse(campaign.run_sample(
                                root, root, binary, {}, manifest, (256, 1, 0, "FULL")))
                            self.assertEqual(run.call_count, 0 if when == "before" else 1)
                    self.assertEqual(manifest["runs"][0]["status"], "failed")
                    self.assertIn("changed", manifest["runs"][0]["failure"])
                    self.assertTrue((root / "manifest.json").exists())

    def test_unsupported_filesystem_stops_before_build(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "campaign"
            args = ["campaign.py", "--output", str(output), "--approved-campaign"]
            host = {"output_mount": {"fstype": "tmpfs", "source": "tmpfs"}}
            with patch.object(campaign.sys, "argv", args):
                with patch.object(campaign, "host_environment", return_value=host):
                    with patch.object(campaign, "build_binary") as build:
                        self.assertEqual(campaign.main(), 1)
                        build.assert_not_called()
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertIn("ext4", manifest["failure"])

    def test_build_rejects_source_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "benchmark").mkdir()
            manifest = {}

            def output(command, *args):
                if "-dumpfullversion" in command:
                    return "14.2.0"
                if command[:2] == ["go", "env"]:
                    return '{"CC": "missing-cc", "CXX": "missing-cxx"}'
                return "test tool"

            def build(command, **kwargs):
                Path(command[command.index("-o") + 1]).write_bytes(b"built")
                return SimpleNamespace(returncode=0)

            with patch.object(provenance, "command_output", side_effect=output):
                with patch.object(provenance.shutil, "which", return_value=None) as which:
                    with patch.object(provenance, "source_manifest", side_effect=[
                        {"source": "before"}, {"source": "after"},
                    ]):
                        with patch.object(provenance.subprocess, "run", side_effect=build) as run:
                            with self.assertRaisesRegex(ValueError, "during build"):
                                provenance.build_binary(root, root, manifest)
                            self.assertEqual(run.call_count, 1)
                            self.assertEqual(which.call_count, 3)
                            for call in which.call_args_list:
                                self.assertEqual(call.kwargs["path"],
                                                 "/usr/local/go/bin:/usr/bin:/bin")

    def test_incomplete_output_is_rejected_and_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = {"runs": [], "revision": "sha256:test"}

            def invoke(command, stdout, stderr, check):
                stdout.write('{"valid": true}')
                return SimpleNamespace(returncode=0)

            with patch.object(campaign, "verify_provenance"):
                with patch.object(campaign.subprocess, "run", side_effect=invoke) as run:
                    self.assertFalse(campaign.run_sample(
                        root, root, root / "binary", {}, manifest, (256, 1, 0, "FULL")))
                    self.assertEqual(run.call_count, 1)
            raw = root / "p256-b1-r00-FULL/result.json"
            self.assertEqual(raw.read_text(), '{"valid": true}')
            self.assertEqual(manifest["runs"][0]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
