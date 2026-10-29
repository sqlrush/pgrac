"""Synthetic process/debugger services only; never attach to a real backend."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from m2_rollover_observer import RolloverObserver


FIELDS = ("query", "cursor", "reservation", "reason", "pending", "shared_segment",
          "shared_next", "handle0_valid", "handle0_key", "handle0_xid", "handle0_generation",
          "handle1_valid", "handle1_key", "handle1_xid", "handle1_generation", "stack")


def digest(data):
    return hashlib.sha256(data).hexdigest()


class RolloverObservation(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.run = self.base / "native-a08-once"
        self.run.mkdir()
        self.proc = self.base / "synthetic-proc"
        self.proc.mkdir()
        self.binary = self.base / "install/bin/postgres"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(b"synthetic binary, never executed")
        (self.base / "source.tar").write_bytes(b"synthetic frozen source archive")
        self.identity = dict(source_commit="1" * 40, source_tree="2" * 40,
                             source_archive="source.tar",
                             source_archive_sha256=digest((self.base / "source.tar").read_bytes()),
                             binary_sha256=digest(self.binary.read_bytes()), node_id=0)
        self.manifest = self.base / "frozen-identity.json"
        self.now = 0.0
        self.calls = []
        self.result_code = 0
        self.error = None
        self.mutate_after = None
        self.output_override = None
        self.target(4, 1011, 101, 111)
        self.target(8, 1012, 102, 222)

    def target(self, number, pid, xid, start):
        process = self.proc / str(pid)
        process.mkdir(exist_ok=True)
        link = process / "exe"
        if not link.exists():
            link.symlink_to(self.binary)
        self.birth(pid, start)
        context = dict(number=number, kind="A1" if number == 4 else "A2",
                       caller="sample_canonical", stage="rollover", xid=xid,
                       producer_pid=pid, reader_pid=pid + 100)
        (self.run / f"m2-context-{number}.json").write_text(json.dumps(context))
        return context

    def birth(self, pid, start):
        # Linux fields3..21 precede starttime22, including a comm with spaces/parentheses.
        (self.proc / str(pid) / "stat").write_text(
            f"{pid} (postgres writer (test)) S " + "0 " * 18 + f"{start} 0\n")

    def observer(self, **changes):
        identity = dict(self.identity, **changes)
        self.manifest.write_text(json.dumps(identity))
        return RolloverObserver(self.base, self.manifest, digest(self.manifest.read_bytes()),
                                uid=os.getuid(), proc_root=self.proc, runner=self.debugger,
                                clock=lambda: self.now)

    def debugger(self, command, **kwargs):
        self.calls.append((command, kwargs))
        pid = int(command[command.index("-p") + 1])
        xid = 101 if pid == 1011 else 102
        output = f"M2_ROLL_PID={pid} NODE=0 XID={xid}\n"
        for field in FIELDS:
            value = ('0x1 "UPDATE m2_roll SET v=v+1"' if field == "query" else
                     "#0 rollover_fixture ()" if field == "stack" else "{fixture value}")
            output += f"M2_ROLL_FIELD={field}\n{value}\n"
        if self.mutate_after:
            self.mutate_after()
        self.now += 0.1
        if self.error:
            raise self.error
        return subprocess.CompletedProcess(command, self.result_code,
                                           (self.output_override or output).encode())

    def test_rollover_context_captures_before_any_locator_or_copy(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.records[0]["status"], "RAW_CAPTURED")
        self.assertEqual(observer.records[0]["birth"]["starttime"], 111)
        self.assertEqual(observer.records[0]["context_sha256"],
                         digest((self.run / "m2-context-4.json").read_bytes()))
        self.assertFalse(observer.records[0]["diagnostic_authority"])
        self.assertEqual(observer.records[0]["receipt_identity"], "UNKNOWN_UNINTERPRETED")
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.now += 1
        observer.step()
        self.assertEqual(len(self.calls), 2)

    def test_two_targets_two_points_are_a_hard_bound_and_restart_refuses(self):
        observer = self.observer()
        observer.step()
        self.now += 1
        for _ in range(10):
            observer.step()
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(len(observer.records), 4)
        with self.assertRaises(FileExistsError):
            self.observer()

    def test_malformed_contexts_never_reach_the_debugger(self):
        for number in (4, 8):
            context = json.loads((self.run / f"m2-context-{number}.json").read_text())
            context["producer_pid"] = "1011; call unsafe()"
            (self.run / f"m2-context-{number}.json").write_text(json.dumps(context))
        observer = self.observer()
        observer.step()
        self.assertEqual(self.calls, [])

    def test_phase_and_context_changes_cannot_be_called_rollover_samples(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        observer.step()
        path = self.run / "m2-context-4.json"
        context = json.loads(path.read_text())
        context["stage"] = "locator_prepare"
        path.write_text(json.dumps(context))
        self.now += 1
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.records[-1]["status"], "NOT_CAPTURED_CONTEXT")

    def test_pid_reuse_does_not_adopt_a_new_process(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        observer.step()
        self.birth(1011, 999)
        self.now += 1
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.records[-1]["status"], "NOT_CAPTURED_IDENTITY")

    def test_post_attach_identity_change_keeps_raw_but_never_claims_capture(self):
        (self.run / "m2-context-8.json").unlink()
        self.mutate_after = lambda: self.birth(1011, 999)
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.records[0]["status"], "IDENTITY_CHANGED_UNKNOWN")
        self.assertTrue((observer.out / "case-4-point-1.log").is_file())

    def test_wrong_executable_and_missing_process_are_unknown_without_attach(self):
        (self.proc / "1011/exe").unlink()
        (self.proc / "1011/exe").symlink_to(self.base / "not-postgres")
        (self.proc / "1012/stat").unlink()
        observer = self.observer()
        observer.step()
        self.assertEqual(self.calls, [])
        self.assertTrue(all(r["status"] == "NOT_CAPTURED_IDENTITY" for r in observer.records))

    def test_missing_symbols_and_wrong_native_identity_remain_unknown(self):
        self.output_override = 'M2_ROLL_PID=1011 NODE=1 XID=999\nNo symbol "receipt"\n'
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(r["status"] == "PARTIAL_UNKNOWN" for r in observer.records))

    def test_debugger_timeout_is_recorded_and_not_retried_at_same_point(self):
        self.error = subprocess.TimeoutExpired("synthetic gdb", 5, output=b"partial trace")
        observer = self.observer()
        observer.step()
        observer.step()
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(r["status"] == "TIMEOUT_DETACH_UNKNOWN" for r in observer.records))
        self.assertEqual((observer.out / "case-4-point-1.log").read_bytes(), b"partial trace")

    def test_untrusted_manifest_and_changed_binary_are_rejected(self):
        self.manifest.write_text(json.dumps(self.identity))
        with self.assertRaises(ValueError):
            RolloverObserver(self.base, self.manifest, "0" * 64, proc_root=self.proc)
        with self.assertRaises(ValueError):
            self.observer(source_archive="../outside.tar")
        self.binary.write_bytes(b"replacement")
        with self.assertRaises(ValueError):
            self.observer()
        self.assertEqual(self.calls, [])

    def test_controller_completion_stops_without_any_attach(self):
        (self.run / "controller-result.json").write_text("{}")
        observer = self.observer()
        observer.step()
        self.assertEqual(self.calls, [])

    def test_same_process_cannot_receive_four_points_via_two_case_numbers(self):
        self.target(8, 1011, 101, 111)
        observer = self.observer()
        observer.step()
        self.now += 1
        observer.step()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual([int(c[0][c[0].index("-p") + 1]) for c in self.calls], [1011, 1011])

    def test_non_rollover_query_is_not_promoted_by_context_alone(self):
        self.output_override = 'M2_ROLL_PID=1011 NODE=0 XID=101\nSELECT 1\n#0 fixture ()\n'
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(r["status"] == "PARTIAL_UNKNOWN" for r in observer.records))

    def test_commands_only_read_and_keep_full_receipt_fields(self):
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 2)
        command, kwargs = self.calls[0]
        commands = [command[i + 1] for i, word in enumerate(command) if word in ("-ex", "-iex")]
        for permission in ("may-call-functions", "may-write-memory", "may-write-registers",
                           "may-insert-breakpoints", "may-insert-tracepoints",
                           "may-insert-fast-tracepoints"):
            self.assertIn("set " + permission + " off", commands)
        self.assertIn("set print elements 0", commands)
        self.assertIn("set print max-depth unlimited", commands)
        self.assertIn("set print raw-values on", commands)
        for index in (0, 1):
            for field in ("key", "valid", "original_full_xid", "journal_slot_generation"):
                self.assertIn(f"p cluster_undo_record_reservation.receipt.ctrc_handles[{index}].{field}", commands)
        self.assertIn("p cluster_undo_record_reservation", commands)
        self.assertEqual(commands[-1], "detach")
        self.assertFalse(any(c.startswith(("call ", "signal ", "break ", "shell ", "python ")) for c in commands))
        self.assertFalse(any("->participant" in c or "->receipt" in c for c in commands))
        self.assertEqual(kwargs["timeout"], 5)

    def test_controller_completion_during_first_point_prevents_second_attach(self):
        self.mutate_after = lambda: (self.run / "controller-result.json").write_text("{}")
        observer = self.observer()
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.attachments, 1)

    def test_deadline_during_first_point_prevents_second_attach(self):
        observer = self.observer()
        observer.deadline = 0.05
        observer.step()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(observer.attachments, 1)

    def test_controller_completion_during_identity_check_prevents_charge(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        original_birth = observer._birth
        checks = []

        def birth(pid):
            value = original_birth(pid)
            checks.append(pid)
            if len(checks) == 2:
                (self.run / "controller-result.json").write_text("{}")
            return value

        observer._birth = birth
        observer.step()
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(observer.attachments, 0)
        self.assertEqual(observer.records[0]["status"], "NOT_CAPTURED_STOP")
        self.assertFalse(observer.records[0]["attach_attempted"])

    def test_controller_completion_after_charge_prevents_launch(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        original_save = observer._save

        def save(record):
            original_save(record)
            if record["status"] == "ATTACH_CHARGED_UNKNOWN":
                (self.run / "controller-result.json").write_text("{}")

        observer._save = save
        observer.step()
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(observer.attachments, 1)
        self.assertEqual(observer.records[0]["status"], "NOT_CAPTURED_STOP")
        self.assertFalse(observer.records[0]["attach_attempted"])

    def test_deadline_after_charge_prevents_launch(self):
        (self.run / "m2-context-8.json").unlink()
        observer = self.observer()
        original_save = observer._save

        def save(record):
            original_save(record)
            if record["status"] == "ATTACH_CHARGED_UNKNOWN":
                self.now = observer.deadline

        observer._save = save
        observer.step()
        self.assertEqual(len(self.calls), 0)
        self.assertEqual(observer.attachments, 1)
        self.assertEqual(observer.records[0]["status"], "NOT_CAPTURED_STOP")
        self.assertFalse(observer.records[0]["attach_attempted"])


if __name__ == "__main__":
    unittest.main()
