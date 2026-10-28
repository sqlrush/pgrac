"""Read existing rollover contexts and retain bounded, non-authoritative traces.

The frozen manifest describes an independently built test installation. Its SHA
must come from the caller's frozen evidence, not from this collector's output.
No process, database or VM is started by this module.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


CASES = {4: "A1", 8: "A2"}
GAP_SECONDS = 1
ATTACH_TIMEOUT = 5
OBSERVER_SECONDS = 1200
READS = (
    ("query", "debug_query_string"),
    ("cursor", "cluster_undo_current_extent"),
    ("reservation", "cluster_undo_record_reservation"),
    ("reason", "cluster_undo_receipt_reason"),
    ("pending", "cluster_undo_pending.active"),
    ("shared_segment", "UndoRecordShared->active_segment_id"),
    ("shared_next", "UndoRecordShared->next_extent_block"),
) + tuple((f"handle{index}_{label}",
           f"cluster_undo_record_reservation.receipt.ctrc_handles[{index}].{field}")
          for index in (0, 1)
          for label, field in (("valid", "valid"), ("key", "key"),
                               ("xid", "original_full_xid"),
                               ("generation", "journal_slot_generation")))


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def _bytes(data):
    return data.encode() if isinstance(data, str) else data or b""


class RolloverObserver:
    def __init__(self, base, manifest, manifest_sha256, *, uid=1000,
                 proc_root=Path("/proc"), runner=subprocess.run, clock=time.monotonic):
        self.base = Path(base).resolve()
        self.manifest = Path(manifest).resolve()
        self.manifest_sha = manifest_sha256
        if (not self.manifest.is_relative_to(self.base)
                or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None):
            raise ValueError("frozen manifest identity invalid")
        raw = self.manifest.read_bytes()
        if sha256(raw) != manifest_sha256:
            raise ValueError("frozen manifest SHA mismatch")
        self.identity = json.loads(raw)
        for field in ("source_commit", "source_tree"):
            if re.fullmatch(r"[0-9a-f]{40}", str(self.identity.get(field))) is None:
                raise ValueError("frozen source identity invalid")
        for field in ("source_archive_sha256", "binary_sha256"):
            if re.fullmatch(r"[0-9a-f]{64}", str(self.identity.get(field))) is None:
                raise ValueError("frozen file identity invalid")
        archive = Path(self.identity.get("source_archive", ""))
        if archive.is_absolute() or ".." in archive.parts:
            raise ValueError("source archive must be within the test root")
        self.archive = (self.base / archive).resolve()
        self.binary = self.base / "install/bin/postgres"
        if (not self.archive.is_relative_to(self.base) or self.binary.is_symlink()
                or type(self.identity.get("node_id")) is not int
                or self.identity["node_id"] != 0 or type(uid) is not int or uid < 0):
            raise ValueError("test installation identity invalid")
        self._check_frozen_files()
        self.run = self.base / "native-a08-once"
        self.out = self.base / "rollover-observation"
        # Refuse a restart rather than replenishing uncertain attachment slots.
        self.out.mkdir(exist_ok=False)
        (self.out / "frozen-identity.json").write_bytes(raw)
        self.uid, self.proc_root = uid, Path(proc_root)
        self.runner, self.clock = runner, clock
        self.records, self.targets, self.births = [], {}, set()
        self.attachments = 0
        self.deadline = clock() + OBSERVER_SECONDS

    def _check_frozen_files(self):
        if (sha256(self.manifest.read_bytes()) != self.manifest_sha
                or sha256(self.archive.read_bytes()) != self.identity["source_archive_sha256"]
                or sha256(self.binary.read_bytes()) != self.identity["binary_sha256"]):
            raise ValueError("frozen files changed")

    def _context(self, number):
        raw = (self.run / f"m2-context-{number}.json").read_bytes()
        context = json.loads(raw)
        if (not isinstance(context, dict) or context.get("kind") != CASES[number]
                or type(context.get("number")) is not int or context["number"] != number
                or context.get("caller") != "sample_canonical"
                or context.get("stage") != "rollover"):
            raise ValueError("rollover context unavailable")
        for field in ("xid", "producer_pid", "reader_pid"):
            if type(context.get(field)) is not int or not 0 < context[field] < 2**64:
                raise ValueError("rollover identity invalid")
        if context["producer_pid"] == context["reader_pid"]:
            raise ValueError("producer identity ambiguous")
        return context, sha256(raw)

    def _birth(self, pid):
        self._check_frozen_files()
        process = self.proc_root / str(pid)
        if (process.is_symlink() or process.stat().st_uid != self.uid
                or os.readlink(process / "exe") != str(self.binary)):
            raise ValueError("producer uid/executable mismatch")
        executable, installed = (process / "exe").stat(), self.binary.stat()
        if (executable.st_dev, executable.st_ino) != (installed.st_dev, installed.st_ino):
            raise ValueError("producer executable changed")
        stat = (process / "stat").read_text()
        prefix, fields = stat.rsplit(")", 1)
        fields = fields.split()
        if (not prefix.startswith(str(pid) + " (") or len(fields) < 20
                or not fields[19].isdigit() or int(fields[19]) <= 0):
            raise ValueError("producer start identity unavailable")
        return dict(pid=pid, starttime=int(fields[19]), uid=self.uid,
                    executable_device=installed.st_dev, executable_inode=installed.st_ino,
                    binary_sha256=self.identity["binary_sha256"])

    def _command(self, pid):
        tools = self.base / "debug-tools/root"
        command = [str(tools / "usr/bin/gdb"), "-q", "-nx", "--batch",
                   "--data-directory=" + str(tools / "usr/share/gdb")]
        permissions = ("auto-load", "debuginfod enabled", "pagination", "may-call-functions",
                       "may-write-memory", "may-write-registers", "may-insert-breakpoints",
                       "may-insert-tracepoints", "may-insert-fast-tracepoints")
        for setting in permissions:
            command += ["-iex", "set " + setting + " off"]
        for setting in ("elements 0", "max-depth unlimited", "raw-values on"):
            command += ["-iex", "set print " + setting]
        command += ["-p", str(pid), "-ex",
                    'printf "M2_ROLL_PID=%d NODE=%d XID=%u\\n", MyProcPid, cluster_node_id, MyProc->xid']
        for name, expression in READS:
            command += ["-ex", "echo M2_ROLL_FIELD=" + name + "\\n", "-ex", "p " + expression]
        command += ["-ex", "echo M2_ROLL_FIELD=stack\\n", "-ex", "thread apply all bt full",
                    "-ex", "detach"]
        return command

    @staticmethod
    def _complete_raw(raw, context):
        text = raw.decode(errors="replace")
        native = re.findall(r"^M2_ROLL_PID=(\d+) NODE=(\d+) XID=(\d+)$", text, re.M)
        if native != [(str(context["producer_pid"]), "0", str(context["xid"] & 0xffffffff))]:
            return False
        fields = re.findall(r"^M2_ROLL_FIELD=(\w+)$", text, re.M)
        if fields != [name for name, _ in READS] + ["stack"]:
            return False
        query = text.split("M2_ROLL_FIELD=query\n", 1)[1].split("M2_ROLL_FIELD=", 1)[0]
        return ("UPDATE m2_roll SET v=v+1" in query and re.search(r"^#0\s", text, re.M)
                and not re.search(r"No symbol|optimized out|Cannot access memory|No stack|"
                                  r"Undefined (?:command|set)|not available|<incomplete type>|\.{3}", text))

    def _save(self, record):
        path = self.out / f"case-{record['case']}-point-{record['point']}.json"
        path.write_text(json.dumps(record, indent=2) + "\n")

    def _capture(self, number, target):
        target["points"] += 1
        point = target["points"]
        started = self.clock()
        record = dict(case=number, point=point, context=target["context"],
                      context_sha256=target["context_sha256"], birth=target["birth"],
                      frozen_manifest_sha256=self.manifest_sha, source=dict(self.identity),
                      monotonic_started=started, status="NOT_CAPTURED_IDENTITY",
                      attach_attempted=False, diagnostic_authority=False,
                      receipt_identity="UNKNOWN_UNINTERPRETED", not_a_performance_sample=True)
        self.records.append(record)
        self._save(record)
        raw = b""
        try:
            context, digest = self._context(number)
            if digest != target["context_sha256"]:
                raise ValueError("rollover context changed")
        except (OSError, ValueError) as error:
            record.update(status="NOT_CAPTURED_CONTEXT", error=str(error))
        else:
            try:
                if target["birth"] is None or self._birth(context["producer_pid"]) != target["birth"]:
                    raise ValueError("producer start identity changed or unbound")
                if self.attachments >= 4:
                    raise ValueError("attachment budget consumed")
            except (OSError, ValueError) as error:
                record["error"] = str(error)
            else:
                self.attachments += 1
                command = self._command(context["producer_pid"])
                record.update(status="ATTACH_CHARGED_UNKNOWN", attach_attempted=True,
                              command=command, attach_started=self.clock())
                self._save(record)  # Charge before any debugger execution.
                environment = dict(os.environ)
                environment.pop("LD_PRELOAD", None)
                environment["LD_LIBRARY_PATH"] = str(self.base / "debug-tools/root/usr/lib/aarch64-linux-gnu")
                try:
                    result = self.runner(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, timeout=ATTACH_TIMEOUT, env=environment)
                    raw = _bytes(result.stdout)
                    record.update(returncode=result.returncode, status="PARTIAL_UNKNOWN")
                    try:
                        _, current_digest = self._context(number)
                        same = (current_digest == digest
                                and self._birth(context["producer_pid"]) == target["birth"])
                    except (OSError, ValueError):
                        same = False
                    if not same:
                        record["status"] = "IDENTITY_CHANGED_UNKNOWN"
                    elif result.returncode == 0 and self._complete_raw(raw, context):
                        record["status"] = "RAW_CAPTURED"
                except subprocess.TimeoutExpired as error:
                    raw = _bytes(error.output)
                    record.update(status="TIMEOUT_DETACH_UNKNOWN", error=str(error), returncode=124)
                except OSError as error:
                    record.update(status="ATTACH_FAILED_UNKNOWN", error=str(error))
                record["attach_elapsed_seconds"] = self.clock() - record["attach_started"]
                (self.out / f"case-{number}-point-{point}.log").write_bytes(raw)
        record["observation_elapsed_seconds"] = self.clock() - started
        target["next_due"] = self.clock() + GAP_SECONDS
        self._save(record)

    def step(self):
        if self.clock() >= self.deadline or (self.run / "controller-result.json").exists():
            return []
        before = len(self.records)
        for number in CASES:
            if number not in self.targets:
                try:
                    context, digest = self._context(number)
                except (OSError, ValueError):
                    continue
                try:
                    birth = self._birth(context["producer_pid"])
                    key = (birth["pid"], birth["starttime"])
                    if key in self.births:
                        birth = None
                    else:
                        self.births.add(key)
                except (OSError, ValueError):
                    birth = None
                self.targets[number] = dict(context=context, context_sha256=digest, birth=birth,
                                            points=0, next_due=self.clock())
            target = self.targets[number]
            if target["points"] < 2 and self.clock() >= target["next_due"]:
                self._capture(number, target)
        return self.records[before:]

    def summary(self):
        return dict(attachments=self.attachments, maximum_attachments=4,
                    records=self.records, cases_without_rollover_context=sorted(set(CASES) - self.targets.keys()),
                    diagnostic_authority=False, not_a_performance_sample=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--uid", type=int, default=1000)
    options = parser.parse_args()
    observer = RolloverObserver(options.base, options.manifest, options.manifest_sha256, uid=options.uid)
    while time.monotonic() < observer.deadline and not (observer.run / "controller-result.json").exists():
        observer.step()
        time.sleep(0.05)
    (observer.out / "summary.json").write_text(json.dumps(observer.summary(), indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
