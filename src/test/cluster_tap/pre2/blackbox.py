#!/usr/bin/env python3
"""ClusterPRE2 command adapter. Never writes database/control/voting bytes.

The entry file maps *actual* product commands/SQL. Missing entries fail before
starting any server. Only test evidence and the input layout are written here.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


class Dependency(RuntimeError):
    pass


class BlackBox:
    def __init__(self, layout, bindir, profile=None, runner=subprocess.run):
        self.layout, self.bindir, self.profile = layout, Path(bindir), profile
        self.runner = runner
        self.root = Path(layout.get("root", os.environ.get("TESTLOGDIR", ".")))
        self.state_path = self.root / "blackbox-state.json"
        self.log = Path(os.environ.get("TESTLOGDIR", str(self.root))) / "pre2-blackbox.jsonl"
        self.deadline = None

    def remaining(self):
        left = self.deadline - time.monotonic() if self.deadline is not None else 120
        if left <= 0:
            raise RuntimeError("PRE2 black-box operation deadline expired")
        return left

    def phase_budget(self, seconds):
        self.deadline = time.monotonic() + min(seconds, self.remaining())

    def record(self, record):
        self.log.parent.mkdir(parents=True, exist_ok=True)
        with self.log.open("a") as output:
            output.write(json.dumps(record, sort_keys=True) + "\n")

    def command(self, tool, argv, *, input=None, check=True, seconds=60):
        binary = self.bindir / tool
        env = dict(os.environ, LC_ALL="C", PGCONNECT_TIMEOUT="3")
        # pgrac-init resolves these tools via environment before PATH.
        for name in ("INITDB", "PG_CTL", "PG_BASEBACKUP", "PG_CONTROLDATA"):
            env[name] = str(self.bindir / name.lower())
        env["PATH"] = str(self.bindir) + os.pathsep + env.get("PATH", "")
        try:
            result = self.runner([str(binary), *argv], input=input, text=True,
                                 capture_output=True, timeout=min(seconds, self.remaining()), env=env)
        except subprocess.TimeoutExpired as error:
            self.record(dict(tool=tool, argv=argv, timed_out=True,
                             stdout=str(error.stdout), stderr=str(error.stderr)))
            raise RuntimeError(f"{tool}: command deadline expired") from error
        self.record(dict(tool=tool, argv=argv, rc=result.returncode,
                         stdout=result.stdout, stderr=result.stderr))
        if check and result.returncode != 0:
            raise RuntimeError(f"{tool} rc={result.returncode}: {result.stderr or result.stdout}")
        self.remaining()  # A late success is still a deadline failure.
        return result

    def validate(self):
        profile = self.profile
        if not profile or not profile.get("initialize"):
            raise Dependency("A S11/S12: supported fresh PRE2 member initialization and atomic "
                             "format activation are absent (PGRAC_PRE2_ENTRY_FILE); "
                             "legacy backup-based --cluster-join is not a replacement")
        if profile.get("version") != 1:
            raise ValueError("unsupported test entry file version")
        seeds = 0
        cohorts = 0
        for entry in profile["initialize"]:
            if set(entry) != {"tool", "argv"} or entry["tool"] not in ("pgrac-init", "initdb"):
                raise ValueError("fresh initialization must use a supported product creator")
            argv = entry["argv"]
            self.validate_argv(argv)
            if entry["tool"] == "initdb":
                cohorts += argv.count("--pgrac-initdb-cohort")
                if argv.count("--pgrac-initdb-cohort") != 1 or sum(
                        a.startswith("--pgrac-initdb-shared-config=") for a in argv) != 1:
                    raise ValueError("initdb requires the native cohort and canonical request")
            else:
                seeds += argv.count("--cluster-seed")
            if any(a.split("=")[0] in ("--force", "--join-from", "--join-from-backup") for a in argv):
                raise ValueError("no overwrite or backup-based PRE2 fresh initializer")
        if not ((seeds == 1 and cohorts == 0) or
                (seeds == 0 and cohorts == 1 and len(profile["initialize"]) == 1)):
            raise ValueError("one and only one fresh cluster creator is required")
        for entry in profile.get("operations", {}).values():
            if set(entry) == {"node", "sql"} and isinstance(entry["sql"], str) and entry["sql"].strip():
                continue
            if set(entry) == {"tool", "argv"} and entry["tool"] in ("pgrac", "pgrac-ctl"):
                self.validate_argv(entry["argv"])
                continue
            raise ValueError("operation must query SQL or a product CLI, not supply a result")

    @staticmethod
    def validate_argv(argv):
        if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
            raise ValueError("entry argv must be a nonempty string array")

    def describe(self):
        # Run even when no entry file exists: retain the actual installed CLI.
        tool = "initdb" if self.profile and any(e.get("tool") == "initdb"
               for e in self.profile.get("initialize", [])) else "pgrac-init"
        self.command(tool, ["--help"])
        self.validate()
        return dict(capabilities=["fresh_init", "shared_start", "shared_stop",
                                  *self.profile.get("operations", {})])

    def expand(self, argv, args):
        values = {key: str(value) for key, value in self.layout.items()
                  if isinstance(value, (str, int))}
        values["layout_file"] = str(self.root / "blackbox-layout.json")
        values["args_json"] = json.dumps(args, sort_keys=True)
        for node in self.layout.get("nodes", []):
            for key, value in node.items():
                values[f"node{node['id']}_{key}"] = str(value)
        def replace(match):
            if match[1] not in values:
                raise ValueError("unknown placeholder: " + match[1])
            return values[match[1]]
        return [re.sub(r"\$\{([^}]+)\}", replace, arg) for arg in argv]

    def fresh_init(self):
        self.validate()
        for node in self.layout["nodes"]:
            data = Path(node["data_dir"])
            if data.is_symlink():
                raise ValueError("fresh DATA is a symlink: " + str(data))
            if data.exists() and any(data.iterdir()):
                raise ValueError("fresh DATA is not empty: " + str(data))
        for key in ("shared_data_dir", "wal_root"):
            path = Path(self.layout[key]) if key in self.layout else None
            if path and path.exists() and any(path.iterdir()):
                raise ValueError("fresh shared path is not empty: " + str(path))
        for path in self.layout.get("voting_disks", []):
            if os.path.lexists(path):
                raise ValueError("voting path already exists: " + path)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "blackbox-layout.json").write_text(json.dumps(self.layout, indent=2) + "\n")
        allowed = [self.root.resolve(), *(Path(n["data_dir"]).resolve() for n in self.layout["nodes"])]
        commands = []
        for entry in self.profile["initialize"]:
            argv = self.expand(entry["argv"], {})
            for arg in argv:
                value = arg.split("=", 1)[-1]
                if value.startswith("/"):
                    path = Path(value).resolve()
                    if not any(path == root or root in path.parents for root in allowed):
                        raise ValueError("initializer path is outside this disposable fixture: " + value)
            commands.append((entry["tool"], argv))
        self.prepare_initialize()
        for tool, argv in commands:
            self.command(tool, argv)
        self.finish_initialize()
        for node in self.layout["nodes"]:
            data = Path(node["data_dir"])
            if not (data / "global/pg_control").is_file() or (data / "backup_label").exists():
                raise ValueError("initializer did not create fresh native PRE2 DATA")
        return {}

    def prepare_initialize(self):
        pass

    def finish_initialize(self):
        pass

    def sql(self, node, sql, args=None, check=True):
        argv = ["-XAtq", "-v", "ON_ERROR_STOP=1", "-h", node["host"], "-p", str(node["port"]),
                "-d", "postgres", "-v", "pre2_args=" + json.dumps(args or {}, sort_keys=True), "-f", "-"]
        return self.command("psql", argv, input=sql, check=check, seconds=10)

    def observe(self, op, args):
        entry = self.profile["operations"][op]
        if "sql" in entry:
            node_id = args.get("node", 0) if entry["node"] == "argument" else entry["node"]
            if type(node_id) is not int or not 0 <= node_id < len(self.layout["nodes"]):
                raise ValueError("invalid observation node")
            result = self.sql(self.layout["nodes"][node_id], entry["sql"], args)
        else:
            result = self.command(entry["tool"], self.expand(entry["argv"], args))
        reply = json.loads(result.stdout)
        if not isinstance(reply, dict) or any(k in reply for k in ("status", "version", "verdict")):
            raise ValueError("observation must be product fields, not an adapter verdict")
        return reply

    def shared_start(self):
        self.phase_budget(60)
        state = {"log_offsets": {}, "starts": {}}
        for node in self.layout["nodes"]:
            log = Path(node["logfile"])
            state["log_offsets"][str(node["id"])] = log.stat().st_size if log.exists() else 0
        self.state_path.write_text(json.dumps(state))
        # Start every member before waiting; peer-dependent startup cannot be serialized.
        for node in self.layout["nodes"]:
            self.command("pg_ctl", ["start", "-D", node["data_dir"], "-l", node["logfile"], "-W"])
        pending = list(self.layout["nodes"])
        while pending:
            self.check_start_logs(state)
            pending = [n for n in pending if self.sql(n, "SELECT 1", check=False).returncode != 0]
            self.remaining()
            self.check_start_logs(state)
            if pending:
                time.sleep(0.1)
        return {}

    def check_start_logs(self, state):
        for node in self.layout['nodes']:
            path = Path(node['logfile'])
            if not path.exists():
                continue
            with path.open('rb') as log:
                log.seek(state['log_offsets'][str(node['id'])])
                text = log.read().decode(errors='replace')
            error = next((line for line in text.splitlines() if re.search(
                r'FATAL:|PANIC:|Assertion|terminated by signal|abnormal database system shutdown|'
                r'reinitializing|server process .*exited with exit code [1-9]', line)), None)
            if error:
                raise RuntimeError(f'node{node["id"]} startup failed: {error}')

    def is_stopped(self, node):
        result = self.command("pg_ctl", ["status", "-D", node["data_dir"]], check=False)
        if result.returncode not in (0, 3):
            raise RuntimeError("unexpected pg_ctl status: " + str(result.returncode))
        return result.returncode == 3

    @staticmethod
    def processes():
        result = subprocess.run(["ps", "-axo", "pid=,ppid=,lstart=,comm="],
                                text=True, capture_output=True, check=True, timeout=5)
        return {int(parts[0]): (int(parts[1]), parts[2])
                for line in result.stdout.splitlines() if len(parts := line.split(None, 2)) == 3}

    def shared_stop(self, args):
        self.phase_budget(60)
        ids = args.get("nodes", [n["id"] for n in self.layout["nodes"]])
        nodes = [n for n in self.layout["nodes"] if n["id"] in ids]
        if not nodes or len(nodes) != len(ids):
            raise ValueError("invalid shutdown member set")
        state = json.loads(self.state_path.read_text())
        process_table = self.processes()
        owned = {}
        for node in nodes:
            if self.is_stopped(node):
                raise RuntimeError("node already stopped before normal shutdown")
            pid = int((Path(node["data_dir"]) / "postmaster.pid").read_text().splitlines()[0])
            if pid not in process_table:
                raise RuntimeError("postmaster missing from process inventory")
            owned[pid] = process_table[pid][1]
        while True:
            more = {pid: info[1] for pid, info in process_table.items() if info[0] in owned and pid not in owned}
            if not more:
                break
            owned.update(more)
        self.record(dict(before_normal_stop=owned, nodes=ids))
        for node in nodes:
            self.command("pg_ctl", ["stop", "-D", node["data_dir"], "-m", "fast", "-W"])
        while True:
            current = self.processes()
            survivors = [pid for pid, identity in owned.items()
                         if pid in current and current[pid][1] == identity]
            stopped = all(self.is_stopped(node) for node in nodes)
            self.remaining()
            if stopped and not survivors:
                break
            time.sleep(0.1)
        for node in nodes:
            with Path(node["logfile"]).open("rb") as log:
                log.seek(state["log_offsets"][str(node["id"])])
                tail = log.read().decode(errors="replace")
            if re.search(r"PANIC:|terminated by signal|abnormal database system shutdown|"
                         r"reinitializing|server process .*exited with exit code [1-9]", tail):
                raise RuntimeError("abnormal process exit in node log: " + node["logfile"])
            if "database system is shut down" not in tail:
                raise RuntimeError("missing normal shutdown record: " + node["logfile"])
        self.record(dict(normal_stop_complete=True, exited_pids=sorted(owned)))
        self.remaining()
        return dict(all_processes_exited=True)


def main():
    request = json.load(sys.stdin)
    op = request["op"]
    if request.get("version") != 1:
        raise ValueError("unknown adapter protocol")
    bindir = os.environ.get("PGRAC_PRE2_BINDIR")
    if not bindir:
        init = shutil.which("pgrac-init")
        if not init:
            raise Dependency("installed pgrac-init is unavailable")
        bindir = str(Path(init).parent)
    profile_path = os.environ.get("PGRAC_PRE2_ENTRY_FILE")
    profile = json.loads(Path(profile_path).read_text()) if profile_path else None
    driver_class = BlackBox
    if profile and profile.get('fixture', {}).get('kind') == 'local-cohort-v1':
        from cohort_entry import CohortEntry
        driver_class = CohortEntry
    driver = driver_class(request.get("layout") or {}, bindir, profile)
    driver.phase_budget(float(request.get("budget_seconds", 120)))
    if op == "describe":
        identities = {}
        for tool in ("postgres", "pgrac-init", "pg_ctl", "psql"):
            path = Path(bindir) / tool
            identities[tool] = hashlib.sha256(path.read_bytes()).hexdigest()
        driver.record(dict(installed_binaries=identities, entry_file=profile_path,
                           entry_sha256=hashlib.sha256(Path(profile_path).read_bytes()).hexdigest()
                           if profile_path else None))
        reply = driver.describe()
    elif op in ("fresh_init", "shared_start"):
        reply = getattr(driver, op)()
    elif op == "shared_stop":
        reply = driver.shared_stop(request.get("args", {}))
    else:
        driver.validate()
        reply = driver.observe(op, request.get("args", {}))
    print(json.dumps(dict(version=1, status="OK", **reply)))


if __name__ == "__main__":
    try:
        main()
    except Dependency as error:
        print(json.dumps(dict(version=1, status="BLOCKED", dependency=str(error))))
    except Exception as error:
        print(json.dumps(dict(version=1, status="ERROR", reason=str(error))))
