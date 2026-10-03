#!/usr/bin/env python3
"""Run fresh shared acceptance cases. BLOCKED exits nonzero and is never PASS."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from contract import check_wal, check_reset, check_membership

_member_spec = importlib.util.spec_from_file_location(
    "pre2_membership_cli", Path(__file__).resolve().parents[4] / "scripts/deploy/pre2/membership.py")
membership_cli = importlib.util.module_from_spec(_member_spec)
_member_spec.loader.exec_module(membership_cli)


def identity_key(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def accepted_reply(reply, request):
    membership_cli.validate_response(reply, "execute", request)
    assert reply["status"] in ("accepted", "retry"), "member execution was not accepted"


class Blocked(Exception):
    pass


BASE = ("fresh_init", "shared_start", "shared_stop", "root_observation")
CASES = {
    "FRESH-2N": (2, ()),
    "FRESH-4N": (4, ()),
    "W06-REDIRTY": (3, ("wal_observation", "current_handoff", "wal_recycle")),
    "W06-CHECKPOINT-HANDOFF": (3, ("wal_observation", "current_handoff", "checkpoint_barrier", "wal_recycle")),
    "W06-HOLDER-EIO": (3, ("wal_observation", "current_handoff", "holder_write_eio", "wal_recycle")),
    "W06-CONTRIBUTOR-CRASH": (3, ("wal_observation", "current_handoff", "terminal_fence", "recovery_observation", "wal_recycle")),
    "RD-DDL-UNCOMMITTED": (3, ("ddl_barrier", "terminal_fence", "recovery_observation", "persistent_reader")),
    "RD-DDL-COMMITTED": (3, ("ddl_barrier", "terminal_fence", "recovery_observation", "persistent_reader")),
    "RD-OID-REPLAY": (3, ("terminal_fence", "oid_candidate", "replay_window", "repeat_drop_replay")),
    "RD-DROP-DATABASE": (2, ()),
    "RD-BACKUP-LABEL": (2, ("backup_refusal_target",)),
    "MEM-LEAVE-FAILURE": (3, ("membership_owner", "leave_barrier", "terminal_fence", "pi_retirement", "wal_observation", "recovery_deadline")),
    "MEM-COMMIT-PEER": (3, ("membership_owner", "leave_barrier", "terminal_fence", "pi_retirement", "wal_observation", "recovery_deadline")),
    "MEM-REMOVE-REJOIN": (3, ("membership_owner", "terminal_fence", "pi_retirement", "wal_observation", "rejoin_admission", "xid_era_observation")),
}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


class Driver:
    def __init__(self, executable, transcript):
        if not executable or not Path(executable).is_file() or not os.access(executable, os.X_OK):
            raise Blocked("A fresh initialization/format activation adapter is unavailable")
        self.executable, self.transcript = str(Path(executable).resolve()), transcript

    def call(self, op, layout=None, **args):
        request = dict(version=1, op=op, layout=layout, args=args)
        result = subprocess.run([self.executable], input=json.dumps(request), text=True,
                                capture_output=True, timeout=120)
        with self.transcript.open("a") as log:
            log.write(json.dumps(dict(request=request, rc=result.returncode,
                                      stdout=result.stdout, stderr=result.stderr)) + "\n")
        assert result.returncode == 0, f"adapter {op} failed: {result.stderr}"
        reply = json.loads(result.stdout)
        assert reply.get("version") == 1, "incompatible test adapter"
        if op == "describe" and reply.get("status") == "BLOCKED":
            raise Blocked(reply["dependency"])
        assert reply.get("status") == "OK", f"adapter {op}: {reply}"
        return reply


class Fixture:
    def __init__(self, driver, root, count, bindir):
        self.driver, self.root, self.bindir = driver, root, bindir
        self.started = False
        self.sockets = []
        nodes = []
        for i in range(count):
            ports = []
            for _ in range(3):
                s = socket.socket()
                s.bind(("127.0.0.1", 0))
                self.sockets.append(s)
                ports.append(s.getsockname()[1])
            nodes.append(dict(id=i, data_dir=str(root / f"node{i}"), host="127.0.0.1",
                              port=ports[0], ic_port=ports[1], data_port=ports[2],
                              logfile=str(root / f"node{i}.log")))
        self.layout = dict(name=root.name.replace("-", "_"), root=str(root), nodes=nodes,
                           shared_data_dir=str(root / "shared"), wal_root=str(root / "wal"),
                           voting_disks=[str(root / f"vote{i}") for i in range(3)],
                           extra_conf=["autovacuum=off", "cluster.lms_workers=1"])

    def call(self, op, **args):
        return self.driver.call(op, self.layout, **args)

    def init(self):
        for node in self.layout["nodes"]:
            assert not Path(node["data_dir"]).exists(), "not a fresh PGDATA"
        self.call("fresh_init")
        for s in self.sockets:
            s.close()
        self.sockets = []
        self.started = True  # cleanup also covers partial product startup
        self.call("shared_start")
        identities = []
        for i, node in enumerate(self.layout["nodes"]):
            assert not (Path(node["data_dir"]) / "backup_label").exists(), "backup clone used as fresh node"
            value = self.sql(i, "SELECT system_identifier::text||'|'||catalog_version_no FROM pg_control_system()")
            sysid, catalog = value.split("|")
            assert int(sysid) > 0 and catalog == "202609290", "PRE2 format activation is not complete"
            identities.append(sysid)
            assert self.sql(i, "SELECT current_setting('cluster.enabled')||'|'||"
                            "current_setting('cluster.shared_catalog')||'|'||current_setting('cluster.shared_config')") == "on|on|on"
            assert self.sql(i, "SHOW cluster.node_id") == str(i), "wrong node identity"
        assert len(set(identities)) == 1, "different database identities"
        self.identity = self.call("root_observation")
        assert self.identity["system_identifier"] == identities[0]
        assert re.fullmatch(r"[0-9a-f]{64}", self.identity["root_digest"]) and self.identity["generation"] > 0
        self.identity["postgres_sha256"] = digest(self.bindir / "postgres")
        self.identity["adapter_sha256"] = digest(self.driver.executable)
        self.identity["test_source_commit"] = subprocess.check_output(
            ["git", "-C", str(Path(__file__).resolve().parents[4]), "rev-parse", "HEAD"], text=True).strip()
        self.identity["runner_sha256"] = digest(__file__)
        self.identity["config_sha256"] = [digest(Path(n["data_dir"]) / "postgresql.conf")
                                           for n in self.layout["nodes"]]
        (self.root / "identity.json").write_text(json.dumps(self.identity, indent=2) + "\n")

    def command(self, node, db="postgres"):
        n = self.layout["nodes"][node]
        return [str(self.bindir / "psql"), "-X", "-qAtw", "-v", "ON_ERROR_STOP=1",
                "-v", "VERBOSITY=verbose", "-h", n["host"], "-p", str(n["port"]), "-d", db]

    def result(self, node, query, db="postgres"):
        result = subprocess.run(self.command(node, db), input=query, text=True,
                                capture_output=True, timeout=120)
        with (self.root / "sql.jsonl").open("a") as log:
            log.write(json.dumps(dict(node=node, database=db, sql=query, rc=result.returncode,
                                      stdout=result.stdout, stderr=result.stderr)) + "\n")
        return result

    def sql(self, node, query, db="postgres"):
        result = self.result(node, query, db)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    def async_sql(self, node, query):
        process = subprocess.Popen(self.command(node), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.stdin.write(query + "\n")
        process.stdin.close()
        process.stdin = None
        return process

    def finish(self, process, error_expected=False):
        try:
            out, err = process.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError("SQL did not terminate within the test watchdog")
        assert (process.returncode != 0) if error_expected else (process.returncode == 0), err
        return out.strip()

    def close(self):
        for s in self.sockets:
            s.close()
        if self.started:
            self.call("shared_stop")

    def barrier(self, name, node, **args):
        self.call("barrier_arm", name=name, node=node, **args)

    def hit(self, name, node):
        result = self.call("barrier_wait", name=name, node=node)
        assert result["name"] == name and result["node"] == node and result["hits"] > 0
        return result

    def release(self, name, node):
        self.call("barrier_release", name=name, node=node)

    def crash(self, node):
        result = self.call("crash_and_fence", node=node)
        assert result["node"] == node and result["system_identifier"] == self.identity["system_identifier"]
        assert result["terminal_io_proof"] and result["old_incarnation"] > 0
        return result

    def wal(self, require_release=False):
        events = self.call("wal_observation")["events"]
        pending = check_wal(events, require_release)
        for path in pending:
            assert Path(path).is_file(), f"required WAL missing: {path}"
        return events

    def retired_contributor(self, node):
        events = self.wal()
        owed = {e["id"] for e in events if e["kind"] == "contribution" and e["node"] == node}
        retired = {e["contribution"] for e in events if e["kind"] == "retire"}
        assert owed and owed <= retired, "leaving node still has uncovered PI obligations"


def seed(f):
    f.sql(0, "CREATE TABLE pre2_updates(id int PRIMARY KEY, value bigint, history text);"
          "INSERT INTO pre2_updates VALUES(1,0,''); CHECKPOINT")


def update(f, node, serial):
    # All updates are committed, and the ordered string detects lost/repeated writes.
    f.sql(node, f"UPDATE pre2_updates SET value=value+{serial}, history=history||'{serial},' WHERE id=1")


def verify(f, serials, nodes):
    expected = str(sum(serials)) + "|" + "".join(f"{i}," for i in serials)
    for node in nodes:
        assert f.sql(node, "SELECT value||'|'||history FROM pre2_updates WHERE id=1") == expected, "lost or repeated update"


def w06(f, case):
    seed(f)
    f.call("observe_begin", relation="pre2_updates")
    identity, previous, versions = None, None, set()
    for serial, node in enumerate([0, 1, 2, 0, 1, 2, 0, 1, 2], 1):
        update(f, node, serial)
        witness = f.call("current_handoff", relation="pre2_updates", node=node)
        assert witness["holder"] == node and witness["block"] == 0 and witness["space"]
        assert witness["version"] and witness["writer"]
        current_identity = (witness["tag"], witness["space"])
        if identity is not None:
            assert current_identity == identity, "handoffs did not use one block incarnation"
            assert witness["previous_holder"] == previous and witness["handoff_id"], "no actual cross-node handoff"
        assert witness["version"] not in versions, "redirty reused a page version"
        identity, previous = current_identity, node
        versions.add(witness["version"])
        f.wal()
    serials, survivors = list(range(1, 10)), [0, 1, 2]
    if case == "W06-CHECKPOINT-HANDOFF":
        f.barrier("checkpoint_before_holder_data", 2)
        checkpoint = f.async_sql(0, "CHECKPOINT")
        f.hit("checkpoint_before_holder_data", 2)
        transfer = f.async_sql(1, "UPDATE pre2_updates SET value=value+10, history=history||'10,' WHERE id=1")
        waiting = f.call("current_handoff_pending", node=1, relation="pre2_updates")
        assert waiting["requester"] == 1 and waiting["tag"] == identity[0] and waiting["request_id"]
        f.wal()
        f.release("checkpoint_before_holder_data", 2)
        f.finish(checkpoint)
        f.finish(transfer)
        serials.append(10)
        f.wal()  # old DATA receipt cannot retire this new dirty generation
    elif case == "W06-HOLDER-EIO":
        f.barrier("holder_data_eio", 2)
        checkpoint = f.async_sql(0, "CHECKPOINT")
        failure = f.hit("holder_data_eio", 2)
        assert failure["errno"] == "EIO"
        f.wal()
        f.release("holder_data_eio", 2)
        # A retry may succeed; a failed checkpoint must leave the committed SQL intact.
        out, err = checkpoint.communicate(timeout=120)
        assert checkpoint.returncode == 0 or "Input/output error" in err or "I/O error" in err, err
    elif case == "W06-CONTRIBUTOR-CRASH":
        f.barrier("holder_before_data", 2)
        checkpoint = f.async_sql(1, "CHECKPOINT")
        f.hit("holder_before_data", 2)
        failed = f.crash(0)
        f.wal()
        f.release("holder_before_data", 2)
        f.finish(checkpoint)
        recovery = f.call("recovery_observation", failed=failed)
        assert recovery["terminal_duty"] == failed["duty"]
        survivors = [1, 2]
    verify(f, serials, survivors)
    f.sql(survivors[0], "CHECKPOINT")
    f.call("wal_recycle")
    f.wal(require_release=True)
    verify(f, serials, survivors)


def rd_ddl(f, committed):
    f.sql(0, "CREATE TABLE pre2_ddl(id int); INSERT INTO pre2_ddl VALUES(11); CHECKPOINT")
    readers = f.call("persistent_reader", operation="prime", relation="pre2_ddl", nodes=[1, 2])
    cut = "ddl_committed_before_invalidation" if committed else "ddl_before_commit"
    f.barrier(cut, 0)
    process = f.async_sql(0, "BEGIN; TRUNCATE pre2_ddl; ALTER TABLE pre2_ddl ADD COLUMN extra int; COMMIT")
    f.hit(cut, 0)
    failed = f.crash(0)
    f.finish(process, error_expected=True)
    recovery = f.call("recovery_observation", failed=failed)
    boots = {int(k): v for k, v in recovery["survivor_boots"].items()}
    assert set(boots) == {1, 2}, "missing survivor observation"
    check_reset(recovery["events"], failed["duty"], boots)
    for node in (1, 2):
        assert f.sql(node, "SELECT count(*) FROM pre2_ddl") == ("0" if committed else "1")
        assert f.sql(node, "SELECT count(*) FROM pg_attribute WHERE attrelid='pre2_ddl'::regclass AND attname='extra'") == ("1" if committed else "0")
    cached = f.call("persistent_reader", operation="check", readers=readers, committed=committed)
    assert cached["catalog_reset_barrier"] == recovery["events"][-1]["barrier"]
    assert sorted(cached["nodes"]) == [1, 2] and cached["space_callback_verified"] is True


def rd_oid(f):
    f.sql(0, "CREATE TABLE pre2_old(v int); INSERT INTO pre2_old VALUES(7); CHECKPOINT")
    old = f.sql(0, "SELECT pg_relation_filenode('pre2_old')")
    locator = f.call("relation_files", node=0, relation="pre2_old")
    f.barrier("drop_before_unlink_checkpoint", 0)
    f.sql(0, "DROP TABLE pre2_old")
    assert Path(locator["main"]).is_file() and Path(locator["main"]).stat().st_size == 0
    failed = f.crash(0)
    window = f.call("replay_window", failed=failed, hold_close=True)
    for replay in range(2):
        f.call("repeat_drop_replay", window=window, locator=locator, pass_number=replay + 1)
        assert Path(locator["main"]).is_file(), "replay lost the MAIN placeholder"
        f.call("oid_candidate", node=1, value=old)
        f.sql(1, f"CREATE TABLE pre2_reuse_{replay}(v int); INSERT INTO pre2_reuse_{replay} VALUES({100 + replay})")
        assert f.sql(1, f"SELECT pg_relation_filenode('pre2_reuse_{replay}')") != old
        for prior in range(replay + 1):
            assert f.sql(1, f"SELECT v FROM pre2_reuse_{prior}") == str(100 + prior)
        f.sql(1, "CHECKPOINT")
        assert Path(locator["main"]).is_file(), "ordinary survivor checkpoint retired recovery placeholder"
    closed = f.call("replay_window", window=window, hold_close=False)
    assert closed["durable_closed_window"] == window["id"]
    f.call("reclaim_window", window=window)
    for prior in range(2):
        assert f.sql(1, f"SELECT v FROM pre2_reuse_{prior}") == str(100 + prior)


def refusal(f, case):
    if case == "RD-DROP-DATABASE":
        before = f.sql(0, "SELECT oid FROM pg_database WHERE datname='postgres'", "template1")
        for node in (0, 1):
            result = f.result(node, "DROP DATABASE postgres WITH (FORCE)", "template1")
            assert result.returncode != 0 and "0A000" in result.stderr
            assert "DROP DATABASE is not supported in shared mode" in result.stderr
            assert f.sql(node, "SELECT oid FROM pg_database WHERE datname='postgres'", "template1") == before
            assert f.sql(node, "SELECT 1") == "1"
    else:
        # Adapter makes a disposable target through the approved backup tooling;
        # neither this harness nor the adapter may delete its label to start it.
        target = f.call("backup_refusal_target")
        label, control = Path(target["backup_label"]), Path(target["control"])
        before = (digest(label), digest(control))
        result = f.call("backup_refusal_start", target=target)
        assert result["start_rc"] != 0 and "backup_label recovery is not supported in shared mode" in result["stderr"]
        assert before == (digest(label), digest(control))
        assert result["shared_before_sha256"] == result["shared_after_sha256"]


def member_command(f, node, action, request):
    encoded = json.dumps(request).replace("'", "''")
    return f"SELECT pg_cluster_membership_command('{action}', '{encoded}'::jsonb)"


def members(f, case):
    seed(f)
    f.call("observe_begin", relation="pre2_updates")
    update(f, 1, 1)
    update(f, 0, 2)  # target node1 retains a dirty contribution after handing off current
    action = "remove" if case == "MEM-REMOVE-REJOIN" else "leave"
    request = f.call("membership_request", kind=action, target=1)["request"]
    membership_cli.validate_request(request)
    assert request["operation_kind"] == action and request["target_node"] == 1
    if case == "MEM-REMOVE-REJOIN":
        reply = json.loads(f.sql(0, member_command(f, 0, "execute", request)))
        accepted_reply(reply, request)
        removed = f.call("membership_terminal", request=request)
        assert removed["request"] == request
        assert removed["phase"] == "COMMIT_CLOSED" and removed["pi_obligations_retired"] is True
        f.retired_contributor(1)
        rejoin = f.call("membership_request", kind="rejoin", target=1)["request"]
        membership_cli.validate_request(rejoin)
        assert rejoin["operation_kind"] == "rejoin" and rejoin["target_node"] == 1
        reply = json.loads(f.sql(0, member_command(f, 0, "execute", rejoin)))
        accepted_reply(reply, rejoin)
        admitted = f.call("rejoin_admission", request=rejoin)
        assert admitted["request"] == rejoin
        assert admitted["new_incarnation"] == rejoin["reserved_new_incarnation"]
        assert admitted["old_incarnation"] == rejoin["expected_old_incarnation"]
        assert admitted["new_incarnation"] > admitted["old_incarnation"]
        assert admitted["deny_retired_after_root_commit"] and admitted["admitted_before_open"]
        eras = f.call("xid_era_observation", node=1)
        assert eras["new_era"] > eras["old_era"]
        assert eras["new_first_xid"] > eras["historical_allocated_upper"]
        update(f, 1, 3)
        verify(f, [1, 2, 3], [0, 1, 2])
        return
    cut = "leave_drain_after_reserve" if case == "MEM-LEAVE-FAILURE" else "leave_after_commit"
    f.barrier(cut, 1)
    process = f.async_sql(1, member_command(f, 1, "execute", request))
    f.hit(cut, 1)
    drain_ms = int(f.sql(0, "SHOW cluster.clean_leave_drain_timeout_ms"))
    failed = f.crash(2)
    f.release(cut, 1)
    reply = json.loads(f.finish(process))
    accepted_reply(reply, request)
    result = f.call("membership_trace", request=request, failed=failed)
    assert result["request"] == request and result["failed"] == failed, "trace belongs to another operation"
    leave_key, failure_key = identity_key(request), identity_key(failed)
    assert result["leave_key"] == leave_key and result["failure_key"] == failure_key
    assert result["recovery_budget_ms"] == f.recovery_budget_ms
    if case == "MEM-LEAVE-FAILURE":
        f.wal()  # aborted LEAVE may retain live PI; its WAL must remain protected
        check_membership(result["events"], leave_key, failure_key,
                         drain_ms, result["recovery_budget_ms"])
    else:
        assert result["pi_obligations_retired"] is True
        f.retired_contributor(1)
        assert result["commit_key"] == result["closed_key"] == leave_key
        assert result["close_proof_source"] == "voting_disks"
        assert 2 not in result["required_peer_acks"], "closure still waits for failed peer"
        assert result["closed_elapsed_ms"] <= result["recovery_budget_ms"]
        assert result["failure_complete"] is True
    verify(f, [1, 2], [0])


def run_case(name, driver, root, bindir):
    count, extra = CASES[name]
    capabilities = driver.call("describe")
    missing = sorted(set(BASE + extra) - set(capabilities.get("capabilities", [])))
    if missing:
        raise Blocked("missing product/test observations: " + ", ".join(missing))
    f = Fixture(driver, root, count, bindir)
    if "recovery_deadline" in extra:
        limits = capabilities.get("limits", {})
        if not isinstance(limits.get("recovery_budget_ms"), int) or limits["recovery_budget_ms"] <= 0 or not limits.get("source"):
            f.close()
            raise Blocked("the existing normal recovery deadline has not been bound")
        f.recovery_budget_ms = limits["recovery_budget_ms"]
        (root / "limits.json").write_text(json.dumps(limits, indent=2) + "\n")
    try:
        f.init()
        if name.startswith("W06-"):
            w06(f, name)
        elif name.startswith("RD-DDL-"):
            rd_ddl(f, name.endswith("COMMITTED") and not name.endswith("UNCOMMITTED"))
        elif name == "RD-OID-REPLAY":
            rd_oid(f)
        elif name.startswith("RD-"):
            refusal(f, name)
        elif name.startswith("MEM-"):
            members(f, name)
        else:
            f.sql(0, "CREATE TABLE pre2_fresh(id int); INSERT INTO pre2_fresh VALUES(42)")
            for i in range(count):
                assert f.sql(i, "SELECT id FROM pre2_fresh") == "42"
    finally:
        f.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=tuple(CASES), action="append")
    parser.add_argument("--adapter", default=os.getenv("PGRAC_PRE2_TEST_ADAPTER"))
    parser.add_argument("--bindir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    selected = args.case or list(CASES)
    if args.list:
        print(json.dumps({k: dict(nodes=CASES[k][0], dependencies=BASE + CASES[k][1]) for k in selected}, indent=2))
        return 0
    if args.output is None or args.bindir is None:
        parser.error("--output and --bindir are required")
    args.output.mkdir(parents=True, exist_ok=False)
    records = []
    for name in selected:
        root = args.output / name
        root.mkdir()
        started = time.monotonic()
        try:
            driver = Driver(args.adapter, root / "adapter.jsonl")
            run_case(name, driver, root, args.bindir.resolve())
            status, detail = "PASS", "all real assertions completed"
        except Blocked as error:
            status, detail = "BLOCKED", str(error)
        except Exception as error:
            status, detail = "FAIL", f"{type(error).__name__}: {error}"
        record = dict(case=name, status=status, detail=detail,
                      dependencies=BASE + CASES[name][1], elapsed_s=time.monotonic() - started)
        records.append(record)
        print(json.dumps(record), flush=True)
        (args.output / "results.json").write_text(json.dumps(records, indent=2) + "\n")
    return 0 if all(r["status"] == "PASS" for r in records) else 1


if __name__ == "__main__":
    sys.exit(main())
