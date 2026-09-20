"""Data initialization and measured demo workload. Author: SqlRush <sqlrush@gmail.com>"""

import json
from pathlib import Path
import re
import subprocess
import time

from demo_lib import benchmark_verdict, parse_pgbench, population_rows, row_snapshot


def snapshots(demo, report, label):
    result = []
    for i in range(4):
        path = report / (label+"-node"+str(i)+".tsv")
        with path.open("x") as output:
            demo.sql(i, "COPY (SELECT id,value,pad FROM demo_account ORDER BY id) TO STDOUT;",
                     stdout=output)
        with path.open("rb") as source:
            result.append(row_snapshot(source))
    if any(item != result[0] for item in result):
        raise ValueError("full ordered demo_account bytes differ between nodes")
    return result


def summary(path, value):
    (path / "summary.json").write_text(json.dumps(value, indent=2)+"\n")
    print(json.dumps(value, indent=2))
    print("Reports retained: " + str(path))


def execute(demo, args):
    if demo.state["phase"] != "READY" or demo.running() != [0, 1, 2, 3]:
        raise ValueError("all four nodes must be READY")
    if not all(demo.healthy(i) for i in range(4)):
        raise ValueError("cluster readiness check failed")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())+"-"+str(time.time_ns())
    report = demo.root / "reports" / (args.command+"-"+stamp)
    report.mkdir(parents=True, mode=0o700)
    offsets = demo.log_offsets()
    result = {"verdict": "FAIL", "is_formal_pre": False, "command": args.command,
              "source_revision": demo.state["source_revision"], "image_id": demo.state["image_id"],
              "phase": "starting"}
    try:
        before = snapshots(demo, report, "before")
        result["before"] = before
        if args.command == "init-data":
            rows = population_rows(before[0]["rows"], args.rows)
            if demo.sql(0, "SELECT count(*) FROM demo_smoke;").stdout.strip() != "0":
                raise ValueError("demo_smoke is not empty; refusing to overwrite prior data")
            demo.sql(0, "BEGIN; INSERT INTO demo_account SELECT g,0,repeat('p',64) "
                     "FROM generate_series(1,%d) AS g; INSERT INTO demo_smoke VALUES(1,0); COMMIT;" % rows)
            demo.sql(0, "VACUUM (FREEZE, ANALYZE) demo_account;")
            demo.sql(0, "VACUUM (FREEZE, ANALYZE) demo_smoke;")
            after = snapshots(demo, report, "after")
            if after[0]["rows"] != rows or after[0]["sum"] != 0:
                raise ValueError("initialized row count/value mismatch")
            demo.state["rows"] = rows
            demo.save()
            result.update(after=after, initialized_rows=rows)
        elif args.command == "verify":
            if "rows" not in demo.state:
                raise ValueError("run init-data before verify")
            if before[0]["rows"] != demo.state["rows"]:
                raise ValueError("row count differs from initialized dataset")
            # A real committed cross-node write is checked independently of count/hash.
            initial = int(demo.sql(0, "SELECT value FROM demo_smoke WHERE id=1;").stdout.strip())
            for i in range(4):
                value = demo.sql(i, "UPDATE demo_smoke SET value=value+1 WHERE id=1 RETURNING value;").stdout.strip()
                if value != str(initial+i+1):
                    raise ValueError("cross-node committed increment is incorrect")
            if any(demo.sql(i, "SELECT value FROM demo_smoke WHERE id=1;").stdout.strip()
                   != str(initial+4) for i in range(4)):
                raise ValueError("cross-node smoke readback is incorrect")
            result["cross_node_increment"] = 4
        else:
            if not 1 <= args.clients <= 16 or not 1 <= args.seconds <= 600:
                raise ValueError("clients per node must be 1..16; seconds must be 1..600")
            rows = demo.state.get("rows")
            if not rows or rows != before[0]["rows"]:
                raise ValueError("initialize test data before benchmarking")
            # Consecutive keys are required: an UPDATE of an absent row is not work.
            for i in range(4):
                keys = demo.sql(i, "SELECT min(id)||':'||max(id)||':'||count(*) FROM demo_account;").stdout.strip()
                if keys != "1:%d:%d" % (rows, rows):
                    raise ValueError("benchmark requires exact consecutive primary keys")
            children, outputs = [], []
            started = time.monotonic()
            for i in range(4):
                log = (report / ("pgbench-node%d.log" % i)).open("x")
                outputs.append(log)
                children.append(subprocess.Popen([
                    "podman", "exec", demo.container(i), "pgbench", "-h", "127.0.0.1",
                    "-p", str(demo.state["port_base"]+i), "-U", "pgrac", "-n",
                    "-c", str(args.clients), "-j", str(args.clients), "-T", str(args.seconds),
                    "-D", "rows="+str(rows), "-f", "/opt/pgrac/demo/sql/update.sql", "postgres"],
                    stdout=log, stderr=subprocess.STDOUT))
            # Natural completion only. No forced cancellation is hidden as success.
            codes = [child.wait() for child in children]
            elapsed = time.monotonic()-started
            for output in outputs:
                output.close()
            (report / "client-return-codes.json").write_text(json.dumps(codes)+"\n")
            nodes = [parse_pgbench((report / ("pgbench-node%d.log" % i)).read_text(), codes[i])
                     for i in range(4)]
            after = snapshots(demo, report, "after")
            logs = demo.new_logs(offsets)
            errors = [len(re.findall(r"(?:ERROR|FATAL|PANIC):", log)) for log in logs]
            health = [demo.healthy(i) for i in range(4)]
            for i, text in enumerate(logs):
                (report / ("server-node%d.log" % i)).write_text(text)
            result.update(benchmark_verdict(nodes, before, after, errors, health))
            result.update(after=after, clients_per_node=args.clients, duration_seconds=args.seconds,
                          wall_seconds=elapsed, wall_tps=sum(n["transactions"] for n in nodes)/elapsed,
                          server_errors=errors, healthy=health)
            summary(report, result)
            if result["verdict"] != "PASS":
                raise ValueError("benchmark correctness FAILED; inspect retained report")
            return
        errors = [len(re.findall(r"(?:ERROR|FATAL|PANIC):", log)) for log in demo.new_logs(offsets)]
        if any(errors) or not all(demo.healthy(i) for i in range(4)):
            raise ValueError("server error or post-operation health failure")
        result.update(verdict="PASS", phase="complete", server_errors=errors)
        summary(report, result)
    except Exception as error:
        result.update(verdict="FAIL", failure=str(error))
        summary(report, result)
        raise
