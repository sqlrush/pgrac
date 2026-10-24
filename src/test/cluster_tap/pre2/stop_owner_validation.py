"""One private post-fix validation cohort for the client-cleanup stop path.

The only processes signalled here are the two psql clients created by this
helper.  The database backends and the cluster controller are left to the
normal TAP lifecycle.  No process-memory or allocator inspection is used.
"""

from pathlib import Path
import json
import signal
import subprocess
import sys
import time


request = json.loads(Path(sys.argv[1]).read_text())
root = Path(request["output"])
bindir = request["bindir"]
nodes = request["nodes"]
state = {"started_unix": time.time(), "checks": [], "events": []}


def save():
    target = root / "stop-owner-validation.json"
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    tmp.replace(target)


def check(label, passed, **fields):
    item = {"label": label, "passed": bool(passed)}
    item.update(fields)
    state["checks"].append(item)
    save()


def psql(node, sql, timeout=30):
    command = [bindir + "/psql", "-X", "-qAt", "-v", "VERBOSITY=verbose",
               "-d", nodes[node]["connstr"], "-f", "-"]
    completed = subprocess.run(command, input=sql, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=timeout)
    state["events"].append({"kind": "sql", "node": node, "sql": sql,
                             "rc": completed.returncode,
                             "stdout": completed.stdout,
                             "stderr": completed.stderr})
    save()
    return completed


def start_client(node, sql):
    command = [bindir + "/psql", "-X", "-qAt", "-v", "VERBOSITY=verbose",
               "-d", nodes[node]["connstr"], "-f", "-"]
    proc = subprocess.Popen(command, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)
    assert proc.stdin is not None
    proc.stdin.write(sql)
    proc.stdin.flush()
    state["events"].append({"kind": "client_started", "node": node,
                             "pid": proc.pid, "sql": sql,
                             "started_unix": time.time()})
    save()
    return proc


def finish_client(label, proc, sig=signal.SIGTERM):
    if proc.poll() is None:
        proc.send_signal(sig)
        state["events"].append({"kind": "client_signal", "label": label,
                                 "pid": proc.pid, "signal": sig.name,
                                 "sent_unix": time.time()})
    try:
        out, err = proc.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate(timeout=8)
        state["events"].append({"kind": "client_escalation", "label": label,
                                 "pid": proc.pid, "signal": "SIGKILL",
                                 "sent_unix": time.time()})
    event = {"kind": "client_exit", "label": label, "pid": proc.pid,
             "returncode": proc.returncode, "stdout": out, "stderr": err,
             "finished_unix": time.time()}
    state["events"].append(event)
    save()
    return event


holder = waiter = None
try:
    setup = psql(0, """CREATE TABLE stop_owner_wait(id int PRIMARY KEY, v int);
INSERT INTO stop_owner_wait VALUES (1, 0);
CHECKPOINT;
SELECT 'SETUP_DONE';
""")
    check("setup completes", setup.returncode == 0 and "SETUP_DONE" in setup.stdout,
          rc=setup.returncode)

    # Keep the row lock and its cross-node current ownership in the node-0
    # backend while node 1 requests the same row.
    holder = start_client(0, "BEGIN; UPDATE stop_owner_wait SET v=1 WHERE id=1;\n")
    time.sleep(1.0)
    waiter = start_client(1, "BEGIN; UPDATE stop_owner_wait SET v=2 WHERE id=1;\n")
    time.sleep(2.0)

    activity = psql(0, """SELECT count(*) FROM pg_stat_activity
WHERE query LIKE '%stop_owner_wait%' AND wait_event IS NOT NULL;
""")
    wait_lines = activity.stdout.strip().splitlines()
    wait_count = int(wait_lines[-1]) if wait_lines and wait_lines[-1].isdigit() else 0
    check("cross-node waiter remains active before client cleanup",
          waiter.poll() is None and wait_count >= 1,
          waiter_alive=waiter.poll() is None, wait_count=wait_count,
          activity_stdout=activity.stdout, activity_stderr=activity.stderr)

    waiter_exit = finish_client("waiter", waiter)
    check("waiting client receives forced SIGTERM", waiter_exit["returncode"] < 0,
          returncode=waiter_exit["returncode"], stderr=waiter_exit["stderr"])
    holder_exit = finish_client("holder", holder)
    check("holder client cleanup completes", holder_exit["returncode"] < 0,
          returncode=holder_exit["returncode"], stderr=holder_exit["stderr"])
    holder = waiter = None
finally:
    for label, proc in (("waiter-finally", waiter), ("holder-finally", holder)):
        if proc is not None:
            try:
                finish_client(label, proc)
            except Exception as error:
                state["events"].append({"kind": "cleanup_error", "label": label,
                                         "error": repr(error)})
    state["finished_unix"] = time.time()
    state["failed_checks"] = sum(not item["passed"] for item in state["checks"])
    state["complete"] = True
    save()

sys.exit(1 if state["failed_checks"] else 0)
