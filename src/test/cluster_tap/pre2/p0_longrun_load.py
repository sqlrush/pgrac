"""Shared-table UPDATE workload that keeps running after errors.

Every connection repeats BEGIN / UPDATE one random row / COMMIT on the same
table through its own member, without retrying a failed transaction. Each
failure is kept with its SQLSTATE and server fields, each acknowledged
commit enters a ledger, and nothing here grants a positive result.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from collections import Counter
import queue
import random
import threading
import time
from typing import Callable, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple

TABLE = 'lab.stop_load'
COMMITTED, ABORTED, COMMIT_FAILED, IN_DOUBT, ANOMALY = 'C', 'E', 'F', 'U', 'Z'


class Endpoint(NamedTuple):
    node: int
    conninfo: str


class Transaction(NamedTuple):
    outcome: str
    stage: str                      # statement that decided the outcome
    failures: Tuple[Tuple[str, object], ...]   # (stage, pq Result) in order
    link_lost: bool


def run_transaction(conn: object, row: int, note: Callable[[str], None]) -> Transaction:
    """One no-retry transaction. A COMMIT answered by an error is not a commit;
    a COMMIT whose answer never arrived is in doubt."""
    failures: List[Tuple[str, object]] = []
    note('BEGIN')
    result = conn.execute('BEGIN')
    if not result.ok:
        return Transaction(ABORTED, 'BEGIN', (('BEGIN', result),), result.connection_lost)
    note('UPDATE')
    result = conn.execute('UPDATE %s SET v=v+1 WHERE id=%d' % (TABLE, row))
    outcome = None
    if not result.ok:
        failures.append(('UPDATE', result))
        outcome = ABORTED
    elif result.command != 'UPDATE 1':
        # The row exists for every snapshot; any other count is a wrong answer.
        failures.append(('UPDATE', result._replace(
            ok=False, severity='ANOMALY',
            message='UPDATE of one existing row reported "%s"' % result.command)))
        outcome = ANOMALY
    if outcome is not None:
        if result.connection_lost:
            return Transaction(outcome, 'UPDATE', tuple(failures), True)
        note('ROLLBACK')
        undo = conn.execute('ROLLBACK')
        if not undo.ok:
            failures.append(('ROLLBACK', undo))
        return Transaction(outcome, 'UPDATE', tuple(failures), undo.connection_lost)
    note('COMMIT')
    result = conn.execute('COMMIT')
    if result.ok and result.command == 'COMMIT':
        return Transaction(COMMITTED, 'COMMIT', (), False)
    if result.ok:
        result = result._replace(ok=False, severity='ANOMALY',
                                 message='COMMIT reported "%s"' % result.command)
        return Transaction(ANOMALY, 'COMMIT', (('COMMIT', result),), False)
    outcome = IN_DOUBT if result.connection_lost else COMMIT_FAILED
    return Transaction(outcome, 'COMMIT', (('COMMIT', result),), result.connection_lost)


def client_event(node: int, connection: int, phase: str, stage: str, result: object,
                 backend_pid: Optional[int], row: Optional[int] = None) -> Dict[str, object]:
    return dict(source='client', node=node, connection=connection, phase=phase, stage=stage,
                severity=result.severity or 'CONNECTION', sqlstate=result.sqlstate,
                message=result.message, detail=result.detail, hint=result.hint,
                location=result.location, connection_lost=result.connection_lost,
                backend_pid=backend_pid, row=row, unix=time.time())


class Worker(threading.Thread):
    """One persistent client connection of one member."""

    def __init__(self, endpoint: Endpoint, index: int, rows: int, seed: int,
                 connect: Callable[[Endpoint], object], phase: str,
                 events: 'queue.SimpleQueue', stop: threading.Event,
                 crash_pending: threading.Event) -> None:
        super().__init__(daemon=True, name='load-n%d-c%d' % (endpoint.node, index))
        self.endpoint, self.index, self.rows, self.phase = endpoint, index, rows, phase
        self.random = random.Random(seed)
        self.connect_factory, self.events = connect, events
        self.stop_requested, self.crash_pending = stop, crash_pending
        self.conn: Optional[object] = None
        self.acked: Counter = Counter()
        self.doubt: Counter = Counter()
        self.ledger: List[Tuple[float, int, str]] = []
        self.counts: Counter = Counter()
        self.activity: Tuple[str, Optional[int], float] = ('IDLE', None, time.monotonic())

    def note(self, stage: str, row: Optional[int] = None) -> None:
        self.activity = (stage, row if row is not None else self.activity[1], time.monotonic())

    def completed(self) -> int:
        return sum(self.counts[key] for key in (COMMITTED, ABORTED, COMMIT_FAILED,
                                                IN_DOUBT, ANOMALY))

    def open_connection(self) -> bool:
        """Connect, retrying once a second until the load is told to stop."""
        if self.conn is not None:
            # Keep the published object until its replacement is ready: the
            # controller may still cancel it, even after its handles are closed.
            self.conn.close()
        while not self.stop_requested.is_set():
            self.note('CONNECT')
            conn = self.connect_factory(self.endpoint)
            error = conn.connect()
            if error is None:
                self.conn = conn
                return True
            if self.crash_pending.is_set():
                return False
            self.counts['connect_failures'] += 1
            self.events.put(dict(source='client', node=self.endpoint.node, connection=self.index,
                                 phase=self.phase, stage='CONNECT', severity='CONNECTION',
                                 sqlstate=None, message=error, detail=None, hint=None,
                                 location=None, connection_lost=True, backend_pid=None,
                                 row=None, unix=time.time()))
            self.stop_requested.wait(1.0)
        return False

    def run(self) -> None:
        try:
            if not self.open_connection():
                return
            while not self.stop_requested.is_set():
                row = self.random.randint(1, self.rows)
                self.note('BEGIN', row)
                done = run_transaction(self.conn, row, self.note)
                injected = done.link_lost and self.crash_pending.is_set()
                self.counts[done.outcome] += 1
                self.ledger.append((time.time(), row, done.outcome))
                if done.outcome == COMMITTED:
                    self.acked[row] += 1
                elif done.outcome == IN_DOUBT:
                    self.doubt[row] += 1
                for stage, result in done.failures:
                    if injected and result.connection_lost:
                        self.counts['injected_disconnects'] += 1
                        continue
                    self.events.put(client_event(self.endpoint.node, self.index, self.phase,
                                                 stage, result, self.conn.backend_pid, row))
                if done.link_lost:
                    if injected:
                        return
                    self.counts['reconnects'] += 1
                    if not self.open_connection():
                        return
        except Exception as failure:    # the driver itself broke: never silent
            self.counts[ANOMALY] += 1
            self.events.put(dict(
                source='client', node=self.endpoint.node, connection=self.index,
                phase=self.phase, stage=self.activity[0], severity='ANOMALY', sqlstate=None,
                message='workload client stopped: %s: %s' % (type(failure).__name__, failure),
                detail=None, hint=None, location=None, connection_lost=False,
                backend_pid=getattr(self.conn, 'backend_pid', None), row=self.activity[1],
                unix=time.time()))
        finally:
            self.note('EXITED')
            if self.conn is not None:
                self.conn.close()


class LoadResult(NamedTuple):
    summary: Dict[str, object]
    acked: Counter
    doubt: Counter
    ledgers: Dict[int, List[Tuple[int, float, int, str]]]


def run_load(endpoints: Sequence[Endpoint], clients_per_node: int, seconds: float, rows: int,
             connect: Callable[[Endpoint], object], phase: str,
             on_events: Callable[[List[Dict[str, object]]], None],
             on_progress: Callable[[Dict[str, object]], None],
             grace_seconds: float = 60.0, stall_abort_seconds: float = 300.0,
             progress_seconds: float = 10.0, base_seed: int = 4180,
             crash: Optional[Callable[[], object]] = None) -> LoadResult:
    """Run the workload for the requested time. crash, when given, is called
    at the end of that time while every client is still running."""
    events: 'queue.SimpleQueue' = queue.SimpleQueue()
    stop, crash_pending = threading.Event(), threading.Event()
    workers = [Worker(endpoint, index, rows, (base_seed + endpoint.node) * 1000 + index,
                      connect, phase, events, stop, crash_pending)
               for endpoint in endpoints for index in range(clients_per_node)]

    def drain() -> None:
        batch = []
        while True:
            try:
                batch.append(events.get_nowait())
            except queue.Empty:
                break
        if batch:
            on_events(batch)

    def sample(started: float) -> Dict[str, object]:
        per_node: Dict[int, Counter] = {e.node: Counter() for e in endpoints}
        for worker in workers:
            per_node[worker.endpoint.node].update(worker.counts)
        return dict(elapsed_seconds=round(time.monotonic() - started, 3), unix=time.time(),
                    nodes=[dict(node=node, commits=c[COMMITTED],
                                failed=c[ABORTED] + c[COMMIT_FAILED] + c[ANOMALY],
                                in_doubt=c[IN_DOUBT]) for node, c in sorted(per_node.items())])

    started = time.monotonic()
    for worker in workers:
        worker.start()
    last_progress, last_total, last_change, stall = started, 0, started, None
    while time.monotonic() - started < seconds:
        time.sleep(min(0.5, progress_seconds, max(0.0, seconds - (time.monotonic() - started))))
        drain()
        now = time.monotonic()
        total = sum(worker.completed() for worker in workers)
        if total != last_total:
            last_total, last_change = total, now
        elif stall_abort_seconds > 0 and now - last_change >= stall_abort_seconds:
            stall = dict(seconds_without_any_completed_transaction=round(now - last_change, 3),
                         at_elapsed_seconds=round(now - started, 3))
            break
        if now - last_progress >= progress_seconds:
            last_progress = now
            on_progress(sample(started))
    crash_report = None
    if crash is not None and stall is None:
        crash_pending.set()
        crash_report = crash()
    stop.set()
    limit = time.monotonic() + grace_seconds
    for worker in workers:
        worker.join(max(0.0, limit - time.monotonic()))
    hangs = []
    for worker in workers:
        if worker.is_alive():
            stage, row, since = worker.activity
            hangs.append(dict(node=worker.endpoint.node, connection=worker.index, stage=stage,
                              row=row, blocked_seconds=round(time.monotonic() - since, 3),
                              backend_pid=getattr(worker.conn, 'backend_pid', None),
                              cancel_sent=bool(worker.conn and worker.conn.cancel())))
    for worker in workers:
        if worker.is_alive():
            worker.join(5.0)
    for hang in hangs:
        owner = next(w for w in workers if (w.endpoint.node, w.index)
                     == (hang['node'], hang['connection']))
        hang['returned_after_cancel'] = not owner.is_alive()
    drain()
    elapsed = time.monotonic() - started
    acked: Counter = Counter()
    doubt: Counter = Counter()
    nodes = []
    ledgers: Dict[int, List[Tuple[int, float, int, str]]] = {}
    for endpoint in endpoints:
        mine = [w for w in workers if w.endpoint.node == endpoint.node]
        counts: Counter = Counter()
        for worker in mine:
            counts.update(worker.counts)
            acked.update(worker.acked)
            doubt.update(worker.doubt)
            ledgers.setdefault(endpoint.node, []).extend(
                (worker.index, when, row, outcome) for when, row, outcome in worker.ledger)
        nodes.append(dict(node=endpoint.node, connections=len(mine), commits=counts[COMMITTED],
                          statement_errors=counts[ABORTED], commit_errors=counts[COMMIT_FAILED],
                          in_doubt=counts[IN_DOUBT], anomalies=counts[ANOMALY],
                          reconnects=counts['reconnects'],
                          connect_failures=counts['connect_failures'],
                          injected_disconnects=counts['injected_disconnects']))
    on_progress(sample(started))
    summary = dict(clients_per_node=clients_per_node, requested_seconds=seconds,
                   elapsed_seconds=round(elapsed, 3), rows=rows, base_seed=base_seed,
                   nodes=nodes, commits=sum(n['commits'] for n in nodes), hangs=hangs,
                   stall=stall, crash=crash_report,
                   ran_requested_time=stall is None and elapsed >= seconds)
    return LoadResult(summary, acked, doubt, ledgers)


def reconcile(rows: int, acked: Mapping[int, int], doubt: Mapping[int, int],
              observed_rows: int, observed: Mapping[int, int],
              keep: int = 20) -> Dict[str, object]:
    """Compare one member's table with the commit ledger, row by row.

    Every acknowledged commit must be present; nothing beyond acknowledged
    plus in-doubt commits may be present."""
    violations = []
    for row in sorted(set(acked) | set(doubt) | set(observed)):
        low = acked.get(row, 0)
        high = low + doubt.get(row, 0)
        value = observed.get(row, 0)
        if not 1 <= row <= rows:
            violations.append(dict(kind='UNKNOWN_ROW', row=row, value=value))
        elif value < low:
            violations.append(dict(kind='COMMITTED_MISSING', row=row, value=value,
                                   acknowledged=low, in_doubt=high - low))
        elif value > high:
            violations.append(dict(kind='UNCOMMITTED_PRESENT', row=row, value=value,
                                   acknowledged=low, in_doubt=high - low))
    if observed_rows != rows:
        violations.append(dict(kind='ROW_COUNT', expected=rows, value=observed_rows))
    return dict(status='PASS' if not violations else 'FAIL', rows=observed_rows,
                value_sum=sum(observed.values()), acknowledged=sum(acked.values()),
                in_doubt=sum(doubt.values()), violation_count=len(violations),
                violations=violations[:keep])
