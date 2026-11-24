"""One start -> load -> verify -> normal-stop cycle and its retained evidence.

The cycle only records what happened: step results, per-member log offsets
at every step boundary, every client error, the commit ledger and the
verification answers. The verdict is computed afterwards, from these files
alone, by p0_longrun_judge.py.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from collections import Counter
import json
import logging
from pathlib import Path
import re
import time
from typing import Callable, Dict, List, Mapping, Optional, Sequence

from p0_longrun_cluster import log_offsets
from p0_longrun_load import TABLE, Endpoint, client_event, reconcile, run_load

LOG = logging.getLogger('p0_longrun')
SEED_BATCH = 4096
FORMAL_ROWS, FORMAL_MIN_BLOCKS = 131072, 300
FULL_CLIENT_RECORDS = 100000    # per cycle; later errors are counted per distinct form


class StepFailed(RuntimeError):
    pass


def write_json(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def planned_steps(spec: Mapping[str, object]) -> List[str]:
    steps = ['init', 'start', 'seed', 'load']
    steps += ['restart', 'reverify'] if spec.get('crash') else ['verify']
    if spec.get('drop_step'):
        steps.append('drop')
    steps.append('stop')
    if spec.get('restart_verify'):
        steps += ['restart_again', 'reverify_again', 'stop_again']
    return steps


class Evidence:
    """Step journal of one cycle, rewritten atomically at every boundary."""

    def __init__(self, directory: Path, spec: Mapping[str, object],
                 members: Sequence[Mapping[str, object]], cycle_root: Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.members = members
        self.journal: Dict[str, object] = dict(
            cycle=spec['cycle'], planned=planned_steps(spec), steps=[],
            members=[dict(id=m['id'], logfile=str(Path(str(m['logfile'])).resolve()
                                                 .relative_to(Path(cycle_root).resolve())))
                     for m in members])
        self.errors = (self.directory / 'client-errors.jsonl').open('a')
        self.progress = (self.directory / 'progress.jsonl').open('a')
        self.client_error_count = 0
        self.overflow: Dict[tuple, Dict[str, object]] = {}
        self.save()

    def save(self) -> None:
        write_json(self.directory / 'steps.json', self.journal)

    def client_events(self, events: Sequence[Mapping[str, object]]) -> None:
        for event in events:
            self.client_error_count += 1
            if self.client_error_count <= FULL_CLIENT_RECORDS:
                self.errors.write(json.dumps(event, sort_keys=True) + '\n')
                continue
            form = (event.get('node'), event.get('phase'), event.get('stage'),
                    event.get('severity'), event.get('sqlstate'),
                    re.sub(r'\d+', 'N', str(event.get('message'))))
            if form in self.overflow:
                self.overflow[form]['count'] += 1
            else:
                self.overflow[form] = dict(event, count=1)
        self.errors.flush()

    def progress_sample(self, sample: Mapping[str, object]) -> None:
        self.progress.write(json.dumps(sample, sort_keys=True) + '\n')
        self.progress.flush()
        LOG.info('progress %s', json.dumps(sample, sort_keys=True))

    def run(self, name: str, body: Callable[[], Optional[Dict[str, object]]]) -> bool:
        """Run one step. A failed step is recorded and reported, never raised."""
        step: Dict[str, object] = dict(name=name, status='RUNNING', started_unix=time.time(),
                                      log_start=log_offsets(self.members))
        self.journal['steps'].append(step)
        self.save()
        LOG.info('step %s started', name)
        try:
            step['detail'] = body() or {}
            step['status'] = 'PASS'
        except Exception as error:      # every failure is evidence, not control flow
            step['status'] = 'FAIL'
            step['error'] = '%s: %s' % (type(error).__name__, error)
        step['finished_unix'] = time.time()
        self.save()
        LOG.info('step %s %s %s', name, step['status'], step.get('error', ''))
        return step['status'] == 'PASS'

    def skip(self, names: Sequence[str], reason: str) -> None:
        for name in names:
            self.journal['steps'].append(dict(name=name, status='NOT_RUN', reason=reason))
        self.save()

    def close(self) -> None:
        # Errors beyond the full-record cap: one record per distinct form,
        # carrying the exact number of occurrences.
        for event in self.overflow.values():
            self.errors.write(json.dumps(event, sort_keys=True) + '\n')
        self.errors.close()
        self.progress.close()


class Session:
    """A driver-side session whose failures are kept as client events."""

    def __init__(self, endpoint: Endpoint, connect: Callable[[Endpoint], object], step: str,
                 evidence: Evidence) -> None:
        self.endpoint, self.step, self.evidence = endpoint, step, evidence
        self.conn = connect(endpoint)
        error = self.conn.connect()
        if error is not None:
            evidence.client_events([dict(
                source='client', node=endpoint.node, connection=-1, phase=step,
                stage='CONNECT', severity='CONNECTION', sqlstate=None, message=error,
                detail=None, hint=None, location=None, connection_lost=True,
                backend_pid=None, row=None, unix=time.time())])
            raise StepFailed('member %d: %s' % (endpoint.node, error))

    def query(self, sql: str) -> List[List[Optional[str]]]:
        result = self.conn.execute(sql)
        if not result.ok:
            self.evidence.client_events([client_event(
                self.endpoint.node, -1, self.step, sql.split(None, 1)[0].upper(), result,
                self.conn.backend_pid)])
            raise StepFailed('member %d: %s: %s' % (self.endpoint.node, result.sqlstate,
                                                    result.message))
        return result.rows

    def close(self) -> None:
        self.conn.close()


def seed(endpoints: Sequence[Endpoint], connect: Callable[[Endpoint], object], rows: int,
         evidence: Evidence) -> Dict[str, object]:
    """Create the shared table through member 0 and read it on every member."""
    first = Session(endpoints[0], connect, 'seed', evidence)
    try:
        first.query('CREATE SCHEMA lab')
        first.query('CREATE TABLE %s(id int PRIMARY KEY,v bigint NOT NULL)' % TABLE)
        for start in range(1, rows + 1, SEED_BATCH):
            first.query('INSERT INTO %s SELECT g,0 FROM generate_series(%d,%d) g'
                        % (TABLE, start, min(rows, start + SEED_BATCH - 1)))
    finally:
        first.close()
    geometry = []
    for endpoint in endpoints:
        session = Session(endpoint, connect, 'seed', evidence)
        try:
            row = session.query(
                "SELECT count(*),coalesce(sum(v),0),"
                "count(DISTINCT split_part(ctid::text,',',1)) FROM %s" % TABLE)[0]
        finally:
            session.close()
        counted, total, blocks = (int(value) for value in row)
        geometry.append(dict(node=endpoint.node, rows=counted, value_sum=total,
                             occupied_blocks=blocks))
        if counted != rows or total != 0:
            raise StepFailed('member %d sees %d rows, sum %d after seeding'
                             % (endpoint.node, counted, total))
        if rows == FORMAL_ROWS and blocks < FORMAL_MIN_BLOCKS:
            raise StepFailed('member %d: table spans only %d blocks' % (endpoint.node, blocks))
    return dict(rows=rows, geometry=geometry)


def verify(endpoints: Sequence[Endpoint], connect: Callable[[Endpoint], object], rows: int,
           acked: Counter, doubt: Counter, step: str, evidence: Evidence) -> Dict[str, object]:
    """Read the whole table on every member and reconcile it with the ledger."""
    answers = []
    for endpoint in endpoints:
        try:
            session = Session(endpoint, connect, step, evidence)
            try:
                counted = int(session.query('SELECT count(*) FROM %s' % TABLE)[0][0])
                changed = {int(r[0]): int(r[1]) for r in session.query(
                    'SELECT id,v FROM %s WHERE v<>0' % TABLE)}
            finally:
                session.close()
            answer = reconcile(rows, acked, doubt, counted, changed)
        except StepFailed as error:
            answer = dict(status='ERROR', error=str(error))
        answers.append(dict(answer, node=endpoint.node))
    write_json(evidence.directory / ('%s.json' % step), dict(step=step, members=answers))
    bad = [a['node'] for a in answers if a['status'] != 'PASS']
    if bad:
        raise StepFailed('ledger reconciliation failed on member(s) %s' % bad)
    return dict(members=len(answers), acknowledged=sum(acked.values()),
                in_doubt=sum(doubt.values()))


def drop_step(endpoints: Sequence[Endpoint], connect: Callable[[Endpoint], object],
              evidence: Evidence) -> Dict[str, object]:
    """DROP a table that every member has read."""
    first = Session(endpoints[0], connect, 'drop', evidence)
    try:
        first.query('CREATE TABLE lab.drop_after_load(id int PRIMARY KEY,v int NOT NULL)')
        first.query('INSERT INTO lab.drop_after_load SELECT g,g FROM generate_series(1,64) g')
        for endpoint in endpoints:
            session = Session(endpoint, connect, 'drop', evidence)
            try:
                row = session.query('SELECT count(*),sum(v) FROM lab.drop_after_load')[0]
            finally:
                session.close()
            if row != ['64', '2080']:
                raise StepFailed('member %d read %s from the DROP target' % (endpoint.node, row))
        started = time.time()
        first.query('DROP TABLE lab.drop_after_load')
        return dict(started_unix=started, finished_unix=time.time())
    finally:
        first.close()


def run_cycle(spec: Mapping[str, object], provider: object,
              connect: Callable[[Endpoint], object], cycle_root: Path,
              after_seed: Optional[Callable[[Endpoint], None]] = None) -> Dict[str, object]:
    """Execute the planned steps of one cycle; returns the step journal.
    after_seed is for the driver's own self-test only."""
    evidence = Evidence(Path(cycle_root) / 'evidence', spec, provider.members, cycle_root)
    plan = planned_steps(spec)
    rows = int(spec['rows'])
    state: Dict[str, object] = dict(acked=Counter(), doubt=Counter())

    def load() -> Dict[str, object]:
        result = run_load(provider.endpoints(), int(spec['clients_per_node']),
                          float(spec['seconds']), rows, connect, 'load',
                          evidence.client_events, evidence.progress_sample,
                          grace_seconds=float(spec['grace_seconds']),
                          stall_abort_seconds=float(spec['stall_abort_seconds']),
                          crash=provider.crash if spec.get('crash') else None)
        state['acked'], state['doubt'] = result.acked, result.doubt
        write_json(evidence.directory / 'load.json', result.summary)
        for node, entries in result.ledgers.items():
            with (evidence.directory / ('ledger-node%d.txt' % node)).open('w') as ledger:
                ledger.writelines('%d %.6f %d %s\n' % entry for entry in entries)
        summary = result.summary
        if summary['hangs'] or summary['stall'] or not summary['ran_requested_time']:
            raise StepFailed('workload did not finish as planned: %d hung connection(s), '
                             'stall=%s' % (len(summary['hangs']), summary['stall']))
        if spec.get('crash') and not summary['crash']:
            raise StepFailed('the crash step was requested but did not run')
        return dict(commits=summary['commits'], client_errors=evidence.client_error_count)

    def check(step: str) -> Callable[[], Dict[str, object]]:
        return lambda: verify(provider.endpoints(), connect, rows, state['acked'],
                              state['doubt'], step, evidence)

    def seeded() -> Dict[str, object]:
        detail = seed(provider.endpoints(), connect, rows, evidence)
        if after_seed is not None:
            after_seed(provider.endpoints()[0])
        return detail

    bodies: Dict[str, Callable[[], Optional[Dict[str, object]]]] = dict(
        init=provider.init, start=provider.start, seed=seeded, load=load,
        verify=check('verify'), restart=provider.start, reverify=check('reverify'),
        drop=lambda: drop_step(provider.endpoints(), connect, evidence), stop=provider.stop,
        restart_again=provider.start, reverify_again=check('reverify_again'),
        stop_again=provider.stop)
    # A failure in one of these leaves nothing meaningful for the later steps.
    blocking = ('init', 'start', 'seed', 'restart', 'stop', 'restart_again')
    try:
        for index, name in enumerate(plan):
            if evidence.run(name, bodies[name]) or name not in blocking:
                continue
            rest = plan[index + 1:]
            # A failed seed still owes the running fixture its normal stop.
            if name == 'seed' and 'stop' in rest:
                cut = rest.index('stop')
                evidence.skip(rest[:cut], 'step %s failed' % name)
                evidence.run('stop', bodies['stop'])
                rest = rest[cut + 1:]
            evidence.skip(rest, 'step %s failed' % name)
            break
    finally:
        evidence.journal['left_running'] = bool(getattr(provider, 'running', False))
        evidence.journal['log_end'] = log_offsets(provider.members)
        evidence.save()
        evidence.close()
    return evidence.journal
