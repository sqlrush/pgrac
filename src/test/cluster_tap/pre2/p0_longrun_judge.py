"""Judge a retained long-run directory; reads files only.

The rule is fixed: every server ERROR/FATAL/PANIC, every crash indicator and
every client error fails the run unless it is classified and an explicit
allow-list entry names it. With no allow-list nothing is allowed.
Unclassified records, PANIC, crash indicators and wrong answers can never
be allowed. Labels from p0_longrun_rules.py sort failures for the reader.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
from typing import Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import p0_longrun_rules as rules
from p0_longrun_cycle import planned_steps
from p0_longrun_logs import read_events, signature

NEVER_ALLOWED = ('PANIC', 'CRASH', 'ANOMALY')
NATIVE, SELFTEST = 'native-four-member', 'selftest-plain-single'
KEEP_RAW = 20


def load_allow_list(path: Optional[Path]) -> Dict[str, object]:
    """Entries need an id, a reason and a message pattern that cannot match
    everything; they apply to ERROR records unless severities says FATAL."""
    if path is None:
        return dict(file=None, sha256=None, entries=[])
    data = Path(path).read_bytes()
    document = json.loads(data)
    if not isinstance(document, dict) or document.get('version') != 1:
        raise ValueError('unsupported allow-list version')
    entries, seen = [], set()
    for entry in document.get('entries', []):
        unknown = set(entry) - {'id', 'reason', 'message', 'sqlstate', 'phases', 'sources',
                                'severities'}
        if unknown or not all(isinstance(entry.get(k), str) and entry[k].strip()
                              for k in ('id', 'reason', 'message')):
            raise ValueError('allow-list entry needs id, reason and message only')
        if entry['id'] in seen:
            raise ValueError('duplicate allow-list entry: ' + entry['id'])
        seen.add(entry['id'])
        pattern = re.compile(entry['message'])
        if pattern.search('') or (pattern.search('qqq') and pattern.search('777')):
            raise ValueError('allow-list pattern matches arbitrary messages: ' + entry['id'])
        severities = tuple(entry.get('severities', ('ERROR',)))
        if not severities or set(severities) - {'ERROR', 'FATAL', 'CONNECTION'}:
            raise ValueError('allow-list may only name ERROR, FATAL or CONNECTION records')
        entries.append(dict(id=entry['id'], reason=entry['reason'], pattern=pattern,
                            message=entry['message'], sqlstate=entry.get('sqlstate'),
                            phases=tuple(entry.get('phases', ())),
                            sources=tuple(entry.get('sources', ())), severities=severities))
    return dict(file=str(path), sha256=hashlib.sha256(data).hexdigest(), entries=entries)


def allowed_by(event: Mapping[str, object], allow: Mapping[str, object]) -> Optional[str]:
    if event.get('severity') in NEVER_ALLOWED:
        return None
    for entry in allow['entries']:
        if (event.get('severity') in entry['severities']
                and (not entry['sqlstate'] or event.get('sqlstate') == entry['sqlstate'])
                and (not entry['phases'] or event.get('phase') in entry['phases'])
                and (not entry['sources'] or event.get('source') in entry['sources'])
                and entry['pattern'].search(str(event.get('message') or ''))):
            return entry['id']
    return None


class Tally:
    """Counts of every failure record, sorted by label."""

    def __init__(self, allow: Mapping[str, object]) -> None:
        self.allow = allow
        self.buckets: Dict[str, Dict[str, object]] = {
            bucket: dict(server=0, client=0, allowed=0, rules=Counter(), steps=Counter(),
                         sample=None) for bucket in rules.BUCKETS}
        self.unclassified_count = 0
        self.unclassified_signatures: Counter = Counter()
        self.unclassified_raw: List[str] = []       # first record of each distinct form
        self.unclassified_more: List[str] = []      # later records, to fill the list
        self.allowed: Counter = Counter()
        self.severities: Counter = Counter()
        self.failing = Counter()        # by source

    def add(self, event: Mapping[str, object], cycle: int) -> None:
        times = int(event.get('count') or 1)    # aggregated client records carry a count
        self.severities['%s/%s' % (event['source'], event['severity'])] += times
        rule = rules.classify(event)
        permit = allowed_by(event, self.allow) if rule is not None else None
        if permit is not None:
            self.allowed[permit] += times
            if rule is not None:
                self.buckets[rule.bucket]['allowed'] += times
            return
        self.failing[str(event['source'])] += times
        where = 'cycle %d node %s step %s' % (cycle, event.get('node'), event.get('phase'))
        raw = str(event.get('raw') or json.dumps(
            {k: event.get(k) for k in ('severity', 'sqlstate', 'message', 'detail', 'stage',
                                       'location', 'backend_pid')}, sort_keys=True))
        if rule is None:
            self.unclassified_count += times
            form = signature(event)
            keep = self.unclassified_raw if form not in self.unclassified_signatures \
                else self.unclassified_more
            self.unclassified_signatures[form] += times
            if len(keep) < KEEP_RAW:
                keep.append('[%s %s] %s' % (event['source'], where, raw))
            return
        bucket = self.buckets[rule.bucket]
        bucket[str(event['source'])] += times
        bucket['rules'][rule.rule_id] += times
        bucket['steps'][str(event.get('phase'))] += times
        if bucket['sample'] is None:
            bucket['sample'] = '[%s %s] %s' % (event['source'], where, raw)


def boundaries(journal: Mapping[str, object], node: str,
               forced: Optional[Mapping[str, object]]) -> List[Tuple[int, str]]:
    marks = [(int(step['log_start'][node]), str(step['name']))
             for step in journal['steps'] if 'log_start' in step]
    if forced:
        marks.append((int(forced['log_start'][node]), 'forced_cleanup'))
    return marks


def client_events(path: Path) -> Iterator[Dict[str, object]]:
    if not path.exists():
        return
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def judge_cycle(directory: Path, spec: Mapping[str, object], members: int,
                tally: Tally) -> Dict[str, object]:
    cycle = int(spec['cycle'])
    report: Dict[str, object] = dict(cycle=cycle, clients_per_node=spec['clients_per_node'],
                                    seconds=spec['seconds'], failures=[], steps=[], nodes=[],
                                    server_records=0, client_records=0)
    failures: List[Dict[str, object]] = report['failures']
    journal_path = directory / 'evidence' / 'steps.json'
    if not journal_path.exists():
        failures.append(dict(code='EVIDENCE_MISSING', what='steps.json'))
        return report
    journal = json.loads(journal_path.read_text())
    if len(journal['members']) != members:
        failures.append(dict(code='MEMBER_COUNT', expected=members,
                             actual=len(journal['members'])))
    done = {step['name']: step for step in journal['steps']}
    for name in planned_steps(spec):
        step = done.get(name, dict(status='NOT_RUN', reason='no record'))
        report['steps'].append(dict(name=name, status=step['status'],
                                    error=step.get('error') or step.get('reason'),
                                    seconds=round(step['finished_unix'] - step['started_unix'], 3)
                                    if 'finished_unix' in step else None))
        if step['status'] != 'PASS':
            failures.append(dict(code='STEP_NOT_PASSED', step=name, status=step['status'],
                                 error=step.get('error') or step.get('reason')))
    forced_path = directory / 'forced-cleanup.json'
    forced = json.loads(forced_path.read_text()) if forced_path.exists() else None
    if forced:
        failures.append(dict(code='FORCED_CLEANUP', detail=forced.get('detail')))
    elif journal.get('left_running'):
        failures.append(dict(code='LEFT_RUNNING'))
    before = sum(tally.failing.values())
    for member in journal['members']:
        log = directory / member['logfile']
        if not log.exists():
            if any(s['name'] == 'start' for s in journal['steps']):
                failures.append(dict(code='EVIDENCE_MISSING', what=member['logfile']))
            continue
        for event in read_events(log, int(member['id']),
                                 boundaries(journal, str(member['id']), forced)):
            report['server_records'] += 1
            tally.add(event, cycle)
    for event in client_events(directory / 'evidence' / 'client-errors.jsonl'):
        report['client_records'] += int(event.get('count') or 1)
        tally.add(event, cycle)
    report['failing_records'] = sum(tally.failing.values()) - before
    if report['failing_records']:
        failures.append(dict(code='ERRORS_NOT_ALLOWED', count=report['failing_records']))
    load_path = directory / 'evidence' / 'load.json'
    if load_path.exists():
        load = json.loads(load_path.read_text())
        report.update(nodes=load['nodes'], commits=load['commits'],
                      elapsed_seconds=load['elapsed_seconds'], hangs=load['hangs'],
                      stall=load['stall'], crash=load['crash'])
        for node in load['nodes']:
            if node['commits'] <= 0:
                failures.append(dict(code='NODE_WITHOUT_COMMITS', node=node['node']))
            if node['anomalies']:
                failures.append(dict(code='WRONG_CLIENT_ANSWERS', node=node['node'],
                                     count=node['anomalies']))
        if load['hangs']:
            failures.append(dict(code='HUNG_CONNECTIONS', count=len(load['hangs'])))
        if load['stall']:
            failures.append(dict(code='WORKLOAD_STALLED', detail=load['stall']))
    report['reconciliations'] = []
    for name in ('verify', 'reverify', 'reverify_again'):
        path = directory / 'evidence' / (name + '.json')
        if path.exists():
            answer = json.loads(path.read_text())
            report['reconciliations'].append(answer)
            bad = [m for m in answer['members'] if m['status'] != 'PASS']
            if bad:
                failures.append(dict(code='LEDGER_MISMATCH', step=name,
                                     members=[m['node'] for m in bad]))
    return report


def judge(run: Path, allow_file: Optional[Path] = None) -> Dict[str, object]:
    """Compute the verdict of a retained run directory."""
    run = Path(run)
    plan = json.loads((run / 'plan.json').read_text())
    allow = load_allow_list(allow_file)
    tally = Tally(allow)
    native = plan['mode'] == NATIVE
    members = 4 if native else int(plan['members'])
    cycles = [judge_cycle(run / ('cycle%d' % spec['cycle']), spec, members, tally)
              for spec in plan['cycles']]
    failures = [dict(failure, cycle=c['cycle']) for c in cycles for failure in c['failures']]
    controller = run / 'controller.json'
    if controller.exists():
        state = json.loads(controller.read_text())
        for problem in state.get('problems', []):
            failures.append(dict(code='CONTROLLER', detail=problem))
    crash_cycles = [c for c, spec in zip(cycles, plan['cycles']) if spec.get('crash')]

    def stop_passed(cycle: Mapping[str, object]) -> bool:
        return any(s['name'] == 'stop' and s['status'] == 'PASS' for s in cycle['steps'])

    passed = not failures
    result = dict(
        verdict=('PASS' if passed else 'FAIL') if native
        else ('SELFTEST_OK' if passed else 'SELFTEST_FAILED'),
        acceptance_result=native, mode=plan['mode'], zero_errors=not sum(tally.failing.values()),
        failures=failures, cycles=cycles,
        totals=dict(failing_server_records=tally.failing['server'],
                    failing_client_records=tally.failing['client'],
                    allowed_records=sum(tally.allowed.values()),
                    unclassified=tally.unclassified_count, by_severity=dict(tally.severities)),
        buckets={name: dict(server=b['server'], client=b['client'], allowed=b['allowed'],
                            rules=dict(b['rules']), steps=dict(b['steps']), sample=b['sample'])
                 for name, b in tally.buckets.items()},
        buckets_without_text_rule=list(rules.BUCKETS_WITHOUT_TEXT_RULE),
        unclassified=dict(count=tally.unclassified_count,
                          first_raw=(tally.unclassified_raw + tally.unclassified_more)[:KEEP_RAW],
                          signatures=[dict(count=n, signature=s) for s, n in
                                      tally.unclassified_signatures.most_common(50)]),
        allow_list=dict(file=allow['file'], sha256=allow['sha256'],
                        entries=[dict(id=e['id'], reason=e['reason'], message=e['message'],
                                      matched=tally.allowed[e['id']]) for e in allow['entries']]),
        sections=dict(
            workload='RUN' if any('commits' in c for c in cycles) else 'NOT_RUN',
            normal_stop=dict(requested=len(cycles), passed=sum(stop_passed(c) for c in cycles)),
            crash_restart_reconcile=('NOT_RUN' if not crash_cycles else 'PASS'
                                     if not any(c['failures'] for c in crash_cycles) else 'FAIL'),
            service_after_member_isolation='NOT_IMPLEMENTED',
            directed_capacity_cases='NOT_RUN'),
        rule_table=dict(rules=len(rules.RULES), sha256=hashlib.sha256(
            Path(rules.__file__).read_bytes()).hexdigest()),
        identity=json.loads((run / 'identity.json').read_text())
        if (run / 'identity.json').exists() else None)
    return result
