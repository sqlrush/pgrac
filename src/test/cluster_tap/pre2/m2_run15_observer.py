"""Opt-in bounded rollover observations; never start or control a workload."""
import json
import math
import os
from pathlib import Path
import pwd
import re
import subprocess
import time

from m2_rollover_observer import CASES, READS, RolloverObserver as BaseObserver, sha256


POINTS = (30, 80)
POINT_TOLERANCE = 2
WINDOW = 90
SQL_TIMEOUT = 0.75
RUN15_READS = READS + (
    ('pgproc', '*MyProc'),
    ('cv_target', 'cv_sleep_target'),
    ('capacity_cv', '&undo_cleaner_state->capacity_cv'),
    ('wait_event', '*my_wait_event_info'),
    ('cleaner', '*undo_cleaner_state'),
    ('current_tt', 'ClusterTTSlotShm->per_node[cluster_node_id]'),
    ('pool_state', '*UndoRecordShared'),
)


class Run15Observer(BaseObserver):
    def __init__(self, *args, timeline_runner=subprocess.run, **kwargs):
        super().__init__(*args, **kwargs)
        self.psql = self.base / 'install/bin/psql'
        expected = self.identity.get('psql_sha256', '')
        if (not re.fullmatch('[0-9a-f]{64}', expected) or self.psql.is_symlink()
                or sha256(self.psql.read_bytes()) != expected):
            raise ValueError('frozen SQL client identity unavailable')
        self.timeline_runner = timeline_runner
        self.timeline = []
        self.capture_deadline = None
        debugger = self.runner

        def in_window(*args, **kwargs):
            if self.capture_deadline is None or self.clock() > self.capture_deadline:
                raise OSError('capture window expired before debugger launch')
            return debugger(*args, **kwargs)

        self.runner = in_window

    def _command(self, pid):
        return super()._command(pid, reads=RUN15_READS)

    @staticmethod
    def _complete_raw(raw, context):
        return BaseObserver._complete_raw(raw, context, reads=RUN15_READS)

    def _endpoint(self, pid, evidence):
        # Search only the registered cohort layout level, never PGDATA contents.
        owned = self.run / 'testdata'
        layouts = list(owned.glob('*/blackbox-layout.json'))
        if len(layouts) != 1 or layouts[0].is_symlink():
            raise ValueError('original cohort layout is missing or ambiguous')
        layout = layouts[0]
        if not layout.resolve(strict=True).is_relative_to(owned.resolve()):
            raise ValueError('cohort layout escaped the run')
        raw = layout.read_bytes()
        nodes = [n for n in json.loads(raw)['nodes'] if n.get('id') == 0]
        if len(nodes) != 1:
            raise ValueError('original node endpoint is ambiguous')
        node = nodes[0]
        data = Path(node['data_dir']).resolve(strict=True)
        host, port = node['host'], node['port']
        if (not data.is_relative_to(owned.resolve()) or not isinstance(host, str)
                or not host.startswith('/') or type(port) is not int or not 0 < port < 65536):
            raise ValueError('original node endpoint is invalid')
        evidence.update(path=str(layout), sha256=sha256(raw), atomic=False,
                        diagnostic_authority=False,
                        expected=dict(data_dir=str(data), port=str(port), host=host))
        pidfile = data / 'postmaster.pid'
        pid_raw = pidfile.read_bytes()
        evidence['postmaster_pidfile'] = dict(path=str(pidfile), sha256=sha256(pid_raw),
                                              text=pid_raw.decode())
        statfile = self.proc_root / str(pid) / 'stat'
        stat_raw = statfile.read_bytes()
        evidence['producer_stat'] = dict(path=str(statfile), sha256=sha256(stat_raw),
                                          text=stat_raw.decode())
        lines = pid_raw.decode().splitlines()
        stat = stat_raw.decode().rsplit(')', 1)[1].split()
        pid_text = lines[0] if lines else None
        postmaster_pid = int(pid_text) if pid_text and pid_text.isdigit() else None
        pid_valid = postmaster_pid is not None and postmaster_pid > 1
        try:
            parent_pid = int(stat[1])
        except (ValueError, IndexError):
            parent_pid = None
        observed = dict(postmaster_pid=pid_text, producer_parent_pid=parent_pid,
                        data_dir=lines[1] if len(lines) > 1 else None,
                        port=lines[3] if len(lines) > 3 else None,
                        host=lines[4] if len(lines) > 4 else None)
        checks = dict(pidfile_complete=len(lines) >= 5, postmaster_pid_valid=pid_valid,
                      data_directory_matches=lines[1] == str(data) if len(lines) > 1 else None,
                      port_matches=lines[3] == str(port) if len(lines) > 3 else None,
                      socket_directory_matches=lines[4] == host if len(lines) > 4 else None,
                      producer_parent_valid=parent_pid is not None,
                      producer_parent_matches=parent_pid == postmaster_pid
                        if pid_valid and parent_pid is not None else None)
        evidence.update(observed=observed, checks=checks,
                        failed_predicates=[key for key, value in checks.items() if value is False],
                        unknown_predicates=[key for key, value in checks.items() if value is None])
        if not all(value is True for value in checks.values()):
            raise ValueError('producer and original postmaster endpoint disagree')
        evidence['postmaster_pid'] = postmaster_pid
        return node, evidence

    def _process_sample(self, pid):
        result = {}
        try:
            fields = (self.proc_root / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
            result.update(utime_ticks=int(fields[11]), stime_ticks=int(fields[12]))
        except (OSError, ValueError, IndexError) as exc:
            result['cpu_unknown'] = str(exc)
        try:
            for line in (self.proc_root / str(pid) / 'io').read_text().splitlines():
                key, value = line.split(':', 1)
                if key in ('read_bytes', 'write_bytes', 'cancelled_write_bytes', 'rchar', 'wchar'):
                    result[key] = int(value)
        except (OSError, ValueError) as exc:
            result['io_unknown'] = str(exc)
        return result

    def _sample(self, number, target, slot):
        started = self.clock()
        row = dict(case=number, slot=slot, skipped_slots=slot-target['sample_slot']-1,
                   monotonic_started=started, wall_started=time.time(),
                   context_sha256=target['context_sha256'], birth=target['birth'],
                   status='UNKNOWN', atomic_graph=False, diagnostic_authority=False)
        target['sample_slot'] = slot
        pid = target['context']['producer_pid']
        try:
            if (target['birth'] is None or self._birth(pid) != target['birth']
                    or self._context(number)[1] != target['context_sha256']):
                raise ValueError('producer/context identity changed')
            row['endpoint'] = {}
            node, endpoint = self._endpoint(pid, row['endpoint'])
            if sha256(self.psql.read_bytes()) != self.identity['psql_sha256']:
                raise ValueError('frozen SQL client changed')
            sql = ("SELECT row_to_json(s) FROM (SELECT pid,backend_start::text,query_start::text,"
                   "query,state,backend_xid::text,wait_event_type,wait_event,"
                   "pg_blocking_pids(pid) AS blockers,clock_timestamp()::text AS observed_at,"
                   "current_setting('cluster.node_id')::int AS node FROM pg_stat_activity WHERE pid="
                   + str(pid) + ') s')
            command = [str(self.psql), '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U',
                       pwd.getpwuid(self.uid).pw_name, '-h', node['host'], '-p', str(node['port']),
                       '-d', 'postgres', '-c', sql]
            environment = dict(os.environ)
            for key in list(environment):
                if key.startswith('PG') or key == 'LD_PRELOAD':
                    environment.pop(key)
            environment['PGOPTIONS'] = ('-c default_transaction_read_only=on '
                                        '-c statement_timeout=250ms -c lock_timeout=100ms')
            if self._stopped():
                raise ValueError('controller ended before timeline read')
            reply = self.timeline_runner(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, timeout=SQL_TIMEOUT, env=environment)
            row.update(command=command, endpoint=endpoint, returncode=reply.returncode,
                       stdout=reply.stdout.decode(errors='replace') if isinstance(reply.stdout, bytes)
                              else reply.stdout,
                       stderr=reply.stderr.decode(errors='replace') if isinstance(reply.stderr, bytes)
                              else reply.stderr)
            if reply.returncode:
                raise OSError('short timeline query failed')
            activity = json.loads(reply.stdout)
            row['activity'] = activity
            if (not isinstance(activity, dict) or type(activity.get('pid')) is not int
                    or activity['pid'] != pid or activity.get('node') != 0
                    or not activity.get('backend_start') or not activity.get('query_start')):
                raise ValueError('producer activity identity unavailable')
            current = (activity['backend_start'], activity['query_start'], activity.get('query'))
            if target['query_identity'] is not None and current != target['query_identity']:
                raise ValueError('producer statement identity changed')
            if activity.get('backend_xid') != str(target['context']['xid'] & 0xffffffff):
                raise ValueError('producer transaction identity changed')
            if re.fullmatch(r'UPDATE m2_roll SET v=v\+1;?', activity.get('query', '')):
                if target['query_identity'] is None:
                    target['query_identity'] = current
                row['status'] = 'SAMPLED' if activity.get('state') == 'active' else 'STATEMENT_ENDED'
                target['ended'] = activity.get('state') != 'active'
            else:
                row['status'] = 'NOT_ACTIVE_YET'
            row['process'] = self._process_sample(pid)
            if self._birth(pid) != target['birth'] or self._context(number)[1] != target['context_sha256']:
                raise ValueError('producer identity changed during timeline read')
        except (ValueError, KeyError, IndexError) as exc:
            row.update(status='IDENTITY_UNKNOWN', error=str(exc))
            target['ended'] = True
        except (OSError, subprocess.TimeoutExpired) as exc:
            row.update(status='SAMPLE_UNKNOWN', error=str(exc))
        row['elapsed_seconds'] = self.clock()-started
        target['last_sample'] = row
        self.timeline.append(row)
        with (self.out / f'case-{number}-timeline.jsonl').open('a') as output:
            output.write(json.dumps(row) + '\n')

    def step(self):
        if self._stopped():
            return []
        before = len(self.records)
        for number in CASES:
            if self._stopped():
                break
            if number not in self.targets:
                try:
                    context, digest = self._context(number)
                except (OSError, ValueError):
                    continue
                try:
                    birth = self._birth(context['producer_pid'])
                    key = (birth['pid'], birth['starttime'])
                    if key in self.births:
                        birth = None
                    else:
                        self.births.add(key)
                except (OSError, ValueError):
                    birth = None
                self.targets[number] = dict(context=context, context_sha256=digest, birth=birth,
                    anchor=self.clock(), points=0, sample_slot=-1, query_identity=None,
                    last_sample=None, ended=False)
            target = self.targets[number]
            elapsed = self.clock()-target['anchor']
            if target['ended'] or elapsed > WINDOW:
                continue
            slot = math.floor(elapsed)
            if slot > target['sample_slot']:
                self._sample(number, target, slot)
            elapsed = self.clock()-target['anchor']
            point = target['points']
            while point < len(POINTS) and elapsed > POINTS[point] + POINT_TOLERANCE:
                target['points'] += 1
                record = dict(case=number, point=target['points'], scheduled_seconds=POINTS[point],
                    status='MISSED_CAPTURE_POINT', attach_attempted=False,
                    diagnostic_authority=False, monotonic_started=self.clock())
                self.records.append(record)
                self._save(record)
                point = target['points']
            if target['ended'] or point >= len(POINTS) or elapsed < POINTS[point]:
                continue
            last = target['last_sample']
            if last and last['status'] == 'SAMPLED':
                self.capture_deadline = target['anchor'] + POINTS[point] + POINT_TOLERANCE
                try:
                    self._capture(number, target)
                finally:
                    self.capture_deadline = None
                self.records[-1].update(scheduled_seconds=POINTS[point], anchor=target['anchor'],
                    query_identity=target['query_identity'],
                    sampled_query_monotonic=last['monotonic_started'], raw_shared_image_atomic=False)
                self._save(self.records[-1])
            else:
                target['points'] += 1
                record = dict(case=number, point=target['points'], scheduled_seconds=POINTS[point],
                    status='NOT_CAPTURED_QUERY_UNKNOWN', attach_attempted=False,
                    diagnostic_authority=False, monotonic_started=self.clock())
                self.records.append(record)
                self._save(record)
        return self.records[before:]

    def summary(self):
        return dict(super().summary(), profile='run15', timeline_samples=len(self.timeline),
                    maximum_timeline_samples=182, points_seconds=POINTS,
                    timing_anchor='first observed rollover context, not SQL dispatch',
                    targets={str(n):dict(context=t['context'], context_sha256=t['context_sha256'],
                        birth=t['birth'], anchor=t['anchor'], points=t['points'],
                        sample_slot=t['sample_slot'], ended=t['ended'])
                        for n, t in self.targets.items()},
                    extra_capacity_SQL=0, extra_CURRENT_SQL=0, extra_S2_SQL=0)


# Preserve the existing frozen adapter's factory contract.
RolloverObserver = Run15Observer
