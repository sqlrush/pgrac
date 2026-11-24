"""Read ERROR/FATAL/PANIC records and crash indicators from server stderr logs.

Works on retained files; never contacts a server. Plain and verbose
(log_error_verbosity=verbose) stderr formats are accepted. Each record is
attributed to the driver step whose byte range of the log contains it.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from bisect import bisect_right
from pathlib import Path
import re
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

PRIMARY = ('DEBUG', 'LOG', 'INFO', 'NOTICE', 'WARNING', 'ERROR', 'FATAL', 'PANIC')
CONTINUATION = ('DETAIL', 'HINT', 'QUERY', 'CONTEXT', 'LOCATION', 'STATEMENT')
SEVERE = ('ERROR', 'FATAL', 'PANIC')
_TOKEN = re.compile(r'(?<![A-Za-z0-9_])(DEBUG[1-5]?|LOG|INFO|NOTICE|WARNING|ERROR|FATAL|PANIC|'
                    r'DETAIL|HINT|QUERY|CONTEXT|LOCATION|STATEMENT):  ')
_SQLSTATE = re.compile(r'([0-9A-Z]{5}): (.*)', re.S)
_PID = re.compile(r'\[(\d+)\]')
# Postmaster and assertion lines that report an abnormal process exit. The
# first alternatives are the ones the existing fixture stop check refuses.
_CRASH = re.compile(r'terminated by signal|abnormal database system shutdown|reinitializing|'
                    r'server process .*exited with exit code [1-9]')
_TRAP = re.compile(r'^TRAP: |Assertion .* failed')


def parse_line(line: str) -> Optional[Dict[str, object]]:
    """Split one stderr line; None for a line that is not a log record start."""
    if line.startswith('\t'):
        return None
    match = _TOKEN.search(line)
    if match is None:
        return None
    prefix, kind, rest = line[:match.start()], match.group(1), line[match.end():]
    pids = _PID.findall(prefix)
    record: Dict[str, object] = dict(kind=kind, prefix=prefix.strip(),
                                    pid=int(pids[-1]) if pids else None, text=rest)
    if kind not in CONTINUATION:
        coded = _SQLSTATE.fullmatch(rest)
        record['sqlstate'] = coded.group(1) if coded else None
        record['text'] = coded.group(2) if coded else rest
    return record


def phase_at(boundaries: Sequence[Tuple[int, str]], offset: int) -> str:
    """boundaries: ascending (start offset, phase). Bytes before the first
    boundary belong to no driver step and are reported as 'before'."""
    index = bisect_right([b[0] for b in boundaries], offset) - 1
    return boundaries[index][1] if index >= 0 else 'before'


def read_events(path: Path, node: int, boundaries: Sequence[Tuple[int, str]],
                start: int = 0) -> Iterator[Dict[str, object]]:
    """Yield severe records and crash indicators of one node log. A record is
    yielded once its DETAIL/LOCATION/STATEMENT lines are complete, so the
    order is close to, but not exactly, file order; use its offset to sort."""
    pending: Dict[object, Dict[str, object]] = {}
    order: List[object] = []

    def flush(key: object) -> Iterator[Dict[str, object]]:
        event = pending.pop(key, None)
        if event is not None:
            order.remove(key)
            yield event

    offset, number = start, 0
    with Path(path).open('rb') as stream:
        stream.seek(start)
        for raw in stream:
            line_offset, offset, number = offset, offset + len(raw), number + 1
            line = raw.decode('utf-8', errors='replace').rstrip('\r\n')
            record = parse_line(line)
            if record is None:
                if _TRAP.search(line):
                    yield dict(source='server', node=node, severity='CRASH', sqlstate=None,
                               phase=phase_at(boundaries, line_offset), message=line.strip(),
                               pid=None, time='', line=number, offset=line_offset, raw=line)
                continue
            key = record['pid'] if record['pid'] is not None else record['prefix']
            if record['kind'] in CONTINUATION:
                event = pending.get(key)
                if event is not None:
                    field = str(record['kind']).lower()
                    event[field] = ((str(event[field]) + '\n') if event.get(field) else '') \
                        + str(record['text'])
                    event['raw'] = str(event['raw']) + '\n' + line
                continue
            # A new primary record of this process completes its previous one.
            for event in flush(key):
                yield event
            severity = str(record['kind'])
            message = str(record['text'])
            if severity not in SEVERE:
                if not (_CRASH.search(message) or _TRAP.search(message)):
                    continue
                severity = 'CRASH'
            pending[key] = dict(source='server', node=node, severity=severity,
                                sqlstate=record['sqlstate'], message=message,
                                phase=phase_at(boundaries, line_offset), pid=record['pid'],
                                time=_PID.sub('', str(record['prefix'])).strip(),
                                line=number, offset=line_offset, raw=line)
            order.append(key)
            # Bound memory on very long logs: complete the oldest open record.
            if len(order) > 4096:
                for event in flush(order[0]):
                    yield event
    for key in list(order):
        for event in flush(key):
            yield event


def family(event: Dict[str, object]) -> Optional[str]:
    """The emitter family declared in a record's DETAIL, when present."""
    match = re.search(r'PGRAC_FAMILY=([A-Z0-9_]+)', str(event.get('detail') or ''))
    return match.group(1) if match else None


def signature(event: Dict[str, object]) -> str:
    """Group key that ignores identifiers and counters inside a message."""
    first_line = str(event.get('message') or '').split('\n', 1)[0]
    return '%s|%s|%s' % (event.get('severity'), event.get('sqlstate') or '-',
                         re.sub(r'\d+', 'N', first_line)[:240])
