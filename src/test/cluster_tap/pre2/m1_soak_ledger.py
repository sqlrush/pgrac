#!/usr/bin/env python3
"""Compare retained raw pgbench commits with four declared member row cuts.

This offline subcheck starts no processes and cannot certify the full soak.
Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat

MAX64 = (1 << 63) - 1
MISSING = ['actual elapsed duration', 'candidate/cohort identity',
           'real recycle generation progress', 'complete four-resource census',
           'original normal-stop proof']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(text, high=MAX64):
    require(re.fullmatch(r'0|[1-9][0-9]*', text) is not None, 'nondecimal ledger field')
    value = int(text)
    require(value <= high, 'ledger field exceeds its native range')
    return value


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON field')
        result[key] = value
    return result


class BoundFiles:
    """Hash while parsing; retain at most one client's sequence per client."""
    def __init__(self, root):
        self.root = root.resolve(strict=True)
        self.seen = set()
        self.observed = []

    def read(self, descriptor, parse):
        require(type(descriptor) is dict and set(descriptor) == {
            'path', 'size_bytes', 'sha256'}, 'invalid retained file descriptor')
        relative = descriptor['path']
        require(type(relative) is str and relative and not Path(relative).is_absolute()
                and '..' not in Path(relative).parts, 'retained path escapes its capture')
        path = self.root/relative
        require(not any(part.is_symlink() for part in (path, *path.parents)),
                'retained path contains a symlink')
        require(path.resolve(strict=True).is_relative_to(self.root),
                'retained file is outside its capture')
        require(type(descriptor['size_bytes']) is int and descriptor['size_bytes'] >= 0
                and type(descriptor['sha256']) is str
                and re.fullmatch('[0-9a-f]{64}', descriptor['sha256']) is not None,
                'invalid retained file size or digest')
        digest, size = hashlib.sha256(), 0
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
            before = os.fstat(stream.fileno())
            identity = before.st_dev, before.st_ino
            require(stat.S_ISREG(before.st_mode) and identity not in self.seen,
                    'retained input is not a distinct regular file')
            self.seen.add(identity)

            def lines():
                nonlocal size
                for raw in stream:
                    digest.update(raw)
                    size += len(raw)
                    yield raw.decode('ascii')

            result = parse(lines())
            require(stream.read(1) == b'', 'retained parser did not consume the full file')
            after = os.fstat(stream.fileno())
            current = path.lstat()
            stamp = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            require(stamp(before) == stamp(after) == stamp(current),
                    'retained input changed during parsing')
        require(size == descriptor['size_bytes'] and digest.hexdigest() == descriptor['sha256'],
                'retained file size or SHA256 differs')
        self.observed.append(dict(descriptor))
        return result


def summary_fields(lines, clients):
    text = ''.join(lines)
    expectations = {r'number of clients: ([0-9]+)': clients,
                    r'number of threads: ([0-9]+)': 2,
                    r'maximum number of tries: ([0-9]+)': 1,
                    r'duration: ([0-9]+) s': 3600,
                    r'number of failed transactions: ([0-9]+) \([0-9.]+%\)': 0}
    for pattern, expected in expectations.items():
        found = re.findall('^'+pattern+'$', text, re.M)
        require(len(found) == 1 and number(found[0]) == expected,
                'pgbench summary parameters or failure count differ')
    require(re.findall(r'^query mode: (\w+)$', text, re.M) == ['simple'],
            'pgbench query mode differs from the fixed workload')
    require(not re.search(r'^number of transactions (?:skipped|retried):|^total number of retries:',
                          text, re.M), 'skipped or retry mode is not the fixed workload')
    found = re.findall(r'^number of transactions actually processed: ([0-9]+)$', text, re.M)
    require(len(found) == 1, 'missing or duplicate pgbench transaction total')
    return number(found[0])


def row_cut(lines):
    rows = {}
    for line in lines:
        require(line.endswith('\n') and len(line.split('|')) == 2, 'invalid member row cut')
        client, value = (number(field) for field in line.rstrip('\n').split('|'))
        require(client < 64 and client not in rows, 'duplicate or invalid member row')
        rows[client] = value
    require(set(rows) == set(range(64)), 'member cut does not cover all 64 heaps')
    return [rows[i] for i in range(64)]


def read_ledger(manifest_path):
    manifest_path = Path(manifest_path)
    request = json.loads(manifest_path.read_text(), object_pairs_hook=unique_object)
    require(type(request) is dict and set(request) == {
        'version', 'clients', 'duration_seconds', 'pgbench_rc', 'workload', 'summary',
        'stderr', 'logs', 'before', 'after'}, 'incomplete or unknown ledger manifest fields')
    require(type(request['version']) is int and request['version'] == 1
            and type(request['clients']) is int and request['clients'] in (16, 48, 64)
            and type(request['duration_seconds']) is int and request['duration_seconds'] == 3600
            and type(request['pgbench_rc']) is int and request['pgbench_rc'] == 0,
            'not a successful fixed one-hour workload invocation')
    require(type(request['logs']) is list and len(request['logs']) == 2
            and all(type(request[key]) is list and len(request[key]) == 4
                    for key in ('before', 'after')), 'missing raw logs or four member cuts')
    files = BoundFiles(manifest_path.parent)
    source_workload = Path(__file__).with_name('m1_soak.sql').read_text()
    workload = files.read(request['workload'], lambda lines: ''.join(lines))
    require(workload == source_workload, 'workload differs from the fixed SQL input')
    clients = request['clients']
    total = files.read(request['summary'], lambda lines: summary_fields(lines, clients))
    require(not files.read(request['stderr'], lambda lines: ''.join(lines)).strip(),
            'pgbench stderr is not empty')
    counts, owners = [0]*clients, [None]*clients

    def raw_log(lines, log_index):
        for line in lines:
            fields = line.split()
            require(line.endswith('\n') and len(fields) == 6,
                    'not a complete six-column raw pgbench record')
            client, sequence, latency, script, seconds, micros = map(number, fields)
            require(client < clients and script == 0 and seconds > 0 and micros < 1000000,
                    'invalid raw pgbench client/script/timestamp')
            require(owners[client] is None or owners[client] == log_index,
                    'client occurs in more than one raw log')
            require(sequence == counts[client] + 1, 'client commit sequence has a gap or duplicate')
            owners[client] = log_index
            counts[client] = sequence

    for index, descriptor in enumerate(request['logs']):
        files.read(descriptor, lambda lines: raw_log(lines, index))
    require(all(count > 0 for count in counts) and sum(counts) == total,
            'missing client progress or summary/ledger commit mismatch')
    expected = counts + [0]*(64-clients)
    for descriptor in request['before']:
        require(files.read(descriptor, row_cut) == [0]*64, 'initial member values are not zero')
    for descriptor in request['after']:
        require(files.read(descriptor, row_cut) == expected,
                'member final values differ from the independent per-client ledger')
    return dict(workload_ledger_passed=True, p07='NOT_READY', stage_acceptance='NOT_READY',
                missing_gates=MISSING, clients=clients, configured_duration_seconds=3600,
                total_commits=total, commits_by_client=counts, verified_files=files.observed,
                identity_binding='DECLARED_NOT_ATTESTED')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    args = parser.parse_args()
    try:
        result = read_ledger(args.manifest)
    except (OSError, ValueError, TypeError, KeyError) as error:
        result = dict(workload_ledger_passed=False, p07='NOT_READY',
                      stage_acceptance='NOT_READY', error=str(error))
    print(json.dumps(result, indent=2))
    return 0 if result['workload_ledger_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
