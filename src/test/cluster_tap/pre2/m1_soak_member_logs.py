#!/usr/bin/env python3
"""Check four closed native stderr logs without assigning workload credit.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat

from workload_log import SEVERITY


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def check_logs(paths):
    """Scan each declared member once; identities are declarations, not proofs."""
    if len(paths) != 4:
        raise ValueError('exactly four closed member logs are required')
    members, seen, signatures = [], set(), []
    for node, supplied in enumerate(paths):
        path = Path(supplied)
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('member log is not a regular non-symlink file: '+str(path))
        counts = dict(ERROR=0, FATAL=0, PANIC=0)
        first, digest, size = {}, hashlib.sha256(), 0
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if _signature(opened) != _signature(before):
                raise ValueError('member log changed before reading: '+str(path))
            identity = (opened.st_dev, opened.st_ino)
            if identity in seen:
                raise ValueError('member logs alias the same file: '+str(path))
            seen.add(identity)
            for number, raw in enumerate(stream, 1):
                if not raw.endswith(b'\n'):
                    raise ValueError('unterminated member log line: '+str(path))
                text = raw.decode('utf-8')
                digest.update(raw)
                severity = SEVERITY.search(text)
                if severity is not None and severity[1] in counts:
                    name = severity[1]
                    counts[name] += 1
                    if name not in first:
                        first[name] = dict(line=number, byte_offset=size,
                            byte_length=len(raw), sha256=hashlib.sha256(raw).hexdigest(),
                            raw_hex=raw[:4096].hex())
                        if len(raw) > 4096:
                            first[name]['raw_truncated'] = True
                size += len(raw)
            if (_signature(os.fstat(stream.fileno())) != _signature(opened)
                    or size != opened.st_size):
                raise ValueError('member log changed while reading: '+str(path))
        signatures.append((path, _signature(opened)))
        members.append(dict(declared_node=node, path=str(path), size_bytes=size,
            sha256=digest.hexdigest(), counts=counts, first_by_severity=first))
    for path, signature in signatures:
        if _signature(path.lstat()) != signature:
            raise ValueError('member log changed during the four-file scan: '+str(path))
    return dict(member_logs_clean=not any(any(row['counts'].values()) for row in members),
        members=members, identity_binding='DECLARED_ONLY', p07='NOT_READY',
        stage_acceptance='NOT_READY')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('logs', type=Path, nargs=4, metavar='MEMBER_LOG',
                        help='closed node 0, 1, 2 and 3 stderr files, in that order')
    args = parser.parse_args()
    try:
        result = check_logs(args.logs)
    except (OSError, ValueError) as error:
        result = dict(member_logs_clean=False, error=str(error),
            identity_binding='DECLARED_ONLY', p07='NOT_READY', stage_acceptance='NOT_READY')
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['member_logs_clean'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
