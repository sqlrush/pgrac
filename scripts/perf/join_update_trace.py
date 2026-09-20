#!/usr/bin/env python3
"""Author: SqlRush. Join client attempts and UPDATE spans by exact identity.

Select the slowest client 20% BEFORE joining. Missing slow records must never
silently turn into a faster, apparently well-attributed cohort. Client and
server monotonic timestamps are not assumed to share an epoch.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

from analyze_update_trace import analyze_records, load_jsonl


def identity(row):
    return tuple(int(row[k]) for k in ('node_id', 'pid', 'backend_sequence'))


def unique_index(rows, side):
    indexed = {}
    for row in rows:
        key = identity(row)
        if key in indexed:
            raise ValueError(f'duplicate {side} identity: {key}')
        indexed[key] = row
    return indexed


def cohort_timing(clients, server_index):
    """Comparable duration accounting; never compare clock epochs across processes."""
    totals = Counter(client_wall_ns=0, client_update_ns=0, client_commit_ns=0,
                     server_executor_ns=0, outside_executor_ns=0, matched_records=0,
                     missing_timing_records=0)
    for client in clients:
        server = server_index.get(identity(client))
        totals['client_wall_ns'] += client['client_total_ns']
        if server is None or 'client_update_end_ns' not in client:
            totals['missing_timing_records'] += 1
            continue
        update = client['client_update_end_ns'] - client['client_start_ns']
        commit = client['client_end_ns'] - client['client_update_end_ns']
        if (update < 0 or commit < 0
                or update + commit != client['client_total_ns']):
            raise ValueError(f'client duration conservation: {identity(client)}')
        if server['status'] != 'ok' or server.get('affected_rows', 1) != 1:
            raise ValueError(f'client/server successful outcome mismatch: {identity(client)}')
        if server['total_ns'] > update:
            raise ValueError(f'server ExecutorRun exceeds client UPDATE: {identity(client)}')
        totals.update(client_update_ns=update, client_commit_ns=commit,
                      server_executor_ns=server['total_ns'],
                      outside_executor_ns=update-server['total_ns'], matched_records=1)
    return dict(totals)


def join_and_analyze(clients, servers):
    metadata = [r for r in clients if r.get('metadata')]
    clients = [r for r in clients if not r.get('metadata')]
    client_index = unique_index(clients, 'client')
    server_index = unique_index(servers, 'server')
    completed = sorted((r for r in clients if r['status'] == 0 and r['measurement']),
                       key=lambda r: (r['client_total_ns'], identity(r)))
    tail = completed[-math.ceil(len(completed) * .20):] if completed else []
    missing = [r for r in clients if identity(r) not in server_index]
    tail_matched = [server_index[identity(r)] for r in tail if identity(r) in server_index]
    measurement_matched = [server_index[identity(r)] for r in completed
                           if identity(r) in server_index]
    dropped = sum(r['dropped'] for r in metadata)
    return {
        'latency_basis': 'client_update_plus_commit',
        'client_status_counts': dict(Counter(str(r['status']) for r in clients)),
        'client_records': len(clients),
        'client_measurement_successes': len(completed),
        'client_p80_ns': completed[math.ceil(len(completed)*.8)-1]['client_total_ns'] if completed else None,
        'client_tail_count': len(tail),
        'client_tail_wall_ns': sum(r['client_total_ns'] for r in tail),
        'client_tail_unmatched': len(tail)-len(tail_matched),
        'unmatched_clients': len(missing),
        'unmatched_servers': len(server_index.keys()-client_index.keys()),
        'client_dropped': dropped,
        'complete_identity_coverage': not missing and not dropped and client_index.keys() == server_index.keys(),
        'client_tail_server_attribution': analyze_records(tail_matched),
        'measurement_server_attribution': analyze_records(measurement_matched),
        'measurement_timing': cohort_timing(completed, server_index),
        'client_tail_timing': cohort_timing(tail, server_index),
        'server_all': analyze_records(servers),
        'unmatched_client_examples': [identity(r) for r in missing[:20]],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clients', type=Path, nargs='+', required=True)
    parser.add_argument('--servers', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    clients = [json.loads(line) for path in args.clients
               for line in path.read_text().splitlines() if line.strip()]
    report = join_and_analyze(clients, load_jsonl(args.servers))
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')


if __name__ == '__main__':
    main()
