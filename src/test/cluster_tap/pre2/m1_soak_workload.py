#!/usr/bin/env python3
"""Print the fixed native soak workload plan without starting any processes.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import json


def workload_plan():
    setup = []
    for client in range(64):
        setup.extend([
            f'CREATE TABLE m1_soak_{client} (client_id integer PRIMARY KEY, value bigint NOT NULL);',
            f'INSERT INTO m1_soak_{client} VALUES ({client}, 0);'])
    return dict(status='NOT_RUN', p07='NOT_READY', stage_acceptance='NOT_READY',
        missing_gates=['real recycle generation progress', 'complete four-resource census'],
        writer_node=0, readers=[0, 1, 2, 3],
        extra_conf=['autovacuum=off', 'max_connections=100',
                    'cluster.undo_cleaner_interval_ms=1000',
                    'log_error_verbosity=verbose', "log_line_prefix='%m [%p] '"],
        setup_sql='\n'.join(setup),
        row_cut_sql=' UNION ALL '.join(
            f'SELECT client_id, value FROM m1_soak_{client}' for client in range(64))
            +' ORDER BY client_id;',
        cases=[dict(clients=clients, pgbench_arguments=[
            '-n', '-M', 'simple', '-j', '2', '-c', str(clients),
            '-T', '3600', '--max-tries=1', '-l']) for clients in (16, 48, 64)])


if __name__ == '__main__':
    argparse.ArgumentParser(description=__doc__).parse_args()
    print(json.dumps(workload_plan(), indent=2))
