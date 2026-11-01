"""Exercise the bounded collector using synthetic SQL, processes and debugger."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_m2_rollover_observer as legacy


class Run15Observation(unittest.TestCase):
    def setUp(self):
        self.f = legacy.RolloverObservation()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.sql_calls, self.debug_calls = [], []
        self.row_change, self.sql_error = {}, None
        self.sql_elapsed = 0
        psql = self.f.base / 'install/bin/psql'
        psql.write_bytes(b'synthetic SQL client, never executed')
        self.f.identity['psql_sha256'] = legacy.digest(psql.read_bytes())
        layout = self.f.run / 'testdata/cohort/blackbox-layout.json'
        layout.parent.mkdir(parents=True)
        data = self.f.run / 'testdata/node0/pgdata'
        data.mkdir(parents=True)
        self.host = self.f.base / 'socket'
        self.host.mkdir()
        (data / 'postmaster.pid').write_text(f'42\n{data}\n1234\n18000\n{self.host}\n')
        layout.write_text(json.dumps(dict(nodes=[dict(id=0, host=str(self.host),
                                                     port=18000, data_dir=str(data))])))
        self.listener(18000)
        for pid in (1011, 1012):
            stat = self.f.proc / str(pid) / 'stat'
            stat.write_text(stat.read_text().replace(') S 0 ', ') S 42 ', 1))
            (self.f.proc / str(pid) / 'io').write_text('read_bytes: 20\nwrite_bytes: 30\n')

    def listener(self, port, *, pid=42, start=1234):
        path = self.host / f'.s.PGSQL.{port}'
        listener = socket.socket(socket.AF_UNIX)
        self.addCleanup(listener.close)
        listener.bind(str(path))
        listener.listen(1)
        lock = Path(str(path) + '.lock')
        data = self.f.run / 'testdata/node0/pgdata'
        lock.write_text(f'{pid}\n{data}\n{start}\n{port}\n{self.host}\n')
        return lock

    def test_stale_layout_port_uses_the_postmaster_owned_listener(self):
        layout = self.f.run / 'testdata/cohort/blackbox-layout.json'
        value = json.loads(layout.read_text())
        value['nodes'][0]['port'] = 19000
        layout.write_text(json.dumps(value))
        observer = self.observer(); observer.step()
        self.assertEqual(observer.timeline[0]['status'], 'SAMPLED')
        command = self.sql_calls[0][0]
        self.assertEqual(command[command.index('-p') + 1], '18000')

    def test_stale_pidfile_port_uses_the_matching_live_socket_not_the_layout(self):
        pidfile, _ = self.endpoint_inputs()
        pidfile.write_text(pidfile.read_text().replace('\n18000\n', '\n5432\n'))
        observer = self.observer(); observer.step()
        row = observer.timeline[0]
        self.assertEqual(row['status'], 'SAMPLED')
        self.assertEqual(row['endpoint']['observed']['port'], '5432')
        self.assertEqual(row['endpoint']['listener']['port'], 18000)
        self.assertFalse(row['endpoint']['listener']['pidfile_port_matches'])

    def test_socket_for_another_postmaster_is_not_an_endpoint(self):
        (self.host / '.s.PGSQL.18000.lock').write_text('99\n/foreign\n1234\n18000\n')
        observer = self.observer(); observer.step()
        self.assertEqual(observer.timeline[0]['status'], 'IDENTITY_UNKNOWN')
        self.assertEqual(self.sql_calls, [])

    def test_socket_of_another_start_or_two_matching_listeners_are_ambiguous(self):
        lock = self.host / '.s.PGSQL.18000.lock'
        lock.write_text(lock.read_text().replace('\n1234\n', '\n1235\n'))
        observer = self.observer(); observer.step()
        self.assertEqual(observer.timeline[0]['status'], 'IDENTITY_UNKNOWN')
        self.assertEqual(self.sql_calls, [])

    def test_two_owned_listener_ports_are_not_guessed(self):
        self.listener(18001)
        observer = self.observer(); observer.step()
        self.assertEqual(observer.timeline[0]['status'], 'IDENTITY_UNKNOWN')
        self.assertEqual(self.sql_calls, [])

    def test_live_server_port_must_match_the_selected_socket(self):
        self.row_change['server_port'] = 18001
        observer = self.observer(); observer.step()
        self.assertEqual(observer.timeline[0]['status'], 'IDENTITY_UNKNOWN')
        self.f.now = 80; observer.step()
        self.assertEqual(self.debug_calls, [])

    def test_nonregular_socket_lock_cannot_block_endpoint_validation(self):
        lock = self.host / '.s.PGSQL.18000.lock'
        lock.unlink()
        os.mkfifo(lock)
        self.f.manifest.write_text(json.dumps(self.f.identity))
        # The original collector runs in an owned child so a blocked open is
        # a finite test failure, not a hanging regression suite.
        code = '''import os,sys
from pathlib import Path
from m2_run15_observer import Run15Observer
o=Run15Observer(sys.argv[1],sys.argv[2],sys.argv[3],uid=os.getuid(),proc_root=Path(sys.argv[4]))
try:
 o._endpoint(1011,{})
except ValueError:
 print('REFUSED')
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(self.f.base),
            str(self.f.manifest), legacy.digest(self.f.manifest.read_bytes()), str(self.f.proc)],
            cwd=Path(__file__).parent, capture_output=True, text=True, timeout=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), 'REFUSED')

    def observer(self):
        try:
            import m2_run15_observer
        except ModuleNotFoundError:
            self.fail('the approved bounded run15 collector is not implemented')
        self.f.manifest.write_text(json.dumps(self.f.identity))
        try:
            return m2_run15_observer.RolloverObserver(self.f.base, self.f.manifest,
                legacy.digest(self.f.manifest.read_bytes()), uid=os.getuid(),
                proc_root=self.f.proc, clock=lambda: self.f.now,
                runner=self.debugger, timeline_runner=self.sql)
        except TypeError as exc:
            self.fail('the existing adapter cannot construct the run15 collector: ' + str(exc))

    def sql(self, command, **kwargs):
        self.sql_calls.append((command, kwargs))
        if self.sql_error:
            raise self.sql_error
        pid = 1011 if 'pid=1011' in command[-1] else 1012
        row = dict(pid=pid, node=0, server_port=int(command[command.index('-p') + 1]),
                   backend_start='birth', query_start='query-start',
                   query='UPDATE m2_roll SET v=v+1;', state='active',
                   backend_xid=str(101 if pid == 1011 else 102),
                   wait_event_type='LWLock', wait_event='test', blockers=[], observed_at='wall')
        row.update(self.row_change)
        self.f.now += self.sql_elapsed
        return subprocess.CompletedProcess(command, 0, json.dumps(row).encode(), b'')

    def debugger(self, command, **kwargs):
        self.debug_calls.append((command, kwargs))
        # Missing symbols stay UNKNOWN; no real debugger is executed.
        return subprocess.CompletedProcess(command, 0, b'No symbol fixture\n')

    def test_capture_points_are_30_and_80_not_the_old_one_second_gap(self):
        observer = self.observer()
        for second, expected in ((0, 0), (1, 0), (29, 0), (30, 2), (31, 2), (79, 2), (80, 4)):
            self.f.now = second; observer.step()
            self.assertEqual(len(self.debug_calls), expected)
        self.assertEqual(observer.attachments, 4)
        self.f.now = 90; observer.step(); observer.step()
        self.assertEqual(len(self.debug_calls), 4)
        self.assertTrue(all(r['status'] == 'PARTIAL_UNKNOWN' for r in observer.records))

    def test_each_target_has_at_most_91_independent_short_read_only_samples(self):
        observer = self.observer()
        for second in range(100):
            self.f.now = second; observer.step(); observer.step()
        self.assertEqual(len(self.sql_calls), 182)
        for command, options in self.sql_calls:
            self.assertEqual(options['timeout'], 0.75)
            self.assertIn('-XAtq', command)
            sql = command[-1]
            self.assertIn('pg_stat_activity', sql)
            self.assertIn('pg_blocking_pids(pid)', sql)
            self.assertNotIn('BEGIN', sql)
            self.assertNotIn('pg_cluster_state', sql)
            self.assertNotIn('cluster_dump_state', sql)
            self.assertIn('default_transaction_read_only=on', options['env']['PGOPTIONS'])
        row = observer.timeline[0]
        self.assertEqual(row['process']['read_bytes'], 20)
        self.assertFalse(row['atomic_graph'])

    def test_delayed_step_records_missing_slots_without_a_catchup_sql_burst(self):
        observer = self.observer(); observer.step()
        self.f.now = 10; observer.step()
        self.assertEqual(len(self.sql_calls), 4)
        self.assertEqual(observer.timeline[-1]['skipped_slots'], 9)

    def test_missed_30_second_point_is_not_replayed_beside_the_80_second_point(self):
        observer = self.observer(); observer.step()
        self.f.now = 79; observer.step()
        self.assertEqual(len(self.debug_calls), 0)
        self.f.now = 80; observer.step(); observer.step()
        self.assertEqual(len(self.debug_calls), 2)
        self.assertEqual(len(observer.records), 4)
        missed = [r for r in observer.records if r['status'] == 'MISSED_CAPTURE_POINT']
        self.assertEqual(len(missed), 2)
        self.assertTrue(all(r['scheduled_seconds'] == 30 for r in missed))

    def test_changed_sql_client_or_original_postmaster_blocks_all_probes(self):
        observer = self.observer()
        (self.f.base / 'install/bin/psql').write_bytes(b'changed client')
        observer.step(); self.f.now = 80; observer.step()
        self.assertEqual((self.sql_calls, self.debug_calls), ([], []))
        self.assertTrue(all(t['ended'] for t in observer.targets.values()))

    def endpoint_inputs(self):
        return (self.f.run / 'testdata/node0/pgdata/postmaster.pid',
                self.f.proc / '1011/stat')

    def endpoint_refusal(self):
        (self.f.run / 'm2-context-8.json').unlink()
        observer = self.observer()
        observer.step(); self.f.now = 80; observer.step()
        self.assertEqual((self.sql_calls, self.debug_calls), ([], []))
        self.assertTrue(observer.targets[4]['ended'])
        rows = (observer.out / 'case-4-timeline.jsonl').read_text().splitlines()
        self.assertEqual(len(rows), 1)
        row = json.loads(rows[0])
        self.assertEqual(row['status'], 'IDENTITY_UNKNOWN')
        self.assertEqual(row['error'], 'producer and original postmaster endpoint disagree')
        self.assertFalse(row['atomic_graph'])
        self.assertIn('endpoint', row, 'endpoint refusal must retain its compared inputs')
        return row['endpoint']

    def test_valid_endpoint_retains_the_exact_bytes_used_by_its_checks(self):
        observer = self.observer(); observer.step()
        endpoint = observer.timeline[0]['endpoint']
        self.assertIn('postmaster_pidfile', endpoint, 'successful checks must retain their input bytes')
        pidfile, stat = self.endpoint_inputs()
        self.assertEqual(endpoint['postmaster_pidfile']['text'], pidfile.read_text())
        self.assertEqual(endpoint['postmaster_pidfile']['sha256'], legacy.digest(pidfile.read_bytes()))
        self.assertEqual(endpoint['producer_stat']['text'], stat.read_text())
        self.assertEqual(endpoint['producer_stat']['sha256'], legacy.digest(stat.read_bytes()))
        self.assertEqual(endpoint['expected'], dict(data_dir=str(pidfile.parent),
                                                   port=18000, host=str(self.host)))
        self.assertTrue(all(endpoint['checks'].values()))
        self.assertEqual(endpoint['failed_predicates'], [])
        self.assertEqual(endpoint['unknown_predicates'], [])
        self.assertFalse(endpoint['atomic'])
        self.assertFalse(endpoint['diagnostic_authority'])

    def test_parent_refusal_records_both_compared_pids_without_running_probes(self):
        _, stat = self.endpoint_inputs()
        stat.write_text(stat.read_text().replace(') S 42 ', ') S 999 ', 1))
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['producer_parent_matches'])
        self.assertEqual(evidence['observed']['producer_parent_pid'], 999)
        self.assertEqual(evidence['observed']['postmaster_pid'], '42')
        self.assertEqual(evidence['producer_stat']['text'], stat.read_text())

    def test_data_directory_refusal_preserves_the_unmodified_path(self):
        pidfile, _ = self.endpoint_inputs()
        lines = pidfile.read_text().splitlines()
        lines[1] = '/other/pgdata'
        pidfile.write_text('\n'.join(lines) + '\n')
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['data_directory_matches'])
        self.assertEqual(evidence['observed']['data_dir'], '/other/pgdata')
        self.assertEqual(evidence['expected']['data_dir'], str(pidfile.parent))

    def test_invalid_port_refusal_preserves_the_observed_port_text(self):
        pidfile, _ = self.endpoint_inputs()
        pidfile.write_text(pidfile.read_text().replace('\n18000\n', '\ninvalid\n'))
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['port_valid'])
        self.assertEqual(evidence['observed']['port'], 'invalid')
        self.assertEqual(evidence['expected']['port'], 18000)

    def test_socket_refusal_preserves_the_unmodified_directory(self):
        pidfile, _ = self.endpoint_inputs()
        pidfile.write_text(pidfile.read_text().replace(str(self.host), '/tmp/other-socket'))
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['socket_directory_matches'])
        self.assertEqual(evidence['observed']['host'], '/tmp/other-socket')
        self.assertEqual(evidence['expected']['host'], str(self.host))

    def test_short_pidfile_marks_unavailable_fields_unknown_without_guessing(self):
        pidfile, _ = self.endpoint_inputs()
        pidfile.write_text(f'42\n{pidfile.parent}\n')
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['pidfile_complete'])
        self.assertEqual(evidence['unknown_predicates'], ['port_valid', 'socket_directory_matches'])
        self.assertIsNone(evidence['observed']['port'])
        self.assertIsNone(evidence['observed']['host'])

    def test_invalid_postmaster_pid_does_not_manufacture_a_parent_match(self):
        pidfile, _ = self.endpoint_inputs()
        pidfile.write_text(pidfile.read_text().replace('42\n', 'not-a-pid\n', 1))
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['postmaster_pid_valid'])
        self.assertEqual(evidence['unknown_predicates'], ['producer_parent_matches'])
        self.assertEqual(evidence['observed']['postmaster_pid'], 'not-a-pid')

    def test_invalid_producer_parent_does_not_hide_other_endpoint_mismatches(self):
        pidfile, stat = self.endpoint_inputs()
        stat.write_text(stat.read_text().replace(') S 42 ', ') S unknown ', 1))
        pidfile.write_text(pidfile.read_text().replace('\n18000\n', '\ninvalid\n'))
        evidence = self.endpoint_refusal()
        self.assertEqual(evidence['failed_predicates'], ['port_valid', 'producer_parent_valid'])
        self.assertEqual(evidence['unknown_predicates'], ['producer_parent_matches'])
        self.assertIsNone(evidence['observed']['producer_parent_pid'])
        self.assertIn(') S unknown ', evidence['producer_stat']['text'])

    def test_missing_second_point_preserves_the_first_point_evidence(self):
        observer = self.observer(); observer.step()
        self.f.now = 30; observer.step()
        original = (observer.out / 'case-4-point-1.json').read_bytes()
        self.f.now = 83; observer.step()
        self.assertEqual((observer.out / 'case-4-point-1.json').read_bytes(), original)
        missed = json.loads((observer.out / 'case-4-point-2.json').read_text())
        self.assertEqual((missed['status'], missed['scheduled_seconds']), ('MISSED_CAPTURE_POINT', 80))
        self.assertEqual(len(self.debug_calls), 2)

    def test_missing_both_points_does_not_attach_after_either_window(self):
        observer = self.observer(); observer.step()
        self.f.now = 83; observer.step(); observer.step()
        self.assertEqual(self.debug_calls, [])
        self.assertEqual(len(observer.records), 4)
        self.assertTrue(all(r['status'] == 'MISSED_CAPTURE_POINT' for r in observer.records))

    def test_query_time_counts_against_the_capture_window(self):
        observer = self.observer(); observer.step()
        self.f.now = 30; observer.step()
        self.f.now = 81.9; self.sql_elapsed = 0.2; observer.step()
        self.assertEqual(len(self.debug_calls), 2)
        self.assertTrue(all(r['status'] == 'MISSED_CAPTURE_POINT'
                            for r in observer.records if r['point'] == 2))

    def test_libpq_environment_cannot_redirect_the_observer_connection(self):
        polluted = dict(PGHOSTADDR='192.0.2.1', PGSERVICE='foreign-service',
                        PGTARGETSESSIONATTRS='read-write', PGOPTIONS='-c search_path=foreign')
        with patch.dict(os.environ, polluted):
            observer = self.observer(); observer.step()
            self.assertEqual(os.environ['PGHOSTADDR'], '192.0.2.1')
        for _, options in self.sql_calls:
            self.assertNotIn('PGHOSTADDR', options['env'])
            self.assertNotIn('PGSERVICE', options['env'])
            self.assertNotIn('PGTARGETSESSIONATTRS', options['env'])
            self.assertIn('default_transaction_read_only=on', options['env']['PGOPTIONS'])

    def test_export_failure_leaves_the_controller_and_attach_budget_untouched(self):
        observer = self.observer()
        (observer.out / 'case-4-timeline.jsonl').mkdir()
        with self.assertRaises(OSError): observer.step()
        self.assertEqual(observer.attachments, 0)
        self.assertEqual(self.debug_calls, [])
        self.assertFalse((self.f.run / 'controller-result.json').exists())

    def test_query_or_backend_identity_change_stops_sampling_and_attaching(self):
        for field, value in (('pid', 999), ('node', 1), ('backend_start', 'other'),
                             ('query_start', 'other'), ('query', 'SELECT 1'),
                             ('backend_xid', '999')):
            with self.subTest(field=field):
                # Each trial needs the collector's exclusive output directory.
                with self.subTest(value=value):
                    fixture = self.f
                    if (fixture.base / 'rollover-observation').exists():
                        self.tearDown(); self.setUp()
                    observer = self.observer(); observer.step()
                    self.row_change[field] = value
                    self.f.now = 1; observer.step()
                    before = len(self.sql_calls)
                    self.f.now = 80; observer.step()
                    self.assertEqual(len(self.sql_calls), before)
                    self.assertEqual(self.debug_calls, [])
                    self.assertTrue(all(t['ended'] for t in observer.targets.values()))

    def test_process_exit_or_reuse_does_not_follow_the_new_process(self):
        observer = self.observer(); observer.step()
        self.f.birth(1011, 999)
        self.f.now = 30; observer.step()
        self.assertEqual(len(self.debug_calls), 1)
        self.assertEqual(int(self.debug_calls[0][0][self.debug_calls[0][0].index('-p') + 1]), 1012)

    def test_unknown_sql_sample_never_authorizes_an_attach_or_retry(self):
        self.sql_error = subprocess.TimeoutExpired('synthetic psql', 0.75)
        observer = self.observer()
        self.f.now = 0; observer.step()
        self.f.now = 30; observer.step()
        self.f.now = 80; observer.step()
        self.assertEqual(len(self.sql_calls), 6)
        self.assertEqual(self.debug_calls, [])
        self.assertEqual(len(observer.records), 4)

    def test_debugger_remains_read_only_and_contains_only_direct_memory_reads(self):
        observer = self.observer(); observer.step()
        self.f.now = 30; observer.step()
        command = self.debug_calls[0][0]
        for capability in ('functions', 'memory', 'registers'):
            self.assertIn('set may-' + ('call-' if capability == 'functions' else 'write-')
                          + capability + ' off', command)
        for expression in ('p cv_sleep_target', 'p &undo_cleaner_state->capacity_cv',
                           'p *my_wait_event_info', 'p *MyProc'):
            self.assertIn(expression, command)
        self.assertFalse(any(s.startswith(('call ', 'signal ', 'break ')) for s in command))


if __name__ == '__main__':
    unittest.main()
