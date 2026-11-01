"""Host collector integration with real owned pipes, UDP and graph consumers."""
import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

import wait_graph_capture as graph


# Only the external SQL server is controlled. PsqlReader, its framing and
# process ownership, CaptureSession, persistence and graph joining are real.
PSQL_FIXTURE = r'''
import json
from pathlib import Path
import sys

root = Path(__file__).parent
node = int(sys.argv[sys.argv.index('-d') + 1].removeprefix('node='))
plan = json.loads((root / 'responses.json').read_text())
local_reads = 0
with (root / ('queries-%d.jsonl' % node)).open('w') as log:
    for line in sys.stdin:
        sql = line.strip()
        if sql.startswith('\\echo '):
            print(sql[6:], flush=True)
            continue
        log.write(json.dumps(sql) + '\n')
        log.flush()
        if sql == 'SELECT test_pgrac_wait_graph_prepare();':
            print('t', flush=True)
        elif sql == 'EXECUTE pgrac_wg_local;':
            local_reads += 1
            cut = max(0, min(local_reads - 2, 1))
            if local_reads == 2 and node == plan.get('error_node'):
                print('ERROR: h31 first diagnostic query failure', file=sys.stderr, flush=True)
                sys.exit(1)
            print(json.dumps(plan['snapshots'][cut][node]), flush=True)
        elif sql.startswith('EXECUTE pgrac_wg_block('):
            print(json.dumps(plan['resources'][node]), flush=True)
        elif sql.startswith('EXECUTE pgrac_wg_enqueue('):
            print(json.dumps(dict(kind='enqueue', node=node, status=1)), flush=True)
        elif not sql.startswith('PREPARE '):
            print('unexpected SQL: ' + sql, file=sys.stderr, flush=True)
            sys.exit(2)
'''


def snapshots():
    return [dict(version=2, node=n, epoch_begin=7, epoch_end=7,
                 atomic_graph=False, observed_at=100,
                 owners=[], rejected_slots=[], procs=[],
                 stats=dict(available=True, scanned=16, busy=0, changed=0,
                            dead=0, owners=0, omitted_locks=0),
                 ko=dict(status=0, omitted=0, rows=[])) for n in range(4)]


def owner(*, pid=101, procno=2, generation=9007199254740993):
    return dict(node=0, pid=pid, procno=procno, backend_id=3, backend_start=4,
                proc_generation=generation, revision=11, omitted_locks=0,
                locks=[], omitted_grants=0, grants=[], native_wait=None,
                enqueue=None, pcm_wait=None, pcm_service=None, tx_wait=None,
                ko_wait=None, retry_wait=None)


class H31Tests(unittest.TestCase):
    def keep(self, paths):
        """An optional evidence directory keeps outputs, never child programs."""
        destination = os.environ.get('K_P130_H31_OUTPUT')
        if destination is None:
            return
        root = Path(destination) / self._testMethodName
        root.mkdir(parents=True, exist_ok=True)
        for path in paths:
            if path.is_file():
                shutil.copyfile(path, root / path.name)

    @contextmanager
    def collector(self, before=None, after=None, *, error_node=None, resources=None):
        before = snapshots() if before is None else before
        after = copy.deepcopy(before) if after is None else after
        if resources is None:
            resources = [dict(kind='pcm', node=n, status=2,
                              omitted_blocks=0, blocks=[]) for n in range(4)]
        with tempfile.TemporaryDirectory(prefix='k-p130-h31-') as directory:
            root = Path(directory)
            executable = root / 'psql-fixture'
            executable.write_text('#!' + sys.executable + '\n' + PSQL_FIXTURE)
            executable.chmod(0o700)
            (root / 'responses.json').write_text(json.dumps(dict(
                snapshots=[before, after], resources=resources, error_node=error_node)))
            config = [dict(node=n, connstr='node=%d' % n) for n in range(4)]
            with patch.object(graph.shutil, 'which', return_value=str(executable)):
                session = graph.CaptureSession(config, root / 'capture')
            try:
                yield session
            finally:
                session.close()
                for reader in session.readers:
                    self.assertIsNotNone(reader.process.poll())
                    self.assertTrue(reader.process.stdin.closed)
                    self.assertTrue(reader.process.stdout.closed)
                    self.assertTrue(reader.stderr.closed)
                queries = [json.loads(line) for n in range(4)
                           for line in (root / ('queries-%d.jsonl' % n)).read_text().splitlines()]
                print('# H31 ' + json.dumps(dict(method=self._testMethodName,
                      readers_reaped=4, queries=len(queries)), sort_keys=True))
                self.keep(list(session.output.iterdir()) + list(root.glob('queries-*.jsonl')))

    def capture(self, before=None, after=None, **kwargs):
        with self.collector(before, after, **kwargs) as session:
            result = session.capture(budget_seconds=5)
            raw = json.loads((session.output / 'capture.json').read_text())
            evidence = json.loads((session.output / 'wait-for-graph.json').read_text())
            self.assertEqual(len(raw['rounds']), 2)
            self.assertEqual([len(cut) for cut in raw['rounds']], [4, 4])
            self.assertEqual(result['complete'], evidence['complete'])
            self.assertEqual(raw['graph_complete'], evidence['complete'])
            self.assertEqual(raw['status'], 'DIAGNOSTIC_ONLY')
            self.assertFalse(evidence['atomic_graph'])
            print('# H31 ' + json.dumps(dict(method=self._testMethodName,
                  complete=evidence['complete'], edges=len(evidence['edges']),
                  unknown=[row['reason'] for row in evidence['unknown']]), sort_keys=True))
            return result, raw, evidence

    def incomplete(self, before, reason, **kwargs):
        result, raw, evidence = self.capture(before, **kwargs)
        self.assertFalse(result['complete'])
        self.assertIn(reason, [row['reason'] for row in evidence['unknown']])
        return raw, evidence

    def receive(self, rows):
        with tempfile.TemporaryDirectory(prefix='k-p130-h31-udp-') as directory:
            path = Path(directory) / 'udp.log'
            receiver = graph.DeadlineReceiver(1, 0, path)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                    for payload in rows:
                        sender.sendto(payload, receiver.socket.getsockname())
            finally:
                result = receiver.close()
            raw = path.read_bytes()
            self.assertEqual(json.loads(path.with_suffix('.json').read_text()), result)
            self.assertEqual(result['completeness_scope'], 'datagram_stream_only')
            sidecar = json.loads(path.with_suffix('.os.json').read_text())
            self.assertFalse(sidecar['native_incarnation_bound'])
            self.assertEqual(sidecar['termination_receipt'], 'UNKNOWN')
            print('# H31 ' + json.dumps(dict(method=self._testMethodName,
                  stream_complete=result['complete'], records=len(receiver.rows),
                  errors=result['errors']), sort_keys=True))
            self.keep(list(Path(directory).iterdir()))
            return result, raw

    def test_quiet_capture_uses_four_owned_readers_and_two_cuts(self):
        result, raw, evidence = self.capture()
        self.assertTrue(result['complete'])
        self.assertEqual(evidence['unknown'], [])
        self.assertEqual(evidence['vertices'], [])
        self.assertEqual(evidence['edges'], [])
        self.assertEqual(raw['errors'], [])
        self.assertEqual([frame['node'] for frame in raw['rounds'][0]], [0, 1, 2, 3])

    def test_exact_content_cycle_survives_real_pipe_framing(self):
        before = snapshots()
        left, right = owner(), owner(pid=102, procno=3, generation=9007199254740995)
        left['locks'] = [dict(lifetime=21, buffer=1, mode=0, tag=[1, 2, 3, 0, 4])]
        left['native_wait'] = dict(lifetime=22, buffer=2, mode=0, tag=[1, 2, 3, 0, 5])
        right['locks'] = [dict(lifetime=23, buffer=2, mode=0, tag=[1, 2, 3, 0, 5])]
        right['native_wait'] = dict(lifetime=24, buffer=1, mode=0, tag=[1, 2, 3, 0, 4])
        before[0]['owners'] = [left, right]
        result, raw, evidence = self.capture(before)
        self.assertTrue(result['complete'])
        self.assertEqual(len(evidence['edges']), 2)
        self.assertEqual(len(evidence['cycles']), 1)
        self.assertEqual({e['quality'] for e in evidence['edges']}, {'exact_local_content'})
        self.assertEqual(raw['rounds'][0][0]['value']['owners'][0]['proc_generation'],
                         9007199254740993)

    def test_content_observation_cap_is_incomplete(self):
        before = snapshots()
        before[0]['stats']['omitted_locks'] = 1
        self.incomplete(before, 'omitted_locks')

    def test_grant_observation_cap_is_incomplete(self):
        before = snapshots()
        capped = owner()
        capped['omitted_grants'] = 1
        before[0]['owners'] = [capped]
        self.incomplete(before, 'omitted_grants')

    def test_ko_observation_cap_is_incomplete(self):
        before = snapshots()
        before[0]['ko']['omitted'] = 1
        self.incomplete(before, 'ko_snapshot_unavailable_or_capped')

    def test_pcm_observation_cap_queries_resource_and_remains_incomplete(self):
        before = snapshots()
        current = owner()
        current['locks'] = [dict(lifetime=21, buffer=1, mode=0, tag=[1, 2, 3, 0, 4])]
        before[0]['owners'] = [current]
        resources = [dict(kind='pcm', node=n, status=2, omitted_blocks=0, blocks=[])
                     for n in range(4)]
        resources[0].update(status=0, omitted_blocks=1)
        raw, evidence = self.incomplete(before, 'resource_unavailable_or_changed', resources=resources)
        self.assertEqual(raw['resource_keys'], [['pcm', [1, 2, 3, 0, 4]]])
        self.assertEqual(len(raw['resources']), 2)
        self.assertEqual([len(cut['frames']) for cut in raw['resources']], [4, 4])
        self.assertEqual(evidence['edges'], [])

    def test_unknown_native_owner_cannot_qualify_an_empty_graph(self):
        before = snapshots()
        before[0]['owners'] = [owner(generation=0)]
        _, evidence = self.incomplete(before, 'owner_native_identity_unproven')
        self.assertEqual(evidence['vertices'], [])
        self.assertEqual(evidence['edges'], [])

    def test_missing_content_holder_remains_unknown(self):
        before = snapshots()
        waiter = owner()
        waiter['native_wait'] = dict(lifetime=21, buffer=1, mode=0, tag=[1, 2, 3, 0, 4])
        before[0]['owners'] = [waiter]
        _, evidence = self.incomplete(before, 'content_holder_missing')
        self.assertEqual(evidence['edges'], [])

    def test_reused_owner_preserves_original_first_cut(self):
        before = snapshots()
        before[0]['owners'] = [owner()]
        after = copy.deepcopy(before)
        after[0]['owners'][0].update(pid=102, proc_generation=9007199254740995)
        raw, evidence = self.incomplete(before, 'owner_changed_or_wrong_node', after=after)
        self.assertEqual(raw['rounds'][0][0]['value']['owners'][0]['pid'], 101)
        self.assertEqual(raw['rounds'][1][0]['value']['owners'][0]['pid'], 102)
        self.assertEqual(evidence['vertices'], [])

    def test_first_query_error_preserves_partial_cuts_and_owned_readers(self):
        with self.collector(error_node=1) as session:
            result = session.capture(budget_seconds=5)
            raw = json.loads((session.output / 'capture.json').read_text())
            evidence = json.loads((session.output / 'wait-for-graph.json').read_text())
            self.assertFalse(result['complete'])
            self.assertFalse(evidence['complete'])
            self.assertIn('capture_incomplete', [row['reason'] for row in evidence['unknown']])
            self.assertEqual(raw['rounds'][0][1]['status'], 'ERROR')
            self.assertEqual(raw['rounds'][0][0]['status'], 'OK')
            self.assertEqual(raw['rounds'][1][0]['status'], 'OK')
            self.assertIn('ERROR: h31 first diagnostic query failure',
                          (session.output / 'reader-node1.stderr').read_text())
            self.assertEqual(raw['status'], 'DIAGNOSTIC_ONLY')

    def test_original_watchdog_failure_survives_a_complete_diagnostic(self):
        with self.collector() as session:
            result = graph.capture_failed_watchdog(lambda: session.capture(budget_seconds=5))
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['reason'], 'original_watchdog_expired')
            self.assertTrue(result['capture']['complete'])
            self.assertIsNone(result['capture_error'])

    def test_original_watchdog_failure_survives_capture_write_error(self):
        with self.collector() as session:
            (session.output / 'capture.json').mkdir()
            result = graph.capture_failed_watchdog(lambda: session.capture(budget_seconds=5))
            self.assertEqual(result['status'], 'FAIL')
            self.assertIsNotNone(result['capture_error'])
            self.assertNotIn('capture', result)

    def test_udp_repeated_captures_keep_exact_bytes_and_sequence_domains(self):
        rows = []
        for capture in (1, 2):
            rows.extend([
                ('node=1 observer=42 sequence=1 capture=%d PGRAC_WAIT_GCS_BEGIN node=1\n' % capture).encode(),
                ('node=1 observer=42 sequence=2 capture=%d PGRAC_WAIT_GCS_SLOT node=1 slot=0 source_request=9007199254740993\n' % capture).encode(),
                ('node=1 observer=42 sequence=3 capture=%d PGRAC_WAIT_GCS_END node=1 dropped=0\n' % capture).encode()])
        rows.extend([b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=1\n',
                     b'node=1 observer=42 sequence=2 PGRAC_WAIT_DEADLINE_END node=1 dropped=0\n'])
        result, raw = self.receive(rows)
        self.assertTrue(result['complete'])
        self.assertEqual(raw, b''.join(rows))
        self.assertEqual([(o['family'], o['capture'], o['records']) for o in result['observers']],
                         [('GCS', 1, 3), ('GCS', 2, 3), ('DEADLINE', 0, 2)])

    def test_udp_missing_sequence_remains_incomplete(self):
        result, raw = self.receive([
            b'node=1 observer=42 sequence=1 capture=1 PGRAC_WAIT_GCS_BEGIN node=1\n',
            b'node=1 observer=42 sequence=3 capture=1 PGRAC_WAIT_GCS_END node=1 dropped=0\n'])
        self.assertFalse(result['complete'])
        self.assertIn(b'sequence=3', raw)

    def test_udp_dropped_records_remain_incomplete(self):
        result, raw = self.receive([
            b'node=1 observer=42 sequence=1 capture=1 PGRAC_WAIT_GCS_BEGIN node=1\n',
            b'node=1 observer=42 sequence=2 capture=1 PGRAC_WAIT_GCS_END node=1 dropped=1\n'])
        self.assertFalse(result['complete'])
        self.assertIn(b'dropped=1', raw)

    def test_udp_missing_end_remains_incomplete(self):
        result, _ = self.receive([
            b'node=1 observer=42 sequence=1 capture=1 PGRAC_WAIT_GCS_BEGIN node=1\n'])
        self.assertFalse(result['complete'])

    def test_udp_truncation_remains_incomplete_and_keeps_received_bytes(self):
        payload = b'node=1 observer=42 sequence=1 ' + b'x' * 4096 + b'\n'
        result, raw = self.receive([payload])
        self.assertFalse(result['complete'])
        self.assertEqual(raw, payload[:4096])
        self.assertIn('malformed_or_wrong_node_datagram', result['errors'])

    def test_complete_udp_stream_does_not_resolve_unknown_native_owner(self):
        result, _ = self.receive([
            b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=1\n',
            b'node=1 observer=42 sequence=2 PGRAC_WAIT_DEADLINE_END node=1 dropped=0\n'])
        self.assertTrue(result['complete'])
        before = snapshots()
        before[0]['owners'] = [owner(generation=0)]
        self.incomplete(before, 'owner_native_identity_unproven')

    def test_udp_missing_begin_remains_incomplete(self):
        result, _ = self.receive([
            b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_END node=1 dropped=0\n'])
        self.assertFalse(result['complete'])

    def test_empty_udp_stream_does_not_qualify(self):
        result, raw = self.receive([])
        self.assertFalse(result['complete'])
        self.assertEqual(result['observers'], [])
        self.assertEqual(raw, b'')

    def test_udp_wrong_node_is_retained_as_invalid_evidence(self):
        payload = b'node=2 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=2\n'
        result, raw = self.receive([payload])
        self.assertFalse(result['complete'])
        self.assertEqual(raw, payload)
        self.assertIn('malformed_or_wrong_node_datagram', result['errors'])

    def test_transport_completeness_does_not_erase_observation_cap(self):
        result, raw = self.receive([
            b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=1\n',
            b'node=1 observer=42 sequence=2 PGRAC_WAIT_DEADLINE_END node=1 available=1 scanned=16 owners=1 busy=0 changed=0 dead=0 omitted=1 dropped=0\n'])
        self.assertTrue(result['complete'])
        self.assertIn(b'omitted=1', raw)
        before = snapshots()
        before[0]['stats']['omitted_locks'] = 1
        self.incomplete(before, 'omitted_locks')


METHODS = (
    'test_quiet_capture_uses_four_owned_readers_and_two_cuts',
    'test_exact_content_cycle_survives_real_pipe_framing',
    'test_content_observation_cap_is_incomplete',
    'test_grant_observation_cap_is_incomplete',
    'test_ko_observation_cap_is_incomplete',
    'test_pcm_observation_cap_queries_resource_and_remains_incomplete',
    'test_unknown_native_owner_cannot_qualify_an_empty_graph',
    'test_missing_content_holder_remains_unknown',
    'test_reused_owner_preserves_original_first_cut',
    'test_first_query_error_preserves_partial_cuts_and_owned_readers',
    'test_original_watchdog_failure_survives_a_complete_diagnostic',
    'test_original_watchdog_failure_survives_capture_write_error',
    'test_udp_repeated_captures_keep_exact_bytes_and_sequence_domains',
    'test_udp_missing_sequence_remains_incomplete',
    'test_udp_dropped_records_remain_incomplete',
    'test_udp_missing_end_remains_incomplete',
    'test_udp_truncation_remains_incomplete_and_keeps_received_bytes',
    'test_complete_udp_stream_does_not_resolve_unknown_native_owner',
    'test_udp_missing_begin_remains_incomplete',
    'test_empty_udp_stream_does_not_qualify',
    'test_udp_wrong_node_is_retained_as_invalid_evidence',
    'test_transport_completeness_does_not_erase_observation_cap',
)


def main():
    discovered = unittest.defaultTestLoader.getTestCaseNames(H31Tests)
    if len(METHODS) != 22 or len(set(METHODS)) != 22 or set(discovered) != set(METHODS):
        print('Bail out! H31 method plan differs from discovery')
        return 1
    print('1..22', flush=True)
    failed = False
    for number, name in enumerate(METHODS, 1):
        result = unittest.TestResult()
        H31Tests(name).run(result)
        ok = (result.testsRun == 1 and result.wasSuccessful() and not result.skipped
              and not result.expectedFailures and not result.unexpectedSuccesses)
        print('%s %d - %s' % ('ok' if ok else 'not ok', number, name), flush=True)
        for _, detail in result.failures + result.errors:
            for line in detail.splitlines():
                print('# ' + line)
        failed |= not ok
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
