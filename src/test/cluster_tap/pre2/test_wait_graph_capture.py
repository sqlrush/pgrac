"""Actual collector policy; no database or workload in these tests."""
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
import wait_graph_capture as graph


class GraphTests(unittest.TestCase):
    def test_pcm_active_service_is_candidate_and_never_an_exact_cycle(self):
        key = dict(tag=[1,2,3,4,5], requester=0, request_seq=9, formation=7,
                   master_session=8, base=0, expected_master=2, request_known=True)
        requester = dict(node=0, pid=10, procno=1, backend_id=2, backend_start=3,
                         proc_generation=17, pcm_wait=dict(key), locks=[],
                         pcm_service=dict(key, requester=1, source=0))
        service = dict(node=1, pid=20, procno=2, backend_id=-1, backend_start=0,
                       proc_generation=18, pcm_service=dict(key, wire_sender_connection=42, source=1),
                       pcm_wait=dict(key, requester=1), locks=[])
        snapshots = [dict(node=n, owners=[row]) for n, row in enumerate((requester, service))]
        result = graph.build_graph(snapshots, [])
        self.assertEqual(len(result['edges']), 2)
        self.assertTrue(all(x['quality']=='candidate_pcm_service' for x in result['edges']))
        self.assertEqual(result['cycles'], [])
        self.assertFalse(result['complete'])
        self.assertEqual(result['edges'][0]['resource']['service_wire_sender_connection'], 42)
        # Distinct transport namespaces are not silently compared or filled in.
        requester['pcm_wait']['requester_connection'] = 99
        self.assertEqual(len(graph.build_graph(snapshots, [])['edges']), 2)
        for field in ('tag', 'requester', 'request_seq', 'formation', 'master_session', 'base', 'expected_master'):
            original = service['pcm_service'][field]
            service['pcm_service'][field] = [1,2,3,0,5] if field=='tag' else original+1
            self.assertEqual(len(graph.build_graph(snapshots, [])['edges']), 1, field)
            service['pcm_service'][field] = original
        for field in ('request_seq', 'formation', 'master_session'):
            service['pcm_service'][field] = 0
            self.assertEqual(len(graph.build_graph(snapshots, [])['edges']), 1, field)
            service['pcm_service'][field] = key[field]
        service['stable'] = False
        self.assertEqual(graph.build_graph(snapshots, [])['edges'], [])

    def test_capture_queries_service_tag_without_a_wait_or_hold(self):
        tag = [1,2,3,4,5]
        class Reader:
            def __init__(self, node, directory): self.node = node['node']
            def query(self, sql, deadline):
                if sql.endswith('_prepare()'): return 't'
                if 'pgrac_wg_block(' in sql:
                    return graph.json.dumps(dict(kind='pcm', node=self.node, status=2))
                return graph.json.dumps(dict(node=self.node, epoch_begin=7, epoch_end=7, stats={},
                    owners=[dict(node=self.node, pid=10+self.node, procno=1, backend_id=-1,
                                 backend_start=0, proc_generation=17, pcm_service=dict(tag=tag))]))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)], Path(directory)/'capture')
            try:
                self.assertFalse(session.capture()['complete'])
                raw = graph.json.loads((session.output/'capture.json').read_text())
                self.assertEqual(raw['resource_keys'], [['pcm', tag]])
                self.assertEqual(len(raw['resources']), 2)
            finally:
                session.close()

    def test_native_content_cycle_requires_real_owner_modes(self):
        owners = [dict(node=0, pid=10, procno=1, backend_id=2, backend_start=3, proc_generation=17,
                       locks=[dict(tag=[1, 2, 3, 0, 5], mode=0)],
                       native_wait=dict(tag=[1, 2, 3, 4, 1], mode=0)),
                  dict(node=0, pid=20, procno=2, backend_id=3, backend_start=4, proc_generation=17,
                       locks=[dict(tag=[1, 2, 3, 4, 1], mode=0)],
                       native_wait=dict(tag=[1, 2, 3, 0, 5], mode=0))]
        result = graph.build_graph([dict(node=0, owners=owners, procs=[], stats={})], [])
        self.assertEqual(len(result['cycles']), 1)
        self.assertTrue(all(e['quality'] == 'exact_local_content' for e in result['edges']))
        owners[1]['node'] = 1
        result = graph.build_graph([dict(node=0, owners=owners, procs=[], stats={})], [])
        self.assertEqual(result['cycles'], [])

    def test_idle_wait_is_not_an_unpublished_resource_wait(self):
        snapshot = dict(node=0, owners=[], stats={}, procs=[dict(procno=1, pid=10,
                        wait_event_info=1, wait_name='ClientRead')])
        self.assertTrue(graph.build_graph([snapshot], [])['complete'])
        snapshot['procs'][0]['wait_name'] = 'BufferContent'
        self.assertFalse(graph.build_graph([snapshot], [])['complete'])

    def test_ges_candidate_does_not_prove_backend_incarnation_or_cycle(self):
        owners = [dict(node=n, pid=10+n, procno=1, backend_id=2, backend_start=3, proc_generation=17,
                       locks=[], enqueue=dict(resid=[1,2,3,n,4,1], mode=8,
                           holder=[n,1,7,0], request=10+n, wait_seq=20+n)) for n in range(2)]
        snapshots = [dict(node=n, owners=[owners[n]], epoch_begin=7, epoch_end=7) for n in range(2)]
        resources = [dict(kind='enqueue', node=n, resid=owners[n]['enqueue']['resid'], status=0,
                         members=[dict(kind=1, id=[n,1,7,10+n], mode=8, wait_seq=20+n),
                                  dict(kind=0, id=[1-n,1,7,101+n], mode=8)]) for n in range(2)]
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(len(result['edges']), 2)
        self.assertEqual(result['cycles'], [])
        self.assertFalse(result['complete'])
        resources[0]['members'][0]['wait_seq'] += 1
        self.assertEqual(len(graph.build_graph(snapshots, resources)['edges']), 1)

    def test_absent_nonmaster_is_distinct_from_busy_or_unstable(self):
        absent = [dict(kind='enqueue', status=1), dict(kind='pcm', status=2)]
        self.assertTrue(graph.build_graph([], absent)['complete'])
        for resource in (dict(kind='enqueue', status=5), dict(kind='pcm', status=0, stable=False)):
            self.assertFalse(graph.build_graph([], [resource])['complete'])

    def test_changed_owner_or_census_cannot_form_a_content_cycle(self):
        owner = dict(node=0, pid=10, procno=1, backend_id=2, backend_start=3, proc_generation=17,
                     stable=False, locks=[], native_wait=dict(tag=[1,2,3,0,1], mode=0))
        result = graph.build_graph([dict(node=0, owners=[owner])], [])
        self.assertFalse(result['complete'])
        self.assertEqual(result['vertices'], [])
        owner['stable'] = True
        census = dict(procno=1, pid=11, backend_id=2, stable=True)
        result = graph.build_graph([dict(node=0, owners=[owner], procs=[census])], [])
        self.assertEqual(result['vertices'], [])

    def test_cycle_search_handles_deep_chain_without_recursion(self):
        edges = [dict(from_=str(n), to=str(n+1), quality='exact_local_content') for n in range(3000)]
        for edge in edges:
            edge['from'] = edge.pop('from_')
        edges.append(dict(to='1500', quality='exact_local_content', **{'from': '3000'}))
        cycles = graph._cycles(edges)
        self.assertEqual(len(cycles), 1)
        self.assertEqual(cycles[0][0], cycles[0][-1])

    def test_four_node_capture_preserves_resource_error_and_closes_owned_readers(self):
        events = []
        class Reader:
            def __init__(self, node, directory):
                self.node = node['node']
            def query(self, sql, deadline):
                if sql.endswith('_prepare()'):
                    events.append(('prepare', self.node))
                    return 't'
                if '_block(' in sql:
                    if self.node == 3 and '(0,0,0,0,0)' not in sql and not sql.startswith('PREPARE'):
                        raise RuntimeError('busy connection')
                    return graph.json.dumps(dict(kind='pcm', node=self.node, status=2))
                owner = dict(node=self.node, pid=10+self.node, procno=1, backend_id=2,
                             backend_start=3, proc_generation=17, locks=[dict(tag=[1,2,3,0,1], mode=1)])
                return graph.json.dumps(dict(node=self.node, epoch_begin=7, epoch_end=7,
                                             owners=[owner], stats=dict(available=True)))
            def close(self):
                events.append(('close', self.node))
        config = [dict(node=n, connstr='node%d' % n) for n in range(4)]
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession(config, Path(directory)/'capture')
            try:
                result = session.capture()
                self.assertFalse(result['complete'])
                raw = graph.json.loads((session.output/'capture.json').read_text())
                self.assertTrue(any('busy connection' in x for x in raw['errors']))
            finally:
                session.close()
        self.assertEqual(events[:4], [('prepare', n) for n in range(4)])
        self.assertEqual(events[-4:], [('close', n) for n in range(4)])

    def test_busy_or_live_ko_responsibility_is_not_an_empty_graph(self):
        for ko in (dict(status=1, rows=[]), dict(status=0, omitted=1, rows=[]),
                   dict(status=0, rows=[dict(kind=0, pid=10, failed=True)])):
            self.assertFalse(graph.build_graph([dict(node=0, owners=[], ko=ko)], [])['complete'])

    def test_actual_ges_and_tx_names_require_published_wait_target(self):
        for name in ('ClusterGesS4Wait', 'ClusterGesReplyWait', 'GesTxEnqueueWait'):
            snapshot = dict(node=0, owners=[], stats=dict(available=True),
                            procs=[dict(procno=1, pid=10, wait_event_info=1, wait_name=name)])
            self.assertFalse(graph.build_graph([snapshot], [])['complete'], name)

    def test_four_connections_to_one_actual_node_are_never_complete(self):
        class Reader:
            def __init__(self, node, directory): self.node = node['node']
            def query(self, sql, deadline):
                if sql.endswith('_prepare()'): return 't'
                return graph.json.dumps(dict(node=0, epoch_begin=7, epoch_end=7,
                                             owners=[], stats=dict(available=True)))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)],
                                           Path(directory)/'capture')
            try:
                self.assertFalse(session.capture()['complete'])
                raw = graph.json.loads((session.output/'capture.json').read_text())
                self.assertTrue(any('node identity' in f.get('error', '')
                                    for cut in raw['rounds'] for f in cut))
            finally:
                session.close()

    def test_hw_grant_requires_exact_live_identity_and_canonical_master(self):
        grant = dict(resid=[1,2,3,0,242,1], holder=[1,2,7,101], mode=7)
        waiter = dict(node=0, pid=10, procno=1, backend_id=2, backend_start=3, proc_generation=17, locks=[],
                      enqueue=dict(resid=grant['resid'], holder=[0,1,7,0], request=10,
                                   wait_seq=20, mode=7, epoch=7))
        holder = dict(node=1, pid=11, procno=2, backend_id=3, backend_start=4, proc_generation=17, locks=[],
                      grants=[grant], omitted_grants=0)
        snapshots = [dict(node=n, epoch_begin=7, epoch_end=7, owners=[row])
                     for n,row in enumerate([waiter, holder])]
        resource = dict(kind='enqueue', node=2, resid=grant['resid'], status=0,
                        route_known=True, master=2, master_generation=1, shard_phase=0, epoch=7,
                        members=[dict(kind=1, id=[0,1,7,10], mode=7, wait_seq=20, lock_group=0),
                                 dict(kind=0, id=[1,2,7,101], mode=7, lock_group=0)])
        result = graph.build_graph(snapshots, [resource])
        self.assertEqual(result['edges'][0]['quality'], 'exact_ges_holder')
        for mutation in ('wrong_request', 'nonmaster', 'unknown_route', 'wrong_epoch', 'non_hw_ko'):
            changed = graph.json.loads(graph.json.dumps(resource))
            if mutation == 'wrong_request': changed['members'][1]['id'][3] += 1
            if mutation == 'nonmaster': changed['master'] = 3
            if mutation == 'unknown_route': changed['route_known'] = False
            if mutation == 'wrong_epoch': changed['epoch'] += 1
            if mutation == 'non_hw_ko':
                changed['resid'] = [1,2,3,0,1,1]
                waiter['enqueue']['resid'] = changed['resid']
                grant['resid'] = changed['resid']
            result = graph.build_graph(snapshots, [changed])
            self.assertFalse(any(e['quality'] == 'exact_ges_holder' for e in result['edges']), mutation)

    def test_ges_native_parallel_group_cannot_be_a_blocker(self):
        resid = [1,2,3,0,242,1]
        waiter = dict(node=0, pid=10, procno=1, backend_id=2, backend_start=3, proc_generation=17, locks=[],
                      enqueue=dict(resid=resid, holder=[0,1,7,0], request=10, epoch=7,
                                   wait_seq=20, mode=7))
        holder = dict(node=0, pid=11, procno=2, backend_id=3, backend_start=4, proc_generation=17, locks=[],
                      grants=[dict(resid=resid, holder=[0,2,7,101], mode=7)])
        resource = dict(kind='enqueue', node=2, resid=resid, status=0, epoch=7,
                        route_known=True, master=2, master_generation=1, shard_phase=0,
                        members=[dict(kind=1, id=[0,1,7,10], mode=7, wait_seq=20, lock_group=3),
                                 dict(kind=0, id=[0,2,7,101], mode=7, lock_group=0)])
        result = graph.build_graph([dict(node=0, epoch_begin=7, epoch_end=7,
                                         owners=[waiter, holder])], [resource])
        self.assertEqual(result['edges'], [])

    def test_exact_cross_node_hw_ko_cycle_uses_full_local_grants(self):
        keys = [[1,2,3,0,242,1], [1,2,3,0,246,1]]
        owners, resources = [], []
        for node in range(2):
            owners.append(dict(node=node, pid=10+node, procno=1, backend_id=2, backend_start=3, proc_generation=17,
                               enqueue=dict(resid=keys[node], holder=[node,1,7,0], request=10+node,
                                            wait_seq=20+node, mode=7, epoch=7),
                               grants=[dict(resid=keys[1-node], holder=[node,1,7,100+node], mode=7)]))
            resources.append(dict(kind='enqueue', node=2+node, resid=keys[node], status=0, epoch=7,
                                  route_known=True, master=2+node, master_generation=0, shard_phase=0,
                                  members=[dict(kind=1, id=[node,1,7,10+node], mode=7, wait_seq=20+node),
                                           dict(kind=0, id=[1-node,1,7,101-node], mode=7)]))
        snapshots = [dict(node=n, epoch_begin=7, epoch_end=7, owners=[owners[n]]) for n in range(2)]
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(len(result['cycles']), 1)
        self.assertTrue(all(e['quality'] == 'exact_ges_holder' for e in result['edges']))
        owners[1]['grants'][0]['holder'][3] += 1
        self.assertEqual(graph.build_graph(snapshots, resources)['cycles'], [])

    def test_capture_queries_held_grant_keys_without_an_enqueue_wait(self):
        resid = [1,2,3,0,242,1]
        class Reader:
            def __init__(self, node, directory): self.node = node['node']
            def query(self, sql, deadline):
                if sql.endswith('_prepare()'): return 't'
                if 'pgrac_wg_enqueue(' in sql:
                    return graph.json.dumps(dict(kind='enqueue', node=self.node, resid=resid, status=1))
                return graph.json.dumps(dict(node=self.node, epoch_begin=7, epoch_end=7, stats={},
                    owners=[dict(node=self.node, pid=10+self.node, procno=1, backend_id=2, backend_start=3, proc_generation=17,
                                 grants=[dict(resid=resid, holder=[self.node,1,7,101], mode=7)])]))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)],
                                           Path(directory)/'capture')
            try:
                self.assertFalse(session.capture()['complete'])
                raw = graph.json.loads((session.output/'capture.json').read_text())
                self.assertEqual(raw['resource_keys'], [['enqueue', resid]])
                self.assertEqual(len(raw['resources']), 2)
            finally:
                session.close()

    def test_auxiliary_cycle_requires_native_generation_and_matching_census(self):
        owners = [dict(node=0, pid=10+n, procno=1+n, backend_id=-1 if n else 2,
                       backend_start=0 if n else 3, proc_generation=17+n,
                       locks=[dict(tag=[1,2,3,0,n], mode=0)],
                       native_wait=dict(tag=[1,2,3,0,1-n], mode=0)) for n in range(2)]
        census = [dict(procno=row['procno'], pid=row['pid'], backend_id=row['backend_id'],
                       proc_generation=row['proc_generation'], stable=True) for row in owners]
        snapshot = dict(node=0, owners=owners, procs=census)
        result = graph.build_graph([snapshot], [])
        self.assertEqual(len(result['cycles']), 1)
        self.assertIn('g18', result['edges'][0]['to'])
        census[1]['proc_generation'] += 1
        result = graph.build_graph([snapshot], [])
        self.assertEqual(result['cycles'], [])
        self.assertTrue(any(x['reason']=='process_census_changed' for x in result['unknown']))
        census[1]['proc_generation'] -= 1
        for invalid in (0, None):
            owners[1]['proc_generation'] = invalid
            result = graph.build_graph([snapshot], [])
            self.assertEqual(result['cycles'], [])
            self.assertTrue(any(x['reason']=='owner_native_identity_unproven' for x in result['unknown']))

    def test_owned_pipe_reader_frames_split_output_and_passes_native_dbname(self):
        # A real owned pipe process checks the connection argument and emits
        # split stdout chunks; it does not connect to any database.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root/'psql-fixture'
            executable.write_text("#!/usr/bin/env python3\n"
                "import sys\n"
                "if '-d' not in sys.argv or sys.argv[sys.argv.index('-d')+1] != 'host=fixture dbname=postgres': sys.exit(2)\n"
                "for line in sys.stdin:\n"
                " if line.startswith('\\\\echo '): print(line[6:].strip(), flush=True)\n"
                " else:\n"
                "  sys.stdout.write('{\\\"node\\\":'); sys.stdout.flush()\n"
                "  sys.stdout.write('0}\\n'); sys.stdout.flush()\n")
            executable.chmod(0o700)
            with patch.object(graph.shutil, 'which', return_value=str(executable)):
                reader = graph.PsqlReader(dict(node=0, connstr='host=fixture dbname=postgres'), root)
                try:
                    for _ in range(2):
                        self.assertEqual(graph.json.loads(reader.query('SELECT fixture()', graph.time.monotonic()+5)),
                                         dict(node=0))
                finally:
                    reader.close()
            self.assertIsNotNone(reader.process.poll())

    def test_prepare_refusal_closes_all_owned_connections(self):
        closed = []
        class Reader:
            def __init__(self, node, directory): self.node = node['node']
            def query(self, sql, deadline): return 'f'
            def close(self): closed.append(self.node)
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            with self.assertRaisesRegex(RuntimeError, 'prepare unavailable'):
                graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)], Path(directory)/'capture')
        self.assertEqual(closed, list(range(4)))

    def test_collect_before_cleanup_never_changes_failure(self):
        events = []
        def collect():
            events.append('capture')
            raise RuntimeError('observer unavailable')
        result = graph.capture_failed_watchdog(collect)
        events.append('terminate')
        self.assertEqual(events, ['capture', 'terminate'])
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('observer unavailable', result['capture_error'])


if __name__ == '__main__':
    unittest.main()
