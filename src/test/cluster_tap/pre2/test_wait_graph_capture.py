"""Actual collector policy; no database or workload in these tests."""
import unittest
import copy
import tempfile
from pathlib import Path
from unittest.mock import patch
import wait_graph_capture as graph


class GraphTests(unittest.TestCase):
    def test_deadline_receiver_drains_and_marks_missing_sequence_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            receiver=graph.DeadlineReceiver(1,0,Path(directory)/'deadline.log')
            sender=graph.socket.socket(graph.socket.AF_INET,graph.socket.SOCK_DGRAM)
            address=receiver.socket.getsockname()
            sender.sendto(b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=1\n',address)
            sender.sendto(b'node=1 observer=42 sequence=2 PGRAC_WAIT_DEADLINE_END node=1 dropped=0\n',address)
            result=receiver.close()
            self.assertTrue(result['complete']);self.assertEqual(result['observers'][0]['records'],2)
            receiver=graph.DeadlineReceiver(1,0,Path(directory)/'missing.log')
            address=receiver.socket.getsockname()
            sender.sendto(b'node=1 observer=42 sequence=1 PGRAC_WAIT_DEADLINE_BEGIN node=1\n',address)
            sender.sendto(b'node=1 observer=42 sequence=3 PGRAC_WAIT_DEADLINE_END node=1 dropped=0\n',address)
            self.assertFalse(receiver.close()['complete'])
            sender.close()

    def test_pcm_holder_maps_real_claim_to_its_wait_without_inventing_service(self):
        actor=dict(node=2,pid=29,procno=7,backend_id=2,backend_start=8,
                   proc_generation=31,stable=True,enqueue=dict(lifetime=9,resid=[1,2,3,0,242,1]))
        tag=[1,2,3,4,1]
        resource=dict(kind='pcm',node=2,tag=tag,status=0,stable=True,
                      local_owner=dict(valid=True,state=2,highest_generation=93,
                          handle=dict(tag=tag,requester=2,procno=7,generation=93),
                          claim=dict(pid=29,proc_generation=31,owner_generation=93)))
        holders=graph.pcm_claimed_holders(tag,[resource],{(2,7):actor})
        self.assertEqual(holders[0]['process'],graph.identity(actor))
        self.assertEqual(holders[0]['own_waits']['enqueue'],actor['enqueue'])
        self.assertFalse(holders[0]['wait_dependency_proven'])
        for change in ('pid','proc_generation','owner_generation'):
            bad=copy.deepcopy(resource);bad['local_owner']['claim'][change]+=1
            self.assertEqual(graph.pcm_claimed_holders(tag,[bad],{(2,7):actor}),[])
        resource['stable']=False
        self.assertEqual(graph.pcm_claimed_holders(tag,[resource],{(2,7):actor}),[])

    def test_native_lifetimes_keep_halfchain_despite_diagnostic_updates(self):
        row = dict(node=1,pid=5182,procno=90,backend_id=3,backend_start=4,
                   proc_generation=32, revision=125085,
                   locks=[dict(buffer=9,tag=[1,2,3,0,354],mode=0,lifetime=71)],
                   grants=[dict(resid=[1,2,3,0,242,1],holder=[1,90,1,95210],mode=7,lifetime=72)],
                   pcm_wait=dict(tag=[1,2,3,4,1],binding=1224,request_seq=5812,
                                 lifetime=73,snapshot=30234))
        final=copy.deepcopy(row);final['revision']+=40;final['pcm_wait']['snapshot']+=20
        stable=graph.stabilize_owner(row,final)
        self.assertTrue(stable['stable'])
        self.assertEqual(stable['locks'],row['locks']);self.assertEqual(stable['grants'],row['grants'])
        self.assertEqual(stable['pcm_wait'],row['pcm_wait'])
        self.assertEqual(stable.get('unproven_resources'),[])
        self.assertNotIn('stable',row)  # Raw cuts remain immutable.

    def test_native_lifetimes_reject_aba_zero_missing_and_changed_target(self):
        row=dict(node=1,pid=5182,procno=90,backend_id=3,backend_start=4,proc_generation=32,
                 locks=[dict(buffer=9,tag=[1,2,3,0,354],mode=0,lifetime=71)],
                 retry_wait=dict(buffer=10,tag=None,lifetime=72))
        for token in (0,None,73):
            final=copy.deepcopy(row);final['locks'][0]['lifetime']=token
            stable=graph.stabilize_owner(row,final)
            self.assertTrue(stable['stable']);self.assertEqual(stable['locks'],[])
            self.assertTrue(stable['unproven_resources'])
        final=copy.deepcopy(row);final['retry_wait']['buffer']+=1
        stable=graph.stabilize_owner(row,final)
        self.assertIsNone(stable['retry_wait']);self.assertEqual(len(stable['locks']),1)
        final['proc_generation']+=1
        self.assertFalse(graph.stabilize_owner(row,final)['stable'])

    def pcm_bridge_fixture(self):
        wait = dict(lifetime=41, tag=[1,2,3,4,5], requester=0, request_seq=9, formation=7,
                    master_session=8, base=1, expected_master=2, r4=12,
                    requester_connection=15, ingress_connection=16, transport_known=True)
        service = dict(wait, lifetime=42, authority=3, ordered_lane=4, source=1, state=2, mode=0,
                       source_candidate=1, retain_pi_if_dirty=1, common_flags=1,
                       common_outcome=0, wire_sender_connection=42)
        queue = dict(wait, queue_kind=1, source=1, phase=2, head=5, enqueue_order=6,
                     ordered_lane=4, incompatible=2, blocked=0, request_known=True)
        retained = dict(service, wire_sender_connection=1)
        block = dict(holder=1, decoded=True, neutral_sender=True, slot_matches=True,
                     reject=0, slot=dict(state=1, owner_node=2), request=retained)
        resource = dict(kind='pcm', node=2, status=0, stable=True, omitted_blocks=0,
                        view=dict(tag=wait['tag'], head_requester=0, head_request=9,
                                  head=5, enqueue_order=6), queue=[queue], blocks=[block])
        return wait, service, [resource]

    def test_pcm_logical_bridge_keeps_three_connection_namespaces_separate(self):
        wait, service, resources = self.pcm_bridge_fixture()
        bridge = graph.pcm_logical_bridge(wait, service, resources)
        self.assertEqual(bridge['quality'], 'retained_logical_only')
        self.assertEqual([bridge[k] for k in ('requester_connection', 'master_ingress',
                         'retained_sender', 'service_sender')], [15,16,1,42])
        self.assertFalse(bridge['physical_send_proven'])
        snapshots = [dict(node=0, owners=[dict(node=0,pid=10,procno=1,backend_id=2,
                        backend_start=3,proc_generation=17,pcm_wait=wait)]),
                     dict(node=1, owners=[dict(node=1,pid=20,procno=2,backend_id=-1,
                        backend_start=0,proc_generation=18,pcm_service=service)])]
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(result['edges'][0]['resource']['logical_bridge'], bridge)
        self.assertEqual(result['edges'][0]['quality'], 'candidate_pcm_service')
        self.assertEqual(result['cycles'], []); self.assertFalse(result['complete'])
        # Native DATA completion clears the slot, retaining original payload.
        resources[0]['blocks'][0]['slot'] = dict(state=0)
        resources[0]['blocks'][0]['slot_matches'] = False
        self.assertFalse(graph.pcm_logical_bridge(wait, service, resources)['active_slot'])

    def test_pcm_logical_bridge_rejects_missing_duplicate_and_drifted_evidence(self):
        wait, service, resources = self.pcm_bridge_fixture()
        mutations = [lambda r: r[0].update(stable=False),
                     lambda r: r[0].update(status=1),
                     lambda r: r[0].update(omitted_blocks=1),
                     lambda r: r.append(copy.deepcopy(r[0])),
                     lambda r: r[0]['queue'].append(copy.deepcopy(r[0]['queue'][0])),
                     lambda r: r[0]['blocks'].append(copy.deepcopy(r[0]['blocks'][0])),
                     lambda r: r[0]['blocks'][0].update(decoded=False),
                     lambda r: r[0]['blocks'][0].update(neutral_sender=False),
                     lambda r: r[0]['blocks'][0].update(slot_matches=False),
                     lambda r: r[0]['blocks'][0]['slot'].update(owner_node=3),
                     lambda r: r[0]['blocks'][0]['slot'].update(state=3)]
        for field in ('request_seq','formation','master_session','base','requester_connection',
                      'ingress_connection','head','enqueue_order','ordered_lane','source'):
            mutations.append(lambda r,f=field: r[0]['queue'][0].update({f:r[0]['queue'][0][f]+1}))
        for field in ('authority','ordered_lane','source_candidate','retain_pi_if_dirty',
                      'common_flags','common_outcome','state','mode'):
            mutations.append(lambda r,f=field: r[0]['blocks'][0]['request'].update({f:r[0]['blocks'][0]['request'][f]+1}))
        for i, mutate in enumerate(mutations):
            changed=copy.deepcopy(resources);mutate(changed)
            self.assertIsNone(graph.pcm_logical_bridge(wait, service, changed), i)
        for field in ('r4','requester_connection','ingress_connection'):
            changed=dict(wait);changed[field]=0
            self.assertIsNone(graph.pcm_logical_bridge(changed, service, resources), field)

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

    def admitted_service_fixture(self):
        wait, service, resources = self.pcm_bridge_fixture()
        service['service_admission'] = dict(supplied=True, known=True, entered=True,
            feature_bit=1024, record_generation=12, formation_epoch=7, side=1,
            gate_phase=0, gate_formation=7, gate_freeze=19, master_node=2,
            master_session=8, peer_capability_generation=61, origin=1)
        return wait, service, resources

    def test_pcm_native_admission_distinguishes_ingress_and_retained_retries(self):
        wait, service, resources = self.admitted_service_fixture()
        for origin, name in ((1, 'ingress'), (2, 'source_drain'), (3, 'source_finish')):
            service['service_admission']['origin'] = origin
            proof = graph.pcm_admission_bridge(wait, service, resources)
            self.assertEqual(proof['quality'], 'native_admitted_service')
            self.assertEqual(proof['origin'], name)
            self.assertEqual(proof['peer_capability_generation'], 61)
            self.assertEqual(proof['service_sender'], 42)
            self.assertEqual(proof['new_inbound_frame'], origin == 1)
            self.assertTrue(proof['route_checked_at_entry'])
            self.assertFalse(proof['current_route_proven'])
        snapshots = [dict(node=0, owners=[dict(node=0,pid=10,procno=1,backend_id=2,
                        backend_start=3,proc_generation=17,pcm_wait=wait)]),
                     dict(node=1, owners=[dict(node=1,pid=20,procno=2,backend_id=-1,
                        backend_start=0,proc_generation=18,pcm_service=service)])]
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(result['edges'][0]['resource']['native_admission'], proof)
        self.assertEqual(result['edges'][0]['quality'], 'candidate_pcm_service')
        self.assertEqual(result['cycles'], []); self.assertFalse(result['complete'])

    def test_pcm_native_admission_rejects_unknown_drift_and_logical_gaps(self):
        wait, service, resources = self.admitted_service_fixture()
        for field in service['service_admission']:
            changed = copy.deepcopy(service)
            value = changed['service_admission'][field]
            if field in ('origin', 'peer_capability_generation'):
                changed['service_admission'][field] = 0
            elif field == 'gate_freeze':
                changed['service_admission'][field] = -1
            else:
                changed['service_admission'][field] = False if isinstance(value, bool) else value + 1
            self.assertIsNone(graph.pcm_admission_bridge(wait, changed, resources), field)
            del changed['service_admission'][field]
            self.assertIsNone(graph.pcm_admission_bridge(wait, changed, resources), 'missing '+field)
        changed = copy.deepcopy(service); changed.pop('service_admission')
        self.assertIsNone(graph.pcm_admission_bridge(wait, changed, resources))
        self.assertIsNone(graph.pcm_admission_bridge(wait, service, []))
        self.assertIsNone(graph.pcm_admission_bridge(wait, service, resources + resources))

    def current_route_fixture(self):
        wait, service, resources = self.admitted_service_fixture()
        snapshots = []
        for node in range(4):
            semantic = dict(known=True, seq=2+2*node, active_bits=1025,
                            record_generation=12, formation_epoch=7, transition_closed=False)
            gate = dict(known=True, phase=0, formation=7, freeze=19 if node == 1 else node)
            context = dict(semantic_begin=semantic, semantic_end=copy.deepcopy(semantic),
                           pcm_gate_begin=gate, pcm_gate_end=copy.deepcopy(gate))
            snapshots.append(dict(node=node, epoch_begin=7, epoch_end=7, context_stable=True,
                                  owners=[], **copy.deepcopy(context)))
            if node != 2:
                resources.append(dict(kind='pcm', node=node, status=2, stable=True))
            resource = next(r for r in resources if r['node'] == node)
            route = dict(known=True, master=2, epoch=7, declared=[0,1,2,3],
                         membership_generation=2+2*node, dead_generation=node)
            resource.update(copy.deepcopy(context), tag=wait['tag'], epoch=7,
                            route_begin=route, route_end=copy.deepcopy(route))
        snapshots[0]['owners'] = [dict(node=0,pid=10,procno=1,backend_id=2,
            backend_start=3,proc_generation=17,pcm_wait=wait)]
        snapshots[1]['owners'] = [dict(node=1,pid=20,procno=2,backend_id=-1,
            backend_start=0,proc_generation=18,pcm_service=service)]
        return wait, service, resources, snapshots

    def test_pcm_current_route_requires_all_native_cuts_and_remains_candidate(self):
        wait, service, resources, snapshots = self.current_route_fixture()
        proof = graph.pcm_current_bridge(wait, service, resources, snapshots)
        self.assertEqual(proof['quality'], 'current_healthy_route_context')
        self.assertEqual(proof['master'], 2)
        self.assertEqual(proof['nodes'], [0,1,2,3])
        self.assertEqual(proof['source_gate_freeze'], 19)
        self.assertFalse(proof['peer_capability_current_proven'])
        self.assertFalse(proof['claimed_owner_proven'])
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(result['edges'][0]['resource']['current_context'], proof)
        self.assertEqual(result['edges'][0]['quality'], 'candidate_pcm_service')
        self.assertEqual(result['cycles'], [])
        self.assertFalse(result['complete'])

    def test_pcm_current_route_rejects_missing_duplicate_or_changed_node_cuts(self):
        wait, service, resources, snapshots = self.current_route_fixture()
        for group in ('snapshots', 'resources'):
            for mutation in ('missing', 'duplicate'):
                rs, ss = copy.deepcopy(resources), copy.deepcopy(snapshots)
                items = ss if group == 'snapshots' else rs
                if mutation == 'missing': items.pop()
                else: items.append(copy.deepcopy(items[0]))
                self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, ss), (group,mutation))
        for group in ('snapshots', 'resources'):
            for cut in ('semantic_begin','semantic_end','pcm_gate_begin','pcm_gate_end'):
                for mutation in ('missing','unknown','drift'):
                    rs, ss = copy.deepcopy(resources), copy.deepcopy(snapshots)
                    item = (ss if group == 'snapshots' else rs)[0]
                    if mutation == 'missing': del item[cut]
                    elif mutation == 'unknown': item[cut]['known'] = False
                    else:
                        field = 'seq' if cut.startswith('semantic') else 'freeze'
                        item[cut][field] += 2
                    self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, ss), (group,cut,mutation))

    def test_pcm_current_route_rejects_consistent_but_invalid_context(self):
        wait, service, resources, snapshots = self.current_route_fixture()
        for cut, field, value in (
                ('semantic','record_generation',13), ('semantic','formation_epoch',8),
                ('semantic','active_bits',1), ('semantic','transition_closed',True),
                ('semantic','seq',0), ('semantic','seq',3), ('pcm_gate','phase',1),
                ('pcm_gate','formation',8), ('pcm_gate','freeze',2**64-1)):
            rs, ss = copy.deepcopy(resources), copy.deepcopy(snapshots)
            for item in rs + ss:
                for side in ('begin','end'): item[cut+'_'+side][field] = value
            self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, ss), (cut,field,value))
        rs = copy.deepcopy(resources)
        for side in ('begin','end'):
            next(r for r in rs if r['node']==1)['pcm_gate_'+side]['freeze'] = 20
        self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, snapshots))
        for field, value in (('known',False), ('master',3), ('epoch',8),
                             ('declared',[0,1,2]), ('declared',[0,1,2,2]),
                             ('membership_generation',0), ('membership_generation',3)):
            rs = copy.deepcopy(resources)
            for r in rs:
                for side in ('begin','end'): r['route_'+side][field] = value
            self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, snapshots), (field,value))
        for field in ('epoch','tag','route_end'):
            rs = copy.deepcopy(resources)
            if field=='tag': rs[0][field]=[1,2,3,4,6]
            elif field=='epoch': rs[0][field]=8
            else: rs[0][field]['dead_generation'] += 1
            self.assertIsNone(graph.pcm_current_bridge(wait, service, rs, snapshots), field)
        self.assertIsNone(graph.pcm_current_bridge(wait, {}, resources, snapshots))

    def test_capture_rejects_context_change_between_outer_owner_cuts(self):
        wait, service, resources, snapshots = self.current_route_fixture()
        class Reader:
            def __init__(self, node, directory): self.node, self.calls = node['node'], 0
            def query(self, sql, deadline):
                if sql.endswith('_prepare()'): return 't'
                if 'pgrac_wg_block(' in sql:
                    return graph.json.dumps(next(r for r in resources if r['node']==self.node))
                if 'pgrac_wg_enqueue(' in sql:
                    return graph.json.dumps(dict(node=self.node,kind='enqueue',status=1))
                self.calls += 1
                value = copy.deepcopy(snapshots[self.node])
                value.pop('context_stable')
                if self.calls > 1:
                    value['semantic_begin']['seq'] += 2
                    value['semantic_end']['seq'] += 2
                return graph.json.dumps(value)
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n,connstr=str(n)) for n in range(4)], Path(directory)/'capture')
            try:
                for reader in session.readers: reader.calls = 0
                session.capture()
                result = graph.json.loads((session.output/'wait-for-graph.json').read_text())
                self.assertIsNone(result['edges'][0]['resource']['current_context'])
                self.assertEqual(result['cycles'], [])
                self.assertFalse(result['complete'])
            finally:
                session.close()

    def source_custody_fixture(self):
        wait, service, resources, snapshots = self.current_route_fixture()
        for node in range(4):
            rows = [dict(node=n, bits=0, generation=0, valid=False) for n in range(128)]
            rows[2].update(bits=0x20000, generation=61 if node==1 else 30+node, valid=True)
            peers = dict(known=True, busy=False, rows=rows)
            snapshot = snapshots[node]
            snapshot.update(peer_begin=peers, peer_end=copy.deepcopy(peers), peer_stable=True)
            next(r for r in resources if r['node']==node).update(
                peer_begin=copy.deepcopy(peers), peer_end=copy.deepcopy(peers))
        source = next(r for r in resources if r['node']==1)
        source['status'] = 0
        source['local_owner'] = dict(valid=True, successor_known=True, state=2,
            highest_generation=4, held_buffer_id_plus_one=0,
            handle=dict(tag=wait['tag'], requester=1, formation=7, acquisition=3,
                master_session=8, r4=12, ownership=60, reservation=19, generation=4,
                deadline=1000, procno=2),
            successor=dict(wait, ordered_lane=4, authority=3))
        return wait, service, resources, snapshots, snapshots[1]['owners'][0]

    def claim_incarnation_fixture(self):
        wait, service, resources, snapshots, owner = self.source_custody_fixture()
        owner['stable'] = True
        next(r for r in resources if r['node']==1)['local_owner']['claim'] = dict(
            pid=20, proc_generation=18, owner_generation=4)
        return wait, service, resources, snapshots, owner

    def dependency_fixture(self):
        wait, service, resources, snapshots, owner = self.claim_incarnation_fixture()
        waiter = snapshots[0]['owners'][0]
        waiter['stable'] = True
        streams = dict(known=True, epoch=7, channels=2, rows=[
            dict(node=n, known=n==2, control=61 if n==2 else 0,
                 data=[71,72] if n==2 else []) for n in range(128)])
        snapshots[1].update(streams_begin=streams, streams_end=copy.deepcopy(streams),
                            streams_stable=True)
        source = next(r for r in resources if r['node']==1)
        source.update(streams_begin=copy.deepcopy(streams), streams_end=copy.deepcopy(streams))
        # A current source executor is waiting for a SPACE content holder,
        # which is waiting for HW held by the original PCM requester.
        space = [1,2,3,0,1]
        owner['native_wait'] = dict(tag=space, mode=0)
        resid = [1,2,3,0,242,1]
        grant = dict(resid=resid, holder=[0,1,7,100], mode=7)
        waiter['grants'] = [grant]
        snapshots[1]['owners'].append(dict(node=1,pid=21,procno=3,backend_id=3,
            backend_start=4,proc_generation=19,stable=True,
            locks=[dict(tag=space,mode=0)], enqueue=dict(resid=resid, holder=[1,3,7,0],
                mode=7, request=50, epoch=7, wait_seq=60)))
        resources.append(dict(kind='enqueue',node=2,status=0,stable=True,resid=resid,
            route_known=True,master=2,master_generation=1,shard_phase=0,epoch=7,
            members=[dict(kind=1,id=[1,3,7,50],mode=7,wait_seq=60),
                     dict(kind=0,id=[0,1,7,100],mode=7)]))
        return wait, service, resources, snapshots, owner, waiter

    def test_native_pcm_dependency_closes_cross_node_content_hw_cycle(self):
        wait, service, resources, snapshots, owner, waiter = self.dependency_fixture()
        proof = graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter)
        self.assertTrue(proof['native_pending_reply_dependency'])
        self.assertFalse(proof['physical_frame_receipt_proven'])
        self.assertFalse(proof['authority_proof'])
        self.assertEqual(proof['current_peer']['control'], 61)
        self.assertEqual(proof['current_peer']['data'], [71,72])
        result = graph.build_graph(snapshots,resources)
        self.assertTrue(result['complete'],result['unknown'])
        self.assertFalse(result['atomic_graph'])
        self.assertEqual(len(result['cycles']),1)
        self.assertEqual({e['quality'] for e in result['edges']},
                         {'exact_observed_pcm_dependency','exact_local_content','exact_ges_holder'})
        self.assertEqual(result['cycles'][0][0],result['cycles'][0][-1])

    def test_native_pcm_dependency_rejects_missing_closed_or_changed_peer_streams(self):
        mutations = [lambda s:s.update(known=False), lambda s:s.update(epoch=8),
                     lambda s:s.update(channels=0), lambda s:s.update(channels=9),
                     lambda s:s['rows'][2].update(known=False),
                     lambda s:s['rows'][2].update(control=42),
                     lambda s:s['rows'][2].update(control=True),
                     lambda s:s['rows'][2].update(data=[71]),
                     lambda s:s['rows'][2].update(data=[71,0]),
                     lambda s:s['rows'][2].update(data=[71,2**64-1]),
                     lambda s:s['rows'][2].update(data=[71,True]),
                     lambda s:s['rows'].pop()]
        for i,mutate in enumerate(mutations):
            wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
            # Consistently invalid cuts must fail even when all samples agree.
            source=next(r for r in resources if r.get('kind')=='pcm' and r['node']==1)
            for cut in (snapshots[1],source):
                for side in ('streams_begin','streams_end'): mutate(cut[side])
            result=graph.build_graph(snapshots,resources)
            self.assertFalse(result['complete'],i);self.assertEqual(result['cycles'],[],i)
            self.assertFalse(any(e['quality']=='exact_observed_pcm_dependency' for e in result['edges']),i)
        for location,key in (('local','streams_stable'),('owner','stable'),('waiter','stable')):
            wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
            {'local':snapshots[1],'owner':owner,'waiter':waiter}[location][key]=False
            self.assertIsNone(graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter))
        wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
        snapshots[1]['streams_end']['rows'][2]['data'][1]+=1
        self.assertIsNone(graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter))
        snapshots[1].pop('streams_begin')
        self.assertIsNone(graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter))

    def test_native_pcm_dependency_preserves_missing_claim_request_and_extra_source(self):
        for field,value in (('pid',21),('proc_generation',19),('owner_generation',5)):
            wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
            source=next(r for r in resources if r.get('kind')=='pcm' and r['node']==1)
            source['local_owner']['claim'][field]=value
            result=graph.build_graph(snapshots,resources)
            self.assertFalse(result['complete']);self.assertEqual(result['cycles'],[])
        wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
        master=next(r for r in resources if r.get('kind')=='pcm' and r['node']==2)
        master['queue'][0]['request_seq']+=1
        self.assertEqual(graph.build_graph(snapshots,resources)['cycles'],[])
        wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
        master=next(r for r in resources if r.get('kind')=='pcm' and r['node']==2)
        master['queue'][0]['incompatible'] |= 1<<3
        result=graph.build_graph(snapshots,resources)
        self.assertFalse(result['complete'])
        self.assertEqual(len(result['cycles']),1)  # Proven subcycle remains visible.
        self.assertTrue(any(x['reason']=='pcm_incompatible_sources_unmapped' for x in result['unknown']))

    def test_local_master_source_uses_native_loopback_without_peer_hello_or_data(self):
        wait,service,resources,snapshots,owner,waiter=self.dependency_fixture()
        old_master=next(r for r in resources if r.get('kind')=='pcm' and r['node']==2)
        source=next(r for r in resources if r.get('kind')=='pcm' and r['node']==1)
        source.update(queue=old_master.pop('queue'),blocks=old_master.pop('blocks'),view=old_master.pop('view'),
                      omitted_blocks=0)
        old_master['status']=2
        for item in (wait,service,source['queue'][0],source['blocks'][0]['request'],source['local_owner']['successor']):
            item['expected_master']=1
        source['blocks'][0]['slot']['owner_node']=1
        service['service_admission'].update(master_node=1,peer_capability_generation=1)
        for resource in resources:
            if resource['kind']=='pcm':
                resource['route_begin']['master']=resource['route_end']['master']=1
        for cut in (snapshots[1],source):
            for side in ('streams_begin','streams_end'): cut[side]['local_bits']=0x20000
        proof=graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter)
        self.assertTrue(proof['current_peer']['loopback'])
        self.assertIsNone(proof['current_peer']['control'])
        self.assertEqual(proof['current_peer']['data'],[])
        result=graph.build_graph(snapshots,resources)
        self.assertTrue(result['complete'],result['unknown']);self.assertEqual(len(result['cycles']),1)
        for cut in (snapshots[1],source):
            for side in ('streams_begin','streams_end'): cut[side]['local_bits']=0
        self.assertIsNone(graph.pcm_dependency_bridge(wait,service,resources,snapshots,owner,waiter))

    def test_nonfailed_ko_rows_remain_responsibilities_without_invented_wait_edges(self):
        ko=dict(status=0,omitted=0,rows=[dict(kind=0,pid=10,serial=3,failed=False,invalid=False)])
        result=graph.build_graph([dict(node=0,owners=[],ko=ko)],[])
        self.assertTrue(result['complete'])
        self.assertEqual(result['responsibilities'],[dict(node=0,**ko)])
        self.assertEqual(result['edges'],[])
        ko['rows'][0]['invalid']=True
        self.assertFalse(graph.build_graph([dict(node=0,owners=[],ko=ko)],[])['complete'])

    def test_quiet_four_node_capture_is_complete_without_blanket_qualification_override(self):
        class Reader:
            def __init__(self,node,directory): self.node=node['node']
            def query(self,sql,deadline):
                if sql.endswith('_prepare()'): return 't'
                return graph.json.dumps(dict(node=self.node,epoch_begin=7,epoch_end=7,
                    owners=[],procs=[],stats=dict(available=True),ko=dict(status=0,omitted=0,rows=[])))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory,patch.object(graph,'PsqlReader',Reader):
            session=graph.CaptureSession([dict(node=n,connstr=str(n)) for n in range(4)],Path(directory)/'capture')
            try:
                result=session.capture()
                self.assertTrue(result['complete'])
                saved=graph.json.loads((session.output/'wait-for-graph.json').read_text())
                self.assertTrue(saved['complete']);self.assertEqual(saved['unknown'],[])
                self.assertFalse(saved['atomic_graph'])
            finally: session.close()

    def test_claim_incarnation_binds_original_claim_and_keeps_candidate_boundary(self):
        wait, service, resources, snapshots, owner = self.claim_incarnation_fixture()
        proof = graph.pcm_claim_incarnation_bridge(wait, service, resources, snapshots, owner)
        self.assertEqual(proof['quality'], 'native_claim_incarnation_cut')
        self.assertEqual([proof[k] for k in ('pid','procno','proc_generation','owner_generation')],
                         [20,2,18,4])
        self.assertTrue(proof['incarnation_matches_at_cut'])
        self.assertFalse(proof['physical_frame_receipt_proven'])
        self.assertFalse(proof['peer_open_current_proven'])
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(result['edges'][0]['resource']['claim_incarnation'], proof)
        self.assertEqual(result['edges'][0]['quality'], 'candidate_pcm_service')
        self.assertFalse(result['complete']); self.assertEqual(result['cycles'], [])

    def test_claim_incarnation_rejects_reused_slots_and_unknown_native_claim(self):
        wait, service, resources, snapshots, owner = self.claim_incarnation_fixture()
        for field in ('pid','proc_generation','owner_generation'):
            for value in (None, False, 0, -1, '18', 2**64-1, 999):
                rs=copy.deepcopy(resources)
                next(r for r in rs if r['node']==1)['local_owner']['claim'][field]=value
                self.assertIsNone(graph.pcm_claim_incarnation_bridge(wait,service,rs,snapshots,owner),
                                  (field,value))
        for claim in (None, [], {}, dict(pid=20,proc_generation=18)):
            rs=copy.deepcopy(resources)
            next(r for r in rs if r['node']==1)['local_owner']['claim']=claim
            self.assertIsNone(graph.pcm_claim_incarnation_bridge(wait,service,rs,snapshots,owner))
        for field,value in (('pid',21),('proc_generation',19),('stable',False),('stable',None)):
            self.assertIsNone(graph.pcm_claim_incarnation_bridge(
                wait,service,resources,snapshots,dict(owner,**{field:value})), (field,value))
        # Matching process identity cannot override an unproved native source owner.
        next(r for r in resources if r['node']==1)['local_owner']['state']=5
        self.assertIsNone(graph.pcm_claim_incarnation_bridge(wait,service,resources,snapshots,owner))

    def test_source_custody_keeps_holder_successor_and_peer_namespaces_separate(self):
        wait, service, resources, snapshots, owner = self.source_custody_fixture()
        proof = graph.pcm_source_custody_bridge(wait, service, resources, snapshots, owner)
        self.assertEqual(proof['quality'], 'native_source_custody_cut')
        self.assertEqual(proof['peer_capability_generation'], 61)
        self.assertEqual(service['wire_sender_connection'], 42)
        self.assertEqual(proof['holder_acquisition'], 3)
        self.assertEqual(wait['request_seq'], 9)
        self.assertEqual(proof['owner_procno'], 2)
        self.assertFalse(proof['owner_claim_incarnation_proven'])
        self.assertFalse(proof['physical_frame_receipt_proven'])
        result = graph.build_graph(snapshots, resources)
        self.assertEqual(result['edges'][0]['resource']['source_custody'], proof)
        self.assertEqual(result['edges'][0]['quality'], 'candidate_pcm_service')
        self.assertFalse(result['complete']); self.assertEqual(result['cycles'], [])

    def test_source_custody_refuses_peer_unavailable_drift_and_generation_alias(self):
        wait, service, resources, snapshots, owner = self.source_custody_fixture()
        for where in ('snapshot','resource'):
            for mutation in ('missing','unknown','busy','drift','duplicate','short','malformed','invalid','bits','wire_alias'):
                rs, ss = copy.deepcopy(resources), copy.deepcopy(snapshots)
                item = ss[1] if where=='snapshot' else next(r for r in rs if r['node']==1)
                if mutation=='missing': del item['peer_begin']
                elif mutation=='unknown': item['peer_begin']['known']=False
                elif mutation=='busy': item['peer_begin']['busy']=True
                elif mutation=='drift': item['peer_end']['rows'][2]['generation']+=1
                else:
                    for side in ('begin','end'):
                        rows=item['peer_'+side]['rows']
                        if mutation=='duplicate': rows.append(copy.deepcopy(rows[2]))
                        elif mutation=='short': rows.pop()
                        elif mutation=='malformed': rows[2]=None
                        elif mutation=='invalid': rows[2]['valid']=False
                        elif mutation=='bits': rows[2]['bits']=0
                        else: rows[2]['generation']=42
                self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,rs,ss,owner), (where,mutation))
        snapshots[1]['peer_stable']=False
        self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,resources,snapshots,owner))

    def test_source_custody_refuses_consistently_malformed_peer_record(self):
        wait,service,resources,snapshots,owner=self.source_custody_fixture()
        source=next(r for r in resources if r['node']==1)
        for item in (snapshots[1],source):
            for side in ('begin','end'): item['peer_'+side]['rows'][2]=None
        self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,resources,snapshots,owner))

    def test_source_custody_refuses_nonexecutor_and_owner_key_drift(self):
        wait, service, resources, snapshots, owner = self.source_custody_fixture()
        for field,value in (('valid',False),('successor_known',False),('state',0),('state',1),
                            ('state',3),('state',4),('state',5),('highest_generation',5),
                            ('held_buffer_id_plus_one',1)):
            rs=copy.deepcopy(resources)
            next(r for r in rs if r['node']==1)['local_owner'][field]=value
            self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,rs,snapshots,owner), (field,value))
        for field in ('tag','requester','formation','acquisition','master_session','r4','ownership',
                      'reservation','generation','deadline','procno'):
            rs=copy.deepcopy(resources); h=next(r for r in rs if r['node']==1)['local_owner']['handle']
            h[field]=[1,2,3,4,6] if field=='tag' else (0 if field not in ('procno','requester') else 9)
            self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,rs,snapshots,owner), field)
        for field in ('tag','requester','request_seq','formation','master_session','r4','base',
                      'expected_master','ordered_lane','authority'):
            rs=copy.deepcopy(resources); row=next(r for r in rs if r['node']==1)['local_owner']['successor']
            row[field]=[1,2,3,4,6] if field=='tag' else row[field]+1
            self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,rs,snapshots,owner), field)
        self.assertIsNone(graph.pcm_source_custody_bridge(wait,service,resources,snapshots,dict(owner,procno=3)))

    def test_capture_rejects_peer_change_between_outer_owner_cuts(self):
        wait, service, resources, snapshots, owner = self.source_custody_fixture()
        class Reader:
            def __init__(self,node,directory): self.node,self.calls=node['node'],0
            def query(self,sql,deadline):
                if sql.endswith('_prepare()'): return 't'
                if 'pgrac_wg_block(' in sql:
                    return graph.json.dumps(next(r for r in resources if r['node']==self.node))
                if 'pgrac_wg_enqueue(' in sql:
                    return graph.json.dumps(dict(node=self.node,kind='enqueue',status=1))
                self.calls+=1
                value=copy.deepcopy(snapshots[self.node]);value.pop('peer_stable')
                if self.node==1 and self.calls>1:
                    for side in ('begin','end'): value['peer_'+side]['rows'][2]['generation']+=1
                return graph.json.dumps(value)
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory,patch.object(graph,'PsqlReader',Reader):
            session=graph.CaptureSession([dict(node=n,connstr=str(n)) for n in range(4)],Path(directory)/'capture')
            try:
                for reader in session.readers: reader.calls=0
                session.capture()
                result=graph.json.loads((session.output/'wait-for-graph.json').read_text())
                evidence=result['edges'][0]['resource']
                self.assertIsNotNone(evidence['current_context'])
                self.assertIsNone(evidence['source_custody'])
                self.assertFalse(result['complete']);self.assertEqual(result['cycles'],[])
            finally: session.close()

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
                                 backend_start=0, proc_generation=17, pcm_service=dict(tag=tag,lifetime=43))]))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)], Path(directory)/'capture')
            try:
                self.assertTrue(session.capture()['complete'])
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
                                 grants=[dict(resid=resid, holder=[self.node,1,7,101], mode=7,lifetime=44)])]))
            def close(self): pass
        with tempfile.TemporaryDirectory() as directory, patch.object(graph, 'PsqlReader', Reader):
            session = graph.CaptureSession([dict(node=n, connstr=str(n)) for n in range(4)],
                                           Path(directory)/'capture')
            try:
                self.assertTrue(session.capture()['complete'])
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
