"""Read-only four-node wait evidence. No cancel, grant or workload verdict authority."""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import time
import copy
import socket
import threading
import re
from concurrent.futures import ThreadPoolExecutor


# Native PostgreSQL heavyweight conflict table; mode0 is not a granted lock.
CONFLICTS = {1: {8}, 2: {7, 8}, 3: {5, 6, 7, 8},
             4: {4, 5, 6, 7, 8}, 5: {3, 4, 6, 7, 8},
             6: {3, 4, 5, 6, 7, 8}, 7: {2, 3, 4, 5, 6, 7, 8},
             8: set(range(1, 9))}


def identity(row):
    return 'n%d:p%d:s%d:b%d:t%d:g%d' % tuple(row[x] for x in
                                        ('node', 'pid', 'procno', 'backend_id', 'backend_start', 'proc_generation'))


def stabilize_owner(first, last):
    """Per-native-episode proof; unrelated publications do not end a hold."""
    result = copy.deepcopy(first)
    result['stable'] = last is not None and identity(first) == identity(last)
    result['unproven_resources'] = []
    if not result['stable']:
        return result

    def same(a, b):
        token = a.get('lifetime')
        return (type(token) is int and 0 < token < 2**64 and token == b.get('lifetime') and
                {k:v for k,v in a.items() if k != 'snapshot'} ==
                {k:v for k,v in b.items() if k != 'snapshot'})

    for field in ('locks','grants'):
        before, after = first.get(field, []), last.get(field, [])
        result[field] = [a for a in before if sum(same(a,b) for b in after) == 1 and
                         sum(same(a,b) for b in before) == 1]
        if (len(result[field]) != len(before) or len(result[field]) != len(after)):
            result['unproven_resources'].append(dict(field=field,before=before,after=after))
    for field in ('native_wait','enqueue','pcm_wait','tx_wait','pcm_service','ko_wait','retry_wait'):
        a,b = first.get(field),last.get(field)
        if a is None and b is None:
            continue
        if a is None or b is None or not same(a,b):
            result[field] = None
            result['unproven_resources'].append(dict(field=field,before=a,after=b))
    return result


def pcm_claimed_holders(tag, resources, owners):
    """Actual native claimant -> own waits. Ownership is not a service edge."""
    result = []
    for resource in resources:
        if (resource.get('kind') != 'pcm' or resource.get('tag') != tag or
                resource.get('status') != 0 or resource.get('stable') is not True):
            continue
        native = resource.get('local_owner') or {}
        handle, claim = native.get('handle') or {}, native.get('claim') or {}
        actor = owners.get((resource.get('node'),handle.get('procno')))
        if (native.get('valid') is not True or native.get('state') not in (1,2) or
                handle.get('tag') != tag or handle.get('requester') != resource.get('node') or
                actor is None or actor.get('stable') is not True or
                claim.get('pid') != actor.get('pid') or
                claim.get('proc_generation') != actor.get('proc_generation') or
                type(claim.get('owner_generation')) is not int or
                not 0 < claim['owner_generation'] < 2**64-1 or
                claim['owner_generation'] != handle.get('generation') or
                claim['owner_generation'] != native.get('highest_generation')):
            continue
        result.append(dict(process=identity(actor), quality='native_claimed_holder_observation',
                           native=native, own_waits={k:actor[k] for k in
                           ('native_wait','enqueue','pcm_wait','tx_wait','ko_wait','retry_wait') if actor.get(k)},
                           service=actor.get('pcm_service'), wait_dependency_proven=False))
    return result


def _cycles(edges):
    # Linear DFS returns cycle witnesses, not the exponentially many simple cycles.
    adjacent = {}
    for edge in edges:
        if not edge['quality'].startswith('exact_'):
            continue
        adjacent.setdefault(edge['from'], set()).add(edge['to'])
    result, colour, position = [], {}, {}
    for start in sorted(adjacent):
        if start in colour:
            continue
        path = [start]
        colour[start], position[start] = 1, 0
        stack = [(start, iter(sorted(adjacent.get(start, ()))))]
        while stack:
            here, targets = stack[-1]
            target = next(targets, None)
            if target is None:
                stack.pop()
                path.pop()
                colour[here] = 2
                del position[here]
            elif target not in colour:
                colour[target], position[target] = 1, len(path)
                path.append(target)
                stack.append((target, iter(sorted(adjacent.get(target, ())))))
            elif colour[target] == 1:
                result.append(path[position[target]:] + [target])
    return result


RESOURCE_WAITS = {'BufferContent', 'PcmBlockConvertWait', 'ClusterRelExtendWait',
                  'ClusterPcmCompatibleStateWait', 'ClusterGesS4Wait',
                  'ClusterGesReplyWait', 'GesTxEnqueueWait', 'GesGrantWait',
                  'GesConvertWait', 'GesBastWait', 'GesCancelDrain', 'ClusterCfEnqueueWait'}


def pcm_logical_bridge(wait, service, resources):
    """Owned retained logical selection only; never route or send authority."""
    if (not wait.get('transport_known') or
            any(not isinstance(wait.get(k), int) or wait[k] <= 0
                for k in ('r4', 'requester_connection', 'ingress_connection')) or
            wait['r4'] != service.get('r4') or service.get('source') not in range(32)):
        return None
    logical = ('tag', 'requester', 'request_seq', 'formation', 'master_session', 'base')
    common = logical + ('authority', 'ordered_lane', 'source', 'state', 'mode',
                       'source_candidate', 'retain_pi_if_dirty', 'common_flags', 'common_outcome')
    if (service.get('mode') != 0 or service.get('state') not in (1, 2) or
            service.get('source_candidate') != 1 or service.get('retain_pi_if_dirty') != 1 or
            service.get('common_flags') != 1 or service.get('common_outcome') != 0):
        return None
    queues = []
    for resource in resources:
        if (resource.get('kind') != 'pcm' or resource.get('node') != wait.get('expected_master') or
                resource.get('status') != 0 or resource.get('stable') is not True or
                resource.get('omitted_blocks') != 0):
            continue
        view = resource.get('view', {})
        for queue in resource.get('queue', []):
            if (queue.get('queue_kind') == 1 and queue.get('request_known') is True and
                    queue.get('transport_known') is True and queue.get('phase') in (2, 3) and
                    all(wait.get(k) == queue.get(k) for k in logical) and
                    queue.get('requester_connection') == wait['requester_connection'] and
                    queue.get('ingress_connection') == wait['ingress_connection'] and
                    queue.get('ordered_lane') == service.get('ordered_lane') and
                    queue.get('source') == service['source'] and
                    queue.get('incompatible', 0) & (1 << service['source']) and
                    not queue.get('blocked', 0) & (1 << service['source']) and
                    all(isinstance(queue.get(k), int) and queue[k] > 0 for k in ('head', 'enqueue_order')) and
                    all(queue[k] == view.get(k) for k in ('head', 'enqueue_order')) and
                    view.get('tag') == wait.get('tag') and
                    view.get('head_requester') == wait.get('requester') and
                    view.get('head_request') == wait.get('request_seq')):
                queues.append((resource, queue))
    if len(queues) != 1:
        return None
    resource, queue = queues[0]
    if any(type(queue.get(k)) is not int or not 0 <= queue[k] < 2**32
           for k in ('incompatible','blocked')):
        return None
    blocks = []
    for block in resource.get('blocks', []):
        slot = block.get('slot', {})
        state = slot.get('state')
        retained = block.get('request') or {}
        if (block.get('holder') == service['source'] and block.get('decoded') is True and
                block.get('neutral_sender') is True and block.get('reject') == 0 and
                (state == 0 or (state in (1, 2) and block.get('slot_matches') is True and
                                slot.get('owner_node') == resource['node'])) and
                all(k in service and k in retained and service[k] == retained[k] for k in common)):
            blocks.append(block)
    if len(blocks) != 1:
        return None
    block = blocks[0]
    return dict(quality='retained_logical_only', master=resource['node'],
                enqueue_order=queue['enqueue_order'], head=queue['head'],
                awaited_sources=queue['incompatible'] & ~queue.get('blocked', 0),
                active_slot=block['slot']['state'] != 0,
                requester_connection=wait['requester_connection'], master_ingress=wait['ingress_connection'],
                retained_sender=block['request']['wire_sender_connection'],
                service_sender=service.get('wire_sender_connection'),
                canonical_route_proven=False, physical_send_proven=False)


def pcm_admission_bridge(wait, service, resources):
    """Already accepted native entry context, without ongoing route authority."""
    logical = pcm_logical_bridge(wait, service, resources)
    admission = service.get('service_admission') or {}
    if logical is None or any(admission.get(k) is not True for k in ('supplied', 'known', 'entered')):
        return None
    fields = ('feature_bit', 'record_generation', 'formation_epoch', 'gate_formation',
              'gate_freeze', 'master_session', 'peer_capability_generation',
              'gate_phase', 'origin', 'master_node', 'side')
    if any(type(admission.get(k)) is not int for k in fields):
        return None
    if (admission['feature_bit'] != 1024 or admission['side'] != 1 or
            admission['gate_phase'] != 0 or admission['origin'] not in (1, 2, 3) or
            not 0 <= admission['gate_freeze'] < 2**64 or
            not 0 < admission['record_generation'] < 2**64-1 or
            not 0 < admission['peer_capability_generation'] < 2**32 or
            not isinstance(service.get('wire_sender_connection'), int) or
            not 0 < service['wire_sender_connection'] < 2**32 or
            admission['record_generation'] != wait.get('r4') or
            admission['record_generation'] != service.get('r4') or
            admission['formation_epoch'] != wait.get('formation') or
            admission['gate_formation'] != wait.get('formation') or
            admission['master_session'] != wait.get('master_session') or
            admission['master_node'] != wait.get('expected_master') or
            admission['master_node'] != logical['master']):
        return None
    return dict(quality='native_admitted_service',
                origin={1: 'ingress', 2: 'source_drain', 3: 'source_finish'}[admission['origin']],
                checked_master=admission['master_node'],
                peer_capability_generation=admission['peer_capability_generation'],
                service_sender=service['wire_sender_connection'],
                new_inbound_frame=admission['origin'] == 1,
                route_checked_at_entry=True, current_route_proven=False)


CONTEXT_CUTS = ('semantic_begin', 'semantic_end', 'pcm_gate_begin', 'pcm_gate_end')


def pcm_current_bridge(wait, service, resources, snapshots):
    """Supplement finite healthy-route cuts only; no live owner/capability proof."""
    if pcm_admission_bridge(wait, service, resources) is None:
        return None
    nodes = [s.get('node') for s in snapshots]
    if (len(nodes) != 4 or any(type(n) is not int or not 0 <= n < 128 for n in nodes) or
            len(set(nodes)) != 4):
        return None
    nodes = sorted(nodes)
    if any(n not in nodes for n in (wait.get('requester'), service.get('source'), wait.get('expected_master'))):
        return None
    formation, record = wait['formation'], wait['r4']
    cuts = [r for r in resources if r.get('kind') == 'pcm' and r.get('tag') == wait.get('tag')]
    if len(cuts) != 4 or sorted(r.get('node', -1) for r in cuts) != nodes:
        return None

    def integer(value, minimum=0):
        return type(value) is int and minimum <= value < 2**64-1

    source_freeze = None
    for snapshot in snapshots:
        node = snapshot['node']
        resource = next(r for r in cuts if r['node'] == node)
        if (snapshot.get('context_stable') is not True or
                snapshot.get('epoch_begin') != formation or snapshot.get('epoch_end') != formation or
                resource.get('epoch') != formation or resource.get('stable') is not True or
                resource.get('status') not in (0, 2)):
            return None
        route = resource.get('route_begin') or {}
        if (route != resource.get('route_end') or route.get('known') is not True or
                route.get('master') != wait['expected_master'] or route.get('epoch') != formation or
                route.get('declared') != nodes or
                not integer(route.get('membership_generation'), 1) or route['membership_generation'] & 1 or
                not integer(route.get('dead_generation'))):
            return None
        for item in (snapshot, resource):
            semantic = item.get('semantic_begin') or {}
            gate = item.get('pcm_gate_begin') or {}
            if (semantic != item.get('semantic_end') or gate != item.get('pcm_gate_end') or
                    semantic.get('known') is not True or gate.get('known') is not True or
                    not integer(semantic.get('seq'), 1) or semantic['seq'] & 1 or
                    not integer(semantic.get('active_bits')) or not semantic['active_bits'] & 1024 or
                    semantic.get('record_generation') != record or semantic.get('formation_epoch') != formation or
                    semantic.get('transition_closed') is not False or gate.get('phase') != 0 or
                    gate.get('formation') != formation or not integer(gate.get('freeze'))):
                return None
        if any(snapshot.get(k) != resource.get(k) for k in CONTEXT_CUTS):
            return None
        if node == service['source']:
            source_freeze = snapshot['pcm_gate_begin']['freeze']
    if source_freeze != service['service_admission']['gate_freeze']:
        return None
    return dict(quality='current_healthy_route_context', master=wait['expected_master'], nodes=nodes,
                formation=formation, record_generation=record, source_gate_freeze=source_freeze,
                peer_capability_current_proven=False, claimed_owner_proven=False)


def pcm_source_custody_bridge(wait, service, resources, snapshots, service_owner):
    """Finite receiver-local capability/owner-slot observations, not a PID claim."""
    if pcm_current_bridge(wait, service, resources, snapshots) is None:
        return None
    source, master = service['source'], wait['expected_master']
    if (service_owner.get('node') != source or
            service_owner.get('pcm_service') != service or service_owner.get('stable') is False or
            type(service_owner.get('procno')) is not int or service_owner['procno'] < 0):
        return None
    local = [s for s in snapshots if s.get('node') == source]
    copies = [r for r in resources if r.get('kind') == 'pcm' and r.get('node') == source and
              r.get('tag') == wait['tag'] and r.get('status') == 0 and r.get('stable') is True]
    if len(local) != 1 or len(copies) != 1 or local[0].get('peer_stable') is not True:
        return None
    peer_cut = local[0].get('peer_begin') or {}
    if any(item.get(key) != peer_cut for item in (local[0], copies[0]) for key in ('peer_begin','peer_end')):
        return None
    rows = peer_cut.get('rows')
    if (peer_cut.get('known') is not True or peer_cut.get('busy') is not False or
            not isinstance(rows, list) or len(rows) != 128 or
            any(not isinstance(r, dict) for r in rows) or
            [r.get('node') for r in rows] != list(range(128))):
        return None
    peer = rows[master]
    # Native same-instance dispatch has no HELLO peer record and uses the
    # separate local capability-current convention1, never a remote wire ID.
    generation = 1 if source == master else peer.get('generation')
    if (generation != service['service_admission']['peer_capability_generation'] or
            (source != master and (peer.get('valid') is not True or type(peer.get('bits')) is not int or
             not 0 <= peer['bits'] < 2**32 or not peer['bits'] & 0x20000 or
             type(generation) is not int or not 0 < generation < 2**32))):
        return None
    owner = copies[0].get('local_owner') or {}
    handle, successor = owner.get('handle') or {}, owner.get('successor') or {}
    keys = ('tag','requester','request_seq','formation','master_session','r4','base','expected_master')
    if (owner.get('valid') is not True or owner.get('successor_known') is not True or
            owner.get('state') != 2 or owner.get('held_buffer_id_plus_one') != 0 or
            any(successor.get(k) != wait.get(k) for k in keys) or
            any(successor.get(k) != service.get(k) for k in ('authority','ordered_lane')) or
            handle.get('tag') != wait['tag'] or handle.get('requester') != source or
            handle.get('formation') != wait['formation'] or handle.get('r4') != wait['r4'] or
            handle.get('master_session') != wait['master_session'] or
            handle.get('procno') != service_owner['procno'] or
            handle.get('generation') != owner.get('highest_generation') or
            any(type(handle.get(k)) is not int or not 0 < handle[k] < 2**64-1 for k in
                ('acquisition','ownership','reservation','generation','deadline'))):
        return None
    return dict(quality='native_source_custody_cut', source=source, master=master,
                peer_capability_generation=generation, owner_procno=handle['procno'],
                owner_generation=handle['generation'], holder_acquisition=handle['acquisition'],
                receiver_local_peer_record_at_cuts=source != master, peer_open_current_proven=False,
                owner_claim_incarnation_proven=False, physical_frame_receipt_proven=False)


def pcm_claim_incarnation_bridge(wait, service, resources, snapshots, service_owner):
    """Bind the original native claim to a stable current process observation."""
    custody = pcm_source_custody_bridge(wait, service, resources, snapshots, service_owner)
    if custody is None or service_owner.get('stable') is not True:
        return None
    source = next(r for r in resources if r.get('kind') == 'pcm' and
                  r.get('node') == custody['source'] and r.get('tag') == wait['tag'])
    claim = source['local_owner'].get('claim')
    if (not isinstance(claim, dict) or
            any(type(claim.get(k)) is not int or not 0 < claim[k] < 2**64-1 for k in
                ('pid','proc_generation','owner_generation')) or claim['pid'] >= 2**31 or
            claim['pid'] != service_owner.get('pid') or
            claim['proc_generation'] != service_owner.get('proc_generation') or
            claim['owner_generation'] != custody['owner_generation']):
        return None
    return dict(quality='native_claim_incarnation_cut', pid=claim['pid'],
                procno=custody['owner_procno'], proc_generation=claim['proc_generation'],
                owner_generation=claim['owner_generation'], incarnation_matches_at_cut=True,
                peer_open_current_proven=False, physical_frame_receipt_proven=False)


def pcm_dependency_bridge(wait, service, resources, snapshots, service_owner, waiter):
    """Exact native causal dependency at finite cuts, never delivery authority.

    A current master is still awaiting this BLOCK successor; the same live
    claimant is executing its already admitted service. Packet reconstruction
    is unnecessary for that wait relation and remains explicitly unproved.
    """
    claim = pcm_claim_incarnation_bridge(wait, service, resources, snapshots, service_owner)
    if (claim is None or waiter.get('stable') is not True or
            waiter.get('node') != wait.get('requester') or waiter.get('pcm_wait') != wait or
            type(waiter.get('proc_generation')) is not int or waiter['proc_generation'] <= 0):
        return None
    source = service['source']
    local = next(s for s in snapshots if s['node'] == source)
    resource = next(r for r in resources if r.get('kind') == 'pcm' and
                    r.get('node') == source and r.get('tag') == wait['tag'])
    streams = local.get('streams_begin') or {}
    if (local.get('streams_stable') is not True or
            any(cut.get(side) != streams for cut in (local,resource)
                for side in ('streams_begin','streams_end')) or
            streams.get('known') is not True or streams.get('epoch') != wait['formation'] or
            type(streams.get('channels')) is not int or not 1 <= streams['channels'] <= 8):
        return None
    rows = streams.get('rows')
    if (not isinstance(rows,list) or len(rows) != 128 or
            any(not isinstance(row,dict) for row in rows) or
            [row.get('node') for row in rows] != list(range(128))):
        return None
    peer = rows[wait['expected_master']]
    loopback = source == wait['expected_master']
    data, control = peer.get('data'), peer.get('control')
    if loopback:
        if (type(streams.get('local_bits')) is not int or
                not 0 <= streams['local_bits'] < 2**32 or not streams['local_bits'] & 0x20000):
            return None
        data, control = [], None  # No remote stream identity exists for loopback.
    elif (peer.get('known') is not True or type(control) is not int or
            not 0 < control < 2**32 or
            control != service['service_admission']['peer_capability_generation'] or
            not isinstance(data,list) or len(data) != streams['channels'] or
            any(type(g) is not int or not 0 < g < 2**64-1 for g in data)):
        return None
    return dict(quality='exact_observed_pcm_dependency',
                native_pending_reply_dependency=True, claim=claim,
                current_peer=dict(node=wait['expected_master'], loopback=loopback, control=control, data=list(data)),
                atomic_graph=False, authority_proof=False, physical_frame_receipt_proven=False)


def build_graph(snapshots, resources):
    """Join copied native identities only; unknown edges remain in the result."""
    owners, unknown, edges, epochs, responsibilities = {}, [], [], {}, []
    for snapshot in snapshots:
        node = snapshot['node']
        stats = snapshot.get('stats', {})
        if snapshot.get('epoch_begin') != snapshot.get('epoch_end'):
            unknown.append(dict(node=node, reason='node_epoch_changed'))
            continue
        epochs[node] = snapshot.get('epoch_begin')
        for field in ('busy', 'changed', 'dead', 'omitted_locks'):
            if stats.get(field, 0):
                unknown.append(dict(node=node, reason=field, count=stats[field]))
        if stats.get('available') is False:
            unknown.append(dict(node=node, reason='publisher_unavailable'))
        ko = snapshot.get('ko')
        if ko is not None:
            responsibilities.append(dict(ko, node=node))
            if ko.get('status') != 0 or ko.get('omitted', 0):
                unknown.append(dict(node=node, reason='ko_snapshot_unavailable_or_capped', ko=ko))
            if any(row.get('failed') is not False or row.get('invalid') is not False
                   for row in ko.get('rows', [])):
                unknown.append(dict(node=node, reason='ko_process_resource_edges_unresolved', ko=ko))
        for row in snapshot.get('owners', []):
            if row.get('node') != node or row.get('stable', True) is not True:
                unknown.append(dict(node=node, reason='owner_changed_or_wrong_node', pid=row.get('pid')))
                continue
            generation = row.get('proc_generation')
            if (not isinstance(generation, int) or generation <= 0 or
                    (row['backend_id'] <= 0 and row['backend_id'] != -1)):
                unknown.append(dict(node=node, pid=row.get('pid'), procno=row.get('procno'),
                                    reason='owner_native_identity_unproven'))
                continue
            owners[(node, row['procno'])] = row
            for unproven in row.get('unproven_resources', []):
                unknown.append(dict(process=identity(row), reason='resource_lifetime_unproven', **unproven))
            if row.get('omitted_grants', 0):
                unknown.append(dict(process=identity(row), reason='omitted_grants',
                                    count=row['omitted_grants']))
        for proc in snapshot.get('procs', []):
            if proc.get('wait_name') in RESOURCE_WAITS and (node, proc['procno']) not in owners:
                unknown.append(dict(node=node, pid=proc['pid'], procno=proc['procno'],
                                    wait_event_info=proc['wait_event_info'], wait_name=proc.get('wait_name'),
                                    reason='unpublished_waiter'))
            owner = owners.get((node, proc['procno']))
            if owner is not None and (proc.get('stable') is False or
                    (proc['pid'], proc['backend_id'], proc.get('proc_generation')) !=
                    (owner['pid'], owner['backend_id'], owner['proc_generation'])):
                unknown.append(dict(node=node, procno=proc['procno'], reason='process_census_changed'))
                owners.pop((node, proc['procno']), None)
            elif owner is not None and proc.get('wait_name') in RESOURCE_WAITS and not any(
                    owner.get(key) for key in ('native_wait', 'enqueue', 'pcm_wait', 'tx_wait', 'ko_wait', 'retry_wait')):
                unknown.append(dict(process=identity(owner), reason='wait_target_unpublished',
                                    wait_name=proc.get('wait_name')))

    def add(waiter, holder, resource, quality):
        if identity(waiter) != identity(holder):
            edge = dict(from_=identity(waiter), to=identity(holder), resource=resource, quality=quality)
            edge['from'] = edge.pop('from_')
            if edge not in edges:
                edges.append(edge)

    for row in owners.values():
        native = row.get('native_wait')
        if native:
            tag = native.get('tag')
            matched = False
            for holder in owners.values():
                if holder['node'] != row['node']:
                    continue
                for lock in holder.get('locks', []):
                    # LW_EXCLUSIVE=0, LW_SHARED=1, UNTIL_FREE=2.
                    if (holder is not row and tag is not None and lock.get('tag') == tag
                            and (native['mode'] != 1 or lock['mode'] == 0)):
                        add(row, holder, dict(kind='content', node=row['node'], tag=tag),
                            'exact_local_content')
                        matched = True
            if not matched:
                unknown.append(dict(process=identity(row), reason='content_holder_missing', target=native))
        wait = row.get('enqueue')
        if wait:
            matches = []
            for resource in resources:
                if resource.get('kind') != 'enqueue' or resource.get('resid') != wait['resid']:
                    continue
                if resource.get('status') != 0 or resource.get('stable', True) is not True:
                    continue
                for member in resource['members']:
                    if (member['kind'] in (1, 2) and member['id'] == wait['holder'][:3] + [wait['request']]
                            and member['mode'] == wait['mode'] and member['wait_seq'] == wait['wait_seq']):
                        matches.append((resource, member))
            if len(matches) != 1:
                unknown.append(dict(process=identity(row), reason='enqueue_master_wait_not_unique', wait=wait))
                continue
            master, queued = matches[0]
            canonical = (master.get('route_known') is True and
                         master.get('master') == master.get('node') and
                         isinstance(master.get('master_generation'), int) and
                         master['master_generation'] >= 0 and master.get('shard_phase') == 0 and
                         master.get('epoch', 0) == wait.get('epoch', 0) != 0 and
                         epochs.get(row['node']) == wait.get('epoch') and
                         wait['holder'][:2] == [row['node'], row['procno']] and
                         wait['holder'][2] == wait.get('epoch'))
            matched = False
            for member in master['members']:
                if member['kind'] != 0 or member['mode'] not in CONFLICTS.get(wait['mode'], set()):
                    continue
                node, procno, epoch, request = member['id']
                holder = owners.get((node, procno))
                if holder is None:
                    unknown.append(dict(process=identity(row), reason='enqueue_holder_process_unmapped', holder=member))
                    continue
                # The instance epoch is a different namespace from a request ID.
                if epochs.get(node) != epoch:
                    unknown.append(dict(process=identity(row), reason='holder_epoch_mismatch', holder=member))
                    continue
                # Native GRD excludes the same instance's lock group; an
                # ungrouped holder is its own leader (procno+1).
                if node == row['node'] and (procno == row['procno'] or
                        (member.get('lock_group') or procno + 1) ==
                        (queued.get('lock_group') or row['procno'] + 1)):
                    continue
                grants = [grant for grant in holder.get('grants', []) if
                          grant.get('resid') == wait['resid'] and
                          grant.get('holder') == member['id'] and grant.get('mode') == member['mode']]
                exact = (canonical and wait['resid'][4] in (0xf2, 0xf6) and
                         member['mode'] == 7 and len(grants) == 1)
                add(row, holder, dict(kind='enqueue', resid=wait['resid'],
                                    master=master['node'], holder_request=request),
                    'exact_ges_holder' if exact else 'candidate_ges_process')
                if not exact:
                    unknown.append(dict(process=identity(row), reason='enqueue_holder_identity_or_route_unproven',
                                        holder=member))
                matched = True
            if not matched:
                unknown.append(dict(process=identity(row), reason='enqueue_no_proven_conflicting_holder', wait=wait))
        if row.get('pcm_wait'):
            wait = row['pcm_wait']
            # This inventory remains useful when no active service scope can
            # be certified. It never substitutes for the service bridge.
            holders = pcm_claimed_holders(wait.get('tag'), resources, owners)
            row['pcm_holder_observations'] = holders
            resolved_sources, awaited_sources = 0, 0
            keys = ('tag', 'requester', 'request_seq', 'formation',
                    'master_session', 'base', 'expected_master')
            for service_owner in owners.values():
                service = service_owner.get('pcm_service')
                if (service and wait.get('requester') == row['node'] and
                        wait.get('tag') is not None and
                        isinstance(wait.get('base'), int) and wait['base'] >= 0 and
                        isinstance(wait.get('expected_master'), int) and wait['expected_master'] >= 0 and
                        service.get('source') == service_owner['node'] and
                        all(isinstance(wait.get(key), int) and wait[key] > 0
                            for key in ('request_seq', 'formation', 'master_session')) and
                        all(wait.get(key) == service.get(key) for key in keys)):
                    # Current native pending successor + original claimant can
                    # prove the causal edge without inventing a frame receipt.
                    dependency = pcm_dependency_bridge(wait, service, resources, snapshots, service_owner, row)
                    logical = pcm_logical_bridge(wait, service, resources)
                    if dependency is not None:
                        resolved_sources |= 1 << service['source']
                        awaited_sources |= logical['awaited_sources']
                    add(row, service_owner, dict(kind='pcm', tag=wait['tag'],
                        request_seq=wait['request_seq'], requester=wait['requester'],
                        formation=wait['formation'], master_session=wait['master_session'],
                        base=wait['base'], master=wait['expected_master'],
                        logical_bridge=logical, dependency=dependency,
                        native_admission=pcm_admission_bridge(wait, service, resources),
                        current_context=pcm_current_bridge(wait, service, resources, snapshots),
                        source_custody=pcm_source_custody_bridge(wait, service, resources, snapshots, service_owner),
                        claim_incarnation=pcm_claim_incarnation_bridge(wait, service, resources, snapshots, service_owner),
                        service_wire_sender_connection=service.get('wire_sender_connection')),
                        'exact_observed_pcm_dependency' if dependency is not None else 'candidate_pcm_service')
            if not resolved_sources:
                unknown.append(dict(process=identity(row), reason='pcm_service_process_edge_unresolved',
                                    wait=wait, native_claimed_holders=holders,
                                    native_resource_cuts=[{k:r.get(k) for k in
                                        ('node','status','stable','view','queue','local_owner')}
                                        for r in resources if r.get('kind') == 'pcm' and
                                        r.get('tag') == wait.get('tag')]))
            elif awaited_sources & ~resolved_sources:
                unknown.append(dict(process=identity(row), reason='pcm_incompatible_sources_unmapped',
                                    source_mask=awaited_sources & ~resolved_sources, wait=wait))
        if row.get('tx_wait'):
            unknown.append(dict(process=identity(row), reason='tx_source_edge_unresolved', wait=row['tx_wait']))
        for field in ('ko_wait','retry_wait'):
            if row.get(field):
                unknown.append(dict(process=identity(row), reason=field+'_service_edge_unresolved', wait=row[field]))
    for resource in resources:
        absent = 1 if resource.get('kind') == 'enqueue' else 2
        if (resource.get('status') not in (0, absent) or resource.get('stable', True) is not True or
                resource.get('omitted_blocks', 0)):
            unknown.append(dict(reason='resource_unavailable_or_changed', resource=resource))
        for block in resource.get('blocks', []):
            if (block.get('decoded') is not True or block.get('slot', {}).get('state') not in (0, 1, 2) or
                    (block['slot']['state'] != 0 and block.get('slot_matches') is not True)):
                unknown.append(dict(reason='pcm_retained_block_invalid', node=resource.get('node'), block=block))
    return dict(version=3, atomic_graph=False, complete=not unknown,
                completeness_scope='captured_active_waits', responsibilities=responsibilities,
                vertices=[dict(id=identity(x), **x) for x in owners.values()],
                edges=edges, cycles=_cycles(edges), unknown=unknown)


def capture_failed_watchdog(collect):
    """The caller has reached its original failure boundary; never turns PASS."""
    result = dict(status='FAIL', reason='original_watchdog_expired', capture_error=None)
    try:
        result['capture'] = collect()
    except BaseException as exc:
        result['capture_error'] = repr(exc)
    return result


class DeadlineReceiver:
    """Local UDP observation only. Keep alive through original native stop.

    Use the node's TCP port number as a separate UDP port. No SQL connection
    survives/changes frontend retirement. Missing END/sequence means UNKNOWN.
    """
    def __init__(self, node, port, output):
        self.node, self.rows, self.errors = node, [], []
        self.path = Path(output)
        self.stream = self.path.open('xb')
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4*1024*1024)
            self.socket.bind(('127.0.0.1', port))
            self.socket.settimeout(.1)
        except BaseException:
            self.socket.close(); self.stream.close()
            raise
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._receive, daemon=True)
        self.thread.start()

    def _receive(self):
        while True:
            try:
                payload, sender = self.socket.recvfrom(4096)
            except socket.timeout:
                if self.stop.is_set():
                    break
                continue
            except OSError as exc:
                self.errors.append(repr(exc)); break
            self.stream.write(payload); self.stream.flush()
            text = payload.decode('utf-8', errors='replace')
            match = re.fullmatch(r'node=(\d+) observer=(\d+) sequence=(\d+) (.*)\n', text)
            if match and int(match[1]) == self.node and sender[0] == '127.0.0.1':
                self.rows.append(dict(observer=int(match[2]),sequence=int(match[3]),text=match[4]))
            else:
                self.errors.append('malformed_or_wrong_node_datagram')

    def close(self):
        # Caller closes only after original stop; allow already queued local
        # datagrams to drain before asking the receiver thread to exit.
        self.socket.settimeout(.05)
        self.stop.set(); self.thread.join(1)
        if self.thread.is_alive():
            raise RuntimeError('deadline receiver did not stop')
        self.socket.close(); self.stream.close()
        groups = {}
        for row in self.rows:
            groups.setdefault(row['observer'], []).append(row)
        result = dict(node=self.node,errors=self.errors,observers=[],complete=False,
                      completeness_scope='datagram_stream_only')
        for observer, rows in groups.items():
            ordered = sorted(rows,key=lambda r:r['sequence'])
            sequences = [r['sequence'] for r in ordered]
            complete = (sequences == list(range(1,len(rows)+1)) and
                        'PGRAC_WAIT_DEADLINE_BEGIN ' in ordered[0]['text'] and
                        'PGRAC_WAIT_DEADLINE_END ' in ordered[-1]['text'] and
                        ordered[-1]['text'].endswith(' dropped=0'))
            result['observers'].append(dict(observer=observer,records=len(rows),complete=complete))
        result['complete'] = bool(groups) and not self.errors and all(o['complete'] for o in result['observers'])
        self.path.with_suffix('.json').write_text(json.dumps(result,indent=2)+'\n')
        return result


class PsqlReader:
    """Owned persistent diagnostic connection, prepared before workload clients."""
    def __init__(self, node, directory):
        self.node = node['node']
        self.pending = b''
        self.counter = 0
        binary = shutil.which('psql')
        if binary is None:
            raise RuntimeError('installed psql missing')
        self.stderr = (directory / ('reader-node%d.stderr' % self.node)).open('xb')
        env = dict(os.environ, PGAPPNAME='pgrac-wait-graph',
                   PGCONNECT_TIMEOUT='3', LC_ALL='C',
                   PGOPTIONS='-c statement_timeout=2000 -c lock_timeout=2000 -c cluster.trace_tx_identity=on')
        try:
            # libpq expands conninfo only for an explicit dbname argument;
            # PGDATABASE defaults are not parsed as connection strings.
            self.process = subprocess.Popen([binary, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
                                             '-d', node['connstr']],
                                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                            stderr=self.stderr, env=env, start_new_session=True)
        except BaseException:
            self.stderr.close()
            raise
        os.set_blocking(self.process.stdout.fileno(), False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def query(self, sql, deadline):
        if self.process.poll() is not None:
            raise RuntimeError('diagnostic connection exited node%d rc%d' % (self.node, self.process.returncode))
        self.counter += 1
        marker = ('WG_END_%d_%d' % (os.getpid(), self.counter)).encode()
        self.process.stdin.write(sql.encode() + b';\n\\echo ' + marker + b'\n')
        self.process.stdin.flush()
        while True:
            lines = self.pending.split(b'\n')
            if marker in lines[:-1]:
                index = lines.index(marker)
                result = b'\n'.join(lines[:index]).decode('utf-8')
                self.pending = b'\n'.join(lines[index + 1:])
                return result.strip()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('diagnostic query budget expired node%d' % self.node)
            events = self.selector.select(min(remaining, .25))
            for key, _ in events:
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    raise RuntimeError('diagnostic connection closed node%d' % self.node)
                self.pending += chunk

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        self.selector.close()
        self.process.stdin.close()
        self.process.stdout.close()
        self.stderr.close()


class CaptureSession:
    def __init__(self, config, output):
        if len(config) != 4 or {x['node'] for x in config} != set(range(4)):
            raise ValueError('exact four-node connection map required')
        if len({x['connstr'] for x in config}) != 4:
            raise ValueError('duplicate node connection')
        self.output = Path(output)
        self.output.mkdir(exist_ok=False)
        self.readers = []
        try:
            for node in sorted(config, key=lambda x: x['node']):
                reader = PsqlReader(node, self.output)
                self.readers.append(reader)
            # DSA attach is a separate pre-workload operation, never inside a hang snapshot.
            for reader in self.readers:
                if reader.query('SELECT test_pgrac_wait_graph_prepare()', time.monotonic() + 5) != 't':
                    raise RuntimeError('diagnostic prepare unavailable node%d' % reader.node)
                # Plan and execute every diagnostic shape before clients. The
                # later hang cut reuses these connections and cached plans.
                preparations = [
                    'PREPARE pgrac_wg_local AS SELECT test_pgrac_wait_graph()',
                    'PREPARE pgrac_wg_enqueue(bigint,bigint,bigint,bigint,bigint,bigint) AS '
                    'SELECT test_pgrac_wait_graph_enqueue($1,$2,$3,$4,$5,$6)',
                    'PREPARE pgrac_wg_block(bigint,bigint,bigint,bigint,bigint) AS '
                    'SELECT test_pgrac_wait_graph_block($1,$2,$3,$4,$5)',
                    'EXECUTE pgrac_wg_local', 'EXECUTE pgrac_wg_enqueue(0,0,0,0,255,255)',
                    'EXECUTE pgrac_wg_block(0,0,0,0,0)']
                for sql in preparations:
                    reader.query(sql, time.monotonic() + 5)
        except BaseException:
            self.close()
            raise

    def _query_all(self, sql_by_node, deadline):
        def one(reader):
            record = dict(node=reader.node, started_unix=time.time())
            try:
                raw = reader.query(sql_by_node[reader.node], deadline)
                record['raw'] = raw
                value = json.loads(raw)
                if not isinstance(value, dict) or value.get('node') != reader.node:
                    record['rejected_value'] = value
                    raise ValueError('response node identity mismatch: expected node%d' % reader.node)
                record.update(value=value, status='OK')
            except BaseException as exc:
                record.update(status='ERROR', error=repr(exc))
            record['finished_unix'] = time.time()
            return record
        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(one, self.readers))

    def capture(self, budget_seconds=30):
        deadline = time.monotonic() + budget_seconds
        result = dict(status='DIAGNOSTIC_ONLY', started_unix=time.time(), rounds=[],
                      budget_seconds=budget_seconds, resources=[], errors=[])
        def save():
            (self.output / 'capture.json').write_text(json.dumps(result, indent=2) + '\n')
        save()
        for cut in range(2):
            result['rounds'].append(self._query_all({n: 'EXECUTE pgrac_wg_local' for n in range(4)}, deadline))
            if cut == 0:
                keys = set()
                for frame in result['rounds'][0]:
                    for owner in frame.get('value', {}).get('owners', []):
                        for lock in owner.get('locks', []) + [owner.get('native_wait') or {},
                                owner.get('pcm_wait') or {}, owner.get('pcm_service') or {},
                                owner.get('retry_wait') or {}]:
                            if lock.get('tag') is not None:
                                keys.add(('pcm', tuple(lock['tag'])))
                        for grant in owner.get('grants', []):
                            keys.add(('enqueue', tuple(grant['resid'])))
                        if owner.get('enqueue'):
                            keys.add(('enqueue', tuple(owner['enqueue']['resid'])))
                result['resource_keys'] = [[kind, list(key)] for kind, key in sorted(keys)]
                for resource_cut in range(2):
                    for kind, key in sorted(keys):
                        if time.monotonic() >= deadline:
                            result['errors'].append('resource capture budget exhausted')
                            break
                        function = 'pgrac_wg_block' if kind == 'pcm' else 'pgrac_wg_enqueue'
                        # Only integers from validated native typed resource keys enter SQL.
                        sql = 'EXECUTE %s(%s)' % (function, ','.join(str(int(x)) for x in key))
                        frames = self._query_all({n: sql for n in range(4)}, deadline)
                        result['resources'].append(dict(cut=resource_cut, kind=kind, key=list(key), frames=frames))
                        result['errors'].extend('resource node%d: %s' % (f['node'], f['error'])
                                                for f in frames if f['status'] != 'OK')
                        save()
            save()
        before = {x['node']: x.get('value') for x in result['rounds'][0]}
        after = {x['node']: x.get('value') for x in result['rounds'][1]}
        snapshots = []
        for node in range(4):
            first, last = before[node], after[node]
            if first is None or last is None:
                result['errors'].append('node%d missing local cut' % node)
                continue
            if not (first['epoch_begin'] == first['epoch_end'] ==
                    last['epoch_begin'] == last['epoch_end']):
                result['errors'].append('node%d epoch changed across cuts' % node)
                continue
            stable_owners = []
            for owner in first['owners']:
                matching = [x for x in last['owners'] if x['procno'] == owner['procno']]
                stable_owners.append(stabilize_owner(owner, matching[0] if len(matching) == 1 else None))
            for owner in last['owners']:
                if not any(x['procno'] == owner['procno'] for x in first['owners']):
                    stable_owners.append(dict(owner, stable=False))
            # Raw cuts remain intact in capture.json, including all changed
            # targets. The graph uses only individually proved projections.
            first = dict(first, owners=stable_owners)
            first['context_stable'] = all(first.get(k) == last.get(k) for k in CONTEXT_CUTS)
            first['peer_stable'] = all(first.get(k) == last.get(k) for k in ('peer_begin','peer_end'))
            first['streams_stable'] = all(first.get(k) == last.get(k) for k in ('streams_begin','streams_end'))
            snapshots.append(first)
        resources = []
        for entry in result['resources']:
            if entry['cut'] != 0:
                continue
            matching = [x for x in result['resources'] if x['cut'] == 1 and x['kind'] == entry['kind'] and x['key'] == entry['key']]
            final = {x['node']: x.get('value') for x in matching[0]['frames']} if len(matching) == 1 else {}
            for frame in entry['frames']:
                value = frame.get('value')
                if value is not None:
                    value['stable'] = final.get(frame['node']) == value
                    resources.append(value)
        graph = build_graph(snapshots, resources)
        # Completeness describes this cut's active waits, not an installed or
        # protocol qualification. Unsupported *active* waits remain UNKNOWN.
        if result['errors'] or len(snapshots) != 4:
            graph['complete'] = False
            graph['unknown'].append(dict(reason='capture_incomplete', errors=result['errors']))
        (self.output / 'wait-for-graph.json').write_text(json.dumps(graph, indent=2) + '\n')
        result.update(finished_unix=time.time(), graph_complete=graph['complete'], cycles=graph['cycles'])
        save()
        return dict(output=str(self.output), complete=graph['complete'], cycles=graph['cycles'])

    def close(self):
        for reader in self.readers:
            reader.close()
