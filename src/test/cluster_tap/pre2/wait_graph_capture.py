"""Read-only four-node wait evidence. No cancel, grant or workload verdict authority."""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor


# Native PostgreSQL heavyweight conflict table; mode0 is not a granted lock.
CONFLICTS = {1: {8}, 2: {7, 8}, 3: {5, 6, 7, 8},
             4: {4, 5, 6, 7, 8}, 5: {3, 4, 6, 7, 8},
             6: {3, 4, 5, 6, 7, 8}, 7: {2, 3, 4, 5, 6, 7, 8},
             8: set(range(1, 9))}


def identity(row):
    return 'n%d:p%d:s%d:b%d:t%d:g%d' % tuple(row[x] for x in
                                        ('node', 'pid', 'procno', 'backend_id', 'backend_start', 'proc_generation'))


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


def build_graph(snapshots, resources):
    """Join copied native identities only; unknown edges remain in the result."""
    owners, unknown, edges, epochs = {}, [], [], {}
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
            if ko.get('status') != 0 or ko.get('omitted', 0):
                unknown.append(dict(node=node, reason='ko_snapshot_unavailable_or_capped', ko=ko))
            if ko.get('rows'):
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
                    owner.get(key) for key in ('native_wait', 'enqueue', 'pcm_wait', 'tx_wait')):
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
                    # This is an active native call, not a grant or canonical
                    # route/transport receipt. Master sender != requester wire.
                    add(row, service_owner, dict(kind='pcm', tag=wait['tag'],
                        request_seq=wait['request_seq'], requester=wait['requester'],
                        formation=wait['formation'], master_session=wait['master_session'],
                        base=wait['base'], master=wait['expected_master'],
                        logical_bridge=pcm_logical_bridge(wait, service, resources),
                        native_admission=pcm_admission_bridge(wait, service, resources),
                        current_context=pcm_current_bridge(wait, service, resources, snapshots),
                        service_wire_sender_connection=service.get('wire_sender_connection')),
                        'candidate_pcm_service')
            unknown.append(dict(process=identity(row), reason='pcm_service_process_edge_unresolved',
                                wait=row['pcm_wait']))
        if row.get('tx_wait'):
            unknown.append(dict(process=identity(row), reason='tx_source_edge_unresolved', wait=row['tx_wait']))
    for resource in resources:
        absent = 1 if resource.get('kind') == 'enqueue' else 2
        if (resource.get('status') not in (0, absent) or resource.get('stable', True) is not True or
                resource.get('omitted_blocks', 0)):
            unknown.append(dict(reason='resource_unavailable_or_changed', resource=resource))
        for block in resource.get('blocks', []):
            if (block.get('decoded') is not True or block.get('slot', {}).get('state') not in (0, 1, 2) or
                    (block['slot']['state'] != 0 and block.get('slot_matches') is not True)):
                unknown.append(dict(reason='pcm_retained_block_invalid', node=resource.get('node'), block=block))
    return dict(version=2, atomic_graph=False, complete=not unknown,
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
                                owner.get('pcm_wait') or {}, owner.get('pcm_service') or {}]:
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
            for owner in first['owners']:
                matching = [x for x in last['owners'] if x['procno'] == owner['procno']]
                owner['stable'] = len(matching) == 1 and matching[0] == owner
            first['context_stable'] = all(first.get(k) == last.get(k) for k in CONTEXT_CUTS)
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
        # Canonical source/service transport joins remain unqualified. Even a
        # quiet cut cannot qualify full S2 in this slice.
        graph['observed_edges_resolved'] = graph['complete']
        graph['complete'] = False
        graph['unknown'].append(dict(reason='s2_source_join_qualification_pending',
                                    sources=['pcm_service', 'other_ges_holder_lifecycles', 'tx']))
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
