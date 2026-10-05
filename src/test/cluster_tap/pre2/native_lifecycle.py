"""Project the existing native writer CLI and SQL into lifecycle TAP fields.

No expected writer, elapsed silence or SQL success supplies an observation.
These are test mappings, not database permissions or a product CLI ABI.
Author: SqlRush <sqlrush@gmail.com>
"""
import json
import re


OPERATIONS = frozenset(('root_observation', 'open_observation', 'shutdown_observation'))
ENTRY = {'native': 'writer-lifecycle-v1'}
RUNTIME_SQL = """SELECT json_build_object(
  'runtime', (SELECT CASE WHEN value = 'unavailable' THEN NULL ELSE value::json END
    FROM cluster_dump_state() WHERE category='lifecycle' AND key='native_writer'),
  'membership', (SELECT json_agg(m) FROM cluster_get_membership() m))"""


def _root(cut):
    value = {key: cut[key] for key in ('system_identifier', 'root_digest', 'generation')}
    if (not re.fullmatch('[1-9][0-9]*', value['system_identifier'])
            or not re.fullmatch('[0-9a-f]{64}', value['root_digest'])
            or type(value['generation']) is not int or value['generation'] <= 0):
        raise ValueError('native selected ROOT identity is incomplete')
    return value


def _sample(driver, node):
    result = driver.command('postgres', ['--pgrac-observe-writer', node['data_dir'],
        driver.layout['shared_data_dir'], driver.layout['wal_root'], str(node['id'])], seconds=10)
    cut = json.loads(result.stdout)
    _root(cut)
    if cut['writer']['node'] != node['id']:
        raise ValueError('native writer does not belong to the observed node')
    return cut


def _positive(value):
    return type(value) is int and value > 0


def _incarnation(value):
    # The membership SQL SRF returns int8; the original identity is uint64.
    return value & ((1 << 64) - 1) if type(value) is int else None


def _open(driver, node):
    nodes = driver.layout['nodes']
    cuts = [_sample(driver, n) for n in nodes]
    before = cuts[node['id']]
    result = json.loads(driver.sql(node, RUNTIME_SQL).stdout)
    after = _sample(driver, node)
    root = _root(before)
    pending = dict(root, phase='WAIT')
    ids = [n['id'] for n in nodes]
    if (before != after or any(_root(c) != root or c['root_phase'] != 'OPEN'
                              or c['members'] != ids for c in cuts)):
        return pending
    runtime, members = result.get('runtime'), result.get('membership')
    if (not isinstance(runtime, dict) or not isinstance(members, list)
            or any(not _positive(runtime.get(k))
                   for k in ('epoch', 'resource_x_formation', 'r4_generation'))):
        return pending
    writer = runtime.get('writer')
    if (not isinstance(writer, dict) or not isinstance(writer.get('predecessor'), dict)
            or {k: v for k, v in writer.items() if k != 'predecessor'} != before['writer']):
        return pending
    if (any(not isinstance(m, dict) for m in members)
            or sorted(m.get('node_id', -1) for m in members) != ids):
        return pending
    for member in members:
        selected = cuts[member['node_id']]['writer']
        if (member.get('declared') is not True or member.get('state') != 'member'
                or member.get('removed') is not False
                or member.get('admitted_epoch') != runtime['epoch']
                or _incarnation(member.get('presented_incarnation')) != selected['incarnation']
                or _incarnation(member.get('last_admitted_incarnation')) != selected['incarnation']):
            return pending
    return dict(root, phase='OPEN', members=before['members'], writer=writer)


def observe(driver, op, args):
    nodes = driver.layout['nodes']
    if [n['id'] for n in nodes] != list(range(len(nodes))):
        raise ValueError('lifecycle mapping requires the declared fixed cohort')
    if op == 'root_observation':
        return _root(_sample(driver, nodes[0]))
    if op == 'open_observation':
        node = args.get('node')
        if type(node) is not int or not 0 <= node < len(nodes):
            raise ValueError('invalid lifecycle observation node')
        return _open(driver, nodes[node])
    if op != 'shutdown_observation' or args.get('nodes') != list(range(len(nodes))):
        raise ValueError('native lifecycle mapping requires a complete fixed-member stop')
    cuts = [_sample(driver, n) for n in nodes]
    root = _root(cuts[0])
    if any(_root(c) != root or c['root_phase'] != 'CLOSED'
           or c.get('final_checkpoint') is not True for c in cuts):
        raise ValueError('native CLOSED final checkpoint is unproven')
    # Process exit remains the responsibility of the original shared_stop judge.
    return dict(root, final_checkpoint=True,
                writers=[dict(c['writer'], state=c['root_phase']) for c in cuts])
