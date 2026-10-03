"""Canonical input for one native fixed-member initdb cohort.

Only configuration text is produced here, never persistent authority bytes.
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import re


def quote(value):
    value = str(value)
    if '\n' in value or '\r' in value or '\0' in value:
        raise ValueError('multiline configuration value')
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def test_settings(lines, fixed):
    """Render test inputs, leaving type/scope qualification to native initdb.

    INSTANCE names follow config_policies in cluster_shared_config_guc.c;
    ordinary scalar defaults are COMMON. Unsupported strings still fail the
    original producer's policy. Discovery/identity bindings cannot be replaced.
    """
    instance_names = {'shared_buffers', 'unix_socket_group', 'unix_socket_permissions',
                      'log_directory', 'log_filename', 'logging_collector', 'log_destination',
                      'log_line_prefix', 'cluster.external_fence_socket_path'}
    common, instance = {}, {}
    for line in lines:
        match = re.fullmatch(r"\s*([a-z][a-z0-9_.]*)\s*=\s*('(?:''|[^'\\\r\n])*'|[A-Za-z0-9_./:+-]+)\s*", line)
        if not match or '\n' in line or '\r' in line or '\0' in line:
            raise ValueError('test setting must be one scalar assignment')
        name, value = match.groups()
        if value.startswith("'"):
            value = value[1:-1].replace("''", "'")
        if name in fixed:
            if fixed[name] is None or value != str(fixed[name]):
                raise ValueError('test setting changes fixed cohort identity: '+name)
            continue
        target = instance if name in instance_names else common
        if name in target:
            raise ValueError('duplicate cohort test setting: '+name)
        target[name] = value
    return common, instance


def request(layout, quorum_name, addresses, system_identifier, authority_uuid, storage_uuid):
    nodes = layout['nodes']
    if len(nodes) not in (2, 4) or [n['id'] for n in nodes] != list(range(len(nodes))):
        raise ValueError('cohort requires the complete fixed two or four member set')
    if (len(addresses) != len(nodes) or len(set(addresses)) != len(nodes)
            or not re.fullmatch('[a-zA-Z0-9_]+', quorum_name)):
        raise ValueError('ambiguous storage member mapping')
    for value in (authority_uuid, storage_uuid):
        if not re.fullmatch('[0-9a-f]{32}', value) or int(value, 16) == 0:
            raise ValueError('invalid creation UUID')
    if authority_uuid[12] != '4' or authority_uuid[16] not in '89ab':
        raise ValueError('native ROOT authority UUID must be RFC-4122 version 4')
    if not 0 < int(system_identifier) < 2**64:
        raise ValueError('invalid creation system identity')
    root = Path(layout['root'])
    common = {
        'cluster.controlfile_shared_authority': 'on', 'cluster.enabled': 'on',
        'cluster.merged_recovery': 'on', 'cluster.shared_catalog': 'on',
        'cluster.shared_config': 'on', 'cluster.shared_data_dir': layout['shared_data_dir'],
        'cluster.shared_storage_backend': 'cluster_fs',
        'cluster.shared_storage_uuid': '-'.join([storage_uuid[:8], storage_uuid[8:12],
            storage_uuid[12:16], storage_uuid[16:20], storage_uuid[20:]]),
        'cluster.smgr_user_relations': 'on', 'cluster.undo_tablespace_path': str(root/'undo'),
        'cluster.wal_threads_dir': layout['wal_root'], 'cluster.interconnect_tier': 'tier1',
        'cluster.lms_workers': '1', 'cluster.online_join': 'on', 'cluster.xid_striping': 'on',
        'cluster.cf_enqueue_timeout_ms': '1000',
        'cluster.storage_quorum_cluster': quorum_name,
        'cluster.storage_quorum_nodes': ','.join(f'{n}:{n+1}' for n in range(len(nodes))),
    }
    fixed = {name: value for name, value in common.items() if name not in (
        'cluster.lms_workers', 'cluster.cf_enqueue_timeout_ms')}
    # These vary per node and are owned by the caller's allocated topology.
    fixed.update({'cluster.node_id': None, 'listen_addresses': None,
                  'port': None, 'unix_socket_directories': None})
    extra_common, extra_instance = test_settings(layout.get('extra_conf', []), fixed)
    common.update(extra_common)
    text = (f'@authority_uuid={authority_uuid}\n@configured_0={(1 << len(nodes))-1:016x}\n'
            f'@configured_1=0000000000000000\n@database_incarnation=1\n@format=1\n@generation=1\n'
            f'@storage_uuid={storage_uuid}\n@system_identifier={system_identifier}\n')
    text += ''.join(f'common.{key}={quote(common[key])}\n' for key in sorted(common))
    for node, address in zip(nodes, addresses):
        values = {'cluster.node_id': node['id'], 'listen_addresses': address, 'port': node['port'],
                  'shared_buffers': '16MB', 'unix_socket_directories': node['host']}
        values.update(extra_instance)
        text += ''.join(f'node{node["id"]:03d}.{key}={quote(values[key])}\n' for key in sorted(values))
    return text


def topology(name, nodes, addresses):
    return '[cluster]\nname = '+name+'\n' + ''.join(
        f'[node.{node["id"]}]\ninterconnect_addr = {address}:{node["ic_port"]}\n'
        f'data_addr = {address}:{node["data_port"]}\n' for node, address in zip(nodes, addresses))


def discovery(layout, node, votes):
    root = Path(layout['root'])
    values = {'cluster.enabled': 'on', 'cluster.shared_config': 'on',
              'cluster.controlfile_shared_authority': 'on', 'cluster.node_id': node['id'],
              'cluster.shared_data_dir': layout['shared_data_dir'],
              'cluster.wal_threads_dir': layout['wal_root'],
              'cluster.undo_tablespace_path': str(root/'undo'),
              'cluster.voting_disks': ','.join(votes)}
    return '\n' + ''.join(f'{key}={quote(value)}\n' for key, value in values.items())
