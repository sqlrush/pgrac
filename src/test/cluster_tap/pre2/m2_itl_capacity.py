"""Select only a fully matched observed ITL capacity blocker for a test."""
import json
import re
import struct
import sys


KEY_FIELDS = (
    'format_version', 'owner_instance', 'origin_node_id', 'segment_id',
    'segment_generation', 'slot_offset', 'slot_wrap', 'xid', 'cluster_epoch',
    'system_identifier', 'origin_boot_incarnation', 'formation_epoch',
    'admission_record_generation', 'root_descriptor_incarnation', 'root_id', 'root_generation')


def number(value):
    if type(value) is not int and not (isinstance(value, str) and re.fullmatch(r'[0-9]+', value)):
        raise ValueError('missing or invalid integer observation')
    result = int(value)
    if not 0 <= result < 2**64:
        raise ValueError('integer observation out of range')
    return result


def select_blocker(case):
    """Return a diagnostic join for one owned test client, never a verdict."""
    trace = case['trace']
    if (trace.get('export_complete') is not True or trace.get('overflow') is not False
            or trace.get('available') is not True or trace.get('enabled') is not True):
        raise ValueError('incomplete native trace')
    holders = case['holders']
    if (not 1 <= len(holders) <= 8 or len({h['pid'] for h in holders}) != len(holders)
            or len({(h['node'], h['xid']) for h in holders}) != len(holders)
            or any(h['pid'] == case['waiter_pid'] for h in holders)):
        raise ValueError('ambiguous or self producer registry')
    pages = [e for e in trace['events'] if e.get('kind') == 4
             and e.get('sequence') == case['page_sequence']]
    if len(pages) != 1 or pages[0].get('page_known') is not True or pages[0].get('tag') != case['tag']:
        raise ValueError('copied page identity unavailable')
    page = pages[0]
    if page.get('authoritative') is not False or len(page['slots']) != 8:
        raise ValueError('invalid copied page observation')
    active = []
    for index, slot in enumerate(page['slots']):
        if slot.get('index') != index or slot.get('flags') not in (1, 2, 3):
            raise ValueError('page is not the expected full DATA ITL set')
        if slot['flags'] == 1:
            active.append(slot)
    if len(active) != len(holders):
        raise ValueError('active page and producer registry disagree')
    if not active:
        raise ValueError('no active blocker')
    slot = active[0]  # Native selector's lowest page index, independent of clients.
    matching = [h for h in holders if h['xid'] == slot['xid']]
    if len(matching) != 1:
        raise ValueError('lowest active slot has no unique registered producer')
    holder = matching[0]
    producers = [e for e in trace['events'] if e.get('kind') == 2
                 and e.get('pid') == holder['pid'] and e.get('node') == holder['node']
                 and e.get('key', {}).get('xid') == holder['xid']]
    if len(producers) != 1 or producers[0].get('key_known') is not True or producers[0].get('authoritative') is not False:
        raise ValueError('canonical ACTIVE publication missing or ambiguous')
    producer = producers[0]
    try:
        key = {name: number(producer['key'][name]) for name in KEY_FIELDS}
    except KeyError as exc:
        raise ValueError('incomplete canonical producer key') from exc
    tt = producer.get('tt_slot', {})
    if (key['format_version'] != 1 or key['origin_node_id'] != holder['node']
            or key['owner_instance'] != holder['node'] + 1 or key['xid'] != holder['xid']
            or not 0 <= key['slot_wrap'] < 65535
            or not all(key[k] for k in ('segment_id', 'system_identifier',
                        'origin_boot_incarnation', 'admission_record_generation',
                        'root_descriptor_incarnation', 'root_id', 'root_generation'))
            or any(tt.get(k) != v for k, v in dict(xid=key['xid'], wrap=key['slot_wrap'], status=1).items())):
        raise ValueError('invalid canonical ACTIVE publication')
    rows = []
    for line in case['log'].splitlines():
        if 'TX_IDENTITY ' not in line:
            continue
        pairs = re.findall(r'([a-z_0-9]+)=([^ \n]+)', line.split('TX_IDENTITY ', 1)[1])
        row = dict(pairs)
        if len(row) != len(pairs):
            raise ValueError('duplicate diagnostic fields')
        if number(row.get('pid')) != case['waiter_pid'] or number(row.get('actor_node')) != case['waiter_node']:
            continue
        if row.get('event') == 'LIMIT':
            raise ValueError('wait identity trace overflow')
        if row.get('event') in ('WAIT_TARGET', 'WAIT_END'):
            rows.append(row)
    starts = [r for r in rows if r['event'] == 'WAIT_TARGET']
    if not starts:
        raise ValueError('no exact target wait observed')
    latest = max(number(r.get('seq')) for r in starts)
    current = [r for r in starts if number(r['seq']) == latest]
    if len(current) != 1 or any(number(r.get('seq')) >= latest for r in rows if r['event'] == 'WAIT_END'):
        raise ValueError('target wait ended or ambiguous')
    wait = current[0]
    required = dict(node=holder['node'], xid=key['xid'], epoch=key['formation_epoch'],
                    target_known=1, uba_known=1, itl_wrap=key['slot_wrap'],
                    itl_kind=1, itl_slot=slot['index'])
    if any(number(wait.get(k)) != v for k, v in required.items()) or number(wait.get('wait_seq')) == 0:
        raise ValueError('current wait does not name the lowest exact DATA owner')
    try:
        words = [int(wait[k], 16) for k in ('uba0', 'uba1')]
        if any(not re.fullmatch('[0-9a-f]{16}', wait[k]) for k in ('uba0', 'uba1')):
            raise ValueError('invalid UBA observation')
        # Existing native dump is in memory byte order. This fixture is for
        # homogeneous little-endian Mac/Linux builds, not a wire decoder.
        if sys.byteorder != 'little' or struct.pack('<QQ', *words).hex() != slot['uba_bytes']:
            raise ValueError('wait UBA differs from copied page')
    except (KeyError, OverflowError, struct.error) as exc:
        raise ValueError('incomplete wait UBA') from exc
    if ((words[0] & 0xffffffff) != key['segment_id'] or words[0] >> 32 == 0
            or (words[1] & 0xffff) != key['slot_offset'] or words[1] >> 32 != 0):
        raise ValueError('wait UBA and canonical coordinates disagree')
    return dict(holder, slot=slot['index'], page_itl_wrap=slot['itl_wrap'],
                key=key, wait_seq=number(wait['wait_seq']), authoritative=False)


if __name__ == '__main__':
    try:
        print(json.dumps(dict(selected=select_blocker(json.load(sys.stdin)))))
    except (ValueError, KeyError, TypeError) as exc:
        print(json.dumps(dict(error=str(exc))))
        raise SystemExit(1)
