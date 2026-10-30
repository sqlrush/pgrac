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


def validate_wait_exit(case):
    """Require the original wait to end without releasing any test owner."""
    selected = select_blocker(case['start'])
    if not case['log'].startswith(case['start']['log']):
        raise ValueError('exit log no longer contains the original observation')
    causes = dict(cancel='canceling statement due to user request',
                  statement_timeout='canceling statement due to statement timeout')
    kind = case['kind']
    if kind not in causes or case['cancel_sent'] is not (kind == 'cancel'):
        raise ValueError('unexpected cancellation boundary or signal')
    errors = re.findall(r'^ERROR:\s+([0-9A-Z]{5}): (.*)$', case['stderr'], re.MULTILINE)
    if (case['client_had_error'] is not True or case['stdout'].strip() != 'B08_SQLSTATE 57014'
            or errors != [('57014', causes[kind])]):
        raise ValueError('original SQLSTATE and native error cause are not proved')
    reader = case['reader']
    if (number(reader['pid']) != case['start']['waiter_pid']
            or reader['state'] != 'idle in transaction (aborted)'
            or reader.get('wait_event') == 'GesTxEnqueueWait'):
        raise ValueError('original backend did not leave its wait in the aborted transaction')
    identity = lambda h: tuple(number(h[k]) for k in ('node', 'pid', 'xid', 'row'))
    expected = {identity(h) for h in case['start']['holders']}
    holders = case['holders']
    if (len(holders) != len(expected) or {identity(h) for h in holders} != expected
            or any(h['state'] != 'idle in transaction' for h in holders)):
        raise ValueError('an original owner was released, replaced or omitted')
    rows = []
    for line in case['log'].splitlines():
        if 'TX_IDENTITY ' not in line:
            continue
        pairs = re.findall(r'([a-z_0-9]+)=([^ \n]+)', line.split('TX_IDENTITY ', 1)[1])
        row = dict(pairs)
        if len(row) != len(pairs):
            raise ValueError('duplicate diagnostic fields')
        if (number(row.get('pid')) != case['start']['waiter_pid']
                or number(row.get('actor_node')) != case['start']['waiter_node']):
            continue
        if row.get('event') == 'LIMIT':
            raise ValueError('wait identity trace overflow')
        if row.get('event') in ('WAIT_TARGET', 'WAIT_END'):
            rows.append(row)
    starts = [r for r in rows if r['event'] == 'WAIT_TARGET'
              and number(r.get('wait_seq')) == selected['wait_seq']]
    if len(starts) != 1:
        raise ValueError('original wait start missing or ambiguous')
    start = starts[0]
    sequence = number(start.get('seq'))
    later = [r for r in rows if number(r.get('seq')) >= sequence]
    ends = [r for r in later if r['event'] == 'WAIT_END']
    if len(later) != 2 or len(ends) != 1 or number(ends[0].get('seq')) <= sequence:
        raise ValueError('original wait end missing, ambiguous or superseded')
    end = ends[0]
    fields = ('node', 'xid', 'epoch', 'target_known', 'uba_known',
              'itl_wrap', 'itl_kind', 'itl_slot', 'wait_seq')
    if (any(number(start.get(k)) != number(end.get(k)) for k in fields)
            or any(start.get(k) != end.get(k) for k in ('uba0', 'uba1'))):
        raise ValueError('wait end does not match the original exact blocker')
    return dict(kind=kind, sqlstate='57014', selected=selected,
                wait_seq=selected['wait_seq'], end_seq=number(end['seq']), authoritative=False)


if __name__ == '__main__':
    try:
        case = json.load(sys.stdin)
        if sys.argv[1:] == ['--exit']:
            print(json.dumps(dict(exit=validate_wait_exit(case))))
        elif not sys.argv[1:]:
            print(json.dumps(dict(selected=select_blocker(case))))
        else:
            raise ValueError('unexpected evaluator arguments')
    except (ValueError, KeyError, TypeError) as exc:
        print(json.dumps(dict(error=str(exc))))
        raise SystemExit(1)
