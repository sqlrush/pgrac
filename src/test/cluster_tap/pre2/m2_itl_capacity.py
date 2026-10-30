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


def select_page_owner(case):
    """Map the lowest observed DATA slot to an owned test client, not authority."""
    trace = case['trace']
    if (trace.get('export_complete') is not True or trace.get('overflow') is not False
            or trace.get('available') is not True or trace.get('enabled') is not True):
        raise ValueError('incomplete native trace')
    holders = case['holders']
    if (not 1 <= len(holders) <= 8 or len({h['pid'] for h in holders}) != len(holders)
            or len({(h['node'], h['xid']) for h in holders}) != len(holders)):
        raise ValueError('ambiguous producer registry')
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
    return dict(holder, slot=slot['index'], page_itl_wrap=slot['itl_wrap'],
                key=key, authoritative=False)


def select_blocker(case):
    """Return a diagnostic join for one owned test client, never a verdict."""
    if any(h['pid'] == case['waiter_pid'] for h in case['holders']):
        raise ValueError('self producer registry')
    selected = select_page_owner(case)
    holder, key = selected, selected['key']
    page = next(e for e in case['trace']['events'] if e.get('kind') == 4
                and e.get('sequence') == case['page_sequence'])
    slot = page['slots'][selected['slot']]
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
    return dict(selected, wait_seq=number(wait['wait_seq']))


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


def _deadlock_wait_rows(log, pid, node):
    rows = []
    offset = 0
    for line in log.splitlines(keepends=True):
        offset += len(line)
        if 'TX_IDENTITY ' not in line:
            continue
        pairs = re.findall(r'([a-z_0-9]+)=([^ \n]+)', line.split('TX_IDENTITY ', 1)[1])
        row = dict(pairs)
        if len(row) != len(pairs):
            raise ValueError('duplicate diagnostic fields')
        if number(row.get('pid')) != pid:
            continue
        if row.get('event') == 'LIMIT':
            raise ValueError('wait identity trace overflow')
        if number(row.get('actor_node')) != node:
            raise ValueError('waiter node changed')
        if row.get('event') in ('WAIT_TARGET', 'WAIT_END'):
            if rows and number(row.get('seq')) <= number(rows[-1][0].get('seq')):
                raise ValueError('wait trace reordered or duplicated')
            rows.append((row, offset))
    return rows


def _deadlock_original_end(rows, selected):
    if len(rows) < 2:
        raise ValueError('both original wait endpoints are required')
    start, end = rows[0][0], rows[1][0]
    fields = ('node', 'xid', 'epoch', 'target_known', 'uba_known',
              'itl_wrap', 'itl_kind', 'itl_slot', 'wait_seq', 'proc')
    if (start['event'] != 'WAIT_TARGET' or end['event'] != 'WAIT_END'
            or number(start.get('wait_seq')) != selected['wait_seq']
            or any(number(start.get(k)) != number(end.get(k)) for k in fields)
            or any(start.get(k) != end.get(k) for k in ('uba0', 'uba1'))):
        raise ValueError('original exact wait end is missing or changed')
    sequence = selected['wait_seq']
    for index, (row, _) in enumerate(rows[2:]):
        if row['event'] != ('WAIT_TARGET' if index % 2 == 0 else 'WAIT_END'):
            raise ValueError('overlapping or duplicate wait registration')
        current = number(row.get('wait_seq'))
        if index % 2 == 0:
            if current <= sequence:
                raise ValueError('a later wait reused the original registration')
            sequence = current
        elif current != sequence:
            raise ValueError('later wait endpoints disagree')
    return end


def validate_deadlock(case):
    """Join original native waits, the confirmed LMD cycle and the SQL error."""
    forward, reverse, log = case['forward'], case['reverse'], case['log']
    if (case['cancel_sent'] is not False or forward['waiter_node'] != 0
            or reverse['waiter_node'] != 0 or forward['tag'] == reverse['tag']
            or not log.startswith(reverse['log'])
            or not reverse['log'].startswith(forward['log'])):
        raise ValueError('deadlock cell changed its original observation or sent a cancel')
    selected = [select_blocker(forward)]
    for page in (forward, reverse):
        if len(page['holders']) != 8:
            raise ValueError('deadlock requires two originally full ITL pages')
    owners = forward['holders'] + reverse['holders']
    if (len({number(h['pid']) for h in owners}) != 16
            or len({(number(h['node']), number(h['xid'])) for h in owners}) != 16):
        raise ValueError('deadlock producer groups overlap')
    rows = [_deadlock_wait_rows(log, c['waiter_pid'], c['waiter_node'])
            for c in (forward, reverse)]
    if not rows[1] or rows[1][0][0]['event'] != 'WAIT_TARGET':
        raise ValueError('reverse exact wait was never observed')
    # Use a byte-preserving prefix of the actual log, not a synthesized wait
    # or a live-page reread after the victim has aborted.
    if len(reverse['log']) >= rows[1][0][1]:
        raise ValueError('reverse page was not observed before its wait')
    selected.append(select_blocker(dict(reverse, log=log[:rows[1][0][1]])))
    physical = ('origin_node_id', 'segment_id', 'segment_generation', 'slot_offset', 'slot_wrap')
    if len({tuple(s['key'][field] for field in physical) for s in selected}) != 2:
        raise ValueError('two live xids claim the same canonical slot incarnation')
    waiters = [selected[1], selected[0]]
    if any(c['waiter_pid'] != w['pid'] or c['waiter_node'] != w['node']
           for c, w in zip((forward, reverse), waiters)):
        raise ValueError('two exact waits do not close on their original owners')
    ends = [_deadlock_original_end(r, s) for r, s in zip(rows, selected)]
    expected = {(number(w['node']), number(r[0][0].get('proc')), number(w['xid']),
                 number(s['key']['formation_epoch']), 0, s['wait_seq'])
                for w, r, s in zip(waiters, rows, selected)}
    members = []
    offset = 0
    for line in log.splitlines(keepends=True):
        offset += len(line)
        if 'PGRAC_FAMILY=DEADLOCK PGRAC_REASON=CONFIRMED_CYCLE ' not in line:
            continue
        pairs = re.findall(r'([A-Za-z_0-9]+)=([^ \n]+)', line.split('PGRAC_FAMILY=', 1)[1])
        row = dict(pairs)
        if len(row) != len(pairs):
            raise ValueError('duplicate confirmed cycle fields')
        members.append((row, offset))
    if len(members) != 2:
        raise ValueError('one complete two-member confirmed LMD cycle is required')
    cycle, cancel = (number(members[0][0].get(k)) for k in ('cycle', 'cancel'))
    actual = set()
    for member, offset in members:
        if (not cycle or not cancel or number(member.get('cycle')) != cycle
                or number(member.get('cancel')) != cancel or number(member.get('total')) != 2
                or offset <= max(r[0][1] for r in rows)
                or offset >= min(r[1][1] for r in rows)):
            raise ValueError('confirmed cycle identity or original wait overlap is missing')
        actual.add(tuple(number(member.get(k)) for k in
                         ('node', 'procno', 'xid', 'epoch', 'request', 'wait_seq')))
    if (actual != expected or {number(m[0].get('member')) for m in members} != {0, 1}):
        raise ValueError('confirmed cycle does not match the two actual waits')
    victim = number(case['victim_pid'])
    if victim not in {w['pid'] for w in waiters}:
        raise ValueError('deadlock victim is not an original participant')
    for waiter, end, raw in zip(waiters, ends, rows):
        if number(end.get('status')) != (5 if waiter['pid'] == victim else 0):
            raise ValueError('original wait did not return native DEADLOCK or RESOLVED')
        if waiter['pid'] == victim and len(raw) != 2:
            raise ValueError('victim registered another wait before rollback')
    errors = re.findall(r'^ERROR:\s+([0-9A-Z]{5}): (.*)$', case['stderr'], re.MULTILINE)
    if (case['client_had_error'] is not True or case['stdout'].strip() != 'B08_SQLSTATE 40P01'
            or errors != [('40P01', 'deadlock detected while waiting for heap update ITL '
                           'capacity (INITRANS=8)')]):
        raise ValueError('original ITL deadlock SQLSTATE and native cause are not proved')
    observed = case['holders']
    identity = lambda h: tuple(number(h[k]) for k in ('node', 'pid', 'row'))
    by_id = {identity(h): h for h in observed}
    if len(observed) != 16 or len(by_id) != 16 or set(by_id) != {identity(h) for h in owners}:
        raise ValueError('an original owner disappeared or was replaced')
    for owner in owners:
        current = by_id[identity(owner)]
        if owner['pid'] == victim:
            if (current['state'] != 'idle in transaction (aborted)' or current['xid'] is not None
                    or current.get('wait_event') == 'GesTxEnqueueWait'):
                raise ValueError('original victim backend did not leave its wait')
        elif (number(current['xid']) != owner['xid']
              or current['state'] not in (('active', 'idle in transaction')
                                         if owner['pid'] in {w['pid'] for w in waiters}
                                         else ('idle in transaction',))):
            raise ValueError('a surviving original owner was released or changed')
    return dict(sqlstate='40P01', victim_pid=victim, cycle=cycle, cancel=cancel,
                selected=selected, authoritative=False)


if __name__ == '__main__':
    try:
        case = json.load(sys.stdin)
        if sys.argv[1:] == ['--exit']:
            print(json.dumps(dict(exit=validate_wait_exit(case))))
        elif sys.argv[1:] == ['--page-owner']:
            print(json.dumps(dict(selected=select_page_owner(case))))
        elif sys.argv[1:] == ['--deadlock']:
            print(json.dumps(dict(deadlock=validate_deadlock(case))))
        elif not sys.argv[1:]:
            print(json.dumps(dict(selected=select_blocker(case))))
        else:
            raise ValueError('unexpected evaluator arguments')
    except (ValueError, KeyError, TypeError) as exc:
        print(json.dumps(dict(error=str(exc))))
        raise SystemExit(1)
