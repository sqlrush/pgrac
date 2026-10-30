"""A native ITL deadlock needs two exact waits and the actual LMD cycle."""
import copy
import unittest

import m2_itl_capacity as evidence
from test_m2_itl_capacity import fixture


def deadlock_fixture(victim=200):
    forward = fixture()
    forward['log'] = forward['log'].replace('pid=200 ', 'pid=200 proc=20 ')
    reverse = fixture()
    reverse['tag'][2] = 20001
    reverse['page_sequence'] = 91
    for holder in reverse['holders']:
        holder['pid'] += 100
        holder['xid'] += 1000
    for producer in reverse['trace']['events'][:-1]:
        producer['pid'] += 100
        producer['key']['xid'] += 1000
        producer['key']['segment_id'] = 2
        producer['tt_slot']['xid'] += 1000
    reverse['trace']['events'][-1].update(sequence=91, tag=reverse['tag'][:])
    for slot in reverse['trace']['events'][-1]['slots']:
        slot['xid'] += 1000
        raw = bytearray.fromhex(slot['uba_bytes'])
        raw[:4] = (2).to_bytes(4, 'little')
        slot['uba_bytes'] = raw.hex()
    reverse.update(waiter_pid=100, log=forward['log'])
    other = forward['log'].replace('pid=200 proc=20', 'pid=100 proc=10')
    other = other.replace('xid=1200', 'xid=2200').replace('seq=17 ', 'seq=27 ')
    other = other.replace('wait_seq=9', 'wait_seq=19')
    other = other.replace('uba0=0000000100000001', 'uba0=0000000100000002')
    members = ''.join(
        'DETAIL: PGRAC_FAMILY=DEADLOCK PGRAC_REASON=CONFIRMED_CYCLE '
        f'cycle=71 cancel=81 member={i} total=2 node=0 procno={proc} '
        f'xid={xid} epoch=0 request=0 wait_seq={seq}\n'
        for i, proc, xid, seq in ((0, 20, 2200, 9), (1, 10, 1200, 19)))
    ends = []
    for pid, start in ((200, forward['log']), (100, other)):
        ends.append(start.replace('event=WAIT_TARGET', 'event=WAIT_END')
                    .replace('seq=17 ', 'seq=18 ').replace('seq=27 ', 'seq=28 ')
                    .rstrip() + (' status=5\n' if pid == victim else ' status=0\n'))
    holders = [dict(h, state='idle in transaction', wait_event='ClientRead')
               for h in forward['holders'] + reverse['holders']]
    for holder in holders:
        if holder['pid'] == victim:
            holder.update(state='idle in transaction (aborted)', xid=None)
    return dict(forward=forward, reverse=reverse,
                log=forward['log'] + other + members + ''.join(ends),
                victim_pid=victim, cancel_sent=False, holders=holders,
                stdout='B08_SQLSTATE 40P01', client_had_error=True,
                stderr='ERROR:  40P01: deadlock detected while waiting for heap '
                       'update ITL capacity (INITRANS=8)\n')


class DeadlockEvidence(unittest.TestCase):
    def validate(self, case):
        validator = getattr(evidence, 'validate_deadlock', None)
        self.assertTrue(callable(validator), 'exact native deadlock evaluator is missing')
        return validator(case)

    def test_either_actual_participant_can_be_the_victim(self):
        for pid in (100, 200):
            case = deadlock_fixture(pid)
            before = copy.deepcopy(case)
            result = self.validate(case)
            self.assertEqual(result['victim_pid'], pid)
            self.assertEqual(result['sqlstate'], '40P01')
            self.assertEqual(result['cycle'], 71)
            self.assertFalse(result['authoritative'])
            self.assertEqual(case, before)

    def test_page_selection_requires_the_existing_full_canonical_join(self):
        selector = getattr(evidence, 'select_page_owner', None)
        self.assertTrue(callable(selector), 'observed page owner selector is missing')
        case = deadlock_fixture()['reverse']
        case.pop('waiter_pid')
        case.pop('waiter_node')
        selected = selector(case)
        self.assertEqual((selected['pid'], selected['xid']), (200, 2200))
        self.assertEqual(selected['key']['root_id'], 32768)
        self.assertNotIn('wait_seq', selected)
        case['trace']['events'][0]['key'].pop('root_generation')
        with self.assertRaises(ValueError): selector(case)

    def test_a_one_way_wait_or_guessed_client_is_not_a_cycle(self):
        for mutate in (
                lambda c: c['reverse'].update(waiter_pid=101),
                lambda c: c['forward'].update(waiter_pid=201),
                lambda c: c['reverse']['trace']['events'][0].update(key_known=False),
                lambda c: c['reverse']['trace']['events'][-1]['slots'][0].update(flags=4),
                lambda c: c['reverse'].update(tag=c['forward']['tag']),
                lambda c: c['reverse']['holders'][0].update(xid=999)):
            case = deadlock_fixture(); mutate(case)
            with self.subTest(mutate=str(mutate)):
                with self.assertRaises(ValueError): self.validate(case)

    def test_each_cycle_member_must_match_node_proc_xid_epoch_and_wait_sequence(self):
        for old, new in (('procno=20', 'procno=21'), ('xid=2200 epoch', 'xid=2201 epoch'),
                         ('node=0 procno=20', 'node=1 procno=20'),
                         ('epoch=0 request', 'epoch=1 request'), ('request=0', 'request=1'),
                         ('wait_seq=19\n', 'wait_seq=20\n'), ('total=2', 'total=3'),
                         ('cancel=81 member=1', 'cancel=82 member=1')):
            case = deadlock_fixture(); case['log'] = case['log'].replace(old, new)
            with self.subTest(new=new):
                with self.assertRaises(ValueError): self.validate(case)

    def test_missing_duplicate_or_unconfirmed_cycle_is_not_accepted(self):
        case = deadlock_fixture()
        rows = case['log'].splitlines(keepends=True)
        members = [r for r in rows if 'CONFIRMED_CYCLE' in r]
        for log in (case['log'].replace(members[0], ''), case['log'] + members[0],
                    case['log'].replace('CONFIRMED_CYCLE', 'CANDIDATE_CYCLE'),
                    case['log'].replace('cycle=71', 'cycle=0')):
            case = deadlock_fixture(); case['log'] = log
            with self.assertRaises(ValueError): self.validate(case)

    def test_both_original_waits_must_end_once_with_matching_identity(self):
        case = deadlock_fixture()
        rows = case['log'].splitlines(keepends=True)
        ends = [r for r in rows if 'event=WAIT_END' in r]
        for end in ends:
            for replacement in ('', end * 2, end.replace('itl_wrap=50', 'itl_wrap=10'),
                                end.replace('uba1=000000000040000a', 'uba1=000000000040000b'),
                                end.replace('wait_seq=', 'wait_seq=8'),
                                end.replace('seq=18 ', 'seq=16 ').replace('seq=28 ', 'seq=26 ')):
                changed = deadlock_fixture(); changed['log'] = changed['log'].replace(end, replacement)
                with self.subTest(end=end, replacement=replacement):
                    with self.assertRaises(ValueError): self.validate(changed)

    def test_native_deadlock_result_cannot_be_replaced_by_timeout_cancel_or_another_error(self):
        for field, value in (
                ('stdout', 'B08_SQLSTATE 57014'), ('stderr', 'ERROR:  40P01: deadlock detected\n'),
                ('stderr', 'ERROR:  57014: canceling statement due to statement timeout\n'),
                ('client_had_error', False), ('cancel_sent', True), ('victim_pid', 999)):
            case = deadlock_fixture(); case[field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError): self.validate(case)
        for old, new in (('status=5', 'status=1'), ('status=0', 'status=4')):
            case = deadlock_fixture(); case['log'] = case['log'].replace(old, new)
            with self.assertRaises(ValueError): self.validate(case)

    def test_the_other_fourteen_owners_remain_open_and_identical(self):
        for mutate in (lambda hs: hs.pop(), lambda hs: hs.append(copy.deepcopy(hs[1])),
                       lambda hs: hs[1].update(state='idle'), lambda hs: hs[1].update(xid=999),
                       lambda hs: hs[1].update(node=1), lambda hs: hs[1].update(row=9),
                       lambda hs: hs[0].update(state='idle in transaction (aborted)'),
                       lambda hs: hs[8].update(state='active', xid=2200)):
            case = deadlock_fixture(); mutate(case['holders'])
            with self.assertRaises(ValueError): self.validate(case)

    def test_truncated_limited_or_duplicate_raw_wait_trace_is_refused(self):
        for mutate in (
                lambda c: c.update(log=c['log'][1:]),
                lambda c: c.update(log=c['log'] + 'TX_IDENTITY event=LIMIT actor_node=0 pid=100\n'),
                lambda c: c.update(log=c['log'] + c['forward']['log']),
                lambda c: c['reverse'].update(log='not a raw prefix'),
                lambda c: c.update(log=c['log'].replace('pid=100 proc=10', 'pid=100 proc=10 proc=10'))):
            case = deadlock_fixture(); mutate(case)
            with self.assertRaises(ValueError): self.validate(case)

    def test_a_survivor_can_register_a_new_wait_only_after_the_original_end(self):
        case = deadlock_fixture()
        case['holders'][0].update(state='active', wait_event='GesTxEnqueueWait')
        later = (case['log'].splitlines(keepends=True)[1]
                 .replace('seq=27 ', 'seq=29 ').replace('wait_seq=19', 'wait_seq=20'))
        case['log'] += later
        self.assertEqual(self.validate(case)['victim_pid'], 200)
        case['log'] += later
        with self.assertRaises(ValueError): self.validate(case)

    def test_two_live_xids_cannot_claim_the_same_canonical_slot_incarnation(self):
        case = deadlock_fixture()
        case['reverse']['trace']['events'][0]['key']['segment_id'] = 1
        slot = case['reverse']['trace']['events'][-1]['slots'][0]
        slot['uba_bytes'] = case['forward']['trace']['events'][-1]['slots'][0]['uba_bytes']
        case['log'] = case['log'].replace('uba0=0000000100000002', 'uba0=0000000100000001')
        with self.assertRaises(ValueError): self.validate(case)


if __name__ == '__main__':
    unittest.main()
