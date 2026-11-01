"""Exercise the native fixture's blocker evidence, never fabricate authority."""
import copy
import unittest
from m2_itl_capacity import select_blocker


def fixture():
    holders, events, slots = [], [], []
    for i in range(8):
        holders.append(dict(pid=100+i, xid=1200+i, node=0, row=i+1))
        key = dict(format_version=1, owner_instance=1, origin_node_id=0,
                   segment_id=1, segment_generation=0, slot_offset=10+i,
                   slot_wrap=50+i, xid=1200+i, cluster_epoch=0,
                   system_identifier='12345', origin_boot_incarnation='20',
                   formation_epoch='0', admission_record_generation='4',
                   root_descriptor_incarnation='5', root_id='32768', root_generation='1')
        events.append(dict(kind=2, node=0, pid=100+i, key_known=True, key=key,
                           tt_slot=dict(xid=1200+i, wrap=50+i, status=1),
                           authoritative=False))
        raw0, raw1 = ((i+1) << 32) | 1, (64 << 16) | (10+i)
        slots.append(dict(index=i, xid=1200+i, itl_wrap=10+i, flags=1,
                          uba_bytes=(raw0.to_bytes(8, 'little')+raw1.to_bytes(8, 'little')).hex()))
    events.append(dict(sequence=90,kind=4,page_known=True,tag=[1663,5,20000,0,0],
                       slots=slots,authoritative=False))
    log = ('TX_IDENTITY event=WAIT_TARGET seq=17 actor_node=0 pid=200 node=0 xid=1200 '
           'epoch=0 target_known=1 uba_known=1 uba0=0000000100000001 '
           'uba1=000000000040000a itl_wrap=50 itl_kind=1 itl_slot=0 wait_seq=9\n')
    return dict(holders=holders,trace=dict(export_complete=True,overflow=False,
                available=True,enabled=True,events=events),page_sequence=90,
                tag=[1663,5,20000,0,0],log=log,waiter_pid=200,waiter_node=0)


def own_lock_fixture():
    case = fixture()
    case['holders'] = case['holders'][1:]
    slot = case['trace']['events'][-1]['slots'][0]
    slot.update(flags=5, xid=1400, itl_wrap=2,
                uba_bytes='01000000090000001200400000000000')
    producer = copy.deepcopy(case['trace']['events'][0])
    producer.update(pid=200, procno=20, backend_id=3, backend_start='1000',
                    process_incarnation='41', sequence=9, bind_lsn='1111')
    producer['key'].update(xid=1400, slot_offset=18, slot_wrap=58)
    producer['tt_slot'].update(xid=1400, wrap=58)
    case['trace']['events'].insert(8, producer)
    case['log'] += case['log'].replace('event=WAIT_TARGET', 'event=WAIT_END').replace(
        'seq=17 ', 'seq=18 ').rstrip() + ' status=0\n'
    case['log'] += ('TX_IDENTITY event=ORIGIN_MAP seq=19 actor_node=0 pid=200 proc=20 '
        'node=0 xid=1400 epoch=0 tt_known=1 tt_segment=1 tt_slot=18 tt_wrap=58 '
        'generation_known=1 tt_generation=0 target_known=1 uba_known=1 '
        'uba0=0000000900000001 uba1=0000000000400012 itl_wrap=65535 '
        'itl_kind=5 itl_slot=0 wait_seq=0 status=1\n'
        'TX_IDENTITY event=WAIT_TARGET seq=20 actor_node=0 pid=200 node=0 xid=1201 '
        'epoch=0 target_known=1 uba_known=1 uba0=0000000200000001 '
        'uba1=000000000040000b itl_wrap=51 itl_kind=1 itl_slot=1 wait_seq=10\n')
    return case


class CapacityEvidence(unittest.TestCase):
    def observed(self,c):
        try:
            return select_blocker(c)
        except ValueError as exc:
            self.fail('valid exact blocker rejected: '+str(exc))

    def test_exact_blocker_uses_lowest_actual_slot_not_client_creation_order(self):
        c=fixture()
        c['holders']=c['holders'][3:]+c['holders'][:3]
        selected=self.observed(c)
        self.assertEqual(selected['pid'],100)
        self.assertEqual(selected['row'],1)
        self.assertEqual(selected['slot'],0)
        self.assertEqual(selected['wait_seq'],9)
        self.assertEqual(selected['key']['root_id'],32768)
        self.assertFalse(selected['authoritative'])

    def test_owner_instance_uses_the_native_one_based_namespace(self):
        c=fixture()
        self.assertEqual(self.observed(c)['key']['owner_instance'],1)
        c['trace']['events'][0]['key']['owner_instance']=0
        with self.assertRaises(ValueError): select_blocker(c)

    def test_canonical_wrap_is_not_page_itl_wrap_and_zero_generation_is_valid(self):
        selected=self.observed(fixture())
        self.assertEqual(selected['key']['slot_wrap'],50)
        self.assertEqual(selected['page_itl_wrap'],10)
        self.assertEqual(selected['key']['segment_generation'],0)
        self.assertEqual(selected['key']['formation_epoch'],0)

    def test_initial_wrap_zero_and_last_valid_wrap_keep_the_exact_key(self):
        for wrap,valid in ((0,True),(65534,True),(65535,False),(65536,False)):
            with self.subTest(wrap=wrap):
                c=fixture()
                c['trace']['events'][0]['key']['slot_wrap']=wrap
                c['trace']['events'][0]['tt_slot']['wrap']=wrap
                c['log']=c['log'].replace('itl_wrap=50','itl_wrap='+str(wrap))
                if valid:
                    self.assertEqual(self.observed(c)['key']['slot_wrap'],wrap)
                else:
                    with self.assertRaises(ValueError): select_blocker(c)

    def test_missing_or_changed_producer_coordinates_never_choose_a_client(self):
        for field in ('segment_id','slot_offset','slot_wrap','xid','origin_node_id',
                      'formation_epoch','system_identifier','root_id','root_generation'):
            for missing in (True,False):
                with self.subTest(field=field,missing=missing):
                    c=fixture(); key=c['trace']['events'][0]['key']
                    if missing: del key[field]
                    else: key[field]=0 if field in ('system_identifier','root_id','root_generation') else 99
                    with self.assertRaises(ValueError): select_blocker(c)

    def test_actual_wait_key_pid_node_uba_and_lowest_slot_must_match(self):
        for before,after in (('pid=200','pid=201'),('actor_node=0','actor_node=1'),
                ('node=0 xid','node=1 xid'),('xid=1200','xid=1201'),
                ('itl_slot=0','itl_slot=1'),('itl_wrap=50','itl_wrap=10'),
                ('uba1=000000000040000a','uba1=000000000040000b'),
                ('target_known=1','target_known=0'),('uba_known=1','uba_known=0')):
            with self.subTest(after=after):
                c=fixture(); c['log']=c['log'].replace(before,after)
                with self.assertRaises(ValueError): select_blocker(c)

    def test_ended_missing_duplicate_and_limited_waits_are_not_current(self):
        for suffix in ('TX_IDENTITY event=WAIT_END seq=18 actor_node=0 pid=200\n',
                       'TX_IDENTITY event=LIMIT actor_node=0 pid=200\n'):
            c=fixture();c['log']+=suffix
            with self.assertRaises(ValueError):select_blocker(c)
        for change in ('empty','duplicate'):
            c=fixture();c['log']='' if change=='empty' else c['log']*2
            with self.assertRaises(ValueError):select_blocker(c)

    def test_page_or_export_gaps_and_duplicate_producers_fail_closed(self):
        mutations=[lambda c:c['trace'].update(overflow=True),
                   lambda c:c['trace'].update(export_complete=False),
                   lambda c:c['trace']['events'][-1].update(page_known=False),
                   lambda c:c['trace']['events'][-1]['tag'].__setitem__(4,1),
                   lambda c:c['trace']['events'][-1]['slots'][0].update(xid=999),
                   lambda c:c['trace']['events'][-1]['slots'][0].update(uba_bytes='00'*16),
                   lambda c:c['trace']['events'].append(copy.deepcopy(c['trace']['events'][0])),
                   lambda c:c['holders'].append(copy.deepcopy(c['holders'][0]))]
        for mutate in mutations:
            with self.subTest(mutate=str(mutate)):
                c=fixture();mutate(c)
                with self.assertRaises(ValueError):select_blocker(c)

    def test_completed_history_is_not_selected_after_the_actual_owner_finishes(self):
        c=fixture();c['holders']=c['holders'][1:]
        c['trace']['events'][-1]['slots'][0]['flags']=2
        c['log']+='TX_IDENTITY event=WAIT_END seq=18 actor_node=0 pid=200\n'
        c['log']+=('TX_IDENTITY event=WAIT_TARGET seq=19 actor_node=0 pid=200 node=0 xid=1201 '
                   'epoch=0 target_known=1 uba_known=1 uba0=0000000200000001 '
                   'uba1=000000000040000b itl_wrap=51 itl_kind=1 itl_slot=1 wait_seq=10\n')
        selected=self.observed(c)
        self.assertEqual(selected['pid'],101)
        self.assertEqual(selected['slot'],1)
        self.assertEqual(selected['wait_seq'],10)
        self.assertEqual(c['trace']['events'][-1]['slots'][0]['flags'],2)

    def test_unknown_or_self_slot_does_not_get_a_guessed_blocker(self):
        for mutate in (lambda c:c['trace']['events'][-1]['slots'][0].update(flags=4),
                       lambda c:c['holders'][0].update(pid=200),
                       lambda c:c['trace']['events'][0].update(key_known=False),
                       lambda c:c['trace']['events'][0].update(authoritative=True)):
            c=fixture();mutate(c)
            with self.assertRaises(ValueError): select_blocker(c)

    def test_exact_own_lock_slot_leaves_the_lowest_foreign_data_owner_selected(self):
        case = own_lock_fixture(); before = copy.deepcopy(case)
        selected = self.observed(case)
        self.assertEqual((selected['pid'], selected['slot'], selected['wait_seq']), (101, 1, 10))
        self.assertEqual(selected['key']['xid'], 1201)
        self.assertFalse(selected['authoritative'])
        self.assertEqual(case, before)

    def test_lock_slot_requires_the_original_waiter_full_publication(self):
        for field, value in (('pid', 201), ('node', 1), ('procno', 21),
                             ('backend_start', 0), ('process_incarnation', 0),
                             ('sequence', 91), ('bind_lsn', 0), ('key_known', False)):
            case = own_lock_fixture(); case['trace']['events'][8][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): select_blocker(case)
        for field in ('segment_id', 'segment_generation', 'slot_offset', 'slot_wrap',
                      'xid', 'formation_epoch', 'root_id', 'root_generation'):
            case = own_lock_fixture()
            key = case['trace']['events'][8]['key']; key[field] = int(key[field]) + 1
            with self.subTest(field=field), self.assertRaises(ValueError): select_blocker(case)

    def test_missing_duplicate_or_changed_self_mapping_fails_closed(self):
        for old, new in (('proc=20', 'proc=21'), ('node=0 xid=1400', 'node=1 xid=1400'),
                         ('tt_known=1', 'tt_known=0'), ('tt_generation=0', 'tt_generation=1'),
                         ('tt_wrap=58', 'tt_wrap=59'), ('uba_known=1', 'uba_known=0'),
                         ('uba1=0000000000400012', 'uba1=0000000000400013'),
                         ('itl_kind=5', 'itl_kind=1'), ('status=1', 'status=0'),
                         ('status=1', 'status=2'), ('status=1', 'status=3'),
                         ('status=1', 'status=4')):
            case = own_lock_fixture(); case['log'] = case['log'].replace(old, new)
            with self.subTest(new=new), self.assertRaises(ValueError): select_blocker(case)
        for count in (0, 2):
            case = own_lock_fixture(); lines = case['log'].splitlines(keepends=True)
            mapping = next(line for line in lines if 'event=ORIGIN_MAP' in line)
            case['log'] = case['log'].replace(mapping, mapping * count)
            with self.subTest(count=count), self.assertRaises(ValueError): select_blocker(case)

    def test_free_terminal_or_foreign_lock_slots_are_not_self_exemptions(self):
        for field, value in (('flags', 0), ('flags', 4), ('flags', 6), ('flags', 7),
                             ('flags', 8), ('xid', 1201), ('uba_bytes', '00' * 16)):
            case = own_lock_fixture(); case['trace']['events'][-1]['slots'][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): select_blocker(case)

    def test_own_lock_does_not_make_an_ended_or_self_targeted_wait_current(self):
        for mode in ('ended', 'self'):
            case = own_lock_fixture()
            if mode == 'ended':
                case['log'] += case['log'].splitlines()[-1].replace(
                    'event=WAIT_TARGET', 'event=WAIT_END').replace('seq=20 ', 'seq=21 ') + ' status=4\n'
            else:
                case['log'] = case['log'].replace('xid=1201', 'xid=1400')
            with self.subTest(mode=mode), self.assertRaises(ValueError): select_blocker(case)


if __name__ == '__main__':
    unittest.main()
