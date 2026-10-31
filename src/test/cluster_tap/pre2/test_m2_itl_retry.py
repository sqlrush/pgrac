"""Join actual retry observations without fabricating receipt or pin authority."""
import copy
import unittest
import m2_itl_capacity as native
from test_m2_itl_cross_page import completed
from test_m2_itl_requalify import end_for


def trace(events):
    n = len(events)
    return dict(version=1, sequence_domain='NATIVE_ITL', atomic_graph=False,
                available=True, enabled=True, capacity=4096, first_seq=1 if n else 0,
                last_seq=n, reserved_before=n, reserved_after=n, exported=n,
                dropped=0, overflow=False, export_complete=True, events=events)


def observed(*, extra_cycle=False, alias=False):
    finish = completed()
    start = finish['cross_page']['start']
    events = copy.deepcopy(start['trace']['events'][:-1])
    page = copy.deepcopy(start['trace']['events'][-1])
    actor = dict(node=0, pid=200, procno=40, backend_id=5,
                 backend_start='900', process_incarnation='6', authoritative=False)
    key = dict(events[0]['key'], segment_id=2, slot_offset=0, slot_wrap=0, xid=1400)
    active = dict(actor, kind=2, key_known=True, key=key,
                  bind_lsn='1111', tt_slot=dict(xid=1400, wrap=0, status=1))
    events.append(active)
    receipt = dict(observed=True, old_vm=3, new_vm=3 if alias else 4,
                   receipt_present=True, receipt_projection_only=True,
                   ready_known=False, shared_retirement_known=False, vm_reference_counts_known=False,
                   magic=0x55525250, record_type=2, owner_instance=1, payload_capacity=1024,
                   segment_id=2, slot_offset=0, actual_segment_id=3,
                   reservation_sequence='9007199254740993', prepare_deadline_us='100',
                   attempt_generation=1, pending_mask=3, prepared_mask=1, applied_mask=0, reuse_mask=0)
    binding = dict(segment_id=2, segment_generation=0, xid=1400, slot_offset=0, slot_wrap=0,
                   origin_instance=1, publish_state=3, reserved16=0, active_lsn='1111')
    def cut(phase, deadline):
        c = dict(receipt, phase=phase, wait_deadline_us=deadline)
        if phase != 1:
            c.update(old_vm=0, new_vm=0)
        return dict(actor, kind=7, key_known=False, native_binding_known=True,
                    native_binding=copy.deepcopy(binding), bind_lsn='1111', update_retry=c)
    if extra_cycle:
        events += [cut(1, '0'), cut(2, '0'), cut(3, '500')]
    events += [cut(1, '500' if extra_cycle else '0'),
               cut(2, '500' if extra_cycle else '0'), page]
    for index, event in enumerate(events, 1):
        event['sequence'] = index
    start['trace'] = trace(copy.deepcopy(events))
    start['page_sequence'] = len(events)
    finish['waits'] = [copy.deepcopy(start)]
    events.append(cut(3, '500'))
    events[-1]['sequence'] = len(events)
    return dict(finish=finish, trace=trace(events))


class RetryEvidence(unittest.TestCase):
    def setUp(self):
        self.check = getattr(native, 'validate_retry_observations', None)
        self.assertTrue(callable(self.check), 'native retry observation validator missing')

    def test_full_waiter_identity_and_projected_receipt_span(self):
        r = self.check(observed())
        self.assertEqual((r['kind'], r['waiter_pid'], r['waiter_xid']), ('pair_retry', 200, 1400))
        self.assertEqual((r['cycles'], r['completed_waits'], r['vm_handle_slots_observed']), (1, 1, 2))
        self.assertEqual(r['key']['segment_id'], 2)
        self.assertEqual(r['key']['root_id'], 32768)
        self.assertEqual(r['reservation_sequence'], '9007199254740993')
        self.assertEqual((r['prepare_deadline_us'], r['wait_deadline_us']), ('100', '500'))
        self.assertEqual(r['scope'], 'local_projection_only')
        for field in ('authoritative', 'ready_known', 'shared_retirement_known', 'vm_reference_counts_known'):
            self.assertIs(r[field], False)

    def test_aliased_handles_and_multiple_capacity_cycles_are_observed_without_pin_counts(self):
        r = self.check(observed(extra_cycle=True, alias=True))
        self.assertEqual(r['cycles'], 2)
        self.assertEqual(r['completed_waits'], 1)
        self.assertIs(r['aliased_handles_observed'], True)
        self.assertIs(r['vm_reference_counts_known'], False)

    def test_one_capacity_call_may_contain_multiple_exact_wait_registrations(self):
        c = observed()
        start = c['finish']['cross_page']['start']
        target = start['log'].replace('seq=17 ', 'seq=19 ').replace('wait_seq=9', 'wait_seq=10')
        end = end_for(start, 20).replace('wait_seq=9', 'wait_seq=10')
        c['finish']['log'] += target + end
        r = self.check(c)
        self.assertEqual((r['cycles'], r['completed_waits']), (1, 2))

    def test_fresh_page_intent_between_cycles_keeps_the_original_local_reservation(self):
        c = observed(extra_cycle=True)
        for snapshot in (c['trace'], c['finish']['cross_page']['start']['trace'], c['finish']['waits'][0]['trace']):
            for row in snapshot['events']:
                if row.get('kind') == 7 and row['sequence'] >= 13:
                    row['update_retry'].update(attempt_generation=2, prepared_mask=3, reuse_mask=1)
        result = self.check(c)
        self.assertEqual(result['cycles'], 2)
        self.assertEqual(result['reservation_sequence'], '9007199254740993')

    def test_missing_holey_overflowed_or_substituted_prefix_refuses(self):
        for change in (lambda t:t.update(export_complete=False), lambda t:t.update(overflow=True),
                       lambda t:t.update(dropped=1), lambda t:t.update(reserved_after=999),
                       lambda t:t['events'].pop(0), lambda t:t['events'][0].update(sequence=2),
                       lambda t:t['events'][0]['key'].update(root_id='999')):
            c = observed(); change(c['trace'])
            with self.subTest(change=change), self.assertRaises(ValueError): self.check(c)

    def test_missing_duplicate_reordered_or_still_pending_phase_refuses(self):
        for action in ('missing', 'duplicate', 'reordered', 'pending'):
            c = observed(); events = c['trace']['events']
            if action == 'missing': events.pop(-2)
            if action == 'duplicate': events.insert(-1, copy.deepcopy(events[-1]))
            if action == 'reordered': events[-1]['update_retry']['phase'] = 2
            if action == 'pending': events.pop()
            c['trace'] = trace(events)
            with self.subTest(action=action), self.assertRaises(ValueError): self.check(c)

    def test_actual_actor_identity_cannot_be_replaced_after_the_wait(self):
        for field, value in (('pid', 201), ('node', 1), ('procno', 41), ('backend_id', 6),
                             ('backend_start', '901'), ('process_incarnation', '7')):
            c = observed(); c['trace']['events'][-1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(c)

    def test_native_binding_cannot_join_by_xid_or_another_incarnation(self):
        for field in ('segment_id', 'segment_generation', 'xid', 'slot_offset', 'slot_wrap',
                      'origin_instance', 'publish_state', 'reserved16', 'active_lsn'):
            c = observed(); binding = c['trace']['events'][-1]['native_binding']
            binding[field] = str(int(binding[field]) + 1)
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(c)
        c = observed(); c['trace']['events'][-1]['native_binding_known'] = False
        with self.assertRaises(ValueError): self.check(c)

    def test_missing_or_ambiguous_original_active_publication_refuses(self):
        for action in ('missing', 'duplicate', 'unproved'):
            c = observed(); events = c['trace']['events']
            active = events[8]
            if action == 'missing': events.pop(8)
            if action == 'duplicate': events.append(copy.deepcopy(active))
            if action == 'unproved': active['key_known'] = False
            c['trace'] = trace(events)
            with self.subTest(action=action), self.assertRaises(ValueError): self.check(c)

    def test_no_vm_handle_or_a_retained_handle_cannot_cover_vm_release(self):
        c = observed()
        for snapshot in (c['trace'], c['finish']['cross_page']['start']['trace'], c['finish']['waits'][0]['trace']):
            for row in snapshot['events']:
                if row.get('kind') == 7 and row['update_retry']['phase'] == 1:
                    row['update_retry'].update(old_vm=0, new_vm=0)
        with self.assertRaises(ValueError): self.check(c)
        for phase in (2, 3):
            c = observed()
            for row in c['trace']['events']:
                if row.get('kind') == 7 and row['update_retry']['phase'] == phase:
                    row['update_retry']['new_vm'] = 3
            with self.assertRaises(ValueError): self.check(c)

    def test_receipt_identity_masks_or_original_deadlines_cannot_change_in_the_wait(self):
        for field in ('reservation_sequence', 'prepare_deadline_us', 'owner_instance', 'segment_id',
                      'slot_offset', 'actual_segment_id', 'payload_capacity', 'attempt_generation',
                      'pending_mask', 'prepared_mask', 'applied_mask', 'reuse_mask', 'magic', 'record_type'):
            c = observed(); cut = c['trace']['events'][-1]['update_retry']
            cut[field] = str(int(cut[field]) + 1)
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(c)
        c = observed(extra_cycle=True); c['trace']['events'][-1]['update_retry']['wait_deadline_us'] = '501'
        with self.assertRaises(ValueError): self.check(c)

    def test_missing_projection_or_claimed_ready_retirement_pin_authority_refuses(self):
        for field, value in (('observed', False), ('receipt_present', False), ('receipt_projection_only', False),
                             ('ready_known', True), ('shared_retirement_known', True),
                             ('vm_reference_counts_known', True)):
            c = observed(); c['trace']['events'][-1]['update_retry'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(c)
        c = observed(); c['trace']['events'][-1]['authoritative'] = True
        with self.assertRaises(ValueError): self.check(c)

    def test_original_pair_finish_and_exact_wait_end_remain_required(self):
        for change in (lambda f:f.update(stdout='B06_PAIR_SQLSTATE 57014'),
                       lambda f:f['rows'][0].update(ctid='(1,1)'),
                       lambda f:f.update(log=f['cross_page']['log']),
                       lambda f:f['remaining'].pop()):
            c = observed(); change(c['finish'])
            with self.subTest(change=change), self.assertRaises(ValueError): self.check(c)


if __name__ == '__main__':
    unittest.main()
