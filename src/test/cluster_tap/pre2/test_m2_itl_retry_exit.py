"""Keep failed UPDATE observations bound to the original waiting transaction."""
import copy
import unittest

import m2_itl_capacity as native
from test_m2_itl_capacity_timeout import capacity_timeout_fixture
from test_m2_itl_retry import observed, trace
from test_m2_itl_wait_exit import exit_fixture


def retry_exit(kind='cancel', *, extra_cycle=False):
    exited = capacity_timeout_fixture() if kind == 'capacity_timeout' else exit_fixture(kind)
    events = observed(extra_cycle=extra_cycle)['trace']['events'][:-1]
    page = copy.deepcopy(exited['start']['trace']['events'][-1])
    page['sequence'] = len(events)
    events[-1] = page
    for row in events:
        if row.get('kind') == 7:
            row['update_retry'].update(old_vm=0, new_vm=0)
        if kind == 'capacity_timeout':
            if row.get('pid') == 200 and row.get('kind') == 2:
                row['key']['xid'] = row['tt_slot']['xid'] = 2200
            if row.get('kind') == 7:
                row['native_binding']['xid'] = 2200
                if row['update_retry']['wait_deadline_us'] != '0':
                    row['update_retry']['wait_deadline_us'] = '5000000'
    exited['start'].update(trace=trace(copy.deepcopy(events)), page_sequence=len(events))
    return dict(exit=exited, trace=trace(events))


class RetryExitEvidence(unittest.TestCase):
    def setUp(self):
        self.check = getattr(native, 'validate_retry_exit_observations', None)
        self.assertTrue(callable(self.check), 'failed UPDATE retry observation validator missing')

    def test_each_original_exit_retains_its_cause_and_exact_wait_registration(self):
        for kind, code, xid in (('cancel', '57014', 1400),
                                ('statement_timeout', '57014', 1400),
                                ('capacity_timeout', '53R70', 2200)):
            case = retry_exit(kind)
            before = copy.deepcopy(case)
            result = self.check(case)
            self.assertEqual((result['kind'], result['exit_kind'], result['sqlstate']),
                             ('wait_retry_exit', kind, code))
            self.assertEqual((result['waiter_pid'], result['waiter_key']['xid'],
                              result['wait_seq'], result['end_seq']), (200, xid, 9, 18))
            self.assertEqual(result['reservation_sequence'], '9007199254740993')
            self.assertEqual((result['completed_cycles'], result['last_phase'],
                              result['vm_handle_slots_observed']), (0, 2, 0))
            self.assertEqual(result['scope'], 'local_projection_only')
            for field in ('authoritative', 'ready_known', 'shared_retirement_known',
                          'vm_reference_counts_known', 'abort_cleanup_known', 'wait_deadline_known'):
                self.assertIs(result[field], False)
            self.assertEqual(result['wait_deadline_us'], '0')
            self.assertEqual(case, before)

    def test_prior_completed_cycles_keep_the_established_deadline(self):
        result = self.check(retry_exit('capacity_timeout', extra_cycle=True))
        self.assertEqual(result['completed_cycles'], 1)
        self.assertEqual(result['last_phase'], 2)
        self.assertEqual(result['wait_deadline_us'], '5000000')
        self.assertIs(result['wait_deadline_known'], True)

    def test_error_cause_owner_set_and_exact_wait_end_are_still_required(self):
        for change in (lambda c: c['exit'].update(stdout='B08_SQLSTATE 00000'),
                       lambda c: c['exit'].update(log=c['exit']['start']['log']),
                       lambda c: c['exit']['holders'].pop(),
                       lambda c: c['exit']['reader'].update(pid=201)):
            case = retry_exit(); change(case)
            with self.subTest(change=change), self.assertRaises(ValueError): self.check(case)

    def test_missing_incomplete_or_replaced_wait_prefix_refuses(self):
        for change in (lambda c: c['trace'].update(export_complete=False),
                       lambda c: c['trace'].update(overflow=True),
                       lambda c: c['trace'].update(dropped=1),
                       lambda c: c['trace']['events'][0]['key'].update(root_id='999'),
                       lambda c: c['exit']['start']['trace'].update(reserved_before=999)):
            case = retry_exit(); change(case)
            with self.subTest(change=change), self.assertRaises(ValueError): self.check(case)

    def test_waiter_identity_or_original_native_binding_cannot_be_replaced(self):
        for field in ('pid', 'node', 'procno', 'backend_id', 'backend_start', 'process_incarnation'):
            case = retry_exit(); row = case['trace']['events'][-2]
            row[field] = int(row[field]) + 1
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(case)
        for field in ('segment_id', 'segment_generation', 'xid', 'slot_offset', 'slot_wrap',
                      'origin_instance', 'publish_state', 'reserved16', 'active_lsn'):
            case = retry_exit(); binding = case['trace']['events'][-2]['native_binding']
            binding[field] = str(int(binding[field]) + 1)
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(case)

    def test_active_publication_must_be_unique_and_precede_the_wait(self):
        for action in ('missing', 'duplicate', 'unproved', 'after'):
            case = retry_exit(); events = case['trace']['events']; active = events[8]
            if action == 'missing': events.pop(8)
            if action == 'duplicate': events.append(copy.deepcopy(active))
            if action == 'unproved': active['key_known'] = False
            if action == 'after': events.append(events.pop(8))
            for i, event in enumerate(events, 1): event['sequence'] = i
            case['trace'] = trace(events)
            with self.subTest(action=action), self.assertRaises(ValueError): self.check(case)

    def test_original_wait_cannot_be_replaced_by_a_later_capacity_span(self):
        case = retry_exit()
        cuts = [copy.deepcopy(e) for e in case['trace']['events'] if e.get('kind') == 7]
        completed = copy.deepcopy(cuts[-1]); completed['update_retry']['phase'] = 3
        completed['update_retry']['wait_deadline_us'] = '500'
        events = case['trace']['events'] + [completed, *cuts]
        for i, event in enumerate(events, 1): event['sequence'] = i
        case['trace'] = trace(events)
        with self.assertRaises(ValueError): self.check(case)

    def test_no_completed_requalification_or_new_transaction_may_follow_the_failed_wait(self):
        for kind in ('cancel', 'statement_timeout', 'capacity_timeout'):
            for action in ('completed', 'new-active'):
                case = retry_exit(kind); events = case['trace']['events']
                row = copy.deepcopy(events[-2] if action == 'completed' else events[8])
                if action == 'completed':
                    row['update_retry'].update(phase=3, wait_deadline_us='500')
                else:
                    row['key']['xid'] += 1; row['tt_slot']['xid'] += 1
                row['sequence'] = len(events) + 1; events.append(row)
                case['trace'] = trace(events)
                with self.subTest(kind=kind, action=action), self.assertRaises(ValueError): self.check(case)

    def test_reservation_and_handle_ownership_cannot_change_between_two_open_points(self):
        for field in ('reservation_sequence', 'prepare_deadline_us', 'owner_instance', 'segment_id',
                      'slot_offset', 'actual_segment_id', 'payload_capacity', 'attempt_generation',
                      'pending_mask', 'prepared_mask', 'applied_mask', 'reuse_mask', 'magic', 'record_type'):
            case = retry_exit(); cut = case['trace']['events'][-2]['update_retry']
            cut[field] = str(int(cut[field]) + 1)
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(case)
        case = retry_exit(); case['trace']['events'][-2]['update_retry']['new_vm'] = 3
        with self.assertRaises(ValueError): self.check(case)

    def test_capacity_error_must_name_the_same_waiter_and_established_deadline(self):
        for field in ('waiter', 'deadline'):
            case = retry_exit('capacity_timeout', extra_cycle=True)
            for row in case['trace']['events']:
                if field == 'waiter':
                    if row.get('kind') == 2 and row.get('pid') == 200:
                        row['key']['xid'] = row['tt_slot']['xid'] = 2201
                    if row.get('kind') == 7: row['native_binding']['xid'] = 2201
                elif row.get('kind') == 7 and row['update_retry']['wait_deadline_us'] != '0':
                    row['update_retry']['wait_deadline_us'] = '5000001'
            case['exit']['start']['trace'] = copy.deepcopy(case['trace'])
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(case)

    def test_projection_does_not_establish_ready_or_abort_cleanup(self):
        for field in ('ready_known', 'shared_retirement_known', 'vm_reference_counts_known'):
            case = retry_exit(); case['trace']['events'][-2]['update_retry'][field] = True
            with self.subTest(field=field), self.assertRaises(ValueError): self.check(case)


if __name__ == '__main__':
    unittest.main()
