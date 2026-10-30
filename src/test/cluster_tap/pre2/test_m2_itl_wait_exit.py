"""Reject incomplete cancellation evidence before the native fixture continues."""
import copy
import unittest

import m2_itl_capacity
from test_m2_itl_capacity import fixture


def exit_fixture(kind='cancel'):
    start = fixture()
    end = start['log'].replace('event=WAIT_TARGET seq=17', 'event=WAIT_END seq=18')
    message = ('canceling statement due to user request' if kind == 'cancel'
               else 'canceling statement due to statement timeout')
    return dict(start=start, kind=kind, cancel_sent=kind == 'cancel',
                stdout='B08_SQLSTATE 57014', stderr='ERROR:  57014: ' + message + '\n',
                client_had_error=True, log=start['log'] + end,
                reader=dict(pid=200, state='idle in transaction (aborted)', wait_event='ClientRead'),
                holders=[dict(h, state='idle in transaction', wait_event='ClientRead')
                         for h in start['holders']])


class WaitExitEvidence(unittest.TestCase):
    def observed(self, case):
        try:
            result = m2_itl_capacity.validate_wait_exit(case)
        except ValueError as exc:
            self.fail('complete native exit evidence rejected: ' + str(exc))
        self.assertEqual(result['sqlstate'], '57014')
        self.assertEqual(result['wait_seq'], 9)
        self.assertEqual(result['end_seq'], 18)
        self.assertEqual(result['selected']['pid'], 100)
        self.assertFalse(result['authoritative'])
        return result

    def test_user_cancel_requires_exact_wait_end_and_original_owners(self):
        case = exit_fixture()
        before = copy.deepcopy(case)
        self.assertEqual(self.observed(case)['kind'], 'cancel')
        self.assertEqual(case, before)

    def test_original_sql_timeout_has_no_cancel_signal(self):
        self.assertEqual(self.observed(exit_fixture('statement_timeout'))['kind'], 'statement_timeout')

    def test_other_errors_or_missing_native_cause_never_count_as_cancel(self):
        for stdout, stderr, had_error in (
                ('B08_SQLSTATE 00000', '', False),
                ('B08_SQLSTATE 40P01', 'ERROR:  40P01: deadlock detected', True),
                ('B08_SQLSTATE 57014', 'ERROR:  53R97: resolver refused', True),
                ('B08_SQLSTATE 57014', '', True),
                ('B08_SQLSTATE 57014', 'ERROR:  57014: canceling statement due to user request', False),
                ('B08_SQLSTATE 57014\nB08_SQLSTATE 57014',
                 'ERROR:  57014: canceling statement due to user request', True)):
            with self.subTest(stdout=stdout, stderr=stderr, had_error=had_error):
                case = exit_fixture()
                case.update(stdout=stdout, stderr=stderr, client_had_error=had_error)
                with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)

    def test_signal_and_native_cause_must_match_the_requested_boundary(self):
        for kind in ('cancel', 'statement_timeout'):
            for field in ('kind', 'cancel_sent', 'stderr'):
                case = exit_fixture(kind)
                if field == 'kind': case[field] = 'deadlock'
                elif field == 'cancel_sent': case[field] = not case[field]
                else: case[field] = exit_fixture('cancel' if kind == 'statement_timeout'
                                                else 'statement_timeout')['stderr']
                with self.subTest(kind=kind, field=field):
                    with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)

    def test_missing_duplicate_late_or_wrong_wait_end_is_rejected(self):
        case = exit_fixture()
        start, end = case['start']['log'], case['log'][len(case['start']['log']):]
        for log in (start, start + end * 2,
                    start + end.replace('seq=18', 'seq=16'),
                    start + end.replace('wait_seq=9', 'wait_seq=10'),
                    start + end.replace('pid=200', 'pid=201'),
                    start + end.replace('uba1=000000000040000a', 'uba1=000000000040000b'),
                    start + end.replace('itl_wrap=50', 'itl_wrap=10'),
                    (start + end).replace('xid=1200', 'xid=1201'),
                    start + end + start.replace('seq=17', 'seq=19'),
                    start + end + 'TX_IDENTITY event=LIMIT actor_node=0 pid=200\n'):
            with self.subTest(log=log):
                case = exit_fixture(); case['log'] = log
                with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)

    def test_unknown_or_unmatched_initial_blocker_remains_failure(self):
        for mutate in (lambda c: c['start']['trace'].update(export_complete=False),
                       lambda c: c['start']['trace']['events'][-1]['slots'][0].update(flags=4),
                       lambda c: c['start'].update(log=''),
                       lambda c: c['start']['holders'][0].update(xid=999)):
            case = exit_fixture(); mutate(case)
            with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)

    def test_cancelled_backend_must_be_the_same_live_aborted_session(self):
        for field, value in (('pid', 201), ('state', 'idle'), ('state', 'active'),
                             ('wait_event', 'GesTxEnqueueWait')):
            case = exit_fixture(); case['reader'][field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)

    def test_no_foreign_owner_may_be_finished_replaced_or_omitted(self):
        for mutate in (lambda h: h.pop(), lambda h: h.append(copy.deepcopy(h[0])),
                       lambda h: h[0].update(state='idle'),
                       lambda h: h[0].update(xid=999),
                       lambda h: h[0].update(pid=999),
                       lambda h: h[0].update(node=1),
                       lambda h: h[0].update(row=9)):
            case = exit_fixture(); mutate(case['holders'])
            with self.assertRaises(ValueError): m2_itl_capacity.validate_wait_exit(case)


if __name__ == '__main__':
    unittest.main()
