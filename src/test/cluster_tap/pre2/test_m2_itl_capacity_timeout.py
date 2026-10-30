"""A finite ITL budget is distinct from statement cancellation or SQL timeout."""
import copy
import unittest

from m2_itl_capacity import validate_wait_exit
from test_m2_itl_wait_exit import exit_fixture


def capacity_timeout_fixture():
    case = exit_fixture('statement_timeout')
    case.update(kind='capacity_timeout', stdout='B08_SQLSTATE 53R70',
                stderr='ERROR:  53R70: ITL slot OVERFLOW on heap page (INITRANS=8 full)\n'
                       'DETAIL: PGRAC_FAMILY=ITL_CAPACITY '
                       'PGRAC_REASON=ITL_CAPACITY_WAIT_EXPIRED PGRAC_NODE=0 '
                       'PGRAC_ATTEMPT=0 wait_result=1 deadline_us=5000000\n',
                settings=dict(capacity_timeout_ms=5000, statement_timeout_ms=10000, waiter_xid=2200))
    case['log'] = case['log'].rstrip() + ' status=1\n'
    case['log'] += ('2026-10-30 [200] DETAIL: PGRAC_FAMILY=ITL_CAPACITY_DIAGNOSTIC '
                    'PGRAC_REASON=WAIT_TERMINAL waiter_xid=2200 blocker_xid=1200 '
                    'blocker_wrap=50 lock_only=0 returned=1 result=1 '
                    'resolve_reason=21 deadline_us=5000000\n')
    return case


class CapacityTimeoutEvidence(unittest.TestCase):
    def test_original_capacity_budget_has_its_own_error_and_exact_cleanup(self):
        case = capacity_timeout_fixture()
        before = copy.deepcopy(case)
        try:
            result = validate_wait_exit(case)
        except ValueError as exc:
            self.fail('complete original capacity timeout refused: ' + str(exc))
        self.assertEqual(result['sqlstate'], '53R70')
        self.assertEqual(result['kind'], 'capacity_timeout')
        self.assertEqual(result['deadline_us'], 5000000)
        self.assertEqual(result['wait_seq'], 9)
        self.assertEqual(result['end_seq'], 18)
        self.assertFalse(result['authoritative'])
        self.assertEqual(case, before)

    def test_cancel_statement_timeout_and_other_refusals_are_not_capacity_expiry(self):
        for key, value in (('cancel_sent', True), ('client_had_error', False),
                           ('stdout', 'B08_SQLSTATE 57014'), ('stderr', ''),
                           ('stderr', 'ERROR:  57014: canceling statement due to statement timeout\n'),
                           ('stderr', 'ERROR:  53R70: timed out waiting for remote writer\n')):
            case = capacity_timeout_fixture(); case[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError): validate_wait_exit(case)

    def test_exact_caller_owner_wrap_result_and_original_deadline_must_match(self):
        for old, new in (('[200]', '[201]'), ('waiter_xid=2200', 'waiter_xid=2201'),
                         ('blocker_xid=1200', 'blocker_xid=1201'),
                         ('blocker_wrap=50', 'blocker_wrap=10'), ('returned=1', 'returned=0'),
                         ('result=1 resolve_reason', 'result=4 resolve_reason'),
                         ('resolve_reason=21', 'resolve_reason=22'), ('lock_only=0', 'lock_only=1'),
                         ('deadline_us=5000000', 'deadline_us=6000000')):
            case = capacity_timeout_fixture(); case['log'] = case['log'].replace(old, new)
            with self.subTest(new=new):
                with self.assertRaises(ValueError): validate_wait_exit(case)

    def test_missing_duplicate_or_unbounded_deadline_diagnostics_fail_closed(self):
        case = capacity_timeout_fixture()
        diagnostic = case['log'].splitlines(keepends=True)[-1]
        for log in (case['log'].replace(diagnostic, ''), case['log'] + diagnostic,
                    case['log'].replace('deadline_us=5000000', 'deadline_us=0'),
                    case['log'].replace('returned=1', 'returned=1 returned=1')):
            changed = capacity_timeout_fixture(); changed['log'] = log
            with self.assertRaises(ValueError): validate_wait_exit(changed)
        for old, new in (('deadline_us=5000000', 'deadline_us=0'),
                         ('deadline_us=5000000', 'deadline_us=18446744073709551615'),
                         ('PGRAC_REASON=ITL_CAPACITY_WAIT_EXPIRED', 'PGRAC_REASON=ITL_BLOCKER_UNPROVABLE'),
                         ('wait_result=1', 'wait_result=4')):
            changed = capacity_timeout_fixture(); changed['stderr'] = changed['stderr'].replace(old, new)
            with self.assertRaises(ValueError): validate_wait_exit(changed)

    def test_the_actual_finite_setting_must_precede_the_sql_statement_timeout(self):
        for key, value in (('capacity_timeout_ms', 0), ('capacity_timeout_ms', -1),
                           ('capacity_timeout_ms', 10000), ('statement_timeout_ms', 1000),
                           ('waiter_xid', 1200), ('waiter_xid', 0)):
            case = capacity_timeout_fixture(); case['settings'][key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaises(ValueError): validate_wait_exit(case)
        case = capacity_timeout_fixture(); del case['settings']
        with self.assertRaises((KeyError, ValueError)): validate_wait_exit(case)

    def test_original_wait_end_must_report_the_real_timeout_result(self):
        for old, new in (('status=1', 'status=4'), ('status=1', 'status=5'),
                         ('status=1', ''), ('wait_seq=9', 'wait_seq=10'),
                         ('event=WAIT_END seq=18', 'event=WAIT_END seq=16')):
            case = capacity_timeout_fixture()
            before, after = case['log'].split('\n', 1)
            case['log'] = before + '\n' + after.replace(old, new)
            with self.subTest(new=new):
                with self.assertRaises(ValueError): validate_wait_exit(case)

    def test_no_capacity_exit_can_release_or_replace_an_original_owner(self):
        for field, value in (('state', 'idle'), ('xid', 999), ('pid', 999), ('row', 9)):
            case = capacity_timeout_fixture(); case['holders'][0][field] = value
            with self.assertRaises(ValueError): validate_wait_exit(case)

    def test_error_details_cannot_be_duplicated_or_labeled_as_another_node(self):
        case = capacity_timeout_fixture()
        detail = case['stderr'].splitlines(keepends=True)[1]
        for stderr in (case['stderr'] + detail,
                       case['stderr'].replace('PGRAC_NODE=0', 'PGRAC_NODE=1'),
                       case['stderr'].replace('PGRAC_ATTEMPT=0', 'PGRAC_ATTEMPT=1'),
                       case['stderr'].replace('wait_result=1', 'wait_result=1 wait_result=1')):
            changed = capacity_timeout_fixture(); changed['stderr'] = stderr
            with self.assertRaises(ValueError): validate_wait_exit(changed)


if __name__ == '__main__':
    unittest.main()
