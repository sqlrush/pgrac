"""Reject stale or unjoined native B06 UPDATE requalification evidence."""
import copy
import unittest
import m2_itl_capacity as native
from test_m2_itl_capacity import fixture


def mutation(kind='update'):
    start = fixture()
    sql = 'UPDATE m2_b06_' + kind + ' SET v=v+1 WHERE id=9;'
    before = dict(id=9, v=0, ctid='(0,9)', xmin='1100', xmax='0')
    changed = (dict(id=9, v=10, ctid='(0,18)', xmin='1200', xmax='0')
               if kind == 'update' else copy.deepcopy(before))
    return dict(start=start, kind=kind, sql=sql, waiter_xid=1400,
                before=before, mutation_row=changed, mutator_pid=100,
                mutator_xid=1200, log=start['log'],
                reader=dict(pid=200, state='active', query=sql, wait_event='GesTxEnqueueWait'),
                holders=[dict(h, state='idle in transaction') for h in start['holders']])


def end_for(start, seq=18):
    row = start['log'].splitlines()[-1]
    return row.replace('event=WAIT_TARGET', 'event=WAIT_END').replace('seq=17 ', 'seq='+str(seq)+' ') + ' status=0\n'


def finished(kind='update'):
    m = mutation(kind)
    return dict(mutation=m, waits=[copy.deepcopy(m['start'])], log=m['log']+end_for(m['start']),
                stdout='B06_SQLSTATE 00000', stderr='', client_had_error=False,
                reader=dict(pid=200, state='idle in transaction', query=m['sql'], wait_event='ClientRead'),
                waiter_xid=1400, rows=([dict(id=9, v=11, ctid='(0,19)', xmin='1400', xmax='0')]
                                      if kind == 'update' else []),
                remaining=[dict(h, state='idle in transaction') for h in m['start']['holders'][1:]])


class RequalificationEvidence(unittest.TestCase):
    def setUp(self):
        self.mutate = getattr(native, 'validate_requalification_mutation', None)
        self.finish = getattr(native, 'validate_requalification_finish', None)
        self.assertTrue(callable(self.mutate) and callable(self.finish),
                        'native B06 evidence validators are missing')

    def test_both_mutations_join_the_original_exact_owner_before_commit(self):
        for kind in ('update', 'delete'):
            with self.subTest(kind=kind):
                r = self.mutate(mutation(kind))
                self.assertEqual((r['kind'], r['selected']['pid'], r['selected']['wait_seq']),
                                 (kind, 100, 9))
                self.assertFalse(r['authoritative'])

    def test_unrelated_owner_or_transaction_cannot_commit(self):
        for field, value in (('mutator_pid', 101), ('mutator_xid', 1201),
                             ('waiter_xid', 1200), ('waiter_xid', 0), ('kind', 'other')):
            c = mutation(); c[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): self.mutate(c)

    def test_stale_row_plan_wrong_tuple_or_cross_page_mutation_refuses(self):
        changes = [('id', 8), ('v', 1), ('xmin', '1201'), ('ctid', '(0,9)'),
                   ('ctid', '(1,18)'), ('ctid', '(0,0)'), ('xmax', '1234')]
        for field, value in changes:
            c = mutation(); c['mutation_row'][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): self.mutate(c)
        c = mutation(); c['before']['v'] = 8
        with self.assertRaises(ValueError): self.mutate(c)

    def test_delete_returns_the_original_target_only(self):
        for field, value in (('id', 8), ('v', 10), ('ctid', '(0,18)'), ('xmin', '999')):
            c = mutation('delete'); c['mutation_row'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.mutate(c)

    def test_mutation_requires_the_original_wait_still_running(self):
        for state in (dict(pid=201), dict(state='idle in transaction'),
                      dict(wait_event='ClientRead'), dict(query='SELECT 1')):
            c = mutation(); c['reader'].update(state)
            with self.subTest(state=state), self.assertRaises(ValueError): self.mutate(c)
        for suffix in (end_for(fixture()), fixture()['log']):
            c = mutation(); c['log'] += suffix
            with self.assertRaises(ValueError): self.mutate(c)

    def test_no_other_owner_is_released_or_replaced_before_commit(self):
        changes = [lambda c:c['holders'].pop(), lambda c:c['holders'][1].update(xid=999),
                   lambda c:c['holders'][2].update(state='idle'),
                   lambda c:c['holders'][0].update(pid=999)]
        for change in changes:
            c = mutation(); change(c)
            with self.assertRaises(ValueError): self.mutate(c)

    def test_same_update_observes_latest_value_or_deleted_row(self):
        for kind in ('update', 'delete'):
            r = self.finish(finished(kind))
            self.assertEqual((r['kind'], r['completed_waits']), (kind, 1))
            self.assertFalse(r['authoritative'])

    def test_finish_refuses_stale_value_reused_tuple_or_wrong_creator(self):
        for field, value in (('v', 1), ('id', 8), ('xmin', '1200'), ('ctid', '(0,18)')):
            c = finished(); c['rows'][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.finish(c)
        for kind in ('update', 'delete'):
            c = finished(kind); c['rows'] = [] if kind == 'update' else [dict(id=9, v=1)]
            with self.assertRaises(ValueError): self.finish(c)

    def test_finish_requires_same_backend_transaction_and_success(self):
        for change in (lambda c:c['reader'].update(pid=201),
                       lambda c:c['reader'].update(query='SELECT 1'),
                       lambda c:c['reader'].update(state='idle in transaction (aborted)'),
                       lambda c:c.update(waiter_xid=1401), lambda c:c.update(client_had_error=True),
                       lambda c:c.update(stdout='B06_SQLSTATE 57014'), lambda c:c.update(stderr='ERROR')):
            c = finished(); change(c)
            with self.assertRaises(ValueError): self.finish(c)

    def test_wait_end_must_match_each_exact_registration(self):
        for before, after in (('status=0', 'status=1'), ('itl_wrap=50', 'itl_wrap=51'),
                              ('uba1=000000000040000a', 'uba1=000000000040000b'),
                              ('wait_seq=9', 'wait_seq=10')):
            c = finished(); prefix = c['mutation']['log']
            c['log'] = prefix + c['log'][len(prefix):].replace(before, after)
            with self.subTest(after=after), self.assertRaises(ValueError): self.finish(c)
        for suffix in ('', end_for(fixture())*2):
            c = finished(); c['log'] = c['mutation']['log']+suffix
            with self.assertRaises(ValueError): self.finish(c)

    def test_unselected_owners_remain_open_until_completion_is_proved(self):
        for change in (lambda c:c['remaining'].pop(),
                       lambda c:c['remaining'][0].update(state='idle'),
                       lambda c:c['remaining'][0].update(xid=999)):
            c = finished(); change(c)
            with self.assertRaises(ValueError): self.finish(c)

    def test_later_capacity_wait_requires_its_own_raw_owner_join(self):
        c = finished(); second = copy.deepcopy(c['waits'][0])
        second['holders'] = second['holders'][1:]
        second['trace']['events'][-1]['slots'][0]['flags'] = 2
        second['log'] = c['log'] + ('TX_IDENTITY event=WAIT_TARGET seq=19 actor_node=0 pid=200 node=0 xid=1201 '
            'epoch=0 target_known=1 uba_known=1 uba0=0000000200000001 '
            'uba1=000000000040000b itl_wrap=51 itl_kind=1 itl_slot=1 wait_seq=10\n')
        c['waits'].append(second)
        c['log'] = second['log'] + second['log'].splitlines()[-1].replace('event=WAIT_TARGET', 'event=WAIT_END').replace('seq=19 ', 'seq=20 ') + ' status=0\n'
        c['remaining'] = c['remaining'][1:]
        self.assertEqual(self.finish(c)['completed_waits'], 2)
        c['waits'].pop()
        with self.assertRaises(ValueError): self.finish(c)

    def test_completed_recheck_of_an_already_finished_owner_needs_no_second_commit(self):
        c = finished()
        start = c['waits'][0]['log'].replace('seq=17 ', 'seq=19 ').replace('wait_seq=9', 'wait_seq=10')
        end = start.replace('event=WAIT_TARGET', 'event=WAIT_END').replace('seq=19 ', 'seq=20 ')
        c['log'] += start + end.rstrip() + ' status=0\n'
        self.assertEqual(self.finish(c)['completed_waits'], 2)
        self.assertEqual(self.finish(c)['released_owners'], 1)


if __name__ == '__main__':
    unittest.main()
