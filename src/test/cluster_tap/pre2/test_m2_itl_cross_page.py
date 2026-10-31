"""Require a real destination-page capacity wait for the ordinary UPDATE."""
import copy
import unittest
import m2_itl_capacity as native
from test_m2_itl_capacity import fixture
from test_m2_itl_requalify import end_for


def cross_page():
    start = fixture()
    for i, holder in enumerate(start['holders']):
        holder['ctid'] = '(0,' + str(10+i) + ')'
    layout = [dict(id=i, ctid='(0,'+str(i)+')', payload_bytes=0) for i in range(1, 9)]
    layout += [dict(id=9, ctid='(1,1)', payload_bytes=900),
               dict(id=10, ctid='(0,9)', payload_bytes=6472),
               dict(id=11, ctid='(1,2)', payload_bytes=6600)]
    sql = "UPDATE m2_b06_pair SET v=v+1,pad=repeat('n',480) WHERE id=9;"
    return dict(start=start, source_tag=[1663,5,20000,0,1],
                settings=dict(block_size=8192, payload_storage='p', inline_max_bytes=1024), layout=layout,
                before=dict(id=9, v=0, ctid='(1,1)', xmin='1100', xmax='0', payload_bytes=900, tuple_bytes=936),
                sql=sql, waiter_xid=1400, log=start['log'],
                reader=dict(pid=200, state='active', query=sql, wait_event='GesTxEnqueueWait'),
                holders=[dict(h, state='idle in transaction') for h in start['holders']])


def completed():
    c = cross_page()
    return dict(cross_page=c, waits=[copy.deepcopy(c['start'])], log=c['log']+end_for(c['start']),
                stdout='B06_PAIR_SQLSTATE 00000', stderr='', client_had_error=False,
                reader=dict(pid=200, state='idle in transaction', query=c['sql'], wait_event='ClientRead'),
                waiter_xid=1400,
                rows=[dict(id=9, v=1, ctid='(0,18)', xmin='1400', xmax='0',
                           payload_bytes=480, payload_matches=True)],
                remaining=[dict(h, state='idle in transaction') for h in c['start']['holders'][1:]])


class CrossPageEvidence(unittest.TestCase):
    def setUp(self):
        self.wait = getattr(native, 'validate_cross_page_wait', None)
        self.finish = getattr(native, 'validate_cross_page_finish', None)
        self.assertTrue(callable(self.wait) and callable(self.finish), 'cross-page evidence validators are missing')

    def test_original_source_and_exact_full_destination_are_distinct(self):
        result = self.wait(cross_page())
        self.assertEqual(result['source_tag'], [1663,5,20000,0,1])
        self.assertEqual(result['destination_tag'], [1663,5,20000,0,0])
        self.assertEqual((result['selected']['pid'], result['selected']['wait_seq']), (100,9))
        self.assertFalse(result['authoritative'])

    def test_alias_wrong_relation_noninline_or_wrong_shape_cannot_prove_pair(self):
        changes = [lambda c:c.update(source_tag=c['start']['tag']),
                   lambda c:c['source_tag'].__setitem__(2,999),
                   lambda c:c['settings'].update(payload_storage='x'),
                   lambda c:c['settings'].update(block_size=16384),
                   lambda c:c['before'].update(ctid='(0,9)'),
                   lambda c:c['before'].update(payload_bytes=1000),
                   lambda c:c['before'].update(xmax='1200')]
        for change in changes:
            c = cross_page(); change(c)
            with self.subTest(change=change), self.assertRaises(ValueError): self.wait(c)

    def test_all_original_owners_and_filler_locations_must_match_destination(self):
        changes = [lambda c:c['layout'].pop(), lambda c:c['layout'][9].update(payload_bytes=0),
                   lambda c:c['layout'][0].update(ctid='(1,3)'),
                   lambda c:c['start']['holders'][1].update(ctid='(1,4)'),
                   lambda c:c['start']['holders'][1].pop('ctid'),
                   lambda c:c['holders'][2].update(state='idle')]
        for change in changes:
            c = cross_page(); change(c)
            with self.assertRaises((ValueError, KeyError)): self.wait(c)

    def test_only_the_live_original_update_can_release_its_observed_owner(self):
        changes = [lambda c:c['reader'].update(pid=201), lambda c:c['reader'].update(query='SELECT 1'),
                   lambda c:c.update(waiter_xid=1200), lambda c:c['holders'][0].update(xid=999),
                   lambda c:c.update(log=c['log']+end_for(c['start'])),
                   lambda c:c['start']['trace'].update(overflow=True)]
        for change in changes:
            c = cross_page(); change(c)
            with self.assertRaises(ValueError): self.wait(c)

    def test_same_update_moves_to_destination_with_the_new_payload(self):
        result = self.finish(completed())
        self.assertEqual((result['completed_waits'], result['released_owners']), (1,1))
        self.assertFalse(result['authoritative'])

    def test_stale_location_creator_value_or_payload_refuses(self):
        for field, value in (('ctid','(1,1)'), ('ctid','(2,1)'), ('xmin','1200'),
                             ('v',0), ('payload_bytes',1800), ('payload_matches',False)):
            c = completed(); c['rows'][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.finish(c)

    def test_error_or_replaced_original_backend_transaction_refuses(self):
        changes = [lambda c:c.update(waiter_xid=1401), lambda c:c['reader'].update(pid=201),
                   lambda c:c['reader'].update(state='idle in transaction (aborted)'),
                   lambda c:c.update(stdout='B06_PAIR_SQLSTATE 57014'),
                   lambda c:c.update(client_had_error=True), lambda c:c.update(rows=[])]
        for change in changes:
            c = completed(); change(c)
            with self.assertRaises(ValueError): self.finish(c)

    def test_all_exact_wait_ends_and_unselected_owners_are_still_required(self):
        changes = [lambda c:c.update(log=c['cross_page']['log']),
                   lambda c:c.update(log=c['log'].replace('status=0','status=1')),
                   lambda c:c['remaining'].pop(), lambda c:c['remaining'][0].update(xid=999)]
        for change in changes:
            c = completed(); change(c)
            with self.assertRaises(ValueError): self.finish(c)


if __name__ == '__main__':
    unittest.main()
