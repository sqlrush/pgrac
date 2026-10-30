"""Reject an ordinary two-page input that cannot fit its original undo receipt."""
import unittest

import m2_itl_capacity as native
from test_m2_itl_cross_page import cross_page


def setup():
    case = cross_page()
    case['destination_tag'] = case['start']['tag']
    for row in case['layout']:
        row['payload_bytes'] = {9: 900, 10: 6472, 11: 6600}.get(row['id'], 0)
    case['before'].update(payload_bytes=900, tuple_bytes=936)
    case['settings'].update(inline_max_bytes=1024)
    case['sql'] = "UPDATE m2_b06_pair SET v=v+1,pad=repeat('n',480) WHERE id=9;"
    return case


class CrossPageSetup(unittest.TestCase):
    def setUp(self):
        self.check = getattr(native, 'validate_cross_page_setup', None)
        self.assertTrue(callable(self.check), 'cross-page payload preflight is missing')

    def test_small_original_tuple_fits_the_unchanged_receipt(self):
        result = self.check(setup())
        self.assertEqual(result, dict(kind='pair_setup', source_tag=[1663,5,20000,0,1],
                         destination_tag=[1663,5,20000,0,0], update_payload_bytes=948,
                         inline_max_bytes=1024, authoritative=False))

    def test_old_oversized_input_is_not_eligible(self):
        case = setup()
        case['before'].update(payload_bytes=1800, tuple_bytes=1836)
        for row in case['layout']:
            row['payload_bytes'] = {9: 1800, 10: 5800, 11: 5600}.get(row['id'], 0)
        case['sql'] = "UPDATE m2_b06_pair SET v=v+1,pad=repeat('n',1000) WHERE id=9;"
        with self.assertRaises(ValueError): self.check(case)

    def test_live_capacity_bounds_the_full_old_tuple_plus_undo_header(self):
        case = setup()
        case['settings']['inline_max_bytes'] = 948
        self.assertEqual(self.check(case)['update_payload_bytes'], 948)
        for cap in (0, 1, 947):
            case['settings']['inline_max_bytes'] = cap
            with self.subTest(cap=cap), self.assertRaises(ValueError): self.check(case)

    def test_missing_or_underreported_physical_tuple_size_refuses(self):
        for size in (0, 900, 935, 937, 1836):
            case = setup(); case['before']['tuple_bytes'] = size
            with self.subTest(size=size), self.assertRaises(ValueError): self.check(case)
        case = setup(); del case['before']['tuple_bytes']
        with self.assertRaises((KeyError, ValueError)): self.check(case)

    def test_original_two_page_geometry_and_inline_storage_are_still_required(self):
        changes = [lambda c:c['layout'][9].update(payload_bytes=5800),
                   lambda c:c['layout'][10].update(ctid='(0,10)'),
                   lambda c:c['settings'].update(payload_storage='x'),
                   lambda c:c['settings'].update(block_size=16384),
                   lambda c:c.update(destination_tag=c['source_tag']),
                   lambda c:c['before'].update(xmax='1200')]
        for change in changes:
            case = setup(); change(case)
            with self.subTest(change=change), self.assertRaises(ValueError): self.check(case)

    def test_wait_cannot_bypass_an_oversized_or_unknown_preflight(self):
        case = cross_page()
        case['settings']['inline_max_bytes'] = 947
        case['before']['tuple_bytes'] = 936
        with self.assertRaises(ValueError): native.validate_cross_page_wait(case)


if __name__ == '__main__':
    unittest.main()
