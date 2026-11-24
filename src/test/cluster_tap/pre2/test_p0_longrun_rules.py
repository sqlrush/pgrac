"""The label table must quote the product source it claims to quote.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

from pathlib import Path
import re
import sys
from typing import Dict, List, Optional
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
SOURCE = HERE.parents[3]

import p0_longrun_rules as rules  # noqa: E402

_LITERAL = re.compile(r'"((?:[^"\\\n]|\\.)*)"')


def c_string_literals(text: str) -> List[str]:
    """String literals of a C file, with adjacent literals concatenated."""
    joined: List[str] = []
    last_end = None
    for match in _LITERAL.finditer(text):
        if last_end is not None and not text[last_end:match.start()].strip():
            joined[-1] += match.group(1)
        else:
            joined.append(match.group(1))
        last_end = match.end()
    return joined


def event(message: str, sqlstate: Optional[str] = None, phase: str = 'load',
          severity: str = 'ERROR', source: str = 'server') -> Dict[str, object]:
    return dict(message=message, sqlstate=sqlstate, phase=phase, severity=severity,
                source=source)


def label(*args: object, **kwargs: object) -> Optional[str]:
    rule = rules.classify(event(*args, **kwargs))
    return rule.rule_id if rule else None


class RuleTable(unittest.TestCase):
    def test_table_is_well_formed(self) -> None:
        self.assertTrue(rules.validate())
        self.assertEqual(len({r.rule_id for r in rules.RULES}), len(rules.RULES))

    def test_every_text_rule_quotes_a_message_in_the_source(self) -> None:
        cache: Dict[str, List[str]] = {}
        for rule in rules.RULES:
            for relative, fragment in rule.anchors:
                path = SOURCE / relative
                self.assertTrue(path.is_file(), '%s: missing %s' % (rule.rule_id, relative))
                if relative not in cache:
                    cache[relative] = c_string_literals(path.read_text(errors='replace'))
                self.assertTrue(any(fragment in literal for literal in cache[relative]),
                                '%s: %s no longer contains "%s"' % (rule.rule_id, relative,
                                                                    fragment))

    def test_every_quoted_message_matches_its_own_rule(self) -> None:
        for rule in rules.RULES:
            if rule.basis != 'text':
                continue
            for _, fragment in rule.anchors:
                rendered = (fragment.replace('\\"', '"').replace('%s', 'x')
                            .replace('%u', '7').replace('%d', '7'))
                found = rules.classify(event(rendered))
                self.assertIsNotNone(found, fragment)
                self.assertEqual(found.bucket, rule.bucket, fragment)

    def test_declared_classes_are_in_errcodes(self) -> None:
        declared = {}
        for line in (SOURCE / 'src/backend/utils/errcodes.txt').read_text().splitlines():
            fields = line.split()
            if len(fields) >= 3 and re.fullmatch('[0-9A-Z]{5}', fields[0]):
                declared[fields[0]] = fields[2]
        for rule in rules.RULES:
            for sqlstate, macro in rule.codes:
                self.assertEqual(declared.get(sqlstate), macro, rule.rule_id)


class Classification(unittest.TestCase):
    def test_retained_original_messages(self) -> None:
        cases = [
            ('cluster TT status unknown for R4 scratch xid 6264227: '
             'RECYCLED_AUTHORITY_UNPROVABLE', None, 'p0-5.tt-status-unknown'),
            ('cluster_gcs_block: could not obtain read image from X holder 1 for tag spc=1663 '
             'db=5 relNumber=16388 block=137', None, 'p0-10.read-image'),
            ('R4 HOT reconstruction cannot prove the logical row: HOT root or member is unused '
             'or unprovable', None, 'p0-12.hot-unprovable'),
            ('cluster TT slot allocator: retention rollover failed for segment 1024 (undo '
             'segment pool hard cap reached)', None, 'p0-7.undo-pool-hard-cap'),
            ('cluster TT slot allocator exhausted on segment 514 (48 slots full)', None,
             'p0-9.tt-segment-slots-full'),
            ('ITL slot OVERFLOW on heap page (INITRANS=8 full)', '53R70', 'p0-8.itl-overflow'),
            ('CTRC receipt preparation failed before heap update', '53R9D',
             'p0-11.ctrc-receipt-prepare'),
            ('shared-heap UPDATE on relation "t" does not support subtransactions', '0A000',
             'p0-13.subtransaction-refused'),
            ('cluster normal-stop: pre-checkpoint barrier failed', None, 'p0-1.normal-stop'),
            ('LMON normal-stop post-dispatch check failed', None, 'p0-1.normal-stop'),
            ('timed out waiting for remote writer 4195200 on node 2', '53R70',
             'hot-tx-01.remote-writer-wait'),
        ]
        for message, sqlstate, expected in cases:
            self.assertEqual(label(message, sqlstate), expected, message)

    def test_quoted_text_decides_before_the_declared_class(self) -> None:
        self.assertEqual(label('R4 HOT full-block fetch did not return a complete page',
                               '53R97'), 'p0-12.hot-unprovable')
        self.assertEqual(label('some other proof refusal', '53R97'),
                         'p0-5.sqlstate-tt-status-unknown')
        self.assertEqual(label('cluster CR cross-instance UBA encountered (origin_node_id=1, '
                               'local=0)', '53R9G'), 'p0-10.sqlstate-cr-cross-instance')

    def test_unknown_error_gets_no_label(self) -> None:
        self.assertIsNone(label('could not create exact SPACE identity for relation 16385',
                                'XX001'))
        self.assertIsNone(label('deadlock detected', '40P01'))
        self.assertIsNone(label('some other proof refusal'))

    def test_lifecycle_step_labels_only_severe_server_records(self) -> None:
        self.assertEqual(label('anything', None, 'stop', 'PANIC'), 'p0-1.stop-step-severe')
        self.assertEqual(label('anything', None, 'start', 'FATAL'), 'p0-4.start-step-severe')
        self.assertEqual(label('anything', None, 'restart', 'CRASH'), 'p0-4.start-step-severe')
        self.assertIsNone(label('anything', None, 'stop', 'ERROR'))
        self.assertIsNone(label('anything', None, 'stop', 'FATAL', 'client'))
        self.assertIsNone(label('anything', None, 'load', 'PANIC'))
        self.assertEqual(label('anything', None, 'drop', 'ERROR', 'client'), 'p0-14.drop-step')

    def test_labels_without_a_quoted_message_are_declared(self) -> None:
        self.assertEqual(set(rules.BUCKETS_WITHOUT_TEXT_RULE),
                         {'P0-3', 'P0-4', 'P0-6', 'P0-14'})


if __name__ == '__main__':
    unittest.main()
