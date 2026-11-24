"""Map server and client errors of a four-member long run to issue labels.

Every text rule quotes a message emitted by the product source and names the
file it comes from; test_p0_longrun_rules.py checks those quotations against
the tree. A label is bookkeeping for the reader of a run report. It is never
an allowance: the judge fails a run for every error that is not explicitly
allowed, whether or not a rule recognises it.
Author: SqlRush <sqlrush@gmail.com>
"""
from __future__ import annotations

import re
from typing import Mapping, NamedTuple, Optional, Sequence, Tuple


class Rule(NamedTuple):
    """One label assignment. basis: 'text' quotes an emitted message,
    'sqlstate' is the declared error class only, 'phase' is the lifecycle
    step in which a severe record appeared."""
    rule_id: str
    bucket: str
    basis: str
    message: Optional[str]
    sqlstates: Tuple[str, ...]
    phases: Tuple[str, ...]
    severities: Tuple[str, ...]
    sources: Tuple[str, ...]
    anchors: Tuple[Tuple[str, str], ...]
    codes: Tuple[Tuple[str, str], ...]


def _rule(rule_id: str, bucket: str, basis: str, message: Optional[str] = None,
          sqlstates: Sequence[str] = (), phases: Sequence[str] = (),
          severities: Sequence[str] = (), sources: Sequence[str] = (),
          anchors: Sequence[Tuple[str, str]] = (),
          codes: Sequence[Tuple[str, str]] = ()) -> Rule:
    return Rule(rule_id, bucket, basis, message, tuple(sqlstates), tuple(phases),
                tuple(severities), tuple(sources), tuple(anchors), tuple(codes))


HEAPAM = 'src/backend/access/heap/heapam.c'
VISIBILITY = 'src/backend/access/heap/heapam_visibility.c'
TT_LOCAL = 'src/backend/cluster/cluster_tt_local.c'
SEVERE = ('FATAL', 'PANIC', 'CRASH')

# Order matters: the first matching rule labels the record.
RULES = (
    _rule('p0-12.hot-unprovable', 'P0-12', 'text',
          message=r'R4 HOT (reconstruction cannot prove the logical row'
                  r'|full-block fetch did not return a complete page)',
          anchors=((HEAPAM, 'R4 HOT reconstruction cannot prove the logical row: %s'),
                   (HEAPAM, 'R4 HOT full-block fetch did not return a complete page'))),
    _rule('p0-5.tt-status-unknown', 'P0-5', 'text',
          message=r'cluster TT (status unknown|slot recycled) for ',
          anchors=((VISIBILITY, 'cluster TT status unknown for R4 scratch xid %u: %s'),
                   (VISIBILITY, 'cluster TT status unknown for xid %u'),
                   (VISIBILITY, 'cluster TT slot recycled for xmin %u'),
                   (HEAPAM, 'cluster TT status unknown for remote writer %u'))),
    _rule('p0-7.undo-pool-hard-cap', 'P0-7', 'text',
          message=r'undo segment pool hard cap reached',
          anchors=((TT_LOCAL, 'undo segment pool hard cap reached'),
                   ('src/backend/cluster/cluster_undo_record.c',
                    'cluster undo segment pool hard cap reached for instance %u'))),
    _rule('p0-7.tt-rollover-failed', 'P0-7', 'text',
          message=r'cluster TT slot allocator: retention rollover failed for segment',
          anchors=((TT_LOCAL, 'cluster TT slot allocator: retention rollover failed for '
                              'segment %u (%s)'),)),
    _rule('p0-9.tt-segment-slots-full', 'P0-9', 'text',
          message=r'cluster TT slot allocator exhausted on segment \d+ \(48 slots full\)',
          anchors=((TT_LOCAL, 'cluster TT slot allocator exhausted on segment %u '
                              '(48 slots full)'),)),
    _rule('p0-8.itl-overflow', 'P0-8', 'text',
          message=r'ITL slot OVERFLOW (on heap page|before heap tuple lock)',
          anchors=((HEAPAM, 'ITL slot OVERFLOW on heap page (INITRANS=%d full)'),
                   (HEAPAM, 'ITL slot OVERFLOW before heap tuple lock (INITRANS=%d full)'))),
    _rule('p0-8.itl-capacity-wait', 'P0-8', 'text',
          message=r'while waiting for heap (update|tuple-lock) ITL capacity',
          anchors=((HEAPAM, 'deadlock detected while waiting for heap update ITL capacity '
                            '(INITRANS=%d)'),
                   (HEAPAM, 'deadlock detected while waiting for heap tuple-lock ITL capacity '
                            '(INITRANS=%d)'))),
    _rule('p0-8.itl-receipt-prepare', 'P0-8', 'text',
          message=r'cluster ITL receipt preparation failed',
          anchors=((HEAPAM, 'cluster ITL receipt preparation failed on follow_updates'),)),
    _rule('p0-10.read-image', 'P0-10', 'text',
          message=r'could not obtain read image from X holder',
          anchors=(('src/backend/cluster/cluster_gcs_block.c',
                    'cluster_gcs_block: could not obtain read image from X holder %d '
                    'for tag spc=%u db=%u relNumber=%u block=%u'),)),
    _rule('p0-11.ctrc-receipt-prepare', 'P0-11', 'text',
          message=r'CTRC receipt preparation failed before ',
          anchors=((HEAPAM, 'CTRC receipt preparation failed before heap update'),
                   (HEAPAM, 'CTRC receipt preparation failed before heap insert'),
                   (HEAPAM, 'CTRC receipt preparation failed before heap delete'))),
    _rule('p0-11.undo-receipt-prepare', 'P0-11', 'text',
          message=r'cluster undo receipt preparation failed for ',
          anchors=((HEAPAM, 'cluster undo receipt preparation failed for heap_update'),
                   (HEAPAM, 'cluster undo receipt preparation failed for heap_insert'))),
    _rule('p0-13.subtransaction-refused', 'P0-13', 'text',
          message=r'does not support subtransactions',
          anchors=((HEAPAM, 'shared-heap %s on relation \\"%s\\" does not support '
                            'subtransactions'),)),
    _rule('p0-1.normal-stop', 'P0-1', 'text',
          message=r'normal[- ]stop|normal shutdown'
                  r'|could not confirm canonical control-root read-lock release',
          anchors=(('src/backend/postmaster/checkpointer.c',
                    'cluster normal-stop: pre-checkpoint barrier failed'),
                   ('src/backend/cluster/cluster_lmon.c',
                    'LMON normal-stop post-dispatch check failed'),
                   ('src/backend/cluster/cluster_sinval_bcast.c',
                    'SI broadcaster cannot register normal-stop work'),
                   ('src/backend/access/transam/xlog.c',
                    'checkpoint interrupted to service normal shutdown'),
                   ('src/backend/cluster/cluster_control_root.c',
                    'could not confirm canonical control-root read-lock release'))),
    _rule('p0-2.wraparound-limit', 'P0-2', 'text',
          message=r'to avoid wraparound data loss'
                  r'|cluster xid window hard limit reached'
                  r'|cluster mxid half-space limit reached',
          anchors=(('src/backend/access/transam/varsup.c',
                    'database is not accepting commands to avoid wraparound data loss '
                    'in database \\"%s\\"'),
                   ('src/backend/access/transam/varsup.c',
                    'refusing to assign a new transaction ID: cluster xid window hard '
                    'limit reached'),
                   ('src/backend/access/transam/multixact.c',
                    'refusing to assign a new MultiXactId: cluster mxid half-space limit '
                    'reached'))),
    _rule('hot-tx-01.remote-writer-wait', 'HOT-TX-01', 'text',
          message=r'timed out waiting for remote writer \d+ on node \d+',
          anchors=((HEAPAM, 'timed out waiting for remote writer %u on node %u'),)),
    # Declared error class only; the message was not one quoted above.
    _rule('p0-10.sqlstate-cr-cross-instance', 'P0-10', 'sqlstate', sqlstates=('53R9G',),
          codes=(('53R9G', 'ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED'),)),
    _rule('p0-5.sqlstate-tt-status-unknown', 'P0-5', 'sqlstate', sqlstates=('53R97',),
          codes=(('53R97', 'ERRCODE_CLUSTER_TT_STATUS_UNKNOWN'),)),
    _rule('p0-8.sqlstate-itl-overflow', 'P0-8', 'sqlstate', sqlstates=('53R9A',),
          codes=(('53R9A', 'ERRCODE_CLUSTER_ITL_SLOT_OVERFLOW'),)),
    _rule('p0-7.sqlstate-undo-hard-cap', 'P0-7', 'sqlstate', sqlstates=('53R9E',),
          codes=(('53R9E', 'ERRCODE_CLUSTER_UNDO_SEGMENTS_HARD_CAP_REACHED'),)),
    _rule('p0-14.sqlstate-object-flush', 'P0-14', 'sqlstate', sqlstates=('53RAA',),
          codes=(('53RAA', 'ERRCODE_CLUSTER_OBJECT_FLUSH_UNAVAILABLE'),)),
    _rule('p0-6.sqlstate-quorum-fence', 'P0-6', 'sqlstate',
          sqlstates=('53R40', '53R50', '53R51'),
          codes=(('53R40', 'ERRCODE_CLUSTER_QUORUM_LOST'),
                 ('53R50', 'ERRCODE_CLUSTER_QUORUM_LOST_BACKEND'),
                 ('53R51', 'ERRCODE_CLUSTER_WRITE_FENCED'))),
    # Lifecycle step of the driver; applies only when nothing above matched.
    _rule('p0-14.drop-step', 'P0-14', 'phase', phases=('drop',)),
    _rule('p0-1.stop-step-severe', 'P0-1', 'phase', phases=('stop', 'stop_again'),
          severities=SEVERE, sources=('server',)),
    _rule('p0-4.start-step-severe', 'P0-4', 'phase', phases=('start', 'restart', 'restart_again'),
          severities=SEVERE, sources=('server',)),
)

BUCKETS = tuple('P0-%d' % n for n in range(1, 15)) + ('HOT-TX-01',)
# Labels for which no emitted message is quoted; their errors stay unlabelled
# unless a declared class or lifecycle step identifies them.
BUCKETS_WITHOUT_TEXT_RULE = tuple(
    b for b in BUCKETS if not any(r.bucket == b and r.basis == 'text' for r in RULES))

_COMPILED = {r.rule_id: re.compile(r.message) for r in RULES if r.message}


def matches(rule: Rule, event: Mapping[str, object]) -> bool:
    """event: mapping with source, phase, severity, sqlstate, message."""
    if rule.sources and event.get('source') not in rule.sources:
        return False
    if rule.phases and event.get('phase') not in rule.phases:
        return False
    if rule.severities and event.get('severity') not in rule.severities:
        return False
    if rule.sqlstates and event.get('sqlstate') not in rule.sqlstates:
        return False
    if rule.message and not _COMPILED[rule.rule_id].search(str(event.get('message') or '')):
        return False
    return True


def classify(event: Mapping[str, object], rules: Sequence[Rule] = RULES) -> Optional[Rule]:
    """Return the first matching rule, or None for an unlabelled record."""
    for rule in rules:
        if matches(rule, event):
            return rule
    return None


def validate(rules: Sequence[Rule] = RULES) -> bool:
    seen: set = set()
    for rule in rules:
        if rule.rule_id in seen:
            raise ValueError('duplicate rule: ' + rule.rule_id)
        seen.add(rule.rule_id)
        if rule.bucket not in BUCKETS or rule.basis not in ('text', 'sqlstate', 'phase'):
            raise ValueError('invalid rule label: ' + rule.rule_id)
        selective = {'text': rule.message, 'sqlstate': rule.sqlstates, 'phase': rule.phases}
        if not selective[rule.basis]:
            raise ValueError('rule cannot match anything specific: ' + rule.rule_id)
        if rule.basis == 'text' and not rule.anchors:
            raise ValueError('text rule quotes no source message: ' + rule.rule_id)
        if rule.basis == 'sqlstate' and {c for c, _ in rule.codes} != set(rule.sqlstates):
            raise ValueError('declared class lacks its errcodes.txt name: ' + rule.rule_id)
    return True
