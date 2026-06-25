#-------------------------------------------------------------------------
#
# 298_sequence_redo_block_scn.pl
#    spec-2.41 D4 / E group — sequence pd_block_scn redo persistence.
#
#    Proves the spec-2.41 D4 redo contract: a sequence page's pd_block_scn
#    stamp survives crash recovery because xl_seq_rec carries write_scn and
#    seq_redo restores PageHeader.pd_block_scn from THAT recorded SCN (the
#    historical write_scn), never from a replay-time current SCN.
#
#    Background (spec-2.41 §2.4 / G7): XLOG_SEQ_LOG uses REGBUF_WILL_INIT, so
#    seq_redo PageInit-rebuilds the page (clearing pd_block_scn to InvalidScn)
#    instead of restoring a full-page image.  Before D4 the stamp is therefore
#    LOST on redo.  D4 adds xl_seq_rec.write_scn + a seq_redo re-stamp so the
#    page version is durable across crash recovery.
#
#    Stamp gate (spec-2.41 §2.2, refined at D4 grounding): the sequence stamp
#    is gated on cluster_storage_mode_enabled() (cluster.enabled + a valid
#    cluster.node_id) AND relNumber >= FirstNormalObjectId — peers-INDEPENDENT,
#    consistent with the heap ITL pd_block_scn stamp (cluster_mode.h: version
#    metadata gates on storage mode, not peers).  A single cluster-storage node
#    therefore stamps user sequences, so this test is single-node (sessionC
#    Q5e) and never touches a cross-node / remote-PCM path.
#
#    Producer coverage (spec-2.41 §2.7 P1-A): a single-node cluster keeps user
#    sequences CLSQ_NATIVE, so THIS test covers the NATIVE producers only:
#    nextval_internal (@1246), do_setval (@1447) and fill_seq_fork_with_data
#    (@748 CREATE).  The managed-refill producer cluster_sq_refill_page (@243)
#    only runs with peers; its redo uses the SAME xl_seq_rec.write_scn /
#    seq_redo mechanism, but managed-refill redo is NOT proven here — that
#    evidence remains separately tracked pending C / Tab5 (do NOT assume the
#    2-node t/284 / t/285 fully cover managed-refill redo).
#
#    Number 298 is the formal spec-2.41/5.7 slot (5.7 misc-enqueue band
#    292-298; t/291 is spec-5.8, the 300-band is CR).
#
#    TDD provenance (spec-2.41 D4): L1 was RED pre-fix (stamp absent ->
#    pd_block_scn reads 8 zero bytes), GREEN post-fix.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/298_sequence_redo_block_scn.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

my $ZERO_SCN = '0000000000000000';    # InvalidScn = 0 as 8 little-endian bytes

# pd_block_scn lives at PageHeaderData byte offset 24 (0-based): pd_lsn(8) +
# pd_checksum(2) + pd_flags(2) + pd_lower(2) + pd_upper(2) + pd_special(2) +
# pd_pagesize_version(2) + pd_prune_xid(4) = 24, then SCN pd_block_scn (8B).
# substring() is 1-based, so offset 24 -> position 25.  (Same read as
# t/021 L8 / t/022.)
my $SCN_SUBSTR = q{SELECT encode(substring(get_raw_page('%s', 0), 25, 8), 'hex')};

my $node = PgracClusterNode->new('main');
$node->init;
# Enable cluster STORAGE mode (peers-independent): cluster.enabled defaults on
# but cluster.node_id defaults to -1, which leaves storage mode off.  Set a
# valid node_id so cluster_storage_mode_enabled() is true and the D4 sequence
# stamp fires.  Single-node (no pgrac.conf topology) => no peers => user
# sequences stay CLSQ_NATIVE => the nextval_internal / do_setval producers run.
$node->append_conf('postgresql.conf',
	"cluster.enabled = on\n" . "cluster.node_id = 0\n");
$node->start;

my $has_pageinspect = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_available_extensions WHERE name='pageinspect'});

SKIP:
{
	skip 'pageinspect contrib extension not available in this build', 5
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pageinspect;');

	# ----------------------------------------------------------------
	# E1: nextval_internal (@1246) stamps + redo restores pd_block_scn.
	# ----------------------------------------------------------------
	$node->safe_psql('postgres', q{
		CREATE SEQUENCE myseq;
		SELECT nextval('myseq');
		SELECT nextval('myseq');
		SELECT nextval('myseq');
	});

	my $before = $node->safe_psql('postgres', sprintf($SCN_SUBSTR, 'myseq'));

	# L1 (the RED gate pre-fix): the stamp must have happened — a tracked
	# sequence page in cluster storage mode carries a non-zero pd_block_scn.
	isnt($before, $ZERO_SCN,
		'L1 nextval stamped a valid pd_block_scn (D4 producer @1246; RED pre-fix)');

	# Advance the global SCN past `before` via unrelated WAL activity so a
	# (buggy) replay-time current-SCN re-stamp would differ from the recorded
	# write_scn.  Crucially: do NOT checkpoint after the myseq writes, so crash
	# recovery actually replays the XLOG_SEQ_LOG records and exercises seq_redo.
	$node->safe_psql('postgres', q{
		CREATE SEQUENCE other_seq;
		SELECT nextval('other_seq') FROM generate_series(1, 50);
		CREATE TABLE churn(x int);
		INSERT INTO churn SELECT g FROM generate_series(1, 1000) g;
	});

	# Crash (no clean shutdown / no checkpoint) and recover.
	$node->stop('immediate');
	$node->start;

	my $after = $node->safe_psql('postgres', sprintf($SCN_SUBSTR, 'myseq'));

	# L2: after crash recovery the page version equals the WAL-recorded
	# write_scn (== `before`), proving seq_redo restored it from
	# xl_seq_rec.write_scn and NOT from a replay-time current SCN.
	is($after, $before,
		'L2 seq_redo restored pd_block_scn from xl_seq_rec.write_scn (not replay-current)');
	isnt($after, $ZERO_SCN,
		'L2b pd_block_scn is still valid after recovery (not PageInit InvalidScn)');

	# ----------------------------------------------------------------
	# E3: do_setval (@1447) producer also stamps + survives redo.
	# ----------------------------------------------------------------
	$node->safe_psql('postgres', q{
		CREATE SEQUENCE setval_seq;
		SELECT setval('setval_seq', 4242);
	});
	my $sv_before = $node->safe_psql('postgres', sprintf($SCN_SUBSTR, 'setval_seq'));
	isnt($sv_before, $ZERO_SCN,
		'L3 setval stamped a valid pd_block_scn (D4 producer @1447)');

	$node->safe_psql('postgres',
		q{INSERT INTO churn SELECT g FROM generate_series(1, 500) g;});
	$node->stop('immediate');
	$node->start;

	my $sv_after = $node->safe_psql('postgres', sprintf($SCN_SUBSTR, 'setval_seq'));
	is($sv_after, $sv_before,
		'L4 setval pd_block_scn restored from xl_seq_rec.write_scn after recovery');

	# Note: fill_seq_fork_with_data (@748 CREATE) is exercised implicitly above
	# (every CREATE SEQUENCE inits the page).  cluster_sq_refill_page (@243,
	# managed refill) requires peers; its redo path uses the same mechanism
	# (xl_seq_rec.write_scn + seq_redo) but is NOT proven by this single-node
	# test — managed-refill redo evidence is separately tracked pending C / Tab5.
}

$node->stop;
done_testing();
