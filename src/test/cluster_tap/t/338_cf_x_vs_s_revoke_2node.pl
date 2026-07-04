#-------------------------------------------------------------------------
#
# 338_cf_x_vs_s_revoke_2node.pl
#    spec-6.14a -- CF X-vs-S invalidate-then-grant, 2-node mechanism legs.
#
#    Exercises the cross-node "write a block another live node has cached
#    in S" shapes on USER tables (same-DDL / same-relfilenode harness --
#    shared catalog is spec-6.14's consumer test).  Genuine S residency
#    needs cluster.read_scache = on (off keeps one-shot read-image
#    shipping for X-held blocks: no S bit at the master, so the X-vs-S
#    shape never arises); the shared-catalog boot shape gets S residency
#    from N-state first-reads instead, which this harness cannot stage
#    without a node restart.  Hence: one plain-share leg with the GUC
#    off, then the revoke legs run with it on.
#
#      L1  boot: 2-node pair with shared data, read_scache OFF (default).
#      L2  plain-share leg (read_scache off): writer inserts, both nodes
#          read (one-shot image ships, no S residency), writer updates
#          every block.  Asserts: reads correct, write-back succeeds,
#          X-held reads really served, zero
#          cross-node-write-not-supported wall hits (L442).
#      L3  revoke leg, forward direction (read_scache flipped ON both
#          nodes): node1 re-reads -> quiescent X->S downgrade leaves
#          BOTH nodes S-resident; node0 then updates every block, so
#          node0-mastered blocks take the local (a) arm and
#          node1-mastered blocks take the remote B1 leg.  Asserts: the
#          strict X-vs-S grant counters moved, zero wall hits.
#      L4  revoke leg, reverse direction: on a dedicated node1-owned
#          table, node0 reads (S residency), node1 updates every block
#          -- node0's S copies get revoked.  One writer per table keeps
#          every writer resolving only its own xids (see L7 note).
#      L5  content integrity: materialized sums on each table's OWNER
#          node match the committed workload (owner-node check, L444 --
#          no cross-node read-by-name before shared catalog).
#      L6  (b)-leg surface: the no-carrier fail-closed counters are
#          exposed and stayed 0 through the whole normal workload --
#          normal write paths never need the non-holder leg.
#      L7  invalidate-ack stall (injection, node1 holder): writer gets a
#          bounded retryable error, no hang (HG7), timeout counter
#          moved; after disarming, the same write succeeds.  Runs on a
#          dedicated table only node0 ever writes, so the stalled writer
#          resolves nothing but its own xids (the phantom-shared harness
#          has per-node TT + overlapping xid spaces: writing blocks last
#          written by the OTHER node can hit the known
#          recycled-remote-TT-slot fail-closed, feature #119 boundary --
#          a real gap, but not the surface under test here).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/338_cf_x_vs_s_revoke_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-6.14a-cf-s-revoke-x-grant.md (D6)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'CF X-vs-S revoke requires --enable-cluster';
}

my $NROWS = 4000;				# multiple heap blocks so both nodes master some

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

# Sum a counter across both nodes (each master keeps its own).
sub pair_val
{
	my ($pair, $cat, $key) = @_;
	return state_val($pair->node0, $cat, $key)
		+ state_val($pair->node1, $cat, $key);
}

# Strict X-vs-S grant evidence: the local (a) arm grant, the remote
# non-holder (B2) grant, and the S->X upgrade transition (B1 / local arm
# at the master).  Deliberately NOT block_storage_fallback_count -- that
# moves on plain reads and would make this assertion vacuous.
sub grants	   { return pair_val($_[0], 'gcs', 'local_s_upgrade_grant_count')
					+ pair_val($_[0], 'gcs', 'x_vs_s_nonholder_grant_count')
					+ pair_val($_[0], 'pcm', 'trans_s_to_x_upgrade_count'); }
sub no_carrier { return pair_val($_[0], 'gcs', 'x_vs_s_no_carrier_denied_count')
					+ pair_val($_[0], 'pcm', 'local_s_revoke_nonholder_failclosed_count'); }

# CI-triage aid: dump non-zero gcs/pcm counters per node as TAP notes.
sub diag_counters
{
	my ($pair, $tag) = @_;
	for my $i (0, 1)
	{
		my $node = $i == 0 ? $pair->node0 : $pair->node1;
		my $rows = $node->safe_psql('postgres',
			q{SELECT category || '/' || key || '=' || value
			  FROM pg_cluster_state
			  WHERE category IN ('gcs','pcm') AND value NOT IN ('0','')
			  ORDER BY 1});
		Test::More::note("DIAG[$tag] node$i:\n$rows");
	}
	return;
}

# Bounded-retry write: cross-node revoke rounds may serve bounded retryable
# fail-closed (rule 8.A behaviour, not a test failure -- L445).  Returns the
# number of attempts used, 0 on give-up.
sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 20)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return $i if $ok;
		usleep(500_000);
	}
	return 0;
}

# ALTER SYSTEM + reload: PGC_SIGHUP GUCs (read_scache, injection_points)
# must reach EVERY process on the node, including the aux process that
# acks INVALIDATE (LMS/LMON both run ProcessConfigFile on SIGHUP) -- the
# SQL SRF injection arm is process-local (spec-0.27 s3.6) and cannot.
sub set_guc_reload
{
	my ($node, $guc, $val) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET $guc = '$val'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	return;
}

sub slurp_grep
{
	my ($node, $pat) = @_;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	my @hits = ($log =~ /\Q$pat\E/g);
	return scalar(@hits);
}

# ============================================================
# L1: boot -- shared data, read_scache stays OFF (default).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'xvss614a',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		# L355: widen heartbeat deathwatch under CI load.
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0');

my $node0 = $pair->node0;
my $node1 = $pair->node1;

# Same-DDL harness premise: both nodes run the DDL, land on the same
# shared relfilenode (t/248 L0 convention; catalog stays per-node here).
# Plain heap, no index: duplicated index DDL cannot work over coincident
# files (t/334 convention -- the second build sees a non-empty index).
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres',
		'CREATE TABLE xvss (id int, v bigint);');
}
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss')});
is($p0, $p1, 'L1 same relfilenode on both nodes (shared-data premise)');

# ============================================================
# L2: plain-share leg -- read_scache OFF, one-shot image ships.
# ============================================================
my $xheld_before = pair_val($pair, 'gcs', 'cf_xheld_read_ship_count');

ok(write_retry($node0,
	"INSERT INTO xvss SELECT g, g FROM generate_series(1,$NROWS) g"),
	'L2 node0 seeded the table');

# Both nodes read every block; node1's reads of node0's X-held blocks are
# served as one-shot image ships (no S residency with read_scache off).
my $c0 = $node0->safe_psql('postgres', 'SELECT count(*) FROM xvss');
my $c1 = $node1->safe_psql('postgres', 'SELECT count(*) FROM xvss');
is($c0, "$NROWS", 'L2 node0 sees all rows');
is($c1, "$NROWS", 'L2 node1 sees all rows');
cmp_ok(pair_val($pair, 'gcs', 'cf_xheld_read_ship_count'), '>', $xheld_before,
	'L2 X-held reads really served via one-shot image ship');

my $att = write_retry($node0, 'UPDATE xvss SET v = v + 1');
ok($att, "L2 node0 whole-table UPDATE under plain-share (attempts=$att)");

# The wall must be gone: no cross-node-write-not-supported in either log.
my $wall0 = slurp_grep($node0, 'cross-node block write transfer not supported');
my $wall1 = slurp_grep($node1, 'cross-node block write transfer not supported');
is($wall0 + $wall1, 0, 'L2 (L442) zero FEATURE_NOT_SUPPORTED wall hits');

# ============================================================
# L3: revoke leg, forward -- read_scache ON, node0 writes over S copies.
# ============================================================
set_guc_reload($_, 'cluster.read_scache', 'on') for ($node0, $node1);

my $grants_before = grants($pair);

# Hint-bit discipline (harness survival rule, #119 boundary per header):
# the owner re-reads (commits hint bits into its cached pages) and
# CHECKPOINTs (so any serve path that reads shared storage also sees
# hinted pages), THEN the peer reads -- the peer then resolves no
# cross-node xids.  X-held blocks take the quiescent X->S downgrade, so
# BOTH nodes end S-resident (the owner keeps its downgraded copy cached).
$node0->safe_psql('postgres', 'SELECT count(*) FROM xvss');
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'SELECT count(*) FROM xvss');

$att = write_retry($node0, 'UPDATE xvss SET v = v + 1');
ok($att, "L3 node0 whole-table UPDATE against S copies (attempts=$att)");

my $grants_after = grants($pair);
cmp_ok($grants_after, '>', $grants_before,
	'L3 (L442) X-vs-S grant counters moved');

is(slurp_grep($node0, 'cross-node block write transfer not supported')
	+ slurp_grep($node1, 'cross-node block write transfer not supported'),
	0, 'L3 (L442) still zero FEATURE_NOT_SUPPORTED wall hits');

# ============================================================
# L4: revoke leg, reverse -- node1 writes, node0's S copies get revoked.
# ============================================================
# Dedicated node1-owned table: one writer per table keeps every writer
# resolving only its OWN xids (see the L7 header note); the peer only
# ever reads.  Same-DDL premise as xvss.
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'CREATE TABLE xvss_r (id int, v bigint);');
}
is($node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss_r')}),
	$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss_r')}),
	'L4 setup: reverse table coincident on both nodes');
ok(write_retry($node1,
	"INSERT INTO xvss_r SELECT g, g FROM generate_series(1,$NROWS) g"),
	'L4 node1 seeded the reverse table');

# Owner hint-bit pre-read + checkpoint, then node0 reads -> S residency.
$node1->safe_psql('postgres', 'SELECT count(*) FROM xvss_r');
$node1->safe_psql('postgres', 'CHECKPOINT');
is($node0->safe_psql('postgres', 'SELECT count(*) FROM xvss_r'), "$NROWS",
	'L4 node0 sees all reverse-table rows');

$att = write_retry($node1, 'UPDATE xvss_r SET v = v + 1');
ok($att, "L4 node1 whole-table UPDATE against node0 S copies (attempts=$att)");

cmp_ok(grants($pair), '>', $grants_after,
	'L4 (L442) grant counters moved again for the reverse direction');

# ============================================================
# L5: content integrity, owner-node materialized checks (L444).
# ============================================================
my $sum = $node0->safe_psql('postgres', 'SELECT sum(v) FROM xvss');
my $expect = 0;
$expect += $_ + 2 for (1 .. $NROWS);	# two whole-table +1 rounds (L2+L3)
is($sum, "$expect", 'L5 forward-table sum matches two committed update rounds');

$sum = $node1->safe_psql('postgres', 'SELECT sum(v) FROM xvss_r');
$expect = 0;
$expect += $_ + 1 for (1 .. $NROWS);	# one +1 round (L4)
is($sum, "$expect", 'L5 reverse-table sum matches its committed round (no lost update)');

# ============================================================
# L6: (b)-leg surface -- exposed, and zero on the normal workload.
# ============================================================
is(no_carrier($pair), 0,
	'L6 no-carrier fail-closed legs stayed 0 through the normal workload');

# D4 regression face (spec s4 L8): ANALYZE drives the extend-liveness
# engage path from a backend with a live peer; under cassert the old
# postmaster-only Assert in cluster_lms_wait_for_ready would SIGABRT the
# node.  safe_psql succeeding on both nodes proves the engage edge is
# backend-safe.
$node0->safe_psql('postgres', 'ANALYZE xvss');
$node1->safe_psql('postgres', 'ANALYZE xvss_r');
pass('L6 (D4) ANALYZE-driven engage path returned on both nodes, no TRAP');

# ============================================================
# L7: invalidate-ack stall on the holder -> bounded, retryable, no hang.
# ============================================================
# While armed, every handler pass skips the ack (dispatch re-raises
# skip_pending), so master-side retransmits stall too and the ack
# timeout must fire.  Dedicated node0-only table: see header.
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'CREATE TABLE xvss_l7 (id int, v bigint);');
}
is($node0->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss_l7')}),
	$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('xvss_l7')}),
	'L7 setup: stall table coincident on both nodes');
ok(write_retry($node0,
	"INSERT INTO xvss_l7 SELECT g, g FROM generate_series(1,$NROWS) g"),
	'L7 setup: node0 seeded the stall table');
# Owner hint-bit pre-read + checkpoint, then node1 reads -> X->S
# downgrade -> both nodes S-resident.
$node0->safe_psql('postgres', 'SELECT count(*) FROM xvss_l7');
$node0->safe_psql('postgres', 'CHECKPOINT');
$node1->safe_psql('postgres', 'SELECT count(*) FROM xvss_l7');

my $to_before = pair_val($pair, 'gcs', 'block_invalidate_timeout_count');
set_guc_reload($node1, 'cluster.injection_points',
	'cluster-gcs-block-invalidate-stall-ack:skip');

diag_counters($pair, 'pre-stall');
my ($rc, $out, $err) = $node0->psql('postgres', 'UPDATE xvss_l7 SET v = v + 1');
diag_counters($pair, 'post-stall');
# psql returning at all proves HG7 (bounded, no hang); the stalled round
# must surface the bounded fail-closed error, not silently succeed.
pass('L7 writer returned (no hang) under stalled invalidate ACKs');
isnt($rc, 0, 'L7 stalled round surfaced the bounded fail-closed error');
cmp_ok(pair_val($pair, 'gcs', 'block_invalidate_timeout_count'), '>', $to_before,
	'L7 (L442) timeout counter moved for the stalled round');

# Disarm: ':none' re-arms the point to NONE in every process (an empty
# list would leave the SKIP arm in place -- the assign hook only reverts
# WARNING-armed points), then clear the override.
set_guc_reload($node1, 'cluster.injection_points',
	'cluster-gcs-block-invalidate-stall-ack:none');
$node1->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');

$att = write_retry($node0, 'UPDATE xvss_l7 SET v = v + 1');
ok($att, "L7 same write succeeds after disarming the stall (attempts=$att)");

$pair->stop_pair;
done_testing();
