#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 401_gcs_master_direct_storage_fallback_2node.pl
#	  branch-1 (S3 step-2 forensics) — master-direct STALE ship rescue.
#
#	  The S3 step-2 stop-capture proved the "branch 1" verdict: a master
#	  that retained a stale resident copy (a kept Past Image) served it as
#	  the master-direct grant payload; the SCN self-check fail-closed
#	  DENIED_LOST_WRITE and every hit cost the requester a 53R93 client
#	  abort — even though shared storage already held a version covering
#	  the watermark.  The rescue: when the master-direct self-check says
#	  STALE, probe the shared-storage page pd_block_scn; if it covers the
#	  authoritative pi_watermark_scn, convert the reply to
#	  GRANTED_STORAGE_FALLBACK (ship no image; page_lsn carries the
#	  watermark — the state=N grant contract) so the requester
#	  proves/refreshes its copy via cluster_gcs_block_fallback_verify_
#	  refresh.  Storage unprovable → keep DENIED_LOST_WRITE (Rule 8.A).
#
#	  L1  ClusterPair startup baseline (both postmasters healthy)
#	  L2  NEW counter lost_write_master_direct_storage_fallback_count
#	      present (= 0) on both nodes;  gcs category has 119 keys
#	  L3  GREEN leg: warmed cross-node heap block + CHECKPOINT (storage
#	      current), then cluster-gcs-block-stale-ship-resident forces a
#	      STALE master-direct ship → rescue converts it to
#	      GRANTED_STORAGE_FALLBACK: the NEW counter grows, the producer
#	      LOG carries BOTH the STALE verdict on the master-direct ship
#	      and the rescue line (path-fire proof: the inject exists ONLY at
#	      the master-direct self-check), and the write completes.
#	  L4  fail-closed leg: same STALE ship but cluster-gcs-block-master-
#	      direct-fallback-storage-stale forces the storage probe to
#	      InvalidScn → rescue REFUSED → DENIED_LOST_WRITE → requester
#	      53R93;  lost_write_detected_count grows;  the rescue counter
#	      does NOT grow.
#	  L5  disarm + restart: cluster stays usable (no corruption).
#
# Spec: spec-2.41-cross-node-block-version-scn-lost-write.md §2.6 +
#	S3 step-2 forensics bundle (RACvsRAC bench/results/
#	s3-step2-forensics-20260714-112856) branch-1 verdict.
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


sub gcs_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup (same true-2-node harness as t/116).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'gcs_md_storage_fallback',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		# The master-direct GRANTED serve for a remote READ against a
		# master-local X-held quiescent block runs through the spec-6.12a
		# X->S self-downgrade (scache_downgraded_fall_through); without it
		# the read takes the one-shot READ_IMAGE path and the master-direct
		# self-check never fires in a 2-node pair.
		'cluster.read_scache = on',
	]);
$pair->start_pair;

usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: NEW counter surface + gcs key count.
# ============================================================
for my $node ($pair->node0, $pair->node1)
{
	my $name = $node->name;

	is($node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state
			   WHERE category='gcs'
			     AND key='lost_write_master_direct_storage_fallback_count'}),
		'1', "L2 $name NEW rescue counter key present");
	is(gcs_int($node, 'lost_write_master_direct_storage_fallback_count'), 0,
		"L2 $name rescue counter = 0 at startup");
}

is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '119',
   'L2 pg_cluster_state.gcs category has 119 keys (convert-queue image/holder observability +7)');


# ============================================================
# Shared driving machinery (t/116 pattern: mastership is fixed by tag
# hash and unknowable a priori — drive both directions each round and
# assert on the SUM of both nodes' counters).
# ============================================================

# Readiness for globalized DDL.
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'peers connected (voting quorum ready for globalized DDL)');

# Identical DDL order on both fresh nodes → identical relfilenode → the
# heap block routes to the same shared-storage file (t/116 seq pattern).
$pair->node0->safe_psql('postgres', 'CREATE TABLE lw_b1 (v int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE lw_b1 (v int)');
$pair->node0->safe_psql('postgres', 'INSERT INTO lw_b1 VALUES (0)');

# Retrying SQL: a fresh 2-node TAP cluster runs at LOW xids, so a
# cross-node scan of a not-yet-hinted foreign tuple can transiently hit
# "cluster TT slot recycled" (retryable per its own errhint; a TAP-only
# low-xid artifact, see the serve-stall lane).  The own-xid read after
# every write below (t/394 hint recipe) makes this rare; retry bounds it.
sub retry_safe_sql
{
	my ($node, $sql) = @_;
	my ($rc, $out, $err);
	for my $attempt (1 .. 5)
	{
		($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 30);
		return $out if $rc == 0;
		last unless defined $err && $err =~ /TT slot recycled/i;
		usleep(200_000);
	}
	die "retry_safe_sql exhausted on '$sql': " . ($err // 'unknown');
}

# Warm up (NO inject): cross-node UPDATE ping-pong installs the heap page
# on each node with a valid pd_block_scn and advances the master's
# pi_watermark_scn (grant_x / take_x_after_transfer feeds), so the forced
# (watermark-1) shipped page trips the STALE branch rather than SKIP.
# The writer VACUUMs its own table right after every write (t/394
# ITL-quiesce recipe): own-xid ITL cleanout + dead-version prune, so the
# peer's next scan never has to resolve a foreign ITL xid through the TT
# (a fresh low-xid TAP cluster hits "TT slot recycled" otherwise).
sub write_and_quiesce
{
	my ($node) = @_;
	retry_safe_sql($node, 'UPDATE lw_b1 SET v = v + 1');
	retry_safe_sql($node, 'VACUUM lw_b1');
	return;
}
$pair->node0->safe_psql('postgres', 'VACUUM lw_b1');
for (1 .. 4)
{
	write_and_quiesce($pair->node0);
	write_and_quiesce($pair->node1);
}

sub arm_points
{
	my ($val) = @_;
	for my $node ($pair->node0, $pair->node1)
	{
		$node->safe_psql('postgres',
			"ALTER SYSTEM SET cluster.injection_points = '$val'");
		$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	return;
}

sub checkpoint_both
{
	# Flush every dirty block so the shared-storage version covers the
	# watermark for whatever block ships next (the rescue precondition).
	$pair->node0->safe_psql('postgres', 'CHECKPOINT');
	$pair->node1->safe_psql('postgres', 'CHECKPOINT');
	return;
}

my $saw_53r93 = 0;
my $err_53r93 = '';
sub note_53r93
{
	my ($err) = @_;
	if (defined $err && $err =~ /53R93|lost write detected|stale storage-fallback/i)
	{
		$saw_53r93 = 1;
		$err_53r93 = $err;
	}
	return;
}

# Writer leg: take X via UPDATE, then quiesce (commit + ITL cleanout) so
# the block is servable; both statements tolerant.
sub try_write
{
	my ($node) = @_;
	my ($rc, $out, $err) =
		$node->psql('postgres', 'UPDATE lw_b1 SET v = v + 1', timeout => 30);
	note_53r93($err);
	($rc, $out, $err) = $node->psql('postgres', 'VACUUM lw_b1', timeout => 30);
	note_53r93($err);
	return;
}

# Reader leg: a remote N->S read against the writer's X-held quiescent
# block — the master-direct GRANTED trigger (X->S self-downgrade).
sub try_read
{
	my ($node) = @_;
	my ($rc, $out, $err) =
		$node->psql('postgres', 'SELECT count(*) FROM lw_b1', timeout => 30);
	note_53r93($err);
	return;
}

sub sum_counter
{
	my ($key) = @_;
	return gcs_int($pair->node0, $key) + gcs_int($pair->node1, $key);
}


# ============================================================
# L3: GREEN leg — STALE master-direct ship rescued via storage.
# ============================================================
my $resc_before = sum_counter('lost_write_master_direct_storage_fallback_count');
my $det_l3_before = sum_counter('lost_write_detected_count');

# Mastership is hash-fixed and unknowable a priori: drive BOTH
# writer/reader assignments each round — only the direction where the
# WRITER masters the tag serves master-direct (the other direction takes
# the holder-forward read).  Writer takes X + quiesces + CHECKPOINT
# (storage current — the rescue precondition), then the peer's remote
# read triggers the X->S downgrade GRANTED with the forced-stale ship.
my $resc_after = $resc_before;
for my $round (1 .. 8)
{
	last if $resc_after > $resc_before;
	try_write($pair->node0);
	checkpoint_both();
	arm_points('cluster-gcs-block-stale-ship-resident:skip');
	try_read($pair->node1);
	try_write($pair->node1);
	checkpoint_both();
	arm_points('cluster-gcs-block-stale-ship-resident:skip');
	try_read($pair->node0);
	usleep(500_000);
	$resc_after = sum_counter('lost_write_master_direct_storage_fallback_count');
}

diag("L3 rescue counter $resc_before->$resc_after; saw_53r93=$saw_53r93");

# Path diagnosis: which serve path did the rounds take, and did the
# resident inject site ever fire?
for my $node ($pair->node0, $pair->node1)
{
	my $inj = $node->safe_psql('postgres',
		q{SELECT key || '=' || value FROM pg_cluster_state
		   WHERE category='inject' AND key LIKE '%stale-ship%'
		   ORDER BY key});
	$inj =~ s/\n/ /g;
	diag($node->name . " inject: $inj");
	my $gcs = $node->safe_psql('postgres',
		q{SELECT key || '=' || value FROM pg_cluster_state
		   WHERE category='gcs'
		     AND (key LIKE '%lost_write%' OR key LIKE '%fallback%'
		          OR key LIKE '%read_image%' OR key LIKE '%ship%'
		          OR key LIKE '%grant%')
		     AND value <> '0'
		   ORDER BY key});
	$gcs =~ s/\n/ /g;
	diag($node->name . " gcs nonzero: $gcs");
}

cmp_ok($resc_after, '>', $resc_before,
	'L3 STALE master-direct ship rescued: '
	. "lost_write_master_direct_storage_fallback_count grew ($resc_before → $resc_after)");

# Path-fire proof (t/398 lesson): the producer LOG must show the STALE
# verdict ON THE MASTER-DIRECT SHIP — the inject exists only at that
# self-check, so a green here cannot come from holder-forward.
my $stale_line = '';
my $rescue_line = '';
for my $node ($pair->node0, $pair->node1)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	$stale_line = $1
		if $stale_line eq ''
		&& $log =~ /(cluster_gcs_block: lost-write verdict STALE on master-direct ship[^\n]*)/;
	$rescue_line = $1
		if $rescue_line eq ''
		&& $log =~ /(cluster_gcs_block: master-direct stale ship rescued[^\n]*)/;
}
isnt($stale_line, '',
	'L3b producer LOG shows the STALE verdict on the master-direct ship (path fired)');
isnt($rescue_line, '',
	'L3c producer LOG shows the GRANTED_STORAGE_FALLBACK rescue line');
like($rescue_line, qr/storage pd_block_scn=\d+/,
	'L3c2 rescue LOG carries the storage-probe pd_block_scn')
	if $rescue_line ne '';

# The rescue leg must not have produced fail-closed denials of its own
# beyond what later legs will drive; record for the L4 delta instead.
my $det_l3_after = sum_counter('lost_write_detected_count');
diag("L3 lost_write_detected_count $det_l3_before->$det_l3_after");

# Cluster stays usable right after the rescue (write completes cleanly).
arm_points('');
is(retry_safe_sql($pair->node0, 'UPDATE lw_b1 SET v = v + 1 RETURNING v > 0'),
	't', 'L3d write path stays usable after the rescue');

# Hard gates for the GREEN-leg claims (branch-1 review P1-3): the rescued
# leg must be 53R93-free, and both nodes must agree on the data end value
# — a rescue that let the requester consume a stale storage version would
# diverge the two readbacks.
is($saw_53r93, 0, 'L3e rescued leg surfaced zero 53R93 client aborts')
	or diag("53R93 err: $err_53r93");
# Quiesce first (owner-side ITL cleanout, the t/394 recipe used by the
# warm-up above): without it the cross-node readback can trip the known
# low-xid "TT slot recycled" TAP artifact past the retry budget.
retry_safe_sql($pair->node0, 'VACUUM lw_b1');
my $v_n0 = retry_safe_sql($pair->node0, 'SELECT v FROM lw_b1');
my $v_n1 = retry_safe_sql($pair->node1, 'SELECT v FROM lw_b1');
is($v_n1, $v_n0, "L3f cross-node readback agrees after rescue (v=$v_n0)");
cmp_ok($v_n0, '>=', 9,
	'L3g end value covers the 8 warm-up increments plus L3d');


# ============================================================
# L4: fail-closed leg — storage probe unprovable → DENIED_LOST_WRITE.
# ============================================================
my $det_before = sum_counter('lost_write_detected_count');
my $resc_l4_before = sum_counter('lost_write_master_direct_storage_fallback_count');
$saw_53r93 = 0;
$err_53r93 = '';

my $det_after = $det_before;
for my $round (1 .. 8)
{
	last if $det_after > $det_before;
	try_write($pair->node0);
	checkpoint_both();
	arm_points('cluster-gcs-block-stale-ship-resident:skip,'
			. 'cluster-gcs-block-master-direct-fallback-storage-stale:skip');
	try_read($pair->node1);
	try_write($pair->node1);
	checkpoint_both();
	arm_points('cluster-gcs-block-stale-ship-resident:skip,'
			. 'cluster-gcs-block-master-direct-fallback-storage-stale:skip');
	try_read($pair->node0);
	usleep(500_000);
	$det_after = sum_counter('lost_write_detected_count');
}

diag("L4 detected $det_before->$det_after; saw_53r93=$saw_53r93");

cmp_ok($det_after, '>', $det_before,
	'L4 storage-unprovable rescue refused: lost_write_detected_count grew '
	. "($det_before → $det_after) — fail-closed retained");
ok($saw_53r93,
	'L4b requester saw 53R93 / lost-write (rescue must not mask fail-closed)');
is(sum_counter('lost_write_master_direct_storage_fallback_count'), $resc_l4_before,
	'L4c rescue counter did NOT grow while storage was unprovable');


# ============================================================
# L5: disarm + restart, cluster stays usable.
# ============================================================
arm_points('');
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L5 cluster restarted injection-free after both legs');
is(retry_safe_sql($pair->node0, 'SELECT count(*) FROM lw_b1'),
	'1', 'L5 table still readable after both legs (no corruption)');


done_testing();
