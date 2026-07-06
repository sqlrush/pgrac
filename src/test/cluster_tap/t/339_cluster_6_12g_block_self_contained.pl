#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 339_cluster_6_12g_block_self_contained.pl
#	  spec-6.12 wave g -- block self-containment (active-ITL migration +
#	  opportunistic commit cleanout) on a 2-node ClusterPair.
#
#	  The write-write collapse root (spec-5.2 D11): a block held X by node0
#	  with an UNCOMMITTED ITL slot on row2 cannot be X-transferred, so a
#	  node1 writer of a DIFFERENT row1 on the same block is falsely
#	  serialized (53R9H).  With cluster.block_self_contained = on the block
#	  migrates WITH the active ITL; row1 proceeds, only same-ROW conflicts
#	  serialize (cross-node TX enqueue, t/280).  8.A: the holder's later
#	  commit skips the stamp for the drifted block (D-g1) and every reader
#	  resolves the migrated ACTIVE slot through the TT authority (AD-006).
#
#	  L1  pair boots + shared table + GUC default off
#	  L2  collapse (GUC off baseline): node0 holds an uncommitted row2, a
#	      node1 write of row1 (DIFFERENT row) fails closed 53R9H (the D11
#	      false serialization)
#	  L3  collapse LIFTED (GUC on): the same node1 row1 write SUCCEEDS while
#	      node0's row2 stays uncommitted; the active-ITL transfer counter moves
#	  L3+ D-g1 stamp-skip: node0's ROLLBACK finishes its lock-only slot while
#	      the block is drifted on node1, so the commit cleanout SKIPS the stamp
#	      (verified via the local skip counter)
#	  L4  counter surface: 3 wave-g xnode_lever keys present on both nodes
#	  L6  (D-h1) Past Image kept on X-transfer + NEVER-SERVE outcomes audit
#	  L7  (D-h2) discard protocol: the new holder's write + CHECKPOINT proves
#	      the current durable (Q25-A dual trigger) -> the master retires the
#	      watermarks + fans out PI_DISCARD -> the old holder's PI is truly
#	      invalidated (h_pi_write_note + h_pi_discarded deltas)
#
#	  Substrate honesty (L373/L374 / spec-6.12 §3.5, same as wave-b): the hold
#	  uses a LOCK-ONLY hold (SELECT ... FOR UPDATE) -- like t/280 -- so no data
#	  UPDATE recycles the seed ITL slot and node1 resolves the block via the
#	  propagated TT hint.  Two 8.A properties are covered ELSEWHERE, unchanged
#	  by wave g, rather than re-proven here on a substrate that cannot carry
#	  them:  (a) same-ROW cross-node serialization is t/280 (wave g only changes
#	  the DIFFERENT-row path);  (b) the full DATA-visibility crash matrix (a
#	  drifted DATA writer resolved / rolled-back via TT under a natural
#	  cross-node read) hits the Stage-6 recycled-slot 53R97 boundary on the
#	  phantom-shared harness and belongs on the spec-6.0a substrate, exactly
#	  where wave-b's data-plane correctness and the true tax% measurement live.
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave g)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/339_cluster_6_12g_block_self_contained.pl
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

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# Poll for a committed value on an independent connection (authoritative
# end-to-end signal that a blocked writer woke + applied).
sub wait_for_val
{
	my ($node, $sql, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $v && $v eq $want;
		usleep(200_000);
	}
	return 0;
}

sub wait_for_wait_event
{
	my ($node, $qlike, $event, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql('postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq $event;
		usleep(200_000);
	}
	return 0;
}

# ============================================================
# L1: boot + shared table.  GUC armed from conf so a mid-test pair restart is
# not needed (the decision runs in the serve path).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'g612_selfcont',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.block_self_contained'), 'off',
	'L1 block_self_contained default off');

# Two rows guaranteed on ONE heap block.
$node0->safe_psql('postgres', 'CREATE TABLE g_t (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE g_t (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('g_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('g_t')});
is($p0, $p1, 'L1 g_t relfilepath coincidence holds');
ok(write_retry($node0, 'INSERT INTO g_t VALUES (1, 10), (2, 20)'), 'L1 seeded rows 1,2');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: collapse baseline (GUC off).  node0 holds an uncommitted row2; a node1
# write of the DIFFERENT row1 fails closed 53R9H (the D11 false serialization).
# ============================================================
{
	my $a = $node0->background_psql('postgres', on_error_stop => 0);
	$a->query_safe('BEGIN');
	# LOCK-ONLY hold: an ACTIVE lock-only ITL slot on the block (triggers the
	# D11 deferral) without recycling the seed's data slot (so node1 can still
	# resolve the block via the propagated TT hint).
	$a->query_safe('SELECT v FROM g_t WHERE id = 2 FOR UPDATE');

	# node1 writes row1 (different row).  Off baseline -> read-image deferral ->
	# 53R9H fail-closed.
	my ($rc, $out, $err) = $node1->psql('postgres', 'UPDATE g_t SET v = 11 WHERE id = 1');
	like($err, qr/53R9H|cross[- ]node write/i,
		'L2 GUC-off: node1 write of a DIFFERENT row fails closed 53R9H (D11 false serialization)');

	$a->query_safe('ROLLBACK');
	$a->quit;
}

# Arm the wave on both nodes for the remaining legs.
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.block_self_contained = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.block_self_contained'), 'on',
	'L3 block_self_contained armed on both nodes');

# ============================================================
# L3: collapse LIFTED (GUC on).  Same setup -> node1's row1 write SUCCEEDS
# while node0's row2 stays uncommitted.
# ============================================================
{
	my $g0 = state_val($node1, 'xnode_lever', 'g_active_itl_transfer_count')
		   + state_val($node0, 'xnode_lever', 'g_active_itl_transfer_count');
	my $sk0 = state_val($node0, 'xnode_lever', 'g_stamp_skipped_count')
			+ state_val($node1, 'xnode_lever', 'g_stamp_skipped_count');

	my $a = $node0->background_psql('postgres', on_error_stop => 0);
	$a->query_safe('BEGIN');
	$a->query_safe('SELECT v FROM g_t WHERE id = 2 FOR UPDATE');   # lock-only hold on row2

	my $ok = write_retry($node1, 'UPDATE g_t SET v = 11 WHERE id = 1');   # DIFFERENT row1
	ok($ok, 'L3 GUC-on: node1 write of a DIFFERENT row SUCCEEDS (collapse lifted)');

	# node0's ROLLBACK finishes the lock-only ITL slot; the block has drifted
	# to node1, so D-g1 skips the stamp (block not resident here).
	$a->query_safe('ROLLBACK');
	$a->quit;

	my $g1 = state_val($node1, 'xnode_lever', 'g_active_itl_transfer_count')
		   + state_val($node0, 'xnode_lever', 'g_active_itl_transfer_count');
	cmp_ok($g1, '>', $g0, 'L3 an active-ITL block transfer was counted');

	# D-g1: node0's ROLLBACK finished its lock-only slot while the block was
	# drifted on node1, so the commit cleanout SKIPPED the stamp here.  Checked
	# via the local counter (pg_cluster_state) -- no cross-node data read, which
	# on this phantom harness would hit the Stage-6 recycled-slot boundary.
	my $sk1 = state_val($node0, 'xnode_lever', 'g_stamp_skipped_count')
			+ state_val($node1, 'xnode_lever', 'g_stamp_skipped_count');
	cmp_ok($sk1, '>', $sk0, 'L3 D-g1 stamp-skip counter advanced (drifted-block cleanout skipped)');
}

# ============================================================
# L3b: master==holder TWIN SITE (spec-6.12g D-g2 completion).  L3's flow
# goes requester(master)->FORWARD->holder, exercising the holder-forward
# gate; when the block's static PCM home is the HOLDER node instead, the
# X request lands in the master==holder self-ship branch, whose active-ITL
# deferral originally missed the self-contained gate (a committed-but-
# unstamped Fast-Commit ITL then blocks a peer writer forever).  Six
# single-block tables spread the tag hash over both homes (P(no node0-
# mastered table) = 2^-6); the master==holder engagements are visible as
# block_x_self_ship_count growth (that counter only increments in the
# self-ship branch -- pre-fix those tables read-image-deferred instead
# and the peer write failed 53R9H).
# ============================================================
{
	my $ship0 = state_val($node0, 'gcs', 'block_x_self_ship_count')
			  + state_val($node1, 'gcs', 'block_x_self_ship_count');
	my $made = 0;
	for my $i (1 .. 6)
	{
		my $t = "g_m$i";
		$node0->safe_psql('postgres', "CREATE TABLE $t (id int, v int)");
		$node1->safe_psql('postgres', "CREATE TABLE $t (id int, v int)");
		my $q0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
		my $q1 = $node1->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
		next if $q0 ne $q1;    # coincidence drifted: skip this table
		next unless write_retry($node0, "INSERT INTO $t VALUES (1, 10), (2, 20)");
		next unless write_retry($node0, 'CHECKPOINT');
		$made++;

		my $a = $node0->background_psql('postgres', on_error_stop => 0);
		$a->query_safe('BEGIN');
		$a->query_safe("SELECT v FROM $t WHERE id = 2 FOR UPDATE");
		my $ok = write_retry($node1, "UPDATE $t SET v = 11 WHERE id = 1");
		ok($ok, "L3b $t: peer DIFFERENT-row write succeeds under active-ITL hold");
		$a->query_safe('ROLLBACK');
		$a->quit;
	}
	cmp_ok($made, '>=', 4, "L3b enough coincident probe tables ($made/6)");
	my $ship1 = state_val($node0, 'gcs', 'block_x_self_ship_count')
			  + state_val($node1, 'gcs', 'block_x_self_ship_count');
	cmp_ok($ship1, '>', $ship0,
		'L3b master==holder self-ship engaged under active ITL (twin-site gate)');
}

# ============================================================
# L4: counter surface on both nodes.
# ============================================================
for my $n ($node0, $node1)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='xnode_lever' AND key IN
		     ('g_active_itl_transfer_count','g_stamp_skipped_count',
		      'g_drift_resolved_via_tt_count')});
	is($rows, '3', 'L4 wave-g lever keys present (' . $n->name . ')');
}

# ============================================================
# L5: no-crash under concurrent DATA writes (Hardening v1.1 regression).
# The commit-time stamp path (cluster_bufmgr_lock_resident_for_stamp) must
# tolerate a block the committing backend ALREADY holds pinned.  A concurrent
# pgbench UPDATE burst exercises exactly that; the pre-fix code used
# PinBuffer_Locked (asserts a first-time pin) and crashed the whole
# postmaster.  Single-node burst isolates the stamp path (no cross-node read
# boundary).  Assertion: node0 stays UP after the burst.
# ============================================================
{
	my $PGBENCH  = $ENV{PGBENCH} // 'pgbench';
	my $conn     = "-h '" . $node0->host . "' -p " . $node0->port . ' postgres';
	my $sfile    = "/tmp/g339_burst_$$.sql";
	open my $sf, '>', $sfile;
	print $sf "\\set id random(1, 2)\nUPDATE g_t SET v = v + 1 WHERE id = :id;\n";
	close $sf;
	`$PGBENCH -n -N -c 4 -T 4 -f $sfile $conn 2>&1`;
	unlink $sfile;
	ok($node0->safe_psql('postgres', 'SELECT 1') eq '1',
		'L5 no-crash: node0 survives a concurrent DATA-write burst with '
		. 'block_self_contained on (PinBuffer_Locked regression)');
}

# ============================================================
# L6 (spec-6.12h D-h1): Past Image retention on X-transfer.
# With cluster.past_image on, the old holder's copy is KEPT as a PI
# (BM_TAG_VALID / !BM_VALID / buffer_type=PI) instead of dropped:
#   (a) the transfer ticks h_pi_kept_count on the shipping side;
#   (b) NEVER-SERVE hard invariant, proven on the data plane: after the
#       peer commits a new version, the old holder's read must return
#       the NEW value (a served PI byte-image would show the stale one);
#       the read itself overwrites the PI = the implicit discard.
# ============================================================
{
	for my $n ($node0, $node1)
	{
		$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.past_image = on');
		$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	usleep(1_000_000);
	is($node1->safe_psql('postgres', 'SHOW cluster.past_image'), 'on',
		'L6 cluster.past_image armed on both nodes');

	$node0->safe_psql('postgres', 'CREATE TABLE g_h (id int, v int)');
	$node1->safe_psql('postgres', 'CREATE TABLE g_h (id int, v int)');
	my $h0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('g_h')});
	my $h1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('g_h')});
	is($h0, $h1, 'L6 g_h relfilepath coincidence holds');
	ok(write_retry($node0, 'INSERT INTO g_h VALUES (1, 10), (2, 20)'),
		'L6 seeded');
	ok(write_retry($node0, 'CHECKPOINT'), 'L6 checkpoint');
	# hint-预读: stamp the seed ITL so the peer's read resolves locally.
	$node0->safe_psql('postgres', 'SELECT sum(v) FROM g_h');

	my $kept0 = state_val($node0, 'xnode_lever', 'h_pi_kept_count')
			  + state_val($node1, 'xnode_lever', 'h_pi_kept_count');

	# Peer write forces the X-transfer away from node0 -> PI conversion at
	# the shipping side (either drop-site twin, depending on the tag home).
	ok(write_retry($node1, 'UPDATE g_h SET v = 111 WHERE id = 1'),
		'L6 peer write moved the block');

	my $kept1 = state_val($node0, 'xnode_lever', 'h_pi_kept_count')
			  + state_val($node1, 'xnode_lever', 'h_pi_kept_count');
	cmp_ok($kept1, '>', $kept0, 'L6 a Past Image was kept on transfer');

	# Stamp the peer's committed version so the old holder's verification
	# read resolves it locally (pair-harness visibility recipe).
	ok(write_retry($node1, 'CHECKPOINT'), 'L6 peer checkpoint');
	$node1->safe_psql('postgres', 'SELECT sum(v) FROM g_h');

	# NEVER-SERVE proof, outcomes-audit shape (t/347 L2b precedent): every
	# node0 read outcome must be EITHER the peer's new value (111) OR a
	# documented fail-closed refusal — NEVER the PI's stale bytes (v=10 =
	# P0 false-visible).  On this phantom-shared harness the positive
	# read-through of a peer-written recycled-ITL version stays behind the
	# Stage-6 boundary (striping / runtime-visibility off here; t/346
	# covers the armed positive path), so persistent 53R97-family refusal
	# is a legal outcome; a served 10 is not.
	{
		my $deadline = time() + 20;
		my ($served_stale, $served_new, $bad_err) = (0, 0, '');
		while (time() < $deadline)
		{
			my $v = eval { $node0->safe_psql('postgres',
					'SELECT v FROM g_h WHERE id = 1'); };
			if (defined $v)
			{
				if    ($v eq '111') { $served_new = 1; last; }
				elsif ($v eq '10')  { $served_stale = 1; last; }
			}
			else
			{
				$bad_err = $1
				  if !$bad_err && $@ !~ /TT slot recycled|TT status unknown/
				  && $@ =~ /(ERROR:[^\n]*)/;
			}
			usleep(200_000);
		}
		is($served_stale, 0,
			'L6 NEVER-SERVE hard invariant: the stale PI bytes were never '
			  . 'returned to a query');
		is($bad_err, '',
			'L6 refusals stay in the documented fail-closed family');
		note($served_new
			  ? 'L6 positive read-through converged (111 served)'
			  : 'L6 positive read-through stayed fail-closed (phantom-harness '
			  . 'Stage-6 boundary; armed positive path covered by t/346)');
	}
}

# ============================================================
# L7 (spec-6.12h D-h2): PI-holder discard protocol (Q25-A dual trigger).
# Fresh table so the sequence is fully controlled:
#   1. seed on node0 (node0 = X holder), transfer to node1 (peer write)
#      -> node0 keeps a PI (D-h1) AND the master's pi_holders_bitmap
#         records node0 (local note when node0 masters the tag; the
#         PI_KEPT ack/note ride when node1 does — the leg is
#         master-position agnostic on purpose);
#   2. CHECKPOINT on node1 = the new holder's CURRENT copy written +
#      fsync-proven durable (the write-note ring sealed by the
#      ProcessSyncRequests bracket);
#   3. node1's LMON drains the note to the master -> watermarks retired
#      -> PI_DISCARD directed at node0 -> node0's PI buffer is TRULY
#      invalidated: sum(h_pi_discarded_count) rises.  No reads touch the
#      table on node0 in between, so the tick cannot be a miss-path
#      artifact (an implicit-discard reread would leave no PI to drop
#      and tick h_pi_discard_miss instead).
# ============================================================
{
	$node0->safe_psql('postgres', 'CREATE TABLE h_d (id int, v int)');
	$node1->safe_psql('postgres', 'CREATE TABLE h_d (id int, v int)');
	is( $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('h_d')}),
		$node1->safe_psql('postgres', q{SELECT pg_relation_filepath('h_d')}),
		'L7 h_d relfilepath coincidence holds');
	ok(write_retry($node0, 'INSERT INTO h_d VALUES (1, 10), (2, 20)'),
		'L7 seeded');
	ok(write_retry($node0, 'CHECKPOINT'), 'L7 seed checkpoint');
	$node0->safe_psql('postgres', 'SELECT sum(v) FROM h_d');

	my $kept0    = state_val($node0, 'xnode_lever', 'h_pi_kept_count')
				 + state_val($node1, 'xnode_lever', 'h_pi_kept_count');
	my $note0    = state_val($node0, 'xnode_lever', 'h_pi_write_note_count')
				 + state_val($node1, 'xnode_lever', 'h_pi_write_note_count');
	my $discard0 = state_val($node0, 'xnode_lever', 'h_pi_discarded_count')
				 + state_val($node1, 'xnode_lever', 'h_pi_discarded_count');

	# Transfer: node1 takes X; node0's copy becomes the PI to be discarded.
	ok(write_retry($node1, 'UPDATE h_d SET v = 222 WHERE id = 1'),
		'L7 peer write moved the block');
	my $kept1 = state_val($node0, 'xnode_lever', 'h_pi_kept_count')
			  + state_val($node1, 'xnode_lever', 'h_pi_kept_count');
	cmp_ok($kept1, '>', $kept0, 'L7 a Past Image was kept on transfer');

	# Q25-A: the new holder writes its CURRENT copy durable.  FlushBuffer
	# notes the write (face 1), the checkpoint's sync bracket seals it
	# (face 2), the LMON drain routes it to the master.
	ok(write_retry($node1, 'CHECKPOINT'), 'L7 new-holder checkpoint');

	my $deadline = time() + 30;
	my ($note1, $discard1) = ($note0, $discard0);
	while (time() < $deadline)
	{
		$note1    = state_val($node0, 'xnode_lever', 'h_pi_write_note_count')
				  + state_val($node1, 'xnode_lever', 'h_pi_write_note_count');
		$discard1 = state_val($node0, 'xnode_lever', 'h_pi_discarded_count')
				  + state_val($node1, 'xnode_lever', 'h_pi_discarded_count');
		last if $discard1 > $discard0;
		usleep(300_000);
	}
	for my $pair_n ([ 'node0', $node0 ], [ 'node1', $node1 ])
	{
		my ($nm, $n) = @$pair_n;
		note("L7 diag $nm: "
			  . join(' ',
				map { "$_=" . state_val($n, 'xnode_lever', "h_pi_$_") }
				  qw(kept_count write_note_count note_overflow_count
					 discard_notify_count discarded_count discard_miss_count)));
		note("L7 diag $nm gcs: retire="
			  . state_val($n, 'gcs', 'pi_watermark_retire_count')
			  . " advance=" . state_val($n, 'gcs', 'pi_watermark_advance_count')
			  . " pcm_pi_holders=" . state_val($n, 'pcm', 'pi_holders_total_count'));
	}
	cmp_ok($note1, '>', $note0,
		'L7 face 1: the tracked-block write was noted (h_pi_write_note)');
	# Directive issuance is proven BY the discarded tick below:
	# cluster_bufmgr_discard_pi_block has exactly two callers, both driven
	# by a master PI_DISCARD directive (local apply or the INVALIDATE ride),
	# so a rising h_pi_discarded implies the notify hop fired.  A strict
	# h_pi_discard_notify delta is deliberately NOT asserted: earlier legs'
	# tags carry legacy HC58 downgrade bits, so any checkpoint can retire
	# them in the background and race the baseline sample.
	cmp_ok($discard1, '>', $discard0,
		'L7 the PI was TRULY invalidated at its holder (h_pi_discarded)');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
