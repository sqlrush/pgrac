#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 365_cluster_7_1a_write_write.pl
#    spec-7.1a D7 -- cross-instance write-write coordination e2e on a
#    2-node ClusterPair, plus the 2026-07-09 review hardening legs (C1b
#    verdict routing / in-doubt boundary / moved-partitions no-bloat).
#
#    Legs (every leg tags its substrate with "# layer:"):
#      L1  # layer: live -- pair boots; cluster.crossnode_write_write is
#          default OFF; seed rows flushed to shared storage.
#      L2  # layer: live -- today's write-write boundary, GUC off: a node1
#          write against an IN-FLIGHT remote writer fails closed (53R97
#          family, retryable) at the visibility judge; after the remote
#          writer commits, the node1 RETRY applies on top of the committed
#          value (no lost update, no silent chain while undecided).
#      L3  # layer: live -- 8A leg, GUC ON: arming crossnode_write_write
#          must NOT invent an unsound resolution -- the same in-flight
#          conflict still fails closed (the D0 chain only ever consumes
#          PROVEN terminal outcomes), and the post-commit retry converges
#          identically.
#      L4  # layer: live -- C1b verdict routing (review finding #1, the
#          P0 red->green leg): the recycled-ITL-slot read (t/346 CP3
#          shape: fetch HIT whose shipped TT block carries a COMMITTED
#          stamp) must NOT conclude committed from the stamp alone -- a
#          durable COMMITTED stamp lands at 2PC pre-commit and is in-doubt
#          evidence until the ORIGIN's C1b CLOG cross-check.  Assert the
#          read still succeeds AND cr.rtvis_verdict_wire_count moved (the
#          stamp was finalized via the origin verdict leg, never consumed
#          as a verdict by the fetch fast leg).
#      L5  # layer: live -- moved-partitions no-bloat invariant (review
#          finding #2 face): a remote cross-partition UPDATE (in-flight,
#          then committed) never grows the OLD partition on the peer --
#          pg_relation_size(pt_a) is unchanged across conflict + retry
#          convergence, and the retry lands on the moved row.
#      L6  # layer: live -- 2PC in-doubt hard boundary (the review #1
#          equivalent assertion): while 'd7a' is PREPARED it is in-doubt
#          BY DEFINITION -- node1 reads and writes of its touched row must
#          fail closed / never serve the uncommitted value; after COMMIT
#          PREPARED (the same durable COMMITTED stamp that would be
#          in-doubt in the crash window, now finalized by the commit
#          record) node1's read heals to the 2PC value through the C1b
#          verdict discipline.  The convert queue then transfers X for a
#          peer WRITE on that block; a second owner-side write proves the
#          resulting value chain contains both increments.
#
#    KNOWN-BLOCKED (honest, 规则 18 -- not SKIPed, not faked green):
#      - D0 writer-chain gain legs (blocked UPDATE wakes onto TM_Ok /
#        TM_Updated / TM_Deleted and chains) and the moved-partitions
#        PROBE guard leg: entering the cross-node TX enqueue wait on a
#        WRITER xmax requires the peer to judge an IN-FLIGHT remote
#        writer as in-progress, and on this branch that resolution is
#        fail-closed by design (resolve_live_overlay_miss_via_origin
#        pulls only TERMINAL verdicts; prepared/in-progress stay
#        fail-closed -- cluster_visibility_resolve.c).  In-progress
#        interread is the spec-7.1 forward-interread serve lane, not
#        landed on this branch; until it lands, the writer-wait path is
#        exercised at the unit layer (test_cluster_writer_chain.c) and
#        the lock-only wait path by t/280.  L2/L3/L5 pin the fail-closed
#        boundary that stands in its place.
#      - crash-in-window in-doubt e2e (COMMIT PREPARED stamps the durable
#        TT slot, owner crashes BEFORE the commit record, xact resolves
#        as ROLLBACK PREPARED, survivor must fail closed): needs a fault
#        injection point between the 2PC prefinish stamp and
#        RecordTransactionCommitPrepared; none exists on this branch and
#        adding one modifies the 2PC commit path (integration-owner lane
#        call).  The deterministic equivalents shipped here are L4 (the
#        stamp is never consumed as a verdict) and L6 (a genuinely
#        in-doubt xact is never served).
#      - below-floor chain-walk leg (review finding #3): the harness
#        boots with xid striping active, so pre-striping (below-floor)
#        local update chains cannot be manufactured here; the predicate
#        divergence is pinned by test_cluster_xid_stripe.c
#        (foreign_class_cheap over-reports below-floor xids,
#        provably_foreign never does).
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off +
#    xid striping (spec-6.15 D4 origin derivation) + 2PC slots.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/365_cluster_7_1a_write_write.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-7.1a-cross-instance-write-write-mvcc-coordination.md (D7)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
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

# Retry a statement until it succeeds (the cluster fail-closed judges are
# retryable by contract -- t/282 L3 idiom).  Returns 1 on success.
sub retry_until_ok
{
	my ($node, $sql, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return 1 if $rc == 0;
		usleep(300_000);
	}
	return 0;
}

# Poll a scalar SQL result until it equals $want (errors retried).
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

# ============================================================
# L1  # layer: live -- boot, defaults, seed.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ww71a',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'max_prepared_transactions = 10',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		# CI shards run in parallel; widen the misscount so a starved
		# heartbeat does not falsely kill the lock-holding peer (t/280 note).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		# spec-6.15 D4: xid-derived origin needs a striped value space;
		# striping requires online_join (boot contract FATALs otherwise).
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		# 6.12i substrate, NOT the GUC under test: under striping the
		# stride-16 congruence recycles a remote xact's TT slot almost
		# immediately, so every remote xmin/xmax resolution on this pair
		# needs the runtime-visibility path from the first read on.
		'cluster.crossnode_runtime_visibility = on',
	]);
$pair->start_pair;
usleep(2_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.crossnode_write_write'), 'off',
	'L1 crossnode_write_write default off');
is($node0->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'), 'on',
	'L1 crossnode_runtime_visibility armed by conf (striped-read substrate)');

for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'CREATE TABLE ww_t (id int, ctr int)');
}
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('ww_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('ww_t')});
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all => "same-DDL relfilepath coincidence does not hold (n0=$p0 n1=$p1)";
}
ok(write_retry($node0, 'INSERT INTO ww_t VALUES (1, 100), (2, 100)'), 'L1 seed rows inserted');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 seed flushed to shared storage');
ok(wait_for_val($node1, 'SELECT ctr FROM ww_t WHERE id = 1', '100', 20),
	'L1 node1 reads the seed row (remote committed xmin resolvable)');

# ============================================================
# L2  # layer: live -- write-write boundary, GUC off: in-flight conflict
# fails closed; the post-commit retry applies on top (no lost update).
# (The blocked-wait chain gain is KNOWN-BLOCKED -- see header.)
# ============================================================
{
	my $h0 = $node0->background_psql('postgres', on_error_die => 1);
	$h0->query_safe('BEGIN');
	$h0->query_safe('UPDATE ww_t SET ctr = 200 WHERE id = 1');

	# The in-flight remote writer is undecided: node1's conflicting write
	# must fail closed (retryable), never wait on a guess, never chain.
	my ($rc, $out, $err) = $node1->psql('postgres', 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 1');
	isnt($rc, 0, 'L2 node1 write vs in-flight remote writer errors (fail-closed)');
	like($err, qr/cluster TT (status unknown|slot recycled)/,
		'L2 the failure is the cluster fail-closed judge, not a native guess');

	$h0->query_safe('COMMIT');
	$h0->quit;

	ok(retry_until_ok($node1, 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 1', 30),
		'L2 node1 retry succeeds once the remote writer is terminal');
	is($node0->safe_psql('postgres', 'SELECT ctr FROM ww_t WHERE id = 1'), '201',
		'L2 node1 applied onto the committed 200 (no lost update)');
}

# ============================================================
# L3  # layer: live -- 8A leg: arming crossnode_write_write must not
# invent a resolution for an UNDECIDED writer (D0 consumes only proven
# terminal outcomes); the same conflict still fails closed, the same
# retry converges.
# ============================================================
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_write_write = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_write_write'), 'on',
	'L3 crossnode_write_write armed on both nodes');

{
	my $h0 = $node0->background_psql('postgres', on_error_die => 1);
	$h0->query_safe('BEGIN');
	$h0->query_safe('UPDATE ww_t SET ctr = 300 WHERE id = 1');

	my ($rc, $out, $err) = $node1->psql('postgres', 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 1');
	isnt($rc, 0, 'L3 GUC on: in-flight conflict still fails closed (no unsound chain)');
	like($err,
		qr/cluster TT (?:status unknown|slot recycled)|cluster PCM ownership transition conflicts with an active reservation/,
		'L3 the failure is still an explicit cluster fail-closed judge');

	$h0->query_safe('COMMIT');
	$h0->quit;

	ok(retry_until_ok($node1, 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 1', 30),
		'L3 node1 retry succeeds on the terminal writer');
	is($node0->safe_psql('postgres', 'SELECT ctr FROM ww_t WHERE id = 1'), '301',
		'L3 node1 applied onto the committed 300 (no lost update)');
}

# ============================================================
# L4  # layer: live -- C1b verdict routing (review finding #1, P0).
# The t/346 CP3 shape: one INSERT xact seeds a single block; >8 committed
# UPDATE xacts recycle its ITL slot; node1's read then resolves the seed
# xid through the runtime-visibility path with a fetch HIT whose shipped
# TT block carries a COMMITTED stamp.  That stamp is EVIDENCE (durable
# stamps land at 2PC pre-commit; stamped-then-crashed is in-doubt), so
# the resolution MUST ride the origin verdict leg (C1b CLOG cross-check)
# -- never conclude from the stamp alone.
# ============================================================
{
	$node0->safe_psql('postgres', 'CREATE TABLE rv_t (id int, v int)');
	$node1->safe_psql('postgres', 'CREATE TABLE rv_t (id int, v int)');
	ok(write_retry($node0, 'INSERT INTO rv_t SELECT g, g * 10 FROM generate_series(1, 12) g'),
		'L4 seed xact inserted 12 rows');
	for my $id (3 .. 12)
	{
		ok(write_retry($node0, "UPDATE rv_t SET v = v + 1 WHERE id = $id"),
			"L4 recycler update xact (id=$id) committed");
	}
	is($node0->safe_psql('postgres',
			q{SELECT count(DISTINCT (ctid::text::point)[0]::int) FROM rv_t}),
		'1', 'L4 all live rows stayed on ONE heap block');
	ok(write_retry($node0, 'CHECKPOINT'), 'L4 checkpoint');

	my $vwire0 = state_val($node1, 'cr', 'rtvis_verdict_wire_count');
	my $fwire0 = state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count');
	my $rc0    = state_val($node1, 'cr', 'rtvis_resolve_committed_count');

	ok(wait_for_val($node1, 'SELECT count(*) FROM rv_t', '12', 20),
		'L4 node1 reads the recycled-slot block (12 rows)');

	cmp_ok(state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count'), '>', $fwire0,
		'L4 fetch leg still rides the wire first (fast-path face intact)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_verdict_wire_count'), '>', $vwire0,
		'L4 C1b: the COMMITTED stamp was finalized via the ORIGIN VERDICT leg '
		  . '(never consumed as a verdict by the fetch fast leg)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_resolve_committed_count'), '>', $rc0,
		'L4 resolve_committed still counted once per resolution (no lost counting)');
}

# ============================================================
# L5  # layer: live -- moved-partitions no-bloat invariant (review
# finding #2 face; the PROBE guard leg itself is KNOWN-BLOCKED -- header).
# A remote cross-partition UPDATE (in-flight, then committed) must never
# grow the OLD partition on the peer, and the peer's retry converges on
# the moved row.
# ============================================================
{
	for my $n ($node0, $node1)
	{
		$n->safe_psql('postgres',
			'CREATE TABLE pt (id int, k int, v int) PARTITION BY LIST (k);'
			  . 'CREATE TABLE pt_a PARTITION OF pt FOR VALUES IN (1);'
			  . 'CREATE TABLE pt_b PARTITION OF pt FOR VALUES IN (5);');
	}
	ok(write_retry($node0, 'INSERT INTO pt VALUES (1, 1, 100)'), 'L5 row seeded in pt_a');
	ok(write_retry($node0, 'CHECKPOINT'), 'L5 seed flushed');
	ok(wait_for_val($node1, 'SELECT v FROM pt WHERE id = 1', '100', 20),
		'L5 node1 reads the seed row in pt_a');

	my $size0 = $node1->safe_psql('postgres', q{SELECT pg_relation_size('pt_a')});

	my $h0 = $node0->background_psql('postgres', on_error_die => 1);
	$h0->query_safe('BEGIN');
	$h0->query_safe('UPDATE pt SET k = 5 WHERE id = 1');    # moves pt_a -> pt_b

	my ($rc, $out, $err) = $node1->psql('postgres', 'UPDATE pt SET v = v + 1 WHERE id = 1');
	isnt($rc, 0, 'L5 node1 write vs the in-flight cross-partition mover fails closed');

	$h0->query_safe('COMMIT');
	$h0->quit;

	ok(retry_until_ok($node1, 'UPDATE pt SET v = v + 1 WHERE id = 1', 30),
		'L5 node1 retry converges on the moved row');
	is($node0->safe_psql('postgres', 'SELECT k, v FROM pt WHERE id = 1'), '5|101',
		'L5 the moved row carries the +1 in pt_b (no lost update across the move)');
	is($node1->safe_psql('postgres', q{SELECT pg_relation_size('pt_a')}), $size0,
		'L5 old partition NOT extended on the peer across conflict + retry (no bloat)');
}

# ============================================================
# L6  # layer: live -- 2PC in-doubt hard boundary (review #1 equivalent
# assertion; see header for the crash-in-window KNOWN-BLOCKED note).
# ============================================================
{
	my $h0 = $node0->background_psql('postgres', on_error_die => 1);
	$h0->query_safe('BEGIN');
	$h0->query_safe('UPDATE ww_t SET ctr = 500 WHERE id = 2');
	$h0->query_safe("PREPARE TRANSACTION 'd7a'");
	$h0->quit;

	# L6a: while PREPARED the xact is in-doubt BY DEFINITION.  node1 must
	# fail closed on its touched row -- and above all must NEVER serve the
	# uncommitted 500 (false-visible) nor apply a write over it
	# (false-commit chain).
	my $saw_500     = 0;
	my $failclosed  = 0;
	for my $try (1 .. 10)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT ctr FROM ww_t WHERE id = 2');
		$saw_500 = 1 if defined $out && $out =~ /^500$/m;
		$failclosed++ if $rc != 0 && $err =~ /cluster TT (status unknown|slot recycled)/;
		usleep(200_000);
	}
	is($saw_500, 0, 'L6a the in-doubt (PREPARED) value 500 was NEVER served on node1');
	cmp_ok($failclosed, '>', 0, 'L6a in-doubt reads failed closed (cluster judge)');

	my ($wrc, $wout, $werr)
		= $node1->psql('postgres', 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 2');
	isnt($wrc, 0, 'L6b a write against the in-doubt holder fails closed (never chains)');

	# L6c: COMMIT PREPARED resolves the doubt -- the SAME durable COMMITTED
	# stamp that would be in-doubt in the crash window is now backed by the
	# commit record, so the C1b verdict discipline lets node1 heal to 500
	# and apply on top.
	$node0->safe_psql('postgres', "COMMIT PREPARED 'd7a'");
	ok(wait_for_val($node1, 'SELECT ctr FROM ww_t WHERE id = 2', '500', 30),
		'L6c after COMMIT PREPARED node1 heals to the 2PC value');

	# COMMIT PREPARED leaves the page ITL unstamped (the prepared xact's
	# touched-buffer list is gone), so churn first forces terminal verdict
	# resolution.  The convert queue must then transfer X to node1 instead of
	# preserving the former deferred-image availability boundary.
	my $churned = 0;
	for my $i (1 .. 10)
	{
		$churned++ if write_retry($node0, 'UPDATE ww_t SET ctr = ctr WHERE id = 1');
	}
	is($churned, 10, 'L6c owner-side xact churn completes (cold 2PC ITL recycled)');
	my ($wrc2, $wout2, $werr2)
		= $node1->psql('postgres', 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 2');
	is($wrc2, 0, 'L6c peer write obtains X for the decided 2PC-touched block');
	is($node1->safe_psql('postgres', 'SELECT ctr FROM ww_t WHERE id = 2'), '501',
		'L6c peer write applies exactly once on the decided 2PC value');
	ok(write_retry($node0, 'UPDATE ww_t SET ctr = ctr + 1 WHERE id = 2'),
		'L6c owner reacquires X and applies the second +1');
	is($node0->safe_psql('postgres', 'SELECT ctr FROM ww_t WHERE id = 2'), '502',
		'L6c final value chains the 2PC commit and both cross-node increments');
}

$pair->stop_pair;
done_testing();
