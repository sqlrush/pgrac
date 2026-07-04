#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 346_cluster_6_12i_runtime_visibility.pl
#	  spec-6.12 wave i -- active-runtime cross-instance recycled-slot
#	  visibility resolution (undo-block CF fetch + live authority gate)
#	  on a 2-node ClusterPair.  L-8A-i legs.
#
#	  Substrate: node0 seeds one heap block with a single INSERT xact, then
#	  runs >8 committed single-row UPDATE xacts on the same block -- the
#	  8-slot page ITL recycles the INSERT xact's slot, so every seed row's
#	  xmin resolves through the RECYCLED remote branch on node1
#	  (ref->local_xid != raw_xid; the pre-6.12i unconditional 53R97).
#
#	  L1  pair boots + GUC default off + one-block table seeded
#	  L2  8A off-leg: node1 read fails closed 53R97 (unchanged boundary)
#	  L3  gain leg (GUC on both): the same node1 read SUCCEEDS; the wave
#	      counters prove the path (resolve_committed / fetch wire / fetch
#	      cache-hit on the requester, undo_served on the origin)
#	  L4  8A origin-refusal leg: the wave GUC is turned OFF on the ORIGIN
#	      only (requester still on) -> the LMON submit gate refuses and
#	      replies DENIED -> a fresh node1 backend fails closed 53R97 again
#	      (proof-insufficient never resolves); requester failclosed
#	      counters move.  (The cluster-lms-undo-fetch injection point
#	      cannot be armed from SQL here: cluster_inject_fault arm state is
#	      process-local (t/015 note) and never reaches the LMS process --
#	      conf-time cluster.injection_points arming is the chaos-harness
#	      face, not this test.)
#	  L5  D-i3 crash leg: node0 fail-stops mid-session; after node1's CSSD
#	      marks it dead + reconfig fires (epoch bump), the SAME session's
#	      re-read must ERROR -- the cached authority from the old epoch is
#	      dead (covers=false) and the wire has no live origin.  A fresh
#	      backend fails too.  NEVER a silent stale/false-visible read.
#	  L6  counter surface: the 8 wave-i cr keys present
#
#	  Honesty (规则 18): L3 proves the FUNCTIONAL gain leg (fail-closed ->
#	  works) on the phantom-shared harness; the true write-scaling numbers
#	  (node1 pgbench write > 0, 53R97 storm gone) belong to the spec-6.0a
#	  block_device rerun of the t/340 scaling probe (§3.5 substrate rule).
#	  L5 asserts fail-closed-on-crash broadly (53R97 / enqueue failure /
#	  fail-stop abort are all acceptable) -- the specific survivor-reads-
#	  dead-node's-data recovery is the spec-4.10 face, out of wave scope.
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave i)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/346_cluster_6_12i_runtime_visibility.pl
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

sub poll_until
{
	my ($node, $sql, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $v && $v eq $want;
		usleep(250_000);
	}
	return 0;
}

# ============================================================
# L1: boot + one-block table whose seed xact's ITL slot gets recycled.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'i612_rtvis',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		# Q-i5 cache leg: the L2 CR pool is default-off (spec-5.58 keep-off);
		# PGC_POSTMASTER, so arm it at boot (t/320 idiom).
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'), 'off',
	'L1 crossnode_runtime_visibility default off');

$node0->safe_psql('postgres', 'CREATE TABLE i_t (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE i_t (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('i_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('i_t')});
is($p0, $p1, 'L1 i_t relfilepath coincidence holds');

# One INSERT xact seeds 12 rows on one block; 10 later single-row committed
# UPDATE xacts on rows 3..12 (10 xacts > 8 ITL slots) recycle the seed slot.
ok(write_retry($node0, 'INSERT INTO i_t SELECT g, g * 10 FROM generate_series(1, 12) g'),
	'L1 seed xact inserted 12 rows');
for my $id (3 .. 12)
{
	ok(write_retry($node0, "UPDATE i_t SET v = v + 1 WHERE id = $id"),
		"L1 recycler update xact (id=$id) committed");
}
is($node0->safe_psql('postgres',
		q{SELECT count(DISTINCT (ctid::text::point)[0]::int) FROM i_t}),
	'1', 'L1 all live rows stayed on ONE heap block');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: 8A off-leg -- node1 read of the recycled-slot block fails closed 53R97.
# ============================================================
{
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM i_t');
	like($err, qr/cluster TT (status unknown|slot recycled)/,
		'L2 GUC-off: node1 read of recycled remote slots fails closed (53R97 boundary)');
}

# ============================================================
# L3: gain leg -- GUC on BOTH sides, the same read succeeds + counters move.
# ============================================================
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'), 'on',
	'L3 crossnode_runtime_visibility armed on both nodes');

{
	my $rc0     = state_val($node1, 'cr', 'rtvis_resolve_committed_count');
	my $wire0   = state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count');
	my $hit0    = state_val($node1, 'cr', 'rtvis_undo_fetch_cache_hit_count');
	my $served0 = state_val($node0, 'cr', 'cr_server_undo_served_count');

	# One backend, two statements: 12 same-xid tuples resolve -- the first
	# fetch rides the wire, the rest hit the pool + per-backend authority memo.
	my $out = $node1->safe_psql('postgres',
		"SELECT count(*) FROM i_t;\nSELECT count(*) FROM i_t WHERE v >= 10;");
	like($out, qr/^12\n12$/,
		'L3 GAIN: node1 reads the recycled-slot block (12 rows, twice, one session)');

	cmp_ok(state_val($node1, 'cr', 'rtvis_resolve_committed_count'), '>', $rc0,
		'L3 resolve_committed counter moved (positive proofs returned)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_undo_fetch_wire_count'), '>', $wire0,
		'L3 fetch wire counter moved (undo-TT block shipped from origin)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_undo_fetch_cache_hit_count'), '>', $hit0,
		'L3 fetch cache-hit counter moved (pool + authority memo pair reused)');
	cmp_ok(state_val($node0, 'cr', 'cr_server_undo_served_count'), '>', $served0,
		'L3 origin served the undo-TT fetch (LMS co-sample leg)');
}

# ============================================================
# L4: 8A origin-refusal leg -- wave GUC OFF on the ORIGIN only (asymmetric
# arming) -> the LMON submit gate refuses -> DENIED -> a fresh node1 backend
# fails closed 53R97 again; proof-insufficient NEVER resolves.
# ============================================================
{
	my $fc0  = state_val($node1, 'cr', 'rtvis_undo_fetch_failclosed_count');
	my $rfc0 = state_val($node1, 'cr', 'rtvis_resolve_failclosed_count');

	$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = off');
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);

	# A fresh backend has no per-backend authority memo, so the cached pool
	# bytes are unreachable and the fetch must ride the wire into the
	# origin's refusal.
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM i_t');
	like($err, qr/cluster TT (status unknown|slot recycled)/,
		'L4 8A: origin refusal keeps the read fail-closed (53R97)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_undo_fetch_failclosed_count'), '>', $fc0,
		'L4 requester fetch failclosed counter moved (DENIED wire outcome)');
	cmp_ok(state_val($node1, 'cr', 'rtvis_resolve_failclosed_count'), '>', $rfc0,
		'L4 requester resolve failclosed counter moved (no positive proof)');

	$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.crossnode_runtime_visibility = on');
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);
}

# ============================================================
# L5: D-i3 crash leg -- origin fail-stop => epoch bump => the session's
# cached authority is dead; re-read must ERROR, never a stale serve.
# ============================================================
{
	# Warm a session: resolves + populates its per-backend authority memo.
	my $s = $node1->background_psql('postgres', on_error_stop => 0);
	my $warm = $s->query('SELECT count(*) FROM i_t');
	like($warm, qr/12/, 'L5 session warmed (memo + pool populated pre-crash)');

	$pair->kill_node9(0);
	ok(poll_until($node1,
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 0},
			't', 40),
		'L5 node1 CSSD marks node0 suspected/dead');
	ok(poll_until($node1, q{SELECT event_id != 0 FROM pg_cluster_reconfig_state}, 't', 40),
		'L5 node1 reconfig fired (epoch bumped -- old authority dead)');

	# Same session re-read: the memo's origin_epoch no longer matches, the
	# cached pair is unreachable, and the wire has no live origin.  It MUST
	# error (53R97 / enqueue failure / fail-stop abort) -- and above all it
	# must NOT return rows from the stale cache.
	my $post = $s->query('SELECT count(*) FROM i_t');
	unlike($post, qr/^\s*12\s*$/m,
		'L5 D-i3: post-crash same-session re-read did NOT serve the stale cached pair');
	eval { $s->quit; };

	# A fresh backend fails closed too.
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM i_t');
	isnt($rc, 0, 'L5 fresh-backend read after origin crash errors (fail-closed)');
	like($err,
		qr/cluster TT (status unknown|slot recycled)|failed to enqueue|fail-stop|could not obtain|connection/i,
		'L5 the failure is a cluster fail-closed error, never a silent result');
}

# ============================================================
# L6: counter surface -- the 8 wave-i cr keys exist on node1 (node0 is dead).
# ============================================================
{
	my $rows = $node1->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='cr' AND key IN
		     ('rtvis_undo_fetch_wire_count','rtvis_undo_fetch_cache_hit_count',
		      'rtvis_undo_fetch_failclosed_count','cr_server_undo_served_count',
		      'cr_server_undo_denied_count','rtvis_resolve_committed_count',
		      'rtvis_resolve_aborted_count','rtvis_resolve_failclosed_count')});
	is($rows, '8', 'L6 all 8 wave-i cr keys present');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
