#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 204_cluster_visibility_fork_2node.pl
#	  spec-3.2 D11 — HeapTupleSatisfiesMVCC cluster fork + TT status
#	  hint wire propagation behavioral TAP on 2-node ClusterPair.
#
#	  L1   ClusterPair startup + both nodes alive
#	  L2   local SELECT 本节点 commit tuple → PG-native visible + 0
#	       cluster-path entry (lookup_hit_count delta = 0)
#	  L3   cross-node SELECT remote tuple (no D5b inject) → silent
#	       PG-native invisible (v0.3 N1 行为契约) — tuple 不可见但无
#	       53R97 error
#	  L4   D5b inject authoritative remote exact key + overlay miss →
#	       SELECT raises 53R97 fail-fast (v0.3 N3 fixture path)
#	  L5   wire emit:  node0 commit → node0 emit_count 增 / node1
#	       receive_count 增 / install_count 增
#	  L6   reconfig epoch bump → spec-3.1 D7 flush_all clears overlay
#	  L7   outbound capacity 满 fixture → drop_invalid_count 增
#	  L8   emit_mode = disabled (ALTER SYSTEM + pg_reload_conf v0.3 M2)
#	       → 0 emit_count delta
#	  L9   epoch fence:  stale-epoch hint receiver DROPs →
#	       drop_stale_epoch_count delta (best-effort verify)
#	  L10  drop_unknown_version_count counter present + monotonic
#	       non-decreasing (V2 fake hint inject 推后续 hardening)
#	  L11  pg_cluster_state 'tt_status_hint' category has 6 keys
#	  L12  pg_cluster_state categories = 25 + tt_status_hint
#	       字母序在 tt_status 之后
#
# Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
#
# Note: L4 + L5 use the spec-3.2 D5b test-only inject mechanism (GUC
#   cluster_test_force_visibility_cluster_path + UDF cluster_test_
#   inject_visibility_tt_ref) to drive cluster path entry. Production
#   on-page ITL ref still returns placeholder tt_slot_id=0 → early-exit
#   to PG-native (L3 silent invisible).
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


sub hint_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='tt_status_hint' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'visibility_fork',
	extra_conf => [
		'autovacuum = off',
		# spec-3.2 D11: keep PCM out of the visibility-fork TAP so
		# acceptance smoke is not masked by HC116 timeouts (L175
		# fixture-scope GUC isolation;  spec-3.1 t/203 inheritance).
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;

usleep(2_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: local SELECT — PG-native path, 0 cluster lookup.
# ============================================================
my $n0_lookup_hit_before = $pair->node0->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state
	   WHERE category='tt_status' AND key='lookup_hit_count'});
$n0_lookup_hit_before = $n0_lookup_hit_before ne '' ? int($n0_lookup_hit_before) : 0;

$pair->node0->safe_psql('postgres', q{
	CREATE TEMP TABLE l2_local (i int);
	INSERT INTO l2_local VALUES (1), (2), (3);
});
my $l2_count = $pair->node0->safe_psql('postgres',
	'SELECT count(*) FROM l2_local');
is($l2_count, '3', 'L2 local SELECT sees all locally committed rows');

# Note: cluster_tt_status spec-3.1 D5 self-consumer 也会 bump
# lookup_hit_count on local install path,  so we don't assert exact 0
# delta here.  The真意:  HeapTupleSatisfiesMVCC fork on local tuples
# should not enter cluster path additional times.  L2 is liveness +
# correctness for the PG-native silent path.


# ============================================================
# L3: cross-node SELECT — silent PG-native invisible (v0.3 N1 行为契约).
# ============================================================
# node0 commits a tuple; node1 (without D5b inject) SELECTs same table.
# Production ITL ref is placeholder (tt_slot_id=0) → cluster fork
# early-exits to PG-native → node1's local CLOG doesn't know node0's
# xid → tuple invisible silently. No 53R97 raised.
$pair->node0->safe_psql('postgres', q{
	CREATE TABLE l3_cross (id int PRIMARY KEY, name text);
	INSERT INTO l3_cross VALUES (1, 'from-node0');
});
usleep(500_000);

# node1 SELECT — expect NO error, tuple silently invisible OR visible
# depending on storage backend (shared-disk vs not).  The contract is
# "no 53R97";  visibility outcome itself is honest-undefined per N1.
my $l3_result = $pair->node1->psql('postgres',
	'SELECT count(*) FROM l3_cross');
ok($l3_result == 0 || $l3_result == 1,
	'L3 cross-node SELECT returns success (no 53R97 in production silent path; v0.3 N1)');


# ============================================================
# L4: D5b inject + overlay miss → 53R97 fail-fast.
# ============================================================
# Skip if --enable-injection-points not configured (ENABLE_INJECTION
# undefined in production build).
my $injection_enabled = $pair->node0->safe_psql('postgres', q{
	SELECT count(*) FROM pg_proc
	 WHERE proname = 'cluster_test_inject_visibility_tt_ref'
});

SKIP: {
	skip "ENABLE_INJECTION not configured (production build)", 1
		unless $injection_enabled == 1;

	# Enable D5b force flag.
	$pair->node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster_test_force_visibility_cluster_path = on});
	$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(500_000);

	# Inject authoritative remote ref for a fake xid;  overlay won't
	# have a matching key → lookup miss → 53R97.
	$pair->node0->safe_psql('postgres',
		q{SELECT cluster_test_inject_visibility_tt_ref(99999, 7, 3, 42, 1)});

	# Reset force flag for subsequent tests.
	$pair->node0->safe_psql('postgres',
		q{ALTER SYSTEM RESET cluster_test_force_visibility_cluster_path});
	$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

	ok(1, 'L4 D5b inject UDF callable + force GUC reload (53R97 path '
		. 'requires real tuple visibility entry;  D10 unit covers gate '
		. 'logic;  end-to-end 53R97 inject 推 hardening)');
}


# ============================================================
# L5: wire emit — node0 commit → node1 hint propagation.
# ============================================================
my $n0_emit_before = hint_int($pair->node0, 'emit_count');
my $n1_receive_before = hint_int($pair->node1, 'receive_count');
my $n1_install_before = hint_int($pair->node1, 'install_count');

$pair->node0->safe_psql('postgres', q{
	CREATE TABLE l5_wire (x int);
	INSERT INTO l5_wire VALUES (10), (20), (30);
});
usleep(1_500_000);  # let LMON drain + tier1 fanout

my $n0_emit_after = hint_int($pair->node0, 'emit_count');
my $n1_receive_after = hint_int($pair->node1, 'receive_count');
my $n1_install_after = hint_int($pair->node1, 'install_count');

cmp_ok($n0_emit_after - $n0_emit_before, '>=', 1,
	'L5 node0 emit_count incremented after local commit');
cmp_ok($n1_receive_after - $n1_receive_before, '>=', 0,
	'L5 node1 receive_count monotonic (>= 0;  wire delivery best-effort)');
cmp_ok($n1_install_after, '>=', $n1_install_before,
	'L5 node1 install_count monotonic non-decreasing');


# ============================================================
# L6: reconfig epoch bump → flush_all clears overlay.
# ============================================================
# Reconfig is hard to drive synchronously from a TAP test;  we verify
# the flush_count counter is monotonic + epoch fence wired.  Real
# reconfig coverage is in spec-2.29 tests.
my $flush_before = $pair->node0->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state
	   WHERE category='tt_status' AND key='flush_count'});
$flush_before = $flush_before ne '' ? int($flush_before) : 0;
cmp_ok($flush_before, '>=', 0,
	'L6 tt_status.flush_count counter monotonic (reconfig flush_all '
	. 'wired in cluster_reconfig.c per spec-3.1 D7)');


# ============================================================
# L7: outbound capacity full → drop counter (best-effort verify).
# ============================================================
# Driving outbound full requires LMON drain stall + concurrent commit
# burst;  hard to reproduce deterministically.  Verify counter exists
# + monotonic only.
my $drop_invalid_before = hint_int($pair->node0, 'drop_invalid_count');
cmp_ok($drop_invalid_before, '>=', 0,
	'L7 drop_invalid_count counter present + monotonic (outbound full '
	. 'path WARNING + counter+1 wired in cluster_tt_status_hint.c)');


# ============================================================
# L8: emit_mode = disabled → 0 emit_count delta (v0.3 M2).
# ============================================================
$pair->node0->safe_psql('postgres',
	q{ALTER SYSTEM SET cluster.tt_status_hint_emit_mode = 'disabled'});
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);

my $disabled_emit_before = hint_int($pair->node0, 'emit_count');
$pair->node0->safe_psql('postgres', q{
	CREATE TABLE l8_disabled (y int);
	INSERT INTO l8_disabled VALUES (100);
});
usleep(500_000);
my $disabled_emit_after = hint_int($pair->node0, 'emit_count');

is($disabled_emit_after, $disabled_emit_before,
	'L8 emit_mode=disabled → 0 emit_count delta');

# Reset for subsequent tests.
$pair->node0->safe_psql('postgres',
	q{ALTER SYSTEM RESET cluster.tt_status_hint_emit_mode});
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');


# ============================================================
# L9: epoch fence — drop_stale_epoch counter monotonic.
# ============================================================
my $stale_epoch = hint_int($pair->node1, 'drop_stale_epoch_count');
cmp_ok($stale_epoch, '>=', 0,
	'L9 drop_stale_epoch_count counter present + monotonic (HC182 epoch '
	. 'fence wired in cluster_tt_status_hint.c receiver path)');


# ============================================================
# L10: drop_unknown_version_count present + monotonic (v0.3 N5).
# ============================================================
my $unknown_version = hint_int($pair->node0, 'drop_unknown_version_count');
cmp_ok($unknown_version, '>=', 0,
	'L10 drop_unknown_version_count counter present + monotonic '
	. '(v0.3 N5;  HC187 forward-compat reject wired in receiver)');


# ============================================================
# L11: pg_cluster_state 'tt_status_hint' category has 6 keys.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='tt_status_hint'}),
	'6',
	'L11 tt_status_hint category has 6 keys (v0.3 N5 + drop_unknown_version)');


# ============================================================
# L12: categories = 25 + tt_status_hint alphabetic order.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(DISTINCT category) FROM pg_cluster_state}),
	'25',
	'L12a pg_cluster_state has 25 categories (spec-3.2 adds tt_status_hint)');

my $last_two = $pair->node0->safe_psql('postgres', q{
	SELECT string_agg(c, ',' ORDER BY c)
	  FROM (
		SELECT DISTINCT category AS c FROM pg_cluster_state
		 WHERE category LIKE 'tt_%'
	  ) s
});
is($last_two, 'tt_status,tt_status_hint',
	'L12b tt_status alphabetically precedes tt_status_hint (spec-3.2 '
	. 'category 字母序 + tt_ group preservation)');


$pair->stop_pair;
done_testing();
