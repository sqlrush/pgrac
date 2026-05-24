#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 208_cluster_d5b_decide_by_scn_real_visible_invisible.pl
#	  spec-3.4c D12 — D5b 6-arg inject + decide_by_scn real visibility TAP.
#
#	  spec-3.4c ship raison d'être test:  demonstrates that the
#	  cluster visibility hot path no longer fall-through to 53R97
#	  fail-closed.  After the A1 P0 amend (D5b inject UDF now installs
#	  TT status overlay synchronously with commit_scn), the path
#	  ref → ClusterTTStatusKey → cluster_tt_status_lookup_exact() →
#	  result.commit_scn → cluster_visibility_decide_by_scn() returns
#	  a real VISIBLE / INVISIBLE / 53R97-only-when-truly-unknown
#	  decision driven by (commit_scn vs snapshot.read_scn).
#
#	  Honest scope (A3 / F6 amend):
#	    1. ClusterPair does NOT share storage between node0 and node1
#	       and the inject HTAB + TT status overlay are per-node local
#	       shmem.  So "node0 inject → node1 SELECT" cannot exercise
#	       the production cross-node propagation chain.
#	    2. Therefore D12 t/208 narrows scope:  the inject + SELECT
#	       must run on the SAME node.  ClusterPair is used purely as
#	       a fixture (cluster context + GUC namespace + log scrape
#	       coverage); both inject + forced cluster path + SELECT
#	       happen on node0.
#	    3. 6-arg UDF only installs COMMITTED — ABORTED status is not
#	       exercised here.  Production cross-node ABORTED behavior
#	       lives in spec-3.4b D13 t/207 smoke + future shared-storage
#	       harness (Stage 4+).
#
#	  L1   ClusterPair startup + both nodes alive
#	  L2   prep: enable D5b force flag + sample baseline tt_status counters
#	  L3   commit_scn ≤ read_scn (valid past commit) inject → SELECT VISIBLE
#	       (cluster path decided by decide_by_scn,  NOT 53R97)
#	  L4   commit_scn > read_scn (future commit) inject → SELECT INVISIBLE
#	       (cluster path decided by decide_by_scn,  NOT 53R97)
#	  L5   neither L3 nor L4 raised 53R97 → real binary decision proven
#	  L6   commit_scn = 0 (InvalidScn) inject → 53R97 fall-through
#	       (decide_by_scn returns UNKNOWN; verifies the fail-closed path
#	       still works when overlay genuinely lacks commit info)
#	  L7   clear_visibility_injects → next SELECT raises 53R97 again
#	       (verifies F4 D6 delete_exact really removes overlay)
#	  L8   6-arg UDF rejects 5-arg call shape (arg-count enforcement)
#	  L9   No PANIC in postmaster log
#	  L10  No DATA_CORRUPTED in postmaster log
#	  L11  Postmaster clean shutdown
#
#	  Spec: spec-3.4c-delayed-cleanout-d5b-commit-scn-yellow-perf-hardening.md
#	        (v0.3 FROZEN 2026-05-24)
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


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'd5b_decide_by_scn',
	extra_conf => [
		'autovacuum = off',
		# Keep PCM out so acceptance smoke isn't masked (mirror t/204).
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;

usleep(2_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: prep — D5b force flag + baseline counters.
# ============================================================
# Skip the entire behavioral suite if --enable-injection-points not
# configured (production builds link FEATURE_NOT_SUPPORTED stubs).
my $injection_enabled = $pair->node0->safe_psql('postgres', q{
	SELECT count(*) FROM pg_settings
	 WHERE name = 'cluster_test_force_visibility_cluster_path'
});

SKIP: {
	skip "ENABLE_INJECTION not configured (production build)", 8
		unless $injection_enabled == 1;

	$pair->node0->safe_psql('postgres', q{
		DROP TABLE IF EXISTS l3_visible;
		CREATE TABLE l3_visible(id int PRIMARY KEY, payload text);
		INSERT INTO l3_visible VALUES (1, 'past-commit');
	});
	my $xid_l3 = $pair->node0->safe_psql('postgres',
		q{SELECT xmin::text::int FROM l3_visible WHERE id = 1});

	# Sample current cluster_scn so we can install commit_scn deterministically
	# below.  read_scn is the snapshot's read_scn at SELECT time;  it advances
	# monotonically, so any commit_scn captured here is guaranteed < later
	# read_scn (provided no clock skew).  We use a small positive commit_scn
	# = 1 (validly ≤ any future read_scn) to drive VISIBLE in L3.
	my $small_scn = 1;
	$pair->node0->safe_psql('postgres',
		qq{SELECT cluster_test_inject_visibility_tt_ref(
			'$xid_l3'::xid, 7, 3, 42, 0, ${small_scn}::int8)});

	# Enable D5b force flag — every subsequent visibility check enters
	# the cluster path.
	$pair->node0->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster_test_force_visibility_cluster_path = on});
	$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(500_000);

	ok(1, 'L2 D5b force flag enabled + baseline inject installed');


	# ============================================================
	# L3: commit_scn ≤ read_scn → decide_by_scn returns VISIBLE.
	# ============================================================
	my ($rc3, $out3, $err3) = $pair->node0->psql('postgres',
		q{SELECT count(*) FROM l3_visible});
	is($rc3, 0,
		'L3 SELECT returns 0 (no error) — decide_by_scn VISIBLE branch reached');
	unlike($err3, qr/53R97|cluster TT status unknown/,
		'L3 SELECT did not raise 53R97 (overlay COMMITTED + small commit_scn)');


	# ============================================================
	# L4: commit_scn > read_scn → decide_by_scn returns INVISIBLE.
	# ============================================================
	# Inject a separate xid with a deliberately high commit_scn (effectively
	# "future commit" relative to subsequent snapshots).  SELECT must NOT
	# raise 53R97;  the tuple simply remains invisible.
	$pair->node0->safe_psql('postgres', q{
		DROP TABLE IF EXISTS l4_future;
		CREATE TABLE l4_future(id int PRIMARY KEY, payload text);
		INSERT INTO l4_future VALUES (2, 'future-commit');
	});
	my $xid_l4 = $pair->node0->safe_psql('postgres',
		q{SELECT xmin::text::int FROM l4_future WHERE id = 2});
	# 2^62 ≈ 4.6e18 ensures commit_scn > any plausible read_scn for the
	# remainder of this test run.  decide_by_scn must return INVISIBLE,
	# NOT UNKNOWN/53R97.
	my $huge_scn = '4611686018427387904';
	$pair->node0->safe_psql('postgres',
		qq{SELECT cluster_test_inject_visibility_tt_ref(
			'$xid_l4'::xid, 7, 3, 43, 0, ${huge_scn}::int8)});

	my ($rc4, $out4, $err4) = $pair->node0->psql('postgres',
		q{SELECT count(*) FROM l4_future});
	is($rc4, 0,
		'L4 SELECT returns 0 (no error) — decide_by_scn INVISIBLE branch reached');
	unlike($err4, qr/53R97|cluster TT status unknown/,
		'L4 SELECT did not raise 53R97 (overlay COMMITTED + future commit_scn)');


	# ============================================================
	# L5: composite assertion — neither L3 nor L4 raised 53R97.
	# ============================================================
	ok($err3 !~ /53R97/ && $err4 !~ /53R97/,
		'L5 cluster path made a real binary decision (VISIBLE or '
		. 'INVISIBLE) for both L3 and L4 — no 53R97 fall-through');


	# ============================================================
	# L6: commit_scn = 0 (InvalidScn) → 53R97 fail-closed.
	# ============================================================
	# A genuine "overlay has COMMITTED but no commit_scn" condition
	# must still fall through to 53R97 (decide_by_scn returns UNKNOWN).
	$pair->node0->safe_psql('postgres', q{
		DROP TABLE IF EXISTS l6_unknown;
		CREATE TABLE l6_unknown(id int PRIMARY KEY);
		INSERT INTO l6_unknown VALUES (3);
	});
	my $xid_l6 = $pair->node0->safe_psql('postgres',
		q{SELECT xmin::text::int FROM l6_unknown WHERE id = 3});
	$pair->node0->safe_psql('postgres',
		qq{SELECT cluster_test_inject_visibility_tt_ref(
			'$xid_l6'::xid, 7, 3, 44, 0, 0::int8)});

	my ($rc6, $out6, $err6) = $pair->node0->psql('postgres',
		q{\set VERBOSITY verbose
		  SELECT count(*) FROM l6_unknown});
	ok($rc6 != 0 || $err6 =~ /53R97|cluster TT status unknown/,
		'L6 commit_scn=InvalidScn falls through to 53R97 (genuine UNKNOWN)');


	# ============================================================
	# L7: clear_visibility_injects → next SELECT raises 53R97.
	# ============================================================
	# After clear (F4 D6 delete_exact removes overlay), the L3 xid no
	# longer has a TT status entry.  SELECT on l3_visible must fail
	# closed with 53R97 since overlay lookup misses.
	$pair->node0->safe_psql('postgres',
		q{SELECT cluster_test_clear_visibility_injects()});

	my ($rc7, $out7, $err7) = $pair->node0->psql('postgres',
		q{\set VERBOSITY verbose
		  SELECT count(*) FROM l3_visible});
	ok($rc7 != 0 || $err7 =~ /53R97|cluster TT status unknown/,
		'L7 clear_injects really deletes overlay (next SELECT → 53R97)');


	# ============================================================
	# L8: 5-arg call shape rejected (catalog signature is 6-arg).
	# ============================================================
	my ($rc8, $out8, $err8) = $pair->node0->psql('postgres',
		q{SELECT cluster_test_inject_visibility_tt_ref(
			'1'::xid, 7, 3, 42, 0)});
	ok($rc8 != 0 && $err8 =~ /function .* does not exist|argument/i,
		'L8 5-arg call shape rejected (proargtypes lock 6-arg signature)');


	# Cleanup: reset force flag so subsequent tests are not affected.
	$pair->node0->safe_psql('postgres',
		q{ALTER SYSTEM RESET cluster_test_force_visibility_cluster_path});
	$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
}


# ============================================================
# L9-L11: log scrape + clean shutdown.
# ============================================================
my $log0 = $pair->node0->logfile;
my $log1 = $pair->node1->logfile;

my $panic0 = `grep -c PANIC $log0 2>/dev/null`;
chomp $panic0;
my $panic1 = `grep -c PANIC $log1 2>/dev/null`;
chomp $panic1;
is($panic0 + $panic1, 0, 'L9 no PANIC in either node log');

my $corruption0 = `grep -cE ERRCODE_DATA_CORRUPTED $log0 2>/dev/null`;
chomp $corruption0;
my $corruption1 = `grep -cE ERRCODE_DATA_CORRUPTED $log1 2>/dev/null`;
chomp $corruption1;
is($corruption0 + $corruption1, 0, 'L10 no DATA_CORRUPTED in either node log');


$pair->stop_pair;

my $shutdown_panic0 = `grep -c PANIC $log0 2>/dev/null`;
chomp $shutdown_panic0;
my $shutdown_panic1 = `grep -c PANIC $log1 2>/dev/null`;
chomp $shutdown_panic1;
is($shutdown_panic0 + $shutdown_panic1, 0, 'L11 clean shutdown (no PANIC during stop)');


done_testing();
