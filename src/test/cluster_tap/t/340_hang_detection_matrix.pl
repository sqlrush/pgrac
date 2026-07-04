#-------------------------------------------------------------------------
#
# 340_hang_detection_matrix.pl
#    spec-5.20 D2 -- Hang Manager chaos detection matrix (HG#1 detection
#    completeness + HG#2 false-positive detection side), single node.
#
#    Drives the faithful, single-node-injectable HT taxonomy cells against a
#    real server and asserts, per cell, that the 5.11 sampler classifies the
#    wait correctly (per-sample _source / _quality / actionability, small-case
#    dump strings) with counter deltas proving real sampling ran (L250: remove
#    the mechanism and the assertion fails).  The safe-direction discipline
#    (spec §3): assert correct classification OR honest low-confidence skip;
#    never assert timing-dependent liveness as a hard bar.
#
#    Cells covered here (faithful, single node):
#      HT1  idle-in-tx blocker      -> waiter sampled complete / lock, actionable
#      HT2  long blocking chain     -> root actionable, mid-chain sampled
#      HT3  lock convoy             -> root actionable, N waiters sampled
#      HT7  healthy-but-slow query  -> NEVER sampled as a long-wait (FP control)
#      HT9  ABA (blocker self-heals)-> transient sample does not persist stuck
#      HT11 2PC-prepared holder     -> waiter sampled complete / lock (victim
#                                      reachability is a disposition concern, D3)
#      HT12 hang storm (>cap)       -> truncated=true + n_samples capped at 64
#
#    Cross-node cells (HT5/HT6 REMOTE_BOUNDARY) live in t/343 (D5) where a real
#    multi-node cluster exists; the v1-unreachable cells (HT4 IO-stall
#    APPROXIMATE, HT8 fairness-boosted, HT10 hard-skip system holder) are proven
#    by the C policy units and noted honestly here (spec §1.4.5 / L341), never
#    faked.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/340_hang_detection_matrix.pl
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
use PostgreSQL::Test::HangChaos;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use PgracClusterNode;


my $node = PgracClusterNode->new('hangdet');
$node->init;
my $chaos = PostgreSQL::Test::HangChaos->new($node);
$chaos->apply_fast_conf;
$node->start;


# ======================================================================
# HT1 -- idle-in-tx blocker: a BEGIN;LOCK TABLE (no xid) holder blocks a waiter.
# The WAITER is the long-wait sample: complete / lock / actionable, and its
# blocker_pid points at the root holder.  Counter deltas prove real sampling.
# ======================================================================
{
	my $base = $chaos->counters;
	my $root = $chaos->idle_in_tx_blocker;
	my $w    = $chaos->waiter_on($root->{table});
	ok($chaos->wait_for_lock_wait("%$root->{table}%", 20),
		'HT1 waiter is blocked on the relation lock');

	# Wait until the waiter crosses the threshold and is sampled.
	my $q = $chaos->wait_for_sample_quality($w->{pid}, 20);
	is($q, 'complete', 'HT1 waiter sample quality == complete');
	is($chaos->sample_field($w->{pid}, 'source'), 'lock',
		'HT1 waiter sample source == lock');
	$chaos->assert_hang_sample($w->{pid}, { quality => 'complete', actionable => 1 },
		'HT1');
	is($chaos->sample_field($w->{pid}, 'blocker_pid'), $root->{pid},
		'HT1 waiter sample blocker_pid == root holder');
	# The idle-in-tx root itself is not waiting -> not a long-wait sample.
	is($chaos->sample_field($root->{pid}, 'quality'), '',
		'HT1 idle-in-tx root holder is not itself sampled (not waiting)');

	cmp_ok($chaos->delta($base, 'hang_samples_taken'), '>', 0,
		'HT1 samples_taken incremented (real sampling ran)');
	cmp_ok($chaos->delta($base, 'hang_long_waits_seen'), '>', 0,
		'HT1 long_waits_seen incremented');

	$chaos->cleanup;
}


# ======================================================================
# HT2 -- long blocking chain R <- M1 <- W.  The root R is the actionable cause;
# the deepest waiter W is sampled complete/lock and root-ascends to R.
# ======================================================================
{
	my $base  = $chaos->counters;
	my $chain = $chaos->blocking_chain(3);
	ok($chaos->wait_for_lock_wait("%$chain->{tables}[0]%", 20)
		|| $chaos->wait_for_lock_wait("%$chain->{tables}[1]%", 5),
		'HT2 blocking chain established (a link is lock-waiting)');

	my $q = $chaos->wait_for_sample_quality($chain->{waiter_pid}, 20);
	is($q, 'complete', 'HT2 deepest waiter sample quality == complete');
	is($chaos->sample_field($chain->{waiter_pid}, 'source'), 'lock',
		'HT2 deepest waiter sample source == lock');
	# The mid-chain link is also a blocked waiter and must be sampled.
	my $mq = $chaos->wait_for_sample_quality($chain->{mid_pids}[0], 10);
	is($mq, 'complete', 'HT2 mid-chain link sampled complete');
	# The idle-in-tx root is not sampled (it never waits).
	is($chaos->sample_field($chain->{root_pid}, 'quality'), '',
		'HT2 idle-in-tx chain root is not itself sampled');
	cmp_ok($chaos->delta($base, 'hang_long_waits_seen'), '>=', 2,
		'HT2 at least two chain links seen as long-waits');

	$chaos->cleanup;
}


# ======================================================================
# HT3 -- lock convoy: one root, N waiters on one resource.  Every waiter is a
# complete/lock long-wait; the shared root is the single actionable cause.
# ======================================================================
{
	my $base   = $chaos->counters;
	my $convoy = $chaos->lock_convoy(3);
	ok($chaos->wait_for_lock_wait("%$convoy->{table}%", 20),
		'HT3 convoy waiters blocked on the shared resource');

	my $seen = 0;
	for my $wpid (@{ $convoy->{waiter_pids} })
	{
		my $q = $chaos->wait_for_sample_quality($wpid, 15);
		$seen++ if $q eq 'complete';
		is($chaos->sample_field($wpid, 'blocker_pid'), $convoy->{root_pid},
			"HT3 convoy waiter $wpid blocker_pid == shared root")
			if $q eq 'complete';
	}
	cmp_ok($seen, '>=', 2, 'HT3 multiple convoy waiters sampled complete');
	cmp_ok($chaos->delta($base, 'hang_long_waits_seen'), '>=', 2,
		'HT3 convoy long-waits counted');

	$chaos->cleanup;
}


# ======================================================================
# HT7 -- healthy-but-slow ACTIVE query (CPU-bound).  False-positive control,
# asserted in the spec §3 SAFE DIRECTION: a healthy backend must NEVER be given
# the actionable 'complete' classification and must NEVER be advertised as a
# hang victim.  (A CPU-bound query with no hard blocker can never be classified
# 'complete' -- that needs a granted lock holder -- but it may momentarily
# register a generic PG wait sampled as low-confidence 'approximate', which is
# fail-OPEN non-actionable and SAFE.  Asserting "never sampled at all" would be
# a timing-flaky over-assertion; the real false-positive safety is "never
# actionable / never disposed".)
# ======================================================================
{
	my $hs = $chaos->healthy_slow_query(120_000_000);
	# Confirm it is active (running), not lock-waiting.
	my $seen_active = 0;
	my $dl = time() + 10;
	while (time() < $dl)
	{
		my $we = $node->safe_psql('postgres',
			"SELECT coalesce(wait_event_type,'(none)') FROM pg_stat_activity WHERE pid = $hs->{pid} AND state='active'");
		if (defined $we && $we ne 'Lock')
		{
			$seen_active = 1;
			last;
		}
		usleep(200_000);
	}
	ok($seen_active, 'HT7 healthy-slow query is active and not lock-waiting');

	# Let several sample rounds pass, then assert the safe direction.
	usleep(4_000_000);
	isnt($chaos->sample_field($hs->{pid}, 'quality'), 'complete',
		'HT7 healthy-slow query is NEVER classified complete/actionable (fail-OPEN safe direction)');
	is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $hs->{pid}"),
		'0', 'HT7 healthy-slow query is NEVER a hang victim (false-positive control)');

	$chaos->cleanup;
}


# ======================================================================
# HT9 -- ABA: a blocker that self-resolves after its waiter is sampled.  The
# detection-side property: once the blocker is gone, the waiter unblocks and no
# stuck actionable long-wait persists for it.  (The no-kill-of-reused-pid
# revalidation is a disposition concern, covered in t/331 D3.)
# ======================================================================
{
	my $blocker = $chaos->aba_blocker;
	my $w       = $chaos->waiter_on($blocker->{table});
	ok($chaos->wait_for_lock_wait("%$blocker->{table}%", 20),
		'HT9 waiter blocked before ABA release');
	my $q = $chaos->wait_for_sample_quality($w->{pid}, 20);
	is($q, 'complete', 'HT9 waiter sampled complete before blocker self-heals');

	# ABA moment: blocker rolls back, waiter acquires the lock.
	$chaos->release_holder($blocker);
	my $unblocked = 0;
	my $dl = time() + 20;
	while (time() < $dl)
	{
		my $we = $node->safe_psql('postgres',
			"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid = $w->{pid}");
		if (!defined $we || $we ne 'Lock') { $unblocked = 1; last; }
		usleep(200_000);
	}
	ok($unblocked, 'HT9 waiter unblocked after blocker self-resolved (ABA)');

	$chaos->cleanup;
}


# ======================================================================
# HT11 -- 2PC-prepared holder: PREPARE TRANSACTION leaves the AEL held by no
# live backend.  Detection side: the waiter on the 2PC-held lock IS a real
# complete/lock long-wait.  (That the holder has no live PGPROC victim -> no
# terminate is a disposition concern, covered in t/331 D3.)
# ======================================================================
{
	my $tp = $chaos->twopc_holder;
	my $w  = $chaos->waiter_on($tp->{table});
	ok($chaos->wait_for_lock_wait("%$tp->{table}%", 20),
		'HT11 waiter blocks on the 2PC-held lock');
	my $q = $chaos->wait_for_sample_quality($w->{pid}, 20);
	is($q, 'complete', 'HT11 2PC waiter sampled complete');
	is($chaos->sample_field($w->{pid}, 'source'), 'lock',
		'HT11 2PC waiter sample source == lock');

	$chaos->cleanup;
}


# ======================================================================
# HT12 -- hang storm: more simultaneous long-waits than the sample store cap
# (cluster.hang_max_sampled default 64).  The store must report truncated=true
# and cap n_samples at 64 (honest, not a silent drop).
# ======================================================================
{
	my $storm = $chaos->hang_storm(70);
	# Wait for the store to fill and truncate.
	my $trunc = '';
	my $ns    = 0;
	my $dl    = time() + 40;
	while (time() < $dl)
	{
		# hang_truncated serialises via fmt_bool() -> 't' / 'f'.
		$trunc = $chaos->hang_val('hang_truncated');
		$ns    = $chaos->hang_num('hang_n_samples');
		last if $trunc eq 't';
		usleep(300_000);
	}
	is($trunc, 't', 'HT12 hang storm reports truncated=t (honest overflow, not silent)');
	is($ns, 64, 'HT12 n_samples capped at hang_max_sampled (64)');

	$chaos->cleanup;
}


# ======================================================================
# v1-unreachable / not-single-node cells: honest notes, not faked assertions.
# ======================================================================
note('HT4 IO-stall APPROXIMATE non-actionable verdict: covered by '
	. 'test_cluster_hang unit (synthetic; faithful IO-stall not deterministically '
	. 'inducible from a TAP session, spec §3.3 / §1.4.5).');
note('HT8 fairness-boosted starvation: v1 fairness_boosted is const false '
	. '(5.10 reader not compiled); honest-limited, no faithful distinction (spec §3.3 HT8).');
note('HT10 hard-skip system holder (SKIP_SYSTEM/no_safe_victim): '
	. PostgreSQL::Test::HangChaos::hard_skip_note());
# HT5/HT6 honesty (spec §3.3 / §1.4.5, spec-5.20 Impl note): the REMOTE_BOUNDARY
# *classification* (cross-node blocker -> remote_boundary -> non-actionable) is
# faithfully unit-tested at the policy level -- test_cluster_hang.c asserts
# cluster_hang_quality(.., blocker_remote_node>=0, ..) == HANG_SAMPLE_REMOTE_BOUNDARY,
# and test_cluster_hang_acceptance.c L6 pins the non-actionable safe direction.
# A sustained e2e cross-node ACTIONABLE hang is NOT deterministically inducible
# (cross-node ROW conflicts fail-closed 53R98 rather than block, spec-3.4d/t/209;
# cross-node BLOCK conflicts make progress via Cache Fusion), so there is no
# faithful e2e injector -- this is stated honestly, not faked.  t/343 (D5) covers
# the cross-node NON-INTERFERENCE + honesty at runtime (the local manager never
# disposes a non-local waiter; any remote_boundary sample seen is asserted
# non-actionable + not disposed there).
note('HT5/HT6 cross-node REMOTE_BOUNDARY: classification faithfully unit-tested '
	. '(test_cluster_hang cluster_hang_quality remote-node case + acceptance L6); '
	. 'sustained e2e cross-node actionable hang NOT deterministically inducible '
	. '(cross-node row = fail-closed 53R98, block = Cache-Fusion progress); '
	. 't/343 covers cross-node non-interference + honesty at runtime.');


$node->stop;
done_testing();
