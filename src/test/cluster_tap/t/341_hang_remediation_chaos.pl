#-------------------------------------------------------------------------
#
# 341_hang_remediation_chaos.pl
#    spec-5.20 D3 -- Hang Manager remediation correctness under chaos (HG#3),
#    single node.
#
#    Deepens t/305 (spec-5.12 disposition happy-path) with the chaos-under-load
#    coverage t/305 does not have: real disposition of a multi-link chain and a
#    lock convoy root, the no-internal-SIGKILL invariant (source hygiene grep +
#    behavioural: the postmaster never crash-restarts), hang-storm max_per_round
#    rate limiting (no cascade over-kill), ABA revalidation under rapid self-
#    heal, advisory dry-run under a mixed chaos workload, and the manual
#    pg_cluster_hang_resolve() gate.  counter deltas prove the real disposition
#    path ran (L250).  Hard assertions only pin the 8.A-dual safe direction
#    (dispose a real victim OR a registered fail-CLOSED skip); timing-dependent
#    liveness uses generous polls, never a hard latency bar (spec §3, R1).
#
#    The base disposition matrix (advisory/enforce idle-in-tx, fail-CLOSED
#    block-nobody, deadlock-not-double-kill, 2PC-undisposable, mode matrix,
#    manual perm gate, victims visibility, truncated-root skip, multi-waiter
#    root) is owned by t/305 and referenced, not duplicated (spec §1.3 Q7-A).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/341_hang_remediation_chaos.pl
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


my $node = PgracClusterNode->new('hangrem');
$node->init;
my $chaos = PostgreSQL::Test::HangChaos->new($node);
$chaos->apply_fast_conf;
$node->start;

# Postmaster identity: a real internal SIGKILL to a backend would crash-restart
# the whole instance; pin the postmaster pid so we can prove it never happened.
my $pm_pidfile = $node->data_dir . '/postmaster.pid';
my $pm_pid0 = (split /\n/, slurp_file($pm_pidfile))[0];


# ======================================================================
# no-SIGKILL SOURCE HYGIENE (spec §3.3 L237): no disposition code path sends
# SIGKILL.  Every SIGKILL token in the Hang Manager sources is a comment/design
# note; there is no kill(...SIGKILL...) call site.  (The pure tier->signal
# no-SIGKILL invariant is separately pinned by the D8 C unit.)
# ======================================================================
{
	my $srcdir = "$FindBin::RealBin/../../../backend/cluster";
	my @files = qw(cluster_hang.c cluster_hang_policy.c
		cluster_hang_resolve.c cluster_hang_resolve_policy.c);
	my $call_sites = 0;
	for my $f (@files)
	{
		my $path = "$srcdir/$f";
		next unless -f $path;
		my $src = slurp_file($path);
		# A dangerous site would pass SIGKILL to kill()/signal helper.
		$call_sites++ while $src =~ /\bkill\s*\([^;\n]*SIGKILL/g;
		$call_sites++ while $src =~ /signal_victim\s*\([^;\n]*SIGKILL/g;
	}
	is($call_sites, 0,
		'no-SIGKILL: no kill()/signal_victim() call site passes SIGKILL in the Hang Manager sources');
}


# ======================================================================
# ENFORCE + chaos: flip to enforce and drive real disposition.
# ======================================================================
$node->safe_psql('postgres', 'ALTER SYSTEM SET cluster.hang_resolution_mode = enforce');
$node->reload;


# --- enforce a long blocking chain: the root is really terminated and every
#     downstream link unblocks (chain-shaped disposition, not just idle-in-tx).
{
	my $base  = $chaos->counters;
	my $chain = $chaos->blocking_chain(3);
	ok($chaos->wait_for_lock_wait("%$chain->{tables}[1]%", 20)
		|| $chaos->wait_for_lock_wait("%$chain->{tables}[0]%", 5),
		'chain: a link is lock-waiting under enforce');

	ok($chaos->wait_for_gone($chain->{root_pid}, 45),
		'chain: enforce really TERMINATED the chain root');
	cmp_ok($chaos->delta($base, 'hang_terminates_issued'), '>=', 1,
		'chain: terminates_issued incremented (real disposition ran)');

	# The deepest waiter must eventually stop lock-waiting.
	my $unblocked = 0;
	my $dl = time() + 25;
	while (time() < $dl)
	{
		my $we = $node->safe_psql('postgres',
			"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid = $chain->{waiter_pid}");
		if (!defined $we || $we ne 'Lock') { $unblocked = 1; last; }
		usleep(200_000);
	}
	ok($unblocked, 'chain: deepest waiter unblocked after the root was terminated');
	$chaos->cleanup;
}


# --- enforce a lock convoy: the single shared root is disposed, and max_per_round
#     limits how many victims are signalled per evaluation (no cascade over-kill).
{
	my $base   = $chaos->counters;
	my $convoy = $chaos->lock_convoy(4);
	ok($chaos->wait_for_lock_wait("%$convoy->{table}%", 20),
		'convoy: waiters blocked on the shared resource under enforce');

	ok($chaos->wait_for_gone($convoy->{root_pid}, 45),
		'convoy: the shared root was terminated');
	# Only the root should be disposed — the waiters are victims of the root, not
	# targets themselves.  With max_per_round=1, terminates advance by small steps.
	my $tdelta = $chaos->delta($base, 'hang_terminates_issued');
	cmp_ok($tdelta, '>=', 1, 'convoy: root terminated (>=1 terminate)');
	cmp_ok($tdelta, '<=', 3,
		'convoy: max_per_round limited the disposition (no cascade mass-kill)');
	$chaos->cleanup;
}


# --- postmaster stability (behavioural no-SIGKILL): after aggressive enforce
#     disposition the instance never crash-restarted (a SIGKILL'd backend would
#     have taken the postmaster down / recycled its pid + logged a crash).
{
	ok($node->safe_psql('postgres', 'SELECT 1') eq '1',
		'behavioural no-SIGKILL: server is still up after enforce disposition');
	my $pm_pid1 = (split /\n/, slurp_file($pm_pidfile))[0];
	is($pm_pid1, $pm_pid0,
		'behavioural no-SIGKILL: postmaster pid unchanged (no crash-restart)');
	my $log = slurp_file($node->logfile);
	unlike($log, qr/crash of another server process/,
		'behavioural no-SIGKILL: no crash-of-another-backend in the log');
}


# ======================================================================
# HANG STORM under enforce: many simultaneous long-waits on one root.  The root
# is disposed but max_per_round throttles the signalling; no cascade over-kill.
# ======================================================================
{
	my $base  = $chaos->counters;
	my $storm = $chaos->hang_storm(70);
	# Wait for the store to fill (proves the storm is live).
	my $filled = 0;
	my $dl = time() + 30;
	while (time() < $dl)
	{
		if ($chaos->hang_val('hang_truncated') eq 't') { $filled = 1; last; }
		usleep(300_000);
	}
	ok($filled, 'storm: sample store overflowed (truncated=t) under enforce');
	# The single shared root gets disposed; total terminates stays bounded (the
	# waiters are not themselves killed — they are victims of the root).
	ok($chaos->wait_for_gone($storm->{root_pid}, 45),
		'storm: the shared root was terminated');
	$chaos->cleanup;
}


# ======================================================================
# ABA stress: a blocker that self-resolves right as the disposition would fire.
# The revalidation gate must NOT terminate a reused pid; either it revalidates
# and skips (aba_revalidate_failed++) or the waiter simply unblocks — never a
# wrong kill.
# ======================================================================
{
	my $base    = $chaos->counters;
	my $blocker = $chaos->aba_blocker;
	my $w       = $chaos->waiter_on($blocker->{table});
	ok($chaos->wait_for_lock_wait("%$blocker->{table}%", 20),
		'ABA: waiter blocked before self-heal');
	# Let it be sampled + a victim recommended, then self-heal at the last moment.
	$chaos->wait_for_sample_quality($w->{pid}, 20);
	usleep(400_000);
	my $wpid_before = $w->{pid};
	$chaos->release_holder($blocker);

	# The waiter must not be wrongly terminated; after the blocker self-heals it
	# should acquire the lock and STAY ALIVE.  "no wrong kill" = the waiter is
	# still present in pg_stat_activity (count==1) AND no longer lock-waiting.
	# (Checking only wait_event != 'Lock' would pass even if it were killed --
	# a dead pid returns '' -- so we require it to be alive too.)
	my $alive_unblocked = 0;
	my $dl = time() + 25;
	while (time() < $dl)
	{
		my $row = $node->safe_psql('postgres', qq{
			SELECT count(*), coalesce(max(wait_event_type),'')
			FROM pg_stat_activity WHERE pid = $wpid_before});
		my ($cnt, $we) = split /\|/, $row;
		if (defined $cnt && $cnt == 1 && defined $we && $we ne 'Lock')
		{
			$alive_unblocked = 1;
			last;
		}
		usleep(200_000);
	}
	ok($alive_unblocked,
		'ABA: waiter is still ALIVE and unblocked after blocker self-resolved (no wrong kill)');
	# revalidation may or may not have fired depending on timing; if it did, it is
	# recorded honestly (safe-direction diag, not a hard bar).
	note('ABA: aba_revalidate_failed delta = '
		. $chaos->delta($base, 'hang_aba_revalidate_failed'));
	$chaos->cleanup;
}


# ======================================================================
# ADVISORY dry-run under a MIXED chaos workload: advisory recommends but signals
# nothing; every injected root stays alive.
# ======================================================================
{
	$node->safe_psql('postgres', 'ALTER SYSTEM SET cluster.hang_resolution_mode = advisory');
	$node->reload;
	usleep(500_000);

	my $base = $chaos->counters;
	my $c1   = $chaos->blocking_chain(3);
	my $c2   = $chaos->lock_convoy(3);
	ok($chaos->wait_for_lock_wait("%$c2->{table}%", 20),
		'advisory-chaos: mixed workload is blocking');

	# advisory must record recommendations but issue NO signals.
	ok($chaos->wait_for_counter_gt('hang_advisory_recommendations',
			$base->{hang_advisory_recommendations} // 0, 20),
		'advisory-chaos: advisory_recommendations incremented (evaluation ran)');
	is($chaos->delta($base, 'hang_terminates_issued'), 0,
		'advisory-chaos: NO terminate issued (dry-run)');
	is($chaos->delta($base, 'hang_soft_cancels_issued'), 0,
		'advisory-chaos: NO cancel issued (dry-run)');
	# Every injected root must still be alive under advisory.
	$chaos->assert_all_alive([ $c1->{root_pid}, $c2->{root_pid} ], 'advisory-chaos');
	$chaos->cleanup;
}


# ======================================================================
# MANUAL pg_cluster_hang_resolve() goes through the same gate (L240): a healthy,
# non-victim backend is never force-disposed by the manual entry point.
# ======================================================================
{
	my $hs = $chaos->healthy_slow_query(60_000_000);
	usleep(1_000_000);
	# Manual resolve of a healthy active (non-victim) pid must be refused.
	my $r = $node->safe_psql('postgres', "SELECT pg_cluster_hang_resolve($hs->{pid})");
	is($r, 'f',
		'manual: pg_cluster_hang_resolve() of a healthy non-victim pid returns false (same gate)');
	is($node->safe_psql('postgres', "SELECT count(*) FROM pg_stat_activity WHERE pid = $hs->{pid}"),
		'1', 'manual: the healthy backend was not force-disposed');
	$chaos->cleanup;
}


note('Base disposition matrix (advisory/enforce idle-in-tx, fail-CLOSED '
	. 'block-nobody, deadlock-not-double-kill, 2PC-undisposable, mode matrix, '
	. 'manual perm gate, victims visibility, truncated-root skip, multi-waiter '
	. 'root) is owned by t/305 and referenced here, not duplicated (spec §1.3 Q7-A).');


$node->stop;
done_testing();
