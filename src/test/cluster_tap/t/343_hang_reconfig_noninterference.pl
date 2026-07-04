#-------------------------------------------------------------------------
#
# 343_hang_reconfig_noninterference.pl
#    spec-5.20 D5 -- Hang Manager x reconfig / multi-node non-interference +
#    cross-node honesty (HG#5).
#
#    Proves the (local-only, spec-5.12 contract) Hang Manager stays correct
#    across a real cluster reconfig and never over-reaches across nodes:
#      - the sampler loop is alive under an active multi-node cluster (3/4
#        node): the per-round hang_sample_epoch advances;
#      - a fail-stop reconfig (SIGKILL one node) does NOT crash or disable the
#        survivors' Hang Managers, does NOT change the survivor postmasters, and
#        issues NO disposition during a pure reconfig window with no injected
#        local hang (reconfig-transient cross-node waits are sampled
#        REMOTE_BOUNDARY -> non-actionable, never disposed);
#      - the survivors' managers keep sampling AFTER the reconfig (it did not
#        break them), and the surviving cluster still serves queries;
#      - cross-node honesty: the manager only ever acts on LOCAL actionable
#        hangs.  pgrac cross-node row conflicts fail-closed rather than hang
#        (spec-3.4d / t/209), so there is no cross-node actionable hang for the
#        local manager to mis-dispose; it never claims to resolve cross-node
#        hangs (5.12 local-only, spec §3.1 HG#5).
#
#    Local disposition correctness (idle-in-tx / chain / convoy actually
#    TERMINATED) is proven single-node in t/340 (detection) + t/341
#    (remediation) + t/305; this multi-node leg is scoped to non-interference
#    and cross-node honesty (spec §1.4.1 phasing).
#
#    HG#5(a) 3-node faithful (ClusterTriple) = REQUIRED.
#    HG#5(b) 4-node (ClusterQuad, merged via spec-5.19) = the integrated leg.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/343_hang_reconfig_noninterference.pl
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
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


# Fast-hang cluster GUC lines + enforce mode, applied to every node.  enforce is
# deliberate: if the manager were going to mis-dispose a reconfig-transient wait,
# enforce mode is where it would actually terminate a backend.
my @HANG_CONF = (
	'cluster.diag_main_loop_interval = 200',
	'cluster.hang_manager_enabled = on',
	'cluster.hang_sample_interval_ms = 200',
	'cluster.hang_threshold_ms = 1500',
	'cluster.hang_dump_enabled = on',
	'cluster.hang_resolution_mode = enforce',
	'cluster.hang_resolution_confirm_rounds = 1',
	'cluster.hang_resolution_soft_timeout_ms = 1000',
	'cluster.hang_resolution_max_per_round = 1',
	'deadlock_timeout = 500',
	'log_min_messages = warning',
	'autovacuum = off',
);

sub hang_key
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT coalesce(value,'') FROM pg_cluster_state WHERE category='hang' AND key='$key'");
	return defined $v ? $v : '';
}

sub wait_in_quorum
{
	my ($node, $secs) = @_;
	my $deadline = time() + ($secs // 30);
	while (time() < $deadline)
	{
		my $q = $node->safe_psql('postgres',
			'SELECT in_quorum FROM pg_cluster_quorum_state');
		return 1 if defined $q && $q eq 't';
		usleep(300_000);
	}
	return 0;
}

# The manager loop is alive iff the per-round hang_sample_epoch advances.
sub epoch_advances
{
	my ($node, $secs) = @_;
	my $e0 = hang_key($node, 'hang_sample_epoch');
	my $deadline = time() + ($secs // 8);
	while (time() < $deadline)
	{
		my $e1 = hang_key($node, 'hang_sample_epoch');
		return 1 if $e1 =~ /^\d+$/ && $e0 =~ /^\d+$/ && $e1 > $e0;
		usleep(300_000);
	}
	return 0;
}

sub pm_pid
{
	my ($node) = @_;
	return (split /\n/, slurp_file($node->data_dir . '/postmaster.pid'))[0];
}

# Cross-node honesty (best-effort observation): scan every per-sample row on
# $node; for any sample classified 'remote_boundary' (a cross-node blocker,
# e.g. a survivor waiting on a GES reply from the dead node during reconfig),
# assert it is NON-actionable -- it must NOT be advertised as a hang victim (the
# local-only manager never disposes a cross-node waiter).  Returns the count of
# remote_boundary samples observed (may be 0; the classification itself is
# unit-tested in test_cluster_hang / acceptance L6, this asserts the runtime
# safety direction whenever such a sample actually materialises).
sub assert_remote_boundary_non_actionable
{
	my ($node, $label) = @_;
	my $rows = $node->safe_psql('postgres', qq{
		SELECT p.value
		FROM pg_cluster_state q
		JOIN pg_cluster_state p
		  ON p.category='hang'
		 AND p.key = 'hang_sample' || (regexp_match(q.key, '^hang_sample(\\d+)_quality\$'))[1] || '_pid'
		WHERE q.category='hang'
		  AND q.key ~ '^hang_sample\\d+_quality\$'
		  AND q.value = 'remote_boundary'});
	my @pids = grep { /^\d+$/ } split /\n/, ($rows // '');
	for my $pid (@pids)
	{
		my $n = $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $pid");
		Test::More::is($n, '0',
			"$label: remote_boundary sample pid $pid is NON-actionable (never a hang victim)");
	}
	Test::More::note("$label: observed " . scalar(@pids)
		. " remote_boundary cross-node sample(s) during the window "
		. "(classification unit-tested; 0 is legal -- cross-node hangs fail-closed/progress)");
	return scalar(@pids);
}


# ======================================================================
# HG#5(a) -- 3-node faithful (ClusterTriple).  REQUIRED.
# ======================================================================
{
	my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
		'hang_reconfig3',
		quorum_voting_disks => 3,
		extra_conf          => [@HANG_CONF]);
	$triple->start_triple;

	for my $i (0 .. 2)
	{
		ok(wait_in_quorum($triple->node($i), 40),
			"3-node L1: node$i reached in_quorum=t");
	}

	# L2: the sampler loop is alive under the live 3-node cluster.
	ok(epoch_advances($triple->node(0), 8),
		'3-node L2: Hang Manager sampler loop alive (sample_epoch advances)');

	# L3: fail-stop reconfig -- SIGKILL node2.  Snapshot survivor disposition
	# counters + postmaster pids BEFORE, run a pure reconfig window with NO
	# injected local hang, and assert no survivor spuriously disposed anything.
	my @surv = (0, 1);
	my %term_before = map { $_ => hang_key($triple->node($_), 'hang_terminates_issued') } @surv;
	my %pm_before   = map { $_ => pm_pid($triple->node($_)) } @surv;

	$triple->kill_node9(2);
	usleep(8_000_000);	# survivors detect the dead node + reconfig

	for my $i (@surv)
	{
		my $n = $triple->node($i);
		is($n->safe_psql('postgres', 'SELECT 1'), '1',
			"3-node L3: survivor node$i still serves queries after fail-stop reconfig");
		is(pm_pid($n), $pm_before{$i},
			"3-node L3: survivor node$i postmaster unchanged (no crash / no self-SIGKILL)");
		is(hang_key($n, 'hang_terminates_issued'), $term_before{$i},
			"3-node L3: survivor node$i issued NO disposition during the pure reconfig window "
			. "(reconfig-transient cross-node waits not mis-disposed)");
		my $log = slurp_file($n->logfile);
		unlike($log, qr/crash of another server process/,
			"3-node L3: survivor node$i has no crash-of-backend in the log");
	}

	# L4: the survivors' managers keep sampling after the reconfig.
	ok(epoch_advances($triple->node(0), 8),
		'3-node L4: survivor Hang Manager still sampling after reconfig (not broken)');

	# L4b: any cross-node remote_boundary sample on a survivor is non-actionable.
	assert_remote_boundary_non_actionable($triple->node(0), '3-node L4b');
	assert_remote_boundary_non_actionable($triple->node(1), '3-node L4b');

	$triple->stop_triple;
}

note('cross-node honesty: pgrac cross-node ROW conflicts fail-closed (53R98, '
	. 'spec-3.4d / t/209) rather than hang, and a genuine cross-node GES wait is '
	. 'sampled REMOTE_BOUNDARY -> non-actionable; there is thus no cross-node '
	. 'actionable hang for the local manager to mis-dispose (5.12 local-only).');


# ======================================================================
# HG#5(b) -- 4-node integrated (ClusterQuad, merged via spec-5.19).
#
# The 4-node leg needs 4 postmasters up at once; each pgrac node claims several
# SysV shared-memory segments, so a SysV-constrained host (macOS default
# kern.sysv.shmmni = 32, easily exhausted by orphaned segments from prior
# SIGKILL'd cluster runs) cannot start the quad.  This is an honest ENV SKIP,
# NOT a silent pass: HG#5(b) runs for real on the Linux nightly shard (SHMMNI =
# 4096), the same place spec-5.19's ClusterQuad legs run (spec §3.1 HG#5b).
# ======================================================================
SKIP: {
	my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
		'hang_reconfig4',
		quorum_voting_disks => 4,
		extra_conf          => [@HANG_CONF]);

	my $started = 1;
	for my $i (0 .. 3)
	{
		unless ($quad->node($i)->start(fail_ok => 1))
		{
			$started = 0;
			last;
		}
	}
	unless ($started)
	{
		eval { $quad->stop_quad; };
		skip "4-node ClusterQuad could not start locally (SysV SHMMNI exhaustion, "
			. "typical on macOS); HG#5(b) 4-node runs for real on the Linux nightly "
			. "shard (spec §3.1 HG#5b, same runner as spec-5.19 ClusterQuad)", 15;
	}
	Test::More::note('ClusterQuad started for HG#5(b) 4-node integrated leg');

	for my $i (0 .. 3)
	{
		ok(wait_in_quorum($quad->node($i), 50),
			"4-node L5: node$i reached in_quorum=t");
	}

	ok(epoch_advances($quad->node(0), 8),
		'4-node L6: Hang Manager sampler loop alive under the 4-node cluster');

	# L7: fail-stop reconfig -- SIGKILL node3; survivors 0,1,2 keep their managers.
	my @surv = (0, 1, 2);
	my %term_before = map { $_ => hang_key($quad->node($_), 'hang_terminates_issued') } @surv;
	my %pm_before   = map { $_ => pm_pid($quad->node($_)) } @surv;

	$quad->kill_node9(3);
	usleep(8_000_000);

	for my $i (@surv)
	{
		my $n = $quad->node($i);
		is($n->safe_psql('postgres', 'SELECT 1'), '1',
			"4-node L7: survivor node$i still serves queries after fail-stop reconfig");
		is(pm_pid($n), $pm_before{$i},
			"4-node L7: survivor node$i postmaster unchanged (no crash)");
		is(hang_key($n, 'hang_terminates_issued'), $term_before{$i},
			"4-node L7: survivor node$i issued NO disposition during the pure reconfig window");
	}

	ok(epoch_advances($quad->node(0), 8),
		'4-node L8: survivor Hang Manager still sampling after reconfig');

	# L8b: any cross-node remote_boundary sample on a survivor is non-actionable.
	assert_remote_boundary_non_actionable($quad->node(0), '4-node L8b');
	assert_remote_boundary_non_actionable($quad->node(1), '4-node L8b');

	$quad->stop_quad;
}


done_testing();
