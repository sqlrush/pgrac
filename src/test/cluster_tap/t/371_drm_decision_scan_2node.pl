#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 371_drm_decision_scan_2node.pl
#	  spec-7.6 6.3c — DRM hotness decision scan, 2-node value leg.
#
#	  End-to-end proof that the decision half runs on top of the 6.3b collection
#	  substrate: under a SKEWED workload (node1 hammers a wide key range while
#	  node0 stays idle), the shards node0 masters accumulate node1 as the sole
#	  dominant accessor.  Once that dominance is sustained across the configured
#	  consecutive tumbling windows, node0's LMON decision scan PROPOSES migrating
#	  those masters to node1 (dominant != current master, benefit > cost).
#
#	  Wave 6.3c proposes only — nothing is executed here (the live remaster
#	  executor is 6.3d), so the assertions read the report-only observability
#	  surface (dump category drm_affinity: scans_run / scan_proposed / per-reason).
#
#	  A short-window profile (window 500 ms, scan 200 ms, 2 consecutive windows,
#	  cost 1) makes the first proposal land within a couple of seconds; these are
#	  test-fixture values, not the conservative production defaults.
#
#	  Legs:
#	    L1 — the decision scan actually runs on node0 (scans_run advances).
#	    L2 — sustained skew yields at least one auto-actionable proposal.
#	    L3 — with drm_manual_only off, every migrate verdict is a proposal
#	         (scan_migrate == scan_proposed: no double-count / no drop).
#	    L4 — drm_manual_only ON suppresses auto-proposals while the decision keeps
#	         firing (proposed delta 0, migrate-reason delta > 0).
#
#	  Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use Time::HiRes qw(usleep);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

# drm_affinity dump counter accessor (-1 when the key is absent).
sub drm_ctr
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() "
		. "WHERE category = 'drm_affinity' AND key = '$key'");
	return defined($v) && $v ne '' ? $v + 0 : -1;
}

# One transaction that takes a small, fixed set of distinct advisory locks: every
# key issues a first-logical GES REQUEST, and the roughly-half that node0 masters
# route to it, where node1 becomes the dominant (only) accessor for those shards.
# Kept intentionally small + concentrated (hammered every round) so the per-shard
# heat builds fast without storming the 2-node GES with a wide lock set.
sub drive_round
{
	my ($node, $base, $count) = @_;
	$node->safe_psql('postgres', qq{
		DO \$\$
		DECLARE i int;
		BEGIN
			FOR i IN 0..$count LOOP
				PERFORM pg_advisory_xact_lock($base + i);
			END LOOP;
		END \$\$;
	});
}

my $SKEW_BASE = 5_100_000;
my $SKEW_KEYS = 24;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'drm371',
	quorum_voting_disks => 3,
	extra_conf => [
		'cluster.shared_storage_backend = local',
		'cluster.grd_max_entries = 4096',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		# DRM decision ON with a deterministic short-window test profile (NOT the
		# conservative production defaults).
		'cluster.drm_enabled = on',
		'cluster.drm_affinity_sample_rate = 1',
		'cluster.drm_min_access_count = 1',
		'cluster.drm_affinity_ratio_pct = 60',
		'cluster.drm_consecutive_triggers = 2',
		'cluster.drm_affinity_window_ms = 800',
		'cluster.drm_scan_interval_ms = 300',
		# cooldown must be >> window: the net-benefit gate caps expected residence
		# at cooldown/window windows, so a cooldown < window would floor it to 0.
		'cluster.drm_cooldown_ms = 30000',
		'cluster.drm_migration_cost = 1',
		'cluster.drm_max_migrations_per_scan = 64',
		'cluster.drm_manual_only = off',
	]);
$pair->start_pair;

$pair->wait_for_peer_state(0, 1, 'connected', 30)
  or BAIL_OUT('node0 did not observe peer 1 connected');
$pair->wait_for_peer_state(1, 0, 'connected', 30)
  or BAIL_OUT('node1 did not observe peer 0 connected');
usleep(3_000_000);    # CSSD READY + quorum + LMS warmup settle

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'node0 accepts SQL');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'node1 accepts SQL');

# ---- Phase 1: sustained skew from node1, poll node0's cumulative proposals ----
my $proposed = 0;
for my $round (1 .. 80) {
	drive_round($pair->node1, $SKEW_BASE, $SKEW_KEYS);
	usleep(250_000);
	$proposed = drm_ctr($pair->node0, 'scan_proposed');
	last if $proposed > 0;
}

my $scans_run = drm_ctr($pair->node0, 'scans_run');
my $migrate = drm_ctr($pair->node0, 'scan_migrate');
note("node0 scans_run=$scans_run scan_proposed=$proposed scan_migrate=$migrate");

# Diagnostic: full drm_affinity surface on both nodes (why did/didn't we propose).
for my $nlabel ([$pair->node0, 'node0'], [$pair->node1, 'node1']) {
	my ($n, $lbl) = @$nlabel;
	my $dump = $n->safe_psql('postgres',
		"SELECT string_agg(key || '=' || value, ' ' ORDER BY key) "
		. "FROM cluster_dump_state() WHERE category = 'drm_affinity'");
	diag("$lbl drm_affinity: $dump");
}

# L1 — the LMON decision scan runs on node0.
cmp_ok($scans_run, '>', 0, 'L1 node0 DRM decision scan runs (scans_run advanced)');

# L2 — sustained skew produces at least one auto-actionable proposal (full
# pipeline: collect -> tumbling window -> decision -> propose).
cmp_ok($proposed, '>', 0, 'L2 node0 proposes a remaster under sustained node1 skew');

# L3 — manual_only off: every migrate verdict is an auto-proposal (no drop / no
# double-count in the accounting).
is($migrate, $proposed, 'L3 scan_migrate == scan_proposed while drm_manual_only is off');

# ---- Phase 2: manual_only ON suppresses auto-proposals, decision still fires ----
$pair->node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.drm_manual_only = on');
$pair->node0->reload;
usleep(1_500_000);    # let the LMON process re-read the SIGHUP'd GUC

my $p_before = drm_ctr($pair->node0, 'scan_proposed');
my $m_before = drm_ctr($pair->node0, 'scan_migrate');

for my $round (1 .. 40) {
	drive_round($pair->node1, $SKEW_BASE, $SKEW_KEYS);
	usleep(250_000);
}

my $p_after = drm_ctr($pair->node0, 'scan_proposed');
my $m_after = drm_ctr($pair->node0, 'scan_migrate');
note("manual_only: proposed $p_before -> $p_after, migrate $m_before -> $m_after");

# L4a — no new auto-actionable proposal while manual_only is on.
is($p_after - $p_before, 0, 'L4a drm_manual_only suppresses auto-proposals');

# L4b — the decision still fires (observability retained): migrate reason advances.
cmp_ok($m_after - $m_before, '>', 0, 'L4b decision still evaluates MIGRATE under manual_only');

$pair->stop_pair if $pair->can('stop_pair');

done_testing();
