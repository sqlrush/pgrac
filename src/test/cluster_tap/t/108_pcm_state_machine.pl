#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 108_pcm_state_machine.pl
#	  spec-2.30 surface smoke for PCM 9-state machine activation.
#
#	  TAP L1-L7 verify SQL-visible surface only;  behavioral transition
#	  tests run in cluster_unit test_cluster_pcm_lock (26 tests).  TAP
#	  does NOT directly invoke C internal cluster_pcm_lock_acquire /
#	  release / upgrade / downgrade APIs (Q7 user lock — no SQL-callable
#	  binding).
#
#	  L1  default cluster.pcm_grd_max_entries=-1 → auto NBuffers + pcm
#	       category visible in pg_cluster_state
#	  L2  explicit cluster.pcm_grd_max_entries=0 → PCM disabled surface
#	  L3  dump_pcm existing 6 row + 14 NEW row = total 20 + api_state
#	       string value "active"/"stub"
#	  L4  9 transition counter rows present + non-negative
#	  L5  ClusterPcmTransitionApply wait event in pg_stat_cluster_wait_events
#	  L6  wait event baseline 75 → 77 → 78 → 79 (spec-2.32 +GCS_REPLY_WAIT;PCM_GRD_INIT + PCM_TRANSITION_APPLY)
#	  L7  no PCM wire opcode smoke (L107 N+5 严守:no PCM wire enum added)
#
# Spec: spec-2.30-pcm-9-state-machine-activation.md (FROZEN v0.3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


# ============================================================
# Default startup — cluster.pcm_grd_max_entries=-1 sentinel auto-resolve.
# ============================================================
my $node_default = PgracClusterNode->new('pcm_default');
$node_default->init;
$node_default->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_default->start;

# L1 — default activation visible
my $pcm_category_rows = $node_default->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_cluster_state WHERE category = 'pcm'");
is($pcm_category_rows, '23',
   'L1 pg_cluster_state pcm category has 23 rows (spec-2.30 D9 surface + spec-6.14a D5 + spec-6.14 D5 KO-aux-defer counter + spec-4.6a D12 dead_cleanup_entries)');

# L3 — api_state shows "active" when GUC=-1 default
my $api_state_default = $node_default->safe_psql(
	'postgres',
	"SELECT value FROM pg_cluster_state WHERE category='pcm' AND key='pcm_api_state'");
is($api_state_default, 'active',
   'L3 pcm_api_state = "active" when cluster.pcm_grd_max_entries=-1 (default)');

# L4 — 9 transition counter rows present + each is a non-negative integer
my $counter_count = $node_default->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_cluster_state WHERE category='pcm' AND key LIKE 'trans_%_count'");
is($counter_count, '9',
   'L4 9 transition counter rows present in pg_cluster_state.pcm');

my $any_negative = $node_default->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_cluster_state WHERE category='pcm' AND key LIKE 'trans_%_count' AND value::bigint < 0");
is($any_negative, '0',
   'L4 all 9 transition counter values are non-negative');

# L5 — ClusterPcmTransitionApply wait event registered
my $apply_event = $node_default->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_stat_cluster_wait_events WHERE name = 'ClusterPcmTransitionApply'");
is($apply_event, '1',
   'L5 ClusterPcmTransitionApply wait event registered');

# L6 — wait event count baseline through spec-6.1.
my $wait_event_count = $node_default->safe_psql(
	'postgres', "SELECT count(*) FROM pg_stat_cluster_wait_events");
is($wait_event_count, '118',
   'L6 wait event baseline 118 (spec-6.13 RDMA wait surface)');

# L7 — no PCM wire opcode smoke (no SQL-visible PCM wire opcode enum surface)
my $pcm_grd_init_event = $node_default->safe_psql(
	'postgres',
	"SELECT count(*) FROM pg_stat_cluster_wait_events WHERE name = 'ClusterPcmGrdInit'");
is($pcm_grd_init_event, '1',
   'L7 ClusterPcmGrdInit wait event registered (no PCM wire opcode);  L107 N+5 严守');

$node_default->stop;


# ============================================================
# Explicit disable — cluster.pcm_grd_max_entries=0.
# ============================================================
my $node_disable = PgracClusterNode->new('pcm_disable');
$node_disable->init;
$node_disable->append_conf('postgresql.conf',
	"cluster.node_id = 0\n" . "cluster.pcm_grd_max_entries = 0\n");
$node_disable->start;

# L2 — api_state shows "stub" when explicit disable
my $api_state_disable = $node_disable->safe_psql(
	'postgres',
	"SELECT value FROM pg_cluster_state WHERE category='pcm' AND key='pcm_api_state'");
is($api_state_disable, 'stub',
   'L2 pcm_api_state = "stub" when cluster.pcm_grd_max_entries=0 explicit disable');

$node_disable->stop;

done_testing();
