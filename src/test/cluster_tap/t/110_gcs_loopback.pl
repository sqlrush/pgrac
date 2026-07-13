#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 110_gcs_loopback.pl
#	  spec-2.32 end-to-end integration of GCS request protocol skeleton on
#	  a single-node cluster.  Production master==self short-circuits before
#	  any wire send (HC72), so wire path coverage is effectively limited
#	  to SQL-visible surface invariants:
#
#	  L1  fresh cluster startup:  pg_cluster_state.gcs has 108 keys
#	  L2  api_state = "active" after postmaster phase 1 init
#	  L3  WAIT_EVENT_GCS_REPLY_WAIT registered in pg_stat_cluster_wait_events
#	  L4  CLUSTER_WAIT_EVENTS_COUNT == 123 (cumulative through spec-7.2)
#	  L5  msg_type registry surface visible:  pg_cluster_ic_msg_types has
#	       gcs_request + gcs_reply rows
#	  L6  workload (SELECT/UPDATE/VACUUM) does NOT inc send_request_count
#	       (HC72 production master==self short-circuits;  wire path test-only)
#	  L7  lookup_master_self_count stays 0 in no-peer single-node fallback;
#	       spec-3.4c no-peer perf gate skips PCM/GCS lookup entirely
#	  L8  outstanding_count = 0 baseline (no in-flight cross-node requests)
#	  L9  cluster.enabled=off restart leaves api_state="stub"-or-zeroed
#	       (4-layer gate skips all PCM paths, GCS module still inits but
#	        sees no producer activity)
#	  L10 reply_late_drop_count + reply_timeout_count = 0 (no stale or
#	       timed-out replies on healthy startup)
#
#	  Behavioral wire path coverage (encode/decode round-trip, dispatch
#	  handler invocation, CV signal correctness) lives in
#	  cluster_unit test_cluster_gcs_dispatch L1-L18 + spec-2.33 real
#	  cross-node send TAP.
#
# Spec: spec-2.32-gcs-request-protocol-skeleton.md §4.2 (FROZEN v0.4)
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


sub gcs_value {
	my ($node, $key) = @_;

	return $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='gcs' AND key='$key'});
}


# ============================================================
# Default cluster — cluster.enabled=true + pcm_grd_max_entries=-1 (auto),
# but no declared peers.  Since spec-3.4c, single-node fallback skips
# PCM/GCS hot-path lookup entirely to avoid RAC write-side cost.
# ============================================================
my $node = PgracClusterNode->new('gcs_loopback');
$node->init;
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->start;


# L1 — pg_cluster_state.gcs surface has 108 keys (round-4c fallback-scn + spec-7.2 D6 hist).
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '108',
   'L1 pg_cluster_state.gcs category has 108 keys (round-4c +3 fallback-scn rows; gcs-race-fix-2 +6 rows; serve-stall round-5 +6 send-admission +4 bounded-drop rows) (spec-7.2 D6)');


# L2 — api_state = "active" after postmaster phase 1 init.
is(gcs_value($node, 'api_state'), 'active',
   'L2 cluster_gcs_get_api_state = "active" after module init');


# L3 — WAIT_EVENT_GCS_REPLY_WAIT registered in pg_stat_cluster_wait_events.
my $gcs_reply_wait_event = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_cluster_wait_events
	   WHERE name='ClusterGcsReplyWait'});
is($gcs_reply_wait_event, '1',
   'L3 ClusterGcsReplyWait wait event registered (spec-2.32 D7)');


# L4 — CLUSTER_WAIT_EVENTS_COUNT == 120 (spec-7.2 D6).
my $total_wait_events = $node->safe_psql(
	'postgres', 'SELECT count(*) FROM pg_stat_cluster_wait_events');
is($total_wait_events, '123',
	'L4 wait_events count 120 (spec-7.2 D6 LMS data-plane wait surface)');


# L6 — Production workload does NOT trigger wire path (HC72 short-circuit).
my $send_before = gcs_value($node, 'send_request_count');
$node->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int, val text);
	INSERT INTO heap_t SELECT g, 'r' || g FROM generate_series(1, 100) g;
	SELECT count(*) FROM heap_t;
	UPDATE heap_t SET val = val || '!' WHERE id <= 50;
	VACUUM heap_t;
});
my $send_after = gcs_value($node, 'send_request_count');
is($send_after, $send_before,
   "L6 production master==self short-circuit:  send_request_count unchanged ($send_before → $send_after) — HC72 wire path test-only");


# L7 — no-peer gate skips lookup_master entirely.
my $self_after = gcs_value($node, 'lookup_master_self_count');
is($self_after, '0',
   "L7 no-peer lookup_master_self_count stays 0 after workload ($self_after)");


# L8 — outstanding_count = 0 (no in-flight cross-node requests).
is(gcs_value($node, 'outstanding_count'), '0',
   'L8 outstanding_count = 0 (no in-flight cross-node requests in single-node)');


# L10 — no stale or timed-out replies on healthy startup.
is(gcs_value($node, 'reply_late_drop_count'), '0',
   'L10a reply_late_drop_count = 0 on healthy startup');
is(gcs_value($node, 'reply_timeout_count'), '0',
   'L10b reply_timeout_count = 0 on healthy startup');


# L5 — msg_type registry surface includes gcs_request + gcs_reply rows.
my $gcs_rows = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_cluster_ic_msg_types
	   WHERE name IN ('gcs_request', 'gcs_reply')});
is($gcs_rows, '2',
   'L5 pg_cluster_ic_msg_types has gcs_request + gcs_reply rows');


$node->stop;


# ============================================================
# L9 — cluster.enabled=off:  4-layer gate skips PCM hook;  GCS module
# still inits but no producer activity.
# ============================================================
my $node_off = PgracClusterNode->new('gcs_loopback_disabled');
$node_off->init;
# spec-5.7 §3.1d: the relation-extend (HW) gate keys on
# cluster.relation_extend_lock_enabled + node_id >= 0, independent of
# cluster.enabled.  With cluster.enabled=off the CSSD is not running, so the
# heap_t INSERT below would engage the HW gate and fail closed.  This section
# tests the GCS-loopback disabled surface, not HW, so disable the HW gate too.
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\n"
	  . "cluster.enabled = off\n"
	  . "cluster.relation_extend_lock_enabled = off\n");
$node_off->start;

$node_off->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int);
	INSERT INTO heap_t SELECT g FROM generate_series(1, 100) g;
	SELECT count(*) FROM heap_t;
});

# With cluster.enabled=off, cluster_pcm_is_active returns false, so the
# PCM acquire path never executes;  hence cluster_gcs_lookup_master is
# never called either, so all GCS counters stay at 0.
is(gcs_value($node_off, 'send_request_count'), '0',
   'L9a cluster.enabled=off:  send_request_count = 0 (gate skips PCM/GCS)');
is(gcs_value($node_off, 'lookup_master_self_count'), '0',
   'L9b cluster.enabled=off:  lookup_master_self_count = 0 (PCM hook skipped)');

$node_off->stop;


done_testing();
