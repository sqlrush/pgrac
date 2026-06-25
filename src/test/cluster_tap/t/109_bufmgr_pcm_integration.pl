#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 109_bufmgr_pcm_integration.pl
#	  spec-2.31 end-to-end integration of bufmgr LockBuffer / LockBuffer-
#	  ForCleanup PCM hook driving the spec-2.30 9-state machine.  This
#	  TAP exercises the real PG hot path:  SELECT/UPDATE/VACUUM/temp
#	  table workloads on a single-node cluster + verifies dump_pcm
#	  transition counters and pg_cluster_state.pcm active surface react
#	  as expected.
#
#	  L1  fresh cluster:  capture startup baseline counters
#	  L2  single-node/no-peer SELECT * FROM heap_t:  trans_n_to_s_count stays 0
#	       (spec-3.4c no-peer perf gate skips PCM/GCS hot path)
#	  L3  single-node/no-peer UPDATE heap_t:  X ownership counters stay 0
#	  L4  single-node/no-peer VACUUM heap_t:  X ownership counters stay 0
#	  L5  restart with cluster.enabled=off:  counter doesn't inc (Layer 2 gate)
#	  L6  restart with cluster.pcm_grd_max_entries=0 + shared_buffers=128kB:
#	       counter doesn't inc (Layer 3 gate);  shared_buffers配套 防 HC62 FATAL
#	  L7  temp table workload:  diagnostic counter delta recorded
#	       (NOT hard-zero asserted — temp DDL touches catalog shared buffer;
#	        hard local-buffer skip covered by cluster_unit L6)
#	  L8  workload no-peer state:  pcm_api_state may be active, but
#	       pcm_grd_active_entries stays 0 because no peer means no PCM tax
#
# Spec: spec-2.31-bufmgr-pcm-content-lock-hook.md §4.2 (FROZEN v0.5)
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


# Helper:  read a transition counter value from dump_pcm.
sub trans_count {
	my ($node, $key) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state
		   WHERE category='pcm' AND key='$key'});
}


# ============================================================
# Default cluster — cluster.enabled=true + pcm_grd_max_entries=-1 (auto NBuffers),
# but no declared peers.  Since spec-3.4c, cluster_pcm_is_active() includes
# cluster_conf_has_peers(), so single-node fallback does not pay PCM hot-path
# cost.  Real PCM/GCS behavior is covered by the 2-node GCS TAPs and by
# cluster_unit; this file now guards the no-peer perf contract.
# ============================================================
my $node = PgracClusterNode->new('bufmgr_pcm_default');
$node->init;
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->start;


# L1 — cluster startup itself touches shared buffers, so counters may
# already be nonzero before user SQL.  Capture that as the workload baseline.
my $n_to_s_startup = trans_count($node, 'trans_n_to_s_count');
my $n_to_x_startup = trans_count($node, 'trans_n_to_x_count');
ok($n_to_s_startup >= 0,
   "L1 captured startup trans_n_to_s_count baseline = $n_to_s_startup");
ok($n_to_x_startup >= 0,
   "L1 captured startup trans_n_to_x_count baseline = $n_to_x_startup");


# L2 — SELECT would trigger LockBuffer(SHARE), but no-peer gate skips PCM.
$node->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int, val text);
	INSERT INTO heap_t SELECT g, 'r' || g FROM generate_series(1, 100) g;
});
# Capture state after INSERT (which uses LockBuffer EXCLUSIVE), then SELECT.
my $n_to_s_before_select = trans_count($node, 'trans_n_to_s_count');
$node->safe_psql('postgres', 'SELECT count(*) FROM heap_t');
my $n_to_s_after_select = trans_count($node, 'trans_n_to_s_count');
is($n_to_s_after_select, $n_to_s_before_select,
   "L2 no-peer SELECT skips trans_n_to_s_count " .
   "($n_to_s_before_select → $n_to_s_after_select)");


# L3 — UPDATE would trigger LockBuffer(EXCLUSIVE), but no-peer gate skips PCM.
my $n_to_x_before_update = trans_count($node, 'trans_n_to_x_count');
my $s_to_x_before_update = trans_count($node, 'trans_s_to_x_upgrade_count');
$node->safe_psql('postgres', 'UPDATE heap_t SET val = val || \'!\' WHERE id <= 50');
my $n_to_x_after_update = trans_count($node, 'trans_n_to_x_count');
my $s_to_x_after_update = trans_count($node, 'trans_s_to_x_upgrade_count');
is(($n_to_x_after_update + $s_to_x_after_update),
   ($n_to_x_before_update + $s_to_x_before_update),
   "L3 no-peer UPDATE skips X ownership counter " .
   "(N→X $n_to_x_before_update → $n_to_x_after_update; " .
   "S→X $s_to_x_before_update → $s_to_x_after_update)");


# L4 — VACUUM uses LockBufferForCleanup; no-peer gate skips PCM.
my $n_to_x_before_vacuum = trans_count($node, 'trans_n_to_x_count');
my $s_to_x_before_vacuum = trans_count($node, 'trans_s_to_x_upgrade_count');
$node->safe_psql('postgres', 'VACUUM heap_t');
my $n_to_x_after_vacuum = trans_count($node, 'trans_n_to_x_count');
my $s_to_x_after_vacuum = trans_count($node, 'trans_s_to_x_upgrade_count');
is(($n_to_x_after_vacuum + $s_to_x_after_vacuum),
   ($n_to_x_before_vacuum + $s_to_x_before_vacuum),
   "L4 no-peer VACUUM skips X ownership counter " .
   "(N→X $n_to_x_before_vacuum → $n_to_x_after_vacuum; " .
   "S→X $s_to_x_before_vacuum → $s_to_x_after_vacuum)");


# L8 — active surface after workload:  pcm_api_state=active + entries > 0.
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_api_state'}),
   'active',
   'L8 pcm_api_state = active during workload');
ok($node->safe_psql('postgres',
		q{SELECT value::bigint = 0 FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_active_entries'}) eq 't',
   'L8 no-peer pcm_grd_active_entries stays 0 after workload');


# L7 — temp table workload:  diagnostic delta only.
# Temp tables use LocalBuffers; HC68 skips PCM hook.  But the surrounding
# CREATE TEMP TABLE DDL touches catalog (pg_class etc.) which DOES go
# through shared buffer PCM hook.  Record delta as diagnostic, don't
# assert hard zero (hard local-buffer skip is covered by cluster_unit L6).
my $n_to_s_before_temp = trans_count($node, 'trans_n_to_s_count');
my $n_to_x_before_temp = trans_count($node, 'trans_n_to_x_count');
$node->safe_psql('postgres', q{
	CREATE TEMP TABLE temp_t (id int);
	INSERT INTO temp_t SELECT g FROM generate_series(1, 50) g;
	SELECT count(*) FROM temp_t;
});
my $n_to_s_after_temp = trans_count($node, 'trans_n_to_s_count');
my $n_to_x_after_temp = trans_count($node, 'trans_n_to_x_count');
diag("L7 temp table diagnostic delta:  trans_n_to_s "
	 . "$n_to_s_before_temp → $n_to_s_after_temp; "
	 . "trans_n_to_x $n_to_x_before_temp → $n_to_x_after_temp "
	 . "(deltas come from CREATE TEMP TABLE catalog touches, NOT temp_t local-buffer ops)");
pass('L7 temp table workload completes without error (local-buffer skip hard correctness:  cluster_unit L6)');


$node->stop;


# ============================================================
# L5 — cluster.enabled=false:  Layer 2 gate skips hook.
# ============================================================
my $node_off = PgracClusterNode->new('bufmgr_pcm_disabled');
$node_off->init;
# spec-5.7 §3.1d: the relation-extend (HW) gate keys on
# cluster.relation_extend_lock_enabled + node_id >= 0, independent of
# cluster.enabled.  With cluster.enabled=off the CSSD is not running, so the
# heap_t INSERT below would engage the HW gate and fail closed.  This section
# tests the PCM Layer-2 disabled surface, not HW, so disable the HW gate too.
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\n"
	  . "cluster.enabled = off\n"
	  . "cluster.relation_extend_lock_enabled = off\n");
$node_off->start;

$node_off->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int);
	INSERT INTO heap_t SELECT g FROM generate_series(1, 100) g;
	SELECT count(*) FROM heap_t;
	UPDATE heap_t SET id = id + 1;
});
# When cluster.enabled=false the dump_pcm surface returns 0 throughout
# (pcm_grd is initialized but the gate skips all hot-path acquires).
# Note:  pcm_grd may show pcm_api_state='active' because the GUC enables
# allocation; cluster_pcm_is_active() returns false because Layer 2 gate
# rejects.  Counters therefore stay at 0.
is(trans_count($node_off, 'trans_n_to_s_count'), '0',
   'L5 cluster.enabled=off:  trans_n_to_s_count stays 0 (Layer 2 gate skips hook)');
is(trans_count($node_off, 'trans_n_to_x_count'), '0',
   'L5 cluster.enabled=off:  trans_n_to_x_count stays 0 (Layer 2 gate skips hook)');

$node_off->stop;


# ============================================================
# L6 — cluster.pcm_grd_max_entries=0:  Layer 3 gate skips hook.
# ============================================================
my $node_disable_pcm = PgracClusterNode->new('bufmgr_pcm_no_grd');
$node_disable_pcm->init;
$node_disable_pcm->append_conf('postgresql.conf',
	"cluster.node_id = 0\n" . "cluster.pcm_grd_max_entries = 0\n");
$node_disable_pcm->start;

$node_disable_pcm->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int);
	INSERT INTO heap_t SELECT g FROM generate_series(1, 100) g;
	SELECT count(*) FROM heap_t;
	UPDATE heap_t SET id = id + 1;
});
is(trans_count($node_disable_pcm, 'trans_n_to_s_count'), '0',
   'L6 pcm_grd_max_entries=0:  trans_n_to_s_count stays 0 (Layer 3 gate skips hook)');
is(trans_count($node_disable_pcm, 'trans_n_to_x_count'), '0',
   'L6 pcm_grd_max_entries=0:  trans_n_to_x_count stays 0 (Layer 3 gate skips hook)');

$node_disable_pcm->stop;


done_testing();
