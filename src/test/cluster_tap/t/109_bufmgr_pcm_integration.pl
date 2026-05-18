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
#	  L2  SELECT * FROM heap_t:  trans_n_to_s_count ≥ 1 (LockBuffer SHARE hook)
#	  L3  UPDATE heap_t:  trans_n_to_x_count ≥ 1 (LockBuffer EXCLUSIVE hook)
#	  L4  VACUUM heap_t:  trans_n_to_x_count further inc (LockBufferForCleanup
#	       reuses internal LockBuffer hook per D4 audit)
#	  L5  restart with cluster.enabled=off:  counter doesn't inc (Layer 2 gate)
#	  L6  restart with cluster.pcm_grd_max_entries=0 + shared_buffers=128kB:
#	       counter doesn't inc (Layer 3 gate);  shared_buffers配套 防 HC62 FATAL
#	  L7  temp table workload:  diagnostic counter delta recorded
#	       (NOT hard-zero asserted — temp DDL touches catalog shared buffer;
#	        hard local-buffer skip covered by cluster_unit L6)
#	  L8  workload-active state:  pcm_api_state = 'active' + pcm_grd_active_entries > 0
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
# Default cluster — cluster.enabled=true + pcm_grd_max_entries=-1 (auto NBuffers).
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


# L2 — SELECT triggers LockBuffer(SHARE) hot path.
$node->safe_psql('postgres', q{
	CREATE TABLE heap_t (id int, val text);
	INSERT INTO heap_t SELECT g, 'r' || g FROM generate_series(1, 100) g;
});
# Capture state after INSERT (which uses LockBuffer EXCLUSIVE), then SELECT.
my $n_to_s_before_select = trans_count($node, 'trans_n_to_s_count');
$node->safe_psql('postgres', 'SELECT count(*) FROM heap_t');
my $n_to_s_after_select = trans_count($node, 'trans_n_to_s_count');
ok($n_to_s_after_select > $n_to_s_before_select,
   "L2 SELECT * FROM heap_t increments trans_n_to_s_count " .
   "($n_to_s_before_select → $n_to_s_after_select)");


# L3 — UPDATE triggers LockBuffer(EXCLUSIVE) hot path.
my $n_to_x_before_update = trans_count($node, 'trans_n_to_x_count');
$node->safe_psql('postgres', 'UPDATE heap_t SET val = val || \'!\' WHERE id <= 50');
my $n_to_x_after_update = trans_count($node, 'trans_n_to_x_count');
ok($n_to_x_after_update > $n_to_x_before_update,
   "L3 UPDATE heap_t increments trans_n_to_x_count " .
   "($n_to_x_before_update → $n_to_x_after_update)");


# L4 — VACUUM triggers LockBufferForCleanup which reuses internal LockBuffer.
my $n_to_x_before_vacuum = trans_count($node, 'trans_n_to_x_count');
$node->safe_psql('postgres', 'VACUUM heap_t');
my $n_to_x_after_vacuum = trans_count($node, 'trans_n_to_x_count');
ok($n_to_x_after_vacuum > $n_to_x_before_vacuum,
   "L4 VACUUM heap_t further increments trans_n_to_x_count " .
   "($n_to_x_before_vacuum → $n_to_x_after_vacuum) " .
   "via LockBufferForCleanup → internal LockBuffer(EXCLUSIVE) hook (D4 audit)");


# L8 — active surface after workload:  pcm_api_state=active + entries > 0.
is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_api_state'}),
   'active',
   'L8 pcm_api_state = active during workload');
ok($node->safe_psql('postgres',
		q{SELECT value::bigint > 0 FROM pg_cluster_state
		   WHERE category='pcm' AND key='pcm_grd_active_entries'}) eq 't',
   'L8 pcm_grd_active_entries > 0 after workload (HTAB populated)');


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
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\n" . "cluster.enabled = off\n");
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
