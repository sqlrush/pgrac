#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 214_cluster_3_8_undo_lifecycle.pl
#	  spec-3.8 D13 — Undo Segment Lifecycle MVP + Autoextend behavioral
#	  TAP on ClusterPair fixture.
#
#	  L1   ClusterPair startup + both nodes alive
#	  L2   undo category in pg_cluster_state has 9 rows (5 record-level
#	       + 4 NEW lifecycle counters)
#	  L3   53R9E SQLSTATE registered (lookup via pg_settings or smoke trigger)
#	  L4   2 NEW GUCs registered with defaults:
#	       cluster.undo_segments_max_per_instance = 256
#	       cluster.undo_segment_create_timeout_ms = 5000
#	  L5   Initial autoextend / segment_switch / create_fail / hard_cap
#	       counters = 0 (无活动时)
#	  L6   GUC SIGHUP reload max_per_instance:  ALTER SYSTEM SET ... = 128;
#	       SELECT pg_reload_conf(); 验证 reload OK
#	  L7   GUC SIGHUP reload create_timeout_ms:  set to 2000ms; reload OK
#	  L8   Counter monotonic + clean shutdown
#
#	  L9   autoextend trigger via test hook: force segment_end + DML →
#	       autoextend_count + segment_switch_count both increment
#	  L10  repeated trigger monotonicity
#	  L11  hard cap real trigger:  set max_per_instance ≤ current pool
#	       size (SIGHUP race-safe floor takes effect) → repeated
#	       force_segment_end + DML eventually raises SQLSTATE 53R9E +
#	       segment_hard_cap_fail_count increments
#	  L12  concurrency double-checked locking — see L12 block below
#
#	  Spec: spec-3.8-undo-segment-lifecycle-autoextend.md (FROZEN v0.3 +
#	        Hardening v1.0.1 H-1/H-2)
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


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_3_8_undo_lifecycle',
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'cluster.undo_segments_max_per_instance = 256',
		'cluster.undo_segment_create_timeout_ms = 5000',
		'max_prepared_transactions = 4',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0 = $pair->node0;


# ----------
# L1: ClusterPair startup + both nodes alive
# ----------
{
	my $r = $node0->safe_psql('postgres', 'SELECT 1');
	is($r, '1', "L1 node0 alive");
}


# ----------
# L2: undo category has 32 rows
#   5 record-level (spec-3.7) + 4 lifecycle (spec-3.8) + 3 commit-fsync +
#   4 smgr (the latter 7 added by the perf-merge undo instrumentation) +
#   5 durable TT slot counters (spec-3.11 D8: commit / lookup hit / lookup
#   miss / by-xid scan / redo apply) +
#   5 retention counters (spec-3.12 D5: horizon gauge / tt_slot_retain_skip /
#   segment_retain_skip / retention_recycle / tt_retention_rollover).
# ----------
my $undo_row_count = $node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='undo'});
is($undo_row_count, '44',
	"L2 undo category has 44 rows (5 record + 4 lifecycle + 3 fsync + 4 smgr + 5 durable-tt + 5 retention + 6 cleaner + 4 buf/extent obs [spec-3.18 D7] + 1 spec-3.22 retention_off_recycle + 4 checkpoint-writeback boundary [spec-4.8ab D7] + 2 record-segment reclaim [spec-4.12a D5] + 1 residual-revalidate-drop [spec-4.12a Hardening v1.0.1])"
);


# ----------
# L3: 4 NEW lifecycle counter keys present
# ----------
my $lifecycle_keys = $node0->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state
	   WHERE category='undo' AND key IN ('autoextend_count', 'segment_switch_count',
	                                      'segment_create_fail_count',
	                                      'segment_hard_cap_fail_count')});
is($lifecycle_keys,
	'autoextend_count,segment_create_fail_count,segment_hard_cap_fail_count,segment_switch_count',
	"L3 all 4 NEW lifecycle counter keys present");


# ----------
# L4: 2 NEW GUCs registered with default values
# ----------
my $max_per_instance = $node0->safe_psql('postgres',
	q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_segments_max_per_instance'});
is($max_per_instance, '256', "L4a max_per_instance default = 256");

my $create_timeout = $node0->safe_psql('postgres',
	q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_segment_create_timeout_ms'});
is($create_timeout, '5000', "L4b create_timeout default = 5000ms");


# ----------
# L5: Initial counters = 0 (无 autoextend 活动时)
# ----------
my $initial_autoextend = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='undo' AND key='autoextend_count'});
ok($initial_autoextend == 0,
	"L5a initial autoextend_count = $initial_autoextend (expected 0)");

my $initial_hard_cap = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='undo' AND key='segment_hard_cap_fail_count'});
ok($initial_hard_cap == 0,
	"L5b initial segment_hard_cap_fail_count = $initial_hard_cap (expected 0)");


# ----------
# L6: GUC SIGHUP reload max_per_instance
# ----------
$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.undo_segments_max_per_instance = 128');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);
my $new_max = $node0->safe_psql('postgres',
	q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_segments_max_per_instance'});
is($new_max, '128', "L6 SIGHUP reload max_per_instance OK (256 → 128)");


# ----------
# L7: GUC SIGHUP reload create_timeout_ms
# ----------
$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.undo_segment_create_timeout_ms = 2000');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);
my $new_timeout = $node0->safe_psql('postgres',
	q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_segment_create_timeout_ms'});
is($new_timeout, '2000', "L7 SIGHUP reload create_timeout OK (5000 → 2000)");


# ----------
# L8: Counter monotonic + clean shutdown
# ----------
my $final_autoextend = $node0->safe_psql('postgres',
	q{SELECT value::bigint FROM pg_cluster_state
	   WHERE category='undo' AND key='autoextend_count'});
ok($final_autoextend >= $initial_autoextend,
	"L8 autoextend_count monotonic (initial=$initial_autoextend, final=$final_autoextend)");


# ----------
# L9: autoextend trigger via test hook
#    Sequence:
#     a) Run DML to ensure active segment is claimed
#     b) Snapshot autoextend / segment_switch counters
#     c) Call cluster_undo_test_force_segment_end() → forces cursor to last block
#     d) Run another DML → triggers autoextend path
#     e) Verify autoextend_count + segment_switch_count both incremented
# ----------
$node0->safe_psql('postgres', q{
    CREATE TABLE IF NOT EXISTS t_autoex (id int, v text);
    INSERT INTO t_autoex VALUES (1, 'claim active segment');
});

my $pre_autoex = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='autoextend_count'});
my $pre_switch = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='segment_switch_count'});

my $force_ok = $node0->safe_psql('postgres',
    q{SELECT cluster_undo_test_force_segment_end()});
is($force_ok, 't', "L9a force_segment_end returns true after active segment claimed");

$node0->safe_psql('postgres',
    q{INSERT INTO t_autoex VALUES (2, 'should trigger autoextend')});

my $post_autoex = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='autoextend_count'});
my $post_switch = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='segment_switch_count'});

ok($post_autoex > $pre_autoex,
    "L9b autoextend_count incremented (pre=$pre_autoex post=$post_autoex)");
ok($post_switch > $pre_switch,
    "L9c segment_switch_count incremented (pre=$pre_switch post=$post_switch)");


# ----------
# L10: counter monotonicity across repeated trigger
#    Two more force+DML cycles → autoextend should keep climbing.
# ----------
my $mid_autoex = $post_autoex;
for my $i (3..4) {
    $node0->safe_psql('postgres',
        q{SELECT cluster_undo_test_force_segment_end()});
    $node0->safe_psql('postgres',
        qq{INSERT INTO t_autoex VALUES ($i, 'extend cycle')});
}
my $final_autoex2 = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='autoextend_count'});
ok($final_autoex2 > $mid_autoex,
    "L10 repeated autoextend monotonic (mid=$mid_autoex final=$final_autoex2)");


# ----------
# L11: hard cap real trigger
#    Strategy:
#     a) Snapshot hard_cap_fail_count
#     b) Set max_per_instance = 2 (SIGHUP race-safe floor will use current
#        pool size if it's already > 2, but that's fine — eventually
#        force+DML will push past the floor)
#     c) Loop up to 256 cycles of force_segment_end + INSERT.  Catch
#        the first INSERT that raises SQLSTATE 53R9E (cluster_undo_segments
#        _hard_cap_reached).
#     d) Verify:  53R9E raised + hard_cap_fail_count > snapshot.
# ----------
# GUC range is [16, 256] (cluster_guc.c).  Set to floor 16; pool will hit
# cap at 16 segments.  Race-safe floor only kicks in if current pool > 16,
# which isn't possible at this point in the test (L9/L10 created maybe 3-4
# segments at most).
$node0->safe_psql('postgres',
    'ALTER SYSTEM SET cluster.undo_segments_max_per_instance = 16');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);

my $pre_hard_cap = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='segment_hard_cap_fail_count'});

my $caught_53r9e = 0;
my $cycles_done  = 0;
for my $i (1..32) {
    $cycles_done = $i;
    $node0->safe_psql('postgres',
        q{SELECT cluster_undo_test_force_segment_end()});
    # Use --set VERBOSITY=verbose so SQLSTATE prefix appears in stderr.
    my ($rc, $stdout, $stderr) = $node0->psql('postgres',
        qq{INSERT INTO t_autoex VALUES ($i + 1000, 'hard cap probe')},
        extra_params => ['-v', 'VERBOSITY=verbose']);
    if ($rc != 0 && ($stderr =~ /53R9E/ || $stderr =~ /hard cap reached/)) {
        $caught_53r9e = 1;
        last;
    }
    last if $rc != 0;  # 任何其他错误 → 终止避免无限重试
}

ok($caught_53r9e == 1,
    "L11a hard cap raised within $cycles_done force+DML cycles (53R9E / hard cap reached)");

my $post_hard_cap = $node0->safe_psql('postgres',
    q{SELECT value::bigint FROM pg_cluster_state
       WHERE category='undo' AND key='segment_hard_cap_fail_count'});
ok($post_hard_cap > $pre_hard_cap,
    "L11b segment_hard_cap_fail_count incremented (pre=$pre_hard_cap post=$post_hard_cap)");

# Restore GUC for subsequent tests
$node0->safe_psql('postgres',
    'ALTER SYSTEM SET cluster.undo_segments_max_per_instance = 256');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);


# ----------
# L12: concurrency — publication path under double-checked locking
#    Setup:  two background_psql sessions s1 + s2 (separate backends).
#    Sequence:
#     a) Snapshot autoextend_count A_pre + segment_switch_count S_pre
#     b) s1 + s2 both run force_segment_end (cursor at last block in
#        both backends' shared view)
#     c) s1 INSERT — wins autoextend race:  acquires lifecycle_lock,
#        extends, mark_full(old) + mark_active(new), publishes new
#        active_segment_id under cursor_lock, A_pre+1
#     d) s2 INSERT — does NOT re-force_segment_end.  When s2 enters
#        cluster_undo_record_alloc, it reads the NEW cursor (published
#        by s1) which points at the fresh segment block 1.  No
#        cursor-exhaustion branch entered → NO autoextend.
#     e) Snapshot autoextend_count A_post + segment_switch_count S_post
#     f) Assert:  delta A = 1 (not 2),  delta S = 1 (not 2).
#
#    This proves the publication half of double-checked locking:  the
#    cursor_lock-protected active_segment_id store is visible to the
#    second writer without it re-entering autoextend.  The race-loser
#    recheck-under-lifecycle-lock path (both writers reach exhaustion
#    simultaneously) requires true parallel execution and is exercised
#    by perf class 8 stress workload (Step 11 baseline).
# ----------
{
    my $a_pre = $node0->safe_psql('postgres',
        q{SELECT value::bigint FROM pg_cluster_state
           WHERE category='undo' AND key='autoextend_count'});
    my $s_pre = $node0->safe_psql('postgres',
        q{SELECT value::bigint FROM pg_cluster_state
           WHERE category='undo' AND key='segment_switch_count'});

    my $s1 = $node0->background_psql('postgres', on_error_die => 1);
    my $s2 = $node0->background_psql('postgres', on_error_die => 1);

    $s1->query_safe('SELECT cluster_undo_test_force_segment_end()');
    $s2->query_safe('SELECT cluster_undo_test_force_segment_end()');

    # s1 wins race
    $s1->query_safe(q{INSERT INTO t_autoex VALUES (9001, 'race winner')});

    # s2 follows — cursor has been published by s1, no autoextend expected
    $s2->query_safe(q{INSERT INTO t_autoex VALUES (9002, 'race observer')});

    $s1->quit;
    $s2->quit;

    my $a_post = $node0->safe_psql('postgres',
        q{SELECT value::bigint FROM pg_cluster_state
           WHERE category='undo' AND key='autoextend_count'});
    my $s_post = $node0->safe_psql('postgres',
        q{SELECT value::bigint FROM pg_cluster_state
           WHERE category='undo' AND key='segment_switch_count'});

    my $da = $a_post - $a_pre;
    my $ds = $s_post - $s_pre;

    ok($da == 1,
        "L12a publication path: autoextend delta = 1 not 2 (pre=$a_pre post=$a_post)");
    ok($ds == 1,
        "L12b publication path: segment_switch delta = 1 not 2 (pre=$s_pre post=$s_post)");
}


$pair->stop_pair;
done_testing();
