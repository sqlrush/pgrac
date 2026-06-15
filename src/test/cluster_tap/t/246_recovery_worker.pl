#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 246_recovery_worker.pl
#    spec-4.4 -- Recovery Worker skeleton: dynamic bgworkers validate
#    candidate streams over a real shared WAL root (ClusterPair).
#
#      L1   peer kill -9 + stale window -> node0 restart spawns ONE
#           worker (requested=1, done=1) and a HEALTHY crash stream
#           (latest segment not full) validates OK -- the P0
#           regression anchor: the v0.1 tail-page scheme fails here
#      L2   LOG lines: launch ("not waiting") + worker summary
#           ("not replaying")
#      L3   corrupt the LAST WRITTEN page header (located from the
#           registry highest_lsn) -> SUSPECT, startup unaffected
#      L4   two candidates / cap=1 -> one worker stripes both threads
#           (forged slot 3 has no thread directory: claim check fails
#           -> SUSPECT; real thread 2 stays OK)
#      L5   cluster.recovery_workers_max=0 -> candidates present but
#           zero spawn (pool idle)
#      L6   no candidates (peer cleanly stopped) -> zero spawn
#      L7   recovery category exposes 39 keys; worker keys read '-'
#           on a flat node
#
#    Worker completion is polled via workers_done (deadline 15s); the
#    workers run concurrently with recovery by design (never waited
#    on by the coordinator).  node1 is CHECKPOINTed before kill -9 so
#    its own later recovery replays an empty redo window (4.7 gap,
#    t/243 pattern).
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.4-recovery-worker-skeleton.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracWalState qw(read_file_raw write_file_raw patch_byte forge_slot_clone read_slot_raw);
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

sub plankey
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value FROM pg_cluster_state
		WHERE category='recovery' AND key='$key'});
}

sub wait_workers_done
{
	my ($node, $want) = @_;
	my $deadline = time() + 15;
	my $done = '';
	while (time() < $deadline) {
		$done = plankey($node, 'workers_done');
		return $done if $done eq $want;
		select(undef, undef, undef, 0.25);
	}
	return $done;
}

# Locate the last written page of thread $tid from its registry slot
# (mirrors cluster_recovery_worker_target_page; 16MB segments, 8KB
# pages) and return (segment_path, page_offset).
sub target_page
{
	my ($root, $regfile, $tid) = @_;
	my $slot = read_slot_raw($regfile, $tid);
	my $target = $slot->{highest_lsn} - 1;
	my $segsz = 16 * 1024 * 1024;
	my $segno = int($target / $segsz);
	my $per_id = int(0x100000000 / $segsz);
	my $fname = sprintf("%08X%08X%08X",
		$slot->{tli}, int($segno / $per_id), $segno % $per_id);
	my $page_off = ($target % $segsz) - (($target % $segsz) % 8192);
	return ("$root/thread_$tid/$fname", $page_off);
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair('recworker',
	wal_threads_root => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.recovery_stale_active_ms = 5000',
		"cluster.cluster_stats_main_loop_interval = '500ms'",
	]);
my $root = $pair->wal_threads_root;
my $regfile = "$root/pgrac_wal_state";
my $node0 = $pair->node0;
my $node1 = $pair->node1;

$pair->start_pair;

# Generate some WAL on node1 so its latest segment is partially
# written (the realistic healthy-crash shape), then let the stats
# loop publish a fresh highest_lsn watermark.  pg_logical_emit_message
# writes pure WAL without touching GCS-tracked heap blocks (pair-mode
# heap DML trips the documented pre-existing 4.7 GCS gap, see t/243).
$node1->safe_psql('postgres',
	q{SELECT pg_logical_emit_message(true, 't246', repeat('x', 200000))});
my $deadline = time() + 15;
while (time() < $deadline) {
	last if read_slot_raw($regfile, 2)->{highest_lsn} > 0;
	select(undef, undef, undef, 0.25);
}
cmp_ok(read_slot_raw($regfile, 2)->{highest_lsn}, '>', 0,
	'setup: peer published a highest_lsn watermark');

# ============================================================
# L1: crash candidate -> one worker, healthy stream validates OK.
# ============================================================
$node1->safe_psql('postgres', 'CHECKPOINT');
$node1->stop('immediate');
$node0->stop;
sleep(6);
my $log_off = -s $node0->logfile;
$node0->start;
is(plankey($node0, 'plan_n_crashed_candidate'), '1', 'L1 precondition: thread 2 is a candidate');
is(plankey($node0, 'workers_requested'), '1', 'L1 one worker requested');
is(wait_workers_done($node0, '1'), '1', 'L1 worker completed');
is(plankey($node0, 'workers_failed'), '0', 'L1 no failures');
is(plankey($node0, 'stream_ok_threads'), '2',
	'L1 healthy crash stream (latest segment not full) validates OK -- P0 anchor');
is(plankey($node0, 'stream_suspect_or_unreadable_threads'), '-', 'L1 nothing suspect');
is(plankey($node0, 'worker_pool_state'), 'done', 'L1 pool reaches done');

# ============================================================
# L2: LOG lines (launch + worker summary, both observational).
# ============================================================
my $log = PostgreSQL::Test::Utils::slurp_file($node0->logfile, $log_off);
like($log, qr/recovery stream validation: launched 1 worker\(s\) for 1 crash candidate\(s\) \(not waiting; recovery proceeds\)/,
	'L2 launch LOG says not waiting');
like($log, qr/recovery stream validation \(not replaying\): worker 0, 1 ok, 0 suspect\/unreadable, 0 skipped/,
	'L2 worker summary LOG says not replaying');

# ============================================================
# L3: corrupt the LAST WRITTEN page header -> SUSPECT, fail-open.
# ============================================================
$node0->stop;
my ($segpath, $page_off) = target_page($root, $regfile, 2);
ok(-f $segpath, 'L3 located the last-written segment from highest_lsn');
patch_byte($segpath, $page_off);    # break xlp_magic low byte
$node0->start;
is(wait_workers_done($node0, '1'), '1', 'L3 worker completed');
is(plankey($node0, 'stream_suspect_or_unreadable_threads'), '2',
	'L3 corrupted last-written page header -> SUSPECT');
is(plankey($node0, 'stream_ok_threads'), '-', 'L3 nothing OK');
ok($node0->safe_psql('postgres', 'SELECT 1') eq '1',
	'L3 startup unaffected (fail-open observational)');
$node0->stop;
patch_byte($segpath, $page_off);    # XOR back: restore the byte

# ============================================================
# L4: two candidates / cap=1 -> one worker stripes both.
# ============================================================
forge_slot_clone($regfile, 2, 3);    # CRC-valid stale-ACTIVE slot 3, no thread_3 dir
$node0->append_conf('postgresql.conf', "cluster.recovery_workers_max = 1\n");
$node0->start;
is(plankey($node0, 'plan_n_crashed_candidate'), '2', 'L4 two candidates (real 2 + forged 3)');
is(plankey($node0, 'workers_requested'), '1', 'L4 cap=1 -> one worker');
is(wait_workers_done($node0, '1'), '1', 'L4 worker completed');
is(plankey($node0, 'stream_ok_threads'), '2', 'L4 real stream OK');
is(plankey($node0, 'stream_suspect_or_unreadable_threads'), '3',
	'L4 forged candidate without a thread directory -> claim check fails (striped same worker)');

# ============================================================
# L5: cap=0 -> candidates present, zero spawn.
# ============================================================
$node0->stop;
$node0->append_conf('postgresql.conf', "cluster.recovery_workers_max = 0\n");
$node0->start;
is(plankey($node0, 'plan_n_crashed_candidate'), '2', 'L5 candidates still present');
is(plankey($node0, 'worker_pool_state'), 'idle', 'L5 cap=0: pool idle, zero spawn');
is(plankey($node0, 'workers_requested'), '0', 'L5 nothing requested');

# ============================================================
# L6: no candidates -> zero spawn.
# ============================================================
$node0->stop;
my $image = read_file_raw($regfile);
{
	# zero out the forged slot 3 (back to EMPTY)
	my $patched = $image;
	substr($patched, 512 + 2 * 512, 512) = "\0" x 512;
	write_file_raw($regfile, $patched);
}
$node0->append_conf('postgresql.conf', "cluster.recovery_workers_max = 4\n");
$node1->start;    # recovers own thread, republishes ACTIVE
$node1->stop;     # clean STOPPED
$node0->start;
is(plankey($node0, 'plan_n_crashed_candidate'), '0', 'L6 no candidates after clean stop');
is(plankey($node0, 'worker_pool_state'), 'idle', 'L6 zero spawn without candidates');

# ============================================================
# L7: key count + flat-node placeholders.
# ============================================================
is($node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'recovery'}),
	'39', 'L7 recovery category: 6 + 4 thread-recovery + 13 + 8 + 8 keys');
$node0->stop;

my $flat = PgracClusterNode->new('recworker_flat');
$flat->init;
$flat->append_conf('postgresql.conf',
	"cluster.enabled = on\ncluster.node_id = 5\ncluster.allow_single_node = on\n");
$flat->start;
is(plankey($flat, 'worker_pool_state'), 'idle', 'L7 flat node pool idle');
is(plankey($flat, 'stream_ok_threads'), '-', 'L7 flat node csv placeholder');
$flat->stop;

done_testing();
