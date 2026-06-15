#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 245_recovery_plan.pl
#    spec-4.3 -- Recovery Coordinator skeleton: the observational
#    recovery plan pass over a real shared WAL root (ClusterPair).
#
#      L1   first boot (node0 starts before node1 ever ran): plan is
#           generated with the OWN slot still EMPTY -- own thread is
#           classified OWN, not EMPTY (P1-1: 127 empty, not 128);
#           clean first boot reports dbstate=shutdowned and
#           local_recovery_needed=f
#      L2   peer running + refreshing -> ALIVE (fresh ACTIVE slot)
#      L3   peer kill -9 + stale window elapsed -> CRASHED_CANDIDATE;
#           the LOG summary names the candidate and says "not acted
#           upon" (observational)
#      L4   peer clean stop -> CLEAN
#      L5   CRC-corrupt peer slot -> UNKNOWN, never a candidate
#      L6   forged CRC-valid slot with node_id != tid-1 (identity
#           invariant violation) -> UNKNOWN, never ALIVE/CRASHED (P2)
#      L7   recovery category exposes 6 + 4 + 13 + 8 + 8 = 39 keys (3.16
#           counters + 4.10 block-recovery + 4.11 thread-recovery + 4.3 plan + 4.4 worker
#           + 4.5a merged surface)
#      L8   flat node (no wal_threads_dir) -> plan_state=none, keys '-'
#
#    The standby-mode gate (no plan in standby) is asserted in t/242 L9
#    next to the RL1 leg it shares the gate with.
#
#    Stale threshold: cluster.recovery_stale_active_ms = 5000 with the
#    peer refreshing every 500ms (10x margin for the ALIVE leg); the
#    CRASHED leg sleeps 6s after the kill.  node1 is CHECKPOINTed
#    before kill -9 so its later own-recovery replays an empty redo
#    window (the heap-redo-on-pair-node path is the documented
#    pre-existing 4.7 gap, see t/243).
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.3-recovery-coordinator-skeleton.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracWalState qw(read_file_raw write_file_raw patch_byte forge_slot_node_id slot_offset);
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

my $pair = PostgreSQL::Test::ClusterPair->new_pair('recplan',
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

# ============================================================
# L1: node0's first-boot plan (generated before node1 ever started).
# ============================================================
is(plankey($node0, 'plan_state'), 'generated', 'L1 plan generated on first boot');
is(plankey($node0, 'plan_own_thread'), '1', 'L1 own thread recorded');
is(plankey($node0, 'plan_threads_scanned'), '128', 'L1 all 128 slots scanned');
is(plankey($node0, 'plan_n_empty'), '127',
	'L1 own slot (EMPTY at plan time, ACTIVE only at RUNNING) counts as OWN, not EMPTY (P1-1)');
is(plankey($node0, 'plan_n_crashed_candidate'), '0', 'L1 no candidates on a pristine root');
is(plankey($node0, 'plan_crashed_candidates'), '-', 'L1 candidate csv empty');
is(plankey($node0, 'plan_dbstate_at_startup'), 'shutdowned',
	'L1 clean first boot: dbstate at the hook is shutdowned');
is(plankey($node0, 'plan_local_recovery_needed'), 'f',
	'L1 clean first boot: local recovery not needed (planning pass, not crash recovery)');

# ============================================================
# L2: peer running + refreshing -> ALIVE.
# ============================================================
# Let node1's stats loop stamp its slot at least once.
my $deadline = time() + 15;
while (time() < $deadline) {
	last if $node1->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		WHERE category='wal_thread' AND key='registry_slot_state'}) eq 'active';
	select(undef, undef, undef, 0.25);
}
$node0->stop;
$node0->start;
is(plankey($node0, 'plan_n_alive'), '1', 'L2 running peer classified ALIVE');
is(plankey($node0, 'plan_n_crashed_candidate'), '0', 'L2 fresh ACTIVE is not a candidate');
is(plankey($node0, 'plan_n_empty'), '126', 'L2 empties shrink as the peer occupies its slot');

# ============================================================
# L3: peer crash + stale window -> CRASHED_CANDIDATE.
# ============================================================
$node1->safe_psql('postgres', 'CHECKPOINT');    # quiesce redo window (4.7 gap, see header)
$node1->stop('immediate');                      # slot stays ACTIVE, refresh stops
$node0->stop;
sleep(6);                                       # > 5000ms staleness window
my $log_off = -s $node0->logfile;
$node0->start;
is(plankey($node0, 'plan_n_crashed_candidate'), '1', 'L3 stale ACTIVE peer is a crash candidate');
is(plankey($node0, 'plan_crashed_candidates'), '2', 'L3 candidate csv names thread 2');
is(plankey($node0, 'plan_n_alive'), '0', 'L3 nothing ALIVE while the peer is down');
my $log = PostgreSQL::Test::Utils::slurp_file($node0->logfile, $log_off);
like($log, qr/recovery plan \(not acted upon\): own thread 1, .*1 crashed candidate \[2\]/,
	'L3 LOG summary names the candidate and stays observational');

# ============================================================
# L4: peer clean stop -> CLEAN.
# ============================================================
$node1->start;    # recovers its own thread (PG native), republishes ACTIVE
$node1->stop;     # clean: publishes STOPPED
$node0->stop;
$node0->start;
is(plankey($node0, 'plan_n_clean'), '1', 'L4 cleanly stopped peer classified CLEAN');
is(plankey($node0, 'plan_n_crashed_candidate'), '0', 'L4 STOPPED is never a candidate');

# ============================================================
# L5: CRC-corrupt peer slot -> UNKNOWN, never a candidate.
# ============================================================
$node0->stop;
my $image = read_file_raw($regfile);
patch_byte($regfile, slot_offset(2) + 41);    # break slot 2's CRC
$node0->start;
is(plankey($node0, 'plan_n_unknown'), '1', 'L5 torn peer slot classified UNKNOWN');
is(plankey($node0, 'plan_unknown_threads'), '2', 'L5 unknown csv names thread 2');
is(plankey($node0, 'plan_n_crashed_candidate'), '0',
	'L5 UNKNOWN is never merged into the candidate set (absence-as-proof)');

# ============================================================
# L6: forged CRC-valid slot with an impossible owner -> UNKNOWN (P2).
# ============================================================
$node0->stop;
write_file_raw($regfile, $image);             # restore slot 2
forge_slot_node_id($regfile, 2, 7);           # valid CRC, node 7 in slot 2
$node0->start;
is(plankey($node0, 'plan_n_unknown'), '1',
	'L6 identity-invariant violation (node_id != tid-1) classified UNKNOWN');
is(plankey($node0, 'plan_n_alive') + plankey($node0, 'plan_n_crashed_candidate'), 0,
	'L6 impossible owner is neither ALIVE nor a candidate (P2)');

# ============================================================
# L7: recovery category = 39 keys (4 spec-3.16 + 2 spec-4.10 block-recovery
#     + 4 spec-4.11 D5 thread-recovery + 13 plan + 8 worker + 8 merged/remote,
#     spec-4.5a D11).
# ============================================================
is($node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'recovery'}),
	'39', 'L7 recovery category: 6 counters + 4 thread-recovery + 13 plan + 8 worker + 8 merged keys');

$node0->stop;
write_file_raw($regfile, $image);             # leave the registry intact

# ============================================================
# L8: flat node (no wal_threads_dir) -> plan_state=none.
# ============================================================
my $flat = PgracClusterNode->new('recplan_flat');
$flat->init;
$flat->append_conf('postgresql.conf',
	"cluster.enabled = on\ncluster.node_id = 5\ncluster.allow_single_node = on\n");
$flat->start;
is(plankey($flat, 'plan_state'), 'none', 'L8 flat layout generates no plan');
is(plankey($flat, 'plan_own_thread'), '-', 'L8 plan keys are placeholders without a plan');
$flat->stop;

done_testing();
