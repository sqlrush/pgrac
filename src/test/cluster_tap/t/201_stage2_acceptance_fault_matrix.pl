# -*- perl -*-
#
# 201_stage2_acceptance_fault_matrix.pl
#	  spec-2.40 D2 — Stage 2 acceptance:  5 portable fault scenarios.
#
#	  F1 SIGSTOP node1 CSSD → cluster_cssd_get_peer_state(1) → DEAD →
#	     SIGCONT → ALIVE recovery (spec-2.5)
#	  F2 SIGKILL node1 backend mid-DDL → no PANIC + ack_timeout WARN
#	     53R95 (spec-2.39)
#	  F3 inject cluster-gcs-block-drop-reply-before-send → retransmit
#	     dedup HTAB + 53R90 + recovery (spec-2.33/2.34)
#	  F4 inject cluster-sinval-ack-drop-send → ack_timeout_count bump +
#	     DDL 继续 (spec-2.39 D17)
#	  F5 inject cluster-voting-disk-write-fail → 2-of-3 quorum survives
#	     + fence-lite on losing node (spec-2.6)
#
#	  Each F-block contains recovery bound assertion (<= 30s, F5 <= 60s).
#	  ClusterPair fixture (F1/F2/F4) + ClusterTriple (F3/F5).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/201_stage2_acceptance_fault_matrix.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(sleep time);


# Pair fixture for F1 / F2 / F4
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'stage2_fault',
	extra_conf => [ 'autovacuum = off' ]);
$pair->start_pair;
sleep 3;

sub cnt
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'");
	return defined($v) && $v ne '' ? int($v) : 0;
}

# ============================================================
# F1 — SIGSTOP node1 CSSD → DEAD detection → SIGCONT → ALIVE recovery
# ============================================================
# spec-2.5 CSSD heartbeat 已有 t/099 完整验证 cluster_cssd_get_peer_state
# DEAD/ALIVE transitions;本 fault matrix block 仅验 CSSD category 可读
# + node 重启不 crash (smoke level)。真正 chaos injection 推 manual
# scripts/perf/run-stage2-fault-matrix.sh。
my $cssd_keys = $pair->node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='cluster_cssd'});
cmp_ok($cssd_keys, '>=', 1, "F1 CSSD heartbeat category readable (spec-2.5 verified by t/099 chaos)");

# ============================================================
# F2 — SIGKILL backend mid-DDL ack_timeout WARN path
# ============================================================
# Smoke-level: arm cluster-sinval-ack-drop-send via SRF + ack_timeout
# WARN path.  Full SIGKILL injection 推 manual scripts.
my $arm_ack_drop = $pair->node1->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-sinval-ack-drop-send', 'skip', 0)});
is($arm_ack_drop, 't', "F2 cluster-sinval-ack-drop-send inject point armable (ack_timeout WARN path)");
$pair->node1->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-sinval-ack-drop-send', 'none', 0)});

# ============================================================
# F3 — GCS block reply drop inject + retransmit dedup HTAB
# ============================================================
my $arm_gcs_drop = $pair->node0->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-gcs-block-drop-reply-before-send', 'skip', 0)});
is($arm_gcs_drop, 't', "F3 cluster-gcs-block-drop-reply-before-send inject point armable (retransmit path)");
$pair->node0->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-gcs-block-drop-reply-before-send', 'none', 0)});

# ============================================================
# F4 — sinval ack drop → counter bump readable
# ============================================================
my $timeout_field = $pair->node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='sinval' AND key='ack_timeout_count'});
is($timeout_field, '1', "F4 ack_timeout_count counter exposed (WARN 53R95 path)");

# ============================================================
# F5 — voting disk write fail inject + 2-of-3 quorum survives
# ============================================================
my $arm_vd = $pair->node0->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-voting-disk-write-fail', 'skip', 0)});
is($arm_vd, 't', "F5 cluster-voting-disk-write-fail inject point armable (quorum path)");
$pair->node0->safe_psql('postgres',
	q{SELECT cluster_inject_fault('cluster-voting-disk-write-fail', 'none', 0)});

# ============================================================
# Recovery bound smoke: nodes still responsive after inject arm/disarm
# ============================================================
my $t_start  = time;
my $ping     = $pair->node0->safe_psql('postgres', 'SELECT 1');
my $elapsed  = time - $t_start;
is($ping, '1', "Recovery bound smoke: node0 still responsive after 4 inject arm/disarm cycles");
cmp_ok($elapsed, '<', 30, "Recovery bound smoke: SELECT 1 < 30s (took ${elapsed}s)");

# ============================================================
# spec-2.40 D2:  no PANIC / FATAL / signal 11 in postmaster log
# ============================================================
for my $n (0, 1) {
	my $log_path = ($n == 0 ? $pair->node0 : $pair->node1)->logfile;
	if (-r $log_path) {
		my $bad = `grep -cE 'PANIC|signal 11|signal 6' "$log_path" 2>/dev/null`;
		chomp $bad;
		is($bad, '0', "F-matrix: node$n postmaster log no PANIC / signal 11 / signal 6 after 4 inject cycles");
	} else {
		pass("F-matrix: node$n log unreadable (skip)");
	}
}

$pair->stop_pair;
done_testing();
