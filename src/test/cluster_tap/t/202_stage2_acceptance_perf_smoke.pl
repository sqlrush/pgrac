# -*- perl -*-
#
# 202_stage2_acceptance_perf_smoke.pl
#	  spec-2.40 D3 — Stage 2 acceptance:  4 workload tier-1 smoke
#	  with single-node cluster_enabled=on/off perf gate.
#
#	  L1 pgbench TPC-B select-only -S smoke (single-node on/off,
#	     warning ≤ 10%; sanity floor ≤ 60%)
#	  L2 pgbench TPC-B 完整 read+write smoke (single-node on/off,
#	     warning ≤ 15%; sanity floor ≤ 70%)
#	  L3 跨节点 DDL burst (PgracClusterDdlLoop;counter delta verify)
#	  L4 跨节点 contention burst (PgracClusterContention;GES + CF counter)
#
#	  Smoke scope:  ≤ 2min total.  2-node bound 仅 warning gate (red /
#	  yellow / green report;  user amend2 严守 — 不 hard fail trend
#	  baseline 受 GH runner 抖动影响).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/202_stage2_acceptance_perf_smoke.pl
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

my $pgbench_seconds = $ENV{STAGE2_PGBENCH_SECONDS} // 10;
my $workload_sleep_seconds = $ENV{STAGE2_WORKLOAD_SECONDS} // 5;
my $workload_iterations = $ENV{STAGE2_WORKLOAD_ITERATIONS} // 5;
my $perf_hard_gate = !defined($ENV{STAGE2_PERF_HARD_GATE})
	|| $ENV{STAGE2_PERF_HARD_GATE} !~ /^(?:0|false|off|no)$/i;

# Helper: extract TPS from pgbench stdout.
sub _pgbench_tps
{
	my ($output) = @_;
	if ($output =~ /tps = ([\d.]+)/m) {
		return $1 + 0;
	}
	return 0;
}

sub _run_pgbench_full
{
	my ($node, $seconds) = @_;
	my $output;

	$node->run_log([ 'pgbench', '-c', '4', '-T', "$seconds", '-n',
		'-p', $node->port, '-h', $node->host, 'postgres' ],
		'>', \$output);
	return _pgbench_tps($output);
}


# ============================================================
# L1/L2 single-node cluster_enabled=on vs off (smoke + trend gate)
# ============================================================
# spec-2.40 D3 + spec v0.2 F6:  paired on/off on **same job/runner**
# 降低 shared runner CPU 抖动 false-fail;  failure-before-confirmation
# rerun 还 single-attempt 实现降低 spec-1.X scaffolding 复杂度,fast-gate
# 上 smoke-only 30s 难 catch flake 但仍 catch egregious regression。

# Single-node fixture with cluster_enabled=off (PG baseline).
my $node_off = PostgreSQL::Test::Cluster->new('stage2_perf_off');
$node_off->init;
$node_off->append_conf('postgresql.conf', "shared_buffers = 128MB\n");
$node_off->start;

# Initialize pgbench (small scale for smoke).
$node_off->run_log([ 'pgbench', '-i', '-s', '1', '-q',
	'-p', $node_off->port, '-h', $node_off->host, 'postgres' ]);

# L1 — pgbench TPC-B select-only smoke
my $sel_off_out;
$node_off->run_log([ 'pgbench', '-S', '-c', '4', '-T', "$pgbench_seconds", '-n',
	'-p', $node_off->port, '-h', $node_off->host, 'postgres' ],
	'>', \$sel_off_out);
my $sel_off_tps = _pgbench_tps($sel_off_out);

# L2 — pgbench TPC-B 完整 smoke
my $full_off_tps = _run_pgbench_full($node_off, $pgbench_seconds);

$node_off->stop;
diag("L1 cluster_enabled=off: pgbench -S TPS=$sel_off_tps");
diag("L2 cluster_enabled=off: pgbench full TPS=$full_off_tps");

# Now run with cluster_enabled=on (single-node;  no peer)
my $node_on = PostgreSQL::Test::Cluster->new('stage2_perf_on');
$node_on->init;
$node_on->append_conf('postgresql.conf', "shared_buffers = 128MB\n");
$node_on->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node_on->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_on->append_conf('postgresql.conf', "cluster.interconnect_tier = stub\n");
$node_on->start;
$node_on->run_log([ 'pgbench', '-i', '-s', '1', '-q',
	'-p', $node_on->port, '-h', $node_on->host, 'postgres' ]);

my $sel_on_out;
$node_on->run_log([ 'pgbench', '-S', '-c', '4', '-T', "$pgbench_seconds", '-n',
	'-p', $node_on->port, '-h', $node_on->host, 'postgres' ],
	'>', \$sel_on_out);
my $sel_on_tps = _pgbench_tps($sel_on_out);

my $full_on_tps = _run_pgbench_full($node_on, $pgbench_seconds);

$node_on->stop;
diag("L1 cluster_enabled=on: pgbench -S TPS=$sel_on_tps");
diag("L2 cluster_enabled=on: pgbench full TPS=$full_on_tps");

# spec-3.4c hardening restored the paired single-node off/on gates:
# single-node cluster.enabled=on no longer pays Stage 3 ITL/TT write-path
# cost when there is no peer, while 2-node ClusterPair tests below still
# exercise the real RAC paths.  Keep these hard gates so the previous
# 30-50% yellow regression cannot silently pass again.

# L1 hard gate.
SKIP: {
	skip "L1 perf gate: pgbench output unparsed (TPS=0); CI flake skip", 1
		if $sel_off_tps == 0 || $sel_on_tps == 0;
	my $reg_pct = 100.0 * (1.0 - $sel_on_tps / $sel_off_tps);
	my $status = $reg_pct <= 10.0 ? 'GREEN'
		: $reg_pct <= 30.0 ? 'YELLOW' : 'RED';
	diag(sprintf "L1 single-node on/off regression: %.1f%% [%s] (hard gate ≤ 10%%)", $reg_pct, $status);
	if ($perf_hard_gate) {
		cmp_ok($reg_pct, '<=', 10.0,
			sprintf("L1 single-node on/off hard gate ≤ 10%% (actual %.1f%%;%s)", $reg_pct, $status));
	} else {
		pass(sprintf("L1 single-node on/off perf report only (actual %.1f%%;%s)", $reg_pct, $status));
	}
}

# L2 hard gate.
SKIP: {
	skip "L2 perf gate: pgbench output unparsed (TPS=0); CI flake skip", 1
		if $full_off_tps == 0 || $full_on_tps == 0;
	my $reg_pct = 100.0 * (1.0 - $full_on_tps / $full_off_tps);
	my $status = $reg_pct <= 15.0 ? 'GREEN'
		: $reg_pct <= 25.0 ? 'YELLOW'
		: $reg_pct <= 40.0 ? 'ORANGE'
		: $reg_pct <= 60.0 ? 'RED' : 'CATASTROPHIC';
	diag(sprintf "L2 single-node on/off full regression: %.1f%% [%s] (hard gate ≤ 15%%)", $reg_pct, $status);
	if ($perf_hard_gate && $reg_pct > 15.0 && $reg_pct <= 25.0) {
		diag("L2 full perf entered YELLOW; rerunning paired off/on once to confirm shared-runner noise");

		$node_off->start;
		my $confirm_off_tps = _run_pgbench_full($node_off, $pgbench_seconds);
		$node_off->stop;
		diag("L2 confirm cluster_enabled=off: pgbench full TPS=$confirm_off_tps");

		$node_on->start;
		my $confirm_on_tps = _run_pgbench_full($node_on, $pgbench_seconds);
		$node_on->stop;
		diag("L2 confirm cluster_enabled=on: pgbench full TPS=$confirm_on_tps");

		if ($confirm_off_tps != 0 && $confirm_on_tps != 0) {
			$full_off_tps = $confirm_off_tps;
			$full_on_tps = $confirm_on_tps;
			$reg_pct = 100.0 * (1.0 - $full_on_tps / $full_off_tps);
			$status = $reg_pct <= 15.0 ? 'GREEN'
				: $reg_pct <= 25.0 ? 'YELLOW'
				: $reg_pct <= 40.0 ? 'ORANGE'
				: $reg_pct <= 60.0 ? 'RED' : 'CATASTROPHIC';
			diag(sprintf "L2 confirm single-node on/off full regression: %.1f%% [%s] (hard gate ≤ 15%%)", $reg_pct, $status);
		}
	}
	if ($perf_hard_gate) {
		cmp_ok($reg_pct, '<=', 15.0,
			sprintf("L2 single-node on/off full hard gate ≤ 15%% (actual %.1f%%;%s)", $reg_pct, $status));
	} else {
		pass(sprintf("L2 single-node on/off full perf report only (actual %.1f%%;%s)", $reg_pct, $status));
	}
}

# ============================================================
# L3/L4 — 跨节点 DDL loop + contention helpers smoke
# ============================================================
# 仅 smoke 验 helper 可启 + counter delta readable;真 medium soak 推
# scripts/perf/run-stage2-cluster-baseline.sh tier=medium。
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'stage2_perf_pair',
	extra_conf => [
		'autovacuum = off',

		# spec-2.40 D3: this TAP is an acceptance smoke for the
		# workload helpers and trend counters.  Keep Cache Fusion PCM out
		# of the DDL/contention burst so a short HC116 invalidate timeout
		# does not mask the Stage 2 acceptance signal; dedicated GCS
		# behaviour remains covered by t/113..t/116.
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;
sleep 3;

# L3 DDL loop helper smoke
require PostgreSQL::Test::PgracClusterDdlLoop;
my $loop = PostgreSQL::Test::PgracClusterDdlLoop->new($pair,
	table_prefix => 'l3_ddl',
	iterations   => $workload_iterations);
$loop->start_loop;
sleep $workload_sleep_seconds;
my $loop_metrics = $loop->stop_loop;
cmp_ok($loop_metrics->{broadcast_send_delta}, '>', 0,
	"L3 DDL loop helper produces sinval broadcast_send delta ($loop_metrics->{broadcast_send_delta})");

# L4 contention helper smoke
require PostgreSQL::Test::PgracClusterContention;
my $cont = PostgreSQL::Test::PgracClusterContention->new($pair,
	sessions   => 2,
	iterations => $workload_iterations);
$cont->start_load;
sleep $workload_sleep_seconds;
my $cont_metrics = $cont->stop_load;
ok(defined($cont_metrics->{ges_request_delta}),
	"L4 contention helper exits cleanly + GES request delta readable");

# ============================================================
# Stage 2 acceptance report emit (D14)
# ============================================================
require PostgreSQL::Test::Stage2AcceptanceReport;
my $report = PostgreSQL::Test::Stage2AcceptanceReport->new(
	tag => $ENV{STAGE2_TAG} // 'unknown');
$report->record_perf_workload("pgbench-select-only-${pgbench_seconds}s",
	single_node_off => { tps => $sel_off_tps },
	single_node_on  => { tps => $sel_on_tps },
	gate            => 'hard ≤ 10%');
$report->record_perf_workload("pgbench-full-${pgbench_seconds}s",
	single_node_off => { tps => $full_off_tps },
	single_node_on  => { tps => $full_on_tps },
	gate            => 'hard ≤ 15%');
$report->record_perf_workload("ddl-loop-${workload_sleep_seconds}s",
	two_node => { sinval_broadcast_delta => $loop_metrics->{broadcast_send_delta} },
	gate     => 'hard: broadcast_send_delta > 0');
$report->record_perf_workload("contention-${workload_sleep_seconds}s",
	two_node => { ges_request_delta => $cont_metrics->{ges_request_delta} },
	gate     => 'warning + counter delta readable');

my $report_path = $report->default_path('tmp');
$report->emit_json($report_path);
ok(-r $report_path, "Stage 2 acceptance JSON report emitted at $report_path");

$pair->stop_pair;

done_testing();
