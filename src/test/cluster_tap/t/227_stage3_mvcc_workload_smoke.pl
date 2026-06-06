# -*- perl -*-
#
# 227_stage3_mvcc_workload_smoke.pl
#	  spec-3.17 D2 — Stage 3 MVCC workload smoke (CI-safe, 4 blocks).
#
#	  L1 pgbench TPC-B select-only   (single-node on/off; report-only)
#	  L2 pgbench TPC-B full r/w      (single-node on/off; report-only —
#	                                  the MVCC write overhead shows here)
#	  L3 undo-heavy UPDATE burst     (drives undo/CR; counter delta report)
#	  L4 2PC burst                   (PREPARE/COMMIT PREPARED loop)
#
#	  Honest-baseline discipline (spec-3.17 §3.2):  every ratio/TPS number
#	  is REPORT-ONLY — no numeric perf fail.  The only hard gates are
#	  functional:  pgbench command succeeded, a TPS field was parsed and is
#	  > 0, the JSON report is complete, and the SQL correctness assertions
#	  pass.  The performance bar is owned by spec-3.18;  multi-node trend is
#	  collected by scripts/perf/run-stage3-mvcc-baseline.sh (D3).
#
#	  Smoke scope:  <= 2min total.
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/227_stage3_mvcc_workload_smoke.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Stage3AcceptanceReport;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $pgbench_seconds = $ENV{STAGE3_PGBENCH_SECONDS} // 8;


# Extract TPS from pgbench stdout (0 when unparsed).
sub _pgbench_tps
{
	my ($output) = @_;
	return ($output // '') =~ /tps = ([\d.]+)/m ? $1 + 0 : 0;
}

sub _diag_limited
{
	my ($label, $text) = @_;
	return if !defined $text || $text eq '';
	$text = substr($text, 0, 4096) . "\n...[truncated]...\n" if length($text) > 4096;
	diag("$label:\n$text");
}

sub _pgbench_init
{
	my ($node, $label) = @_;
	my ($out, $err);
	my $ok = $node->run_log(
		[ 'pgbench', '-i', '-s', '1', '-q', '-p', $node->port, '-h', $node->host,
			'postgres' ], '>', \$out, '2>', \$err);
	if (!$ok)
	{
		_diag_limited("$label pgbench init stdout", $out);
		_diag_limited("$label pgbench init stderr", $err);
	}
	return $ok;
}

# Full TPC-B run.  Dies only on a real functional failure (FATAL/PANIC or
# failed transactions) — a 0 TPS parse alone is treated as report-only.
sub _pgbench_full
{
	my ($node, $seconds) = @_;
	my ($out, $err);
	my $ok = $node->run_log(
		[ 'pgbench', '-c', '4', '-T', "$seconds", '-n', '-p', $node->port,
			'-h', $node->host, 'postgres' ], '>', \$out, '2>', \$err);
	my $tps = _pgbench_tps($out);
	my $failed = ($out // '') =~ /number of failed transactions: (\d+)/m ? $1 + 0 : 0;
	my $fatal = defined $err && $err =~ /(?:ERROR|FATAL|PANIC):|pgbench:\s+error:/i;
	if ($fatal || $failed > 0)
	{
		_diag_limited("pgbench full stdout", $out);
		_diag_limited("pgbench full stderr", $err);
		die "pgbench full failed on node " . $node->name . "\n";
	}
	return $tps;
}

# Read one cluster_debug counter (0 when absent).
sub _counter
{
	my ($node, $cat, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state
			WHERE category='$cat' AND key='$key'), 0)});
}


my $report = PostgreSQL::Test::Stage3AcceptanceReport->new(
	spec => '3.17', tag => $ENV{PGRAC_TAG} // 'unknown');


# ============================================================
# L1/L2: single-node cluster.enabled off vs on (report-only).
# ============================================================
# Baseline (cluster.enabled=off == native PG).
my $off = PostgreSQL::Test::Cluster->new('s3_smoke_off');
$off->init;
$off->append_conf('postgresql.conf', "shared_buffers = 128MB\n");
$off->start;
die "off pgbench init failed\n" unless _pgbench_init($off, 'off');

my $sel_off;
$off->run_log([ 'pgbench', '-S', '-c', '4', '-T', "$pgbench_seconds", '-n',
	'-p', $off->port, '-h', $off->host, 'postgres' ], '>', \$sel_off);
my $sel_off_tps = _pgbench_tps($sel_off);
my $full_off_tps = _pgbench_full($off, $pgbench_seconds);
$off->stop;

# cluster.enabled=on (single node, no peer).  Undo/TT/durable writes stay on;
# hot-page CR reconstruction is gated off so pgbench's tiny teller/branch pages
# do not hit retryable CR before the perf signal is recorded (mirrors t/202;
# CR correctness is covered by t/215-218, perf by spec-3.18).
my $on = PostgreSQL::Test::Cluster->new('s3_smoke_on');
$on->init;
$on->append_conf('postgresql.conf',
	"shared_buffers = 128MB\n"
	  . "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.interconnect_tier = stub\n"
	  . "cluster.cr_mvcc_gate = off\n"
	  . "max_prepared_transactions = 20\n");
$on->start;

my $sel_on_tps = 0;
my $full_on_tps = 0;
if (_pgbench_init($on, 'on'))
{
	my $sel_on;
	$on->run_log([ 'pgbench', '-S', '-c', '4', '-T', "$pgbench_seconds", '-n',
		'-p', $on->port, '-h', $on->host, 'postgres' ], '>', \$sel_on);
	$sel_on_tps = _pgbench_tps($sel_on);
	$full_on_tps = _pgbench_full($on, $pgbench_seconds);
}

# Functional hard gate: both off + on produced a parseable, positive TPS.
ok($sel_off_tps > 0, "L1 select-only off TPS parsed > 0 ($sel_off_tps)");
ok($full_off_tps > 0, "L2 full off TPS parsed > 0 ($full_off_tps)");
ok($sel_on_tps > 0, "L1 select-only on TPS parsed > 0 ($sel_on_tps)");
ok($full_on_tps > 0, "L2 full on TPS parsed > 0 ($full_on_tps)");

# Report-only trend bands (never fail).
sub _band
{
	my ($off_tps, $on_tps, @thresh) = @_;
	return ('n/a', 0) if $off_tps <= 0 || $on_tps <= 0;
	my $reg = 100.0 * (1.0 - $on_tps / $off_tps);
	my @labels = ('GREEN', 'YELLOW', 'ORANGE', 'RED');
	my $status = 'CATASTROPHIC';
	for my $i (0 .. $#thresh)
	{
		if ($reg <= $thresh[$i]) { $status = $labels[$i]; last; }
	}
	return ($status, $reg);
}

my ($l1_status, $l1_reg) = _band($sel_off_tps, $sel_on_tps, 10, 30, 50);
my ($l2_status, $l2_reg) = _band($full_off_tps, $full_on_tps, 15, 25, 40, 60);
diag(sprintf "L1 select-only on/off regression: %.1f%% [%s] (report-only; spec-3.18)",
	$l1_reg, $l1_status);
diag(sprintf "L2 full r/w on/off regression: %.1f%% [%s] (report-only; spec-3.18)",
	$l2_reg, $l2_status);

$report->record_workload('pgbench-select-only', functional => 'PASS',
	single_node_off_tps => $sel_off_tps, single_node_on_tps => $sel_on_tps,
	regression_pct => sprintf('%.1f', $l1_reg), band => $l1_status,
	gate => 'report-only; spec-3.18 read-path target');
$report->record_workload('pgbench-full-rw', functional => 'PASS',
	single_node_off_tps => $full_off_tps, single_node_on_tps => $full_on_tps,
	regression_pct => sprintf('%.1f', $l2_reg), band => $l2_status,
	gate => 'report-only; spec-3.18 write-path target');


# ============================================================
# L3: undo-heavy UPDATE burst (drives undo/CR machinery).
# ============================================================
{
	$on->safe_psql('postgres', q{
		CREATE TABLE l3_heavy (id int primary key, v bigint);
		INSERT INTO l3_heavy SELECT g, 0 FROM generate_series(1, 500) g;
	});
	my $undo_before = _counter($on, 'undo', 'record_alloc_count');
	for my $i (1 .. 20)
	{
		$on->safe_psql('postgres', "UPDATE l3_heavy SET v = v + $i");
	}
	my $undo_after = _counter($on, 'undo', 'record_alloc_count');
	my $sum = $on->safe_psql('postgres', q{SELECT sum(v) FROM l3_heavy});
	# 20 updates * 500 rows, v = 1+2+...+20 = 210 each -> 105000.
	is($sum, '105000', 'L3 undo-heavy UPDATE burst arithmetic correct');
	ok($undo_after > $undo_before,
		"L3 undo accumulates under UPDATE burst ($undo_before -> $undo_after)");
	$report->record_workload('undo-heavy-update-burst', functional => 'PASS',
		undo_record_delta => ($undo_after - $undo_before),
		gate => 'hard: SQL correctness + undo delta > 0');
}


# ============================================================
# L4: 2PC burst (PREPARE / COMMIT PREPARED loop).
# ============================================================
{
	# Wide filler so ~4 rows land per 8KB heap page:  a same-page 2PC burst
	# would otherwise hit the INITRANS=8 ITL cap (committed-but-uncleaned
	# slots accumulate under delayed cleanout — Oracle-aligned ITL density,
	# a fail-closed ERROR, not a visibility bug).  The 1900-byte filler stays
	# UNDER the ~2KB TOAST threshold, so it is stored inline uncompressed
	# (a larger, compressible filler would be toasted+compressed back to a
	# tiny inline tuple and re-cluster every row onto page 0).
	$on->safe_psql('postgres',
		'CREATE TABLE l4_2pc (id int primary key, v int, filler text)');
	my $tp_before = _counter($on, 'tt_2pc', 'twopc_prepare_records');
	my $burst = 20;
	for my $i (1 .. $burst)
	{
		$on->safe_psql('postgres', qq{
			BEGIN; INSERT INTO l4_2pc VALUES ($i, $i, repeat('x', 1900));
			PREPARE TRANSACTION 'b227_$i';
		});
		$on->safe_psql('postgres', qq{COMMIT PREPARED 'b227_$i'});
	}
	my $tp_after = _counter($on, 'tt_2pc', 'twopc_prepare_records');
	my $cnt = $on->safe_psql('postgres', q{SELECT count(*) FROM l4_2pc});
	is($cnt, "$burst", "L4 2PC burst committed all $burst prepared xacts");
	ok($tp_after > $tp_before,
		"L4 twopc_prepare_records advanced ($tp_before -> $tp_after)");
	$report->record_workload('2pc-burst', functional => 'PASS',
		twopc_prepare_delta => ($tp_after - $tp_before), burst => $burst,
		gate => 'hard: all prepared committed + twopc delta > 0');
}

$on->stop;


# ============================================================
# Acceptance report emit (hard gate: JSON complete + readable).
# ============================================================
my $path = $report->default_path($ENV{TESTDIR} ? "$ENV{TESTDIR}/tmp" : 'tmp');
$report->emit_json($path);
ok(-r $path && -s $path, "Stage 3 workload report emitted at $path");

done_testing();
