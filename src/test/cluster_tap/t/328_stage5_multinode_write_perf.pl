#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 328_stage5_multinode_write_perf.pl
#    spec-5.19 D3 — Stage 5 integrated acceptance: write-path performance
#    HARD GATE (MG-B).
#
#    Acceptance ruling (user, 2026-06-29, rule 8.B): the currently-measurable
#    native-vs-cluster write-path degradation is a **perf BLOCKER that spec-
#    5.19 must close**, NOT a measure-only / forward item.  The gate:
#
#        single-node cluster write tax  <=  10%   (REQUIRED PASS)
#        > 10%                          ->  FAIL / BLOCKED  ->  root-cause +
#                                            optimize inside 5.19
#
#    This is the spec-3.25 single-node-no-peer C-floor comparison: native
#    (cluster.enabled = off) vs cluster-enabled single node, same build, same
#    fsync/shared_buffers, pgbench TPC-B write workload, best-of-N for
#    stability, ratio-based so the runner's absolute speed cancels.
#
#    SEPARATE capability limitation (does NOT cover or excuse the gate above):
#    true concurrent multi-node shared-block write competition is bounded by
#    cross-node holder migration (DRM = Stage 6;  spec-5.57 cross-instance
#    CR).  That limit is recorded as a capability forward — it MUST NOT be
#    used to reframe the single-node tax as measure-only (the user's explicit
#    correction).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/328_stage5_multinode_write_perf.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Stage5IntegratedAcceptanceReport;
use Test::More;

my $report = PostgreSQL::Test::Stage5IntegratedAcceptanceReport->new(
	tag => $ENV{PGRAC_TAG} // 'unknown');

my $PGBENCH    = $ENV{PGBENCH} // 'pgbench';
my $SCALE      = 10;
my $SECS       = $ENV{PGRAC_PGBENCH_SECS} // 8;
my $CLIENTS    = 4;
my $ROUNDS     = $ENV{PGRAC_PGBENCH_ROUNDS} // 7;    # interleaved rounds
# The hard gate: cluster write tax must not exceed this percentage.
my $GATE_PCT   = $ENV{PGRAC_WRITE_TAX_GATE_PCT} // 10.0;

# One pgbench TPC-B (-N) run against a node; returns tps (float) or undef.
sub pgbench_one
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $out = `$PGBENCH -n -T $SECS -c $CLIENTS -j 2 -N $conn 2>&1`;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)/;
	return undef;
}

sub pgbench_init
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	system("$PGBENCH -i -s $SCALE -q $conn >/dev/null 2>&1");
	return $? == 0;
}

sub median
{
	my @s = sort { $a <=> $b } @_;
	return undef unless @s;
	return $s[int((@s) / 2)];
}

# Common perf-isolation knobs (CPU-overhead measurement: fsync off removes
# disk variance so the gate measures the cluster machinery's added work).
my @perf_conf = (
	"autovacuum = off\n",
	"fsync = off\n",
	"shared_buffers = 64MB\n",
	"max_wal_size = 4GB\n",
);

# ---------------------------------------------------------------------
# Boot BOTH nodes simultaneously so the interleaved rounds measure native
# and cluster under the same momentary system load — the ratio (tax) then
# cancels runner speed AND transient load.  The MEDIAN of N rounds rejects
# the occasional load-skewed outlier (essential on a shared runner).
# ---------------------------------------------------------------------
my $native = PostgreSQL::Test::Cluster->new('mnw_native');
$native->init;
$native->append_conf('postgresql.conf', $_) for @perf_conf;
$native->start;

my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $clu = PostgreSQL::Test::Cluster->new('mnw_cluster');
$clu->init;
$clu->append_conf('postgresql.conf', "cluster.enabled = on\n");
$clu->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$clu->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$clu->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$clu->append_conf('postgresql.conf', $_) for @perf_conf;
PostgreSQL::Test::Utils::append_to_file(
	$clu->data_dir . '/pgrac.conf',
	"[cluster]\nname = mnw_cluster\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
$clu->start;

my $init_ok = pgbench_init($native) && pgbench_init($clu);

my (@nat, @clu, @taxes);
if ($init_ok)
{
	for my $r (1 .. $ROUNDS)
	{
		my $n = pgbench_one($native);
		my $c = pgbench_one($clu);
		next unless defined $n && $n > 0 && defined $c && $c > 0;
		push @nat, $n;
		push @clu, $c;
		push @taxes, 100.0 * (1.0 - $c / $n);
		note(sprintf("  round %d: native=%.0f cluster=%.0f tax=%.2f%%",
			$r, $n, $c, $taxes[-1]));
	}
}
my $wait_events_present = $clu->safe_psql('postgres',
	"SELECT count(*) FROM pg_stat_cluster_wait_events") // 0;
$native->stop;
$clu->stop;

# ---------------------------------------------------------------------
# M1: single-node write tax — HARD GATE (median of interleaved rounds).
# ---------------------------------------------------------------------
my $have_both = (scalar(@taxes) > 0);
my $tax = $have_both ? median(@taxes) : undef;
my $tax_s = defined $tax ? sprintf('%.2f', $tax) : 'n/a';
my $tps_native = $have_both ? median(@nat) : undef;
my $tps_cluster = $have_both ? median(@clu) : undef;

note("MG-B single-node write-path HARD GATE (median of "
	. scalar(@taxes) . " interleaved rounds):");
note("  native  TPC-B median tps = " . (defined $tps_native ? sprintf('%.0f', $tps_native) : 'n/a'));
note("  cluster TPC-B median tps = " . (defined $tps_cluster ? sprintf('%.0f', $tps_cluster) : 'n/a'));
note("  write tax % (median)     = $tax_s (gate: <= $GATE_PCT%)");

ok($have_both,
	"M0 native + cluster single-node throughput measured over "
	. scalar(@taxes) . " interleaved rounds");

# THE HARD GATE (rule 8.B): tax <= 10%.  > 10% is a perf blocker.
my $gate_pass = (defined $tax && $tax <= $GATE_PCT);
ok($gate_pass,
	"M1 single-node cluster write tax ${tax_s}% <= ${GATE_PCT}% (HARD GATE; "
	. "tax > ${GATE_PCT}% is a perf blocker that must be root-caused + "
	. "optimized inside spec-5.19 — rule 8.B, never deferred)");
unless ($gate_pass)
{
	diag("MG-B PERF BLOCKER: single-node cluster write tax ${tax_s}% exceeds "
		. "the ${GATE_PCT}% gate. spec-5.19 is BLOCKED on this until the "
		. "cluster write hot-path (ITL stamp + undo record + UBA + per-record "
		. "heap-ITL WAL delta) is optimized below the gate.");
}

$report->record_multinode_write_value(1, 'tpcb',
	tps_native => (defined $tps_native ? $tps_native : 0),
	tps_cluster => (defined $tps_cluster ? $tps_cluster : 0),
	write_tax_pct => $tax_s,
	gate_pct => $GATE_PCT,
	gate => $gate_pass ? 'PASS' : 'FAIL-BLOCKER',
	wait_event_rows => $wait_events_present);

# ---------------------------------------------------------------------
# M2: cluster write-path wait-event observability surface present.
# ---------------------------------------------------------------------
ok($wait_events_present > 0,
	"M2 cluster write-path wait-event surface present ($wait_events_present rows)");

# ---------------------------------------------------------------------
# SOUNDNESS — the single-node tax above is REAL + gated.  The TRUE concurrent
# multi-node shared-block write limit is a SEPARATE capability limitation that
# does NOT cover the single-node gate (the user's explicit correction).
# ---------------------------------------------------------------------
$report->set_multinode_write_soundness('single-node-gated',
	real => 1,
	note => 'single-node write tax is REAL + HARD-GATED at <= ' . $GATE_PCT
		. '%. SEPARATE capability limitation: true concurrent cross-node '
		. 'shared-block write competition is DRM-bounded (Stage 6 / spec-5.57) '
		. '-- a capability forward, NOT a cover for the single-node tax gate.');
$report->record_limitation('true concurrent multi-node shared-block write',
	kind => 'capability-forward', forward => '5.57 / 6.4a-DRM',
	note => 'capability limit only; does NOT excuse the single-node write-tax '
		. 'gate (rule 8.B). Concurrent cross-node writes fail-closed (no '
		. 'corruption, t/327 L3).');

my $out_path = $ENV{PGRAC_ACCEPTANCE_JSON}
	// $report->default_path($ENV{TESTDATADIR} || "tmp_check");
eval { $report->emit_json($out_path); Test::More::note("MG-B gate record: $out_path"); };

done_testing();
