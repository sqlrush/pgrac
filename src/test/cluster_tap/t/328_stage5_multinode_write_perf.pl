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
#    Additional report-only measurement:
#        two-node peer-online single-writer write tax
#
#    This boots a real ClusterPair (strict quorum + shared_data) and measures
#    TPC-B writes on node0 while node1 is connected/in quorum.  It is a value
#    report only: no percentage threshold is asserted here.
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
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Stage5IntegratedAcceptanceReport;
use Test::More;

my $report = PostgreSQL::Test::Stage5IntegratedAcceptanceReport->new(
	tag => $ENV{PGRAC_TAG} // 'unknown');

my $PGBENCH    = $ENV{PGBENCH} // 'pgbench';
my $SCALE      = 10;
# De-noise the ratio (2026-07-07): native and cluster are measured in
# SEPARATE windows, so runner load that drifts BETWEEN them skews the
# per-round tax; the median rejects random per-round noise but NOT a
# per-run systematic skew (observed: a run whose native windows all landed
# on a quiet phase read tax 19.71% while native tps was ~2x its usual ~5300
# -- native got faster, cluster did not get slower).  Shorter windows drawn
# closer together keep native and cluster under near-identical momentary
# load so the ratio cancels the drift, and more rounds tighten the median.
# NOT a weakening: same TPC-B (-N) workload, same 4 clients, same fsync/
# shared_buffers, same <=10% gate, same ratio metric -- only the sampling
# is finer (2s x 30 pairs vs 8s x 7).  A real tax regression still reads
# true; the measurement is just tighter around it.
my $SECS       = $ENV{PGRAC_PGBENCH_SECS} // 2;
my $CLIENTS    = 4;
my $ROUNDS     = $ENV{PGRAC_PGBENCH_ROUNDS} // 30;   # interleaved pairs
my $TWO_NODE_ROUNDS = $ENV{PGRAC_2NODE_PGBENCH_ROUNDS} // 10;  # report-only; fewer rounds bound test time
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

sub poll_sql_eq
{
	my ($node, $sql, $want, $timeout_s) = @_;
	$timeout_s //= 15;
	my $deadline = time + $timeout_s;
	my $last = '(never-queried)';
	while (time < $deadline)
	{
		my $got = eval { $node->safe_psql('postgres', $sql); };
		$last = defined $got ? $got : '(undef)';
		return 1 if defined $got && $got eq $want;
		select(undef, undef, undef, 0.25);
	}
	diag("poll_sql_eq timeout after ${timeout_s}s: want='$want' last='$last' sql=$sql");
	return 0;
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

# Surface the measured median to the captured CI log unconditionally:  note()
# is swallowed by non-verbose prove, but diag() reaches the log even on PASS.
# This makes the gate's headroom (e.g. spec-5.19 MG-D v3 WAL delta effect)
# visible without re-running the shard verbose.
diag(sprintf("MG-B single-node write tax (median of %d rounds) = %s%% "
		. "(gate <= %s%%; native=%s cluster=%s median tps)",
	scalar(@taxes), $tax_s, $GATE_PCT,
	(defined $tps_native ? sprintf('%.0f', $tps_native) : 'n/a'),
	(defined $tps_cluster ? sprintf('%.0f', $tps_cluster) : 'n/a')));

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
# M3: two-node peer-online write tax — REPORT ONLY.
#
# This is deliberately NOT a hard gate.  It measures the current two-node
# online shape that Stage 5 can soundly run: strict-quorum ClusterPair with
# shared_data, node0 executing TPC-B writes while node1 is connected and in
# quorum.  True concurrent dual-writer shared-block competition remains the
# separate DRM/Stage-6 limitation recorded below.
# ---------------------------------------------------------------------
my @two_node_tps;
my $two_node_started = 0;
my $two_node_ready = 0;
my $two_init_ok = 0;
my $two_err;
eval {
	my @pair_perf_conf = map { my $line = $_; chomp $line; $line } @perf_conf;
	my $pair = PostgreSQL::Test::ClusterPair->new_pair(
		'mnw_pair',
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [
			@pair_perf_conf,
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 2000',
			'cluster.cssd_dead_deadband_factor = 10',
		]);
	$pair->start_pair;
	$two_node_started = 1;
	$two_node_ready =
	  $pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30)
	  && poll_sql_eq($pair->node0, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20)
	  && poll_sql_eq($pair->node1, 'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20);

	if ($two_node_ready)
	{
		$two_init_ok = pgbench_init($pair->node0) ? 1 : 0;
		if ($two_init_ok)
		{
			for my $r (1 .. $TWO_NODE_ROUNDS)
			{
				my $t = pgbench_one($pair->node0);
				next unless defined $t && $t > 0;
				push @two_node_tps, $t;
				note(sprintf("  two-node round %d: node0 tps=%.0f", $r, $t));
			}
		}
	}
	$pair->stop_pair;
	1;
} or do {
	$two_err = $@ || 'unknown error';
	diag("M3 two-node report-only measurement failed before completion: $two_err");
};

my $two_have = ($two_node_started && $two_node_ready && scalar(@two_node_tps) > 0
	&& defined $tps_native && $tps_native > 0);
my $two_tps = scalar(@two_node_tps) ? median(@two_node_tps) : undef;
my $two_tax = ($two_have && defined $two_tps)
	? 100.0 * (1.0 - $two_tps / $tps_native) : undef;
my $two_tax_s = defined $two_tax ? sprintf('%.2f', $two_tax) : 'n/a';

my $native_s = defined $tps_native ? sprintf('%.0f', $tps_native) : 'n/a';
my $two_tps_s = defined $two_tps ? sprintf('%.0f', $two_tps) : 'n/a';

# Specific unavailable reason (reported even on PASS so the report-only leg is
# never a silent black box).
my $two_reason;
if (!$two_node_started)
{
	$two_reason = "ClusterPair failed to boot/start"
		. (defined $two_err ? ": $two_err" : "");
}
elsif (!$two_node_ready)
{
	$two_reason = "peers did not reach connected + in_quorum within timeout";
}
elsif (!$two_init_ok)
{
	$two_reason = "pgbench init on node0 failed";
}
elsif (!scalar(@two_node_tps))
{
	$two_reason = "pgbench produced no valid (>0 tps) rounds";
}
elsif (!(defined $tps_native && $tps_native > 0))
{
	$two_reason = "native single-node baseline tps missing";
}

# diag() reaches the captured CI log even on PASS / non-verbose prove, so the
# 2-node report-only numbers are always visible alongside the M1 single-node
# gate -- not just when run with -v.
diag("MG-B two-node peer-online write-path REPORT-ONLY measurement:");
diag("  native single-node TPC-B median tps         = $native_s");
diag("  two-node peer-online node0 TPC-B median tps = $two_tps_s");
diag("  two-node write tax % (report-only)          = $two_tax_s"
	. (defined $two_reason ? "  (unavailable: $two_reason)" : ""));

# REPORT ONLY: this leg must never fail the single-node hard gate.  If the
# 2-node ClusterPair could not boot / reach quorum / produce a number this run
# (transient runner shmem pressure, etc.), pass with an explicit unavailable
# note rather than failing -- the HARD gate is the single-node M1 tax only.
if ($two_have)
{
	ok(1,
		"M3 two-node peer-online single-writer write tax measured: ${two_tax_s}% "
		. "(native=$native_s two-node=$two_tps_s tps; REPORT ONLY; no threshold asserted)");
}
else
{
	ok(1,
		"M3 two-node peer-online write tax unavailable this run: "
		. ($two_reason // 'unknown reason')
		. " (REPORT ONLY; never fails the single-node hard gate)");
}
$report->record_multinode_write_value(2, 'tpcb-peer-online-single-writer',
	tps_native => (defined $tps_native ? $tps_native : 0),
	tps_cluster => (defined $two_tps ? $two_tps : 0),
	write_tax_pct => $two_tax_s,
	gate => 'REPORT-ONLY',
	note => 'ClusterPair strict-quorum + shared_data; node0 writes while node1 '
		. 'is connected/in quorum. No threshold asserted.');

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
