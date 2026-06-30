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
#    M3 — two-node peer-online SINGLE-WRITER write tax: HARD AVAILABILITY GATE
#    (user, 2026-06-30).  Boots a real ClusterPair (strict quorum + shared_data)
#    and measures node0's TPC-B writes (1 client) while node1 is connected/in
#    quorum, against an interleaved native 1-client baseline.  NO tax % threshold
#    is asserted, but the number MUST be obtained: pair start / peers connected /
#    both in_quorum / pgbench init / >=1 valid round are each a hard ok() -- any
#    failure fails t/328 (no silent n/a pass).
#
#    SINGLE-WRITER = 1 client.  Sustained MULTI-client same-block extend
#    contention collapses the cross-node GES coordination throughput (the t/327
#    L2 "transient CF timeout"); its root-cause fix is a tracked follow-up
#    (spec-5.19 §10 / Stage-6 DRM), NOT measured here.  SEPARATE capability
#    limitation: true concurrent dual-WRITER shared-block competition is bounded
#    by cross-node holder migration (DRM = Stage 6; spec-5.57).  Neither limit
#    excuses the M1 perf gate or the M3 availability gate (rule 8.B).
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
my $SECS       = $ENV{PGRAC_PGBENCH_SECS} // 8;
my $CLIENTS    = 4;
my $ROUNDS     = $ENV{PGRAC_PGBENCH_ROUNDS} // 7;    # interleaved rounds
my $TWO_NODE_ROUNDS = $ENV{PGRAC_2NODE_PGBENCH_ROUNDS} // $ROUNDS;
# The hard gate: cluster write tax must not exceed this percentage.
my $GATE_PCT   = $ENV{PGRAC_WRITE_TAX_GATE_PCT} // 10.0;

# One pgbench TPC-B (-N) run against a node; returns tps (float) or undef.
# $clients defaults to $CLIENTS; M3 (the two-node single-writer leg) passes 1.
sub pgbench_one
{
	my ($node, $clients) = @_;
	$clients //= $CLIENTS;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $out = `$PGBENCH -n -T $SECS -c $clients -j 2 -N $conn 2>&1`;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)/;
	return undef;
}

# pgbench -i with FULL output capture (never /dev/null): on failure the entire
# init output (incl. server ERROR/CONTEXT/HINT) is diag'd to the CI log so a
# regression in the cluster write path is never a silent black box (user
# directive, 2026-06-30).  Returns 1 on success, 0 on failure.
sub pgbench_init
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	my $out = `$PGBENCH -i -s $SCALE -q $conn 2>&1`;
	my $rc  = $?;
	if ($rc != 0)
	{
		diag("pgbench -i FAILED on '" . $node->name . "' (rc=$rc):\n"
			. "----- pgbench init output -----\n$out\n----- end pgbench init output -----");
	}
	return $rc == 0;
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
# Stop the single-node cluster, but KEEP the native node up: M3 reuses it as the
# native 1-client baseline, interleaved with the two-node pair so the ratio
# cancels runner load (same technique as M1).
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
# M3: two-node peer-online SINGLE-WRITER write tax — HARD AVAILABILITY GATE.
#
# spec-5.19 #3 ruling (user, 2026-06-30): this leg is a HARD AVAILABILITY GATE.
# It asserts NO tax % threshold, but the number MUST be obtained -- n/a is RED.
# It boots a real strict-quorum ClusterPair (shared_data) and measures node0's
# TPC-B writes while node1 is connected + in quorum, against a native 1-client
# baseline interleaved to cancel runner load (same technique as M1).  Each
# precondition -- pair start / peers connected / both in_quorum / pgbench init /
# >=1 valid (>0 tps) round on BOTH native and two-node -- is its own hard ok():
# any failure fails t/328.  No silent n/a pass.
#
# SINGLE-WRITER = exactly 1 client.  A sustained MULTI-client same-block extend
# workload collapses the cross-node GES coordination throughput (a documented
# Stage-5 limitation = the t/327 L2 "transient CF timeout"; its root-cause fix
# is a tracked follow-up -- spec-5.19 §10 / Stage-6 DRM -- NOT measured here).
# 1 client still exercises the FULL cross-node coordination (GCS block-X to
# remote masters + HW relation-extend coordinated with node1), so it is a
# credible 2-node-online write tax and is literally the "single-writer" named.
# ---------------------------------------------------------------------
my $TWO_NODE_CLIENTS = 1;
my @two_node_tps;
my @two_native_tps;
my $two_node_started = 0;
my $two_node_ready = 0;
my $two_init_ok = 0;
my $two_err;
my $pair;
eval {
	my @pair_perf_conf = map { my $line = $_; chomp $line; $line } @perf_conf;
	$pair = PostgreSQL::Test::ClusterPair->new_pair(
		'mnw_pair',
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [
			@pair_perf_conf,
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 2000',
			'cluster.cssd_dead_deadband_factor = 10',
			# spec-5.19 #2 (capacity + Stage-6-reclaim docs): size the GRD/GES
			# tables above the single-writer remote-mastered working set
			# (scale-10 ~ tens of thousands of distinct blocks).  The local-master
			# release reclaim does NOT cover remote-mastered / block-lock entries
			# until the Stage-6 DRM cold-entry reclaim lands, so the cap must
			# exceed the run's distinct remote-mastered resource set.
			'cluster.grd_max_entries = 65536',
			'cluster.ges_dedup_max_entries = 65536',
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
			# Interleave native(1c) and pair-node0(1c) rounds so the ratio
			# cancels momentary runner load.  MEDIAN rejects the odd outlier.
			for my $r (1 .. $TWO_NODE_ROUNDS)
			{
				my $n = pgbench_one($native, $TWO_NODE_CLIENTS);
				my $t = pgbench_one($pair->node0, $TWO_NODE_CLIENTS);
				next unless defined $n && $n > 0 && defined $t && $t > 0;
				push @two_native_tps, $n;
				push @two_node_tps,    $t;
				note(sprintf("  two-node round %d: native(1c)=%.0f node0(1c)=%.0f", $r, $n, $t));
			}
		}
	}
	$pair->stop_pair if $pair;
	1;
} or do {
	$two_err = $@ || 'unknown error';
	diag("M3 two-node measurement threw before completion: $two_err");
	eval { $pair->stop_pair if $pair; };
};

$native->stop;

my $two_tps    = scalar(@two_node_tps)    ? median(@two_node_tps)    : undef;
my $two_native = scalar(@two_native_tps)  ? median(@two_native_tps)  : undef;
my $two_tax = (defined $two_tps && defined $two_native && $two_native > 0)
	? 100.0 * (1.0 - $two_tps / $two_native) : undef;
my $two_tax_s    = defined $two_tax    ? sprintf('%.2f', $two_tax)    : 'n/a';
my $two_native_s = defined $two_native ? sprintf('%.0f', $two_native) : 'n/a';
my $two_tps_s    = defined $two_tps    ? sprintf('%.0f', $two_tps)    : 'n/a';
my $rounds_have  = scalar(@two_node_tps);

# diag() reaches the captured CI log even on PASS / non-verbose prove, so the
# three required numbers are always visible (user directive #6).
diag("MG-B two-node peer-online SINGLE-WRITER (1 client) write-path HARD AVAILABILITY GATE:");
diag("  native single-node (1c) TPC-B median tps     = $two_native_s");
diag("  two-node peer-online node0 (1c) median tps   = $two_tps_s");
diag("  two-node write tax % (no threshold asserted) = $two_tax_s");
diag("  (pre-completion error: $two_err)") if defined $two_err;

# HARD AVAILABILITY GATE (user, 2026-06-30): every precondition is its own hard
# ok().  A failure of ANY step fails t/328 -- there is no silent n/a pass.  The
# tax % itself is reported with NO threshold asserted.
ok($two_node_started,
	"M3a two-node ClusterPair booted"
	. ((defined $two_err && !$two_node_started) ? " — error: $two_err" : ""));
ok($two_node_ready, "M3b two-node peers connected + both in_quorum");
ok($two_init_ok,
	"M3c pgbench -i on node0 succeeded (full output diag'd on failure, never /dev/null)");
ok($rounds_have > 0,
	"M3d two-node node0 produced >=1 valid (>0 tps) single-writer round "
	. "($rounds_have round(s))");
ok(defined $two_tax,
	"M3 two-node peer-online single-writer write tax MEASURED = ${two_tax_s}% "
	. "(native(1c)=$two_native_s two-node(1c)=$two_tps_s tps; HARD AVAILABILITY GATE -- "
	. "the number must be obtained, no threshold asserted; sustained multi-writer "
	. "same-block extend contention is a tracked Stage-6 follow-up, not measured here)");

$report->record_multinode_write_value(2, 'tpcb-peer-online-single-writer-1c',
	tps_native => (defined $two_native ? $two_native : 0),
	tps_cluster => (defined $two_tps ? $two_tps : 0),
	write_tax_pct => $two_tax_s,
	gate => (defined $two_tax ? 'AVAILABILITY-PASS' : 'AVAILABILITY-FAIL'),
	note => 'ClusterPair strict-quorum + shared_data; node0 single writer (1 client) '
		. 'while node1 connected/in quorum; native 1c baseline interleaved. HARD '
		. 'availability gate (number MUST be obtained; no tax threshold asserted).');

# ---------------------------------------------------------------------
# SOUNDNESS — M1 single-node tax is REAL + perf-gated;  M3 two-node single-
# writer (1c) tax is REAL + AVAILABILITY-gated (measured, no threshold).  Two
# SEPARATE limitations are recorded as tracked follow-ups (NOT covers for the
# gates above, rule 8.B):
#   (a) sustained MULTI-client same-block extend contention collapses the
#       cross-node GES coordination throughput (the t/327 L2 transient CF
#       timeout) -- root-cause fix tracked (spec-5.19 §10 / Stage-6 DRM);
#   (b) true concurrent dual-WRITER shared-block competition (holder migration)
#       is DRM-bounded (Stage 6 / spec-5.57).
# ---------------------------------------------------------------------
$report->set_multinode_write_soundness('single-writer-availability-gated',
	real => 1,
	note => 'M1 single-node tax perf-gated (<= ' . $GATE_PCT . '%); M3 two-node '
		. 'single-writer (1c) tax availability-gated (measured, no threshold). '
		. 'SEPARATE: sustained multi-client same-block extend contention collapses '
		. 'cross-node GES throughput (t/327 L2 transient-CF-timeout); root-cause is '
		. 'a tracked follow-up (spec-5.19 §10 / Stage-6 DRM), NOT a cover for either gate.');
$report->record_limitation('sustained multi-writer same-block extend contention',
	kind => 'perf-followup', forward => 'spec-5.19 §10 / Stage-6 DRM',
	note => 'sustained multi-client same-block (relation-extend) contention collapses '
		. 'cross-node GES coordination throughput (engine-level backstops degrade it; '
		. 'reverted). M3 measures the sound single-writer (1c) shape; root-cause fix '
		. 'tracked. Does NOT excuse the M1 perf gate or the M3 availability gate.');
$report->record_limitation('true concurrent multi-node shared-block write',
	kind => 'capability-forward', forward => '5.57 / 6.4a-DRM',
	note => 'capability limit only; does NOT excuse either write-tax gate (rule 8.B). '
		. 'Concurrent cross-node writes fail-closed (no corruption, t/327 L3).');

my $out_path = $ENV{PGRAC_ACCEPTANCE_JSON}
	// $report->default_path($ENV{TESTDATADIR} || "tmp_check");
eval { $report->emit_json($out_path); Test::More::note("MG-B gate record: $out_path"); };

done_testing();
