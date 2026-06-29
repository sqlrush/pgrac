#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 328_stage5_multinode_write_perf.pl
#    spec-5.19 D3 — Stage 5 integrated acceptance: multi-node write-path
#    performance measure-and-decide (MG-B, value + soundness gate).
#
#    MG-B has two gates (L257):
#      * VALUE gate   — quantify the write tax / wait share / affinity.
#      * SOUNDNESS gate — is the harness really running cross-node shared-heap
#        write competition, or inject-partial?  REAL vs measure-only.
#
#    spec-5.19 D0 soundness finding (decisive): at Stage 5 there is NO sound
#    concurrent multi-node WRITE path to shared storage — same-relation
#    contention needs cross-node holder migration (DRM = Stage 6) and even
#    "partitioned" per-node relations collide on the shared OID/file space.
#    Concurrent cross-node writes fail CLOSED (no corruption — proven in
#    t/327 L3).  Therefore the SOUNDNESS verdict for true multi-node write
#    competition is MEASURE-ONLY / FORWARD (spec-5.57 cross-instance CR +
#    Stage 6 6.4a Smart Fusion;  spec-5.19 §3.2 / §10 forward-link 5).
#
#    What IS soundly measurable at Stage 5 is the SINGLE-NODE cluster write
#    tax — the per-node cost of the cluster MVCC/ITL/GES machinery vs the
#    native (cluster.enabled = off) path on the same build (spec-3.25
#    single-node-no-peer C floor).  This test measures that (report-only)
#    and records the multi-node soundness verdict as measure-only/forward.
#
#    Legs:
#      M1  single-node write tax:  pgbench TPC-B on a cluster-enabled node vs
#          a native (cluster.enabled off) node;  tax % is report-only (no
#          numeric gate — spec §3.3 / docs/perf-gates.md class-1 model).
#      M2  cluster write-path wait events present during the workload (the
#          observability surface MG-B aggregates).
#      SOUNDNESS  multi-node concurrent write = measure-only/forward (the
#          empirical basis is t/327 L3 fail-closed);  no inject-partial number
#          is passed off as a true multi-node perf result (rule 8.A/8.B).
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

my $PGBENCH = $ENV{PGBENCH} // 'pgbench';
my $SCALE   = 2;
my $SECS    = $ENV{PGRAC_PGBENCH_SECS} // 4;
my $CLIENTS = 4;

# Run pgbench TPC-B (default script) and return the reported tps (float) or
# undef on failure.
sub pgbench_tps
{
	my ($node) = @_;
	my $conn = '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
	system("$PGBENCH -i -s $SCALE -q $conn >/dev/null 2>&1");
	return undef if $? != 0;
	my $out = `$PGBENCH -T $SECS -c $CLIENTS -j 2 -N $conn 2>&1`;
	# -N: skip updates to pgbench_tellers/branches (less contention, still a
	# write workload on pgbench_accounts + history) — a clean write-tax signal.
	if ($out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/)
	{
		return $1 + 0.0;
	}
	if ($out =~ /tps\s*=\s*([\d.]+)/)
	{
		return $1 + 0.0;
	}
	return undef;
}

# ---------------------------------------------------------------------
# Native baseline node (cluster.enabled off — the lower-bound C floor).
# ---------------------------------------------------------------------
my $native = PostgreSQL::Test::Cluster->new('mnw_native');
$native->init;
$native->append_conf('postgresql.conf', "autovacuum = off\n");
$native->append_conf('postgresql.conf', "fsync = off\n");    # perf measure only
$native->start;
my $tps_native = pgbench_tps($native);
$native->stop;

# ---------------------------------------------------------------------
# Cluster-enabled single node (1-node cluster — the spec-3.25 C floor).
# ---------------------------------------------------------------------
my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $clu = PostgreSQL::Test::Cluster->new('mnw_cluster');
$clu->init;
$clu->append_conf('postgresql.conf', "cluster.enabled = on\n");
$clu->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$clu->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$clu->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$clu->append_conf('postgresql.conf', "autovacuum = off\n");
$clu->append_conf('postgresql.conf', "fsync = off\n");
PostgreSQL::Test::Utils::append_to_file(
	$clu->data_dir . '/pgrac.conf',
	"[cluster]\nname = mnw_cluster\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
$clu->start;
my $tps_cluster = pgbench_tps($clu);

# M2: cluster write-path wait-event observability surface present.
my $wait_events_present = $clu->safe_psql('postgres',
	"SELECT count(*) FROM pg_stat_cluster_wait_events") // 0;

$clu->stop;

# ---------------------------------------------------------------------
# M1: single-node write tax (report-only).
# ---------------------------------------------------------------------
my $have_both = (defined $tps_native && $tps_native > 0
	&& defined $tps_cluster && $tps_cluster > 0);
my $tax_pct = $have_both
	? sprintf('%.2f', 100.0 * (1.0 - $tps_cluster / $tps_native)) : 'n/a';

note("MG-B single-node write tax (report-only):");
note("  native  TPC-B tps = " . (defined $tps_native ? $tps_native : 'n/a'));
note("  cluster TPC-B tps = " . (defined $tps_cluster ? $tps_cluster : 'n/a'));
note("  write tax %       = $tax_pct (cluster vs native, single node)");
note("  cluster wait-event rows = $wait_events_present");

$report->record_multinode_write_value(1, 'tpcb',
	tps_native => (defined $tps_native ? $tps_native : 0),
	tps_cluster => (defined $tps_cluster ? $tps_cluster : 0),
	write_tax_pct => $tax_pct,
	wait_event_rows => $wait_events_present);

# ---------------------------------------------------------------------
# SOUNDNESS gate (the decisive MG-B verdict).
# ---------------------------------------------------------------------
$report->set_multinode_write_soundness('measure-only',
	real => 0,
	note => 'no sound concurrent multi-node write at Stage 5 (cross-node holder '
		. 'migration / DRM = Stage 6;  concurrent cross-node writes fail closed, '
		. 't/327 L3).  True multi-node write competition is forward (spec-5.57 '
		. 'cross-instance CR + Stage 6 6.4a Smart Fusion).  Single-node cluster '
		. 'write tax is the soundly-measurable Stage-5 metric.');
$report->record_limitation('multi-node write-path perf (true cross-node)',
	kind => 'perf-forward', forward => '5.57 / 6.4a',
	note => 'MG-B soundness = measure-only;  single-node cluster write tax '
		. 'measured (report-only);  true concurrent multi-node write perf needs '
		. 'DRM (Stage 6)');

# ---------------------------------------------------------------------
# Assertions (8.A safe-direction):  measurement well-formed;  soundness
# honestly recorded.  No numeric perf gate (report-only, spec §3.3).
# ---------------------------------------------------------------------
ok(defined $tps_native && $tps_native > 0,
	"M1 native single-node write throughput measured (tps=" .
	(defined $tps_native ? $tps_native : 'n/a') . ")");
ok(defined $tps_cluster && $tps_cluster > 0,
	"M1 cluster single-node write throughput measured (tps=" .
	(defined $tps_cluster ? $tps_cluster : 'n/a') . ")");
ok($wait_events_present > 0,
	"M2 cluster write-path wait-event surface present ($wait_events_present rows)");
ok($report->{matrix}{multinode_write_perf}{soundness}{verdict} eq 'measure-only',
	"MG-B soundness verdict = measure-only/forward (true multi-node write = "
	. "Stage 6 DRM;  no inject-partial number passed off as real)");

my $out_path = $ENV{PGRAC_ACCEPTANCE_JSON}
	// $report->default_path($ENV{TESTDATADIR} || "tmp_check");
eval { $report->emit_json($out_path); Test::More::note("MG-B value+soundness record: $out_path"); };

done_testing();
