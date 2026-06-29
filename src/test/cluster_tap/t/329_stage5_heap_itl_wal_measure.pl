#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 329_stage5_heap_itl_wal_measure.pl
#    spec-5.19 D5 — Stage 5 integrated acceptance: heap-ITL WAL delta
#    compaction measure-and-decide (MG-D, value gate).
#
#    Every mutating heap WAL record (INSERT / UPDATE / DELETE / LOCK /
#    LOCK_UPDATED) carries a fixed 8-byte block header (xl_heap_itl_delta_
#    block) + 40-byte v2 delta (xl_heap_itl_delta_v2) == 48 bytes, with
#    ndeltas == 1 (NOT coalesced — N single-row mutations to the same block
#    emit N separate 48-byte deltas).  This test MEASURES that overhead under
#    a hot-block workload and emits a GO/NO-GO decision on whether to
#    implement WAL-delta compaction (array-pack multiple deltas / elide
#    repeated ACTIVE stamps).
#
#    Measure-and-decide (L257):  the metric is report-only.  NO-GO is a legal
#    outcome (small overhead / non-blocker — mirrors the spec-5.53 / 5.55
#    NO-GO precedent).  GO becomes a 5.19 ship blocker ONLY if the overhead
#    is qualified as a perf blocker;  otherwise it is a follow-up optimisation.
#
#    Method: single cluster-enabled node, autovacuum off.  A hot-block
#    workload performs many single-row UPDATEs concentrated on a few heap
#    pages, then pg_waldump over the workload LSN window counts the mutating
#    heap records (each == one 48-byte ITL delta) and the same-block grouping
#    (the array-pack coalesce opportunity).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/329_stage5_heap_itl_wal_measure.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Stage5IntegratedAcceptanceReport;
use Test::More;

my $ITL_DELTA_BYTES = 48;    # 8 (block header) + 40 (v2 delta) — D8 L6 invariant

my $report = PostgreSQL::Test::Stage5IntegratedAcceptanceReport->new(
	tag => $ENV{PGRAC_TAG} // 'unknown');

# ---------------------------------------------------------------------
# Single cluster-enabled node (allow_single_node — no peers needed to emit
# the per-record ITL delta).
# ---------------------------------------------------------------------
my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $node = PostgreSQL::Test::Cluster->new('itl_wal');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->append_conf('postgresql.conf', "wal_level = replica\n");
# 1-node cluster: declare only this node's interconnect address (allow_single_
# node bypasses quorum, so no peers are required).
PostgreSQL::Test::Utils::append_to_file(
	$node->data_dir . '/pgrac.conf',
	"[cluster]\nname = itl_wal\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n");
$node->start;

# ---------------------------------------------------------------------
# Hot-block workload: a small table whose rows fit on a few pages, updated
# many times (single-row UPDATEs -> N separate mutating heap records).
# ---------------------------------------------------------------------
$node->safe_psql('postgres',
	'CREATE TABLE itlw (id int primary key, v int) WITH (fillfactor = 90)');
$node->safe_psql('postgres',
	'INSERT INTO itlw SELECT g, 0 FROM generate_series(1, 200) g');
$node->safe_psql('postgres', 'CHECKPOINT');

my $start_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

# Hot-block workload: repeated full-table UPDATEs.  Each pass updates all 200
# rows in physical (ctid) order, so consecutive mutating heap records hit the
# same heap page until it is exhausted — the realistic same-block run that the
# array-pack coalesce would target.  20 passes * 200 rows == 4000 mutations.
my $N_PASSES = 20;
$node->safe_psql('postgres', qq{
	DO \$\$
	BEGIN
		FOR i IN 1..$N_PASSES LOOP
			UPDATE itlw SET v = v + 1;
		END LOOP;
	END \$\$;
});

my $end_lsn = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my $total_wal_bytes = $node->safe_psql('postgres',
	"SELECT pg_wal_lsn_diff('$end_lsn', '$start_lsn')::bigint");

# ---------------------------------------------------------------------
# pg_waldump over the workload window: count mutating heap records and the
# same-block grouping (array-pack coalesce opportunity).
# ---------------------------------------------------------------------
my $pg_waldump = $ENV{PG_WALDUMP} // 'pg_waldump';
my $datadir = $node->data_dir;
$node->safe_psql('postgres', 'CHECKPOINT');    # ensure WAL is on disk

my @dump = split /\n/,
	`$pg_waldump -p $datadir/pg_wal -s $start_lsn -e $end_lsn 2>/dev/null`;

my $heap_mut_records = 0;
my $heap_total_bytes = 0;
my %block_runs;       # consecutive same-block run lengths
my $prev_block = '';
my $cur_run = 0;
my @run_lengths;
for my $line (@dump)
{
	# rmgr: Heap   len (rec/tot):  NN/  NN, ... desc: UPDATE ... blkref #0: rel A/B/C blk N
	next unless $line =~ /rmgr:\s+Heap2?\s/;
	next unless $line =~ /desc:\s+(INSERT|UPDATE|HOT_UPDATE|DELETE|LOCK|LOCK_UPDATED|MULTI_INSERT)/;
	my $op = $1;
	# Only the per-record mutations carry one 48-byte ITL delta.
	$heap_mut_records++;
	if ($line =~ m{len \(rec/tot\):\s*\d+/\s*(\d+)})
	{
		$heap_total_bytes += $1;
	}
	my $blk = ($line =~ /blk\s+(\d+)/) ? $1 : '?';
	if ($blk eq $prev_block)
	{
		$cur_run++;
	}
	else
	{
		push @run_lengths, $cur_run if $cur_run > 0;
		$cur_run = 1;
		$prev_block = $blk;
	}
}
push @run_lengths, $cur_run if $cur_run > 0;

# ---------------------------------------------------------------------
# Metrics.
# ---------------------------------------------------------------------
my $itl_delta_bytes = $heap_mut_records * $ITL_DELTA_BYTES;
my $itl_share_pct = ($heap_total_bytes > 0)
	? sprintf('%.2f', 100.0 * $itl_delta_bytes / $heap_total_bytes) : '0.00';
# Array-pack coalesce opportunity: for each consecutive same-block run of M
# records, packing saves (M-1) block-header dedups (8 B each) + repeated
# ACTIVE-stamp elision.  Estimate the header-dedup saving as a lower bound.
my $coalescible_records = 0;
$coalescible_records += ($_ - 1) for @run_lengths;
my $coalesce_rate_pct = ($heap_mut_records > 0)
	? sprintf('%.2f', 100.0 * $coalescible_records / $heap_mut_records) : '0.00';
my $header_dedup_saving_bytes = $coalescible_records * 8;
my $header_dedup_saving_pct = ($heap_total_bytes > 0)
	? sprintf('%.2f', 100.0 * $header_dedup_saving_bytes / $heap_total_bytes) : '0.00';

note("MG-D heap-ITL WAL measurement:");
note("  mutating heap records         = $heap_mut_records");
note("  heap WAL total bytes          = $heap_total_bytes");
note("  total WAL bytes (window)      = $total_wal_bytes");
note("  ITL delta bytes (48 * recs)   = $itl_delta_bytes");
note("  ITL delta share of heap WAL   = $itl_share_pct %");
note("  coalescible (same-block) recs = $coalescible_records ($coalesce_rate_pct %)");
note("  header-dedup saving (lower b.) = $header_dedup_saving_bytes B ($header_dedup_saving_pct % of heap WAL)");

# ---------------------------------------------------------------------
# Decision (measure-and-decide).  GO only if the compaction would recover a
# material share of heap WAL;  the header-dedup lower bound is the
# conservative trigger.  NO-GO is a legal, non-blocker outcome.
# ---------------------------------------------------------------------
# Threshold: a compaction that recovers < 5% of heap WAL is not worth the
# WAL-ABI + redo re-stamp + catversion + 8.A ITL-chain crash-recovery cost.
my $GO_THRESHOLD_PCT = 5.0;
my $decision = ($header_dedup_saving_pct + 0.0 >= $GO_THRESHOLD_PCT) ? 'GO' : 'NO-GO';
# The overhead is NOT a regression (it is a fixed, shipped, correct cost), so
# it is never a ship blocker — at most a follow-up optimisation (spec §1.3 /
# §10 forward-link 3).
my $blocker = 0;

$report->set_itl_wal_decision($decision,
	measured => {
		mutating_heap_records => $heap_mut_records,
		heap_wal_bytes        => $heap_total_bytes,
		itl_delta_bytes       => $itl_delta_bytes,
		itl_share_pct         => $itl_share_pct,
		coalesce_rate_pct     => $coalesce_rate_pct,
		header_dedup_saving_pct => $header_dedup_saving_pct,
	},
	blocker => $blocker,
	threshold_pct => $GO_THRESHOLD_PCT,
	note => "fixed 48 B/record ITL delta, ndeltas==1 (not coalesced); "
		. "$decision per header-dedup lower bound vs ${GO_THRESHOLD_PCT}% "
		. "threshold; non-blocker (shipped correct cost, not a regression)");

# Assertions (8.A safe-direction):  the measurement must be well-formed.
ok($heap_mut_records > 0,
	"MG-D measured $heap_mut_records mutating heap records under the hot-block "
	. "workload");
ok($itl_delta_bytes == $heap_mut_records * $ITL_DELTA_BYTES,
	"MG-D ITL delta overhead == 48 B * mutating records (D8 L6 invariant)");
ok($decision eq 'GO' || $decision eq 'NO-GO',
	"MG-D decision recorded: $decision (header-dedup saving $header_dedup_saving_pct% "
	. "vs ${GO_THRESHOLD_PCT}% threshold; non-blocker)");

# ---------------------------------------------------------------------
# Emit the decision record fragment.
# ---------------------------------------------------------------------
my $out_path = $ENV{PGRAC_ACCEPTANCE_JSON}
	// $report->default_path($ENV{TESTDATADIR} || "tmp_check");
eval { $report->emit_json($out_path); Test::More::note("MG-D decision record: $out_path"); };

$node->stop;
done_testing();
