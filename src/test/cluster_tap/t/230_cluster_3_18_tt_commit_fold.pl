#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 230_cluster_3_18_tt_commit_fold.pl
#	  spec-3.18 D4.1 -- fold the durable TT commit delta into the commit
#	  record (normal commit) while 2PC keeps the standalone 0x30.
#
#	  Validates the four review points of the D4.1 behavioral unit:
#
#	  L1  (normal commit folds + NO double-write):  a normal-commit DML window
#	      emits ZERO standalone ClusterUndo UNDO_TT_SLOT_COMMIT (0x30) records,
#	      and its Transaction COMMIT record carries the folded "tt_commit:"
#	      delta.  This is the "no double-write" guard (point 4) and proves the
#	      normal path folds (point 1a).
#	  L2  (2PC keeps 0x30):  a PREPARE + COMMIT PREPARED window emits >= 1
#	      standalone 0x30 and folds NO tt_commit into its Transaction records.
#	      Together L1+L2 are discriminating: a build that ALWAYS emits 0x30
#	      fails L1, one that NEVER emits 0x30 fails L2 -- the split (point 1b)
#	      is observable, not assumed.
#	  L3  (crash-restart redo from the commit record):  after an immediate
#	      (crash) stop, recovery replays the folded delta out of the commit
#	      record (cluster_tt_durable_redo_stamp_slot via xact_redo_commit),
#	      stamping the durable TT slot with NO PANIC and intact data
#	      (points 2 + 3: redo order + crash recovery).  The window holds only
#	      normal commits, so any redo-apply must come from the fold path (L1
#	      already proved normal commits emit no 0x30).
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.18-write-path-perf.md (D4.1)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('s318_ttfold');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->append_conf('postgresql.conf', "max_prepared_transactions = 10\n");
$node->start;

# counter($key) -> integer value of an undo-category durable counter.
my $counter = sub {
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() WHERE key = '$key'");
};

# waldump($rmgr, $start, $end) -> pg_waldump text for one rmgr over [start,end].
my $waldump = sub {
	my ($rmgr, $start, $end) = @_;
	my ($out, $err) = run_command(
		[ $node->installed_command('pg_waldump'),
		  '-p', $node->data_dir . '/pg_wal',
		  '-r', $rmgr, '-s', $start, '-e', $end ]);
	return $out;
};

# ============================================================
# L1: normal commit folds tt_commit + emits NO standalone 0x30 (point 4 + 1a)
# ============================================================
$node->safe_psql('postgres', 'CREATE TABLE l1(id int primary key, v int)');
my $l1_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', 'INSERT INTO l1 VALUES (1,0),(2,0)');
$node->safe_psql('postgres', 'UPDATE l1 SET v = 1 WHERE id = 1');
my $l1_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

my $l1_undo = $waldump->('ClusterUndo', $l1_start, $l1_end);
my $l1_xact = $waldump->('Transaction', $l1_start, $l1_end);
my $l1_0x30  = () = $l1_undo =~ /UNDO_TT_SLOT_COMMIT/g;
my $l1_folds = () = $l1_xact =~ /tt_commit:/g;

is($l1_0x30, 0,
	'L1: normal commit emits NO standalone UNDO_TT_SLOT_COMMIT (0x30) -- no double-write');
cmp_ok($l1_folds, '>=', 1,
	"L1: commit record carries the folded tt_commit delta (n=$l1_folds)");
cmp_ok($counter->('tt_durable_commit_count'), '>', 0,
	'L1: durable TT slot still stamped (write-only path bumps commit count)');

# ============================================================
# L2: 2PC COMMIT PREPARED keeps the standalone 0x30 + folds nothing (point 1b)
# ============================================================
# Pre-create the table on a normal commit OUTSIDE the measured window so the
# 2PC window holds only the prepared INSERT + its COMMIT PREPARED.
$node->safe_psql('postgres', 'CREATE TABLE l2(id int primary key, v int)');
my $l2_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', <<'SQL');
BEGIN;
INSERT INTO l2 VALUES (10, 10), (20, 20);
PREPARE TRANSACTION 'd41_g1';
COMMIT PREPARED 'd41_g1';
SQL
my $l2_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

my $l2_undo = $waldump->('ClusterUndo', $l2_start, $l2_end);
my $l2_xact = $waldump->('Transaction', $l2_start, $l2_end);
my $l2_0x30  = () = $l2_undo =~ /UNDO_TT_SLOT_COMMIT/g;
my $l2_folds = () = $l2_xact =~ /tt_commit:/g;

cmp_ok($l2_0x30, '>=', 1,
	"L2: 2PC COMMIT PREPARED still emits standalone 0x30 (n=$l2_0x30)");
is($l2_folds, 0,
	'L2: 2PC records fold NO tt_commit into the commit record (split is clean)');

# ============================================================
# L3: crash-restart replays the folded delta from the commit record (point 2+3)
# ============================================================
$node->safe_psql('postgres', 'CREATE TABLE l3(id int primary key, v int)');
$node->safe_psql('postgres', 'INSERT INTO l3 VALUES (1,0),(2,0)');
# Checkpoint so the commits below sit AFTER the redo point and MUST replay.
$node->safe_psql('postgres', 'CHECKPOINT');
my $l3_redo_before = $counter->('tt_durable_redo_apply_count');
$node->safe_psql('postgres', 'UPDATE l3 SET v = 1 WHERE id = 1');
$node->safe_psql('postgres', 'UPDATE l3 SET v = 2 WHERE id = 2');
$node->safe_psql('postgres', 'INSERT INTO l3 VALUES (3,3),(4,4)');

$node->stop('immediate');    # crash: folded TT stamp is WAL-only (not fsync'd)
$node->start;                # recovery: xact_redo_commit stamps the slot

is($node->safe_psql('postgres',
		"SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM l3"),
	'1:1,2:2,3:3,4:4', 'L3: data intact after crash recovery (folded redo)');
cmp_ok($counter->('tt_durable_redo_apply_count'), '>', $l3_redo_before,
	'L3: folded TT delta replayed from commit record during crash recovery');
unlike(slurp_file($node->logfile), qr/PANIC/,
	'L3: no redo PANIC stamping the TT slot from the commit record');

$node->stop;
done_testing();
