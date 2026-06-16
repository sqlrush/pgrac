#-------------------------------------------------------------------------
#
# 270_cluster_4_8ab_writeback_boundary_guards.pl
#    spec-4.8ab D1 — checkpoint-writeback boundary guards fail-closed (8.A).
#
#    The undo buffer pool's write-back path makes two durability promises that
#    spec-4.8ab raises from implicit to explicit + guarded:
#      1. WAL-before-data: an undo block is never written back before its
#         XLOG_UNDO_BLOCK_WRITE is durable (block_lsn <= GetFlushRecPtr()).
#      2. checkpoint-coverage: a checkpoint never completes leaving a PRE-redo
#         dirty undo block unflushed (recovery starts at redo and would not
#         replay it -> lost).
#    A detected violation fail-closes with SQLSTATE 53R9N
#    (ERRCODE_CLUSTER_UNDO_WRITEBACK_BOUNDARY_VIOLATION), never silent.
#
#      L3c  CONTROL: a normal CHECKPOINT over buffered-dirty undo succeeds with
#           NO 53R9N and NO PANIC -- the guard does not false-fire on a legal
#           (post-flush / post-redo) dirty block.  This pins the v0.3-amend
#           discipline at the integration level (the guard is NOT block_lsn>redo).
#      L3a  WAL-before-data: arm undo-force-wal-before-data-violation, CHECKPOINT
#           -> the flush_dirty_slot guard fires 53R9N ("write-back flush").
#      L3b  checkpoint-coverage: arm undo-skip-checkpoint-flush-one, CHECKPOINT
#           -> one dirty block is left unflushed and the post-flush re-scan fires
#           53R9N ("checkpoint coverage").
#
# IDENTIFICATION
#    src/test/cluster_tap/t/270_cluster_4_8ab_writeback_boundary_guards.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PgracClusterNode->new('wb_boundary');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.undo_buffers = 64\n"
	  . "cluster.undo_buffer_writeback = on\n"   # write-back active (single-node)
	  . "checkpoint_timeout = 1h\n"              # no auto-checkpoint races
	  . "max_wal_size = 4GB\n"
	  . "log_error_verbosity = verbose\n"        # SQLSTATE 53R9N appears in the log
	  . "autovacuum = off\n");
$node->start;

is($node->safe_psql('postgres', 'SHOW cluster.undo_buffer_writeback'),
	'on', 'write-back GUC enabled (single-node)');

$node->safe_psql('postgres',
	q{CREATE TABLE t_wb (id int primary key, v text);
	  INSERT INTO t_wb SELECT g, 'base' || g FROM generate_series(1, 200) g});

# dirty_undo() — a length-preserving UPDATE writes undo, leaving the undo data
# block buffered-dirty in the pool (write-back defers the pwrite to checkpoint).
my $bump = 0;
sub dirty_undo
{
	$bump++;
	$node->safe_psql('postgres',
		"UPDATE t_wb SET v = 'r${bump}' || id WHERE id <= 50");
}

# ============================================================
# L3c CONTROL — a normal CHECKPOINT over buffered-dirty undo must NOT fire the
# boundary guard (no false-positive on a legal dirty block).
# ============================================================
dirty_undo();
my $log_off = -s $node->logfile;
$node->safe_psql('postgres', 'CHECKPOINT');
my $log = slurp_file($node->logfile, $log_off);
unlike($log, qr/53R9N|checkpoint-writeback boundary violation/,
	'L3c control: normal checkpoint over dirty undo does NOT fire 53R9N');
unlike($log, qr/PANIC/, 'L3c control: no PANIC on a legal dirty block');

# ============================================================
# L3a WAL-before-data — arm the force-violation point; the flush_dirty_slot
# guard must fail-close 53R9N ("write-back flush").
# ============================================================
$node->append_conf('postgresql.conf',
	"cluster.injection_points = 'undo-force-wal-before-data-violation:skip'\n");
$node->restart;
dirty_undo();
$log_off = -s $node->logfile;
# The checkpointer hits the guard mid-CheckPointGuts -> the CHECKPOINT request
# fails;  the 53R9N is logged by the checkpointer.  Tolerate either client rc.
$node->psql('postgres', 'CHECKPOINT');
ok( $node->wait_for_log(
		qr/checkpoint-writeback boundary violation: write-back flush/, $log_off),
	'L3a WAL-before-data guard fires (write-back flush) under injection');
like(slurp_file($node->logfile, $log_off), qr/53R9N/,
	'L3a violation is SQLSTATE 53R9N');

# ============================================================
# L3b checkpoint-coverage — arm the skip-one point; the post-flush re-scan must
# fail-close 53R9N ("checkpoint coverage") on the escaped PRE-redo dirty block.
# ============================================================
$node->append_conf('postgresql.conf',
	"cluster.injection_points = 'undo-skip-checkpoint-flush-one:skip'\n");
$node->restart;
dirty_undo();
$log_off = -s $node->logfile;
$node->psql('postgres', 'CHECKPOINT');
ok( $node->wait_for_log(
		qr/checkpoint-writeback boundary violation: checkpoint coverage/, $log_off),
	'L3b checkpoint-coverage guard fires under injection');
like(slurp_file($node->logfile, $log_off), qr/53R9N/,
	'L3b violation is SQLSTATE 53R9N');

$node->stop;
done_testing();
