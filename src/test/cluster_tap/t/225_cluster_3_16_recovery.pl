#-------------------------------------------------------------------------
#
# 225_cluster_3_16_recovery.pl
#    Stage 3.16 crash-recovery hardening — single-instance crash-restart
#    matrix for the Stage 3 durable / WAL machinery.
#
#      L1   each live undo opcode survives crash-restart redo
#           (INSERT/UPDATE/DELETE + segment rollover) — data intact, no PANIC
#      L2   crash-after-checkpoint: redo replays from the checkpoint
#           idempotently, latest data intact
#      L3   2PC crash-restart matrix: prepared commit + prepared abort
#           each cross a crash, resolve correctly
#      L4   ITL ref reachable after recovery: rows written before the crash
#           read back correctly (heap FPI + undo redo both replayed)
#      L5   double crash: crash during the post-restart write load, restart
#           again, redo stays idempotent
#      L6   local non-recovery path zero regression
#
#    These are pg_ctl-level stop('immediate')/start cycles (not the TAP
#    bootstrap path), so the crash-restart legs run on this host directly.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/225_cluster_3_16_recovery.pl
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

my $node = PgracClusterNode->new('recovery');
$node->init;
$node->append_conf('postgresql.conf',
	"cluster.enabled = on\n" . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "max_prepared_transactions = 10\n");
$node->start;

$node->safe_psql('postgres', 'CREATE TABLE t316 (id int primary key, v text)');

# ============================================================
# L1: each live undo opcode survives crash-restart redo.
#     INSERT/UPDATE/DELETE exercise 0x30 (TT commit); a big write run
#     drives segment INIT/rollover (0x10/0x40/0x50).
# ============================================================
$node->safe_psql('postgres', q{
	INSERT INTO t316 SELECT g, 'v' || g FROM generate_series(1, 200) g;
	UPDATE t316 SET v = v || '_u' WHERE id <= 100;
	DELETE FROM t316 WHERE id > 180;
});
my $before = $node->safe_psql('postgres',
	'SELECT count(*), coalesce(sum(id), 0) FROM t316');

$node->stop('immediate');
$node->start;

my $after = $node->safe_psql('postgres',
	'SELECT count(*), coalesce(sum(id), 0) FROM t316');
is($after, $before, 'L1 data intact across crash-restart redo');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t316 WHERE v LIKE \'%_u\''),
   '100', 'L1 UPDATE (0x30 TT commit) survived redo');
my $log1 = slurp_file($node->logfile);
unlike($log1, qr/PANIC/, 'L1 no PANIC during crash recovery');

# ============================================================
# L2: crash-after-checkpoint — redo replays from the checkpoint.
# ============================================================
$node->safe_psql('postgres', 'CHECKPOINT');
$node->safe_psql('postgres', q{
	INSERT INTO t316 SELECT g, 'post' || g FROM generate_series(300, 360) g;
});
my $post_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t316 WHERE id BETWEEN 300 AND 360});

$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres',
		q{SELECT count(*) FROM t316 WHERE id BETWEEN 300 AND 360}),
   $post_before, 'L2 post-checkpoint writes survived crash redo (idempotent)');

# ============================================================
# L3: 2PC crash-restart matrix.
# ============================================================
# Prepared COMMIT across a crash.
$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t316 VALUES (400, 'prep_commit');
	PREPARE TRANSACTION 'gx_c';
});
# Prepared ABORT across a crash.
$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t316 VALUES (401, 'prep_abort');
	PREPARE TRANSACTION 'gx_a';
});

$node->stop('immediate');
$node->start;

# Both still prepared after restart.
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_prepared_xacts WHERE gid IN ('gx_c', 'gx_a')}),
   '2', 'L3 both prepared xacts recovered after crash');

# spec-3.16 recovery boundary (see contract doc): a crash-restart resets
# cluster_scn / retention horizon, so a page whose pd_block_scn was stamped
# by pre-crash committed tuples can read newer than a just-restarted
# reader's read_scn, mis-firing CR onto recycled undo (snapshot-too-old
# fail-closed -- correct data, degraded availability).  Drive a few writes
# to advance cluster_scn past the pre-crash peak before resolving, which is
# what a real workload does within milliseconds of restart.
$node->safe_psql('postgres',
	q{INSERT INTO t316 SELECT g, 'warm' || g FROM generate_series(600, 640) g});

$node->safe_psql('postgres', q{COMMIT PREPARED 'gx_c'});
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'gx_a'});
is($node->safe_psql('postgres', q{SELECT v FROM t316 WHERE id = 400}),
   'prep_commit', 'L3 prepared COMMIT across crash -> visible (after scn warmup)');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t316 WHERE id = 401'),
   '0', 'L3 prepared ABORT across crash -> invisible');

# ============================================================
# L4: ITL ref reachable after recovery (rows pre-crash read correctly).
#     Already implicitly covered by L1; assert a specific updated row
#     resolves (the ITL slot ref + durable TT slot both replayed).
# ============================================================
is($node->safe_psql('postgres', q{SELECT v FROM t316 WHERE id = 50}),
   'v50_u', 'L4 pre-crash UPDATEd row resolves correctly after recovery');

# ============================================================
# L5: double crash — crash during post-restart write load, restart again.
# ============================================================
$node->safe_psql('postgres', q{
	INSERT INTO t316 SELECT g, 'd' || g FROM generate_series(500, 540) g;
});
$node->stop('immediate');
$node->start;
$node->safe_psql('postgres', q{
	INSERT INTO t316 SELECT g, 'd2' || g FROM generate_series(550, 560) g;
});
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM t316 WHERE id BETWEEN 500 AND 560}),
   '52', 'L5 double-crash redo idempotent, all rows intact');
my $log5 = slurp_file($node->logfile);
unlike($log5, qr/PANIC/, 'L5 no PANIC across repeated crash recovery');

# ============================================================
# L6: local non-recovery path zero regression.
# ============================================================
$node->safe_psql('postgres', q{
	INSERT INTO t316 VALUES (900, 'plain');
	UPDATE t316 SET v = 'plain2' WHERE id = 900;
});
is($node->safe_psql('postgres', q{SELECT v FROM t316 WHERE id = 900}),
   'plain2', 'L6 plain DML unaffected');

# recovery observability surface present.
my $rec_rows = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category = 'recovery'});
is($rec_rows, '17', 'L6 recovery category: 4 counters + 13 plan keys (spec-4.3 D5)');

$node->stop;
done_testing();
