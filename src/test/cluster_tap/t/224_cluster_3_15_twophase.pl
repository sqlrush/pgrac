#-------------------------------------------------------------------------
#
# 224_cluster_3_15_twophase.pl
#    Stage 3.15 two-phase commit of cluster TT state — e2e acceptance.
#
#      L1   PREPARE with cluster undo/TT state succeeds (guards removed)
#           and the prepared rows are invisible (in-progress semantics)
#      L2   COMMIT PREPARED from a DIFFERENT backend makes rows visible
#           (2PC record drives the resolve; backend-local bindings gone)
#      L3   ROLLBACK PREPARED keeps rows invisible (durable 0x31) and
#           counters reflect the abort prefinish
#      L4   crash (immediate) between PREPARE and COMMIT PREPARED ->
#           restart -> recover re-pins the slot (protected map) -> new
#           write load cannot steal it -> COMMIT PREPARED -> visible
#      L5   prepared transaction with subtransactions (SAVEPOINT chain)
#           -> COMMIT PREPARED -> parent + child writes all visible
#      L6   repeated prepared resolves recycle TT allocator state
#      L7   prepare undo flush counter moves (C-P5 durability evidence;
#           L4's crash leg is the behavioural proof)
#      L8   local non-2PC path zero regression
#
#    ("guards removed" is proven by L1/L5 themselves: those PREPAREs
#    would have been rejected by the spec-3.5/3.7 guards.)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/224_cluster_3_15_twophase.pl
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

my $node = PgracClusterNode->new('twophase');
$node->init;
$node->append_conf('postgresql.conf',
	"cluster.enabled = on\n" . "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "max_prepared_transactions = 10\n");
$node->start;

$node->safe_psql('postgres', 'CREATE TABLE t315 (id int primary key, v text)');
$node->safe_psql('postgres',
	'CREATE TABLE t315_reuse (id int, v text); ALTER TABLE t315_reuse ALTER COLUMN v SET STORAGE PLAIN');

sub tt2pc_counter {
	my ($key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		    WHERE category = 'tt_2pc' AND key = '$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# ============================================================
# L1: PREPARE with cluster state succeeds + rows invisible.
# ============================================================
my $prep_before = tt2pc_counter('twopc_prepare_records');

$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t315 VALUES (1, 'one');
	PREPARE TRANSACTION 'gx1';
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM t315 WHERE id = 1'),
   '0', 'L1 prepared row invisible (in-progress)');
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'gx1'}),
   '1', 'L1 gx1 listed in pg_prepared_xacts');
cmp_ok(tt2pc_counter('twopc_prepare_records'), '>', $prep_before,
	   'L1 twopc_prepare_records bumped');

# ============================================================
# L2: COMMIT PREPARED from a different backend -> visible.
#     (each safe_psql is its own backend, so this is cross-backend.)
# ============================================================
my $cm_before = tt2pc_counter('twopc_prefinish_commits');
$node->safe_psql('postgres', q{COMMIT PREPARED 'gx1'});
is($node->safe_psql('postgres', q{SELECT v FROM t315 WHERE id = 1}),
   'one', 'L2 row visible after cross-backend COMMIT PREPARED');
cmp_ok(tt2pc_counter('twopc_prefinish_commits'), '>', $cm_before,
	   'L2 twopc_prefinish_commits bumped');

# ============================================================
# L3: ROLLBACK PREPARED -> invisible + abort prefinish counted.
# ============================================================
my $ab_before = tt2pc_counter('twopc_prefinish_aborts');
$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t315 VALUES (2, 'two');
	PREPARE TRANSACTION 'gx2';
});
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'gx2'});
is($node->safe_psql('postgres', 'SELECT count(*) FROM t315 WHERE id = 2'),
   '0', 'L3 rolled-back prepared row invisible');
cmp_ok(tt2pc_counter('twopc_prefinish_aborts'), '>', $ab_before,
	   'L3 twopc_prefinish_aborts bumped');

# ============================================================
# L4: crash between PREPARE and COMMIT PREPARED.
#     recover must re-pin the prepared slot (protected map) so the
#     post-restart write load cannot steal it; then COMMIT PREPARED
#     must still resolve to visible.
# ============================================================
$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t315 VALUES (3, 'three');
	PREPARE TRANSACTION 'gx3';
});
$node->stop('immediate');
$node->start;

cmp_ok(tt2pc_counter('twopc_recover_rebinds'), '>', 0,
	   'L4 recover callback re-pinned the prepared record');

# Post-restart write load: enough single-statement xacts to churn the
# fresh TT slot allocator; none may steal gx3's protected slot.
for my $i (100 .. 140) {
	$node->safe_psql('postgres', "INSERT INTO t315 VALUES ($i, 'churn')");
}
is($node->safe_psql('postgres', 'SELECT count(*) FROM t315 WHERE id = 3'),
   '0', 'L4 prepared row still invisible after crash + churn');

$node->safe_psql('postgres', q{COMMIT PREPARED 'gx3'});
is($node->safe_psql('postgres', q{SELECT v FROM t315 WHERE id = 3}),
   'three', 'L4 COMMIT PREPARED after crash-restart resolves to visible');

my $log = slurp_file($node->logfile);
unlike($log, qr/PANIC/, 'L4 no PANIC across crash recovery + resolve');

# ============================================================
# L5: prepared xact with subtransactions (SAVEPOINT chain).
# ============================================================
$node->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO t315 VALUES (4, 'parent');
	SAVEPOINT s1;
	INSERT INTO t315 VALUES (5, 'child');
	RELEASE SAVEPOINT s1;
	PREPARE TRANSACTION 'gx4';
});
is($node->safe_psql('postgres',
		'SELECT count(*) FROM t315 WHERE id IN (4, 5)'),
   '0', 'L5 prepared parent+child rows invisible');
$node->safe_psql('postgres', q{COMMIT PREPARED 'gx4'});
is($node->safe_psql('postgres',
		q{SELECT string_agg(v, ',' ORDER BY id) FROM t315 WHERE id IN (4, 5)}),
   'parent,child', 'L5 parent + subxact writes visible after COMMIT PREPARED');

# ============================================================
# L6: repeated prepared resolves recycle shmem TT allocator state.
#     Without the spec-3.15 prefinish mark_committed/mark_aborted handoff,
#     each resolved prepared xact leaves its slot ACTIVE and the 8-slot
#     allocator exhausts after a handful of sequential 2PC transactions.
# ============================================================
for my $i (20 .. 31) {
	my $gid = "gx_reuse_$i";
	$node->safe_psql('postgres', qq{
		BEGIN;
		INSERT INTO t315_reuse VALUES ($i, repeat('reuse', 1000));
		PREPARE TRANSACTION '$gid';
	});
	if ($i % 2 == 0) {
		$node->safe_psql('postgres', qq{COMMIT PREPARED '$gid'});
	} else {
		$node->safe_psql('postgres', qq{ROLLBACK PREPARED '$gid'});
	}
}
is($node->safe_psql('postgres',
	'SELECT count(*) FROM t315_reuse WHERE id BETWEEN 20 AND 31'),
   '6', 'L6 repeated prepared commit/abort does not exhaust TT slots');

# ============================================================
# L7: prepare undo flush counter moved (C-P5).
# ============================================================
cmp_ok(tt2pc_counter('twopc_prepare_undo_flushes'), '>', 0,
	   'L7 prepare-path undo flush ran (C-P5)');

# ============================================================
# L8: local non-2PC path zero regression.
# ============================================================
$node->safe_psql('postgres', q{
	INSERT INTO t315 VALUES (200, 'plain');
	UPDATE t315 SET v = 'plain2' WHERE id = 200;
});
is($node->safe_psql('postgres', q{SELECT v FROM t315 WHERE id = 200}),
   'plain2', 'L8 plain DML unaffected');

$node->stop;
done_testing();
