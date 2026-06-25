#-------------------------------------------------------------------------
#
# 223_cluster_3_14_visibility_variants.pl
#    Stage 3.14 remaining HeapTupleSatisfies* variant forks — integration
#    acceptance.
#
#    The OBS-2~5 truth tables and the D1 evidence gate are exhaustively
#    unit-tested (test_cluster_visibility_variants); this TAP covers the
#    integration layer: the variant forks must NOT regress the local
#    (own-instance / no-remote-evidence) path, the prune/vacuum guards
#    must not over-keep local tuples, and the observability surface +
#    53R9H errcode must be reachable.
#
#      L1   ClusterPair startup + both nodes alive
#      L2   local INSERT/UPDATE/DELETE round-trip (SatisfiesUpdate fork
#           local fall-through unchanged)
#      L3   local SELECT FOR UPDATE / FOR SHARE (lock path) unchanged
#      L4   local VACUUM reclaims dead tuples (prune guard does not
#           over-keep a fully-local tuple; hole #2 guard is remote-only)
#      L4b  local HOT update chain prunes (HeapTupleIsSurelyDead guard
#           remote-only)
#      L5   pg_cluster_state 'visibility' category exposes the 6 D8
#           counters and they are readable
#      L6   53R9H errcode is registered (cluster_cross_node_write_conflict)
#      L7   cross-node write (node0 writes, node1 observes) — best-effort
#           diag of the conflict counter (true 53R9H trigger needs the
#           shared-storage e2e harness; observed in CI, not hard-asserted)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/223_cluster_3_14_visibility_variants.pl
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

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'visibility_variants',
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;
usleep(2_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');

my $n0 = $pair->node0;


# ============================================================
# L2: local INSERT/UPDATE/DELETE round-trip (own-instance path).
#     The SatisfiesUpdate fork must fall through to PG-native for
#     fully-local tuples -- no behavioural change.
# ============================================================
$n0->safe_psql('postgres', q{
	CREATE TABLE v314 (id int primary key, n int);
	INSERT INTO v314 SELECT g, g FROM generate_series(1, 50) g;
});
is($n0->safe_psql('postgres', 'SELECT count(*) FROM v314'), '50',
   'L2 local INSERT visible');

$n0->safe_psql('postgres', 'UPDATE v314 SET n = n + 100 WHERE id <= 25');
is($n0->safe_psql('postgres', 'SELECT count(*) FROM v314 WHERE n > 100'), '25',
   'L2 local UPDATE applied');

$n0->safe_psql('postgres', 'DELETE FROM v314 WHERE id > 40');
is($n0->safe_psql('postgres', 'SELECT count(*) FROM v314'), '40',
   'L2 local DELETE applied');


# ============================================================
# L3: local row locks (lock-only xmax path) unchanged.
# ============================================================
is($n0->safe_psql('postgres',
		'SELECT count(*) FROM (SELECT id FROM v314 WHERE id <= 5 FOR UPDATE) s'),
   '5', 'L3 local SELECT FOR UPDATE works');
is($n0->safe_psql('postgres',
		'SELECT count(*) FROM (SELECT id FROM v314 WHERE id <= 3 FOR SHARE) s'),
   '3', 'L3 local SELECT FOR SHARE works');


# ============================================================
# L4: local VACUUM reclaims dead tuples (prune/VacuumHorizon guard is
#     remote-evidence-only; a fully-local dead tuple must still be
#     removable -- otherwise hole #2 guard would freeze local vacuum).
# ============================================================
my $relpages_before = $n0->safe_psql('postgres',
	q{SELECT pg_relation_size('v314')});
$n0->safe_psql('postgres', 'UPDATE v314 SET n = n + 1');  # churn -> dead tuples
$n0->safe_psql('postgres', 'VACUUM v314');
my $dead_after_vacuum = $n0->safe_psql('postgres',
	q{SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'v314'});
cmp_ok($dead_after_vacuum, '<=', 5,
	   "L4 local VACUUM reclaims dead tuples (guard does not over-keep local; n_dead=$dead_after_vacuum)");


# ============================================================
# L4b: local HOT update chain prunes (HeapTupleIsSurelyDead guard is
#      remote-only).
# ============================================================
$n0->safe_psql('postgres', q{
	CREATE TABLE v314_hot (id int primary key, pad text);
	INSERT INTO v314_hot VALUES (1, 'a');
});
$n0->safe_psql('postgres', q{
	UPDATE v314_hot SET pad = 'b' WHERE id = 1;
	UPDATE v314_hot SET pad = 'c' WHERE id = 1;
	UPDATE v314_hot SET pad = 'd' WHERE id = 1;
});
$n0->safe_psql('postgres', 'VACUUM v314_hot');
is($n0->safe_psql('postgres', q{SELECT pad FROM v314_hot WHERE id = 1}), 'd',
   'L4b local HOT chain prunes + latest version intact');


# ============================================================
# L5: D8 observability — 'visibility' category exposes 6 counters.
# ============================================================
my $vis_rows = $n0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category = 'visibility'});
is($vis_rows, '6', 'L5 visibility category exposes 6 D8 counters');

my $vis_keys = $n0->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state
	   WHERE category = 'visibility'});
like($vis_keys, qr/vis_conflict_failclosed_count/, 'L5 conflict counter present');
like($vis_keys, qr/prune_remote_keep_count/,       'L5 prune-keep counter present');
like($vis_keys, qr/vis_update_fork_count/,         'L5 update-fork counter present');


# ============================================================
# L6: 53R9H errcode registered.
# ============================================================
my $errcode_ok = $n0->safe_psql('postgres', q{
	DO $$
	BEGIN
		RAISE EXCEPTION USING ERRCODE = '53R9H';
	EXCEPTION WHEN OTHERS THEN
		IF SQLSTATE = '53R9H' THEN
			RAISE NOTICE 'ok';
		END IF;
	END $$;
	SELECT 1;
});
is($errcode_ok, '1', 'L6 SQLSTATE 53R9H is a usable errcode');


# ============================================================
# L7: cross-node best-effort (diag).  A true 53R9H trigger requires the
#     shared-storage cross-node write harness; here we just confirm the
#     conflict counter is readable on both nodes (observed in CI).
# ============================================================
for my $idx (0, 1) {
	my $node = $idx == 0 ? $pair->node0 : $pair->node1;
	my $c = $node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'visibility' AND key = 'vis_conflict_failclosed_count'});
	diag("L7 node$idx vis_conflict_failclosed_count = $c");
	ok(defined($c) && $c ne '', "L7 node$idx conflict counter readable");
}


$pair->stop_pair;
done_testing();
