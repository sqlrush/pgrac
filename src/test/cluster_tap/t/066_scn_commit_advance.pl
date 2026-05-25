#-------------------------------------------------------------------------
#
# 066_scn_commit_advance.pl
#    Stage 1.16 + spec-1.16 v0.2 end-to-end: real PG xact.c +
#    twophase.c hooks advancing local_scn at commit / abort decision
#    points.  Covers Q1+Q2 (commit/abort hook before START_CRIT_SECTION),
#    Q4 (subxact unchanged), Q5 (PREPARE skip / COMMIT PREPARED bumps /
#    ROLLBACK PREPARED bumps abort), Q7 (no-XID read-only skip / SELECT
#    FOR UPDATE bumps), Q8 (ROLLBACK PREPARED → abort), and observe()
#    Lamport bump (Q3 upgrade).
#
#    Test matrix (L1-L13):
#      L1   normal commit bumps commit_advance_count + total_advance_count
#      L2   normal abort bumps abort_advance_count + total_advance_count
#      L3   pure SELECT (no XID) commit -- counters unchanged (Q7)
#      L3b  SELECT FOR UPDATE (top XID) commit -- bumps commit (Q7)
#      L4   pure SELECT abort -- counters unchanged
#      L5   subtransaction commit -- only outer commit bumps once (Q4)
#      L6   subtransaction rollback to savepoint -- only outer commit
#           bumps once (Q4)
#      L7   2PC: PREPARE TRANSACTION -- counters unchanged;
#           COMMIT PREPARED bumps commit_advance_count (Q5)
#      L8   2PC: PREPARE TRANSACTION -- counters unchanged;
#           ROLLBACK PREPARED bumps abort_advance_count (Q5+Q8)
#      L9   cluster_scn_observe Lamport bump: remote > current bumps;
#           observe_bump_count++ (Q3)
#      L10  pg_cluster_state has 10 SCN keys (7 + 3 new counters) (Q6)
#      L11  inject :error on cluster-scn-commit-pre-advance fires
#           SQLSTATE 53R0X during commit
#      L12  cluster.enabled=on with no top XID still emits 0 SCN bumps
#           after a long-running pure-SELECT loop (smoke check)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/066_scn_commit_advance.pl
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
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;

# Pin a deterministic node_id; cluster_finalize_startup_running validates
# this before postmaster reaches RUNNING (spec-1.16 D13).
$node->append_conf('postgresql.conf', "cluster.node_id = 7\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "cluster.pcm_grd_max_entries = 0\n");
$node->append_conf('postgresql.conf', "max_prepared_transactions = 8\n");
PostgreSQL::Test::Utils::append_to_file(
	$node->data_dir . '/pgrac.conf', <<'EOC');
[cluster]
name = scn_commit_advance

[node.7]
interconnect_addr = 127.0.0.1:19066

[node.8]
interconnect_addr = 127.0.0.1:19067
EOC

$node->start;

# Helper: read a counter from pg_cluster_state
sub counter
{
	my ($key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value::bigint FROM pg_cluster_state
		 WHERE category='scn' AND key='$key'
	});
}

# Setup test table for L3b / L1 / L2
$node->safe_psql('postgres', q{CREATE TABLE t1 (id int); INSERT INTO t1 VALUES (1);});


# ----------
# L1: normal commit bumps commit + total advance counters.
# ----------
my $commit_before = counter('scn_commit_advance_count');
my $total_before  = counter('scn_total_advance_count');
$node->safe_psql('postgres', 'BEGIN; INSERT INTO t1 VALUES (2); COMMIT;');
my $commit_after = counter('scn_commit_advance_count');
my $total_after  = counter('scn_total_advance_count');
ok($commit_after >= $commit_before + 1,
   "L1 commit_advance_count bumped (+ $commit_before -> $commit_after)");
ok($total_after >= $total_before + 1,
   "L1 total_advance_count bumped (+ $total_before -> $total_after)");


# ----------
# L2: normal abort bumps abort + total advance counters.
# ----------
my $abort_before = counter('scn_abort_advance_count');
$total_before    = counter('scn_total_advance_count');
$node->psql('postgres', 'BEGIN; INSERT INTO t1 VALUES (3); ROLLBACK;');
my $abort_after = counter('scn_abort_advance_count');
$total_after    = counter('scn_total_advance_count');
ok($abort_after >= $abort_before + 1,
   "L2 abort_advance_count bumped ($abort_before -> $abort_after)");
ok($total_after >= $total_before + 1,
   "L2 total_advance_count bumped ($total_before -> $total_after)");


# ----------
# L3: pure SELECT (no top XID) commit -- counters unchanged (Q7).
# ----------
$commit_before = counter('scn_commit_advance_count');
$total_before  = counter('scn_total_advance_count');
$node->safe_psql('postgres', 'BEGIN; SELECT 1; COMMIT;');
$commit_after = counter('scn_commit_advance_count');
$total_after  = counter('scn_total_advance_count');
is($commit_after, $commit_before, 'L3 pure SELECT commit does not bump commit counter (Q7)');
is($total_after,  $total_before,  'L3 pure SELECT commit does not bump total counter');


# ----------
# L3b: SELECT FOR UPDATE -- top XID assigned, commit record written;
# spec-1.16 v0.2 Q7 expects this path TO bump.
# ----------
$commit_before = counter('scn_commit_advance_count');
$node->safe_psql('postgres', 'BEGIN; SELECT * FROM t1 FOR UPDATE; COMMIT;');
$commit_after = counter('scn_commit_advance_count');
ok($commit_after >= $commit_before + 1,
   "L3b SELECT FOR UPDATE bumps commit (top XID + commit record path; Q7 REVISED)");


# ----------
# L4: pure SELECT rollback -- counters unchanged.
# ----------
$abort_before = counter('scn_abort_advance_count');
$node->psql('postgres', 'BEGIN; SELECT 1; ROLLBACK;');
$abort_after = counter('scn_abort_advance_count');
is($abort_after, $abort_before, 'L4 pure SELECT abort does not bump abort counter');


# ----------
# L5: subtransaction commit (RELEASE SAVEPOINT) -- only outer commit
# bumps; subxact does not (Q4).
# ----------
$commit_before = counter('scn_commit_advance_count');
$node->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (10);
	  SAVEPOINT s1;
	  INSERT INTO t1 VALUES (11);
	  RELEASE SAVEPOINT s1;
	  INSERT INTO t1 VALUES (12);
	COMMIT;
});
$commit_after = counter('scn_commit_advance_count');
is($commit_after - $commit_before, 1,
   "L5 subxact commit (SAVEPOINT + RELEASE) bumps outer commit only +1, not +N (Q4)");


# ----------
# L6: subtransaction abort (ROLLBACK TO SAVEPOINT) -- only outer commit
# bumps once; abort_advance_count unchanged.
# ----------
$commit_before = counter('scn_commit_advance_count');
$abort_before  = counter('scn_abort_advance_count');
$node->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (20);
	  SAVEPOINT s1;
	  INSERT INTO t1 VALUES (21);
	  ROLLBACK TO SAVEPOINT s1;
	  INSERT INTO t1 VALUES (22);
	COMMIT;
});
$commit_after = counter('scn_commit_advance_count');
$abort_after  = counter('scn_abort_advance_count');
is($commit_after - $commit_before, 1,
   "L6 subxact abort (ROLLBACK TO SAVEPOINT) bumps outer commit +1");
is($abort_after, $abort_before,
   "L6 subxact abort does not bump abort counter (Q4)");


# ----------
# L7: 2PC -- PREPARE TRANSACTION does not bump; COMMIT PREPARED does (Q5).
# ----------
$commit_before = counter('scn_commit_advance_count');
$node->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (30);
	PREPARE TRANSACTION 'tx1';
});
my $commit_after_prepare = counter('scn_commit_advance_count');
is($commit_after_prepare, $commit_before,
   "L7 PREPARE TRANSACTION does not bump commit (PREPARE != durable commit; Q5)");

$node->safe_psql('postgres', "COMMIT PREPARED 'tx1'");
$commit_after = counter('scn_commit_advance_count');
ok($commit_after >= $commit_before + 1,
   "L7 COMMIT PREPARED bumps commit_advance_count (Q5 durable commit point)");


# ----------
# L8: 2PC -- ROLLBACK PREPARED bumps abort (Q5 + Q8).
# ----------
$commit_before = counter('scn_commit_advance_count');
$abort_before  = counter('scn_abort_advance_count');
$node->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (40);
	PREPARE TRANSACTION 'tx2';
});
is(counter('scn_commit_advance_count'), $commit_before,
   "L8 PREPARE TRANSACTION 'tx2' does not bump commit (Q5)");
is(counter('scn_abort_advance_count'), $abort_before,
   "L8 PREPARE TRANSACTION 'tx2' does not bump abort either");

$node->safe_psql('postgres', "ROLLBACK PREPARED 'tx2'");
my $abort_after_rb = counter('scn_abort_advance_count');
ok($abort_after_rb >= $abort_before + 1,
   "L8 ROLLBACK PREPARED bumps abort_advance_count (Q5 + Q8)");


# ----------
# L9: observe Lamport bump -- remote > current bumps; observe_bump_count++.
# ----------
my $observe_before = counter('scn_observe_bump_count');
my $local_before   = counter('scn_current_local');
my $remote_high    = $local_before + 1_000_000;
my $remote_scn     = $node->safe_psql('postgres',
	"SELECT (7::bigint << 56) | $remote_high");
$node->safe_psql('postgres', "SELECT cluster_scn_observe($remote_scn)");
my $observe_after = counter('scn_observe_bump_count');
my $local_after   = counter('scn_current_local');
ok($observe_after >= $observe_before + 1,
   "L9 observe with remote > current bumps observe_bump_count (Q3)");
ok($local_after >= $remote_high + 1,
   "L9 Lamport bump: current_local advanced to >= remote+1 ($remote_high+1; got $local_after)");


# ----------
# L10: pg_cluster_state has 10 SCN keys (Q6: 7 + 3 new counters).
# ----------
my @expected_keys = (
	'scn_node_id',
	'scn_current_local',
	'scn_current_encoded',
	'scn_max_observed_remote',
	'scn_total_advance_count',
	'scn_initialized_at',
	'scn_last_advance_at',
	'scn_commit_advance_count',
	'scn_abort_advance_count',
	'scn_observe_bump_count');
foreach my $k (@expected_keys)
{
	my $count = $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE category='scn' AND key='$k'");
	is($count, '1', "L10 pg_cluster_state has scn key '$k' (Q6 dump_scn 10 keys)");
}


# ----------
# L11: inject :error on cluster-scn-commit-pre-advance fires during
# commit hook (before START_CRIT_SECTION at xact.c:1404).
# ----------
my ($ret, $stdout, $stderr) = $node->psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-commit-pre-advance', 'error', 0);
	BEGIN;
	  INSERT INTO t1 VALUES (50);
	COMMIT;
});
isnt($ret, 0, 'L11 commit fails when inject :error armed on commit-pre-advance');
like($stderr, qr/53R0X|cluster injection|cluster-scn-commit-pre-advance/i,
   'L11 inject :error fires during commit hook (Q1 placement before critical section)');


# ============================================================
# Hardening v1.0.1 (round 9 codex review) tests
# ============================================================

# ----------
# L12 (round 9 P1 finding 1): cluster.enabled=off must silence SCN
# advance even when cluster.node_id is valid.  cluster_finalize_startup
# _running() docstring promises "cluster.enabled=off for vanilla PG
# behaviour"; without the round 9 fix, commits would still bump SCN
# counters because skip helper only checked shmem + node_id.
#
# cluster.enabled is PGC_POSTMASTER -- must restart, not reload.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.enabled = off\n");
$node->start;

# Read counters BEFORE the SCN-silenced commit (cluster.node_id=7 still
# pinned via initial postgresql.conf; cluster.enabled=off should still
# silence SCN advance per round 9 fix).
my $commit_before_off = counter('scn_commit_advance_count');
my $abort_before_off  = counter('scn_abort_advance_count');
my $total_before_off  = counter('scn_total_advance_count');

$node->safe_psql('postgres', 'BEGIN; INSERT INTO t1 VALUES (60); COMMIT;');
$node->psql('postgres', 'BEGIN; INSERT INTO t1 VALUES (61); ROLLBACK;');

my $commit_after_off = counter('scn_commit_advance_count');
my $abort_after_off  = counter('scn_abort_advance_count');
my $total_after_off  = counter('scn_total_advance_count');

is($commit_after_off, $commit_before_off,
   'L12 commit_advance_count unchanged with cluster.enabled=off + node_id=7 (round 9 P1)');
is($abort_after_off, $abort_before_off,
   'L12 abort_advance_count unchanged with cluster.enabled=off (round 9 P1)');
is($total_after_off, $total_before_off,
   'L12 total_advance_count unchanged with cluster.enabled=off (round 9 P1)');

# Restore cluster.enabled=on for subsequent L13-L15 tests
$node->stop;
# Strip the cluster.enabled=off line we just appended; simplest: rewrite conf
my $conf_path = $node->data_dir . "/postgresql.conf";
my $conf_content = slurp_file($conf_path);
$conf_content =~ s/^cluster\.enabled\s*=\s*off\s*\n//mg;
open(my $fh, '>', $conf_path) or die "Cannot rewrite conf: $!";
print $fh $conf_content;
close($fh);
$node->start;


# ----------
# L13 (round 9 P1 finding 2): observe Lamport bump uses `>=` not `>`.
# When remote_local equals current_local_scn, observe must STILL push
# current to remote+1 (causal-ordering rule).  Two events with the
# same Lamport timestamp must not share an SCN.
# ----------
my $local_eq = counter('scn_current_local');
my $observe_before_eq = counter('scn_observe_bump_count');
# Construct a remote SCN whose local equals current_local_scn.
my $remote_eq = $node->safe_psql('postgres',
	"SELECT 42::bigint * 72057594037927936 + $local_eq");
$node->safe_psql('postgres', "SELECT cluster_scn_observe($remote_eq)");
my $local_after_eq   = counter('scn_current_local');
my $observe_after_eq = counter('scn_observe_bump_count');
ok($local_after_eq >= $local_eq + 1,
   "L13 observe(remote == current=$local_eq) bumps current_local to >= $local_eq+1 (got $local_after_eq; round 9 P1 Lamport `>=` fix)");
ok($observe_after_eq >= $observe_before_eq + 1,
   "L13 observe_bump_count incremented for remote == current case (round 9 P1)");


# ----------
# L14 (round 9 P1 finding 3): observe rejects remote_local at/above
# SCN_MAX_LOCAL with WARNING instead of overflowing the 56-bit field.
# Without the guard, remote_local + 1 would mask back to 0 on
# production builds (silent SCN re-use disaster).
# ----------
# SCN_MAX_LOCAL = 2^56 - 1 = 72057594037927935.  Construct a remote SCN
# whose local == SCN_MAX_LOCAL using node_id=42.
my $remote_max = $node->safe_psql('postgres',
	"SELECT 42::bigint * 72057594037927936 + 72057594037927935");
my $observe_before_max = counter('scn_observe_bump_count');
my $local_before_max   = counter('scn_current_local');
($ret, $stdout, $stderr) = $node->psql('postgres',
	"SELECT cluster_scn_observe($remote_max)");
# Function returns void (success); the guard emits WARNING then returns.
is($ret, 0, 'L14 observe at SCN_MAX_LOCAL returns successfully (WARNING + early return; round 9 P1)');
like($stderr, qr/at or above SCN_MAX_LOCAL|prevent overflow/i,
   'L14 WARNING message mentions SCN_MAX_LOCAL / overflow');
my $local_after_max = counter('scn_current_local');
ok($local_after_max <= 72057594037927934,
   "L14 current_local_scn did NOT overflow past SCN_MAX_LOCAL-1 (got $local_after_max; round 9 P1)");
my $observe_after_max = counter('scn_observe_bump_count');
is($observe_after_max, $observe_before_max,
   'L14 observe_bump_count NOT incremented for rejected SCN_MAX_LOCAL input (round 9 P1)');


# ----------
# L15 (round 9 P2 finding 4): observe_bump_count and total_advance_count
# both incremented INSIDE the same LW_EXCLUSIVE section; dump_scn never
# observes a partial state.  Verify total_advance_count reflects observe
# bumps in addition to commit/abort advance.
# ----------
my $total_before_obs = counter('scn_total_advance_count');
my $observe_before_obs = counter('scn_observe_bump_count');
my $local_now = counter('scn_current_local');
# Observe with remote_local strictly greater than current to force bump
my $remote_force_bump = $node->safe_psql('postgres',
	"SELECT 42::bigint * 72057594037927936 + ($local_now + 500)");
$node->safe_psql('postgres', "SELECT cluster_scn_observe($remote_force_bump)");
my $total_after_obs = counter('scn_total_advance_count');
my $observe_after_obs = counter('scn_observe_bump_count');
is($observe_after_obs - $observe_before_obs, 1,
   'L15 observe_bump_count incremented +1 for forced bump (round 9 P2 same-lock)');
is($total_after_obs - $total_before_obs, 1,
   'L15 total_advance_count ALSO incremented +1 for observe bump (round 9 P2; observe is real SCN advance)');


$node->stop;
done_testing();
