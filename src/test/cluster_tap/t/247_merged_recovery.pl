#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 247_merged_recovery.pl
#    spec-4.5 -- k-way SCN merged recovery: the REACHABLE surface.
#
#    A-closure (2026-06-11): with only stub/local backends a crashed
#    peer's SHARED-storage page cannot be honestly applied, so merged
#    recovery stays CAPABILITY-GATED off on those backends: cluster.
#    merged_recovery=on + crash candidates -> FATAL 53RA3 'not
#    supported without a shared-data storage backend'.  spec-4.5a
#    lands the real cluster_fs shared-data backend, so the former
#    SKIP leg is now a REAL two-node cold-merge smoke (L5); the full
#    cross-instance CR/TT closure lives in t/248.
#
#      L1  merged_recovery=off: crash recovery is single-stream, the
#          node's own rows survive (today's behaviour, byte-identical)
#      L2  merged_recovery=on, NO candidate: not engaged, normal
#          single-stream recovery
#      L3  merged_recovery=on + a forged stale-ACTIVE candidate slot:
#          the capability gate FATALs 53RA3 (backend here is the
#          default stub -- the local-backend leg is preserved) --
#          never a silent single-stream fallback
#      L4  after the gate FATAL, merged_recovery=off recovers the
#          node's own stream cleanly (the candidate slot is observed,
#          not acted upon)
#      L5  true two-node shared-page cold-merge apply-through smoke
#          (cluster_fs backend, spec-4.5a): both nodes write disjoint
#          row sets into ONE shared table tree, both crash, the
#          survivor merges and sees both sets
#
#    NB: this is a Perl TAP file -- never run clang-format on it.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.5-kway-scn-merge-replay.md (FROZEN v1.0, A-closure)
#          spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D13)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracWalState qw(crc32c read_file_raw write_file_raw);
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

# Forge a stale-ACTIVE candidate slot for thread $tid (node $tid-1).
sub forge_candidate
{
	my ($regfile, $tid) = @_;
	my $img = read_file_raw($regfile);
	my $off = 512 + ($tid - 1) * 512;
	my $slot = "\0" x 512;
	substr($slot, 0, 20) = pack('LSSlLL', 0x50475754, 1, $tid, $tid - 1, 1, 1);
	substr($slot, 24, 8) = pack('q', 1);
	substr($slot, 32, 8) = pack('q', 1000);
	substr($slot, 504, 4) = pack('L', crc32c(substr($slot, 0, 504)));
	substr($img, $off, 512) = $slot;
	write_file_raw($regfile, $img);
}

my $wroot = PostgreSQL::Test::Utils::tempdir();
my $regfile = "$wroot/pgrac_wal_state";

my $node = PgracClusterNode->new('merged_a');
$node->init(extra => [ '-X', "$wroot/thread_4" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.recovery_stale_active_ms = 1000\n"
	  . "cluster.recovery_workers_max = 0\n"
	  . "autovacuum = off\n");

# L1: merged_recovery=off -> single-stream crash recovery.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = off\n");
$node->start;
$node->safe_psql('postgres',
	'CREATE TABLE s (a int); INSERT INTO s SELECT generate_series(1, 150)');
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'150', 'L1 merged_recovery=off: single-stream crash recovery survives');

# L2: merged_recovery=on, no candidate -> not engaged.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = on\n");
$node->restart;
$node->safe_psql('postgres', 'INSERT INTO s SELECT generate_series(151, 300)');
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'300', 'L2 merged_recovery=on with no candidate: normal recovery');

# L3: merged_recovery=on + forged candidate -> capability gate 53RA3.
$node->stop('immediate');
forge_candidate($regfile, 5);
my $log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0,
	'L3 start refused when merged_recovery=on meets a crash candidate');
my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/merged k-way recovery is not supported without a shared-data storage backend/,
	'L3 capability gate FATALs 53RA3 (not a silent single-stream fallback)');
like($log, qr/cluster\.shared_data_dir/,
	'L3 the hint points at the shared-data backend prerequisite');

# L4: back to off -> own stream recovers, candidate observed only.
$node->adjust_conf('postgresql.conf', 'cluster.merged_recovery', 'off');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
	'300', 'L4 merged_recovery=off recovers own stream past the forged candidate');
is($node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state "
	  . "WHERE category='recovery' AND key='plan_crashed_candidates'"),
	'5', 'L4 the candidate is observed in the plan, not acted upon');
$node->stop;

# ============================================================
# L5: true two-node shared-page cold-merge apply-through smoke
#     (spec-4.5a cluster_fs backend; full closure in t/248).
# ============================================================
{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('merged247',
		wal_threads_root => 1,
		shared_data      => 1,
		extra_conf       => [
			'autovacuum = off',
			'cluster.merged_recovery = on',
			'cluster.recovery_workers_max = 0',
			'cluster.recovery_stale_active_ms = 1000',
			# spec-4.7a: this leg builds a WAL/undo window for merged recovery,
			# NOT a writer-transfer test.  It has both nodes write the SAME
			# relation concurrently (pre-crash), which under hold-until-revoked
			# (default on) is the deferred cross-node writer-transfer case and
			# bounded-fail-closes.  Disable the node-level PCM cache here so the
			# holder releases on content-lock unlock (pre-4.7a semantics) and
			# the concurrent pre-crash writes proceed.
			'cluster.gcs_block_local_cache = off',
		]);
	my $na = $pair->node0;
	my $nb = $pair->node1;

	$pair->start_pair;
	$pair->wait_for_peer_state(0, 1, 'connected', 30);

	# Same DDL on both sides = same relfilenode into the shared root
	# (the harness naming premise; t/248 L0 pins it, here just bail).
	$na->safe_psql('postgres', 'CREATE TABLE t247 (v int)');
	$nb->safe_psql('postgres', 'CREATE TABLE t247 (v int)');

	# Serialized disjoint row sets: A writes + fences, then B.  B
	# checkpoints BEFORE its writes (t/248 L1 premise): the checkpoint
	# publishes thread_2's redo into the WAL registry, so every B write
	# below lands in the merge window and its undo + commit outcome
	# MATERIALIZE at A -- that is what gives A the authority to judge
	# B's xids.  (A post-write checkpoint instead ships the rows via the
	# page flush but leaves the merge window empty: A then holds B's
	# tuples with NO commit authority, and every read of them is an
	# honest 53R97/53R9G fail-closed -- the apply-through premise this
	# smoke exists to exercise would be silently skipped.)
	$na->safe_psql('postgres',
		'INSERT INTO t247 SELECT generate_series(1, 50)');
	$na->safe_psql('postgres', 'CHECKPOINT');
	$nb->safe_psql('postgres', 'CHECKPOINT');
	my $a_scn = $na->safe_psql('postgres', 'SELECT cluster_scn_current()');
	$nb->safe_psql('postgres', "SELECT cluster_scn_observe($a_scn)");
	$nb->safe_psql('postgres',
		'INSERT INTO t247 SELECT generate_series(51, 100)');

	# All-cold crash; immediate shutdown = crash-state stop (no
	# shutdown checkpoint, wal_state slot stays ACTIVE) without kill
	# -9's orphaned-children shmem residue (t/243 L4 pattern).
	$nb->stop('immediate');
	$na->stop('immediate');
	sleep 2;    # > recovery_stale_active_ms

	# Disaster-recovery form: bring node0 up as a SINGLE node (drop the
	# peer from its pgrac.conf).  Then block-master lookup resolves to
	# self (declared_count == 1) and post-recovery data reads bypass
	# GCS Cache Fusion -- a LIVE 2-node read of a peer-mastered block is
	# blocked by a pre-existing Stage-2 GCS gap (t/243 L4 / roadmap
	# 4.7), orthogonal to the merged-recovery machinery under test.
	# Candidate discovery scans the WAL-state registry (every thread
	# slot), NOT pgrac.conf membership, so node0 still finds thread_2's
	# crashed stream and materializes B's data.
	my $ic0 = $pair->ic_port(0);
	my $conf = $na->data_dir . '/pgrac.conf';
	open my $fh, '>', $conf or die "open $conf: $!";
	print $fh "[cluster]\nname = merged247\n\n"
	  . "[node.0]\ninterconnect_addr = 127.0.0.1:$ic0\n";
	close $fh;

	my $off = -s $na->logfile;
	$na->start;
	my $mlog = PostgreSQL::Test::Utils::slurp_file($na->logfile, $off);
	like($mlog, qr/cluster merged recovery: engage decision PASSED/,
		'L5 cluster_fs backend passes the capability gate (real engage)');

	is($na->safe_psql('postgres', 'SELECT count(*) FROM t247'), '100',
		'L5 survivor sees BOTH row sets after the cold merge (50 own + 50 peer)');
	$na->stop;
}

done_testing();
