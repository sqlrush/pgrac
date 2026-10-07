#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 247_merged_recovery.pl
#    Multi-node crash recovery outside the shared profile.
#
#    Merging several crashed nodes' WAL threads is supported only by the
#    shared profile's typed cold replay (cluster.shared_config = on).  In any
#    other profile a cold crash that needs a peer's thread is refused before
#    any fence admission, claim or replay, and the peer's WAL is left intact;
#    single-node crash recovery is unchanged.
#
#      L1  merged_recovery=off: single-stream crash recovery keeps the
#          node's own rows
#      L2  merged_recovery=on, no crashed peer: not engaged, normal
#          single-stream recovery
#      L3  two nodes write one shared table, both crash: the survivor with
#          merged_recovery=on is refused (53RA3) before any fence, claim or
#          replay -- never a silent single-stream fallback
#      L4  the refusal changed nothing: the peer's thread is intact and a
#          second attempt is refused the same way
#      L5  merged_recovery=off is the operator's explicit single-stream
#          choice: the survivor recovers its own stream only
#
#    Every node is provisioned through the supported entries: initdb with
#    --pgrac-wal-state-root for the WAL state registry and
#    --pgrac-hw-snapshot-root/-owner for the seed's HW image, freshly formatted
#    voting disks for a real quorum, and a native first start that publishes
#    the shared control-file authority (the pair's seed does the same).  No
#    registry, control or root bytes are written by the test.
#
#    NB: this is a Perl TAP file -- never run clang-format on it.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-s9p2-05-instance-and-cluster-recovery.md
#          spec-4.5-kway-scn-merge-replay.md (FROZEN v1.0, A-closure)
#          spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D13)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracColdPair qw(new_cold_pair);
use PostgreSQL::Test::ClusterVotingDisk qw(format_voting_file);
use PostgreSQL::Test::Utils;
use Test::More;

# A real single-member quorum for a WAL-thread node: freshly formatted voting
# disks and a declared topology (as B's ClusterSeed::configure_single_quorum,
# which is not on this line yet).  The registry slot is published only with a
# current quorum.
sub configure_single_quorum
{
	my ($node, $owner) = @_;
	my $dir = PostgreSQL::Test::Utils::tempdir();
	my @disks = map { "$dir/disk$_" } (0 .. 2);
	format_voting_file($disks[$_], $_) for 0 .. 2;
	my $csv = join(',', @disks);
	$node->append_conf('postgresql.conf',
		"cluster.allow_single_node = off\ncluster.interconnect_tier = tier1\n"
		  . "cluster.voting_disks = '$csv'\n");
	die 'single quorum requires an undeclared fixture topology'
	  if -e $node->data_dir . '/pgrac.conf';
	my $ic = PostgreSQL::Test::Cluster::get_free_port();
	my $data = PostgreSQL::Test::Cluster::get_free_port_range(2);
	PostgreSQL::Test::Utils::append_to_file($node->data_dir . '/pgrac.conf',
		"[cluster]\nname = wal_fixture\n[node.$owner]\n"
		  . "interconnect_addr = 127.0.0.1:$ic\ndata_addr = 127.0.0.1:$data\n");
	return;
}

# A statement whose failure is an assertion with the server's reason, not a
# harness abort.
sub sql_ok
{
	my ($node, $sql, $name) = @_;
	my ($rc, $out, $err) = $node->psql('postgres', $sql);
	is($rc, 0, $name) or diag($err);
	return $rc == 0;
}

# A start whose failure is an assertion carrying the server's reason.
sub start_ok
{
	my ($node, $name) = @_;
	my $off = -s $node->logfile // 0;
	my $started = $node->start(fail_ok => 1);
	ok($started, $name)
	  or diag(join("\n", grep { /FATAL|PANIC|DETAIL|HINT/ }
		  split /\n/, PostgreSQL::Test::Utils::slurp_file($node->logfile, $off)));
	return $started;
}

my $wroot = PostgreSQL::Test::Utils::tempdir();
# A formed WAL registry requires the shared control-file authority; the
# node's first start publishes it as the declared sole member.
my $sroot = PostgreSQL::Test::Utils::tempdir();

my $node = PgracClusterNode->new('merged_a');
$node->init(extra => [ '-X', "$wroot/thread_4", "--pgrac-wal-state-root=$wroot",
		"--pgrac-hw-snapshot-root=$sroot", '--pgrac-hw-snapshot-owner=3' ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.recovery_stale_active_ms = 1000\n"
	  . "cluster.recovery_workers_max = 0\n"
	  . "cluster.lms_enabled = on\n"
	  . "cluster.shared_storage_backend = cluster_fs\n"
	  . "cluster.shared_data_dir = '$sroot'\n"
	  . "cluster.controlfile_shared_authority = on\n"
	  . "autovacuum = off\n");
configure_single_quorum($node, 3);

# L1: merged_recovery=off -> single-stream crash recovery.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = off\n");
$node->start;
$node->safe_psql('postgres', 'CREATE TABLE s (a int)');
sql_ok($node, 'INSERT INTO s SELECT generate_series(1, 150)', 'L1 own rows written');
$node->stop('immediate');
if (start_ok($node, 'L1 crash recovery start'))
{
	is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
		'150', 'L1 merged_recovery=off: single-stream crash recovery survives');
}
else
{
	fail('L1 merged_recovery=off: single-stream crash recovery survives (not reached)');
}

# L2: merged_recovery=on, no crashed peer -> not engaged.
$node->append_conf('postgresql.conf', "cluster.merged_recovery = on\n");
$node->stop if defined $node->{_pid};
if (start_ok($node, 'L2 clean restart after crash recovery'))
{
	sql_ok($node, 'INSERT INTO s SELECT generate_series(151, 300)', 'L2 own rows written');
	$node->stop('immediate');
	if (start_ok($node, 'L2 crash recovery start'))
	{
		is($node->safe_psql('postgres', 'SELECT count(*) FROM s'),
			'300', 'L2 merged_recovery=on with no crashed peer: normal recovery');
		$node->stop;
	}
	else
	{
		fail('L2 merged_recovery=on with no crashed peer: normal recovery (not reached)');
	}
}
else
{
	fail('L2 own rows written (not reached)');
	fail('L2 merged_recovery=on with no crashed peer: normal recovery (not reached)');
}

# ============================================================
# L3-L5: two nodes, one shared table, both crash.
# ============================================================
{
	# Both tables exist in the seed's catalog, so both nodes share their
	# files; only the survivor ever writes t247_own.
	my $pair = new_cold_pair('merged247',
		seed_sql => 'CREATE TABLE t247 (v int); CREATE TABLE t247_own (v int)',
		extra_conf => [
			'autovacuum = off',
			'cluster.merged_recovery = on',
			'cluster.recovery_workers_max = 0',
			'cluster.recovery_stale_active_ms = 1000',
			# Both nodes write the same relation before the crash; this leg
			# builds a crash window, not a writer-transfer test, so holders
			# release on content-lock unlock (pre-4.7a semantics).
			'cluster.gcs_block_local_cache = off',
		]);
	my $na = $pair->node0;
	my $nb = $pair->node1;
	my $peer_thread = $pair->wal_threads_root . '/thread_2';

	$pair->start_pair;
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		'L3 DATA/control peer formation is connected');
	ok($pair->wait_for_pcm_x_active(30),
		'L3 PCM-X formation is ACTIVE on both writers before DML');

	is($na->safe_psql('postgres', "SELECT pg_relation_filepath('t247')"),
		$nb->safe_psql('postgres', "SELECT pg_relation_filepath('t247')"),
		'L3 both nodes use one shared relation file');

	# Serialized disjoint row sets; B's rows stay after its checkpoint so
	# they need B's thread to be recovered.
	sql_ok($na, 'INSERT INTO t247_own SELECT generate_series(1, 50)',
		'L3 survivor writes its own table');
	sql_ok($na, 'INSERT INTO t247 SELECT generate_series(1, 50)',
		'L3 survivor writes the shared table');
	$na->safe_psql('postgres', 'CHECKPOINT');
	$nb->safe_psql('postgres', 'CHECKPOINT');
	my $a_scn = $na->safe_psql('postgres', 'SELECT cluster_scn_current()');
	$nb->safe_psql('postgres', "SELECT cluster_scn_observe($a_scn)");
	sql_ok($nb, 'INSERT INTO t247 SELECT generate_series(51, 100)',
		'L3 peer writes the shared table after its checkpoint');

	# All-cold crash: immediate shutdown leaves both registry slots ACTIVE.
	$nb->stop('immediate');
	$na->stop('immediate');
	sleep 2;    # > recovery_stale_active_ms

	# Disaster-recovery form: the survivor alone (drop the peer from its
	# pgrac.conf).  Candidate discovery reads the WAL-state registry, not
	# pgrac.conf membership, so the peer's crashed thread is still found.
	my $ic0 = $pair->ic_port(0);
	my $conf = $na->data_dir . '/pgrac.conf';
	open my $fh, '>', $conf or die "open $conf: $!";
	print $fh "[cluster]\nname = merged247\n\n"
	  . "[node.0]\ninterconnect_addr = 127.0.0.1:$ic0\n";
	close $fh;

	my %peer_wal_before = map { $_ => -s "$peer_thread/$_" }
	  grep { /^[0-9A-F]{24}$/ } do { opendir(my $d, $peer_thread) or die $!; readdir $d };
	ok(%peer_wal_before, 'L3 the peer left WAL segments in its thread');

	my $off = -s $na->logfile;
	is($na->start(fail_ok => 1), 0,
		'L3 survivor start refused: a crashed peer needs the shared profile');
	my $log = PostgreSQL::Test::Utils::slurp_file($na->logfile, $off);
	like($log, qr/multi-node crash recovery requires cluster\.shared_config/,
		'L3 refusal names the shared-profile requirement (53RA3)');
	unlike($log, qr/engage decision PASSED|redo starts at|redo done at/,
		'L3 refused before any engagement or replay');

	# L4: the refusal changed nothing.
	my %peer_wal_after = map { $_ => -s "$peer_thread/$_" }
	  grep { /^[0-9A-F]{24}$/ } do { opendir(my $d, $peer_thread) or die $!; readdir $d };
	is_deeply(\%peer_wal_after, \%peer_wal_before,
		'L4 the peer thread is untouched by the refused start');
	$off = -s $na->logfile;
	is($na->start(fail_ok => 1), 0, 'L4 a second attempt is refused the same way');
	$log = PostgreSQL::Test::Utils::slurp_file($na->logfile, $off);
	like($log, qr/multi-node crash recovery requires cluster\.shared_config/,
		'L4 the second refusal names the same requirement');

	# L5: the operator's explicit single-stream choice recovers only its
	# own stream.
	$na->adjust_conf('postgresql.conf', 'cluster.merged_recovery', 'off');
	$na->start;
	is($na->safe_psql('postgres', 'SELECT count(*) FROM t247_own'), '50',
		'L5 merged_recovery=off recovers the survivor\'s own rows');
	$na->stop;
}

done_testing();
