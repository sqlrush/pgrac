#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 389_cluster_5_22b_undo_dirfsync_dualroot.pl
#	  spec-5.22b Hardening — the undo parent-directory fsync and the
#	  boot-time coherence GUC must follow the same dual-root decision
#	  as the undo segment files themselves.
#
#	  Two shipped defects (found by the first 4-node S3 e2e run) are
#	  pinned red -> green here:
#
#	  (a) cluster.undo_gcs_coherence could not be enabled from
#	      postgresql.conf at boot: its check_hook reads
#	      cluster.shared_data_dir, which registered LATER, so the boot
#	      placeholder application always rejected the value.  Worse than
#	      operability: after a crash, recovery replays
#	      XLOG_UNDO_SEGMENT_INIT with coherence silently off (the
#	      auto.conf value is rejected the same way at boot), landing the
#	      replayed segment on the LOCAL root while runtime wrote the
#	      SHARED root -- the own-instance undo split-brain that the
#	      D2-2 single-decision design exists to prevent.
#
#	  (b) the parent-directory fsync after segment create/truncate
#	      (runtime allocator and redo handler) rebuilt the directory
#	      path from DataDir unconditionally instead of deriving it from
#	      the segment path the resolver produced.  Under coherence the
#	      segment lands on the shared root but the fsync targeted the
#	      local instance dir: on a node whose local instance_<N> was
#	      never created (a basebackup clone, or any node whose first
#	      undo write happens under coherence) the fsync opens a
#	      missing directory and PANICs mid-DML; on a node where the
#	      local dir exists the fsync silently targets the wrong
#	      directory and the shared dirent is never made durable.
#
#	  L1   boot-time conf enable honored: both nodes report
#	       cluster.undo_gcs_coherence = on straight from
#	       postgresql.conf (red before the registration-order fix).
#	  L2   first undo write on node1 under coherence: succeeds, the
#	       segment lands under the SHARED root's instance_1, and the
#	       LOCAL instance_1 directory is never created (red before the
#	       dirfsync fix: ENOENT PANIC kills node1 mid-statement).
#	  L3   crash replay follows the same root: immediate-stop node1,
#	       restart, replay XLOG_UNDO_SEGMENT_INIT; the replayed segment
#	       stays on the SHARED root and the LOCAL instance_1 directory
#	       still does not exist (red before either fix: boot rejects
#	       coherence, redo routes local -> split-brain).
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/389_cluster_5_22b_undo_dirfsync_dualroot.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#	  Spec: spec-5.22b-undo-block-gcs-integration.md (Hardening)
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


# Count undo segment files for one owner instance subtree under a root.
# Instance subdirs are named by cluster_node_id (= owner_instance - 1).
sub undo_seg_count
{
	my ($root, $node_id) = @_;
	my @segs = glob("$root/pg_undo/instance_$node_id/seg_*.dat");
	return scalar(@segs);
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_22b_dirfsync',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'cluster.undo_segments_max_per_instance = 256',
		'cluster.undo_segment_create_timeout_ms = 5000',
		# The whole point of L1: coherence is enabled from postgresql.conf
		# at first boot.  Before the registration-order fix the check_hook
		# rejected this (shared_data_dir's C global not yet applied) and
		# the node silently came up with coherence off.
		'cluster.undo_gcs_coherence = on',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0       = $pair->node0;
my $node1       = $pair->node1;
my $data1       = $node1->data_dir;
my $shared_root = $pair->shared_data_root;


# ----------
# L1: boot-time conf enable honored on both nodes (registration order).
# ----------
{
	my $coh0 = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_gcs_coherence'});
	is($coh0, 'on', "L1a node0 honors cluster.undo_gcs_coherence=on from postgresql.conf");

	my $coh1 = $node1->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_gcs_coherence'});
	is($coh1, 'on', "L1b node1 honors cluster.undo_gcs_coherence=on from postgresql.conf");
}

# Belt: arm via ALTER SYSTEM + reload as well, so L2 reproduces the shipped
# PANIC on an unfixed tree even while L1 is red (the reload path always
# worked; only the boot path was rejected).
for my $n ($node0, $node1) {
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.undo_gcs_coherence = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(500_000);


# ----------
# L2: node1's FIRST undo write under coherence.  The segment must land on
#     the shared root and the local instance_1 tree must never appear.
# ----------
{
	# Precondition: node1 has never written undo, so its local instance_1
	# does not exist (initdb seeds instance_0 only) and the shared root
	# holds no node1 segment yet.
	ok(!-d "$data1/pg_undo/instance_1",
		"L2a precondition: node1 local pg_undo/instance_1 absent before first write");
	is(undo_seg_count($shared_root, 1), 0,
		"L2b precondition: shared root holds no node1 undo segment yet");

	# First-ever undo writes on node1 (its own catalog + heap).  On the
	# unfixed tree this PANICs: the segment file is created on the shared
	# root but the parent-dir fsync opens the missing LOCAL instance_1.
	$node1->safe_psql('postgres', q{
	    CREATE TABLE t_dirfsync (id int primary key, v text);
	    INSERT INTO t_dirfsync SELECT g, 'v' || g FROM generate_series(1, 50) g;
	    UPDATE t_dirfsync SET v = v || '-upd' WHERE id <= 25;
	});

	my $alive = $node1->safe_psql('postgres', 'SELECT 1');
	is($alive, '1', "L2c node1 survived its first coherent undo write (no PANIC)");

	ok(undo_seg_count($shared_root, 1) >= 1,
		"L2d node1 undo segment landed under the SHARED root instance_1");
	ok(!-d "$data1/pg_undo/instance_1",
		"L2e node1 local pg_undo/instance_1 still absent (alloc path never touched local root)");
}


# ----------
# L3: crash replay follows the same root decision.  Immediate-stop forces
#     WAL replay of XLOG_UNDO_SEGMENT_INIT on restart; with coherence
#     honored at boot the redo handler must route (and fsync) the shared
#     root, never creating the local instance_1.
# ----------
{
	$node1->stop('immediate');
	$node1->start;
	$node1->poll_query_until('postgres', 'SELECT true')
	  or die "node1 did not come back after immediate restart";

	my $rows = $node1->safe_psql('postgres',
		q{SELECT count(*) FROM t_dirfsync WHERE v LIKE '%-upd'});
	is($rows, '25', "L3a node1 data intact after crash replay (25 updated rows)");

	ok(undo_seg_count($shared_root, 1) >= 1,
		"L3b replayed segment stays under the SHARED root instance_1");
	ok(!-d "$data1/pg_undo/instance_1",
		"L3c node1 local pg_undo/instance_1 still absent after replay (no split-brain root)");
}

done_testing();
