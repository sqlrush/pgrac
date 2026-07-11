#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 255_cluster_5_22b_undo_gcs_shared.pl
#	  spec-5.22b D2-7 — Shared-undo block GCS integration data-plane TAP
#	  on a 2-node ClusterPair + shared cluster_fs root.
#
#	  This is the behavioral (end-to-end) half of D2-7; the pure decision
#	  cores (owner-as-master routing, S/X grant contracts, admissibility,
#	  serve-gate, PI-discard) are exhaustively covered standalone by
#	  cluster_unit (test_cluster_undo_gcs, U1-U10).  This TAP pins the one
#	  thing cluster_unit cannot: the D2-2 physical migration wired through
#	  the real undo allocator on a running peer-mode pair.
#
#	  L1   ClusterPair startup: both nodes alive under peer-mode +
#	       shared cluster_fs, coherence OFF at boot (default, inert).
#	  L2   the 6 owner-as-master undo GCS grant-plane counter keys are
#	       present + queryable (register-ahead).
#	  L3   coherence OFF (default): node0's own runtime undo lands on the
#	       LOCAL DataDir, and the shared root holds NO node0 undo segment
#	       -- the inert / regression-safe path (negative control).
#	  L4   coherence ON (ALTER SYSTEM + reload): after a forced fresh
#	       segment allocation, node0's own runtime undo now lands on the
#	       SHARED cluster_fs root -- owner-as-master physical migration,
#	       the D2-2 heart, observed end-to-end at the filesystem.
#	  L5   the migrated (shared-root) undo write path is functional:
#	       INSERT / UPDATE / DELETE round-trip correctly with no error,
#	       so the segment relocation did not break the undo data path.
#	  L6   grant-plane counters stay 0 at rest: acquire_shared /
#	       acquire_exclusive / invalidate_peers have no live consumer at
#	       D2 (the W3 wall still fail-closes a foreign-undo self-read, and
#	       master==self fails closed), so the peer-really-reads-foreign-
#	       undo leg is forward-D6.  This is asserted, not faked.
#
#	  NOT covered here (honestly forward-linked, never a faked pass):
#	    - a peer really reading a foreign owner's undo end-to-end
#	      ("committed seed visible") -> D6 wires the seed consumer.
#	    - X-transfer invalidation of a live peer S/PI holder -> D6.
#	    - dead-owner SERVE from shared storage (the positive leg) -> D4.
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/255_cluster_5_22b_undo_gcs_shared.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2-7, §4.2)
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


# Own-instance undo for node0 (cluster_node_id = 0) lives under the
# owner-partitioned instance_0 subtree; the shared and local paths differ
# only in their root (cluster_shared_fs_undo_path_resolve).
my $INSTANCE_SUBDIR = 'pg_undo/instance_0';

# Count node0 undo segment files under a given root's instance_0 subtree.
sub undo_seg_count
{
	my ($root) = @_;
	my @segs = glob("$root/$INSTANCE_SUBDIR/seg_*.dat");
	return scalar(@segs);
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_22b_undo_gcs',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'cluster.undo_segments_max_per_instance = 256',
		'cluster.undo_segment_create_timeout_ms = 5000',
		'max_prepared_transactions = 4',
		# coherence stays OFF at boot -- L3 is the inert negative control;
		# L4 arms it via ALTER SYSTEM so the off->on transition is proven,
		# not just the on state.
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0       = $pair->node0;
my $node1       = $pair->node1;
my $data0       = $node0->data_dir;
my $shared_root = $pair->shared_data_root;


# ----------
# L1: both nodes alive under peer-mode + shared cluster_fs (coherence off).
# ----------
{
	my $r0 = $node0->safe_psql('postgres', 'SELECT 1');
	is($r0, '1', "L1a node0 alive under peer-mode + shared cluster_fs");

	my $r1 = $node1->safe_psql('postgres', 'SELECT 1');
	is($r1, '1', "L1b node1 alive under peer-mode + shared cluster_fs");

	# The pair really is peer-mode (declared 2-node topology), the precondition
	# for the D2-2 migration gate.
	my $ncount = $node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_nodes});
	ok($ncount >= 2, "L1c node0 sees a >=2-node topology (peer-mode, ncount=$ncount)");

	# coherence is OFF at boot (the inert default).
	my $coh = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_gcs_coherence'});
	is($coh, 'off', "L1d cluster.undo_gcs_coherence default = off (inert)");
}


# ----------
# L2: the 6 owner-as-master undo GCS grant-plane counter keys are present.
# ----------
{
	my $keys = $node0->safe_psql('postgres',
		q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state
		   WHERE category='undo' AND key LIKE 'undo_gcs_%'});
	is($keys,
		'undo_gcs_grant_exclusive_count,undo_gcs_grant_shared_count,undo_gcs_invalidate_notify_count,undo_gcs_local_fast_path_count,undo_gcs_remaster_deny_count,undo_gcs_ship_bytes',
		"L2 all 6 undo GCS grant-plane counter keys present");
}


# ----------
# L3: coherence OFF -> node0 own undo lands LOCAL, shared root has none.
#     (the inert / regression-safe negative control for the migration gate)
# ----------
{
	$node0->safe_psql('postgres', q{
	    CREATE TABLE t_undo_gcs (id int, v text);
	    INSERT INTO t_undo_gcs SELECT g, 'boot-local' || g FROM generate_series(1, 20) g;
	});

	# Force a fresh segment allocation so the placement decision is exercised
	# by the allocator under the current (off) coherence value.
	my $force0 = $node0->safe_psql('postgres',
		q{SELECT cluster_undo_test_force_segment_end()});
	is($force0, 't', "L3a force_segment_end true after active segment claimed");
	$node0->safe_psql('postgres',
		q{INSERT INTO t_undo_gcs VALUES (1001, 'local-after-force')});

	my $local_off  = undo_seg_count($data0);
	my $shared_off = undo_seg_count($shared_root);

	ok($local_off >= 1,
		"L3b coherence off: node0 undo on LOCAL DataDir ($local_off seg file(s))");
	is($shared_off, 0,
		"L3c coherence off: shared cluster_fs root holds NO node0 undo (inert)");
}


# ----------
# L4: coherence ON -> node0 own undo migrates to the SHARED cluster_fs root.
#     (owner-as-master physical migration -- the D2-2 heart, observed e2e)
# ----------
{
	my $shared_before = undo_seg_count($shared_root);
	is($shared_before, 0, "L4a shared root empty of node0 undo before arming coherence");

	$node0->safe_psql('postgres',
		'ALTER SYSTEM SET cluster.undo_gcs_coherence = on');
	$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(500_000);

	# A fresh backend reads the armed value (postgresql.auto.conf); force a
	# fresh segment so the next allocation resolves under coherence = on.
	my $coh_on = $node0->safe_psql('postgres',
		q{SELECT setting FROM pg_settings WHERE name = 'cluster.undo_gcs_coherence'});
	is($coh_on, 'on', "L4b cluster.undo_gcs_coherence armed (off -> on)");

	my $force1 = $node0->safe_psql('postgres',
		q{SELECT cluster_undo_test_force_segment_end()});
	is($force1, 't', "L4c force_segment_end true under coherence on");
	$node0->safe_psql('postgres',
		q{INSERT INTO t_undo_gcs VALUES (2001, 'shared-after-arm')});

	my $shared_after = undo_seg_count($shared_root);
	ok($shared_after >= 1,
		"L4d coherence on: node0 own undo migrated to SHARED root ($shared_after seg file(s))");
}


# ----------
# L5: the migrated (shared-root) undo write path is functional.
#     INSERT / UPDATE / DELETE round-trip with no error, so relocating the
#     segment onto the shared root did not break the undo data path.
# ----------
{
	$node0->safe_psql('postgres', q{
	    INSERT INTO t_undo_gcs SELECT g, 'shared-dml' || g FROM generate_series(3000, 3040) g;
	    UPDATE t_undo_gcs SET v = v || '-upd' WHERE id BETWEEN 3000 AND 3040;
	    DELETE FROM t_undo_gcs WHERE id BETWEEN 3000 AND 3010;
	});

	my $updated = $node0->safe_psql('postgres',
		q{SELECT count(*) FROM t_undo_gcs WHERE v LIKE 'shared-dml%-upd'});
	is($updated, '30',
		"L5a shared-root undo write path functional: 30 rows survived update+delete");

	# A rolled-back update must leave the row unchanged (undo applied from the
	# shared-root segment).
	$node0->safe_psql('postgres', q{
	    BEGIN;
	    UPDATE t_undo_gcs SET v = 'ROLLED-BACK' WHERE id = 2001;
	    ROLLBACK;
	});
	my $rb = $node0->safe_psql('postgres',
		q{SELECT v FROM t_undo_gcs WHERE id = 2001});
	is($rb, 'shared-after-arm',
		"L5b rollback restored the pre-image from shared-root undo");
}


# ----------
# L6: grant-plane counters stay 0 at rest -- the peer-really-reads-foreign-
#     undo consumer is forward-D6, asserted (not faked).
#
#     acquire_shared / acquire_exclusive / invalidate_peers are skeleton-ahead
#     at D2: the W3 wall (cluster_undo_record.c) still fail-closes a foreign
#     undo self-read and master==self fails closed, so no live caller drives
#     these counters.  A non-zero here would mean a consumer landed early,
#     contradicting the skeleton-ahead state -- so 0 is a real assertion.
# ----------
{
	my $counters = $node0->safe_psql('postgres',
		q{SELECT string_agg(key || '=' || value, ',' ORDER BY key)
		    FROM pg_cluster_state
		   WHERE category='undo' AND key LIKE 'undo_gcs_%'});
	is($counters,
		'undo_gcs_grant_exclusive_count=0,undo_gcs_grant_shared_count=0,'
		. 'undo_gcs_invalidate_notify_count=0,undo_gcs_local_fast_path_count=0,'
		. 'undo_gcs_remaster_deny_count=0,undo_gcs_ship_bytes=0',
		"L6 grant-plane counters all 0 at rest (peer-read consumer is forward-D6)");

	note('D2-7 forward-D6 (not asserted here, never faked): a peer really '
		. 'reading a foreign owner\'s undo end-to-end ("committed seed visible"), '
		. 'X-transfer invalidation of a live peer holder, and dead-owner SERVE '
		. 'from shared storage. The D2 delivery is the owner-as-master routing + '
		. 'physical migration proven in L1-L5.');
}


$pair->stop_pair;
done_testing();
