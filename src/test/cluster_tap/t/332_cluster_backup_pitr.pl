#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 332_cluster_backup_pitr.pl
#    spec-6.5 -- cluster-aware backup / restore / PITR SQL surface.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/332_cluster_backup_pitr.pl
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

my $node = PgracClusterNode->new('cluster_backup_single');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n");
$node->start;

is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_cluster_backup}),
	'1',
	'L1 backup state view is present');
is($node->safe_psql('postgres',
	q{SELECT backup_parallel_channels || ',' || backup_wal_retention || ',' ||
	          CASE WHEN restore_points_enabled THEN 't' ELSE 'f' END || ',' ||
	          restore_point_interval_ms
	     FROM pg_stat_cluster_backup}),
	'1,0,f,0',
	'L1 backup state view exposes backup/PITR GUC readers');
is($node->safe_psql('postgres',
	q{SELECT target_type || ',' || target_action || ',' ||
	          CASE WHEN reachable THEN 't' ELSE 'f' END || ',' || reason
	     FROM pg_cluster_pitr_status}),
	'latest,pause,t,ok',
	'L2 default PITR target status is latest/pause/ok');

my $backup_row = $node->safe_psql('postgres',
	q{SELECT s.backup_id || ',' ||
	          CASE WHEN s.start_redo_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN s.checkpoint_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN t.consistent_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN t.stop_cut_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN t.manifest_crc > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN t.backup_label LIKE '%CLUSTER_BACKUP_ID: b332%' THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN t.backup_label LIKE '%CLUSTER_MANIFEST_CRC32C:%' THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_start('b332', true) AS s
	     CROSS JOIN LATERAL
	          pg_cluster_backup_stop(COALESCE(s.backup_id = '', false)) AS t});
is($backup_row, 'b332,t,t,t,t,t,t,t',
	'L3 cluster backup start/stop returns checkpoint, SCN, LSN, CRC, and label contract');

is($node->safe_psql('postgres',
	q{SELECT backup_id || ',' || node_count || ',' || thread_count || ',' ||
	          CASE WHEN manifest_crc > 0 THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_history}),
	'b332,1,1,t',
	'L4 latest manifest summary is visible');

is($node->safe_psql('postgres',
	q{SELECT restore_point_name || ',' ||
	          CASE WHEN cut_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN cut_lsn IS NOT NULL THEN 't' ELSE 'f' END
	     FROM pg_cluster_create_restore_point('rp332')}),
	'rp332,t,t',
	'L5 cluster restore point records SCN and LSN');

is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_restore_points
	    WHERE restore_point_name IN ('b332', 'rp332')}),
	'2',
	'L6 backup stop and manual restore point are retained');

$node->stop;

my $peer_node = PgracClusterNode->new('cluster_backup_peers');
$peer_node->init(allows_streaming => 1);
$peer_node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n");
PostgreSQL::Test::Utils::append_to_file($peer_node->data_dir . '/pgrac.conf', <<'EOC');
[cluster]
name = pgrac-backup-peer-failclosed

[node.0]
interconnect_addr = 127.0.0.1:6432

[node.1]
interconnect_addr = 127.0.0.1:6433
EOC
$peer_node->start;

my ($ret, $out, $err) = $peer_node->psql('postgres',
	"\\set VERBOSITY verbose\nSELECT * FROM pg_cluster_backup_start('partial', true)");
isnt($ret, 0, 'L8 peer topology requires complete backup ACKs');
like($err, qr/53RAD|cluster_backup_incomplete/,
	'L8 missing peer ACK fails closed with cluster_backup_incomplete');
is($peer_node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup}),
	'f',
	'L9 failed peer backup did not leave in-progress state');

$peer_node->stop;

my $bad_target_node = PgracClusterNode->new('cluster_backup_bad_target');
$bad_target_node->init(allows_streaming => 1);
$bad_target_node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n"
	. "cluster.recovery_target_scn = '0'\n");
$bad_target_node->start;

is($bad_target_node->safe_psql('postgres',
	q{SELECT target_type || ',' || target_action || ',' ||
	          CASE WHEN reachable THEN 't' ELSE 'f' END || ',' || reason
	     FROM pg_cluster_pitr_status}),
	'scn,pause,f,invalid_target',
	'L10 invalid SCN PITR target fails closed');

$bad_target_node->stop;

done_testing();
