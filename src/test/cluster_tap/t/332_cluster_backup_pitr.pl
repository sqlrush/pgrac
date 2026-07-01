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
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PgracClusterNode->new('cluster_backup_single');
$node->init(allows_streaming => 1);
my $archive_dir = $node->archive_dir;
$node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n"
	. "archive_mode = on\n"
	. "archive_command = 'cp %p $archive_dir/%f'\n");
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

is($node->safe_psql('postgres',
	q{SELECT backup_id || ',' ||
	          CASE WHEN start_redo_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN checkpoint_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN start_tli > 0 THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_start('b332', true);
	  SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup;
	  SELECT CASE WHEN consistent_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN stop_cut_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN manifest_crc <> 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN backup_label LIKE '%START WAL LOCATION%' THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_stop(true);
	  SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup;}),
	"b332,t,t,t\nt\nt,t,t,t\nf",
	'L3-L6 cluster backup start/stop succeeds in one native backup session');
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_backup_history}),
	'1',
	'L7 successful cluster backup publishes one manifest summary');

is($node->safe_psql('postgres',
	q{SELECT backup_id || ',' ||
	          CASE WHEN consistent_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN scn_durable_peak >= consistent_scn THEN 't' ELSE 'f' END || ',' ||
	          node_count || ',' || thread_count
	     FROM pg_cluster_backup_history}),
	'b332,t,t,1,1',
	'L8 manifest summary records SCN high-water and single local thread');

is($node->safe_psql('postgres',
	q{SELECT restore_point_name || ',' ||
	          CASE WHEN cut_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN cut_lsn IS NOT NULL THEN 't' ELSE 'f' END
	     FROM pg_cluster_create_restore_point('rp332')}),
	'rp332,t,t',
	'L9 cluster restore point succeeds through commit-drain fence');

is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_restore_points}),
	'2',
	'L10 stop-cut and manual restore points are retained');

my ($nowait_ret, $nowait_out, $nowait_err) = $node->psql('postgres',
	"\\set VERBOSITY verbose\n"
	. "SELECT * FROM pg_cluster_backup_start('b332_nowait', true);\n"
	. "SELECT * FROM pg_cluster_backup_stop(false);");
isnt($nowait_ret, 0,
	'L11 cluster backup stop without archive wait fails closed');
like($nowait_err, qr/cluster backup stop requires durable WAL archive proof/,
	'L11 cluster backup stop reports missing durable WAL archive proof');
is($node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END || ',' || count(*)
	     FROM pg_stat_cluster_backup CROSS JOIN pg_cluster_backup_history
	    GROUP BY in_progress}),
	'f,1',
	'L12 rejected no-wait stop clears in-progress state and keeps manifest history stable');

$node->stop;

my $peer_node = PgracClusterNode->new('cluster_backup_peers');
my $peer_ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $peer_ic1 = PostgreSQL::Test::Cluster::get_free_port();
$peer_node->init(allows_streaming => 1);
$peer_node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n");
PostgreSQL::Test::Utils::append_to_file($peer_node->data_dir . '/pgrac.conf', <<EOC);
[cluster]
name = pgrac-backup-peer-failclosed

[node.0]
interconnect_addr = 127.0.0.1:$peer_ic0

[node.1]
interconnect_addr = 127.0.0.1:$peer_ic1
EOC
$peer_node->start;

my ($ret, $out, $err) = $peer_node->psql('postgres',
	"\\set VERBOSITY verbose\nSELECT * FROM pg_cluster_backup_start('partial', true)");
isnt($ret, 0, 'L13 declared-peer backup remains fail-closed without cross-node proof');
like($err, qr/0A000|feature_not_supported/,
	'L13 declared-peer backup reports feature_not_supported');
is($peer_node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup}),
	'f',
	'L14 failed peer backup did not leave in-progress state');

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
	'L15 invalid SCN PITR target fails closed');

$bad_target_node->stop;

done_testing();
