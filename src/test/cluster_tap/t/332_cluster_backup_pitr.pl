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
use PostgreSQL::Test::RecursiveCopy;
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

$node->safe_psql('postgres',
	q{CREATE TABLE cluster_backup_probe(id int PRIMARY KEY, note text);
	  INSERT INTO cluster_backup_probe VALUES (1, 'before-backup');});

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
	          CASE WHEN backup_label LIKE '%START WAL LOCATION%' THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN backup_label LIKE '%CLUSTER_BACKUP_ID: b332%' THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN backup_label LIKE '%THREAD_01_START_REDO_LSN%' THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_stop(true);
	  SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup;}),
	"b332,t,t,t\nt\nt,t,t,t,t,t\nf",
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

my ($backup_set_path, $manifest_path, $consistent_scn) = split /\n/,
  $node->safe_psql('postgres',
	q{SELECT backup_set_path FROM pg_cluster_backup_history;
	  SELECT manifest_path FROM pg_cluster_backup_history;
	  SELECT consistent_scn FROM pg_cluster_backup_history;});
ok(-d $backup_set_path, 'L8a backup set directory exists');
ok(-f $manifest_path, 'L8a binary manifest exists at recorded path');
ok(-d "$backup_set_path/data", 'L8a physical data directory was captured');
ok(-f "$backup_set_path/data/backup_label", 'L8a backup_label was written into captured data');
ok(-f "$backup_set_path/data/cluster_backup.manifest",
	'L8a manifest copy was written into captured data');
ok(-d "$backup_set_path/data/pg_wal", 'L8a required WAL directory was captured');
ok(-d "$backup_set_path/control", 'L8a control metadata directory was captured');
ok(-d "$backup_set_path/voting", 'L8a voting metadata evidence was captured');
ok(-f "$backup_set_path/node_0_tt.proof", 'L8a durable TT evidence was captured');
like(slurp_file("$backup_set_path/data/backup_label"),
	qr/CLUSTER_RESTORE_POINT_NAME: cluster_backup_stop_b332/,
	'L8a backup_label carries cluster restore-point metadata');

$node->safe_psql('postgres',
	q{INSERT INTO cluster_backup_probe VALUES (2, 'after-backup');
	  SELECT pg_switch_wal();});

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

my $restore = PgracClusterNode->new('cluster_backup_restore');
PostgreSQL::Test::RecursiveCopy::copypath("$backup_set_path/data", $restore->data_dir);
chmod(0700, $restore->data_dir);
$restore->append_conf('postgresql.conf',
	  "port = " . $restore->port . "\n"
	. "unix_socket_directories = '" . $restore->host . "'\n"
	. "restore_command = 'cp $archive_dir/%f %p'\n"
	. "cluster.recovery_target_scn = '$consistent_scn'\n"
	. "cluster.recovery_target_action = 'promote'\n");
PostgreSQL::Test::Utils::append_to_file($restore->data_dir . '/recovery.signal', '');
$restore->start;
is($restore->safe_psql('postgres',
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM cluster_backup_probe}),
	'1',
	'L13 offline restore/PITR replays to the cluster backup-stop restore point');
$restore->stop;

my $bad_manifest_restore = PgracClusterNode->new('cluster_backup_bad_manifest_restore');
PostgreSQL::Test::RecursiveCopy::copypath("$backup_set_path/data",
	$bad_manifest_restore->data_dir);
chmod(0700, $bad_manifest_restore->data_dir);
open my $bad_manifest_fh, '>', $bad_manifest_restore->data_dir . '/cluster_backup.manifest'
  or die "could not corrupt cluster backup manifest: $!";
print $bad_manifest_fh "bad manifest\n";
close $bad_manifest_fh;
$bad_manifest_restore->append_conf('postgresql.conf',
	  "port = " . $bad_manifest_restore->port . "\n"
	. "unix_socket_directories = '" . $bad_manifest_restore->host . "'\n"
	. "restore_command = 'cp $archive_dir/%f %p'\n"
	. "cluster.recovery_target_scn = '$consistent_scn'\n"
	. "cluster.recovery_target_action = 'promote'\n");
PostgreSQL::Test::Utils::append_to_file(
	$bad_manifest_restore->data_dir . '/recovery.signal', '');
is($bad_manifest_restore->start(fail_ok => 1), 0,
	'L13b corrupt manifest restore fails closed before open');
like(slurp_file($bad_manifest_restore->logfile),
	qr/cluster backup manifest is invalid|cluster_backup_incomplete/,
	'L13b corrupt manifest reports cluster backup incomplete');

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
isnt($ret, 0, 'L14 declared-peer backup remains fail-closed without cross-node proof');
like($err, qr/0A000|feature_not_supported/,
	'L14 declared-peer backup reports feature_not_supported');
is($peer_node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup}),
	'f',
	'L15 failed peer backup did not leave in-progress state');

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
	'L16 invalid SCN PITR target fails closed');

$bad_target_node->stop;

my $multi_target_node = PgracClusterNode->new('cluster_backup_multi_target');
$multi_target_node->init(allows_streaming => 1);
$multi_target_node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n"
	. "cluster.recovery_target_scn = '1'\n"
	. "cluster.recovery_target_name = 'rp332'\n");
$multi_target_node->start;

is($multi_target_node->safe_psql('postgres',
	q{SELECT target_type || ',' || target_action || ',' ||
	          CASE WHEN reachable THEN 't' ELSE 'f' END || ',' || reason
	     FROM pg_cluster_pitr_status}),
	'multiple,pause,f,multiple_targets',
	'L17 multiple cluster PITR targets fail closed');

$multi_target_node->stop;

done_testing();
