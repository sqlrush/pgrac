#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 335_cluster_backup_pitr.pl
#    spec-6.5 -- cluster-aware backup / restore / PITR SQL surface.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/335_cluster_backup_pitr.pl
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
use PgracWalState qw(crc32c);
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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

my ($stop_restore_name, $stop_restore_time) = split /\n/,
  $node->safe_psql('postgres',
	q{SELECT restore_point_name
	     FROM pg_cluster_restore_points
	    WHERE restore_point_name = 'cluster_backup_stop_b332';
	  SELECT created_at
	     FROM pg_cluster_restore_points
	    WHERE restore_point_name = 'cluster_backup_stop_b332';});
is($stop_restore_name, 'cluster_backup_stop_b332',
	'L8b backup-stop restore point is visible by name');

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

PostgreSQL::Test::Utils::command_like(
	[
		'pg_cluster_basebackup', '--fast', '--label', 'cli332',
		'-h', $node->host, '-p', $node->port, '-d', 'postgres'
	],
	qr/\Abackup_id=cli332\nstart_redo_lsn=[0-9A-F]+\/[0-9A-F]+\ncheckpoint_lsn=[0-9A-F]+\/[0-9A-F]+\nstart_tli=\d+\nconsistent_scn=\d+\nstop_cut_lsn=[0-9A-F]+\/[0-9A-F]+\nmanifest_crc=\d+\nbackup_set_path=.+\nmanifest_path=.+cluster_backup\.manifest\n\z/,
	'L12a pg_cluster_basebackup CLI completes the proven success path');
my ($cli_backup_set, $cli_manifest_path, $cli_threads) = split /\n/,
  $node->safe_psql('postgres',
	q{SELECT backup_set_path FROM pg_cluster_backup_history WHERE backup_id = 'cli332';
	  SELECT manifest_path FROM pg_cluster_backup_history WHERE backup_id = 'cli332';
	  SELECT thread_count FROM pg_cluster_backup_history WHERE backup_id = 'cli332';});
is($cli_threads, '1', 'L12a CLI backup records one local WAL thread');
ok(-d "$cli_backup_set/data", 'L12a CLI backup captured physical data directory');
ok(-f $cli_manifest_path, 'L12a CLI backup published manifest path');

my $drain_sleeper = $node->background_psql('postgres', on_error_die => 1);
$drain_sleeper->query_safe(
	q{SELECT cluster_inject_fault('cluster-scn-commit-pre-advance', 'sleep', 5000000)});
$drain_sleeper->{stdin} .=
  "INSERT INTO cluster_backup_probe VALUES (90, 'drain-sleeper');\n";
$drain_sleeper->{run}->pump_nb();
usleep(250_000);
my ($drain_ret, $drain_out, $drain_err) = $node->psql('postgres',
	"\\set VERBOSITY verbose\n"
	  . "SET cluster.restore_point_drain_timeout_ms = 1000;\n"
	  . "SELECT * FROM pg_cluster_create_restore_point('rp332_drain_timeout');\n");
isnt($drain_ret, 0,
	'L12b restore-point creation fails closed when commit drain times out');
like($drain_err,
	qr/timed out waiting for cluster restore-point commit drain|53RAF|cluster_restore_point_drain_timeout/,
	'L12b drain timeout reports the restore-point drain failure');
$drain_sleeper->query_safe(
	q{SELECT cluster_inject_fault('cluster-scn-commit-pre-advance', 'none', 0)});
$drain_sleeper->query_safe(q{SELECT 1});
$drain_sleeper->quit;
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM cluster_backup_probe WHERE id = 90}),
	'1',
	'L12b timed-out fence releases after fail-closed and the held commit completes');

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

my $name_restore = PgracClusterNode->new('cluster_backup_name_restore');
PostgreSQL::Test::RecursiveCopy::copypath("$backup_set_path/data", $name_restore->data_dir);
chmod(0700, $name_restore->data_dir);
$name_restore->append_conf('postgresql.conf',
	  "port = " . $name_restore->port . "\n"
	. "unix_socket_directories = '" . $name_restore->host . "'\n"
	. "restore_command = 'cp $archive_dir/%f %p'\n"
	. "cluster.recovery_target_name = '$stop_restore_name'\n"
	. "cluster.recovery_target_action = 'promote'\n");
PostgreSQL::Test::Utils::append_to_file($name_restore->data_dir . '/recovery.signal', '');
$name_restore->start;
is($name_restore->safe_psql('postgres',
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM cluster_backup_probe}),
	'1',
	'L13c offline restore/PITR resolves named cluster restore point');
$name_restore->stop;

my $time_restore = PgracClusterNode->new('cluster_backup_time_restore');
PostgreSQL::Test::RecursiveCopy::copypath("$backup_set_path/data", $time_restore->data_dir);
chmod(0700, $time_restore->data_dir);
$time_restore->append_conf('postgresql.conf',
	  "port = " . $time_restore->port . "\n"
	. "unix_socket_directories = '" . $time_restore->host . "'\n"
	. "restore_command = 'cp $archive_dir/%f %p'\n"
	. "cluster.recovery_target_cluster_time = '$stop_restore_time'\n"
	. "cluster.recovery_target_action = 'promote'\n");
PostgreSQL::Test::Utils::append_to_file($time_restore->data_dir . '/recovery.signal', '');
$time_restore->start;
is($time_restore->safe_psql('postgres',
	q{SELECT string_agg(id::text, ',' ORDER BY id) FROM cluster_backup_probe}),
	'1',
	'L13d offline restore/PITR resolves cluster-time restore point');
$time_restore->stop;

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

my $pair_archive = PostgreSQL::Test::Utils::tempdir();
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'cluster_backup_pair',
	quorum_voting_disks => 3,
	wal_threads_root    => 1,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'wal_level = replica',
		'archive_mode = on',
		'max_prepared_transactions = 10',
		"archive_command = 'cp %p $pair_archive/%f'",
	]);
$pair->start_pair;
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L14 two-node backup setup has node0 connected to node1');

$pair->node0->safe_psql('postgres',
	q{CREATE TABLE cluster_backup_pair_probe(id int, marker int);
	  CREATE TABLE cluster_backup_pair_pending(id int, marker int);
	  CREATE TABLE cluster_backup_pair_delete_probe(id int, marker int);});
$pair->node1->safe_psql('postgres',
	q{CREATE TABLE cluster_backup_pair_probe(id int, marker int);
	  CREATE TABLE cluster_backup_pair_pending(id int, marker int);
	  CREATE TABLE cluster_backup_pair_delete_probe(id int, marker int);});
my $pair_probe_path0 = $pair->node0->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_probe')});
my $pair_probe_path1 = $pair->node1->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_probe')});
my $pair_pending_path0 = $pair->node0->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_pending')});
my $pair_pending_path1 = $pair->node1->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_pending')});
my $pair_delete_path0 = $pair->node0->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_delete_probe')});
my $pair_delete_path1 = $pair->node1->safe_psql('postgres',
	q{SELECT pg_relation_filepath('cluster_backup_pair_delete_probe')});
if ($pair_probe_path0 ne $pair_probe_path1)
{
	$pair->stop_pair;
	BAIL_OUT("same-DDL shared-data relation path mismatch: "
	  . "node0=$pair_probe_path0 node1=$pair_probe_path1");
}
if ($pair_pending_path0 ne $pair_pending_path1)
{
	$pair->stop_pair;
	BAIL_OUT("same-DDL shared-data pending relation path mismatch: "
	  . "node0=$pair_pending_path0 node1=$pair_pending_path1");
}
if ($pair_delete_path0 ne $pair_delete_path1)
{
	$pair->stop_pair;
	BAIL_OUT("same-DDL shared-data delete relation path mismatch: "
	  . "node0=$pair_delete_path0 node1=$pair_delete_path1");
}
is($pair_probe_path1, $pair_probe_path0,
	'L15 same-DDL pair probe uses one shared-data relation path');
is($pair_pending_path1, $pair_pending_path0,
	'L15 same-DDL pair pending relation uses one shared-data relation path');
is($pair_delete_path1, $pair_delete_path0,
	'L15 same-DDL pair prepared-delete relation uses one shared-data relation path');

$pair->node0->safe_psql('postgres',
	q{INSERT INTO cluster_backup_pair_probe VALUES (1, 10);
	  INSERT INTO cluster_backup_pair_delete_probe VALUES (4, 40);});

my $pair_backup_session = $pair->node0->background_psql('postgres', on_error_die => 1);
is($pair_backup_session->query_safe(
	q{SELECT backup_id || ',' ||
	          CASE WHEN start_redo_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN checkpoint_lsn IS NOT NULL THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_start('pair332', true);}),
	"pair332,t,t",
	'L15a two-node cluster backup start succeeds through peer IC coordination');
my ($pair_concurrent_ret, $pair_concurrent_out, $pair_concurrent_err) =
  $pair->node0->psql('postgres',
	"\\set VERBOSITY verbose\n"
	  . "SELECT * FROM pg_cluster_backup_start('pair332_concurrent', true);");
isnt($pair_concurrent_ret, 0,
	'L15a concurrent cluster backup start fails closed while backup is in progress');
like($pair_concurrent_err,
	qr/a cluster backup is already in progress|cluster_backup_in_progress|53RAB/,
	'L15a concurrent backup reports cluster_backup_in_progress');

$pair->node1->safe_psql('postgres',
	q{BEGIN;
	  INSERT INTO cluster_backup_pair_probe VALUES (2, 20);
	  PREPARE TRANSACTION 'pair332_before_cut';
	  COMMIT PREPARED 'pair332_before_cut';
	  SELECT pg_switch_wal();});
is($pair->node1->safe_psql('postgres',
	q{SELECT count(*) FROM cluster_backup_pair_probe WHERE id = 2}),
	'1',
	'L15b peer COMMIT PREPARED before the backup-stop cut is durable');

$pair->node1->safe_psql('postgres',
	q{BEGIN;
	  INSERT INTO cluster_backup_pair_pending VALUES (3, 30);
	  PREPARE TRANSACTION 'pair332_after_cut';});
is($pair->node1->safe_psql('postgres',
	q{SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'pair332_after_cut'}),
	'1',
	'L15c peer 2PC is prepared before the backup-stop cut');

$pair->node1->safe_psql('postgres',
	q{BEGIN;
	  DELETE FROM cluster_backup_pair_delete_probe WHERE id = 4;
	  PREPARE TRANSACTION 'pair332_delete_after_cut';});
is($pair->node1->safe_psql('postgres',
	q{SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'pair332_delete_after_cut'}),
	'1',
	'L15c peer prepared DELETE is in-doubt before the backup-stop cut');

my ($pair_stop_out, $pair_stop_had_stderr) = $pair_backup_session->query(
	q{SELECT CASE WHEN consistent_scn > 0 THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN stop_cut_lsn IS NOT NULL THEN 't' ELSE 'f' END || ',' ||
	          CASE WHEN manifest_crc <> 0 THEN 't' ELSE 'f' END
	     FROM pg_cluster_backup_stop(true);});
die "unexpected pg_cluster_backup_stop stderr: $pair_backup_session->{stderr}"
  if $pair_stop_had_stderr
  && $pair_backup_session->{stderr} !~ /all required WAL segments have been archived/;
is($pair_stop_out,
	"t,t,t",
	'L15d two-node cluster backup stop cuts while peer 2PC is only prepared');
$pair_backup_session->quit;

my (
	$pair_backup_set, $pair_threads, $pair_nodes, $pair_consistent_scn,
	$pair_scn_durable_peak, $pair_timeline
) = split /\n/,
  $pair->node0->safe_psql('postgres',
	q{SELECT backup_set_path FROM pg_cluster_backup_history;
	  SELECT thread_count FROM pg_cluster_backup_history;
	  SELECT node_count FROM pg_cluster_backup_history;
	  SELECT consistent_scn FROM pg_cluster_backup_history;
	  SELECT scn_durable_peak FROM pg_cluster_backup_history;
	  SELECT timeline FROM pg_cluster_backup_history;});
is("$pair_threads,$pair_nodes", '2,2',
	'L16 two-node manifest records both WAL threads and nodes');
ok(-d "$pair_backup_set/thread_1", 'L17 coordinator WAL thread was captured');
ok(-d "$pair_backup_set/thread_2", 'L17 peer WAL thread was captured');
ok(-d "$pair_backup_set/node_0_pg_undo", 'L17 coordinator undo region was captured');
ok(-d "$pair_backup_set/node_1_pg_undo", 'L17 peer undo region was captured');
ok(-f "$pair_backup_set/node_1_tt.proof", 'L17 peer durable TT evidence was captured');
ok(-d "$pair_backup_set/shared_data", 'L17 shared-data region was captured');

$pair->node1->safe_psql('postgres',
	q{COMMIT PREPARED 'pair332_after_cut';
	  COMMIT PREPARED 'pair332_delete_after_cut';
	  SELECT pg_switch_wal();});
my $pair_after_2pc_scn = $pair->node1->safe_psql('postgres',
	q{SELECT cluster_scn_current()});
ok($pair_after_2pc_scn > $pair_consistent_scn,
	'L17a peer COMMIT PREPARED advances SCN above the backup cut');

$pair->stop_pair;

my $pair_restore_shared_parent = PostgreSQL::Test::Utils::tempdir();
my $pair_restore_shared = "$pair_restore_shared_parent/shared_data";
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/shared_data",
	$pair_restore_shared);

sub configure_pair_restore
{
	my ($restore_node, $shared_dir, $target_scn) = @_;
	my $restore_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $restore_ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $target_conf = defined($target_scn)
	  ? "cluster.recovery_target_scn = '$target_scn'\n"
	  : "";

	unlink $restore_node->data_dir . '/pgrac.conf';
	PostgreSQL::Test::Utils::append_to_file($restore_node->data_dir . '/pgrac.conf',
	<<EOC);
[cluster]
name = cluster_backup_pair_restore

[node.0]
interconnect_addr = 127.0.0.1:$restore_ic0

[node.1]
interconnect_addr = 127.0.0.1:$restore_ic1
EOC
	$restore_node->append_conf('postgresql.conf',
	  "port = " . $restore_node->port . "\n"
	. "unix_socket_directories = '" . $restore_node->host . "'\n"
	. "restore_command = 'cp $pair_archive/%f %p'\n"
	. "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "cluster.voting_disks = ''\n"
	. "cluster.wal_threads_dir = ''\n"
	. "cluster.shared_storage_backend = cluster_fs\n"
	. "cluster.shared_data_dir = '$shared_dir'\n"
	. "cluster.smgr_user_relations = on\n"
	. "max_prepared_transactions = 10\n"
	. "cluster.pcm_grd_max_entries = 0\n"
	. $target_conf
	. "cluster.recovery_target_action = 'promote'\n");
	PostgreSQL::Test::Utils::append_to_file($restore_node->data_dir . '/recovery.signal', '');
}

my $pair_restore = PgracClusterNode->new('cluster_backup_pair_restore');
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/data",
	$pair_restore->data_dir);
chmod(0700, $pair_restore->data_dir);
configure_pair_restore($pair_restore, $pair_restore_shared, $pair_consistent_scn);
$pair_restore->start;
is($pair_restore->safe_psql('postgres',
	q{SELECT string_agg(id::text || ':' || marker::text, ',' ORDER BY id)
	     FROM cluster_backup_pair_probe}),
	'1:10,2:20',
	'L18 two-node backup->restore->PITR reads the manifest-consistent cut');
is($pair_restore->safe_psql('postgres',
	qq{SELECT CASE WHEN cluster_scn_current() >= $pair_scn_durable_peak THEN 't' ELSE 'f' END}),
	't',
	'L18b PITR open restores cluster SCN at or above the manifest high-water');
my $pair_restore_walfile = $pair_restore->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});
my $pair_restore_tli = hex(substr($pair_restore_walfile, 0, 8));
ok($pair_restore_tli > $pair_timeline,
	'L18c PITR open switches to a new RESETLOGS timeline');
my ($pending_ret, $pending_out, $pending_err) = $pair_restore->psql('postgres',
	q{SELECT count(*) FROM cluster_backup_pair_pending WHERE id = 3});
isnt($pending_ret, 0,
	'L18a unresolved foreign PREPARE fails closed instead of becoming ABORTED');
like($pending_err,
	qr/cluster TT status unknown|53R97|in-doubt/,
	'L18a unresolved foreign PREPARE reports an in-doubt visibility outcome');
is($pair_restore->safe_psql('postgres',
	q{SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'pair332_after_cut'}),
	'0',
	'L18a foreign peer 2PC is not exposed as a local prepared xact after PITR');
my ($pending_delete_ret, $pending_delete_out, $pending_delete_err) =
  $pair_restore->psql('postgres',
	q{SELECT count(*) FROM cluster_backup_pair_delete_probe WHERE id = 4});
isnt($pending_delete_ret, 0,
	'L18g unresolved foreign prepared DELETE fails closed instead of making a row visible');
like($pending_delete_err,
	qr/cluster TT status unknown|53R97|in-doubt|cluster TT slot recycled|fresh snapshot/,
	'L18g unresolved prepared DELETE reports a fail-closed visibility outcome');
like(slurp_file($pair_restore->logfile),
	qr/redo done \(cluster backup restore\)/,
	'L18 two-node restore drove the multi-thread restore-mode merge');
$pair_restore->stop;

my $pair_latest_shared_parent = PostgreSQL::Test::Utils::tempdir();
my $pair_latest_shared = "$pair_latest_shared_parent/shared_data";
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/shared_data",
	$pair_latest_shared);
my $pair_latest_restore = PgracClusterNode->new('cluster_backup_pair_latest');
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/data",
	$pair_latest_restore->data_dir);
chmod(0700, $pair_latest_restore->data_dir);
configure_pair_restore($pair_latest_restore, $pair_latest_shared, undef);
$pair_latest_restore->start;
is($pair_latest_restore->safe_psql('postgres',
	q{SELECT string_agg(id::text || ':' || marker::text, ',' ORDER BY id)
	     FROM cluster_backup_pair_probe}),
	'1:10,2:20',
	'L18d two-node restore with no explicit target opens at the backup-set terminus');
is($pair_latest_restore->safe_psql('postgres',
	qq{SELECT CASE WHEN cluster_scn_current() >= $pair_scn_durable_peak THEN 't' ELSE 'f' END}),
	't',
	'L18e no-target restore carries the manifest SCN high-water');
my $pair_latest_walfile = $pair_latest_restore->safe_psql('postgres',
	q{SELECT pg_walfile_name(pg_current_wal_lsn())});
my $pair_latest_tli = hex(substr($pair_latest_walfile, 0, 8));
ok($pair_latest_tli > $pair_timeline,
	'L18f no-target restore opens on a new RESETLOGS timeline');
$pair_latest_restore->stop;

ok($pair_consistent_scn > 1,
	'L19 two-node backup consistent SCN can be decremented for PITR floor test');
my $pair_too_early_scn = $pair_consistent_scn - 1;
my $pair_too_early_shared_parent = PostgreSQL::Test::Utils::tempdir();
my $pair_too_early_shared = "$pair_too_early_shared_parent/shared_data";
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/shared_data",
	$pair_too_early_shared);
my $pair_too_early_restore = PgracClusterNode->new('cluster_backup_pair_too_early');
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/data",
	$pair_too_early_restore->data_dir);
chmod(0700, $pair_too_early_restore->data_dir);
configure_pair_restore($pair_too_early_restore, $pair_too_early_shared,
	$pair_too_early_scn);
is($pair_too_early_restore->start(fail_ok => 1), 0,
	'L19 two-node PITR target before backup consistent SCN fails closed');
like(slurp_file($pair_too_early_restore->logfile),
	qr/cluster PITR target SCN is before the backup consistent SCN|cluster_pitr_target_unreachable/,
	'L19 two-node PITR floor failure is reported before open');

ok(rename("$pair_backup_set/thread_2", "$pair_backup_set/thread_2.missing"),
	'L20 injection removed peer WAL thread from the backup set');
my $pair_missing_thread_shared_parent = PostgreSQL::Test::Utils::tempdir();
my $pair_missing_thread_shared = "$pair_missing_thread_shared_parent/shared_data";
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/shared_data",
	$pair_missing_thread_shared);
my $pair_missing_thread_restore = PgracClusterNode->new('cluster_backup_pair_missing_thread');
PostgreSQL::Test::RecursiveCopy::copypath("$pair_backup_set/data",
	$pair_missing_thread_restore->data_dir);
chmod(0700, $pair_missing_thread_restore->data_dir);
configure_pair_restore($pair_missing_thread_restore, $pair_missing_thread_shared,
	$pair_consistent_scn);
is($pair_missing_thread_restore->start(fail_ok => 1), 0,
	'L20 two-node restore fails closed when a peer WAL thread is missing');
like(slurp_file($pair_missing_thread_restore->logfile),
	qr/cluster backup restore: thread 2 WAL does not reach the manifest cut|cluster_backup_incomplete/,
	'L20 missing peer WAL thread is reported as incomplete cluster backup');

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
	'L21 invalid SCN PITR target fails closed');

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
	'L22 multiple cluster PITR targets fail closed');

$multi_target_node->stop;

my $pin_node = PgracClusterNode->new('cluster_backup_pin_crash');
$pin_node->init(allows_streaming => 1);
my $pin_archive = $pin_node->archive_dir;
$pin_node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = on\n"
	. "wal_level = replica\n"
	. "archive_mode = on\n"
	. "archive_command = 'cp %p $pin_archive/%f'\n");
$pin_node->start;
my $pin_session = $pin_node->background_psql('postgres', on_error_die => 1);
is($pin_session->query_safe(
	q{SELECT backup_id FROM pg_cluster_backup_start('pin335', true);}),
	'pin335',
	'L23 crash-pin fixture starts a cluster backup in a live session');
my $pin_path = $pin_node->data_dir . '/global/pgrac_cluster_backup.pin';
ok(-f $pin_path,
	'L23 durable WAL pin is published while cluster backup is in progress');
my $pin_raw = slurp_file($pin_path);
my ($pin_magic, $pin_version, $pin_lsn, $pin_backup_id, $pin_crc) =
  unpack('L<L<Q<Z64L<', substr($pin_raw, 0, 84));
is($pin_magic, 0x50475049, 'L23 durable WAL pin carries the expected magic');
is($pin_version, 1, 'L23 durable WAL pin carries the expected version');
is($pin_backup_id, 'pin335', 'L23 durable WAL pin records the backup id');
ok($pin_lsn > 0, 'L23 durable WAL pin records a nonzero start REDO LSN');
is($pin_crc, crc32c(substr($pin_raw, 0, 80)),
	'L23 durable WAL pin CRC protects the published retention record');
$pin_node->stop('immediate');
ok(-f $pin_path,
	'L23 durable WAL pin survives node crash during in-progress backup');
$pin_node->start;
$pin_node->safe_psql('postgres', q{CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT;});
unlike(slurp_file($pin_node->logfile),
	qr/cluster backup WAL pin is invalid|cluster backup WAL pin is truncated/,
	'L23 restarted node accepts the crash-surviving durable WAL pin in the recycle gate');
$pin_node->stop;
eval { $pin_session->quit; };

done_testing();
