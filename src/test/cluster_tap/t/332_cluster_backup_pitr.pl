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
use IO::Socket::INET;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $next_high_port = $ENV{PGRAC_BACKUP_TAP_PORT_BASE} // 60432;

sub next_free_high_port
{
	for (1 .. 256)
	{
		my $port = $next_high_port++;
		my $sock = IO::Socket::INET->new(
			Listen    => 5,
			LocalAddr => '127.0.0.1',
			LocalPort => $port,
			Proto     => 'tcp',
			ReuseAddr => 1);
		if ($sock)
		{
			close $sock;
			return $port;
		}
	}
	die "could not find a free high TCP port for cluster backup TAP";
}

my $node = PgracClusterNode->new('cluster_backup_single',
	port => next_free_high_port());
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

my ($backup_ret, $backup_out, $backup_err) = $node->psql('postgres',
	"\\set VERBOSITY verbose\nSELECT * FROM pg_cluster_backup_start('b332', true)");
isnt($backup_ret, 0,
	'L3 cluster backup start fails closed until physical capture lands');
like($backup_err, qr/0A000|feature_not_supported/,
	'L3 cluster backup start reports feature_not_supported');
like($backup_err, qr/physical backup capture|durable WAL pinning|restore integration/,
	'L3 cluster backup start names the missing substrate');

is($node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup}),
	'f',
	'L4 rejected cluster backup does not leave in-progress state');
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_backup_history}),
	'0',
	'L4 rejected cluster backup does not publish a manifest');

my ($rp_ret, $rp_out, $rp_err) = $node->psql('postgres',
	"\\set VERBOSITY verbose\nSELECT * FROM pg_cluster_create_restore_point('rp332')");
isnt($rp_ret, 0,
	'L5 cluster restore point fails closed until commit-drain lands');
like($rp_err, qr/0A000|feature_not_supported/,
	'L5 cluster restore point reports feature_not_supported');
like($rp_err, qr/restore-point commit-drain barrier/,
	'L5 cluster restore point names the missing barrier');

is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_restore_points}),
	'0',
	'L6 rejected restore point is not retained');

$node->stop;

my $peer_node = PgracClusterNode->new('cluster_backup_peers',
	port => next_free_high_port());
my $peer_ic0 = next_free_high_port();
my $peer_ic1 = next_free_high_port();
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
isnt($ret, 0, 'L8 declared-peer backup remains fail-closed without capture substrate');
like($err, qr/0A000|feature_not_supported/,
	'L8 declared-peer backup reports feature_not_supported');
is($peer_node->safe_psql('postgres',
	q{SELECT CASE WHEN in_progress THEN 't' ELSE 'f' END
	     FROM pg_stat_cluster_backup}),
	'f',
	'L9 failed peer backup did not leave in-progress state');

$peer_node->stop;

my $bad_target_node = PgracClusterNode->new('cluster_backup_bad_target',
	port => next_free_high_port());
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
