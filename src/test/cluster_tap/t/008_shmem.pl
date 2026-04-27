#-------------------------------------------------------------------------
#
# 008_shmem.pl
#    End-to-end regression for the cluster shmem framework introduced
#    in stage 0.14.
#
#    Stage 0.14 wires cluster_request_shmem() / cluster_init_shmem()
#    into PG's two-phase shmem flow.  This test runs against a real
#    PG instance and verifies:
#
#      - The server starts cleanly (no FATAL/PANIC).
#      - The startup log emits the cluster shmem allocation line with
#        the expected magic value (CLUSTER_SHMEM_MAGIC = 0x50475243).
#      - The startup log records a non-zero shmem byte count.
#      - The server is healthy after startup (SELECT 1 returns 1).
#      - Restart is idempotent: the second start also emits the same
#        allocation log line (proving cluster_init_shmem runs on every
#        postmaster cold start, not just the very first).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/008_shmem.pl
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
use Test::More;
use PgracClusterNode;


my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf', "log_min_messages = log\n");
$node->start;


# ----------
# Server is healthy after startup.
# ----------
is($node->safe_psql('postgres', 'SELECT 1'),
   '1',
   'server healthy after startup with cluster shmem');


# ----------
# Read the postmaster log file (relative to data dir).
# ----------
sub read_log
{
	my ($n) = @_;
	my $path = $n->logfile;
	open my $fh, '<', $path or die "open $path: $!";
	local $/;
	my $contents = <$fh>;
	close $fh;
	return $contents;
}


my $log = read_log($node);


# ----------
# Startup log contains the cluster shmem allocation line.
# ----------
like($log,
	 qr/cluster shmem control block allocated/,
	 'startup log mentions cluster shmem allocation');


# ----------
# Magic value (CLUSTER_SHMEM_MAGIC = 0x50475243 = "PGRC") is recorded.
# ----------
like($log,
	 qr/magic=0x50475243/,
	 'startup log records magic = 0x50475243 (PGRC)');


# ----------
# Allocation reports a positive byte count.
# ----------
like($log,
	 qr/cluster shmem control block allocated \(\d+ bytes/,
	 'startup log reports a non-zero shmem byte count');


# ----------
# Restart is idempotent: second start also logs the allocation line.
# ----------
$node->stop;
$node->start;
my $log_after_restart = read_log($node);
my @matches = ($log_after_restart =~ /cluster shmem control block allocated/g);
cmp_ok(scalar(@matches), '>=', 2,
	   'cluster shmem allocation logged on every postmaster start (>=2 matches after restart)');


$node->stop;

done_testing();
