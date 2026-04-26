#-------------------------------------------------------------------------
#
# 003_cluster_init_called.pl
#    Verifies the build-time wiring of --enable-cluster:
#    - pg_config reports the option
#    - postgres starts cleanly with cluster code linked in
#    - no FATAL/PANIC from cluster_* code paths in startup log
#
#    Stage 0.5 deliberately does NOT yet hook cluster_init() into
#    PostmasterMain (that lands in stage 0.10+).  This test will
#    grow stronger assertions as those hooks land.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/003_cluster_init_called.pl
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


# Start a single instance and verify build-time integration.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Locate pg_config from the same install used to build this PG.
my $bindir = $node->config_data('--bindir');
my $pg_config_bin = "$bindir/pg_config";

# pg_config --configure should record --enable-cluster in the build flags.
my $configure_args = `"$pg_config_bin" --configure 2>&1`;
like($configure_args,
     qr/--enable-cluster/,
     'pg_config --configure mentions --enable-cluster');

# The startup log must not contain a FATAL/PANIC from cluster code.
my $log_path = $node->logfile;
my $log_content = '';
if (open(my $fh, '<', $log_path))
{
	local $/;
	$log_content = <$fh>;
	close $fh;
}

unlike($log_content,
       qr/cluster_init.*FATAL/i,
       'no cluster_init FATAL in startup log');
unlike($log_content,
       qr/cluster_init.*PANIC/i,
       'no cluster_init PANIC in startup log');
unlike($log_content,
       qr/cluster_shutdown.*FATAL/i,
       'no cluster_shutdown FATAL in startup log');

# Sanity: the server is healthy after start (no hidden cluster failure).
is($node->safe_psql('postgres', 'SELECT 1'),
   '1',
   'server healthy after start with cluster code linked');

$node->stop;

done_testing();
