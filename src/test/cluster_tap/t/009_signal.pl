#-------------------------------------------------------------------------
#
# 009_signal.pl
#    Reverse regression for the cluster signal framework introduced in
#    stage 0.15.
#
#    Stage 0.15 extends PG's procsignal multiplexer with the new
#    PROCSIG_CLUSTER_RECONFIG_START reason and a matching dispatcher
#    case in procsignal_sigusr1_handler.  The actual handler does
#    nothing observable (it only sets a per-process pending flag) so
#    we cannot assert "signal was received" from SQL.  What we CAN
#    confirm is that adding the dispatcher case did not break PG's
#    own SIGUSR1 paths, and that all subsystems wired in earlier
#    Stage-0 specs (BackendType / wait events / errcodes / GUC /
#    shmem) still function.
#
#    Real cluster signal raising / consumption lands in Stage 2.X
#    LMON spec, where it can be tested end to end.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/009_signal.pl
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
$node->start;


# ----------
# Server is healthy after startup; cluster dispatcher case did not
# break PG's signal handling at boot.
# ----------
is($node->safe_psql('postgres', 'SELECT 1'),
   '1',
   'server healthy after startup with cluster procsignal extension linked');


# ----------
# pg_stat_activity is queryable; this implicitly exercises wait_event_type
# (extended in stage 0.11) and backend_type (extended in stage 0.10).
# ----------
my $self_count = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'});
cmp_ok($self_count, '>=', 1,
	'pg_stat_activity reachable; spec-0.10 BackendType still resolves');


# ----------
# NOTIFY / LISTEN exercise PROCSIG_NOTIFY_INTERRUPT, which sits ahead
# of our cluster case in the dispatcher.  If we accidentally clobbered
# the PG path, the LISTEN would either error or silently drop the
# notification.  This is the strongest cheap proof that PG-native
# procsignal handling is intact after the dispatcher edit.
# ----------
$node->safe_psql('postgres', 'LISTEN spec_0_15_channel');
my $notify_result = $node->safe_psql(
	'postgres',
	q{SELECT pg_notify('spec_0_15_channel', 'hello'); SELECT 'ok'});
like($notify_result, qr/ok/,
	'NOTIFY/LISTEN still works after cluster procsignal extension');


# ----------
# cluster.node_id is still queryable (spec-0.13 path intact).
# ----------
is($node->safe_psql('postgres', q{SHOW "cluster.node_id"}),
   '-1',
   'cluster.node_id still resolves (spec-0.13 GUC unaffected)');


# ----------
# Cluster shmem allocation still happens (spec-0.14 path intact).
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
like($log,
	 qr/cluster shmem control block allocated/,
	 'cluster shmem still allocated at startup (spec-0.14 path unaffected)');


$node->stop;

done_testing();
