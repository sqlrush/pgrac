#-------------------------------------------------------------------------
#
# 436_shared_native_parameter_restart_2node.pl
#    A shared cluster cannot change a parameter that its control file
#    records at creation.  ALTER SYSTEM still delivers max_connections as
#    a restart-pending setting; the next start must end with a clear FATAL
#    naming the change and the creation value, never a PANIC.
#
#      L1   ALTER SYSTEM SET max_connections = 300 is delivered as a
#           restart-pending change on both members
#      L2   after a normal stop, the next start is refused and no member
#           reaches readiness
#      L3   the refusal names max_connections=300, its creation value and
#           how to recover; no member log has a PANIC
#
# IDENTIFICATION
#    src/test/cluster_tap/t/436_shared_native_parameter_restart_2node.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $cluster;
my @nodes;

# A failed leg still stops the members it started.
END
{
	if (defined($cluster) && $cluster->{running})
	{
		my $rc = $?;
		eval { $cluster->stop_cluster; 1 } or diag("cleanup stop failed: $@");
		$? = $rc;
	}
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('shared_native_param', nodes => 2,
	blackbox => 1, extra_conf => ['autovacuum = off']);
$cluster->start_cluster;
@nodes = $cluster->nodes;
my $created = $nodes[0]->safe_psql('postgres', 'SHOW max_connections', timeout => 30);
isnt($created, '300', 'the creation value differs from the change');

# L1: the shared configuration accepts and delivers the restart-pending change.
$nodes[0]->safe_psql('postgres', 'ALTER SYSTEM SET max_connections = 300', timeout => 60);
for my $i (0 .. $#nodes)
{
	my $pending = '';
	for (1 .. 300)
	{
		$pending = $nodes[$i]->safe_psql('postgres',
			q{SELECT setting || '|' || pending_restart FROM pg_settings
			  WHERE name = 'max_connections'}, timeout => 30);
		last if $pending eq "$created|t";
		sleep(0.1);
	}
	is($pending, "$created|t", "L1 node$i has max_connections=300 pending a restart");
}
$cluster->stop_cluster;
my @restart_log = map { -s $_->logfile // 0 } @nodes;

# L2: the next start is refused.  The adapter reports the first FATAL; wait
# for every postmaster to leave, and stop one that is still waiting for its
# failed peer.
my $started = eval { $cluster->start_cluster; 1 };
ok(!$started, 'L2 a start with a changed recorded parameter is refused');
$cluster->stop_cluster if $started;
$cluster->{running} = 0;
for (1 .. 600)
{
	last unless grep { -e $_->data_dir . '/postmaster.pid' } @nodes;
	sleep(0.1);
}
for my $i (0 .. $#nodes)
{
	next unless -e $nodes[$i]->data_dir . '/postmaster.pid';
	diag("node$i was still starting after the refusal; stopping it");
	$nodes[$i]->_update_pid(-1);
	$nodes[$i]->stop('immediate', fail_ok => 1);
}

# L3: a clear refusal, never a PANIC, and no member becomes ready.
my $refused = 0;
for my $i (0 .. $#nodes)
{
	my $log = slurp_file($nodes[$i]->logfile, $restart_log[$i]);
	unlike($log, qr/PANIC:/, "L3 node$i has no PANIC");
	unlike($log, qr/database system is ready to accept connections/,
		"L2 node$i does not reach readiness");
	next unless $log =~ /FATAL:.*shared cluster parameters recorded at creation cannot be changed/;
	$refused++;
	like($log,
		qr/PGRAC_REASON=NATIVE_PARAMETER_CHANGE max_connections=300 \(created with \Q$created\E\)\./,
		"L3 node$i names the change and its creation value");
	like($log, qr/HINT:.*Restore the creation values/, "L3 node$i says how to recover");
}
ok($refused >= 1, 'L3 the start ends with the recorded-parameter refusal');

done_testing();
