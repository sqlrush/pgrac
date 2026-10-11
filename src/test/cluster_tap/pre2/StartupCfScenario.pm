# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: real native-start CF contention, with ordinary shutdown on every path.
package StartupCfScenario;
use strict;
use warnings;
use Exporter 'import';
use FindBin;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
our @EXPORT = qw(startup_cf_scenario);

sub startup_cf_scenario
{
	my ($stage) = @_;
	my ($cluster, $error);
	my $passed = eval {
		$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster("pre2_${stage}_cf_wait",
			nodes => 4, blackbox => 1,
			extra_conf => ['autovacuum = off', 'cluster.cf_enqueue_timeout_ms = 1000']);
		$cluster->{driver_argv} = ['python3', "$FindBin::RealBin/../pre2/cf_startup_adapter.py", $stage];
		$cluster->start_cluster;
		my $waits = 0;
		my $consumer = $stage eq 'writer' ? 'startup_(?:select|writer)' : 'startup_checkpoint';
		for my $node ($cluster->nodes)
		{
			my $log = slurp_file($node->logfile);
			$waits += () = $log =~ /PGRAC_FAMILY=CF_OWNER_WAIT consumer=$consumer operation=\w+ mode=\d+ request=[1-9][0-9]* epoch=[1-9][0-9]* caller_cookie=[1-9][0-9]*/g;
		}
		die 'no real original startup owner continued across a CF wait slice' unless $waits;
		diag("startup stage=$stage original owner wait records=$waits");
		$cluster->stop_cluster;
		1;
	};
	$error = $@ unless $passed;
	if (!$passed && $cluster && $cluster->{running})
	{
		my $clean = eval { $cluster->stop_cluster; 1 };
		$error .= "normal cleanup also failed: $@" unless $clean;
	}
	ok($passed, "$stage spans a real CF wait slice and stops normally");
	diag($error) unless $passed;
	done_testing();
}
1;
