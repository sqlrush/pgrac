# Common failure handling for the PRE2 black-box scenarios. No skip paths.
package Scenario;
use strict;
use warnings;
use Exporter 'import';
use PostgreSQL::Test::ClusterPRE2;
use Test::More;
use JSON::PP;
our @EXPORT = qw(run_scenario require_equal require_true require_identity sql);

sub sql { return $_[0]->safe_psql('postgres', $_[1], timeout => 30); }

sub require_equal
{
	my ($actual, $expected, $label) = @_;
	die "$label: expected [$expected], got [" . ($actual // '<missing>') . "]\n"
	  unless defined($actual) && $actual eq $expected;
}
sub require_true { die "$_[1]\n" unless $_[0]; }
sub require_identity
{
	my ($actual, $expected, $label) = @_;
	my $json = JSON::PP->new->canonical;
	require_equal($json->encode($actual), $json->encode($expected), $label);
}

sub run_scenario
{
	my ($name, $operations, $body, $extra_conf) = @_;
	my ($cluster, $error);
	my $passed = eval {
		$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster($name, nodes => 4,
			blackbox => 1, required_operations => $operations,
			extra_conf => ['autovacuum = off', @{$extra_conf // []}]);
		$cluster->start_cluster;
		$body->($cluster);
		die 'scenario left a live cluster' if $cluster->{running};
		1;
	};
	$error = $@ unless $passed;
	if (!$passed && $cluster && $cluster->{running})
	{
		my $clean = eval { $cluster->stop_cluster(nodes => $cluster->{cleanup_nodes}); 1 };
		$error .= "normal cleanup also failed: $@" unless $clean;
	}
	# Missing A dependencies are failing TAP assertions, never TAP skips.
	ok($passed, $name);
	diag($error) unless $passed;
	done_testing();
}
1;
