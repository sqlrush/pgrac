#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# Requires A S11/S12/S16 plus certified terminal-I/O failure and two real
# recoverers on nodes 0/1. Named cuts are adapter operations, not fake DONE.
# RECOVERY_COMPLETE must exclude every executor and deny stale DATA writes.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;

run_scenario('pre2_two_recoverers', [qw(recovery_arm recovery_crash_and_fence
	recovery_wait recovery_release recovery_observation)], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	sql($nodes[0], 'CREATE TABLE pre2_recover (id int PRIMARY KEY, v int NOT NULL)');
	sql($nodes[3], 'BEGIN; INSERT INTO pre2_recover VALUES (1, 41); COMMIT');
	my @writers = map { my $w = $_; +{map { $_ => $w->{$_} }
		qw(node thread incarnation wal_generation boot)} } @{$cluster->{writers}};
	my $identities = {system_identifier => $cluster->{system_identifier},
		victim_writer => $writers[3], recoverer_writers => [@writers[0,1]]};
	my $request = {relation => 'pre2_recover', victim => 3, recoverers => [0, 1], identities => $identities};
	$cluster->observe('recovery_arm', $request);
	my $failure = $cluster->observe('recovery_crash_and_fence', $request);
		require_true(($failure->{victim} // -1) == 3 && $failure->{terminal_proof}
		&& $failure->{duty} && $failure->{token}
		&& ($failure->{system_identifier} // '') eq $cluster->{system_identifier},
		'missing exact victim terminal-I/O and recovery-window proof');
	require_identity($failure->{identities}, $identities, 'failure receipt is for the actual current writers');
	$cluster->{cleanup_nodes} = [0, 1, 2];
	my $scope = {%$request, duty => $failure->{duty}, token => $failure->{token},
		terminal_proof => $failure->{terminal_proof}};
	# Node 0 finished DATA and released IR, but node 1 still owns WALR-S.
	my $cut = $cluster->observe('recovery_wait', {%$scope, cut => 'first_ir_released_peer_s'});
	require_equal($cut->{cut}, 'first_ir_released_peer_s', 'real first recoverer cut');
	require_equal($cut->{duty}, $failure->{duty}, 'cut duty identity');
	require_equal($cut->{token}, $failure->{token}, 'cut window identity');
	$cluster->observe('recovery_release', {%$scope, cut => 'first_completion_attempt'});
	$cut = $cluster->observe('recovery_wait', {%$scope, cut => 'completion_deferred_peer_data'});
	require_equal($cut->{cut}, 'completion_deferred_peer_data', 'competing recoverer performed DATA');
	require_equal($cut->{duty}, $failure->{duty}, 'competing cut duty');
	require_equal($cut->{token}, $failure->{token}, 'competing cut token');
	$cluster->observe('recovery_release', {%$scope, cut => 'peer_finish_then_finalizer'});
	# Observe through executor retirement AND a rejected old-window write probe.
	my $trace = $cluster->observe('recovery_observation', $scope);
	$cluster->check_evidence('recovery', $trace, $scope->{duty}, $scope->{token},
		$scope->{terminal_proof}, [0, 1], 3, $identities);
	for my $i (0 .. 2)
	{
		require_equal(sql($nodes[$i], 'SELECT v FROM pre2_recover WHERE id = 1'),
			'41', 'committed row survived both recovery executors');
	}
	$cluster->stop_cluster(nodes => [0, 1, 2]);
});
