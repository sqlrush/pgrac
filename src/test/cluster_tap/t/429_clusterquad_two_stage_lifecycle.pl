#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 429_clusterquad_two_stage_lifecycle.pl
#    Focused lifecycle checks for the Stage 8 two-stage voting harness.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Digest::SHA qw(sha256_hex);
use File::Temp qw(tempdir);
use JSON::PP ();
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::ClusterVotingDisk qw(format_voting_file);
use Scalar::Util qw(refaddr);
use Test::More;

{
	package ClusterQuadLifecycleTestNode;

	sub new
	{
		my ($class, $pid) = @_;
		return bless { _pid => $pid, cleanup_calls => 0 }, $class;
	}

	sub _update_pid
	{
		my ($self, $pid) = @_;
		$self->{_pid} = $pid;
	}

	sub teardown_node
	{
		my ($self, %opts) = @_;
		$self->{cleanup_calls}++;
		die "blocking teardown_node is forbidden in two-stage cleanup\n"
		  if $self->{forbid_blocking_teardown};
		$self->{_pid} = 0;
		return;
	}

	sub logfile
	{
		return $_[0]->{logfile};
	}

	sub safe_psql
	{
		my ($self, $database, $query) = @_;
		return $self->{formation_row}
		  if index($query, 'current_epoch_at_boot') >= 0;
		return $self->{cf_ges_readiness};
	}

	sub _get_env
	{
		return ();
	}

	sub installed_command
	{
		return 'pg_ctl';
	}

	sub data_dir
	{
		return '/tmp/focused-node-' . $_[0]->{_pid};
	}

	sub name
	{
		return 'focused-node-' . $_[0]->{_pid};
	}
}

{
	package ClusterQuadLifecycleTestHandle;

	sub pid
	{
		return $_[0]->{pid};
	}

	sub result
	{
		return $_[0]->{result};
	}
}

{
	package ClusterQuadLifecycleTestOrchestrator;

	our @ISA = qw(PostgreSQL::Test::ClusterQuad);

	sub new
	{
		return bless { events => [], nodes => [] }, $_[0];
	}

	sub _two_stage_register_cleanup_owner
	{
		my ($self) = @_;
		$self->{two_stage_cleanup_registered} = 1;
		push @{ $self->{events} }, 'owner';
	}

	sub _two_stage_reap_previous_manifests
	{
		push @{ $_[0]->{events} }, 'reap';
		return 'CLEAN';
	}

	sub _two_stage_start_nodes
	{
		my ($self, $while_starting) = @_;
		die 'start preceded cleanup owner'
		  unless $self->{two_stage_cleanup_registered};
		$self->{start_count}++;
		push @{ $self->{events} }, 'start' . $self->{start_count} . ':launch';
		$while_starting->() if defined($while_starting);
		push @{ $self->{events} }, 'start' . $self->{start_count} . ':finish';
	}

	sub _two_stage_wait_for_clean_formation
	{
		my ($self, $phase) = @_;
		push @{ $self->{events} }, "formation:$phase";
	}

	sub _two_stage_wait_phase2_gate_ladder
	{
		push @{ $_[0]->{events} }, 'gate-ladder';
	}

	sub _two_stage_coordinated_clean_stop
	{
		push @{ $_[0]->{events} }, 'stop';
	}

	sub _two_stage_attach_voting_loops
	{
		push @{ $_[0]->{events} }, 'attach';
	}

	sub _two_stage_qualify_block_backend
	{
		my ($self) = @_;
		push @{ $self->{events} }, 'qualify';
		die "forced block qualification failure\n"
		  if $self->{qualify_failure};
	}

	sub _two_stage_install_device_only_voting_config
	{
		push @{ $_[0]->{events} }, 'device-config';
	}

	sub _two_stage_capture_phase2_log_offsets
	{
		push @{ $_[0]->{events} }, 'phase2-log-boundary';
	}
}

{
	my $dir = tempdir(CLEANUP => 1);
	my $path = "$dir/voting0";
	my $current_map_bytes = (8 * 128 + 3) * 512;

	format_voting_file($path, 0);
	is(-s $path, $current_map_bytes,
		'voting formatter allocates the complete current region-7/PGRD map');
	is(PostgreSQL::Test::ClusterQuad::TWO_STAGE_VOTING_BYTES(),
		$current_map_bytes,
		'two-stage loop attestation uses the complete current voting map');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my @nodes = map { ClusterQuadLifecycleTestNode->new(1000 + $_) } 0 .. 3;
	$_->{forbid_blocking_teardown} = 1 for @nodes;
	my $quad = bless {
		nodes => \@nodes,
		voting_disk_paths => [qw(/tmp/vote0 /tmp/vote1 /tmp/vote2)],
		two_stage_loop_devices => [],
	}, 'PostgreSQL::Test::ClusterQuad';
	my $registered = eval {
		$quad->_two_stage_register_cleanup_owner();
		$quad->_two_stage_register_cleanup_owner();
		1;
	};

	ok($registered,
		'cleanup owner can be armed before the first Phase-1 start');
	is(scalar(@PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS), 1,
		'cleanup owner registration is idempotent');
	is(refaddr($PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS[0]),
		refaddr($quad), 'cleanup owner records the exact quad');

	my $cleaned = eval { $quad->_two_stage_cleanup_registered(); 1 };
	ok($cleaned,
		'registered early-failure quad is cleaned with an empty loop set');
	is_deeply([ map { $_->{cleanup_calls} } @nodes ], [ 0, 0, 0, 0 ],
		'early-failure cleanup never enters blocking generic node teardown');
	is_deeply($quad->{two_stage_loop_devices}, [],
		'early-failure cleanup leaves the empty device set exact');
}

{
	my $quad = bless {
		two_stage_loop_devices =>
			[ qw(/dev/loop-test-a /dev/loop-test-b /dev/loop-test-c) ],
		two_stage_loop_records => [
			{ path => '/dev/loop-test-a', backing_realpath => '/tmp/test-a' },
			{ path => '/dev/loop-test-b', backing_realpath => '/tmp/test-b' },
			{ path => '/dev/loop-test-c', backing_realpath => '/tmp/test-c' },
		],
	}, 'PostgreSQL::Test::ClusterQuad';
	my @attempted;

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::run = sub {
		my ($cmd) = @_;
		my $device = $cmd->[-1];
		push @attempted, $device;
		return $device ne '/dev/loop-test-b';
	};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_backing_loop_mappings
		= sub { return []; };
	my $detached = eval { $quad->_two_stage_detach_voting_loops(); 1 };

	ok(!$detached, 'partial detach failure remains visible');
	is_deeply(\@attempted,
		[ qw(/dev/loop-test-c /dev/loop-test-b) ],
		'detach attempts registered devices in reverse order');
	is_deeply($quad->{two_stage_loop_devices},
		[ qw(/dev/loop-test-a /dev/loop-test-b) ],
		'successfully detached tail is removed immediately while exact remainder stays owned');

	@attempted = ();
	local *PostgreSQL::Test::ClusterQuad::run = sub {
		my ($cmd) = @_;
		push @attempted, $cmd->[-1];
		return 1;
	};
	ok(eval { $quad->_two_stage_detach_voting_loops(); 1 },
		'later cleanup resumes the exact partial remainder');
	is_deeply(\@attempted,
		[ qw(/dev/loop-test-b /dev/loop-test-a) ],
		'resumed cleanup preserves reverse order');
	is_deeply($quad->{two_stage_loop_devices}, [],
		'resumed cleanup removes every successfully detached device');
}

{
	my @nodes = map { ClusterQuadLifecycleTestNode->new(0) } 0 .. 3;
	my $quad = bless {
		nodes => \@nodes,
		voting_disk_paths => [qw(/tmp/vote0 /tmp/vote1 /tmp/vote2)],
		two_stage_native_stop_success => [ 1, 1, 1, 1 ],
		two_stage_phase1_actor_pids => [ 2001, 2002 ],
	}, 'PostgreSQL::Test::ClusterQuad';

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_pid_alive
		= sub { return 0; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_open_backing_fd_holders
		= sub { return []; };
	ok(eval { $quad->_two_stage_assert_phase1_offline(); 1 },
		'attach precondition accepts four successful stops, exited actors, and zero backing FDs');

	$quad->{two_stage_native_stop_success}[2] = 0;
	ok(!eval { $quad->_two_stage_assert_phase1_offline(); 1 },
		'attach precondition rejects a missing native stop proof');
	$quad->{two_stage_native_stop_success}[2] = 1;

	local *PostgreSQL::Test::ClusterQuad::_two_stage_pid_alive
		= sub { return $_[1] == 2002; };
	ok(!eval { $quad->_two_stage_assert_phase1_offline(); 1 },
		'attach precondition rejects a surviving Phase-1 actor');

	local *PostgreSQL::Test::ClusterQuad::_two_stage_pid_alive
		= sub { return 0; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_open_backing_fd_holders
		= sub { return [ 'pid=3003 fd=9 path=/tmp/vote1' ]; };
	ok(!eval { $quad->_two_stage_assert_phase1_offline(); 1 },
		'attach precondition rejects an open voting backing FD');
}

{
	my @nodes = map { ClusterQuadLifecycleTestNode->new(100 + $_) } 0 .. 3;
	my $quad = bless { nodes => \@nodes },
		'PostgreSQL::Test::ClusterQuad';

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_pid_alive
		= sub { return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_process_parent_map
		= sub {
			return {
				200 => 100,
				201 => 200,
				202 => 103,
				900 => 899,
			};
		};

	$quad->_two_stage_capture_phase1_actors();
	is_deeply($quad->{two_stage_phase1_actor_pids},
		[ 100, 101, 102, 103, 200, 201, 202 ],
		'Phase-1 actor proof recursively captures exact postmaster descendants only');

	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity
		= sub {
			my ($self, $pid) = @_;
			return {
				exists => 1,
				pid => $pid,
				starttime => 10000 + $pid,
				state => 'S',
			};
		};
	my $processes = $quad->_two_stage_capture_cleanup_processes();
	is_deeply([ map { $_->{pid} } @$processes ],
		[ 100, 101, 102, 103, 200, 201, 202 ],
		'cleanup manifest recursively captures the exact descendant set');
	is_deeply([ map { $_->{starttime} } @$processes ],
		[ map { 10000 + $_ } (100, 101, 102, 103, 200, 201, 202) ],
		'cleanup manifest binds every actor to its exact proc starttime');
}

{
	my $module_root = $INC{'PostgreSQL/Test/ClusterQuad.pm'};
	$module_root =~ s{/PostgreSQL/Test/ClusterQuad\.pm\z}{};
	my $child = q{
		package ClusterQuadEndFailure;
		sub _two_stage_cleanup_registered { die "forced cleanup failure\n"; }
		package main;
		push @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS,
			bless({}, 'ClusterQuadEndFailure');
	};

	system($^X, "-I$module_root", '-MPostgreSQL::Test::ClusterQuad',
		'-e', $child);
	isnt($? >> 8, 0,
		'END surfaces cleanup failure when the original exit status was zero');
	system($^X, "-I$module_root", '-MPostgreSQL::Test::ClusterQuad',
		'-e', "$child\nexit 7;");
	is($? >> 8, 7,
		'END preserves an earlier nonzero exit status across cleanup failure');
}

{
	my $quad = ClusterQuadLifecycleTestOrchestrator->new();
	my $ran = eval { $quad->_two_stage_run_lifecycle(); 1 };

	ok($ran, 'two-stage lifecycle orchestrator completes its bounded sequence');
	is_deeply($quad->{events}, [
		'owner', 'reap', 'start1:launch', 'start1:finish',
		'formation:phase-1', 'stop', 'attach', 'qualify', 'device-config',
		'phase2-log-boundary', 'start2:launch', 'gate-ladder',
		'start2:finish'
	], 'Phase-2 gates are observed while native starts remain in flight');

	my $failed = ClusterQuadLifecycleTestOrchestrator->new();
	$failed->{qualify_failure} = 1;
	ok(!eval { $failed->_two_stage_run_lifecycle(); 1 },
		'a failed scratch qualification aborts the lifecycle');
	is($failed->{start_count}, 1,
		'a failed scratch qualification never starts Phase 2');
	ok(!grep { $_ eq 'device-config' } @{ $failed->{events} },
		'a failed scratch qualification never installs device-only voting paths');
}

{
	my @nodes = map { bless({}, 'ClusterQuadLifecycleTestNode') } 0 .. 3;
	my @phase1 = map {
		{
			formation => 0,
			presented_incarnation => 100 + $_,
			admitted_incarnation => 100 + $_,
				admitted_epoch => 0,
				session_incarnation => 500 + $_,
				postmaster_start => "phase1-node$_",
		}
	} 0 .. 3;
	my @phase2 = map {
		{
			formation => 0,
				presented_incarnation => 100 + $_,
				admitted_incarnation => 100 + $_,
				admitted_epoch => 0,
				session_incarnation => 500 + $_,
				postmaster_start => "phase2-node$_",
		}
	} 0 .. 3;
	my $device_csv = '/dev/loop-a,/dev/loop-b,/dev/loop-c';
	my $effective_csv = $device_csv;
	my @attested;
	my $quad = bless {
		nodes => \@nodes,
		voting_disk_paths => [qw(/tmp/vote0 /tmp/vote1 /tmp/vote2)],
		two_stage_loop_devices => [qw(/dev/loop-a /dev/loop-b /dev/loop-c)],
		two_stage_phase1_identity => \@phase1,
	}, 'PostgreSQL::Test::ClusterQuad';

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_current_attest
		= sub { push @attested, $_[2]; return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_effective_voting_paths
		= sub { return $effective_csv; };

	ok(eval { $quad->_two_stage_validate_phase2_current(\@phase2); 1 },
		'Phase-2 accepts a new postmaster observation when authority counters remain numerically stable');
	is_deeply(\@attested, [ 0, 1, 2 ],
		'Phase-2 re-attests every current voting block device');
	is_deeply($quad->{two_stage_current_identity}, \@phase2,
		'only Phase-2 identity is published to later validation');
	ok(!exists($quad->{two_stage_phase1_identity}),
		'Phase-1 identity is discarded after exact Phase-2 validation');

	$quad->{two_stage_phase1_identity} = \@phase1;
	my @stale = map { +{ %{ $phase2[$_] } } } 0 .. 3;
	$stale[2]{postmaster_start} = $phase1[2]{postmaster_start};
	ok(!eval { $quad->_two_stage_validate_phase2_current(\@stale); 1 },
		'Phase-2 rejects a node that retained its Phase-1 postmaster observation');

	$quad->{two_stage_phase1_identity} = \@phase1;
	$effective_csv = '/tmp/vote0,/tmp/vote1,/tmp/vote2';
	ok(!eval { $quad->_two_stage_validate_phase2_current(\@phase2); 1 },
		'Phase-2 rejects an effective voting configuration containing backing paths');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(8250 + $_) } 0 .. 3 ],
		two_stage_cleanup_deadline => 9000,
		two_stage_start_count => 1,
	}, 'PostgreSQL::Test::ClusterQuad';
	my $finished = 0;
	my $captured = 0;
	my $gate_called = 0;
	my $gate_preceded_finish = 0;

	$quad->_two_stage_register_cleanup_owner();
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::start = sub {
		return bless { result => 1 }, 'ClusterQuadLifecycleTestHandle';
	};
	local *PostgreSQL::Test::ClusterQuad::finish = sub {
		$finished++;
		return 1;
	};
	local *PostgreSQL::Test::ClusterQuad::usleep = sub { return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_capture_phase2_boot_identity_current
		= sub {
			$captured++;
			$_[0]->{two_stage_phase2_boot_identity} = [ map {
				{ pid => 8250 + $_, starttime => 18250 + $_ }
			} 0 .. 3 ];
			return 1;
		};

	ok(!eval {
		$quad->_two_stage_start_nodes(sub {
			$gate_called++;
			$gate_preceded_finish = $finished == 0;
			$quad->_two_stage_record_first_failure(
				'WAL_ACTIVE_BLOCKED', 'current-boot WAL is not ACTIVE', 2,
				'WAL_ACTIVE_PUBLISHED');
			die "focused gate failure\n";
		});
		1;
	}, 'a Phase-2 gate failure remains visible while native starts are in flight');
	is($captured, 1,
		'Phase-2 captures exact current-boot identities before gate observation');
	is($gate_called, 1,
		'Phase-2 invokes the bounded gate observer exactly once');
	ok($gate_preceded_finish,
		'Phase-2 gate observation begins before any native waiter is finished');
	is($finished, 4,
		'all native waiters are reaped after the gate observer fails');
	is($quad->{two_stage_attempt}{first_failure}{class},
		'WAL_ACTIVE_BLOCKED',
		'a later native start failure cannot overwrite the first failed gate');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(4000 + $_) } 0 .. 3 ],
		two_stage_cleanup_deadline => 9000,
	}, 'PostgreSQL::Test::ClusterQuad';
	my @expected = qw(
		DEVICE_STATIC_ATTESTED
		DEVICE_IO_QUALIFIED
		VOTING_OPEN_CURRENT
		WAL_ACTIVE_PUBLISHED
		CF_GES_CURRENT
		FORMATION_CURRENT
		ADMISSION_CURRENT
		R4_SAMPLE_ALLOWED);

	$quad->_two_stage_register_cleanup_owner();
	for my $gate (@expected)
	{
		$quad->_two_stage_record_gate(0, $gate, 'READY', "digest-$gate");
	}
	my $report = $quad->_two_stage_gate_report();
	is_deeply([ map { $_->{gate} } @{ $report->{gates}{0} } ], \@expected,
		'Phase-2 gate recorder accepts exactly the frozen monotonic ladder');
	ok(!defined($report->{first_failure}),
		'a fully ready ladder has no first failure');
	is($report->{cleanup_deadline}, 9000,
		'the attempt retains its original absolute cleanup deadline');
	like($report->{attempt_id}, qr/\A[0-9a-f]+\z/,
		'the focused attempt has a fresh bounded identity');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(5000 + $_) } 0 .. 3 ],
		two_stage_cleanup_deadline => 200,
		two_stage_static_attestation_digest => 'static',
		two_stage_device_io_qualified => 1,
		two_stage_scratch_probe_digest => 'io',
	}, 'PostgreSQL::Test::ClusterQuad';
	my %calls;
	my $waits = 0;

	$quad->_two_stage_register_cleanup_owner();
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_monotonic_now
		= sub { return 100; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_wait_gate_observation_change
		= sub { $waits++; return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_observe_gate = sub {
		my ($self, $node_id, $gate) = @_;
		$calls{"$node_id:$gate"}++;
		return {
			status => 'PENDING', class => 'CF_GES_NOT_CURRENT',
			detail => 'not current yet', evidence_digest => 'cf-pending',
		} if $node_id == 2 && $gate eq 'CF_GES_CURRENT'
		  && $calls{"$node_id:$gate"} == 1;
		return { status => 'READY', evidence_digest => "$node_id-$gate" };
	};

	ok(eval { $quad->_two_stage_wait_phase2_gate_ladder(); 1 },
		'a pending gate can become ready within the original attempt deadline');
	is($waits, 1, 'pending readiness waits without recording a failed gate');
	ok(!defined($quad->_two_stage_gate_report()->{first_failure}),
		'pending-to-ready leaves first failure empty');
	is($quad->{two_stage_attempt}{cleanup_deadline}, 200,
		'pending-to-ready never refreshes the absolute deadline');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(5100 + $_) } 0 .. 3 ],
		two_stage_cleanup_deadline => 101,
	}, 'PostgreSQL::Test::ClusterQuad';
	my $now = 100;

	$quad->_two_stage_register_cleanup_owner();
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_monotonic_now
		= sub { return $now; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_wait_gate_observation_change
		= sub { $now = 101; return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_observe_gate = sub {
		my ($self, $node_id, $gate) = @_;
		return {
			status => 'PENDING', class => 'WAL_ACTIVE_BLOCKED',
			detail => 'wal not current', evidence_digest => 'wal-pending',
		} if $gate eq 'WAL_ACTIVE_PUBLISHED';
		return { status => 'READY', evidence_digest => "$node_id-$gate" };
	};

	ok(!eval { $quad->_two_stage_wait_phase2_gate_ladder(); 1 },
		'a pending gate fails only when the original absolute deadline expires');
	is($quad->_two_stage_gate_report()->{first_failure}{class},
		'WAL_ACTIVE_BLOCKED',
		'deadline preserves the frozen failure class of the first pending gate');
	is($quad->{two_stage_attempt}{cleanup_deadline}, 101,
		'deadline failure does not extend the attempt');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless { nodes => [] }, 'PostgreSQL::Test::ClusterQuad';

	$quad->_two_stage_register_cleanup_owner();
	$quad->_two_stage_record_gate(
		1, 'DEVICE_STATIC_ATTESTED', 'READY', 'static-exact');
	ok(!eval {
		$quad->_two_stage_record_gate(
			1, 'VOTING_OPEN_CURRENT', 'READY', 'voting-skipped-io');
		1;
	}, 'Phase-2 gate recorder rejects a skipped predecessor');

	my $first = $quad->_two_stage_record_gate(
		1, 'DEVICE_STATIC_ATTESTED', 'READY', 'static-exact');
	is(scalar(@{ $quad->_two_stage_gate_report()->{gates}{1} }), 1,
		'a byte-identical duplicate returns the existing gate event once');
	is($first->{evidence_digest}, 'static-exact',
		'the duplicate preserves the original exact evidence');
	ok(!eval {
		$quad->_two_stage_record_gate(
			1, 'DEVICE_STATIC_ATTESTED', 'READY', 'static-drift');
		1;
	}, 'a non-identical duplicate gate result fails closed');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless { nodes => [] }, 'PostgreSQL::Test::ClusterQuad';

	$quad->_two_stage_register_cleanup_owner();
	for my $gate (qw(DEVICE_STATIC_ATTESTED DEVICE_IO_QUALIFIED
		VOTING_OPEN_CURRENT))
	{
		$quad->_two_stage_record_gate(2, $gate, 'READY', "ready-$gate");
	}
	$quad->_two_stage_record_gate(2, 'WAL_ACTIVE_PUBLISHED', {
		status => 'FAILED',
		class => 'WAL_ACTIVE_BLOCKED',
		detail => 'mode=7 result=13',
	}, 'wal-result-13');
	my $cascade = $quad->_two_stage_record_gate(2, 'FORMATION_CURRENT', {
		status => 'FAILED',
		class => 'FORMATION_TIMEOUT',
		detail => 'formation wait expired',
	}, 'formation-timeout');
	my $report = $quad->_two_stage_gate_report();

	is($report->{first_failure}{class}, 'WAL_ACTIVE_BLOCKED',
		'the first Phase-2 failure class is immutable');
	is($report->{first_failure}{detail}, 'mode=7 result=13',
		'the first failure retains its named numeric detail');
	is($cascade->{first_error_class}, 'CASCADE_FROM(WAL_ACTIVE_BLOCKED)',
		'a later formation timeout is classified as a cascade');
	ok(!eval {
		$quad->_two_stage_record_first_failure(
			'FORMATION_TIMEOUT', 'replacement detail');
		1;
	}, 'a different later failure cannot overwrite the first failure');
	ok(!eval {
		$quad->_two_stage_record_gate(
			2, 'CF_GES_CURRENT', 'READY', 'late-ready');
		1;
	}, 'a successful later gate is rejected after a failed predecessor');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless { nodes => [] }, 'PostgreSQL::Test::ClusterQuad';

	$quad->_two_stage_register_cleanup_owner();
	$quad->_two_stage_record_gate(
		3, 'DEVICE_STATIC_ATTESTED', 'READY', 'static-ready');
	my $unknown = $quad->_two_stage_record_gate(
		3, 'DEVICE_IO_QUALIFIED', 73, 'numeric-73');
	is($unknown->{first_error_class}, 'UNCLASSIFIED_INTERNAL_RESULT',
		'an unknown numeric product result has an explicit semantic class');
	is($quad->_two_stage_gate_report()->{first_failure}{detail}, 'result=73',
		'the unknown numeric code is retained only as failure detail');
}

{
	my $quad = bless {}, 'PostgreSQL::Test::ClusterQuad';
	local $ENV{PGRAC_DIRECT_IO_PROBE} = '/mock/pgrac_direct_io_probe';
	my $specs = $quad->_two_stage_direct_io_probe_specs(
		'/dev/loop-scratch', 8192, 'abc123');

	is(scalar(@$specs), 4,
		'scratch qualification launches exactly four child probes');
	is_deeply([ map { $_->{sequences} } @$specs ], [ 16, 16, 16, 16 ],
		'each scratch child owns exactly sixteen write/read sequences');
	is_deeply([ map { $_->{offset} } @$specs ], [ 0, 8192, 16384, 24576 ],
		'scratch child regions are aligned and pairwise disjoint');
	ok(!scalar(grep { $_->{device} ne '/dev/loop-scratch' } @$specs),
		'every child uses only the independent scratch device');
}

{
	my $tmp = tempdir(CLEANUP => 1);
	my $probe = "$tmp/focused-probe";
	open(my $probe_fh, '>', $probe) or die "open $probe: $!";
	print {$probe_fh} <<'PROBE';
#!/usr/bin/env perl
my %arg;
while (@ARGV)
{
	my $key = shift @ARGV;
	$arg{$key} = shift @ARGV;
}
exit(($arg{'--node'} // -1) == 0 ? 7 : 0);
PROBE
	close($probe_fh) or die "close $probe: $!";
	chmod(0700, $probe) or die "chmod $probe: $!";
	my $scratch = "$tmp/scratch";
	open(my $scratch_fh, '>:raw', $scratch)
	  or die "open $scratch: $!";
	truncate($scratch_fh, 32768) or die "truncate $scratch: $!";
	close($scratch_fh) or die "close $scratch: $!";
	my $quad = bless {
		nodes => [],
		two_stage_attempt => {
			attempt_id => 'abc123',
			cleanup_deadline => Time::HiRes::clock_gettime(
				Time::HiRes::CLOCK_MONOTONIC()) + 30,
		},
		two_stage_scratch => {
			backing => $scratch, child_processes => [],
		},
	}, 'PostgreSQL::Test::ClusterQuad';
	local $ENV{PGRAC_DIRECT_IO_PROBE} = $probe;

	ok(!eval {
		$quad->_two_stage_run_direct_io_children('/dev/loop99', 8192);
		1;
	}, 'a failed probe child remains a failed qualification result');
	like($@, qr/direct-I\/O child probe failed/,
		'the failed child preserves the probe failure');
	my $actors = $quad->{two_stage_scratch}{child_processes};
	is(scalar(@{ $actors // [] }), 4,
		'all launched probe actors retain exact cleanup ownership');
	is_deeply([ map { $_->{node} } @{ $actors // [] } ], [ 0, 1, 2, 3 ],
		'probe cleanup ownership retains each exact logical actor');
	ok(!scalar(grep { ($_->{pid} // 0) <= 1
		|| ($_->{starttime} // 0) <= 0 } @{ $actors // [] }),
		'probe cleanup ownership contains Linux pid and starttime');
	my $captured = $quad->_two_stage_capture_cleanup_processes();
	is_deeply([ map { $_->{pid} } @$captured ],
		[ map { $_->{pid} } @{ $actors // [] } ],
		'failed probe actors flow into the persistent cleanup process set');
	ok(eval { $quad->_two_stage_cleanup_scratch(); 1 },
		'exited focused probe children permit exact scratch cleanup');
}

{
	my $quad = bless {}, 'PostgreSQL::Test::ClusterQuad';
	my %fingerprints = (
		'/vote0' => {
			mount_point => '/mnt/pgrac', filesystem => 'btrfs',
			mount_options => 'rw,nodatacow', uid => 501, gid => 20,
			mode => 0600, logical_block_size => 4096,
			physical_block_size => 4096,
			allocation_method => 'formatter-write+truncate',
		},
		'/vote1' => undef,
		'/vote2' => undef,
		'/scratch' => undef,
	);
	$fingerprints{'/vote1'} = { %{ $fingerprints{'/vote0'} } };
	$fingerprints{'/vote2'} = { %{ $fingerprints{'/vote0'} } };
	$fingerprints{'/scratch'} = { %{ $fingerprints{'/vote0'} } };

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_backend_fingerprint
		= sub { return { %{ $fingerprints{ $_[1] } } }; };
	ok(eval {
		$quad->_two_stage_assert_equivalent_backend(
			'/scratch', [qw(/vote0 /vote1 /vote2)]);
		1;
	}, 'scratch qualification requires an exact equivalent backend fingerprint');
	$fingerprints{'/scratch'}{mount_options} = 'rw,cow';
	ok(!eval {
		$quad->_two_stage_assert_equivalent_backend(
			'/scratch', [qw(/vote0 /vote1 /vote2)]);
		1;
	}, 'a direct-I/O/CoW-relevant backend drift blocks qualification');
}

{
	my $tmp = tempdir(CLEANUP => 1);
	my @voting = map { "$tmp/vote$_" } 0 .. 2;
	my @before;
	for my $i (0 .. $#voting)
	{
		open(my $fh, '>:raw', $voting[$i]) or die "open $voting[$i]: $!";
		print {$fh} "authoritative-voting-$i";
		close($fh) or die "close $voting[$i]: $!";
		push @before, sha256_hex("authoritative-voting-$i");
	}
	for my $failure (qw(deadline short_io checksum_mismatch child_exit))
	{
		local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
		my @events;
		my $quad = bless {
			nodes => [], voting_disk_paths => \@voting,
			two_stage_cleanup_deadline => 777,
		}, 'PostgreSQL::Test::ClusterQuad';

		$quad->_two_stage_register_cleanup_owner();
		no warnings 'redefine';
		local *PostgreSQL::Test::ClusterQuad::_two_stage_create_scratch_backing
			= sub {
				my ($self) = @_;
				my $path = "$tmp/scratch-$failure";
				open(my $fh, '>:raw', $path) or die "open $path: $!";
				truncate($fh, 32768) or die "truncate $path: $!";
				close($fh) or die "close $path: $!";
				$self->{two_stage_scratch} = {
					backing => $path, child_processes => [ {
						actor => 'scratch_probe', node => 0, pid => 7700,
						starttime => 8800, finished => 1, exit_code => 7,
					} ],
				};
				push @events, 'create';
				return $path;
			};
		local *PostgreSQL::Test::ClusterQuad::_two_stage_assert_equivalent_backend
			= sub { push @events, 'equivalent'; return 1; };
		local *PostgreSQL::Test::ClusterQuad::_two_stage_attach_scratch_loop
			= sub {
				my ($self) = @_;
				$self->{two_stage_scratch}{device} = '/dev/loop-scratch';
				$self->{two_stage_scratch}{record} = {
					path => '/dev/loop-scratch', major_minor => '7:99',
					backing_realpath => "$tmp/scratch-$failure",
					attach_order => 3, direct_io => 1,
				};
				push @events, 'attach';
				return '/dev/loop-scratch';
			};
		local *PostgreSQL::Test::ClusterQuad::_two_stage_attest_scratch_loop
			= sub {
				push @events, 'attest';
				return { logical_block_size => 4096,
					physical_block_size => 4096 };
			};
		local *PostgreSQL::Test::ClusterQuad::_two_stage_run_direct_io_children
			= sub { push @events, 'probe'; die "$failure\n"; };
		local *PostgreSQL::Test::ClusterQuad::_two_stage_cleanup_scratch
			= sub { delete $_[0]->{two_stage_scratch};
				push @events, 'cleanup'; return 1; };

		ok(!eval { $quad->_two_stage_qualify_block_backend(); 1 },
			"$failure makes the block backend unqualified");
		like($@, qr/BLOCK_DEVICE_UNQUALIFIED/,
			"$failure preserves the named qualification failure");
		is_deeply(\@events,
			[ qw(create equivalent attach attest probe cleanup) ],
			"$failure cleanup follows the failed scratch probe exactly once");
		is($quad->_two_stage_gate_report()->{first_failure}{class},
			'BLOCK_DEVICE_UNQUALIFIED',
			"$failure cannot be overwritten by a later startup gate");
		is($quad->{two_stage_scratch_failure_evidence}{record}{path},
			'/dev/loop-scratch',
			"$failure freezes scratch device identity before cleanup");
		is($quad->{two_stage_scratch_failure_evidence}{child_processes}[0]
			{exit_code}, 7,
			"$failure freezes native probe result before cleanup");
		for my $i (0 .. $#voting)
		{
			open(my $fh, '<:raw', $voting[$i])
			  or die "open $voting[$i]: $!";
			local $/;
			my $bytes = <$fh>;
			close($fh) or die "close $voting[$i]: $!";
			is(sha256_hex($bytes), $before[$i],
				"$failure leaves authoritative voting bytes $i unchanged");
		}
	}
}

{
	my $quad = bless {
		two_stage_scratch => {
			backing => '/tmp/pgrac-scratch', device => '/dev/loop-scratch'
		},
	}, 'PostgreSQL::Test::ClusterQuad';
	my $detaches = 0;

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return [ 'pid=77 fd=9' ]; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_scratch_loop
		= sub { $detaches++; return 1; };
	ok(!eval { $quad->_two_stage_cleanup_scratch(); 1 },
		'scratch cleanup refuses detach while an exact FD holder remains');
	is($detaches, 0, 'scratch detach is not attempted before holders reach zero');
}

{
	my $root = tempdir(CLEANUP => 1);
	my $quad = bless {
		two_stage_artifact_root => $root,
		two_stage_attempt => {
			attempt_id => 'd15ea5e', cleanup_deadline => 5555,
			cleanup_deadline_wallclock => 1_800_000_000,
			first_failure => {
				class => 'WAL_ACTIVE_BLOCKED', detail => 'mode=7 result=13',
			},
		},
		two_stage_cleanup_processes => [ {
			node => 2, pid => 4321, starttime => 8765, last_state => 'D',
		} ],
		two_stage_loop_records => [
			{ path => '/dev/loop-a', major_minor => '7:10',
				backing_realpath => '/exact/vote-a', attach_order => 0 },
			{ path => '/dev/loop-b', major_minor => '7:11',
				backing_realpath => '/exact/vote-b', attach_order => 1 },
		],
	}, 'PostgreSQL::Test::ClusterQuad';

	my $path = $quad->_two_stage_write_cleanup_manifest(
		'OPERATOR_CLEANUP_REQUIRED', 'WAL_ACTIVE_BLOCKED');
	ok(-f $path, 'D-state cleanup manifest survives outside the quad object');
	open(my $fh, '<:raw', $path) or die "open $path: $!";
	local $/;
	my $manifest = JSON::PP->new->decode(<$fh>);
	close($fh) or die "close $path: $!";
	is($manifest->{status}, 'OPERATOR_CLEANUP_REQUIRED',
		'D-state manifest has an explicit operator-cleanup terminal');
	is_deeply($manifest->{processes}[0], {
		node => 2, pid => 4321, starttime => 8765, last_state => 'D',
	}, 'manifest persists exact pid/starttime rather than PID alone');
	is_deeply([ map { $_->{path} } @{ $manifest->{devices} } ],
		[ qw(/dev/loop-a /dev/loop-b) ],
		'manifest persists only exact registered devices');
}

{
	my $root = tempdir(CLEANUP => 1);
	my $resource_root = "$root/resources";
	mkdir $resource_root or die "mkdir $resource_root: $!";
	my @resource_backings = map { "$resource_root/r$_" } 0 .. 2;
	for my $path (@resource_backings)
	{
		open(my $fh, '>:raw', $path) or die "open $path: $!";
		print {$fh} "exact-reaper-backing";
		close($fh) or die "close $path: $!";
	}
	my $manifest_quad = bless {
		two_stage_artifact_root => $root,
		two_stage_attempt => {
			attempt_id => 'e7ac7', cleanup_deadline => 6000,
			cleanup_deadline_wallclock => 1_800_000_100,
			first_failure => { class => 'BLOCK_DEVICE_UNQUALIFIED', detail => 'D' },
		},
		two_stage_cleanup_processes => [ {
			node => 1, pid => 7001, starttime => 9001, last_state => 'D',
		} ],
		two_stage_loop_records => [
			{ path => '/dev/loop20', major_minor => '  7:20  ',
				backing_realpath => $resource_backings[0], attach_order => 0 },
			{ path => '/dev/loop21', major_minor => '  7:21  ',
				backing_realpath => $resource_backings[1], attach_order => 1 },
			{ path => '/dev/loop22', major_minor => '  7:22  ',
				backing_realpath => $resource_backings[2], attach_order => 2 },
		],
	}, 'PostgreSQL::Test::ClusterQuad';
	$manifest_quad->_two_stage_write_cleanup_manifest(
		'OPERATOR_CLEANUP_REQUIRED', 'BLOCK_DEVICE_UNQUALIFIED');

	my $reaper = bless {
		two_stage_artifact_root => $root,
		two_stage_approved_resource_roots => [ $root ],
	},
		'PostgreSQL::Test::ClusterQuad';
	my @detached;
	my $proc_mode = 'exact-live';
	my $holders = [];
	my $mapping_drift = 0;
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity = sub {
		return { pid => 7001,
			starttime => $proc_mode eq 'pid-reused' ? 9002 : 9001,
			state => 'D', ppid => 1 } unless $proc_mode eq 'absent';
		return { pid => 7001, exists => 0 };
	};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return $holders; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_current_loop_mapping
		= sub {
			my ($self, $device) = @_;
			my %expected = (
				'/dev/loop20' => [ '7:20', $resource_backings[0] ],
				'/dev/loop21' => [ '7:21', $resource_backings[1] ],
				'/dev/loop22' => [ '7:22', $resource_backings[2] ],
			);
			return {
				path => $device,
				major_minor => $mapping_drift ? '7:99' : $expected{$device}[0],
				backing_realpath => $expected{$device}[1],
			};
		};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_recorded_device
		= sub { push @detached, $_[1]{path}; return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_recorded_device_mapping_absent
		= sub { return 1; };

	is($reaper->_two_stage_reap_previous_manifests(),
		'OPERATOR_CLEANUP_REQUIRED',
		'a live exact pid/starttime blocks manifest reaping');
	is_deeply(\@detached, [], 'a live exact process permits zero detach');

	$proc_mode = 'pid-reused';
	is($reaper->_two_stage_reap_previous_manifests(),
		'CLEANUP_IDENTITY_CONFLICT',
		'PID reuse is an identity conflict, not proof of process exit');
	is_deeply(\@detached, [], 'PID reuse is neither signalled nor detached');

	$proc_mode = 'absent';
	$holders = [ { pid => 7100, fd => 8,
		path => $resource_backings[1] } ];
	is($reaper->_two_stage_reap_previous_manifests(),
		'OPERATOR_CLEANUP_REQUIRED',
		'an exact FD holder blocks manifest reaping');
	is_deeply(\@detached, [], 'a live FD holder permits zero detach');

	$holders = [];
	$mapping_drift = 1;
	is($reaper->_two_stage_reap_previous_manifests(),
		'CLEANUP_IDENTITY_CONFLICT',
		'device major:minor drift blocks manifest reaping');
	is_deeply(\@detached, [], 'mapping identity drift permits zero detach');

	$mapping_drift = 0;
	is($reaper->_two_stage_reap_previous_manifests(), 'CLEAN',
		'an exited exact process and zero holders permit reaping');
	is_deeply(\@detached,
		[ qw(/dev/loop22 /dev/loop21 /dev/loop20) ],
		'the explicit reaper detaches only registered devices in reverse order');
	is($reaper->_two_stage_reap_previous_manifests(), 'CLEAN',
		'a second explicit reaper call is idempotent');
}

{
	my $root = tempdir(CLEANUP => 1);
	my $resource_root = "$root/deleted-at-tap-exit";
	mkdir $resource_root or die "mkdir $resource_root: $!";
	my $backing = "$resource_root/vote0";
	open(my $fh, '>:raw', $backing) or die "open $backing: $!";
	print {$fh} "exact-deleted-backing";
	close($fh) or die "close $backing: $!";
	my $owner = bless {
		two_stage_artifact_root => $root,
		two_stage_attempt => {
			attempt_id => 'de1e7ed', cleanup_deadline => 7100,
			cleanup_deadline_wallclock => 1_800_000_250,
			first_failure => { class => 'VOTING_OPEN_BLOCKED', detail => 'D' },
		},
		two_stage_cleanup_processes => [],
		two_stage_loop_records => [ {
			path => '/dev/loop40', major_minor => '7:40',
			backing_realpath => $backing, attach_order => 0,
		} ],
	}, 'PostgreSQL::Test::ClusterQuad';
	$owner->_two_stage_write_cleanup_manifest(
		'OPERATOR_CLEANUP_REQUIRED', 'VOTING_OPEN_BLOCKED');
	unlink($backing) or die "unlink $backing: $!";
	rmdir($resource_root) or die "rmdir $resource_root: $!";

	my $reaper = bless {
		two_stage_artifact_root => $root,
		two_stage_approved_resource_roots => [ $root ],
	}, 'PostgreSQL::Test::ClusterQuad';
	my @detached;
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return []; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_current_loop_mapping
		= sub { return { path => '/dev/loop40', major_minor => '7:40',
			backing_realpath => $backing }; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_recorded_device
		= sub { push @detached, $_[1]{path}; return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_recorded_device_mapping_absent
		= sub { return 1; };

	is($reaper->_two_stage_reap_previous_manifests(), 'CLEAN',
		'an exact loop mapping remains reapable after TAP deletes its backing path');
	is_deeply(\@detached, [ '/dev/loop40' ],
		'deleted backing cleanup detaches only the exact recorded device');
}

{
	my $root = tempdir(CLEANUP => 1);
	my $resource_root = "$root/resources";
	mkdir $resource_root or die "mkdir $resource_root: $!";
	my $backing = "$resource_root/post-detach";
	open(my $fh, '>:raw', $backing) or die "open $backing: $!";
	print {$fh} "post-detach-residue";
	close($fh) or die "close $backing: $!";
	my $owner = bless {
		two_stage_artifact_root => $root,
		two_stage_attempt => {
			attempt_id => 'f1a1', cleanup_deadline => 7000,
			cleanup_deadline_wallclock => 1_800_000_200,
			first_failure => { class => 'WAL_ACTIVE_BLOCKED', detail => 'D' },
		},
		two_stage_cleanup_processes => [],
		two_stage_loop_records => [ {
			path => '/dev/loop30', major_minor => '7:30',
			backing_realpath => $backing, attach_order => 0,
		} ],
	}, 'PostgreSQL::Test::ClusterQuad';
	my $path = $owner->_two_stage_write_cleanup_manifest(
		'OPERATOR_CLEANUP_REQUIRED', 'WAL_ACTIVE_BLOCKED');
	my $reaper = bless {
		two_stage_artifact_root => $root,
		two_stage_approved_resource_roots => [ $root ],
	}, 'PostgreSQL::Test::ClusterQuad';
	my @detached;
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return []; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_current_loop_mapping
		= sub { return { path => '/dev/loop30', major_minor => '7:30',
			backing_realpath => $backing }; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_recorded_device
		= sub { push @detached, $_[1]{path}; return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_recorded_device_mapping_absent
		= sub { return 0; };

	is($reaper->_two_stage_reap_previous_manifests(),
		'CLEANUP_IDENTITY_CONFLICT',
		'a mapping still visible after detach cannot be marked REAPED');
	is_deeply(\@detached, [ '/dev/loop30' ],
		'the post-detach proof runs after the exact recorded detach only');
	ok(-f $path,
		'post-detach mapping residue retains the manifest for operator evidence');
	if (-f $path)
	{
		open($fh, '<:raw', $path) or die "open $path: $!";
		local $/;
		my $manifest = JSON::PP->new->decode(<$fh>);
		close($fh) or die "close $path: $!";
		is(scalar(@{ $manifest->{devices} }), 1,
			'a detach lacking zero-mapping proof retains exact device ownership');
	}
	else
	{
		fail('a detach lacking zero-mapping proof retains exact device ownership');
	}
}

{
	my $root = tempdir(CLEANUP => 1);
	my $outside = tempdir(CLEANUP => 1);
	my $backing = "$outside/not-approved";
	open(my $fh, '>:raw', $backing) or die "open $backing: $!";
	print {$fh} "outside-approved-root";
	close($fh) or die "close $backing: $!";
	my $writer = bless { two_stage_artifact_root => $root },
		'PostgreSQL::Test::ClusterQuad';
	my $path = $writer->_two_stage_manifest_path('badf00d');
	$writer->_two_stage_write_manifest_data($path, {
		schema => 1, attempt_id => 'badf00d',
		original_failure => 'BLOCK_DEVICE_UNQUALIFIED',
		created_at => '2030-01-01T00:00:00Z',
		absolute_cleanup_deadline => '1800000300',
		processes => [], status => 'OPERATOR_CLEANUP_REQUIRED',
		devices => [ {
			path => '/dev/loop31', major_minor => '7:31',
			backing_realpath => $backing, attach_order => 0,
		} ],
	});
	my $reaper = bless {
		two_stage_artifact_root => $root,
		two_stage_approved_resource_roots => [ $root ],
	}, 'PostgreSQL::Test::ClusterQuad';
	my @detached;
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return []; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_current_loop_mapping
		= sub { return { path => '/dev/loop31', major_minor => '7:31',
			backing_realpath => $backing }; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_recorded_device
		= sub { push @detached, $_[1]{path}; return 1; };

	is($reaper->_two_stage_reap_previous_manifests(),
		'CLEANUP_IDENTITY_CONFLICT',
		'a manifest resource outside approved test roots fails closed');
	is_deeply(\@detached, [],
		'an unapproved resource path permits zero detach');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $root = tempdir(CLEANUP => 1);
	my $log = "$root/node0.log";
	open(my $fh, '>:raw', $log) or die "open $log: $!";
	print {$fh} "before\nfirst exact product error\nafter\n";
	close($fh) or die "close $log: $!";
	my $node = ClusterQuadLifecycleTestNode->new(8050);
	$node->{logfile} = $log;
	my $quad = bless {
		nodes => [ $node ], two_stage_artifact_root => $root,
		two_stage_attempt => {
			attempt_id => 'e1d3', cleanup_deadline => 7000,
			cleanup_deadline_wallclock => 1_800_000_100,
			first_failure => {
				class => 'WAL_ACTIVE_BLOCKED', detail => 'result=13',
				node_id => 0, gate => 'WAL_ACTIVE_PUBLISHED',
			},
			gates => { 0 => [ {
				gate => 'VOTING_OPEN_CURRENT', status => 'READY',
				evidence_digest => 'open-current',
			} ] },
		},
		two_stage_cleanup_processes => [ {
			node => 0, pid => 8050, starttime => 88050, last_state => 'D',
		} ],
		two_stage_native_start_results => [ {
			node_id => 0, finished => 1, exit_code => 13,
			stdout => '', stderr => 'start failed',
		} ],
		two_stage_native_stop_results => [ {
			node_id => 0, finished => 1, exit_code => 0,
			stdout => 'stopped', stderr => '',
		} ],
		two_stage_loop_records => [],
	}, 'PostgreSQL::Test::ClusterQuad';
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity = sub {
		return { pid => 8050, exists => 1, starttime => 88050,
			state => 'D', ppid => 1 };
	};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_wait_channel
		= sub { return 'submit_bio_wait'; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_kernel_stack
		= sub { return 'UNAVAILABLE'; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_process_fd_evidence
		= sub { return { status => 'OK', entries => [ {
			fd => 9, target => '/dev/loop20',
		} ] }; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_failure_device_evidence
		= sub { return [ { path => '/dev/loop20', major_minor => '7:20',
			backend => 'block_device' } ]; };

	my $path = eval { $quad->_two_stage_capture_failure_evidence() };
	ok(defined($path) && -f $path,
		'first failure atomically persists a bounded exact evidence package');
	if (defined($path) && -f $path)
	{
		open($fh, '<:raw', $path) or die "open $path: $!";
		local $/;
		my $evidence = JSON::PP->new->decode(<$fh>);
		close($fh) or die "close $path: $!";
		is($evidence->{first_failure}{class}, 'WAL_ACTIVE_BLOCKED',
			'evidence preserves the original semantic failure class');
		is($evidence->{processes}[0]{starttime}, 88050,
			'evidence binds process state to exact pid/starttime');
		is($evidence->{processes}[0]{wchan}, 'submit_bio_wait',
			'evidence records the bounded wait channel');
		is($evidence->{processes}[0]{kernel_stack}, 'UNAVAILABLE',
			'an unreadable kernel stack is explicitly preserved');
		is($evidence->{processes}[0]{fds}{entries}[0]{fd}, 9,
			'evidence records exact resource FD ownership');
		is($evidence->{devices}[0]{major_minor}, '7:20',
			'evidence records current device/backend identity');
		like($evidence->{logs}[0]{text}, qr/first exact product error/,
			'evidence contains a bounded log window around the failure');
		is($evidence->{native_results}{start}[0]{exit_code}, 13,
			'evidence preserves the native start exit code');
		is($evidence->{cleanup_manifest}{status}, 'NOT_WRITTEN',
			'initial evidence does not fabricate cleanup completion');

		$quad->_two_stage_update_failure_evidence_manifest(
			'/tmp/exact-cleanup.json', 'OPERATOR_CLEANUP_REQUIRED');
		open($fh, '<:raw', $path) or die "reopen $path: $!";
		local $/;
		$evidence = JSON::PP->new->decode(<$fh>);
		close($fh) or die "close $path: $!";
		is_deeply($evidence->{cleanup_manifest}, {
			path => '/tmp/exact-cleanup.json',
			status => 'OPERATOR_CLEANUP_REQUIRED',
		}, 'evidence is updated only with the exact cleanup manifest terminal');
	}
	else
	{
		fail('evidence preserves the original semantic failure class');
		fail('evidence binds process state to exact pid/starttime');
		fail('evidence records the bounded wait channel');
		fail('an unreadable kernel stack is explicitly preserved');
		fail('evidence records exact resource FD ownership');
		fail('evidence records current device/backend identity');
		fail('evidence contains a bounded log window around the failure');
		fail('evidence preserves the native start exit code');
		fail('initial evidence does not fabricate cleanup completion');
		fail('evidence is updated only with the exact cleanup manifest terminal');
	}
}

for my $case (qw(normal_exit normal_d_state normal_identity_drift
	normal_fd_holder incomplete_start explicit_failure recorded_failure end_cleanup))
{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $root = tempdir(CLEANUP => 1);
	my $node = ClusterQuadLifecycleTestNode->new(8160);
	my $quad = bless {
		nodes => [ $node ], two_stage_artifact_root => $root,
		two_stage_loop_devices => [], two_stage_loop_records => [],
		two_stage_cleanup_deadline => 1,
		two_stage_start_completed => $case ne 'incomplete_start',
	}, 'PostgreSQL::Test::ClusterQuad';
	my ($now, $waits, $stops, $detaches) = (1000, 0, 0, 0);
	my $normal = $case =~ /^normal_/;
	my $expected = $case eq 'normal_exit' ? 'CLEAN'
	  : $case eq 'normal_identity_drift' ? 'CLEANUP_IDENTITY_CONFLICT'
	  : 'OPERATOR_CLEANUP_REQUIRED';
	my $budget = $PostgreSQL::Test::Utils::timeout_default;

	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_monotonic_now
		= sub { return $now; };
	local *PostgreSQL::Test::ClusterQuad::time
		= sub { return 1_800_000_000; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_process_parent_map
		= sub { return {}; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity = sub {
		return { pid => 8160, exists => 0 }
		  if $stops && ($case eq 'normal_fd_holder'
			  || ($case eq 'normal_exit' && $waits));
		return { pid => 8160, exists => 1,
			starttime => $stops && $case eq 'normal_identity_drift' ? 9999 : 9913,
			state => $case eq 'normal_d_state' ? 'D' : 'S', ppid => 1 };
	};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_wait_gate_observation_change
		= sub { $waits++; $now += $budget / 2; return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_signal_exact_process
		= sub { $stops++; return 1; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return $case eq 'normal_fd_holder' ? [ 'exact holder' ] : []; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_detach_voting_loops
		= sub { $detaches++; return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_capture_failure_evidence
		= sub { return; };

	$quad->_two_stage_register_cleanup_owner();
	$quad->{two_stage_attempt}{cleanup_deadline_wallclock} = 1_799_999_001;
	$quad->_two_stage_record_first_failure('WAL_ACTIVE_BLOCKED', 'original', 0,
		'WAL_ACTIVE_PUBLISHED') if $case eq 'recorded_failure';
	my $ok = eval {
		if ($case eq 'end_cleanup')
		{
			$quad->_two_stage_cleanup_registered();
		}
		else
		{
			$quad->stop_quad(failure_cleanup => $case eq 'explicit_failure');
		}
		1;
	};
	is($quad->{two_stage_cleanup_terminal}, $expected,
		"$case: exact cleanup terminal is preserved");
	is(!!$ok, $case eq 'normal_exit' || $case eq 'end_cleanup' ? 1 : '',
		"$case: stop still surfaces any failed cleanup");
	is($quad->{two_stage_attempt}{cleanup_deadline}, 1,
		"$case: original startup/failure deadline is never changed");
	is($quad->{two_stage_attempt}{normal_cleanup_deadline},
		$normal ? 1000 + $budget : undef,
		"$case: only admitted normal stop receives its original-duration budget");
	is($stops, 1, "$case: exactly one stop signal request");
	is($detaches, $case eq 'normal_exit' ? 1 : 0,
		"$case: nonterminal processes or holders never permit detach");
	is($waits, $case eq 'normal_exit' ? 1 : $case eq 'normal_d_state' ? 2 : 0,
		"$case: only normal cleanup waits within its frozen deadline");
	$now += 500;
	is($quad->_two_stage_cleanup_registered(), $expected,
		"$case: END returns the exact previous terminal");
	eval { $quad->stop_quad(); };
	is($quad->{two_stage_attempt}{normal_cleanup_deadline},
		$normal ? 1000 + $budget : undef,
		"$case: repeated stop and END cannot renew or create a budget");
	is($stops, 1, "$case: duplicate cleanup never signals again");
	if ($expected ne 'CLEAN')
	{
		open(my $fh, '<:raw', $quad->{two_stage_cleanup_manifest_path})
		  or die "open $case manifest: $!";
		local $/;
		my $manifest = JSON::PP->new->decode(<$fh>);
		close($fh) or die "close $case manifest: $!";
		is($manifest->{absolute_cleanup_deadline},
			$normal ? 1_800_000_000 + $budget : 1_799_999_001,
			"$case: manifest records the deadline actually used");
		is($manifest->{cleanup_deadline_owner}, $normal ? 'NORMAL_STOP' : 'STARTUP_FAILURE',
			"$case: manifest records budget ownership");
		is($manifest->{startup_cleanup_deadline}, 1_799_999_001,
			"$case: manifest retains the original deadline evidence");
	}
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $root = tempdir(CLEANUP => 1);
	my $node = ClusterQuadLifecycleTestNode->new(8100);
	$node->{forbid_blocking_teardown} = 1;
	my $quad = bless {
		nodes => [ $node ], two_stage_artifact_root => $root,
		two_stage_loop_devices => [], two_stage_loop_records => [],
		two_stage_cleanup_deadline => 1,
	}, 'PostgreSQL::Test::ClusterQuad';

	$quad->_two_stage_register_cleanup_owner();
	$quad->_two_stage_record_first_failure(
		'WAL_ACTIVE_BLOCKED', 'mode=7 result=13', 0,
		'WAL_ACTIVE_PUBLISHED');
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity
		= sub {
			return { pid => 8100, exists => 1, starttime => 9911,
				state => 'D', ppid => 1 };
			};
	my $stop_requests = 0;
	local *PostgreSQL::Test::ClusterQuad::_two_stage_signal_exact_process
		= sub { $stop_requests++; return 1; };
	is($quad->_two_stage_cleanup_registered(),
		'OPERATOR_CLEANUP_REQUIRED',
		'D-state at the original deadline is a failed cleanup terminal');
	is($node->{cleanup_calls}, 0,
		'D-state cleanup never calls blocking generic node teardown');
	is($stop_requests, 1,
		'D-state cleanup issues one exact non-waiting stop request');
	is($quad->_two_stage_cleanup_registered(),
		'OPERATOR_CLEANUP_REQUIRED',
		'END-style duplicate cleanup returns the existing terminal');
	is($node->{cleanup_calls}, 0,
		'END-style duplicate cleanup cannot enter generic node teardown');
	is($stop_requests, 1,
		'END-style duplicate cleanup cannot issue a second stop request');
	is($quad->{two_stage_attempt}{first_failure}{class},
		'WAL_ACTIVE_BLOCKED',
		'cleanup failure cannot overwrite the original product failure');
	ok(-f $quad->{two_stage_cleanup_manifest_path},
		'D-state cleanup persists exact ownership for a later explicit reaper');
	ok(-f ($quad->{two_stage_failure_evidence_path} // ''),
		'D-state cleanup persists the bounded first-failure evidence package');
	if (-f ($quad->{two_stage_failure_evidence_path} // ''))
	{
		open(my $fh, '<:raw', $quad->{two_stage_failure_evidence_path})
		  or die "open cleanup evidence: $!";
		local $/;
		my $evidence = JSON::PP->new->decode(<$fh>);
		close($fh) or die "close cleanup evidence: $!";
		is($evidence->{cleanup_manifest}{status},
			'OPERATOR_CLEANUP_REQUIRED',
			'D-state evidence points at the exact persisted cleanup terminal');
	}
	else
	{
		fail('D-state evidence points at the exact persisted cleanup terminal');
	}
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $root = tempdir(CLEANUP => 1);
	my $node = ClusterQuadLifecycleTestNode->new(8150);
	$node->{forbid_blocking_teardown} = 1;
	my $quad = bless {
		nodes => [ $node ], two_stage_artifact_root => $root,
		two_stage_loop_devices => [], two_stage_loop_records => [],
		two_stage_cleanup_deadline => 104,
	}, 'PostgreSQL::Test::ClusterQuad';
	my $identity_observations = 0;
	my $now = 100;
	my $waits = 0;

	$quad->_two_stage_register_cleanup_owner();
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_process_parent_map
		= sub { return {}; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_proc_identity = sub {
		$identity_observations++;
		return { pid => 8150, exists => 0 }
		  if $identity_observations >= 3;
		return { pid => 8150, exists => 1, starttime => 9912,
			state => 'S', ppid => 1 };
	};
	local *PostgreSQL::Test::ClusterQuad::_two_stage_monotonic_now
		= sub { return $now; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_wait_gate_observation_change
		= sub { $waits++; $now++; return; };
	local *PostgreSQL::Test::ClusterQuad::_two_stage_device_holders
		= sub { return []; };
	my $stop_requests = 0;
	local *PostgreSQL::Test::ClusterQuad::_two_stage_signal_exact_process
		= sub { $stop_requests++; return 1; };

	is($quad->_two_stage_cleanup_registered(), 'CLEAN',
		'exact cleanup closes when the original process exits before deadline');
	is($node->{cleanup_calls}, 0,
		'clean closure never calls blocking generic node teardown');
	is($stop_requests, 1,
		'clean closure issues exactly one exact non-waiting stop request');
	is($waits, 1,
		'cleanup re-observes ownership instead of sampling only once');
	is($quad->{two_stage_attempt}{cleanup_deadline}, 104,
		'cleanup closure never refreshes the original absolute deadline');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(8200 + $_) } 0 .. 3 ],
		two_stage_cleanup_deadline => 9000,
	}, 'PostgreSQL::Test::ClusterQuad';
	my @results = (0, 1, 1, 1);

	$quad->_two_stage_register_cleanup_owner();
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::start = sub {
		return bless { result => shift @results },
		  'ClusterQuadLifecycleTestHandle';
	};
	local *PostgreSQL::Test::ClusterQuad::finish = sub { return 1; };
	local *PostgreSQL::Test::ClusterQuad::usleep = sub { return; };

	ok(!eval { $quad->_two_stage_start_nodes(); 1 },
		'a native Phase-1 start failure remains visible');
	is($quad->{two_stage_attempt}{first_failure}{class},
		'PHASE1_START_FAILED',
		'Phase-1 start failure becomes the preserved original failure');
	is($quad->{two_stage_attempt}{first_failure}{node_id}, 1,
		'Phase-1 start failure identifies the first failed native node');
}

{
	local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
	my $quad = bless {
		nodes => [ map { ClusterQuadLifecycleTestNode->new(9000 + $_) } 0 .. 3 ],
		two_stage_static_attestation_digest => 'static-digest',
		two_stage_device_io_qualified => 1,
		two_stage_scratch_probe_digest => 'io-digest',
		two_stage_phase2_boot_identity => [ map {
			{ pid => 9000 + $_, starttime => 12000 + $_ }
		} 0 .. 3 ],
	}, 'PostgreSQL::Test::ClusterQuad';
	my @gates = @PostgreSQL::Test::ClusterQuad::TWO_STAGE_PHASE2_GATES;
	$quad->{two_stage_observer_fixtures} = {
		map {
			my $node = $_;
			$node => { map {
				$_ => { status => 'READY', evidence_digest => "$node-$_" }
			} @gates }
		} 0 .. 3
	};

	$quad->_two_stage_register_cleanup_owner();
	ok(eval { $quad->_two_stage_wait_phase2_gate_ladder(); 1 },
		'all four nodes can advance through the exact eight-gate ladder');
	my $report = $quad->_two_stage_gate_report();
	for my $node (0 .. 3)
	{
		is_deeply([ map { $_->{gate} } @{ $report->{gates}{$node} } ],
			\@gates, "node$node records every Phase-2 readiness gate once");
	}
}

{
	my @cases = (
		[ 'VOTING_OPEN_CURRENT', 'VOTING_OPEN_BLOCKED' ],
		[ 'WAL_ACTIVE_PUBLISHED', 'WAL_ACTIVE_BLOCKED' ],
		[ 'CF_GES_CURRENT', 'CF_GES_NOT_CURRENT' ],
		[ 'FORMATION_CURRENT', 'FORMATION_NOT_CURRENT' ],
		[ 'ADMISSION_CURRENT', 'ADMISSION_NOT_CURRENT' ],
		[ 'R4_SAMPLE_ALLOWED', 'R4_PREREQUISITE_BLOCKED' ],
	);
	for my $case (@cases)
	{
		local @PostgreSQL::Test::ClusterQuad::TWO_STAGE_LOOP_QUADS;
		my ($failed_gate, $class) = @$case;
		my @gates = @PostgreSQL::Test::ClusterQuad::TWO_STAGE_PHASE2_GATES;
		my $quad = bless {
			nodes => [ map { ClusterQuadLifecycleTestNode->new(9200 + $_) } 0 .. 3 ],
			two_stage_static_attestation_digest => 'static',
			two_stage_device_io_qualified => 1,
			two_stage_scratch_probe_digest => 'qualified',
			two_stage_phase2_boot_identity => [ map {
				{ pid => 9200 + $_, starttime => 13000 + $_ }
			} 0 .. 3 ],
		}, 'PostgreSQL::Test::ClusterQuad';
		$quad->{two_stage_observer_fixtures} = {
			map {
				my $node = $_;
				$node => { map {
					$_ => { status => 'READY', evidence_digest => "$node-$_" }
				} @gates }
			} 0 .. 3
		};
		$quad->{two_stage_observer_fixtures}{2}{$failed_gate} = {
			status => 'FAILED', class => $class,
			detail => "node2 $failed_gate failed",
			evidence_digest => "failed-$failed_gate",
		};
		$quad->_two_stage_register_cleanup_owner();

		ok(!eval { $quad->_two_stage_wait_phase2_gate_ladder(); 1 },
			"$failed_gate aborts the ladder at its first failure");
		my $report = $quad->_two_stage_gate_report();
		is($report->{first_failure}{class}, $class,
			"$failed_gate retains its own semantic failure class");
		my $failed_index = 0;
		$failed_index++ while $gates[$failed_index] ne $failed_gate;
		my %observed = map { $_->{gate} => 1 } @{ $report->{gates}{2} };
		for my $later (@gates[$failed_index + 1 .. $#gates])
		{
			ok(!$observed{$later},
				"$failed_gate does not sample later gate $later");
		}
	}
}

{
	my $root = tempdir(CLEANUP => 1);
	my $log = "$root/node.log";
	open(my $fh, '>', $log) or die "open $log: $!";
	print {$fh} "cluster phase 4: DIAG ready phase1-only\n";
	close($fh) or die "close $log: $!";
	my $node = bless { logfile => $log }, 'ClusterQuadLifecycleTestNode';
	my $quad = bless {
		nodes => [ $node ],
		two_stage_phase2_log_offsets => [ -s $log ],
	}, 'PostgreSQL::Test::ClusterQuad';

	unlike($quad->_two_stage_phase2_log_slice(0), qr/DIAG ready/,
		'Phase-1 log bytes before the captured offset cannot satisfy Phase 2');
	open($fh, '>>', $log) or die "append $log: $!";
	print {$fh} "cluster phase 4: DIAG ready phase2-current\n";
	close($fh) or die "close $log: $!";
	like($quad->_two_stage_phase2_log_slice(0), qr/phase2-current/,
		'only post-boundary Phase-2 evidence is visible to observers');
}

{
	my $quad = bless {}, 'PostgreSQL::Test::ClusterQuad';
	is($quad->_two_stage_named_product_result('cluster_cf_acquire', 13),
		'CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT',
		'CF result 13 is mapped to its exact source enum name');
	is($quad->_two_stage_named_product_result('cluster_cf_acquire', 999),
		'UNCLASSIFIED_INTERNAL_RESULT',
		'an unmapped numeric product result stays explicitly unclassified');
}

{
	my $root = tempdir(CLEANUP => 1);
	my $log = "$root/node.log";
	open(my $fh, '>', $log) or die "open $log: $!";
	close($fh) or die "close $log: $!";
	my $node = bless {
		logfile => $log,
		cf_ges_readiness => 'running|ready|1024',
	}, 'ClusterQuadLifecycleTestNode';
	my $quad = bless {
		nodes => [ $node ],
		two_stage_phase2_log_offsets => [ 0 ],
	}, 'PostgreSQL::Test::ClusterQuad';

	is($quad->_two_stage_observe_cf_ges_current(0)->{status}, 'READY',
		'CF/GES gate reads current phase, LMS, and GRD state without DEBUG logging');
	$node->{cf_ges_readiness} = 'running|starting|1024';
	is($quad->_two_stage_observe_cf_ges_current(0)->{status}, 'PENDING',
		'CF/GES gate remains pending until all current read-only state is ready');
}

{
	my $node = bless {
		formation_row => '0|4|true',
	}, 'ClusterQuadLifecycleTestNode';
	my $quad = bless {
		nodes => [ $node ],
	}, 'PostgreSQL::Test::ClusterQuad';
	is($quad->_two_stage_observe_formation_current(0)->{status}, 'READY',
		'clean formation epoch zero with four members and quorum is current');
	$node->{formation_row} = '0|3|false';
	is($quad->_two_stage_observe_formation_current(0)->{class},
		'FORMATION_NOT_CURRENT',
		'formation pending uses the frozen formation failure class');
}

{
	my $quad = bless {
		nodes => [ bless({}, 'ClusterQuadLifecycleTestNode') ],
		two_stage_observed_formation => 9,
	}, 'PostgreSQL::Test::ClusterQuad';
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterQuad::_two_stage_node_current_identity
		= sub {
			return {
				formation => 8, presented_incarnation => 3,
				admitted_incarnation => 3, admitted_epoch => 8,
				session_incarnation => 4, postmaster_start => 'current',
			};
		};
	is($quad->_two_stage_observe_admission_current(0)->{class},
		'ADMISSION_NOT_CURRENT',
		'admission drift uses the frozen admission failure class');
}

{
	my $quad = bless {
		two_stage_attempt => {
			attempt_id => 'abc', first_failure => undef,
			gates => { map { $_ => [] } 0 .. 3 },
		},
	}, 'PostgreSQL::Test::ClusterQuad';
	is($quad->_two_stage_observe_r4_sample_allowed(0)->{class},
		'R4_PREREQUISITE_BLOCKED',
		'R4 pending uses the frozen prerequisite failure class');
}

{
	my $quad = bless {
		two_stage_cleanup_registered => 1,
	}, 'ClusterQuadStopLifecycleTest';
	no warnings 'once';
	local @ClusterQuadStopLifecycleTest::ISA
		= ('PostgreSQL::Test::ClusterQuad');
	local *ClusterQuadStopLifecycleTest::_two_stage_cleanup_registered
		= sub { $_[0]->{exact_cleanup_calls}++; return 'CLEAN'; };
	local *ClusterQuadStopLifecycleTest::_two_stage_detach_voting_loops
		= sub { die 'generic detach must not bypass exact cleanup'; };
	ok(eval { $quad->stop_quad(); 1 },
		'two-stage stop uses the exact registered cleanup lifecycle');
	is($quad->{exact_cleanup_calls}, 1,
		'two-stage stop invokes exact cleanup once');
}

done_testing();
