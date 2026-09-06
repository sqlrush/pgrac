
# spec-5.19 D1 -- ClusterQuad: 4-instance harness for the Stage 5 integrated
# acceptance (4-node reconfig fault matrix + multi-node write-path workloads).
#
# Encapsulates the boilerplate that the Stage 5 integrated-acceptance TAPs
# (t/32x reconfig matrix / HW-extend workload / production-bench-subset) need
# to spin up four cooperating pgrac instances with mutual interconnect:
#   - allocate 8 random free ports (4 PG + 4 IC)
#   - init all four nodes
#   - append cluster.* GUCs (cluster.enabled = on, tier = tier1, node_id 0..3)
#   - write mutually-trusting pgrac.conf to all four data dirs
#   - start_quad / stop_quad helpers
#
# Mirrors PostgreSQL::Test::ClusterTriple (spec-2.36 D15) and
# PostgreSQL::Test::ClusterPair (spec-2.2 D15) with an extra node slot.  It is
# the substrate for the first 4-node reconfig fault matrix, and is reused by
# spec-5.20 (Hang Manager acceptance) and spec-5.21 (Stage 5 beta close-out).
#
# Reconfig-leg API (used by the fault-matrix TAPs):
#   - kill_node9($i)   : SIGKILL the postmaster (fail-stop / spec-5.14 leg).
#   - leave_node($i)   : cooperative clean-leave (spec-5.13) -- runs
#                        pg_cluster_clean_leave_request() on node $i.
#   - stop_node($i)    : graceful stop (node goes ABSENT without a marker).
#   - join_node($i)    : peer-restart rejoin (spec-5.15) -- starts a node that
#                        was previously down/absent so it re-enters the live
#                        membership via the coordinator two-phase epoch
#                        protocol.  (There is no pg_cluster_join() UDF: 5.15
#                        join is driven by the node coming back online.)
#   - remove_node($coord_i, $target_id)
#                      : permanent removal (spec-5.18) -- runs
#                        pg_cluster_remove_node($target_id) on a surviving
#                        coordinator node.
#
# NOTE: the join-remaster (spec-5.16) leg of the fault matrix is gated on
# spec-5.16 landing on origin/main;  this harness exposes the substrate but
# does not itself depend on 5.16.
#
# Spec authority: pgrac:specs/spec-5.19-stage5-integrated-acceptance.md §1.2 D1.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterQuad;

use strict;
use warnings;

use Cwd qw(abs_path);
use Digest::SHA qw(sha256_hex);
use File::Basename qw(basename dirname);
use File::Path qw(make_path);
use File::Temp qw(tempfile);
use Fcntl qw(O_RDONLY);
use IO::Handle;
use IPC::Run qw(finish run start timeout);
use JSON::PP ();
use POSIX qw(strftime WNOHANG);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterVotingDisk qw(format_voting_file);
use PostgreSQL::Test::Utils;
use Time::HiRes qw(clock_gettime time usleep CLOCK_MONOTONIC);


our $NODES = 4;
our @TWO_STAGE_LOOP_QUADS;
our @TWO_STAGE_PHASE2_GATES = qw(
	DEVICE_STATIC_ATTESTED
	DEVICE_IO_QUALIFIED
	VOTING_OPEN_CURRENT
	WAL_ACTIVE_PUBLISHED
	CF_GES_CURRENT
	FORMATION_CURRENT
	ADMISSION_CURRENT
	R4_SAMPLE_ALLOWED);
our %TWO_STAGE_PHASE2_GATE_INDEX = map {
	$TWO_STAGE_PHASE2_GATES[$_] => $_
} 0 .. $#TWO_STAGE_PHASE2_GATES;
our %TWO_STAGE_PHASE2_FAILURE_CLASS = (
	DEVICE_STATIC_ATTESTED => 'DEVICE_STATIC_MISMATCH',
	DEVICE_IO_QUALIFIED => 'BLOCK_DEVICE_UNQUALIFIED',
	VOTING_OPEN_CURRENT => 'VOTING_OPEN_BLOCKED',
	WAL_ACTIVE_PUBLISHED => 'WAL_ACTIVE_BLOCKED',
	CF_GES_CURRENT => 'CF_GES_NOT_CURRENT',
	FORMATION_CURRENT => 'FORMATION_NOT_CURRENT',
	ADMISSION_CURRENT => 'ADMISSION_NOT_CURRENT',
	R4_SAMPLE_ALLOWED => 'R4_PREREQUISITE_BLOCKED',
);
our $TWO_STAGE_ATTEMPT_SEQUENCE = 0;

use constant TWO_STAGE_VOTING_BYTES => 525824;
use constant VOTING_SLOT_BYTES => 512;


sub _run_capture
{
	my ($cmd, $label) = @_;
	my ($stdout, $stderr) = ('', '');

	run($cmd, '>', \$stdout, '2>', \$stderr)
	  or die "$label failed: $stderr";
	chomp($stdout);
	return $stdout;
}


sub _two_stage_register_cleanup_owner
{
	my ($self) = @_;

	return if $self->{two_stage_cleanup_registered};
	$self->{two_stage_loop_devices} //= [];
	$self->{two_stage_loop_records} //= [];
	my $monotonic = clock_gettime(CLOCK_MONOTONIC);
	my $attempt_id = sprintf('%x%016x%08x', $$,
		int($monotonic * 1_000_000), ++$TWO_STAGE_ATTEMPT_SEQUENCE);
	$self->{two_stage_attempt} = {
		attempt_id => $attempt_id,
		first_failure => undef,
		gates => { map { $_ => [] } 0 .. $NODES - 1 },
		cleanup_deadline => $self->{two_stage_cleanup_deadline}
		  // ($monotonic + $PostgreSQL::Test::Utils::timeout_default),
		cleanup_deadline_wallclock => time()
		  + $PostgreSQL::Test::Utils::timeout_default,
		cleanup_timeout => $PostgreSQL::Test::Utils::timeout_default,
	};
	$self->{two_stage_cleanup_registered} = 1;
	push @TWO_STAGE_LOOP_QUADS, $self;
	return;
}


sub _two_stage_normalize_gate_result
{
	my ($result) = @_;

	if (ref($result) eq 'HASH')
	{
		my $status = $result->{status} // '';

		die "invalid Phase-2 gate result status"
		  unless $status eq 'READY' || $status eq 'FAILED';
		if ($status eq 'READY')
		{
			die "READY Phase-2 gate result carries failure fields"
			  if defined($result->{class}) || defined($result->{detail});
			return ('READY', undef, undef);
		}
		my $class = $result->{class} // '';
		my $detail = $result->{detail} // '';

		die "FAILED Phase-2 gate result lacks a semantic class"
		  unless $class =~ /\A[A-Z][A-Z0-9_]*\z/;
		die "invalid Phase-2 gate failure detail" if ref($detail);
		return ('FAILED', $class, "$detail");
	}
	die "invalid reference Phase-2 gate result" if ref($result);
	return ('READY', undef, undef)
	  if defined($result) && $result eq 'READY';
	if (defined($result) && $result =~ /\A\d+\z/)
	{
		return ('FAILED', 'UNCLASSIFIED_INTERNAL_RESULT',
			"result=$result");
	}
	die "invalid scalar Phase-2 gate result";
}


sub _two_stage_record_first_failure
{
	my ($self, $class, $detail, $node_id, $gate) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "Phase-2 gate recorder lacks an attempt";
	my $first = $attempt->{first_failure};

	die "invalid Phase-2 first failure class"
	  unless defined($class) && $class =~ /\A[A-Z][A-Z0-9_]*\z/;
	die "invalid Phase-2 first failure detail" if ref($detail);
	if (defined($first))
	{
		return $first
		  if $first->{class} eq $class
		  && $first->{detail} eq ($detail // '')
		  && (!defined($node_id) || $first->{node_id} == $node_id)
		  && (!defined($gate) || $first->{gate} eq $gate);
		die "Phase-2 first failure is immutable";
	}
	$attempt->{first_failure} = {
		class => $class,
		detail => $detail // '',
		node_id => $node_id,
		gate => $gate,
	};
	return $attempt->{first_failure};
}


sub _two_stage_require_gate_prefix
{
	my ($self, $node_id, $expected_gate) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "Phase-2 gate recorder lacks an attempt";
	my $index = $TWO_STAGE_PHASE2_GATE_INDEX{$expected_gate};
	my %ready;

	die "invalid Phase-2 node id" unless defined($node_id)
	  && $node_id =~ /\A\d+\z/ && $node_id < $NODES;
	die "unknown Phase-2 gate" unless defined($index);
	for my $event (@{ $attempt->{gates}{$node_id} })
	{
		$ready{$event->{gate}} = 1 if $event->{result} eq 'READY';
	}
	for my $predecessor (0 .. $index - 1)
	{
		die "Phase-2 gate $expected_gate skipped predecessor "
		  . $TWO_STAGE_PHASE2_GATES[$predecessor]
		  unless $ready{$TWO_STAGE_PHASE2_GATES[$predecessor]};
	}
	return 1;
}


sub _two_stage_record_gate
{
	my ($self, $node_id, $gate, $raw_result, $evidence_digest) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "Phase-2 gate recorder lacks an attempt";
	my $index = $TWO_STAGE_PHASE2_GATE_INDEX{$gate};
	my ($result, $observed_class, $detail)
	  = _two_stage_normalize_gate_result($raw_result);
	my $events;
	my $first = $attempt->{first_failure};
	my $first_error_class;

	die "invalid Phase-2 node id" unless defined($node_id)
	  && $node_id =~ /\A\d+\z/ && $node_id < $NODES;
	die "unknown Phase-2 gate" unless defined($index);
	die "invalid Phase-2 evidence digest"
	  unless defined($evidence_digest) && !ref($evidence_digest)
	  && length($evidence_digest) > 0;
	$events = $attempt->{gates}{$node_id};
	for my $existing (@$events)
	{
		next unless $existing->{gate} eq $gate;
		my $expected_class = $observed_class;
		if ($result eq 'FAILED' && defined($first)
			&& !($first->{node_id} == $node_id && $first->{gate} eq $gate))
		{
			$expected_class = "CASCADE_FROM($first->{class})";
		}
		return $existing
		  if $existing->{result} eq $result
		  && ($existing->{first_error_class} // '') eq ($expected_class // '')
		  && ($existing->{detail} // '') eq ($detail // '')
		  && $existing->{evidence_digest} eq $evidence_digest;
		die "non-identical duplicate Phase-2 gate event";
	}
	if ($result eq 'READY')
	{
		if (defined($first))
		{
			my $failed_index = $TWO_STAGE_PHASE2_GATE_INDEX{$first->{gate}};
			die "successful Phase-2 gate follows the first failure"
			  if defined($failed_index) && $index > $failed_index;
		}
		$self->_two_stage_require_gate_prefix($node_id, $gate);
	}
	elsif (!defined($first))
	{
		$self->_two_stage_require_gate_prefix($node_id, $gate);
		$self->_two_stage_record_first_failure(
			$observed_class, $detail, $node_id, $gate);
		$first = $attempt->{first_failure};
		$first_error_class = $observed_class;
	}
	else
	{
		$first_error_class = "CASCADE_FROM($first->{class})";
	}

	my $identity = $self->{two_stage_phase2_boot_identity};
	my $boot = ref($identity) eq 'ARRAY' ? $identity->[$node_id] : undef;
	my $event = {
		attempt_id => $attempt->{attempt_id},
		phase => 2,
		node_id => $node_id + 0,
		boot_pid => ref($boot) eq 'HASH' ? $boot->{pid} : undef,
		boot_starttime => ref($boot) eq 'HASH' ? $boot->{starttime} : undef,
		gate => $gate,
		monotonic_timestamp => clock_gettime(CLOCK_MONOTONIC),
		result => $result,
		first_error_class => $first_error_class,
		detail => $detail,
		evidence_digest => "$evidence_digest",
	};
	push @$events, $event;
	return $event;
}


sub _two_stage_gate_report
{
	my ($self) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "Phase-2 gate recorder lacks an attempt";
	my %gates = map {
		$_ => [ map { +{ %$_ } } @{ $attempt->{gates}{$_} } ]
	} 0 .. $NODES - 1;

	return {
		attempt_id => $attempt->{attempt_id},
		cleanup_deadline => $attempt->{cleanup_deadline},
		first_failure => defined($attempt->{first_failure})
		  ? { %{ $attempt->{first_failure} } } : undef,
		gates => \%gates,
	};
}


sub _two_stage_phase2_log_slice
{
	my ($self, $node_id) = @_;
	my $node = $self->{nodes}[$node_id]
	  or die "Phase-2 log observer lacks node $node_id";
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	my $offset = $self->{two_stage_phase2_log_offsets}[$node_id] // 0;

	die "Phase-2 log boundary exceeds current log size"
	  if $offset < 0 || $offset > length($log);
	return substr($log, $offset);
}


sub _two_stage_named_product_result
{
	my ($self, $scope, $result) = @_;
	my %names = (
		'cluster_cf_acquire:13' => 'CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT',
	);

	return $names{"$scope:$result"} // 'UNCLASSIFIED_INTERNAL_RESULT';
}


sub _two_stage_observer_ready
{
	my ($digest, $detail) = @_;
	return {
		status => 'READY',
		detail => $detail // '',
		evidence_digest => $digest,
	};
}


sub _two_stage_observer_pending
{
	my ($class, $detail, $digest) = @_;
	return {
		status => 'PENDING', class => $class, detail => $detail,
		evidence_digest => $digest,
	};
}


sub _two_stage_observer_failed
{
	my ($class, $detail, $digest) = @_;
	return {
		status => 'FAILED', class => $class, detail => $detail,
		evidence_digest => $digest,
	};
}


sub _two_stage_observe_device_static_attested
{
	my ($self, $node_id) = @_;
	my $digest = $self->{two_stage_static_attestation_digest};
	return _two_stage_observer_failed('DEVICE_STATIC_MISMATCH',
		'authoritative voting devices lack exact static attestation',
		"node$node_id-static-missing")
	  unless defined($digest) && length($digest) > 0
	  && @{ $self->{two_stage_loop_records} // [] } == 3;
	return _two_stage_observer_ready($digest, 'three exact voting devices');
}


sub _two_stage_observe_device_io_qualified
{
	my ($self, $node_id) = @_;
	my $digest = $self->{two_stage_scratch_probe_digest};
	return _two_stage_observer_failed('BLOCK_DEVICE_UNQUALIFIED',
		'independent scratch direct-I/O probe is not complete',
		"node$node_id-io-unqualified")
	  unless $self->{two_stage_device_io_qualified}
	  && defined($digest) && length($digest) > 0;
	return _two_stage_observer_ready($digest, '64 scratch sequences complete');
}


sub _two_stage_node_voting_devices_open
{
	my ($self, $node_id) = @_;
	my $boot = $self->{two_stage_phase2_boot_identity}[$node_id];
	my $devices = $self->{two_stage_loop_devices} // [];
	return 0 unless ref($boot) eq 'HASH' && @$devices == 3;
	my $current = $self->_two_stage_proc_identity($boot->{pid});
	return 0 unless defined($current) && !$current->{unavailable}
	  && (!exists($current->{exists}) || $current->{exists})
	  && ($current->{starttime} // -1) == $boot->{starttime};
	my %descendant = ($boot->{pid} => 1);
	my $parents = $self->_two_stage_process_parent_map();
	my $changed = 1;
	while ($changed)
	{
		$changed = 0;
		for my $pid (keys %$parents)
		{
			next if $descendant{$pid} || !$descendant{ $parents->{$pid} };
			$descendant{$pid} = 1;
			$changed = 1;
		}
	}
	my %device_identity;
	for my $device (@$devices)
	{
		my @st = stat($device);
		return 0 unless @st;
		$device_identity{"$st[0]:$st[1]"} = $device;
	}
	for my $pid (keys %descendant)
	{
		my %opened;
		for my $fd_path (glob("/proc/$pid/fd/[0-9]*"))
		{
			my @st = stat($fd_path);
			next unless @st;
			my $device = $device_identity{"$st[0]:$st[1]"};
			$opened{$device} = 1 if defined($device);
		}
		return 1 if keys(%opened) == @$devices;
	}
	return 0;
}


sub _two_stage_observe_voting_open_current
{
	my ($self, $node_id) = @_;
	my $node = $self->{nodes}[$node_id];
	my $expected = join(',', @{ $self->{two_stage_loop_devices} // [] });
	$self->_two_stage_capture_phase2_boot_identity_current();
	return _two_stage_observer_pending('VOTING_OPEN_BLOCKED',
		'exact current-boot postmaster identity is not yet visible',
		sha256_hex("node$node_id-boot-identity-pending"))
	  unless ref($self->{two_stage_phase2_boot_identity}[$node_id]) eq 'HASH';
	my $effective = eval { $self->_two_stage_effective_voting_paths($node) };
	my $detail = $@;
	return _two_stage_observer_pending('VOTING_OPEN_BLOCKED',
		"cannot read current voting paths: $detail",
		sha256_hex("node$node_id-voting-path-unavailable")) if $@;
	return _two_stage_observer_failed('VOTING_OPEN_BLOCKED',
		"effective voting paths are not exact current devices",
		sha256_hex("node$node_id-effective=$effective"))
	  unless defined($effective) && $effective eq $expected;
	return _two_stage_observer_pending('VOTING_OPEN_BLOCKED',
		'exact current-boot voting device FDs are not open',
		sha256_hex("node$node_id-voting-fd-pending"))
	  unless $self->_two_stage_node_voting_devices_open($node_id);
	return _two_stage_observer_ready(
		sha256_hex("node$node_id|$effective|fds=current"),
		'exact current-boot voting FDs are open');
}


sub _two_stage_crc32c
{
	my ($bytes) = @_;
	my $crc = 0xffffffff;
	for my $byte (unpack('C*', $bytes))
	{
		$crc ^= $byte;
		for (1 .. 8)
		{
			$crc = ($crc >> 1) ^ (($crc & 1) ? 0x82f63b78 : 0);
		}
	}
	return ($crc ^ 0xffffffff) & 0xffffffff;
}


sub _two_stage_wal_slot_active_current
{
	my ($self, $node_id) = @_;
	my $root = $self->{wal_threads_root};
	return 0 unless defined($root);
	my $path = "$root/pgrac_wal_state";
	open(my $fh, '<:raw', $path) or return 0;
	my $thread_id = $node_id + 1;
	seek($fh, 512 * $thread_id, 0) or do { close($fh); return 0; };
	my $read = read($fh, my $slot, 512);
	close($fh);
	return 0 unless defined($read) && $read == 512;
	my $magic = unpack('V', substr($slot, 0, 4));
	my $version = unpack('v', substr($slot, 4, 2));
	my $stored_thread = unpack('v', substr($slot, 6, 2));
	my $stored_node = unpack('l', substr($slot, 8, 4));
	my $state = unpack('V', substr($slot, 12, 4));
	my $stored_crc = unpack('V', substr($slot, 504, 4));
	return $magic == 0x50475754 && $version == 1
	  && $stored_thread == $thread_id && $stored_node == $node_id
	  && $state == 1
	  && $stored_crc == _two_stage_crc32c(substr($slot, 0, 504));
}


sub _two_stage_observe_wal_active_current
{
	my ($self, $node_id) = @_;
	my $log = $self->_two_stage_phase2_log_slice($node_id);
	my $thread = $node_id + 1;
	my $marker = "pgrac WAL thread $thread published ACTIVE in the WAL state registry";
	return _two_stage_observer_pending('WAL_ACTIVE_BLOCKED',
		'current-boot WAL ACTIVE publication is not visible',
		sha256_hex("node$node_id-wal-active-pending"))
	  unless index($log, $marker) >= 0
	  && $self->_two_stage_wal_slot_active_current($node_id);
	return _two_stage_observer_ready(
		sha256_hex("node$node_id|thread=$thread|wal=ACTIVE"),
		$marker);
}


sub _two_stage_observe_cf_ges_current
{
	my ($self, $node_id) = @_;
	my $node = $self->{nodes}[$node_id];
	my $log = $self->_two_stage_phase2_log_slice($node_id);
	if ($log =~ /cluster CF acquire failed \(mode (\d+), result (\d+)\)/)
	{
		my ($mode, $result) = ($1, $2);
		my $name = $self->_two_stage_named_product_result(
			'cluster_cf_acquire', $result);
		return _two_stage_observer_failed('CF_GES_NOT_CURRENT',
			"mode=$mode result=$result name=$name",
			sha256_hex("node$node_id|cf-mode=$mode|result=$result|$name"));
	}
	return _two_stage_observer_failed('CF_GES_NOT_CURRENT',
		'current Phase-2 phase4 failed before CF/GES became current',
		sha256_hex("node$node_id-phase4-failed"))
	  if $log =~ /cluster startup phase phase4_normal failed:/;
	my $readiness = eval { $node->safe_psql('postgres', q{
		SELECT p.value || '|' || l.value || '|' || g.value
		  FROM pg_cluster_state p
		  JOIN pg_cluster_state l
		    ON l.category = 'lms' AND l.key = 'lms_state'
		  JOIN pg_cluster_state g
		    ON g.category = 'grd' AND g.key = 'grd_max_entries'
		 WHERE p.category = 'phase' AND p.key = 'cluster_phase'
	}) };
	return _two_stage_observer_pending('CF_GES_NOT_CURRENT',
		'current Phase-2 CF/GES readiness is not yet current',
		sha256_hex("node$node_id-cf-ges-pending"))
	  if $@ || !defined($readiness)
	  || $readiness !~ /^running\|ready\|([1-9]\d*)$/;
	return _two_stage_observer_ready(
		sha256_hex("node$node_id|cf-ges=$readiness"),
		"current Phase-2 CF/GES service is $readiness");
}


sub _two_stage_observe_formation_current
{
	my ($self, $node_id) = @_;
	my $node = $self->{nodes}[$node_id];
	my $row = eval { $node->safe_psql('postgres', q{
		SELECT q.current_epoch_at_boot::text || '|' ||
		       (SELECT count(*) FROM pg_cluster_membership
		         WHERE state = 'member')::text || '|' || q.in_quorum::text
		  FROM pg_cluster_quorum_state q
		 LIMIT 1
	}) };
	return _two_stage_observer_pending('FORMATION_NOT_CURRENT',
		'current formation query is unavailable',
		sha256_hex("node$node_id-formation-query-pending")) if $@;
	return _two_stage_observer_pending('FORMATION_NOT_CURRENT',
		"current formation is incomplete: $row",
		sha256_hex("node$node_id|formation=$row"))
	  unless defined($row) && $row =~ /^(\d+)\|4\|(?:t|true)$/;
	my $formation = $1 + 0;
	$self->{two_stage_observed_formation} //= $formation;
	return _two_stage_observer_failed('FORMATION_NOT_CURRENT',
		'four nodes observed different current formations',
		sha256_hex("node$node_id|formation=$formation|expected="
			. $self->{two_stage_observed_formation}))
	  unless $formation == $self->{two_stage_observed_formation};
	return _two_stage_observer_ready(
		sha256_hex("node$node_id|formation=$formation|members=4"),
		"formation=$formation");
}


sub _two_stage_observe_admission_current
{
	my ($self, $node_id) = @_;
	my $identity = eval {
		$self->_two_stage_node_current_identity($self->{nodes}[$node_id], $node_id)
	};
	return _two_stage_observer_pending('ADMISSION_NOT_CURRENT',
		'current admission identity is unavailable',
		sha256_hex("node$node_id-admission-pending")) if $@;
	return _two_stage_observer_failed('ADMISSION_NOT_CURRENT',
		'admission identity does not match the current formation',
		sha256_hex("node$node_id-admission-formation-drift"))
	  unless $identity->{formation} == ($self->{two_stage_observed_formation} // -1)
	  && $identity->{presented_incarnation} == $identity->{admitted_incarnation}
	  && $identity->{admitted_epoch} == $identity->{formation};
	$self->{two_stage_phase2_identity}[$node_id] = $identity;
	if ($node_id == $NODES - 1)
	{
		my $valid = eval {
			$self->_two_stage_validate_phase2_current(
				$self->{two_stage_phase2_identity});
			1;
		};
		return _two_stage_observer_failed('ADMISSION_NOT_CURRENT',
			"full Phase-2 identity revalidation failed: $@",
			sha256_hex("node$node_id-phase2-revalidation-failed"))
		  unless $valid;
	}
	return _two_stage_observer_ready(
		sha256_hex(join('|', "node$node_id", map {
			$identity->{$_}
		} qw(formation presented_incarnation admitted_incarnation
			admitted_epoch session_incarnation postmaster_start))),
		'current membership/incarnation admission is exact');
}


sub _two_stage_observe_r4_sample_allowed
{
	my ($self, $node_id) = @_;
	my $report = $self->_two_stage_gate_report();
	for my $member (0 .. $NODES - 1)
	{
		my %ready = map { $_->{gate} => $_->{result} eq 'READY' }
		  @{ $report->{gates}{$member} };
		for my $gate (@TWO_STAGE_PHASE2_GATES[0 .. 6])
		{
			return _two_stage_observer_pending('R4_PREREQUISITE_BLOCKED',
				"node$member predecessor $gate is not ready",
				sha256_hex("node$node_id-r4-blocked-$member-$gate"))
			  unless $ready{$gate};
		}
	}
	return _two_stage_observer_ready(
		sha256_hex("node$node_id-r4-sample-allowed"),
		'all seven Phase-2 predecessor gates are current');
}


sub _two_stage_observe_gate
{
	my ($self, $node_id, $gate) = @_;
	my $fixture = $self->{two_stage_observer_fixtures}{$node_id}{$gate};
	return { %$fixture } if ref($fixture) eq 'HASH';
	my %observer = (
		DEVICE_STATIC_ATTESTED => '_two_stage_observe_device_static_attested',
		DEVICE_IO_QUALIFIED => '_two_stage_observe_device_io_qualified',
		VOTING_OPEN_CURRENT => '_two_stage_observe_voting_open_current',
		WAL_ACTIVE_PUBLISHED => '_two_stage_observe_wal_active_current',
		CF_GES_CURRENT => '_two_stage_observe_cf_ges_current',
		FORMATION_CURRENT => '_two_stage_observe_formation_current',
		ADMISSION_CURRENT => '_two_stage_observe_admission_current',
		R4_SAMPLE_ALLOWED => '_two_stage_observe_r4_sample_allowed',
	);
	my $method = $observer{$gate} or die "unknown Phase-2 observer gate $gate";
	return $self->$method($node_id);
}


sub _two_stage_wait_phase2_gate_ladder
{
	my ($self) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "Phase-2 gate ladder lacks an attempt";
	my $deadline = $attempt->{cleanup_deadline};

	die "Phase-2 gate ladder lacks the original absolute deadline"
	  unless defined($deadline);

	for my $gate (@TWO_STAGE_PHASE2_GATES)
	{
		my %pending = map { $_ => 1 } 0 .. $NODES - 1;
		my %last_pending;
		my $wait_round = 0;

		while (keys %pending)
		{
			for my $node_id (sort { $a <=> $b } keys %pending)
			{
				my $observed = $self->_two_stage_observe_gate($node_id, $gate);
				die "Phase-2 observer returned an invalid result for $gate"
				  unless ref($observed) eq 'HASH'
				  && ($observed->{status} // '') =~ /^(?:READY|PENDING|FAILED)$/
				  && defined($observed->{evidence_digest})
				  && length($observed->{evidence_digest}) > 0;
				if ($observed->{status} eq 'READY')
				{
					$self->_two_stage_record_gate($node_id, $gate, 'READY',
						$observed->{evidence_digest});
					delete $pending{$node_id};
					next;
				}
				if ($observed->{status} eq 'PENDING')
				{
					$last_pending{$node_id} = { %$observed };
					next;
				}
				my $class = $observed->{class}
				  // $TWO_STAGE_PHASE2_FAILURE_CLASS{$gate};
				my $detail = $observed->{detail} // "$gate is not current";
				$self->_two_stage_record_gate($node_id, $gate, {
					status => 'FAILED', class => $class, detail => $detail,
				}, $observed->{evidence_digest});
				die "Phase-2 first failed gate $gate on node$node_id: "
				  . "$class: $detail\n";
			}
			last unless keys %pending;
			if ($self->_two_stage_monotonic_now() >= $deadline)
			{
				my ($node_id) = sort { $a <=> $b } keys %pending;
				my $observed = $last_pending{$node_id};
				my $class = $TWO_STAGE_PHASE2_FAILURE_CLASS{$gate};
				my $detail = "original absolute deadline expired";
				$detail .= ": $observed->{detail}"
				  if defined($observed->{detail}) && length($observed->{detail});
				$self->_two_stage_record_gate($node_id, $gate, {
					status => 'FAILED', class => $class, detail => $detail,
				}, $observed->{evidence_digest});
				die "Phase-2 first failed gate $gate on node$node_id: "
				  . "$class: $detail\n";
			}
			$self->_two_stage_wait_gate_observation_change(
				$deadline, $wait_round++);
		}
	}
	return 1;
}


sub _two_stage_monotonic_now
{
	return clock_gettime(CLOCK_MONOTONIC);
}


sub _two_stage_wait_gate_observation_change
{
	my ($self, $deadline, $round) = @_;
	my $remaining = $deadline - $self->_two_stage_monotonic_now();
	return if $remaining <= 0;

	# Use bounded exponential observation spacing, not a fixed sleep loop.  The
	# attempt deadline is immutable and always caps the next observation.
	my $shift = $round > 5 ? 5 : $round;
	my $delay = 0.01 * (2 ** $shift);
	$delay = 0.25 if $delay > 0.25;
	$delay = $remaining if $delay > $remaining;
	select(undef, undef, undef, $delay) if $delay > 0;
	return;
}


sub _two_stage_pid_alive
{
	my ($self, $pid) = @_;

	return defined($pid) && $pid =~ /^\d+$/ && $pid > 1 && kill(0, $pid);
}


sub _two_stage_process_parent_map
{
	my %ppid;

	for my $stat_path (glob('/proc/[0-9]*/stat'))
	{
		my ($pid) = $stat_path =~ m{/proc/(\d+)/stat\z};
		next unless defined($pid);
		open(my $fh, '<', $stat_path) or next;
		my $stat = <$fh>;
		close($fh);
		next unless defined($stat);
		my ($parent_pid) = $stat =~ /^\d+\s+\(.*\)\s+\S\s+(\d+)\s/;
		next unless defined($parent_pid);
		$ppid{$pid} = $parent_pid;
	}
	return \%ppid;
}


sub _two_stage_capture_phase1_actors
{
	my ($self) = @_;
	my %known;
	my $ppid;

	for my $node (@{ $self->{nodes} })
	{
		my $pid = $node->{_pid};

		die "phase-1 stop lacks a live postmaster pid"
		  unless $self->_two_stage_pid_alive($pid);
		$known{$pid} = 1;
	}
	$ppid = $self->_two_stage_process_parent_map();
	my $changed = 1;
	while ($changed)
	{
		$changed = 0;
		for my $pid (keys %$ppid)
		{
			next if $known{$pid} || !$known{ $ppid->{$pid} };
			$known{$pid} = 1;
			$changed = 1;
		}
	}
	$self->{two_stage_phase1_actor_pids}
	  = [ sort { $a <=> $b } keys %known ];
	return;
}


sub _two_stage_capture_cleanup_processes
{
	my ($self) = @_;
	my %owner;
	my $parents = $self->_two_stage_process_parent_map();

	for my $i (0 .. $#{ $self->{nodes} // [] })
	{
		my $node = $self->{nodes}[$i];
		my $pid = $node ? $node->{_pid} : undef;
		$owner{$pid} = $i
		  if defined($pid) && $pid =~ /^\d+$/ && $pid > 1;
	}
	my $changed = 1;
	while ($changed)
	{
		$changed = 0;
		for my $pid (keys %$parents)
		{
			next if exists($owner{$pid}) || !exists($owner{ $parents->{$pid} });
			$owner{$pid} = $owner{ $parents->{$pid} };
			$changed = 1;
		}
	}
	my @processes;
	for my $pid (sort { $a <=> $b } keys %owner)
	{
		my $identity = $self->_two_stage_proc_identity($pid);
		next unless defined($identity) && !$identity->{unavailable}
		  && (!exists($identity->{exists}) || $identity->{exists})
		  && defined($identity->{starttime});
		push @processes, {
			node => $owner{$pid}, pid => $pid + 0,
			starttime => $identity->{starttime} + 0,
			last_state => $identity->{state} // 'UNAVAILABLE',
		};
	}
	my %captured = map {
		($_->{pid} . ':' . $_->{starttime}) => 1
	} @processes;
	my $scratch = $self->{two_stage_scratch};
	my $scratch_actors = ref($scratch) eq 'HASH'
	  ? ($scratch->{child_processes} // []) : [];
	for my $actor (@$scratch_actors)
	{
		die "scratch cleanup actor lacks exact pid/starttime"
		  unless ref($actor) eq 'HASH'
		  && defined($actor->{node})
		  && defined($actor->{pid}) && $actor->{pid} =~ /^\d+$/
		  && defined($actor->{starttime}) && $actor->{starttime} =~ /^\d+$/;
		my $key = $actor->{pid} . ':' . $actor->{starttime};
		next if $captured{$key}++;
		my $record = {
			node => $actor->{node} + 0,
			pid => $actor->{pid} + 0,
			starttime => $actor->{starttime} + 0,
			last_state => $actor->{last_state} // 'UNAVAILABLE',
		};
		$record->{actor} = $actor->{actor} if defined($actor->{actor});
		push @processes, $record;
	}
	@processes = sort { $a->{pid} <=> $b->{pid} } @processes;
	return \@processes;
}


sub _two_stage_open_backing_fd_holders
{
	my ($self) = @_;
	my %backing_identity;
	my @holders;

	for my $path (@{ $self->{voting_disk_paths} // [] })
	{
		my @st = stat($path);

		die "stat voting backing $path failed: $!" unless @st;
		$backing_identity{"$st[0]:$st[1]"} = $path;
	}
	for my $fd_path (glob('/proc/[0-9]*/fd/[0-9]*'))
	{
		my @st = stat($fd_path);
		next unless @st;
		my $path = $backing_identity{"$st[0]:$st[1]"};
		next unless defined($path);
		my ($pid, $fd) = $fd_path =~ m{/proc/(\d+)/fd/(\d+)\z};
		push @holders, "pid=$pid fd=$fd path=$path";
	}
	return \@holders;
}


sub _two_stage_assert_phase1_offline
{
	my ($self) = @_;
	my $stops = $self->{two_stage_native_stop_success};

	die "phase-1 attach lacks four successful native stop results"
	  unless ref($stops) eq 'ARRAY' && @$stops == $NODES
	  && !grep { !$_ } @$stops;
	for my $node (@{ $self->{nodes} })
	{
		die "phase-1 attach found a live postmaster"
		  if defined($node->{_pid}) && $node->{_pid} != 0;
	}
	for my $pid (@{ $self->{two_stage_phase1_actor_pids} // [] })
	{
		die "phase-1 attach found surviving actor pid=$pid"
		  if $self->_two_stage_pid_alive($pid);
	}
	my $holders = $self->_two_stage_open_backing_fd_holders();
	die "phase-1 attach found open voting backing FDs: "
	  . join(', ', @$holders) if @$holders;
	return 1;
}


sub _two_stage_wait_cleanup_processes
{
	my ($self, $processes, $deadline) = @_;

	die "cleanup closure lacks an exact process set"
	  unless ref($processes) eq 'ARRAY';
	die "cleanup closure lacks the original absolute deadline"
	  unless defined($deadline);
	my $wait_round = 0;
	while (1)
	{
		my $live_exact = 0;
		for my $record (@$processes)
		{
			if (($record->{actor} // '') eq 'scratch_probe')
			{
				my $waited = waitpid($record->{pid}, WNOHANG);
				if ($waited == $record->{pid})
				{
					$record->{last_state} = 'EXITED';
					next;
				}
			}
			my $current = $self->_two_stage_proc_identity($record->{pid});
			if (!defined($current) || $current->{unavailable})
			{
				$live_exact = 1;
				$record->{last_state} = 'UNAVAILABLE';
				next;
			}
			next if exists($current->{exists}) && !$current->{exists};
			return 'CLEANUP_IDENTITY_CONFLICT'
			  unless defined($current->{starttime})
			  && $current->{starttime} == $record->{starttime};
			$live_exact = 1;
			$record->{last_state} = $current->{state} // 'UNAVAILABLE';
		}
		return 'EXITED' unless $live_exact;
		return 'OPERATOR_CLEANUP_REQUIRED'
		  if $self->_two_stage_monotonic_now() >= $deadline;
		$self->_two_stage_wait_gate_observation_change(
			$deadline, $wait_round++);
	}
}


sub _two_stage_signal_exact_process
{
	my ($self, $record) = @_;

	die "cleanup stop lacks exact pid/starttime"
	  unless ref($record) eq 'HASH'
	  && defined($record->{pid}) && $record->{pid} =~ /^\d+$/
	  && $record->{pid} > 1
	  && defined($record->{starttime})
	  && $record->{starttime} =~ /^\d+$/;
	my $current = $self->_two_stage_proc_identity($record->{pid});
	return 1 if defined($current)
	  && exists($current->{exists}) && !$current->{exists};
	die "cleanup stop cannot revalidate pid=$record->{pid}"
	  if !defined($current) || $current->{unavailable};
	die "cleanup stop identity drift for pid=$record->{pid}"
	  unless defined($current->{starttime})
	  && $current->{starttime} == $record->{starttime};

	# pg_ctl -m immediate ultimately delivers SIGQUIT but waits by default.
	# Deliver that exact, already-validated stop edge directly so a postmaster
	# blocked in uninterruptible voting I/O cannot block the TAP event loop.
	return 1 if kill('QUIT', $record->{pid}) == 1;
	$current = $self->_two_stage_proc_identity($record->{pid});
	return 1 if defined($current)
	  && exists($current->{exists}) && !$current->{exists};
	die "cleanup stop signal failed for pid=$record->{pid}: $!";
}


sub _two_stage_request_failure_stops_once
{
	my ($self, $processes) = @_;

	die "cleanup stop request lacks exact process ownership"
	  unless ref($processes) eq 'ARRAY';
	return $self->{two_stage_failure_stop_errors}
	  if ref($self->{two_stage_failure_stop_errors}) eq 'ARRAY';

	my @errors;
	my @requests;
	# Publish the idempotence guard before the first signal.  END cleanup must
	# never issue a second signal even if one request below fails.
	$self->{two_stage_failure_stop_errors} = \@errors;
	$self->{two_stage_failure_stop_requests} = \@requests;
	for my $i (0 .. $#{ $self->{nodes} // [] })
	{
		my $node = $self->{nodes}[$i];
		next unless $node;
		my $pid = $node->{_pid};
		my $request = { node => $i + 0, signal => 'SIGQUIT' };

		# Disable PostgreSQL::Test::Cluster's generic END teardown.  Exact
		# process ownership now lives in $processes and, if necessary, the
		# persistent cleanup manifest.
		$node->{_pid} = undef;
		if (!defined($pid) || $pid !~ /^\d+$/ || $pid <= 1)
		{
			$request->{status} = 'ALREADY_EXITED';
			push @requests, $request;
			next;
		}
		$request->{pid} = $pid + 0;
		my @exact = grep { ($_->{pid} // -1) == $pid } @$processes;
		if (@exact != 1)
		{
			my $current = $self->_two_stage_proc_identity($pid);
			if (defined($current)
				&& exists($current->{exists}) && !$current->{exists})
			{
				$request->{status} = 'ALREADY_EXITED';
				push @requests, $request;
				next;
			}
			$request->{status} = 'IDENTITY_UNAVAILABLE';
			push @requests, $request;
			push @errors,
			  "node$i cleanup stop lacks one captured exact identity for pid=$pid";
			next;
		}
		$request->{starttime} = $exact[0]{starttime} + 0;
		my $sent = eval {
			$self->_two_stage_signal_exact_process($exact[0]);
			1;
		};
		if ($sent)
		{
			$request->{status} = 'STOP_REQUESTED';
		}
		else
		{
			$request->{status} = 'REQUEST_FAILED';
			push @errors, $@ || "node$i exact cleanup stop failed";
		}
		push @requests, $request;
	}
	return \@errors;
}


sub _two_stage_cleanup_registered
{
	my ($self, %opts) = @_;
	return $self->{two_stage_cleanup_terminal}
	  if defined($self->{two_stage_cleanup_terminal});
	if ($self->{two_stage_cleanup_started})
	{
		$self->{two_stage_cleanup_terminal} = 'CLEANUP_FAILED';
		return $self->{two_stage_cleanup_terminal};
	}
	my $attempt = $self->{two_stage_attempt};
	# Startup/failure deadlines remain absolute.  An explicit normal stop of
	# a completely started quad owns a separate, one-shot budget of the same
	# duration; ordinary uptime must not preconsume its process-exit wait.
	# END, failed starts and explicitly failed teardown never enter this path.
	if ($opts{normal_stop} && $self->{two_stage_start_completed}
		&& ref($attempt) eq 'HASH' && !defined($attempt->{first_failure})
		&& !defined($attempt->{normal_cleanup_deadline}))
	{
		$attempt->{normal_cleanup_deadline} = $self->_two_stage_monotonic_now()
		  + $attempt->{cleanup_timeout};
		$attempt->{normal_cleanup_deadline_wallclock} = time()
		  + $attempt->{cleanup_timeout};
	}
	my @errors;
	my @processes = @{ $self->_two_stage_capture_cleanup_processes() };
	my $original_failure = ref($attempt) eq 'HASH'
	  && ref($attempt->{first_failure}) eq 'HASH'
	  ? $attempt->{first_failure}{class} : 'HARNESS_TEARDOWN';
	$self->{two_stage_cleanup_processes} = \@processes;
	$self->{two_stage_cleanup_started} = 1;
	if (ref($attempt) eq 'HASH'
		&& ref($attempt->{first_failure}) eq 'HASH')
	{
		my $captured = eval {
			$self->_two_stage_capture_failure_evidence();
			1;
		};
		push @errors, $@ || 'first-failure evidence capture failed'
		  unless $captured;
	}

	push @errors, @{ $self->_two_stage_request_failure_stops_once(\@processes) };
	my $process_terminal = $self->_two_stage_wait_cleanup_processes(
		\@processes, $attempt->{normal_cleanup_deadline}
		  // $attempt->{cleanup_deadline});
	if ($process_terminal ne 'EXITED')
	{
		$self->_two_stage_write_cleanup_terminal(
			$process_terminal, $original_failure)
		  if ref($attempt) eq 'HASH';
		$self->{two_stage_cleanup_terminal} = $process_terminal;
		return $process_terminal;
	}
	my @devices = map { $_->{path} }
	  @{ $self->{two_stage_loop_records} // [] };
	my @backings = map { $_->{backing_realpath} }
	  @{ $self->{two_stage_loop_records} // [] };
	if (ref($self->{two_stage_scratch}) eq 'HASH')
	{
		push @devices, $self->{two_stage_scratch}{device}
		  if defined($self->{two_stage_scratch}{device});
		push @backings, $self->{two_stage_scratch}{backing}
		  if defined($self->{two_stage_scratch}{backing});
	}
	my $holders = eval { $self->_two_stage_device_holders(\@devices, \@backings) };
	push @errors, $@ if $@;
	if (defined($holders) && @$holders)
	{
		$self->_two_stage_write_cleanup_terminal(
			'OPERATOR_CLEANUP_REQUIRED', $original_failure)
		  if ref($attempt) eq 'HASH';
		$self->{two_stage_cleanup_terminal} = 'OPERATOR_CLEANUP_REQUIRED';
		return $self->{two_stage_cleanup_terminal};
	}
	if (@errors)
	{
		$self->_two_stage_write_cleanup_terminal(
			'CLEANUP_FAILED', $original_failure)
		  if ref($attempt) eq 'HASH';
		$self->{two_stage_cleanup_terminal} = 'CLEANUP_FAILED';
		return $self->{two_stage_cleanup_terminal};
	}
	my $clean = eval {
		$self->_two_stage_cleanup_scratch();
		$self->_two_stage_detach_voting_loops();
		1;
	};
	if (!$clean)
	{
		$self->_two_stage_write_cleanup_terminal(
			'CLEANUP_FAILED', $original_failure)
		  if ref($attempt) eq 'HASH';
		$self->{two_stage_cleanup_terminal} = 'CLEANUP_FAILED';
		return $self->{two_stage_cleanup_terminal};
	}
	$self->_two_stage_update_failure_evidence_manifest(undef, 'CLEAN')
	  if defined($self->{two_stage_failure_evidence_path});
	$self->{two_stage_cleanup_terminal} = 'CLEAN';
	return $self->{two_stage_cleanup_terminal};
}


sub _two_stage_start_nodes
{
	my ($self, $while_starting) = @_;
	$self->{two_stage_start_count} = ($self->{two_stage_start_count} // 0) + 1;
	my @starts;
	my @failures;
	my $while_starting_error;
	my $launch = sub {
		my ($node, $node_id) = @_;
		my %env = $node->_get_env(PGAPPNAME => undef);
		my @cmd = (
			$node->installed_command('pg_ctl'), '-w', '-D', $node->data_dir,
			'-l', $node->logfile, '-o', '--cluster-name=' . $node->name,
			'start');
		my %start = (
			node => $node, node_id => $node_id, stdout => '', stderr => '');

		$start{handle} = start(
			\@cmd, '>', \$start{stdout}, '2>', \$start{stderr},
			init => sub { %ENV = %env; });
		push @starts, \%start;
	};
	my $seed = $self->{nodes}[0];

	# Preserve the established formal-harness seed-candidate ordering.  This
	# bounded launch skew is not a readiness or clean-terminal proof: all four
	# native pg_ctl -w results and the exact formation checks remain mandatory.
	$launch->($seed, 0);
	usleep(2_000_000);
	for my $i (1 .. $#{ $self->{nodes} })
	{
		$launch->($self->{nodes}[$i], $i);
	}
	if (defined($while_starting))
	{
		die "in-flight observation is only valid for Phase 2"
		  unless $self->{two_stage_start_count} == 2
		  && ref($while_starting) eq 'CODE';
		my $observed = eval {
			$self->_two_stage_capture_phase2_boot_identity_current();
			$while_starting->();
			1;
		};
		$while_starting_error = $@ || 'Phase-2 in-flight observation failed'
		  unless $observed;
	}
	for my $start (@starts)
	{
		my $finished = eval { finish($start->{handle}); 1 };
		my $rc = eval { $start->{handle}->result(0) };
		my $node = $start->{node};
		my $result = {
			phase => $self->{two_stage_start_count} + 0,
			node_id => $start->{node_id} + 0,
			finished => $finished ? 1 : 0,
			exit_code => defined($rc) ? $rc + 0 : 'UNAVAILABLE',
			stdout => _two_stage_bounded_text($start->{stdout}),
			stderr => _two_stage_bounded_text($start->{stderr}),
		};
		push @{ $self->{two_stage_native_start_results} //= [] }, $result;

		$node->_update_pid(defined($rc) && $rc == 0 ? 1 : -1);
		push @failures, {
			node_id => $start->{node_id},
			detail => $node->name . ': ' . $start->{stdout} . $start->{stderr},
		} unless $finished && defined($rc) && $rc == 0;
	}
	if (@failures)
	{
		my $phase = $self->{two_stage_start_count};
		my $class = "PHASE${phase}_START_FAILED";
		my $gate = "PHASE${phase}_START";
		my $detail = join('', map { $_->{detail} } @failures);
		$self->_two_stage_record_first_failure(
			$class, $detail, $failures[0]{node_id}, $gate)
		  if ref($self->{two_stage_attempt}) eq 'HASH'
		  && !defined($self->{two_stage_attempt}{first_failure});
		die $while_starting_error if defined($while_starting_error);
		die "two-stage concurrent cluster start failed: $detail";
	}
	die $while_starting_error if defined($while_starting_error);
	if ($self->{two_stage_start_count} == 2
		&& !$self->_two_stage_capture_phase2_boot_identity_current())
	{
		die "Phase-2 start lacks exact postmaster pid/starttime";
	}
	return;
}


sub _two_stage_capture_phase2_boot_identity_current
{
	my ($self) = @_;
	my $identity = $self->{two_stage_phase2_boot_identity} //= [];

	for my $node_id (0 .. $NODES - 1)
	{
		next if ref($identity->[$node_id]) eq 'HASH';
		my $node = $self->{nodes}[$node_id] or next;
		my $pidfile = $node->data_dir . '/postmaster.pid';
		my $raw = eval { PostgreSQL::Test::Utils::slurp_file($pidfile) };
		next if $@ || !defined($raw);
		my ($pid) = $raw =~ /\A([1-9]\d*)\r?\n/;
		next unless defined($pid) && $pid > 1;
		my $current = $self->_two_stage_proc_identity($pid);
		next unless defined($current) && !$current->{unavailable}
		  && (!exists($current->{exists}) || $current->{exists})
		  && ($current->{pid} // -1) == $pid
		  && defined($current->{starttime});
		$identity->[$node_id] = {
			pid => $pid + 0,
			starttime => $current->{starttime} + 0,
		};
	}
	return @$identity == $NODES
	  && !grep { ref($_) ne 'HASH' } @$identity;
}


sub _two_stage_capture_phase2_log_offsets
{
	my ($self) = @_;

	$self->{two_stage_phase2_log_offsets} = [ map {
		my $size = -s $_->logfile;
		defined($size) ? $size : 0;
	} @{ $self->{nodes} } ];
	return;
}


sub _two_stage_node_current_identity
{
	my ($self, $node, $node_id) = @_;
	my $row = $node->safe_psql('postgres', qq{
		SELECT q.current_epoch_at_boot::text || '|' ||
		       m.presented_incarnation::text || '|' ||
		       m.last_admitted_incarnation::text || '|' ||
			       m.admitted_epoch::text || '|' || s.value || '|' ||
			       pg_postmaster_start_time()::text
		  FROM pg_cluster_quorum_state q
		  JOIN pg_cluster_membership m ON m.node_id = $node_id
		  JOIN pg_cluster_state s
		    ON s.category = 'ic'
		   AND s.key = 'tier1_listener_incarnation'
		 WHERE q.in_quorum AND m.state = 'member'
	});
	my ($formation, $presented, $admitted, $admitted_epoch, $session,
		$postmaster_start)
	  = $row =~ /\A(\d+)\|(\d+)\|(\d+)\|(\d+)\|(\d+)\|(.+)\z/;

	die "node$node_id lacks exact current formation/admission/session identity"
	  unless defined($session) && $presented > 0 && $admitted > 0
	  && $session > 0 && defined($postmaster_start)
	  && length($postmaster_start) > 0;
	return {
		formation => $formation + 0,
		presented_incarnation => $presented + 0,
		admitted_incarnation => $admitted + 0,
		admitted_epoch => $admitted_epoch + 0,
		session_incarnation => $session + 0,
		postmaster_start => $postmaster_start,
	};
}


sub _two_stage_effective_voting_paths
{
	my ($self, $node) = @_;

	return $node->safe_psql('postgres', 'SHOW cluster.voting_disks');
}


sub _two_stage_device_current_attest
{
	my ($self, $device, $disk_index) = @_;
	my $backing = $self->{voting_disk_paths}[$disk_index];
	my $bytes = -s $backing;
	my $capacity;
	my $dio;

	die "Phase-2 voting authority $device is not a block device"
	  unless -b $device;
	die "Phase-2 voting backing $backing lacks exact size"
	  unless defined($bytes) && $bytes >= TWO_STAGE_VOTING_BYTES
	  && $bytes % VOTING_SLOT_BYTES == 0;
	$capacity = _run_capture(
		[ 'sudo', '-n', 'blockdev', '--getsize64', $device ],
		"re-attest voting capacity $device");
	$dio = _run_capture(
		[ 'sudo', '-n', 'losetup', '-n', '-O', 'DIO', $device ],
		"re-attest voting DIO $device");
	$dio =~ s/\s+//g;
	die "Phase-2 voting capacity drift for $device"
	  unless $capacity =~ /^\d+$/ && $capacity == $bytes;
	die "Phase-2 voting device $device lacks effective O_DIRECT"
	  unless $dio eq '1';
	open(my $fh, '<:raw', $device) or die "open $device: $!";
	seek($fh, 48, 0) or die "seek $device: $!";
	read($fh, my $raw_index, 4) == 4
	  or die "read $device disk_index: $!";
	close($fh) or die "close $device: $!";
	die "Phase-2 voting disk_index drift for $device"
	  unless unpack('V', $raw_index) == $disk_index;
	return 1;
}


sub _two_stage_validate_phase2_current
{
	my ($self, $phase2) = @_;
	my $phase1 = $self->{two_stage_phase1_identity};
	my $devices = $self->{two_stage_loop_devices} // [];
	my $backing = $self->{voting_disk_paths} // [];
	my $device_csv;
	my $formation;

	die "Phase-2 validation lacks exact four-node identities"
	  unless ref($phase1) eq 'ARRAY' && @$phase1 == $NODES
	  && ref($phase2) eq 'ARRAY' && @$phase2 == $NODES;
	die "Phase-2 validation requires exactly three voting block devices"
	  unless @$devices == 3 && @$backing == 3;
	for my $i (0 .. $#$devices)
	{
		die "Phase-2 voting device $i failed current attestation"
		  unless $self->_two_stage_device_current_attest($devices->[$i], $i);
	}
	$device_csv = join(',', @$devices);
	for my $i (0 .. $NODES - 1)
	{
		my $before = $phase1->[$i];
		my $current = $phase2->[$i];
		my $effective;

		die "Phase-2 node$i identity is incomplete"
		  unless ref($before) eq 'HASH' && ref($current) eq 'HASH'
		  && defined($current->{formation})
		  && ($current->{presented_incarnation} // 0) > 0
		  && ($current->{admitted_incarnation} // 0) > 0
		  && ($current->{session_incarnation} // 0) > 0
		  && length($current->{postmaster_start} // '') > 0
		  && defined($current->{admitted_epoch});
		$formation //= $current->{formation};
		die "Phase-2 node$i formation/admission drift"
		  unless $current->{formation} == $formation
		  && $current->{presented_incarnation}
			 == $current->{admitted_incarnation}
		  && $current->{admitted_epoch} == $formation;
		# Formation/session counters can legally restart with the same numeric
		# value.  Freshness comes from observing the exact current postmaster
		# after the Phase-2 log boundary, not from inventing a larger counter.
		die "Phase-2 node$i retained Phase-1 postmaster observation"
		  unless $current->{postmaster_start}
			 ne ($before->{postmaster_start} // '');
		$effective = $self->_two_stage_effective_voting_paths(
			$self->{nodes}[$i]);
		die "Phase-2 node$i voting paths are not exact current devices"
		  unless defined($effective) && $effective eq $device_csv;
		for my $path (@$backing)
		{
			die "Phase-2 node$i still references voting backing $path"
			  if index($effective, $path) >= 0;
		}
	}
	$self->{two_stage_current_identity}
	  = [ map { +{ %$_ } } @$phase2 ];
	delete $self->{two_stage_phase1_identity};
	return 1;
}


sub _two_stage_wait_for_clean_formation
{
	my ($self, $phase) = @_;
	$phase //= 'phase-1';
	die "invalid two-stage formation phase $phase"
	  unless $phase eq 'phase-1' || $phase eq 'phase-2';
	my $query = q{
		SELECT (SELECT count(*) FROM pg_cluster_membership
				WHERE state = 'member') = 4
		   AND (SELECT in_quorum FROM pg_cluster_quorum_state LIMIT 1)
	};
	my @identity;

	for my $node (@{ $self->{nodes} })
	{
		die "$phase four-member voting formation did not become current"
		  unless $node->poll_query_until('postgres', $query, 't');
	}
	for my $i (0 .. $#{ $self->{nodes} })
	{
		my $node = $self->{nodes}[$i];
		my $marker = "cluster membership: node $i cold-bootstrap membership "
		  . "formation — write gate open";
		my $offset = $phase eq 'phase-2'
		  ? ($self->{two_stage_phase2_log_offsets}[$i] // 0) : 0;
		my $deadline = time() + 30;
		my $found = 0;

		while (time() < $deadline)
		{
			my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
			$log = substr($log, $offset) if $offset > 0;
			if (index($log, $marker) >= 0)
			{
				$found = 1;
				last;
			}
			die "$phase node$i exited before current-boot MEMBER admission"
			  unless defined($node->{_pid}) && kill(0, $node->{_pid});
			usleep(200_000);
		}
		die "$phase node$i did not publish current-boot MEMBER admission"
		  unless $found;
		push @identity, $self->_two_stage_node_current_identity($node, $i);
	}
	if ($phase eq 'phase-1')
	{
		$self->{two_stage_phase1_identity}
		  = [ map { +{ %$_ } } @identity ];
	}
	else
	{
		$self->_two_stage_validate_phase2_current(\@identity);
	}
	return;
}


sub _two_stage_run_lifecycle
{
	my ($self) = @_;

	$self->_two_stage_register_cleanup_owner();
	my $reap = $self->_two_stage_reap_previous_manifests();
	die "two-stage lifecycle has unresolved cleanup ownership: $reap"
	  unless $reap eq 'CLEAN';
	$self->_two_stage_start_nodes();
	$self->_two_stage_wait_for_clean_formation('phase-1');
	$self->_two_stage_coordinated_clean_stop();
	$self->_two_stage_attach_voting_loops();
	$self->_two_stage_qualify_block_backend();
	$self->_two_stage_install_device_only_voting_config();
	$self->_two_stage_capture_phase2_log_offsets();
	$self->_two_stage_start_nodes(sub {
		$self->_two_stage_wait_phase2_gate_ladder();
	});
	return;
}


sub _two_stage_coordinated_clean_stop
{
	my ($self) = @_;
	my @stops;
	my @failures;

	$self->_two_stage_capture_phase1_actors();
	$self->{two_stage_native_stop_success} = [ (0) x $NODES ];
	$self->{two_stage_native_stop_results} = [];

	# Start all four pg_ctl waiters before waiting for any one of them.  This
	# delivers one coordinated fast-shutdown edge while every current member's
	# LMON/LMS/QVOTEC stack is still available to complete the phase-1 round.
	for my $node (@{ $self->{nodes} })
	{
		my @cmd = (
			$node->installed_command('pg_ctl'), '-D', $node->data_dir,
			'-m', 'fast', 'stop');
		my %stop = (stdout => '', stderr => '');

		$stop{handle} = start(\@cmd, '>', \$stop{stdout},
			'2>', \$stop{stderr}, timeout(60));
		push @stops, \%stop;
	}
	for my $i (0 .. $#stops)
	{
		my $stop = $stops[$i];
		my $finished = eval { finish($stop->{handle}); 1 };
		my $rc = eval { $stop->{handle}->result(0) };
		push @{ $self->{two_stage_native_stop_results} }, {
			node_id => $i + 0,
			finished => $finished ? 1 : 0,
			exit_code => defined($rc) ? $rc + 0 : 'UNAVAILABLE',
			stdout => _two_stage_bounded_text($stop->{stdout}),
			stderr => _two_stage_bounded_text($stop->{stderr}),
		};
		if ($finished && defined($rc) && $rc == 0)
		{
			$self->{nodes}[$i]->_update_pid(0);
			$self->{two_stage_native_stop_success}[$i] = 1;
		}
		else
		{
			push @failures, "node$i: " . $stop->{stdout} . $stop->{stderr};
		}
	}
	if (@failures)
	{
		my $detail = join('', @failures);
		$self->_two_stage_record_first_failure(
			'PHASE1_STOP_FAILED', $detail, -1, 'PHASE1_STOP')
		  if ref($self->{two_stage_attempt}) eq 'HASH'
		  && !defined($self->{two_stage_attempt}{first_failure});
		die "phase-1 coordinated fast stop failed: $detail";
	}

	for my $i (0 .. $#{ $self->{nodes} })
	{
		my $node = $self->{nodes}[$i];
		my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
		my @anchors = (
			'checkpoint starting: shutdown',
			'checkpoint complete:',
			'phase-1 full-stop exact WAL STOPPED published',
			'phase-1 full-stop fresh-nonce exact four-member ACK barrier complete',
			'phase-1 full-stop exact release/completion complete',
			'database system is shut down');
		my $cursor = -1;

		die "phase-1 node$i recorded an abnormal shutdown"
		  if index($log, 'abnormal database system shutdown') >= 0;
		for my $anchor (@anchors)
		{
			my $next = index($log, $anchor, $cursor + 1);
			die "phase-1 node$i lacks ordered shutdown evidence: $anchor"
			  if $next < 0;
			$cursor = $next;
		}
	}
}


sub _attach_voting_loops_exact
{
	my ($self, $scope) = @_;
	my @backing = @{ $self->{voting_disk_paths} // [] };
	my $gid = (split(/\s+/, $( ))[0];

	die "$scope voting loop requires exactly three backing files"
	  unless @backing == 3;
	$self->{two_stage_loop_devices} //= [];
	die "$scope voting loop attach started with retained devices"
	  if @{ $self->{two_stage_loop_devices} };
	for my $i (0 .. $#backing)
	{
		my $path = $backing[$i];
		my $bytes = -s $path;
		my $device;

		die "voting backing $path is not final-size aligned"
		  unless defined($bytes) && $bytes >= TWO_STAGE_VOTING_BYTES
		  && $bytes % VOTING_SLOT_BYTES == 0;
		$device = _run_capture(
			[ 'sudo', '-n', 'losetup', '--find', '--show',
			  '--direct-io=on', $path ],
			"attach voting backing $path");
		push @{ $self->{two_stage_loop_devices} }, $device;
		my $record = $self->_two_stage_current_loop_mapping($device);
		$record->{attach_order} = $i;
		push @{ $self->{two_stage_loop_records} }, $record;
		_run_capture(
			[ 'sudo', '-n', 'chown', "$<:$gid", $device ],
			"chown voting device $device");
		die "voting authority $device is not a block device" unless -b $device;
		my $capacity = _run_capture(
			[ 'sudo', '-n', 'blockdev', '--getsize64', $device ],
			"read voting capacity $device");
		my $dio = _run_capture(
			[ 'sudo', '-n', 'losetup', '-n', '-O', 'DIO', $device ],
			"read voting DIO $device");
		$dio =~ s/\s+//g;
		die "voting capacity drift for $device"
		  unless $capacity =~ /^\d+$/ && $capacity == $bytes
		  && $capacity >= TWO_STAGE_VOTING_BYTES
		  && $capacity % VOTING_SLOT_BYTES == 0;
		die "voting device $device lacks effective O_DIRECT" unless $dio eq '1';
		$record->{capacity_bytes} = $capacity + 0;
		$record->{direct_io} = 1;
		$record->{disk_index} = $i + 0;
		_run_capture(
			[ 'cmp', '-n', "$bytes", $path, $device ],
			"compare voting backing $path with $device");

		open(my $fh, '<:raw', $device) or die "open $device: $!";
		seek($fh, 48, 0) or die "seek $device: $!";
		read($fh, my $raw_index, 4) == 4 or die "read $device disk_index: $!";
		close($fh) or die "close $device: $!";
		die "voting disk_index drift for $device"
		  unless unpack('V', $raw_index) == $i;
	}
	$self->{two_stage_static_attestation_digest} = sha256_hex(join('|', map {
		join(':', $_->{path}, $_->{major_minor}, $_->{backing_realpath},
			$_->{attach_order})
	} @{ $self->{two_stage_loop_records} }));

	return;
}


sub _two_stage_attach_voting_loops
{
	my ($self) = @_;

	$self->_two_stage_assert_phase1_offline();
	$self->_attach_voting_loops_exact('two-stage');
	return;
}


sub _happy_path_prepare_voting_loops
{
	my ($self) = @_;

	$self->_two_stage_register_cleanup_owner();
	my $reap = $self->_two_stage_reap_previous_manifests();
	die "happy-path voting lifecycle has unresolved cleanup ownership: $reap"
	  unless $reap eq 'CLEAN';
	for my $node (@{ $self->{nodes} })
	{
		$node->_update_pid(-1);
		die "happy-path voting loop attach requires every postmaster offline"
		  if defined($node->{_pid});
	}
	my $holders = $self->_two_stage_open_backing_fd_holders();
	die "happy-path voting loop attach found open backing-file holders"
	  if @$holders;

	$self->_attach_voting_loops_exact('happy-path');
	$self->_two_stage_install_device_only_voting_config();
	return;
}


sub _two_stage_install_device_only_voting_config
{
	my ($self) = @_;
	my $devices = $self->{two_stage_loop_devices} // [];

	die "device-only voting configuration requires three attested devices"
	  unless @$devices == 3;
	my $csv = join(',', @$devices);
	for my $node (@{ $self->{nodes} })
	{
		$node->append_conf('postgresql.conf',
			"cluster.voting_disks = '$csv'\n");
		$node->append_conf('postgresql.conf',
			"cluster.voting_disk_size_bytes = " . TWO_STAGE_VOTING_BYTES . "\n");
	}
	$self->{two_stage_device_voting_config_installed} = $csv;
	return;
}


sub _two_stage_backend_fingerprint
{
	my ($self, $path) = @_;
	my $resolved = abs_path($path);
	my @st;
	my $mount;
	my ($mount_point, $filesystem, $mount_options);

	die "cannot resolve backend path $path" unless defined($resolved);
	@st = stat($resolved);
	die "cannot stat backend path $path: $!" unless @st;
	$mount = _run_capture(
		[ 'findmnt', '-T', $resolved, '-n', '-o', 'TARGET,FSTYPE,OPTIONS' ],
		"fingerprint backend $path");
	($mount_point, $filesystem, $mount_options) = split(/\s+/, $mount, 3);
	die "incomplete backend mount fingerprint for $path"
	  unless defined($mount_options) && length($mount_options) > 0;
	return {
		mount_point => $mount_point,
		filesystem => $filesystem,
		mount_options => $mount_options,
		uid => $st[4] + 0,
		gid => $st[5] + 0,
		mode => $st[2] & 07777,
		logical_block_size => ($st[11] || 4096) + 0,
		physical_block_size => ($st[11] || 4096) + 0,
		allocation_method => 'formatter-write+truncate',
	};
}


sub _two_stage_assert_equivalent_backend
{
	my ($self, $scratch, $authoritative) = @_;
	my @fields = qw(mount_point filesystem mount_options uid gid mode
		logical_block_size physical_block_size allocation_method);

	die "scratch qualification requires three authoritative backings"
	  unless ref($authoritative) eq 'ARRAY' && @$authoritative == 3;
	for my $path (@$authoritative)
	{
		die "scratch backing aliases authoritative voting path $path"
		  if $scratch eq $path
		  || (defined(abs_path($scratch)) && defined(abs_path($path))
			  && abs_path($scratch) eq abs_path($path));
	}
	my $baseline = $self->_two_stage_backend_fingerprint($authoritative->[0]);
	for my $path (@$authoritative[1 .. $#$authoritative], $scratch)
	{
		my $candidate = $self->_two_stage_backend_fingerprint($path);

		for my $field (@fields)
		{
			die "block backend $field drift for $path"
			  unless defined($baseline->{$field})
			  && defined($candidate->{$field})
			  && "$baseline->{$field}" eq "$candidate->{$field}";
		}
	}
	return 1;
}


sub _two_stage_create_scratch_backing
{
	my ($self) = @_;
	my $authoritative = $self->{voting_disk_paths} // [];
	my @st = @$authoritative ? stat($authoritative->[0]) : ();
	my ($fh, $path);
	my $bytes = 4 * 65536;

	die "scratch backing requires three authoritative paths"
	  unless @$authoritative == 3 && @st;
	($fh, $path) = tempfile('pgrac-phase2-scratch-XXXXXX',
		DIR => dirname($authoritative->[0]), UNLINK => 0);
	binmode($fh, ':raw');
	$self->{two_stage_scratch} = {
		backing => $path,
		device => undef,
		child_processes => [],
	};
	chmod($st[2] & 07777, $path)
	  or die "chmod scratch backing $path: $!";
	chown($st[4], $st[5], $path)
	  or die "chown scratch backing $path: $!";
	my $allocated = "\0" x 65536;
	my $written = syswrite($fh, $allocated);
	die "allocate scratch backing $path: $!"
	  unless defined($written) && $written == length($allocated);
	truncate($fh, $bytes) or die "size scratch backing $path: $!";
	close($fh) or die "close scratch backing $path: $!";
	return $path;
}


sub _two_stage_attach_scratch_loop
{
	my ($self, $backing) = @_;
	my $gid = (split(/\s+/, $( ))[0];
	my $device = _run_capture(
		[ 'sudo', '-n', 'losetup', '--find', '--show',
		  '--direct-io=on', $backing ],
		"attach scratch backing $backing");

	$self->{two_stage_scratch}{device} = $device;
	my $record = $self->_two_stage_current_loop_mapping($device);
	$record->{attach_order} = 3;
	$self->{two_stage_scratch}{record} = $record;
	_run_capture([ 'sudo', '-n', 'chown', "$<:$gid", $device ],
		"chown scratch device $device");
	return $device;
}


sub _two_stage_attest_scratch_loop
{
	my ($self, $device, $backing) = @_;
	my $bytes = -s $backing;
	my $capacity;
	my $dio;
	my $logical;
	my $physical;

	die "scratch qualification path is not a block device"
	  unless -b $device;
	$capacity = _run_capture(
		[ 'sudo', '-n', 'blockdev', '--getsize64', $device ],
		"read scratch capacity $device");
	$dio = _run_capture(
		[ 'sudo', '-n', 'losetup', '-n', '-O', 'DIO', $device ],
		"read scratch DIO $device");
	$logical = _run_capture(
		[ 'sudo', '-n', 'blockdev', '--getss', $device ],
		"read scratch logical block size $device");
	$physical = _run_capture(
		[ 'sudo', '-n', 'blockdev', '--getpbsz', $device ],
		"read scratch physical block size $device");
	$dio =~ s/\s+//g;
	die "scratch device capacity drift"
	  unless defined($bytes) && $capacity =~ /^\d+$/ && $capacity == $bytes;
	die "scratch device lacks effective O_DIRECT" unless $dio eq '1';
	die "scratch device has invalid block geometry"
	  unless $logical =~ /^\d+$/ && $logical > 0
	  && $physical =~ /^\d+$/ && $physical > 0;
	return {
		logical_block_size => $logical + 0,
		physical_block_size => $physical + 0,
	};
}


sub _two_stage_gcd
{
	my ($left, $right) = @_;

	while ($right)
	{
		($left, $right) = ($right, $left % $right);
	}
	return $left;
}


sub _two_stage_direct_io_region_size
{
	my ($logical, $physical) = @_;
	my $region = 4096;

	for my $size ($logical, $physical)
	{
		die "invalid direct-I/O alignment" unless defined($size) && $size > 0;
		$region = int($region / _two_stage_gcd($region, $size)) * $size;
	}
	return $region;
}


sub _two_stage_direct_io_probe_program
{
	my ($self) = @_;

	return $ENV{PGRAC_DIRECT_IO_PROBE}
	  if defined($ENV{PGRAC_DIRECT_IO_PROBE})
	  && length($ENV{PGRAC_DIRECT_IO_PROBE}) > 0;
	my $top_builddir = $ENV{top_builddir} // '';
	my $program = "$top_builddir/src/test/cluster_tap/pgrac_direct_io_probe";
	die "pgrac_direct_io_probe is not built at $program" unless -x $program;
	return $program;
}


sub _two_stage_direct_io_probe_specs
{
	my ($self, $device, $region_size, $attempt_id) = @_;
	$attempt_id //= $self->{two_stage_attempt}{attempt_id};
	die "direct-I/O probe lacks an exact attempt" unless defined($attempt_id)
	  && $attempt_id =~ /^[0-9a-f]+$/;
	die "invalid direct-I/O probe region" unless defined($region_size)
	  && $region_size >= 4096;
	my $program = $self->_two_stage_direct_io_probe_program();
	return [ map {
		my $node_id = $_;
		my $offset = $node_id * $region_size;
		{
			device => $device,
			offset => $offset,
			length => $region_size,
			node => $node_id,
			attempt => $attempt_id,
			sequences => 16,
			command => [ $program, '--device', $device,
				'--offset', $offset, '--length', $region_size,
				'--node', $node_id, '--attempt', $attempt_id,
				'--sequences', 16 ],
		}
	} 0 .. $NODES - 1 ];
}


sub _two_stage_spawn_direct_io_actor
{
	my ($self, $spec) = @_;
	my $scratch = $self->{two_stage_scratch};
	die "direct-I/O actor lacks exact scratch ownership"
	  unless ref($scratch) eq 'HASH' && defined($scratch->{backing});
	my $directory = dirname($scratch->{backing});
	my ($stdout_fh, $stdout_path) = tempfile(
		"pgrac-probe-node$spec->{node}-stdout-XXXXXX",
		DIR => $directory, UNLINK => 0);
	my ($stderr_fh, $stderr_path) = tempfile(
		"pgrac-probe-node$spec->{node}-stderr-XXXXXX",
		DIR => $directory, UNLINK => 0);
	chmod(0600, $stdout_path, $stderr_path)
	  or die "chmod direct-I/O actor output: $!";
	pipe(my $gate_read, my $gate_write)
	  or die "create direct-I/O actor identity gate: $!";
	my $pid = fork();
	if (!defined($pid))
	{
		close($gate_read);
		close($gate_write);
		close($stdout_fh);
		close($stderr_fh);
		unlink($stdout_path, $stderr_path);
		die "fork direct-I/O actor: $!";
	}
	if ($pid == 0)
	{
		close($gate_write);
		open(STDOUT, '>&', $stdout_fh) or POSIX::_exit(126);
		open(STDERR, '>&', $stderr_fh) or POSIX::_exit(126);
		close($stdout_fh);
		close($stderr_fh);
		my $token = '';
		my $read = sysread($gate_read, $token, 1);
		close($gate_read);
		POSIX::_exit(126)
		  unless defined($read) && $read == 1 && $token eq 'G';
		{
			no warnings 'exec';
			exec { $spec->{command}[0] } @{ $spec->{command} };
		}
		POSIX::_exit(127);
	}
	close($gate_read);
	close($stdout_fh);
	close($stderr_fh);
	my $identity = $self->_two_stage_proc_identity($pid);
	if (!defined($identity) || $identity->{unavailable}
		|| (exists($identity->{exists}) && !$identity->{exists})
		|| !defined($identity->{starttime}))
	{
		close($gate_write);
		waitpid($pid, 0);
		unlink($stdout_path, $stderr_path);
		die "direct-I/O actor pid=$pid lacks exact Linux starttime";
	}
	my $actor = {
		actor => 'scratch_probe',
		node => $spec->{node} + 0,
		pid => $pid + 0,
		starttime => $identity->{starttime} + 0,
		last_state => $identity->{state} // 'UNAVAILABLE',
		stdout_path => $stdout_path,
		stderr_path => $stderr_path,
		spec => $spec,
	};
	push @{ $scratch->{child_processes} }, $actor;
	my $written = syswrite($gate_write, 'G');
	close($gate_write);
	die "release direct-I/O actor identity gate: $!"
	  unless defined($written) && $written == 1;
	return $actor;
}


sub _two_stage_collect_direct_io_actors
{
	my ($self, $actors, $deadline) = @_;
	my %pending = map { $_->{pid} => $_ } @$actors;
	my $wait_round = 0;
	while (keys %pending)
	{
		for my $pid (sort { $a <=> $b } keys %pending)
		{
			my $waited = waitpid($pid, WNOHANG);
			next if $waited == 0;
			my $actor = delete $pending{$pid};
			if ($waited == $pid)
			{
				$actor->{wait_status} = $? + 0;
				$actor->{exit_code} = ($? & 127)
				  ? 128 + ($? & 127) : ($? >> 8);
				$actor->{finished} = 1;
			}
			else
			{
				$actor->{wait_error} = "$!";
			}
		}
		last unless keys %pending;
		last if $self->_two_stage_monotonic_now() >= $deadline;
		$self->_two_stage_wait_gate_observation_change(
			$deadline, $wait_round++);
	}
	return [ values %pending ];
}


sub _two_stage_run_direct_io_children
{
	my ($self, $device, $region_size) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "direct-I/O probe lacks an attempt";
	my $specs = $self->_two_stage_direct_io_probe_specs(
		$device, $region_size, $attempt->{attempt_id});
	my $actors = $self->{two_stage_scratch}{child_processes} //= [];
	die "direct-I/O probe started with retained child ownership" if @$actors;

	for my $spec (@$specs)
	{
		die "direct-I/O probe reached its absolute deadline"
		  unless $self->_two_stage_monotonic_now()
		  < $attempt->{cleanup_deadline};
		$self->_two_stage_spawn_direct_io_actor($spec);
	}
	my $pending = $self->_two_stage_collect_direct_io_actors(
		$actors, $attempt->{cleanup_deadline});
	my %pending = map { $_->{pid} => 1 } @$pending;
	my @failures;
	for my $actor (@$actors)
	{
		my $stdout = _two_stage_proc_bounded_read($actor->{stdout_path});
		my $stderr = _two_stage_proc_bounded_read($actor->{stderr_path});
		if ($pending{$actor->{pid}})
		{
			push @failures,
			  "node$actor->{node} deadline pid=$actor->{pid} $stdout$stderr";
		}
		elsif (!$actor->{finished} || ($actor->{exit_code} // -1) != 0)
		{
			push @failures, "node$actor->{node} rc="
			  . ($actor->{finished} ? $actor->{exit_code} : 'unknown')
			  . " $stdout$stderr";
		}
	}
	die "direct-I/O child probe failed: " . join('; ', @failures)
	  if @failures;
	$self->{two_stage_scratch_probe_digest} = sha256_hex(join('|', map {
		join(':', $_->{node}, $_->{offset}, $_->{length}, $_->{sequences})
	} @$specs));
	return 1;
}


sub _two_stage_proc_identity
{
	my ($self, $pid) = @_;
	my $path = "/proc/$pid/stat";

	return { pid => $pid + 0, exists => 0 } unless -e $path;
	open(my $fh, '<', $path)
	  or return { pid => $pid + 0, exists => 1, unavailable => 1 };
	my $stat = <$fh>;
	close($fh);
	return { pid => $pid + 0, exists => 1, unavailable => 1 }
	  unless defined($stat) && $stat =~ /^\d+\s+\(.*\)\s+(.*)$/;
	my @fields = split(/\s+/, $1);
	return { pid => $pid + 0, exists => 1, unavailable => 1 }
	  unless @fields >= 20 && $fields[1] =~ /^\d+$/
	  && $fields[19] =~ /^\d+$/;
	return {
		pid => $pid + 0,
		exists => 1,
		state => $fields[0],
		ppid => $fields[1] + 0,
		starttime => $fields[19] + 0,
	};
}


sub _two_stage_proc_bounded_read
{
	my ($path) = @_;

	open(my $fh, '<', $path) or return 'UNAVAILABLE';
	my $value = '';
	my $read = read($fh, $value, 16384);
	close($fh);
	return 'UNAVAILABLE' unless defined($read);
	$value =~ s/\s+\z//;
	return $value;
}


sub _two_stage_bounded_text
{
	my ($value) = @_;
	$value = '' unless defined($value);
	return $value if length($value) <= 16384;
	return substr($value, 0, 8192)
	  . "\n...BOUNDED...\n"
	  . substr($value, -8192);
}


sub _two_stage_proc_wait_channel
{
	my ($self, $pid) = @_;
	return _two_stage_proc_bounded_read("/proc/$pid/wchan");
}


sub _two_stage_proc_kernel_stack
{
	my ($self, $pid) = @_;
	return _two_stage_proc_bounded_read("/proc/$pid/stack");
}


sub _two_stage_failure_evidence_path
{
	my ($self, $attempt_id) = @_;

	die "invalid failure evidence attempt id"
	  unless defined($attempt_id) && $attempt_id =~ /^[0-9a-f]+$/;
	my $root = $self->_two_stage_artifact_root();
	my $dir = "$root/failure-evidence";
	make_path($dir, { mode => 0700 }) unless -d $dir;
	my $resolved_dir = abs_path($dir);
	die "failure evidence directory escaped the artifact root"
	  unless defined($resolved_dir)
	  && ($resolved_dir eq $root || index($resolved_dir, "$root/") == 0);
	return "$resolved_dir/$attempt_id.json";
}


sub _two_stage_failure_resource_paths
{
	my ($self) = @_;
	my %paths;

	for my $path (@{ $self->{voting_disk_paths} // [] },
		@{ $self->{two_stage_loop_devices} // [] })
	{
		$paths{$path} = 1 if defined($path) && length($path) > 0;
	}
	if (ref($self->{two_stage_scratch}) eq 'HASH')
	{
		for my $key (qw(backing device))
		{
			my $path = $self->{two_stage_scratch}{$key};
			$paths{$path} = 1 if defined($path) && length($path) > 0;
		}
	}
	if (ref($self->{two_stage_scratch_failure_evidence}) eq 'HASH')
	{
		for my $key (qw(backing device))
		{
			my $path = $self->{two_stage_scratch_failure_evidence}{$key};
			$paths{$path} = 1 if defined($path) && length($path) > 0;
		}
	}
	return [ sort keys %paths ];
}


sub _two_stage_process_fd_evidence
{
	my ($self, $pid, $resource_paths) = @_;
	my %resource;

	for my $path (@$resource_paths)
	{
		$resource{$path} = 1;
		my $resolved = abs_path($path);
		$resource{$resolved} = 1 if defined($resolved);
	}
	my $directory = "/proc/$pid/fd";
	opendir(my $dir, $directory)
	  or return { status => 'UNAVAILABLE', entries => [] };
	my @entries;
	my $examined = 0;
	my $truncated = 0;
	while (defined(my $fd = readdir($dir)))
	{
		next unless $fd =~ /^\d+$/;
		if ($examined++ >= 128)
		{
			$truncated = 1;
			last;
		}
		my $target = readlink("$directory/$fd");
		next unless defined($target);
		my $plain = $target;
		$plain =~ s/ \(deleted\)\z//;
		next unless $resource{$target} || $resource{$plain};
		push @entries, { fd => $fd + 0, target => $target };
	}
	closedir($dir);
	return {
		status => 'OK', entries => \@entries,
		examined => $examined + 0, truncated => $truncated ? 1 : 0,
	};
}


sub _two_stage_mountinfo_decode
{
	my ($value) = @_;
	$value =~ s/\\040/ /g;
	$value =~ s/\\011/\t/g;
	$value =~ s/\\012/\n/g;
	$value =~ s/\\134/\\/g;
	return $value;
}


sub _two_stage_mount_fingerprint_readonly
{
	my ($self, $path) = @_;
	my $resolved = abs_path($path);

	return { status => 'UNAVAILABLE' } unless defined($resolved);
	open(my $fh, '<:raw', '/proc/self/mountinfo')
	  or return { status => 'UNAVAILABLE' };
	my $bytes = '';
	my $read = read($fh, $bytes, 1024 * 1024);
	close($fh);
	return { status => 'UNAVAILABLE' } unless defined($read);
	my $best;
	for my $line (split(/\n/, $bytes))
	{
		my ($left, $right) = split(/ - /, $line, 2);
		next unless defined($right);
		my @left = split(/ /, $left);
		my @right = split(/ /, $right);
		next unless @left >= 6 && @right >= 3;
		my $mount_point = _two_stage_mountinfo_decode($left[4]);
		next unless $resolved eq $mount_point
		  || ($mount_point eq '/')
		  || index($resolved, "$mount_point/") == 0;
		next if defined($best)
		  && length($best->{mount_point}) >= length($mount_point);
		$best = {
			status => 'OK', mount_point => $mount_point,
			filesystem => $right[0], source => $right[1],
			mount_options => $left[5], super_options => $right[2],
		};
	}
	return $best // { status => 'UNAVAILABLE' };
}


sub _two_stage_sysfs_block_value
{
	my ($major_minor, $leaf) = @_;
	return 'UNAVAILABLE'
	  unless defined($major_minor) && $major_minor =~ /^\d+:\d+$/;
	return _two_stage_proc_bounded_read(
		"/sys/dev/block/$major_minor/$leaf");
}


sub _two_stage_failure_device_evidence
{
	my ($self) = @_;
	my @evidence;
	my %backing_seen;

	for my $record (@{ $self->{two_stage_loop_records} // [] })
	{
		my %current = %$record;
		$current{role} = 'VOTING_DEVICE';
		my $major_minor = $current{major_minor} // '';
		$major_minor =~ s/^\s+|\s+$//g;
		$current{major_minor} = $major_minor;
		$current{device_exists} = -e $current{path} ? 1 : 0;
		$current{device_is_block} = -b $current{path} ? 1 : 0;
		$current{logical_block_size_current}
		  = _two_stage_sysfs_block_value(
			$major_minor, 'queue/logical_block_size');
		$current{physical_block_size_current}
		  = _two_stage_sysfs_block_value(
			$major_minor, 'queue/physical_block_size');
		$current{sectors_current}
		  = _two_stage_sysfs_block_value($major_minor, 'size');
		$current{backing_bytes_current} = -s $current{backing_realpath}
		  if defined($current{backing_realpath});
		$current{backend} = $self->_two_stage_mount_fingerprint_readonly(
			$current{backing_realpath});
		$backing_seen{$current{backing_realpath}} = 1
		  if defined($current{backing_realpath});
		push @evidence, \%current;
	}
	my $scratch = ref($self->{two_stage_scratch}) eq 'HASH'
	  ? $self->{two_stage_scratch}
	  : $self->{two_stage_scratch_failure_evidence};
	if (ref($scratch) eq 'HASH' && ref($scratch->{record}) eq 'HASH')
	{
		my %current = %{ $scratch->{record} };
		$current{role} = 'SCRATCH_DEVICE';
		my $major_minor = $current{major_minor} // '';
		$major_minor =~ s/^\s+|\s+$//g;
		$current{major_minor} = $major_minor;
		$current{device_exists} = -e $current{path} ? 1 : 0;
		$current{device_is_block} = -b $current{path} ? 1 : 0;
		$current{logical_block_size_current}
		  = _two_stage_sysfs_block_value(
			$major_minor, 'queue/logical_block_size');
		$current{physical_block_size_current}
		  = _two_stage_sysfs_block_value(
			$major_minor, 'queue/physical_block_size');
		$current{sectors_current}
		  = _two_stage_sysfs_block_value($major_minor, 'size');
		$current{backing_bytes_current} = -s $current{backing_realpath}
		  if defined($current{backing_realpath});
		$current{backend} = $self->_two_stage_mount_fingerprint_readonly(
			$current{backing_realpath});
		$backing_seen{$current{backing_realpath}} = 1
		  if defined($current{backing_realpath});
		push @evidence, \%current;
	}
	for my $path (@{ $self->{voting_disk_paths} // [] })
	{
		next if $backing_seen{$path};
		my $resolved = abs_path($path);
		push @evidence, {
			role => 'VOTING_BACKING', path => $path,
			resolved_path => $resolved // 'UNAVAILABLE',
			bytes_current => (-s $path) // 'UNAVAILABLE',
			backend => $self->_two_stage_mount_fingerprint_readonly($path),
		};
	}
	return \@evidence;
}


sub _two_stage_bounded_log_window
{
	my ($self, $node_id) = @_;
	my $node = $self->{nodes}[$node_id];
	my $path = eval { $node->logfile };

	return { status => 'UNAVAILABLE', text => 'UNAVAILABLE' }
	  unless defined($path) && -f $path;
	my $size = -s $path;
	return { status => 'UNAVAILABLE', path => $path,
		text => 'UNAVAILABLE' } unless defined($size);
	my $scan_start = $size > 65536 ? $size - 65536 : 0;
	open(my $fh, '<:raw', $path)
	  or return { status => 'UNAVAILABLE', path => $path,
		text => 'UNAVAILABLE' };
	seek($fh, $scan_start, 0)
	  or do { close($fh); return { status => 'UNAVAILABLE', path => $path,
		text => 'UNAVAILABLE' }; };
	my $scan = '';
	my $read = read($fh, $scan, 65536);
	close($fh);
	return { status => 'UNAVAILABLE', path => $path,
		text => 'UNAVAILABLE' } unless defined($read);
	my $first = $self->{two_stage_attempt}{first_failure} // {};
	my $detail = $first->{detail} // '';
	my $anchor = length($detail) > 0 && length($detail) <= 1024
	  ? index($scan, $detail) : -1;
	if ($anchor < 0 && $scan =~ /(?:PANIC|FATAL|ERROR):/)
	{
		$anchor = $-[0];
	}
	$anchor = length($scan) if $anchor < 0;
	my $window_start = $anchor > 8192 ? $anchor - 8192 : 0;
	$window_start = length($scan) - 16384
	  if length($scan) - $window_start > 16384 && $anchor == length($scan);
	$window_start = 0 if $window_start < 0;
	my $text = substr($scan, $window_start, 16384);
	return {
		status => 'OK', path => $path,
		offset => $scan_start + $window_start,
		bytes => length($text), text => $text,
	};
}


sub _two_stage_capture_failure_evidence
{
	my ($self) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "failure evidence lacks an exact attempt";
	my $first = $attempt->{first_failure};
	die "failure evidence lacks an immutable first failure"
	  unless ref($first) eq 'HASH';
	my $path = $self->_two_stage_failure_evidence_path(
		$attempt->{attempt_id});
	return $path if -f $path;
	my $resources = $self->_two_stage_failure_resource_paths();
	my @processes;
	for my $record (@{ $self->{two_stage_cleanup_processes} // [] })
	{
		my %evidence = %$record;
		my $current = $self->_two_stage_proc_identity($record->{pid});
		my $exact = defined($current) && !$current->{unavailable}
		  && (!exists($current->{exists}) || $current->{exists})
		  && defined($current->{starttime})
		  && $current->{starttime} == $record->{starttime};
		$evidence{identity_status} = $exact ? 'EXACT_CURRENT'
		  : 'UNAVAILABLE_OR_DRIFT';
		$evidence{state} = $exact
		  ? ($current->{state} // 'UNAVAILABLE') : 'UNAVAILABLE';
		$evidence{wchan} = $exact
		  ? $self->_two_stage_proc_wait_channel($record->{pid})
		  : 'UNAVAILABLE';
		$evidence{kernel_stack} = $exact
		  ? $self->_two_stage_proc_kernel_stack($record->{pid})
		  : 'UNAVAILABLE';
		$evidence{fds} = $exact
		  ? $self->_two_stage_process_fd_evidence(
			$record->{pid}, $resources)
		  : { status => 'UNAVAILABLE', entries => [] };
		push @processes, \%evidence;
	}
	my @logs = map { $self->_two_stage_bounded_log_window($_) }
	  0 .. $#{ $self->{nodes} // [] };
	my $manifest = {
		schema => 1,
		attempt_id => $attempt->{attempt_id},
		created_at => strftime('%Y-%m-%dT%H:%M:%SZ', gmtime(time())),
		first_failure => { %$first },
		gates => $attempt->{gates} // {},
		processes => \@processes,
		devices => $self->_two_stage_failure_device_evidence(),
		logs => \@logs,
		native_results => {
			start => [ map { +{ %$_ } }
				@{ $self->{two_stage_native_start_results} // [] } ],
			stop => [ map { +{ %$_ } }
				@{ $self->{two_stage_native_stop_results} // [] } ],
			probe => [ map { +{
				node_id => $_->{node}, pid => $_->{pid},
				starttime => $_->{starttime},
				finished => $_->{finished} ? 1 : 0,
				exit_code => defined($_->{exit_code})
				  ? $_->{exit_code} : 'UNAVAILABLE',
			} } @{ ref($self->{two_stage_scratch}) eq 'HASH'
				? ($self->{two_stage_scratch}{child_processes} // [])
				: (ref($self->{two_stage_scratch_failure_evidence}) eq 'HASH'
					? ($self->{two_stage_scratch_failure_evidence}
						{child_processes} // []) : []) } ],
		},
		cleanup_manifest => { status => 'NOT_WRITTEN' },
	};
	$self->_two_stage_write_manifest_data($path, $manifest);
	$self->{two_stage_failure_evidence_path} = $path;
	return $path;
}


sub _two_stage_update_failure_evidence_manifest
{
	my ($self, $manifest_path, $status) = @_;
	my $path = $self->{two_stage_failure_evidence_path};
	return unless defined($path);
	die "invalid cleanup evidence terminal"
	  unless defined($status) && $status =~ /^[A-Z][A-Z0-9_]*$/;
	open(my $fh, '<:raw', $path)
	  or die "open failure evidence $path: $!";
	my $bytes = '';
	my $read = read($fh, $bytes, 1024 * 1024);
	my $extra = '';
	my $overflow = read($fh, $extra, 1);
	close($fh) or die "close failure evidence $path: $!";
	die "read failure evidence $path: $!" unless defined($read);
	die "failure evidence exceeds bounded schema" if $overflow;
	my $evidence = eval { JSON::PP->new->utf8(1)->decode($bytes) };
	die "invalid failure evidence $path"
	  unless ref($evidence) eq 'HASH'
	  && ($evidence->{attempt_id} // '')
		 eq ($self->{two_stage_attempt}{attempt_id} // '');
	$evidence->{cleanup_manifest} = { status => $status };
	$evidence->{cleanup_manifest}{path} = $manifest_path
	  if defined($manifest_path);
	$self->_two_stage_write_manifest_data($path, $evidence);
	return;
}


sub _two_stage_artifact_root
{
	my ($self) = @_;
	my $root = $self->{two_stage_artifact_root};

	if (!defined($root) || length($root) == 0)
	{
		my $build = $ENV{top_builddir} // $PostgreSQL::Test::Utils::tmp_check;
		$root = "$build/pgrac-stage8-artifacts";
	}
	make_path($root, { mode => 0700 }) unless -d $root;
	my $resolved = abs_path($root);
	die "cannot resolve Phase-2 artifact root $root" unless defined($resolved);
	$self->{two_stage_artifact_root} = $resolved;
	return $resolved;
}


sub _two_stage_manifest_path
{
	my ($self, $attempt_id) = @_;

	die "invalid cleanup manifest attempt id"
	  unless defined($attempt_id) && $attempt_id =~ /^[0-9a-f]+$/;
	my $root = $self->_two_stage_artifact_root();
	my $dir = "$root/cleanup-manifests";
	make_path($dir, { mode => 0700 }) unless -d $dir;
	my $resolved_dir = abs_path($dir);
	die "cleanup manifest directory escaped the artifact root"
	  unless defined($resolved_dir)
	  && ($resolved_dir eq $root || index($resolved_dir, "$root/") == 0);
	return "$resolved_dir/$attempt_id.json";
}


sub _two_stage_fsync_directory
{
	my ($directory) = @_;
	my $dir_fh;

	sysopen($dir_fh, $directory, O_RDONLY)
	  or die "open cleanup manifest directory $directory: $!";
	$dir_fh->sync()
	  or die "fsync cleanup manifest directory $directory: $!";
	close($dir_fh)
	  or die "close cleanup manifest directory $directory: $!";
	return;
}


sub _two_stage_write_manifest_data
{
	my ($self, $path, $manifest) = @_;
	my $directory = dirname($path);
	my ($fh, $temporary) = tempfile('.cleanup-XXXXXX',
		DIR => $directory, UNLINK => 0);
	my $bytes = JSON::PP->new->canonical(1)->utf8(1)->encode($manifest);

	chmod(0600, $temporary)
	  or die "chmod cleanup manifest temporary $temporary: $!";
	binmode($fh, ':raw');
	print {$fh} $bytes
	  or die "write cleanup manifest temporary $temporary: $!";
	$fh->flush()
	  or die "flush cleanup manifest temporary $temporary: $!";
	$fh->sync()
	  or die "fsync cleanup manifest temporary $temporary: $!";
	close($fh)
	  or die "close cleanup manifest temporary $temporary: $!";
	rename($temporary, $path)
	  or die "publish cleanup manifest $path: $!";
	_two_stage_fsync_directory($directory);
	return $path;
}


sub _two_stage_write_cleanup_manifest
{
	my ($self, $status, $original_failure) = @_;
	my $attempt = $self->{two_stage_attempt}
	  or die "cleanup manifest lacks an exact attempt";
	my $attempt_id = $attempt->{attempt_id};
	my $path = $self->_two_stage_manifest_path($attempt_id);
	my @devices = map { +{ %$_ } }
	  @{ $self->{two_stage_loop_records} // [] };
	if (ref($self->{two_stage_scratch}) eq 'HASH'
		&& ref($self->{two_stage_scratch}{record}) eq 'HASH')
	{
		push @devices, { %{ $self->{two_stage_scratch}{record} } };
	}
	my $manifest = {
		schema => 1,
		attempt_id => $attempt_id,
		original_failure => $original_failure // 'UNCLASSIFIED_INTERNAL_RESULT',
		created_at => strftime('%Y-%m-%dT%H:%M:%SZ', gmtime(time())),
		absolute_cleanup_deadline => ''
		  . ($attempt->{normal_cleanup_deadline_wallclock}
			  // $attempt->{cleanup_deadline_wallclock}
			  // $attempt->{cleanup_deadline}),
		cleanup_deadline_owner => defined($attempt->{normal_cleanup_deadline})
		  ? 'NORMAL_STOP' : 'STARTUP_FAILURE',
		startup_cleanup_deadline => ''
		  . ($attempt->{cleanup_deadline_wallclock}
			  // $attempt->{cleanup_deadline}),
		processes => [ map { +{ %$_ } }
			@{ $self->{two_stage_cleanup_processes} // [] } ],
		devices => \@devices,
		status => $status,
	};
	$self->_two_stage_write_manifest_data($path, $manifest);
	$self->{two_stage_cleanup_manifest_path} = $path;
	return $path;
}


sub _two_stage_write_cleanup_terminal
{
	my ($self, $status, $original_failure) = @_;
	my $path = $self->_two_stage_write_cleanup_manifest(
		$status, $original_failure);
	$self->_two_stage_update_failure_evidence_manifest($path, $status)
	  if defined($self->{two_stage_failure_evidence_path});
	return $path;
}


sub _two_stage_load_cleanup_manifests
{
	my ($self) = @_;
	my $root = $self->_two_stage_artifact_root();
	my $dir = "$root/cleanup-manifests";

	return [] unless -d $dir;
	my @loaded;
	for my $path (sort glob("$dir/*.json"))
	{
		my $resolved = abs_path($path);
		die "cleanup manifest path escaped the artifact root"
		  unless defined($resolved) && index($resolved, "$root/") == 0;
		open(my $fh, '<:raw', $resolved)
		  or die "open cleanup manifest $resolved: $!";
		local $/;
		my $bytes = <$fh>;
		close($fh) or die "close cleanup manifest $resolved: $!";
		my $manifest = eval { JSON::PP->new->utf8(1)->decode($bytes) };
		die "invalid cleanup manifest $resolved"
		  unless ref($manifest) eq 'HASH' && $manifest->{schema} == 1
		  && ($manifest->{attempt_id} // '') =~ /^[0-9a-f]+$/
		  && ref($manifest->{processes}) eq 'ARRAY'
		  && ref($manifest->{devices}) eq 'ARRAY';
		push @loaded, { path => $resolved, manifest => $manifest };
	}
	return \@loaded;
}


sub _two_stage_approved_resource_roots
{
	my ($self) = @_;
	my @candidates;

	if (ref($self->{two_stage_approved_resource_roots}) eq 'ARRAY')
	{
		@candidates = @{ $self->{two_stage_approved_resource_roots} };
	}
	else
	{
		@candidates = ($self->_two_stage_artifact_root());
		push @candidates, $PostgreSQL::Test::Utils::tmp_check
		  if defined($PostgreSQL::Test::Utils::tmp_check)
		  && length($PostgreSQL::Test::Utils::tmp_check) > 0;
	}
	my %seen;
	my @roots;
	for my $candidate (@candidates)
	{
		next unless defined($candidate) && length($candidate) > 0;
		my $resolved = abs_path($candidate);
		die "cannot resolve approved Phase-2 resource root $candidate"
		  unless defined($resolved) && -d $resolved;
		next if $seen{$resolved}++;
		push @roots, $resolved;
	}
	die "Phase-2 reaper has no approved resource root" unless @roots;
	return \@roots;
}


sub _two_stage_resolve_resource_path
{
	my ($path) = @_;

	return undef unless defined($path) && $path =~ m{\A/}
	  && index($path, "\0") < 0;
	my $probe = $path;
	my @missing;
	while (!-e $probe)
	{
		my $leaf = basename($probe);
		return undef if $leaf eq '' || $leaf eq '.' || $leaf eq '..';
		unshift @missing, $leaf;
		my $parent = dirname($probe);
		return undef if $parent eq $probe;
		$probe = $parent;
	}
	my $resolved = abs_path($probe);
	return undef unless defined($resolved) && $resolved =~ m{\A/};
	for my $leaf (@missing)
	{
		$resolved .= '/' unless $resolved eq '/';
		$resolved .= $leaf;
	}
	return $resolved;
}


sub _two_stage_manifest_resource_approved
{
	my ($self, $record) = @_;
	return 0 unless ref($record) eq 'HASH';
	my $major_minor = $record->{major_minor} // '';
	$major_minor =~ s/^\s+|\s+$//g;

	return 0 unless defined($record->{path})
	  && $record->{path} =~ m{\A/dev/loop\d+\z}
	  && $major_minor =~ /\A\d+:\d+\z/
	  && defined($record->{attach_order})
	  && $record->{attach_order} =~ /\A\d+\z/
	  && defined($record->{backing_realpath})
	  && $record->{backing_realpath} =~ m{\A/};
	my $backing = _two_stage_resolve_resource_path(
		$record->{backing_realpath});
	return 0 unless defined($backing)
	  && $backing eq $record->{backing_realpath};
	for my $root (@{ $self->_two_stage_approved_resource_roots() })
	{
		return 1 if $backing eq $root || index($backing, "$root/") == 0;
	}
	return 0;
}


sub _two_stage_current_loop_mapping
{
	my ($self, $device) = @_;

	die "recorded loop device is not a block device: $device" unless -b $device;
	my $major_minor = _run_capture(
		[ 'lsblk', '-dn', '-o', 'MAJ:MIN', $device ],
		"read loop identity $device");
	my $backing = _run_capture(
		[ 'sudo', '-n', 'losetup', '-n', '-O', 'BACK-FILE', $device ],
		"read loop backing $device");
	$backing =~ s/\s+\(deleted\)\z//;
	my $resolved = _two_stage_resolve_resource_path($backing);
	die "cannot resolve loop backing $backing" unless defined($resolved);
	$major_minor =~ s/^\s+|\s+$//g;
	return {
		path => $device,
		major_minor => $major_minor,
		backing_realpath => $resolved,
	};
}


sub _two_stage_recorded_device_mapping_absent
{
	my ($self, $record) = @_;

	die "post-detach proof lacks an exact loop device"
	  unless ref($record) eq 'HASH'
	  && defined($record->{path})
	  && $record->{path} =~ m{\A/dev/(loop\d+)\z};
	my $block = "/sys/class/block/" . basename($record->{path});
	die "post-detach proof cannot resolve $record->{path} in sysfs"
	  unless -d $block;
	return !-e "$block/loop/backing_file";
}


sub _two_stage_backing_loop_mappings
{
	my ($self, $backing) = @_;
	my $output = _run_capture(
		[ 'sudo', '-n', 'losetup', '-j', $backing ],
		"verify loop mappings for $backing");
	my @devices = $output =~ m{^(/dev/loop\d+):}mg;
	return \@devices;
}


sub _two_stage_detach_recorded_device
{
	my ($self, $record) = @_;

	run([ 'sudo', '-n', 'losetup', '-d', $record->{path} ])
	  or die "detach recorded device $record->{path} failed";
	return 1;
}


sub _two_stage_reap_one_manifest
{
	my ($self, $loaded) = @_;
	my $path = $loaded->{path};
	my $manifest = $loaded->{manifest};

	if (($manifest->{status} // '') eq 'REAPED')
	{
		unlink($path) or die "remove reaped cleanup manifest $path: $!";
		_two_stage_fsync_directory(dirname($path));
		return 'CLEAN';
	}
	for my $record (@{ $manifest->{processes} })
	{
		return 'CLEANUP_IDENTITY_CONFLICT'
		  unless defined($record->{pid}) && defined($record->{starttime});
		my $current = $self->_two_stage_proc_identity($record->{pid});
		return 'OPERATOR_CLEANUP_REQUIRED'
		  if !defined($current) || $current->{unavailable};
		next if exists($current->{exists}) && !$current->{exists};
		return 'CLEANUP_IDENTITY_CONFLICT'
		  unless defined($current->{starttime})
		  && $current->{starttime} == $record->{starttime};
		return 'OPERATOR_CLEANUP_REQUIRED';
	}
	my (%seen_device, %seen_backing, %seen_order);
	for my $record (@{ $manifest->{devices} })
	{
		return 'CLEANUP_IDENTITY_CONFLICT'
		  unless $self->_two_stage_manifest_resource_approved($record);
		return 'CLEANUP_IDENTITY_CONFLICT'
		  if $seen_device{$record->{path}}++
		  || $seen_backing{$record->{backing_realpath}}++
		  || $seen_order{$record->{attach_order}}++;
	}
	my @devices = map { $_->{path} } @{ $manifest->{devices} };
	my @backings = map { $_->{backing_realpath} } @{ $manifest->{devices} };
	my $holders = $self->_two_stage_device_holders(\@devices, \@backings);
	return 'OPERATOR_CLEANUP_REQUIRED' if @$holders;
	my @already_absent;
	for my $record (@{ $manifest->{devices} })
	{
		my $current = eval {
			$self->_two_stage_current_loop_mapping($record->{path})
		};
		if ($@ || !defined($current))
		{
			my $absent = eval {
				$self->_two_stage_recorded_device_mapping_absent($record)
			};
			return 'CLEANUP_IDENTITY_CONFLICT' if $@ || !$absent;
			push @already_absent, $record;
			next;
		}
		my $record_major = $record->{major_minor} // '';
		my $current_major = $current->{major_minor} // '';
		$record_major =~ s/^\s+|\s+$//g;
		$current_major =~ s/^\s+|\s+$//g;
		return 'CLEANUP_IDENTITY_CONFLICT' if $current_major ne $record_major
		  || ($current->{backing_realpath} // '')
			ne ($record->{backing_realpath} // '');
	}
	for my $record (sort {
		($b->{attach_order} // -1) <=> ($a->{attach_order} // -1)
	} @already_absent)
	{
		@{ $manifest->{devices} } = grep {
			$_->{path} ne $record->{path}
		} @{ $manifest->{devices} };
		$self->_two_stage_write_manifest_data($path, $manifest);
	}
	while (@{ $manifest->{devices} })
	{
		my @ordered = sort {
			($a->{attach_order} // -1) <=> ($b->{attach_order} // -1)
		} @{ $manifest->{devices} };
		my $record = $ordered[-1];
		$self->_two_stage_detach_recorded_device($record);
		my $detached = eval {
			$self->_two_stage_recorded_device_mapping_absent($record);
		};
		return 'CLEANUP_IDENTITY_CONFLICT'
		  if $@ || !$detached;
		@{ $manifest->{devices} } = grep {
			$_->{path} ne $record->{path}
		} @{ $manifest->{devices} };
		$self->_two_stage_write_manifest_data($path, $manifest);
	}
	$manifest->{status} = 'REAPED';
	$self->_two_stage_write_manifest_data($path, $manifest);
	unlink($path) or die "remove reaped cleanup manifest $path: $!";
	_two_stage_fsync_directory(dirname($path));
	return 'CLEAN';
}


sub _two_stage_reap_previous_manifests
{
	my ($self) = @_;
	my $loaded = $self->_two_stage_load_cleanup_manifests();
	my $result = 'CLEAN';

	for my $manifest (@$loaded)
	{
		my $current = $self->_two_stage_reap_one_manifest($manifest);
		return 'CLEANUP_IDENTITY_CONFLICT'
		  if $current eq 'CLEANUP_IDENTITY_CONFLICT';
		$result = $current if $current ne 'CLEAN';
	}
	return $result;
}


sub _two_stage_device_holders
{
	my ($self, $devices, $backings) = @_;
	my %identity;
	my @holders;

	for my $path (@$devices, @$backings)
	{
		next unless defined($path);
		my @st = stat($path);
		next unless @st;
		$identity{"$st[0]:$st[1]"} = $path;
	}
	for my $fd_path (glob('/proc/[0-9]*/fd/[0-9]*'))
	{
		my @st = stat($fd_path);
		next unless @st;
		my $path = $identity{"$st[0]:$st[1]"};
		next unless defined($path);
		my ($pid, $fd) = $fd_path =~ m{/proc/(\d+)/fd/(\d+)\z};
		push @holders, "pid=$pid fd=$fd path=$path";
	}
	return \@holders;
}


sub _two_stage_detach_scratch_loop
{
	my ($self, $device, $backing) = @_;

	run([ 'sudo', '-n', 'losetup', '-d', $device ])
	  or die "detach scratch device $device failed";
	my $mapping = _run_capture(
		[ 'sudo', '-n', 'losetup', '-j', $backing ],
		"verify scratch detach $backing");
	die "scratch mapping remains after detach: $mapping" if length($mapping) > 0;
	return 1;
}


sub _two_stage_cleanup_scratch
{
	my ($self) = @_;
	my $scratch = $self->{two_stage_scratch};

	return 1 unless ref($scratch) eq 'HASH';
	for my $actor (@{ $scratch->{child_processes} // [] })
	{
		die "scratch child cleanup lacks exact pid/starttime"
		  unless ref($actor) eq 'HASH'
		  && defined($actor->{pid}) && defined($actor->{starttime});
		my $current = $self->_two_stage_proc_identity($actor->{pid});
		die "scratch child identity is unavailable pid=$actor->{pid}"
		  if !defined($current) || $current->{unavailable};
		next if exists($current->{exists}) && !$current->{exists};
		die "scratch child identity conflict pid=$actor->{pid}"
		  unless defined($current->{starttime})
		  && $current->{starttime} == $actor->{starttime};
		die "scratch child remains live pid=$actor->{pid}"
		  if $current->{exists};
	}
	for my $actor (@{ $scratch->{child_processes} // [] })
	{
		for my $key (qw(stdout_path stderr_path))
		{
			my $path = $actor->{$key};
			next unless defined($path) && -e $path;
			unlink($path) or die "remove scratch actor output $path: $!";
		}
	}
	my @devices = defined($scratch->{device}) ? ($scratch->{device}) : ();
	my @backings = defined($scratch->{backing}) ? ($scratch->{backing}) : ();
	my $holders = $self->_two_stage_device_holders(\@devices, \@backings);
	die "scratch cleanup found live FD holders: " . join(', ', @$holders)
	  if @$holders;
	if (defined($scratch->{device}))
	{
		$self->_two_stage_detach_scratch_loop(
			$scratch->{device}, $scratch->{backing});
		delete $scratch->{device};
		delete $scratch->{record};
	}
	if (defined($scratch->{backing}) && -e $scratch->{backing})
	{
		unlink($scratch->{backing})
		  or die "remove scratch backing $scratch->{backing}: $!";
		delete $scratch->{backing};
	}
	delete $self->{two_stage_scratch};
	return 1;
}


sub _two_stage_freeze_scratch_failure_evidence
{
	my ($self) = @_;
	my $scratch = $self->{two_stage_scratch};
	return unless ref($scratch) eq 'HASH';
	return if ref($self->{two_stage_scratch_failure_evidence}) eq 'HASH';
	my $frozen = {
		backing => $scratch->{backing},
		device => $scratch->{device},
		child_processes => [ map { +{
			actor => $_->{actor} // 'scratch_probe',
			node => $_->{node}, pid => $_->{pid},
			starttime => $_->{starttime},
			last_state => $_->{last_state} // 'UNAVAILABLE',
			finished => $_->{finished} ? 1 : 0,
			exit_code => defined($_->{exit_code})
			  ? $_->{exit_code} : 'UNAVAILABLE',
			wait_status => defined($_->{wait_status})
			  ? $_->{wait_status} : 'UNAVAILABLE',
		} } @{ $scratch->{child_processes} // [] } ],
	};
	$frozen->{record} = { %{ $scratch->{record} } }
	  if ref($scratch->{record}) eq 'HASH';
	$frozen->{geometry} = { %{ $scratch->{geometry} } }
	  if ref($scratch->{geometry}) eq 'HASH';
	$self->{two_stage_scratch_failure_evidence} = $frozen;
	return $frozen;
}


sub _two_stage_qualify_block_backend
{
	my ($self) = @_;
	my $authoritative = $self->{voting_disk_paths} // [];
	my $scratch;
	my $safe_to_cleanup = 0;
	my $ok = eval {
		$scratch = $self->_two_stage_create_scratch_backing();
		for my $path (@$authoritative)
		{
			die "scratch path aliases authoritative voting bytes"
			  if $scratch eq $path;
		}
		$safe_to_cleanup = 1;
		$self->_two_stage_assert_equivalent_backend($scratch, $authoritative);
		my $device = $self->_two_stage_attach_scratch_loop($scratch);
		my $geometry = $self->_two_stage_attest_scratch_loop($device, $scratch);
		$self->{two_stage_scratch}{geometry} = { %$geometry };
		if (ref($self->{two_stage_scratch}{record}) eq 'HASH')
		{
			$self->{two_stage_scratch}{record}{capacity_bytes} = -s $scratch;
			$self->{two_stage_scratch}{record}{direct_io} = 1;
			$self->{two_stage_scratch}{record}{logical_block_size}
			  = $geometry->{logical_block_size} + 0;
			$self->{two_stage_scratch}{record}{physical_block_size}
			  = $geometry->{physical_block_size} + 0;
		}
		my $region = _two_stage_direct_io_region_size(
			$geometry->{logical_block_size},
			$geometry->{physical_block_size});
		die "scratch device is too small for four disjoint probe regions"
		  unless (-s $scratch) >= $NODES * $region;
		$self->_two_stage_run_direct_io_children($device, $region);
		$self->_two_stage_cleanup_scratch();
		1;
	};
	if ($ok)
	{
		$self->{two_stage_device_io_qualified} = 1;
		return 1;
	}
	my $failure = $@ || 'unknown scratch qualification failure';
	my $cleanup_failure = '';
	$self->_two_stage_freeze_scratch_failure_evidence();
	if ($safe_to_cleanup)
	{
		my $clean = eval { $self->_two_stage_cleanup_scratch(); 1 };
		$cleanup_failure = $@ || 'scratch cleanup failed' unless $clean;
	}
	my $detail = $failure;
	$detail .= "; cleanup=$cleanup_failure" if length($cleanup_failure) > 0;
	$detail =~ s/\s+\z//;
	$self->_two_stage_record_first_failure(
		'BLOCK_DEVICE_UNQUALIFIED', $detail, -1,
		'DEVICE_IO_QUALIFIED');
	die "BLOCK_DEVICE_UNQUALIFIED: $detail\n";
}


sub _two_stage_detach_voting_loops
{
	my ($self) = @_;
	my $devices = $self->{two_stage_loop_devices} //= [];
	my $records = $self->{two_stage_loop_records} //= [];

	while (@$devices)
	{
		my $device = $devices->[-1];
		my @matching = grep { $_->{path} eq $device } @$records;
		die "voting detach lacks one exact resource record for $device"
		  unless @matching == 1;
		my $record = $matching[0];

		run([ 'sudo', '-n', 'losetup', '-d', $device ])
		  or die "detach voting device $device failed";
		my $residual = $self->_two_stage_backing_loop_mappings(
			$record->{backing_realpath});
		die "voting mapping remains after detach for $device"
		  if @$residual;
		pop @$devices;
		@$records = grep {
			$_->{path} ne $device
		} @$records;
	}
	return;
}


END
{
	my $exit_code = $?;
	my @cleanup_errors;

	for my $quad (@TWO_STAGE_LOOP_QUADS)
	{
		my $result;
		my $ok = eval { $result = $quad->_two_stage_cleanup_registered(); 1 };
		push @cleanup_errors, $@ || 'two-stage cleanup failed' unless $ok;
		push @cleanup_errors, "two-stage cleanup result=$result"
		  if $ok && defined($result) && $result ne 'CLEAN';
	}
	if (@cleanup_errors)
	{
		warn "two-stage ClusterQuad cleanup failed: "
		  . join('; ', @cleanup_errors);
		$? = $exit_code != 0 ? $exit_code : 1;
	}
	else
	{
		$? = $exit_code;
	}
}



# Relocate an init_from_backup node's copied pg_wal into its shared WAL thread
# directory, matching the layout initdb -X produces for the seed node.
sub _relocate_backup_pg_wal
{
	my ($node, $wal_threads_root, $thread_id) = @_;
	my $pgwal = $node->data_dir . '/pg_wal';
	my $wal_thread = "$wal_threads_root/thread_$thread_id";

	mkdir $wal_thread or die "mkdir $wal_thread: $!";
	opendir(my $dh, $pgwal) or die "opendir $pgwal: $!";
	for my $e (readdir $dh)
	{
		next if $e eq '.' || $e eq '..';
		rename("$pgwal/$e", "$wal_thread/$e")
		  or die "rename $pgwal/$e -> $wal_thread/$e: $!";
	}
	closedir $dh;
	rmdir $pgwal or die "rmdir $pgwal: $!";
	symlink($wal_thread, $pgwal) or die "symlink $pgwal -> $wal_thread: $!";
	return;
}

#-----------------------------------------------------------------------
# new_quad($class, $cluster_name, %opts)
#
#	Allocate four PG instances that share a pgrac cluster name.
#	Optional %opts (mirror ClusterTriple):
#	  extra_conf          : arrayref of extra GUC lines appended to ALL
#	                        nodes' postgresql.conf
#	  quorum_voting_disks : N  -> strict mode, pre-allocate N shared
#	                        voting-disk files (QVOTEC reaches quorum OK)
#	  wal_threads_root    : 1  -> shared per-thread WAL root
#	  shared_data         : 1  -> shared data root (cluster_fs backend)
#	  shared_system_identifier : 1 -> initialize node0 once and clone the
#	                        other three nodes from that clean baseline
#	  shared_system_identifier_seed_sql : SQL executed by node0 before that
#	                        baseline is cloned; requires shared_data so user
#	                        relation files are seeded in the shared data root
#	  shared_catalog      : 1  -> t/337-style shared-catalog formation;
#	                        implies shared_data + wal_threads_root and enables
#	                        the required online-join/XID-striping substrate
#-----------------------------------------------------------------------
sub new_quad
{
	my ($class, $cluster_name, %opts) = @_;

	# Allocate 12 distinct free ports (4 PG + 4 IC + 4 DATA).
	# spec-7.2 D2: DATA-plane ports are allocator-provided, never
	# offset-derived (r1-F2).  spec-7.3 D9: the LMS worker pool binds a
	# listener per worker at data_port + worker_id, so each node needs a
	# reserved [data_port, data_port + span) range; the default span
	# follows the shipped default cluster.lms_workers = 2 (same root fix
	# as ClusterPair/ClusterTriple -- a span of 1 left data_port + 1
	# unreserved and the quad FATALed at boot on cross-wired worker
	# listeners).
	my @pg_ports;
	my @ic_ports;
	my @data_ports;
	my $data_span = $opts{data_port_span} // 2;
	for (0 .. $NODES - 1)
	{
		push @pg_ports, PostgreSQL::Test::Cluster::get_free_port();
	}
	for (0 .. $NODES - 1)
	{
		push @ic_ports, PostgreSQL::Test::Cluster::get_free_port();
	}
	for (0 .. $NODES - 1)
	{
		push @data_ports,
		  $data_span > 1
		  ? PostgreSQL::Test::Cluster::get_free_port_range($data_span)
		  : PostgreSQL::Test::Cluster::get_free_port();
	}

	my @nodes;
	for my $i (0 .. $NODES - 1)
	{
		push @nodes,
		  PostgreSQL::Test::Cluster->new(
			"${cluster_name}_node$i", port => $pg_ports[$i]);
	}

	if ($opts{shared_catalog})
	{
		$opts{shared_data} = 1;
		$opts{wal_threads_root} = 1;
	}
	elsif (($ENV{PGRAC_TEST_TWO_STAGE_VOTING_LOOP} // '') eq '1')
	{
		# Match the formal two-stage harness substrate: the phase-1 clean
		# terminal is the current-boot shared WAL slot, while shared catalog,
		# control-file authority, and user relations remain on cluster_fs.
		$opts{shared_catalog} = 1;
		$opts{shared_data} = 1;
		$opts{wal_threads_root} = 1;
	}

	# spec-4.6: strict-mode opt-in (mirror ClusterTriple) -- pre-allocate N
	# shared voting-disk files so QVOTEC reaches quorum_state=OK and the GES
	# inbound validation (in_quorum, check 4) accepts cross-node traffic.
	my $voting_disks_csv;
	my @voting_disk_paths;
	if (defined $opts{quorum_voting_disks} && $opts{quorum_voting_disks} > 0)
	{
		my $disk_dir = PostgreSQL::Test::Utils::tempdir();
		for my $i (0 .. $opts{quorum_voting_disks} - 1)
		{
			my $path = "$disk_dir/disk$i";
			format_voting_file($path, $i);
			push @voting_disk_paths, $path;
		}
		$voting_disks_csv = join(',', @voting_disk_paths);
	}

	# spec-4.1 opt-in: shared per-thread WAL root (mirror ClusterTriple).
	my $wal_threads_root;
	if ($opts{wal_threads_root})
	{
		$wal_threads_root = PostgreSQL::Test::Utils::tempdir();
	}

	# spec-4.5a opt-in: shared data root (mirror ClusterTriple).  One tempdir
	# all four postmasters write user-relation blocks into through the
	# cluster_fs shared_fs backend (cluster_smgr passthrough).
	my $shared_data_root;
	if ($opts{shared_data})
	{
		$shared_data_root = PostgreSQL::Test::Utils::tempdir();
		if ($opts{shared_catalog})
		{
			mkdir "$shared_data_root/global"
			  or die "mkdir $shared_data_root/global: $!";
		}
	}
	if ($opts{shared_catalog})
	{
		my $sc_common = <<EOC;
shared_buffers = 16MB
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_data_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

		$nodes[0]->init(allows_streaming => 1,
			extra => [
				'-X', "$wal_threads_root/thread_1",
				"--pgrac-wal-state-root=$wal_threads_root" ]);
		$nodes[0]->start;
		$nodes[0]->backup('clusterquad_scb');
		$nodes[0]->stop;

		for my $i (1 .. $NODES - 1)
		{
			$nodes[$i]->init_from_backup($nodes[0], 'clusterquad_scb');
			_relocate_backup_pg_wal($nodes[$i], $wal_threads_root, $i + 1);
		}

		# Seed the shared catalog/controlfile/OID authorities in a single-node era,
		# then append the real cluster config below.  Last GUC value wins.
		$nodes[0]->append_conf('postgresql.conf', $sc_common);
		$nodes[0]->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC
		my $seed_pgrac_conf = $nodes[0]->data_dir . '/pgrac.conf';
		PostgreSQL::Test::Utils::append_to_file($seed_pgrac_conf, <<EOC);
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_ports[0]
data_addr = 127.0.0.1:$data_ports[0]
EOC
		$nodes[0]->start;
		die "shared_catalog seed did not create catalog authority"
		  unless -e "$shared_data_root/global/pgrac_catalog_authority";
		$nodes[0]->stop;
		die "shared_catalog seed did not create shared pg_control authority"
		  unless -e "$shared_data_root/global/pg_control";
		die "shared_catalog seed created the pre-R4 control root"
		  if -e "$shared_data_root/global/pgrac_control_root"
		  || -e "$shared_data_root/global/pgrac_control_root.bak";
		unlink($seed_pgrac_conf)
		  or die "unlink seed-only $seed_pgrac_conf: $!";
	}
	elsif ($opts{shared_system_identifier})
	{
		my $seed_sql = $opts{shared_system_identifier_seed_sql};
		my @init_extra;
		if (defined $seed_sql)
		{
			die "shared_system_identifier_seed_sql requires shared_data"
			  unless defined $shared_data_root;
			die "shared_system_identifier_seed_sql must be a nonempty scalar"
			  if ref($seed_sql) || $seed_sql eq '';
		}

		push @init_extra, (
			'-X', "$wal_threads_root/thread_1",
			"--pgrac-wal-state-root=$wal_threads_root")
		  if defined $wal_threads_root;
		$nodes[0]->init(allows_streaming => 1,
			extra => \@init_extra);
		if (defined $seed_sql)
		{
			# This is the same native, pre-formation seed boundary used by the
			# macOS harness.  Catalog rows stay in PGDATA for the physical clone;
			# user-relation bytes go directly to the one shared data root.
			$nodes[0]->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_data_root'
cluster.smgr_user_relations = on
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
cluster.relation_extend_lock_enabled = off
EOC
		}
		$nodes[0]->start;
		if (defined $seed_sql)
		{
			$nodes[0]->safe_psql('postgres', $seed_sql,
				timeout => ($opts{shared_system_identifier_seed_timeout} // 900));
		}
		$nodes[0]->backup('clusterquad_homogeneous_baseline');
		$nodes[0]->stop;

		for my $i (1 .. $NODES - 1)
		{
			$nodes[$i]->init_from_backup(
				$nodes[0], 'clusterquad_homogeneous_baseline');
			_relocate_backup_pg_wal(
				$nodes[$i], $wal_threads_root, $i + 1)
			  if defined $wal_threads_root;
		}
	}
	else
	{
		my $wal_node_index = 0;
		for my $node (@nodes)
		{
			if (defined $wal_threads_root)
			{
				my $thread_id = $wal_node_index + 1;
				$node->init(extra => [ '-X', "$wal_threads_root/thread_$thread_id" ]);
			}
			else
			{
				$node->init;
			}
			$wal_node_index++;
		}
	}

	for my $node (@nodes)
	{
		if (defined $wal_threads_root)
		{
			$node->append_conf('postgresql.conf',
				"cluster.wal_threads_dir = '$wal_threads_root'\n");
		}

		if (defined $shared_data_root)
		{
			$node->append_conf('postgresql.conf',
				"cluster.shared_storage_backend = cluster_fs\n");
			$node->append_conf('postgresql.conf',
				"cluster.shared_data_dir = '$shared_data_root'\n");
			$node->append_conf('postgresql.conf',
				"cluster.smgr_user_relations = on\n");
			if ($opts{shared_catalog})
			{
				$node->append_conf('postgresql.conf',
					"cluster.lms_enabled = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.controlfile_shared_authority = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.shared_catalog = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.merged_recovery = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.online_join = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.xid_striping = on\n");
			}
			if (defined $opts{shared_system_identifier_seed_sql})
			{
				# Undo the native seed-only values before the four-node start.
				# extra_conf remains last and may narrow either GUC deliberately.
				$node->append_conf('postgresql.conf',
					"cluster.lms_enabled = on\n");
				$node->append_conf('postgresql.conf',
					"cluster.relation_extend_lock_enabled = on\n");
			}
		}

		# Enable cluster + tier1, same baseline as ClusterTriple (spec-2.2).
		$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
		$node->append_conf('postgresql.conf',
			"cluster.interconnect_tier = tier1\n");
		if (defined $voting_disks_csv)
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = off\n");
			$node->append_conf('postgresql.conf',
				"cluster.voting_disks = '$voting_disks_csv'\n");
		}
		else
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = on\n");
		}

		# Keep shared_buffers small so 4 postmasters fit in CI runners.
		$node->append_conf('postgresql.conf', "shared_buffers = 16MB\n");
		if (($ENV{PGRAC_STAGE8_HAPPY_PATH_ONLY} // '') eq '1')
		{
			# Stage 8's dedicated Linux happy-path VM has enough memory for the
			# correctness/PRE workload.  Keep the ordinary ClusterQuad CI default
			# above; this last-value-wins block is intentionally scoped to the
			# user-approved happy-path campaign.
			$node->append_conf('postgresql.conf', <<'EOC');
shared_buffers = 1GB
cluster.pcm_grd_max_entries = 131072
cluster.ges_dedup_max_entries = 65536
cluster.gcs_block_dedup_max_entries = 32768
wal_buffers = 64MB
max_wal_size = 4GB
min_wal_size = 1GB
max_connections = 512
work_mem = 4MB
maintenance_work_mem = 64MB
checkpoint_timeout = 5min
checkpoint_completion_target = 0.9
EOC
		}

		if ($opts{extra_conf})
		{
			for my $line (@{ $opts{extra_conf} })
			{
				$node->append_conf('postgresql.conf', "$line\n");
			}
		}
	}

	# Per-node identity.
	for my $i (0 .. $NODES - 1)
	{
		$nodes[$i]->append_conf('postgresql.conf', "cluster.node_id = $i\n");
	}

	# Build the shared pgrac.conf body declaring all four peers.
	my $peers_block = "";
	for my $i (0 .. $NODES - 1)
	{
		$peers_block .= "[node.$i]\n"
		  . "interconnect_addr = 127.0.0.1:$ic_ports[$i]\n"
		  . "data_addr = 127.0.0.1:$data_ports[$i]\n\n";
	}

	my $pgrac_conf_body = <<EOC;
[cluster]
name = $cluster_name

$peers_block
EOC

	for my $node (@nodes)
	{
		PostgreSQL::Test::Utils::append_to_file(
			$node->data_dir . '/pgrac.conf', $pgrac_conf_body);
	}

	return bless {
		nodes             => \@nodes,
		cluster_name      => $cluster_name,
		pg_ports          => \@pg_ports,
		ic_ports          => \@ic_ports,
		data_ports        => \@data_ports,
		voting_disk_paths => \@voting_disk_paths,
		wal_threads_root  => $wal_threads_root,
		shared_data_root  => $shared_data_root,
	}, $class;
}


sub start_quad
{
	my ($self, %opts) = @_;
	if (($ENV{PGRAC_STAGE8_HAPPY_PATH_ONLY} // '') eq '1')
	{
		die "happy-path voting loop harness is Linux-only" unless $^O eq 'linux';
		$self->_happy_path_prepare_voting_loops();
		for my $node (@{ $self->{nodes} })
		{
			$node->start(%opts);
		}
	}
	elsif (($ENV{PGRAC_TEST_TWO_STAGE_VOTING_LOOP} // '') eq '1')
	{
		die "two-stage voting loop harness is Linux-only" unless $^O eq 'linux';
		$self->_two_stage_run_lifecycle();
	}
	else
	{
		for my $node (@{ $self->{nodes} })
		{
			$node->start(%opts);
		}
	}
	# Diagnostic note (mirrors ClusterTriple/ClusterPair pattern).
	my $name = $self->{cluster_name} // '(unknown)';
	my $msg = "ClusterQuad started: cluster_name='$name'";
	for my $i (0 .. $NODES - 1)
	{
		my $pg = $self->{nodes}[$i]->port;
		my $ic = $self->{ic_ports}[$i] // -1;
		$msg .= " node$i=pg:$pg/ic:$ic";
	}
	Test::More::note($msg);
	$self->{two_stage_start_completed} = 1;
	return;
}


sub stop_quad
{
	my ($self, %opts) = @_;
	if ($self->{two_stage_cleanup_registered})
	{
		my $result = $self->_two_stage_cleanup_registered(
			normal_stop => !$opts{failure_cleanup});
		die "two-stage ClusterQuad cleanup result=$result"
		  unless defined($result) && $result eq 'CLEAN';
		return;
	}
	for my $node (@{ $self->{nodes} })
	{
		$node->stop if $node;
	}
	$self->_two_stage_detach_voting_loops();
	return;
}


#-----------------------------------------------------------------------
# kill_node9($self, $idx)
#
#	spec-5.14 fail-stop leg -- hard-kill one node of the quad (SIGKILL to
#	the postmaster).  Children (the CSSD heartbeat producer included) exit
#	on postmaster death, so the survivors' deadband fires the DEAD edge
#	without any cooperative shutdown handshake.  kill9 clears the node's
#	_pid, so a later stop_quad skips the dead node.
#-----------------------------------------------------------------------
sub kill_node9
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad kill_node9: SIGKILL node$idx postmaster");
	$self->{nodes}[$idx]->kill9;
	return;
}


#-----------------------------------------------------------------------
# stop_node($self, $idx)
#
#	Graceful stop of one node (postmaster fast shutdown).  The node leaves
#	the live membership as ABSENT without writing a clean-leave marker --
#	the substrate for a later join_node() peer-restart rejoin (spec-5.15).
#-----------------------------------------------------------------------
sub stop_node
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad stop_node: graceful stop node$idx");
	$self->{nodes}[$idx]->stop;
	return;
}


#-----------------------------------------------------------------------
# leave_node($self, $idx)
#
#	spec-5.13 cooperative clean-leave leg -- ask node $idx to leave the
#	cluster.  Returns the pg_cluster_clean_leave_request() result string
#	('accepted' on success;  a 'rejected:...' reason otherwise).  The
#	caller polls pg_cluster_clean_leave_state.phase / the survivors'
#	pg_cluster_reconfig_state for convergence.
#-----------------------------------------------------------------------
sub leave_node
{
	my ($self, $idx) = @_;
	Test::More::note("ClusterQuad leave_node: clean-leave request on node$idx");
	return $self->{nodes}[$idx]->safe_psql('postgres',
		'SELECT pg_cluster_clean_leave_request()');
}


#-----------------------------------------------------------------------
# join_node($self, $idx, %opts)
#
#	spec-5.15 online join/rejoin leg -- start node $idx (which must be
#	currently down: previously kill_node9'd, stop_node'd, or never
#	started) so it re-enters the live membership via the coordinator
#	two-phase epoch protocol.  There is no pg_cluster_join() UDF;  5.15
#	join is driven by a declared node coming back online.
#-----------------------------------------------------------------------
sub join_node
{
	my ($self, $idx, %opts) = @_;
	Test::More::note("ClusterQuad join_node: peer-restart rejoin node$idx");
	$self->{nodes}[$idx]->start(%opts);
	return;
}


#-----------------------------------------------------------------------
# remove_node($self, $coord_idx, $target_id)
#
#	spec-5.18 permanent-removal leg -- ask a surviving coordinator node
#	($coord_idx) to permanently remove node $target_id from the cluster
#	(fence + cluster-wide cleanup).  Returns the pg_cluster_remove_node()
#	result.  The target must already be down/fenced.
#-----------------------------------------------------------------------
sub remove_node
{
	my ($self, $coord_idx, $target_id) = @_;
	Test::More::note(
		"ClusterQuad remove_node: node$coord_idx removes node $target_id");
	return $self->{nodes}[$coord_idx]->safe_psql('postgres',
		"SELECT pg_cluster_remove_node($target_id)");
}


sub node      { return $_[0]->{nodes}[ $_[1] ]; }
sub node0     { return $_[0]->{nodes}[0]; }
sub node1     { return $_[0]->{nodes}[1]; }
sub node2     { return $_[0]->{nodes}[2]; }
sub node3     { return $_[0]->{nodes}[3]; }
sub nodes     { return @{ $_[0]->{nodes} }; }
sub ic_port   { return $_[0]->{ic_ports}[ $_[1] ]; }
sub pg_port   { return $_[0]->{pg_ports}[ $_[1] ]; }
sub cluster_name { return $_[0]->{cluster_name}; }

# spec-4.1: shared per-thread WAL root (undef unless wal_threads_root => 1).
sub wal_threads_root { return $_[0]->{wal_threads_root}; }

# spec-4.5a: shared data root (undef unless shared_data => 1).
sub shared_data_root { return $_[0]->{shared_data_root}; }

# spec-2.6: pre-allocated voting-disk file paths (empty without strict mode).
sub voting_disk_paths { return @{ $_[0]->{voting_disk_paths} // [] }; }


#-----------------------------------------------------------------------
# wait_for_peer_state($self, $from, $to, $expected_state, $timeout_s)
#
#	Polls $from's pg_cluster_ic_peers.state for $to until it matches
#	$expected_state or $timeout_s elapses.  Returns 1 on success, 0 on
#	timeout.  Mirrors ClusterTriple::wait_for_peer_state for the 4-node
#	reconfig matrix.
#-----------------------------------------------------------------------
sub wait_for_peer_state
{
	my ($self, $from, $to, $expected_state, $timeout_s) = @_;
	$timeout_s //= 30;
	my $node = $self->{nodes}[$from];
	my $deadline = time + $timeout_s;
	my $last_state = '(never-queried)';
	while (time < $deadline)
	{
		my $state = $node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $to");
		$last_state = $state // '(null)';
		return 1 if defined $state && $state eq $expected_state;
		select(undef, undef, undef, 0.25);
	}
	Test::More::diag(
		"ClusterQuad wait_for_peer_state TIMEOUT after ${timeout_s}s: "
		. "from=node$from to=$to expected='$expected_state' "
		. "last_observed='$last_state'");
	return 0;
}


#-----------------------------------------------------------------------
# wait_for_membership_count($self, $from, $expected_members, $timeout_s)
#
#	Polls $from's pg_cluster_membership for the number of nodes in the
#	'member' state (the membership-decision SSOT, INV-J8) until it equals
#	$expected_members or $timeout_s elapses.  Used by the reconfig matrix
#	to assert membership convergence after a fault/leave/join.  The
#	membership view's state column is one of absent/dead/joining/member/
#	rejected (spec-5.15 D6);  'member' is the live admitted state.
#	Returns 1 on success, 0 on timeout.
#-----------------------------------------------------------------------
sub wait_for_membership_count
{
	my ($self, $from, $expected_members, $timeout_s) = @_;
	$timeout_s //= 30;
	my $node = $self->{nodes}[$from];
	my $deadline = time + $timeout_s;
	my $last = '(never-queried)';
	while (time < $deadline)
	{
		my $n = $node->safe_psql('postgres',
			"SELECT count(*) FROM pg_cluster_membership "
			. "WHERE state = 'member'");
		$last = defined $n ? $n : '(null)';
		return 1 if defined $n && $n == $expected_members;
		select(undef, undef, undef, 0.25);
	}
	Test::More::diag(
		"ClusterQuad wait_for_membership_count TIMEOUT after ${timeout_s}s: "
		. "from=node$from expected_members=$expected_members last=$last");
	return 0;
}


1;
