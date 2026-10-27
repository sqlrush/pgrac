#!/usr/bin/env perl
# Observe a native CONTROL reconnect under the original OPEN and normal stop.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my ($cluster, $stop_attempted, $paused_pid, $paused_fingerprint);
my (@nodes, @controls);
my $json = JSON::PP->new->canonical->pretty;
my $evidence = $ENV{TESTLOGDIR} // '.';
my %progress = (pause_sent => 0, timeout_seen => 0, resumed => 0,
	control_rebound => 0, atomic_graph => JSON::PP::false,
	before_pause_ack_exported => JSON::PP::false);

sub save_json
{
	my ($name, $value) = @_;
	open my $f, '>', "$evidence/control-reconnect-$name.json" or die $!;
	print {$f} $json->encode($value);
	close $f or die $!;
}

sub require_true
{
	my ($value, $description) = @_;
	ok($value, $description) or die "$description\n";
}

sub require_equal
{
	my ($actual, $expected, $description) = @_;
	is($actual, $expected, $description) or die "$description\n";
}

sub fingerprint
{
	my ($pid) = @_;
	die 'invalid native LMON pid' unless $pid =~ /^[1-9][0-9]*$/;
	my ($out, $err) = ('', '');
	run(['ps', '-o', 'ppid=,lstart=', '-p', $pid], '>', \$out, '2>', \$err,
		timeout(2)) or die "cannot sample native LMON birth: $err";
	die 'missing native LMON birth' unless $out =~ /\S/;
	return $out;
}

sub resume_owner
{
	return 1 unless defined $paused_pid;
	# This OS birth/parent check guards only a cleanup signal. It is not a
	# substitute for the native PGPROC identity asserted on both sides below.
	my $same = eval { fingerprint($paused_pid) eq $paused_fingerprint };
	if (!$same)
	{
		warn "cannot prove the original paused process for SIGCONT: $@";
		return 0;
	}
	my $pid = $paused_pid;
	kill('CONT', $pid) or die "SIGCONT $pid failed: $!";
	$paused_pid = undef;
	$progress{resumed} = 1;
	$progress{resume_monotonic} = clock_gettime(CLOCK_MONOTONIC);
	return 1;
}

END
{
	eval { resume_owner() };
	warn "CONTROL owner resume failed: $@" if $@;
	for my $control (@controls) { eval { $control->quit } }
	if (defined($cluster) && $cluster->{running} && !$stop_attempted)
	{
		$stop_attempted = 1;
		eval { $cluster->stop_cluster };
		warn "original cleanup normal stop failed: $@" if $@;
	}
}

my $info_sql = q{
	SELECT json_build_object(
		'observer_pid', pg_backend_pid(),
		'postmaster_start', pg_postmaster_start_time()::text,
		'lmon', (SELECT row_to_json(a) FROM
			(SELECT pid, backend_start::text FROM pg_stat_activity
			 WHERE backend_type='lmon') a),
		'members', (SELECT json_agg(row_to_json(m) ORDER BY node_id) FROM
			(SELECT node_id, declared, state, presented_incarnation::text,
			 last_admitted_incarnation::text, admitted_epoch::text,
			 removed, removed_epoch::text FROM pg_cluster_membership) m),
		'peers', (SELECT json_agg(row_to_json(p) ORDER BY node_id) FROM
			(SELECT node_id, state, interconnect_addr, reconnect_count,
			 heartbeat_recv_count, last_heartbeat_recv_at::text, last_error
			 FROM pg_cluster_ic_peers) p))};

sub sample
{
	my ($node, $label) = @_;
	my $cut = {node => $node, monotonic => clock_gettime(CLOCK_MONOTONIC)};
	$cut->{before} = decode_json($controls[$node]->query_safe($info_sql));
	$cut->{graph} = decode_json($controls[$node]->query_safe(
		'SELECT control_reconnect_graph()'));
	$cut->{after} = decode_json($controls[$node]->query_safe($info_sql));
	save_json("$label-node$node", $cut);
	return $cut;
}

sub row_for
{
	my ($rows, $field, $value) = @_;
	return undef unless ref($rows) eq 'ARRAY' && defined($value);
	my @found = grep { ref($_) eq 'HASH' && defined($_->{$field})
		&& "$_->{$field}" eq "$value" } @$rows;
	return @found == 1 ? $found[0] : undef;
}

sub uint_fields
{
	my ($row, @fields) = @_;
	return 0 unless ref($row) eq 'HASH';
	for my $field (@fields)
	{
		return 0 unless defined($row->{$field}) && !ref($row->{$field})
			&& $row->{$field} =~ /^[0-9]+$/;
	}
	return 1;
}

sub bool_fields
{
	my ($row, @fields) = @_;
	return 0 unless ref($row) eq 'HASH';
	for my $field (@fields)
	{
		return 0 unless defined($row->{$field})
			&& (!ref($row->{$field}) || JSON::PP::is_bool($row->{$field}))
			&& $row->{$field} =~ /^[01]$/;
	}
	return 1;
}

sub native_lmon
{
	my ($cut) = @_;
	my ($before, $after) = @{$cut}{qw(before after)};
	return undef unless ref($before->{lmon}) eq 'HASH' && ref($after->{lmon}) eq 'HASH';
	return undef unless uint_fields($before->{lmon}, 'pid') && $before->{lmon}{pid} > 0
		&& $json->encode($before->{lmon}) eq $json->encode($after->{lmon})
		&& ($before->{lmon}{backend_start} // '') ne ''
		&& ($before->{postmaster_start} // '') ne ''
		&& defined($after->{postmaster_start})
		&& $before->{postmaster_start} eq $after->{postmaster_start};
	my $proc = row_for($cut->{graph}{procs}, 'pid', $before->{lmon}{pid});
	return undef unless uint_fields($proc, qw(pid procno proc_generation))
		&& defined($proc->{backend_id}) && !ref($proc->{backend_id})
		&& $proc->{backend_id} eq '-1'
		&& bool_fields($proc, 'stable') && $proc->{stable} && $proc->{proc_generation} > 0;
	return {pid => $proc->{pid}, procno => $proc->{procno}, backend_id => $proc->{backend_id},
		proc_generation => $proc->{proc_generation},
		backend_start => $before->{lmon}{backend_start},
		postmaster_start => $before->{postmaster_start}};
}

sub endpoint
{
	my ($cut, $peer) = @_;
	my $g = $cut->{graph};
	my ($caps, $stream);
	for my $side (qw(begin end))
	{
		my $c = $g->{"peer_$side"};
		my $s = $g->{"streams_$side"};
		return undef unless bool_fields($c, qw(known busy)) && $c->{known} && !$c->{busy}
			&& bool_fields($s, 'known') && $s->{known}
			&& uint_fields($s, qw(epoch channels local_bits)) && $s->{channels} > 0;
		my $current = row_for($c->{rows}, 'node', $peer);
		my $session = row_for($s->{rows}, 'node', $peer);
		return undef unless uint_fields($current, qw(node bits generation))
			&& bool_fields($current, 'valid') && $current->{valid} && $current->{generation} > 0
			&& uint_fields($session, qw(node control)) && $session->{control} > 0
			&& bool_fields($session, 'known') && $session->{known}
			&& $session->{control} eq $current->{generation};
		return undef unless ref($session->{data}) eq 'ARRAY'
			&& @{$session->{data}} == $s->{channels};
		for my $generation (@{$session->{data}})
		{
			return undef unless defined($generation) && !ref($generation)
				&& $generation =~ /^[1-9][0-9]*$/;
		}
		return undef if defined($caps) && $json->encode($caps) ne $json->encode($current);
		return undef if defined($stream) && $json->encode($stream) ne $json->encode($session);
		($caps, $stream) = ($current, $session);
	}
	return {caps => $caps, stream => $stream};
}

sub stable_cut
{
	my ($cut) = @_;
	return 0 unless defined native_lmon($cut);
	my ($a, $b) = @{$cut}{qw(before after)};
	return 0 unless uint_fields($a, 'observer_pid') && uint_fields($b, 'observer_pid')
		&& $a->{observer_pid} > 0
		&& $a->{observer_pid} eq $b->{observer_pid};
	return 0 unless ref($a->{members}) eq 'ARRAY' && @{$a->{members}} == 4
		&& $json->encode($a->{members}) eq $json->encode($b->{members});
	for my $node (0 .. 3)
	{
		my $member = row_for($a->{members}, 'node_id', $node);
		return 0 unless uint_fields($member, qw(node_id presented_incarnation
			last_admitted_incarnation admitted_epoch removed_epoch))
			&& bool_fields($member, qw(declared removed)) && $member->{declared}
			&& ($member->{state} // '') eq 'member' && !$member->{removed}
			&& $member->{presented_incarnation} > 0
			&& $member->{last_admitted_incarnation} > 0 && $member->{admitted_epoch} > 0;
	}
	my $g = $cut->{graph};
	return 0 unless uint_fields($g, qw(epoch_begin epoch_end)) && $g->{epoch_begin} > 0
		&& $g->{epoch_begin} eq $g->{epoch_end};
	for my $field (qw(known epoch channels local_bits))
	{
		return 0 unless defined($g->{streams_begin}{$field})
			&& defined($g->{streams_end}{$field})
			&& $g->{streams_begin}{$field} eq $g->{streams_end}{$field};
	}
	return 0 unless $g->{streams_begin}{epoch} eq $g->{epoch_begin};
	my ($first, $last) = @{$g}{qw(semantic_begin semantic_end)};
	return 0 unless bool_fields($first, qw(known transition_closed)) && $first->{known}
		&& bool_fields($last, qw(known transition_closed)) && $last->{known}
		&& uint_fields($first, qw(seq active_bits record_generation formation_epoch))
		&& uint_fields($last, qw(seq active_bits record_generation formation_epoch))
		&& $json->encode($first) eq $json->encode($last)
		&& ($first->{active_bits} & 1) != 0 && $first->{record_generation} > 0
		&& $first->{formation_epoch} > 0
		&& !$first->{transition_closed};
	for my $peer (grep { $_ != $cut->{node} } 0 .. 3)
	{
		return 0 unless defined endpoint($cut, $peer);
	}
	return 1;
}

sub stable_samples
{
	my ($label) = @_;
	for my $attempt (1 .. 10)
	{
		my @cuts = map { sample($_, "$label-$attempt") } 0 .. 3;
		return \@cuts unless grep { !stable_cut($_) } @cuts;
		sleep(0.1);
	}
	die "no known stable native $label cut; raw samples retained\n";
}

sub check_formation
{
	my ($root, $open, $label) = @_;
	require_true(scalar(($root->{system_identifier} // '') =~ /^[1-9][0-9]*$/
		&& ($root->{generation} // '') =~ /^[1-9][0-9]*$/
		&& ($root->{root_digest} // '') =~ /^[0-9a-f]{64}$/),
		"$label has a complete selected native ROOT identity");
	for my $node (0 .. 3)
	{
		my $o = $open->[$node];
		require_true(($o->{phase} // '') eq 'OPEN'
			&& ($o->{system_identifier} // '') eq $root->{system_identifier}
			&& ($o->{generation} // '') eq $root->{generation}
			&& ($o->{root_digest} // '') eq $root->{root_digest}
			&& join(',', @{$o->{members} // []}) eq '0,1,2,3'
			&& ref($o->{writer}) eq 'HASH' && defined($o->{writer}{node})
			&& $o->{writer}{node} == $node && ($o->{writer}{boot} // '') ne ''
			&& !(grep { ($o->{writer}{$_} // '') !~ /^[1-9][0-9]*$/ }
				qw(thread incarnation wal_generation)),
			"$label node$node has its original native OPEN and complete writer identity");
	}
}

my ($before, $after);
my $body_ok = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('control_reconnect',
		nodes => 4, blackbox => 1, required_operations => [],
		extra_conf => ['autovacuum=off', 'cluster.trace_normal_stop=on',
			'cluster.trace_tx_identity=on', 'log_error_verbosity=verbose',
			'log_connections=on', 'log_disconnections=on']);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	$nodes[0]->safe_psql('postgres', q{
		CREATE FUNCTION control_reconnect_graph() RETURNS text
		AS '$libdir/test_pgrac_shared_config', 'test_pgrac_wait_graph'
		LANGUAGE C VOLATILE PARALLEL UNSAFE;
		REVOKE ALL ON FUNCTION control_reconnect_graph() FROM PUBLIC});
	for my $node (@nodes)
	{
		push @controls, $node->background_psql('postgres', timeout => 30);
	}
	my $root = $cluster->observe('root_observation');
	my @open = map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3;
	save_json('formation', {root => $root, open => \@open});
	check_formation($root, \@open, 'before pause');
	$before = stable_samples('before');
	require_true(@$before == 4, 'four actual member cuts have live R4 and known native identities');
	my $owner = native_lmon($before->[2]);
	my $heartbeat = row_for($before->[0]{after}{peers}, 'node_id', 2);
	require_true(defined($heartbeat) && $heartbeat->{state} eq 'connected'
		&& $heartbeat->{heartbeat_recv_count} > 0,
		'node0 has an authentic connected CONTROL peer and received heartbeats');
	$paused_fingerprint = fingerprint($owner->{pid});
	$paused_pid = $owner->{pid};
	kill('STOP', $paused_pid) or die "SIGSTOP $paused_pid failed: $!";
	$progress{pause_sent} = 1;
	$progress{pause_monotonic} = clock_gettime(CLOCK_MONOTONIC);
	my ($state, $state_error) = ('', '');
	my $state_deadline = clock_gettime(CLOCK_MONOTONIC) + 1;
	while (clock_gettime(CLOCK_MONOTONIC) < $state_deadline)
	{
		run(['ps', '-o', 'stat=', '-p', $paused_pid], '>', \$state, '2>', \$state_error,
			timeout(2)) or die "cannot observe paused LMON: $state_error";
		last if $state =~ /T/;
		sleep(0.02);
	}
	save_json('pause', {owner => $owner, os_birth => $paused_fingerprint,
		state => $state, monotonic => $progress{pause_monotonic}});
	require_true(scalar($state =~ /T/), 'the original native LMON actually reached the OS stop state');
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 8;
	my @polls;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		my $info = decode_json($controls[0]->query_safe($info_sql));
		my $peer = row_for($info->{peers}, 'node_id', 2);
		push @polls, {monotonic => clock_gettime(CLOCK_MONOTONIC), peer => $peer};
		if (defined($peer) && $peer->{reconnect_count} > $heartbeat->{reconnect_count}
			&& ($peer->{last_error} // '') =~ /liveness timeout/)
		{
			$progress{timeout_seen} = 1;
			last;
		}
		sleep(0.05);
	}
	save_json('timeout-polls', \@polls);
	require_true(resume_owner(), 'resume the original paused LMON under its OS birth guard');
	require_true($progress{timeout_seen}, 'original heartbeat timeout closed the real CONTROL stream');
	$after = stable_samples('after');
	for my $node (0 .. 3)
	{
		require_equal($json->encode(native_lmon($after->[$node])),
			$json->encode(native_lmon($before->[$node])), "node$node retains its native LMON and postmaster identity");
		require_equal($json->encode($after->[$node]{after}{members}),
			$json->encode($before->[$node]{after}{members}), "node$node retains full membership and boot identities");
		for my $field (qw(active_bits record_generation formation_epoch))
		{
			require_equal($after->[$node]{graph}{semantic_end}{$field},
				$before->[$node]{graph}{semantic_end}{$field}, "node$node keeps semantic $field");
		}
	}
	for my $pair ([0, 2], [2, 0])
	{
		my ($node, $peer) = @$pair;
		my $a = endpoint($before->[$node], $peer);
		my $b = endpoint($after->[$node], $peer);
		require_equal($a->{caps}{bits}, $b->{caps}{bits}, "node$node peer$peer keeps capability bits");
		require_true($a->{stream}{control} ne $b->{stream}{control},
			"node$node peer$peer has a new authenticated CONTROL lifetime");
	}
	$progress{control_rebound} = 1;
	my $root_after = $cluster->observe('root_observation');
	my @open_after = map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3;
	save_json('after-formation', {root => $root_after, open => \@open_after});
	check_formation($root_after, \@open_after, 'after reconnect');
	for my $field (qw(system_identifier generation root_digest))
	{
		require_equal($root_after->{$field}, $root->{$field}, "reconnect retains selected ROOT $field");
	}
	for my $node (0 .. 3)
	{
		for my $field (qw(node thread incarnation wal_generation boot))
		{
			require_equal($open_after[$node]{writer}{$field}, $open[$node]{writer}{$field},
				"node$node retains OPEN writer $field");
		}
	}
	$progress{identity_preserved} = 1;
	1;
};
my $body_error = $@;
diag("CONTROL reconnect body failed: $body_error") unless $body_ok;
ok($body_ok, 'native same-key CONTROL reconnect prerequisites complete');
eval { resume_owner() };
diag("CONTROL resume cleanup failed: $@") if $@;
my @client_errors;
for my $control (@controls)
{
	my $closed = eval { $control->quit };
	my $error = $@;
	push @client_errors, length($error) ? $error : 'psql quit returned failure'
		unless $closed;
}
ok(!@client_errors, 'original observation clients close without an ignored error');
$progress{client_close_errors} = \@client_errors;
@controls = ();

my ($stopped, $stop_error);
if (defined($cluster) && $cluster->{running})
{
	$stop_attempted = 1;
	$stopped = eval { $cluster->stop_cluster; 1 };
	$stop_error = $@;
}
ok($stopped, 'original four-member shared_stop and shutdown observer pass');
diag("original normal stop failed: " . ($stop_error // 'formation did not start')) unless $stopped;
ok(defined($cluster) && $cluster->{normal_stop_proven}, 'original observer proves all members CLOSED');
my @rejections;
for my $node (0 .. $#nodes)
{
	my $log = slurp_file($nodes[$node]->logfile);
	unlike($log, qr/\b(?:ERROR|FATAL|PANIC):|invalid page in block|checksum verification failed/,
		"node$node original log has no severe error or damaged page");
	my @lines = split /\n/, $log;
	for my $line (0 .. $#lines)
	{
		push @rejections, {node => $node, line => $line + 1, raw => $lines[$line]}
			if $lines[$line] =~ /semantic normal-stop /;
	}
}
save_json('matcher-trace', \@rejections);
save_json('result', {progress => \%progress, body_ok => $body_ok ? 1 : 0,
	body_error => $body_error, stop_attempted => $stop_attempted // 0,
	stop_ok => $stopped ? 1 : 0, stop_error => $stop_error // '',
	root_closed => defined($cluster) && $cluster->{normal_stop_proven} ? 1 : 0});
done_testing();
