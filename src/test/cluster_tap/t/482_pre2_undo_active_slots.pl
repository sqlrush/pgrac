#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 482_pre2_undo_active_slots.pl
#    Native full-segment wait, owner commit/abort, and caller cancellation.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/482_pre2_undo_active_slots.pl
#
# NOTES
#    Four normal members; all 49 transactions run on node 0. Independent
#    heaps avoid page ITL and row-lock pressure. No allocator state is set.
#-------------------------------------------------------------------------
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

my ($cluster, $writer);
my @nodes;
my @sessions;
my @expected;
my $stop_attempted = 0;
my $evidence = $ENV{TESTLOGDIR} // '.';
my @counter_keys = qw(tt_capacity_stats_available tt_capacity_origin_node_id
  tt_slot_active_full_wait_count tt_capacity_pool_wait_count tt_capacity_cancel_count
  segment_switch_count autoextend_count segment_reuse_count record_alloc_count);

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/undo-slot-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub evaluate_sample
{
	my ($sample) = @_;
	my $input = encode_json($sample);
	my ($out, $err) = ('', '');
	run([$ENV{PYTHON} // 'python3', "$FindBin::RealBin/../pre2/undo_slot_wait.py"],
		'<', \$input, '>', \$out, '2>', \$err, timeout(10))
	  or die "native slot evidence evaluator failed: $err";
	return decode_json($out);
}

sub judge
{
	my ($name, $sample) = @_;
	save_json("$name-input", $sample);
	my $result = evaluate_sample($sample);
	save_json("$name-result", $result);
	die "NOT_REACHED $name: $result->{reason}\n" unless $result->{passed};
	pass("$name joins exact allocator, canonical, and native lock evidence");
	return $result;
}

sub wait_idle
{
	my ($name) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 30;
	my $sample;
	my $first = 1;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		$sample = {stage => 'idle', snapshot => decode_json($writer->safe_psql('postgres',
			'SELECT test_pgrac_undo_slot_snapshot()', timeout => 10))};
		save_json("$name-idle-first", $sample) if $first;
		$first = 0;
		my $result = evaluate_sample($sample);
		if ($result->{passed})
		{
			judge("$name-idle", $sample);
			return $sample->{snapshot};
		}
		sleep(0.03);
	}
	save_json("$name-idle-last", $sample);
	die "NOT_REACHED $name: original CURRENT terminal cleanup did not finish\n";
}

sub counters
{
	my $keys = join(',', map { "'$_'" } @counter_keys);
	return decode_json($writer->safe_psql('postgres',
		"SELECT json_object_agg(key,value) FROM cluster_dump_state() WHERE key IN ($keys)",
		timeout => 10));
}

sub open_session
{
	my ($label) = @_;
	my $entry = {client => $writer->background_psql('postgres', timeout => 180,
		on_error_stop => 0), label => $label, transaction => 0, pending => 0};
	push @sessions, $entry;
	$entry->{client}->query_safe("SET application_name='$label'; SET statement_timeout=90000");
	$entry->{pid} = 0 + $entry->{client}->query_safe('SELECT pg_backend_pid()');
	return $entry;
}

sub binding
{
	my ($entry) = @_;
	my $sample = decode_json($entry->{client}->query_safe('SELECT test_pgrac_undo_slot_binding()'));
	die 'native binding belongs to another backend' if $sample->{available}
	  && $sample->{pid} != $entry->{pid};
	return $sample;
}

sub census
{
	my ($owners) = @_;
	return {stage => 'full', bindings => [map { binding($_) } @$owners],
		snapshot => decode_json($writer->safe_psql('postgres',
			'SELECT test_pgrac_undo_slot_snapshot()', timeout => 10))};
}

sub activity
{
	my ($pid) = @_;
	return decode_json($writer->safe_psql('postgres',
		"SELECT row_to_json(a) FROM (SELECT pid,backend_xid::text AS xid,"
		. "wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers "
		. "FROM pg_stat_activity WHERE pid=$pid) a", timeout => 10));
}

sub wait_witness
{
	my ($name, $entry) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 10;
	my $sample;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		$sample = activity($entry->{pid});
		if (($sample->{wait_event} // '') eq 'ClusterUndoTxSlotWait')
		{
			save_json("$name-wait-activity", $sample);
			return $sample;
		}
		sleep(0.03);
	}
	save_json("$name-last-activity", $sample);
	die "NOT_REACHED $name: original native transaction-slot wait not witnessed\n";
}

sub lock_rows
{
	my ($waiter, $owner) = @_;
	return decode_json($writer->safe_psql('postgres',
		"SELECT coalesce(json_agg(l),'[]'::json) FROM (SELECT pid,locktype,mode,granted,"
		. "virtualxid FROM pg_locks WHERE pid IN ($waiter,$owner) "
		. "AND locktype='virtualxid' ORDER BY pid,mode,virtualxid) l", timeout => 10));
}

sub end_owner
{
	my ($entry, $command) = @_;
	$entry->{client}->query_safe($command);
	$entry->{transaction} = 0;
}

sub finish_sessions
{
	my ($name) = @_;
	my @errors;
	for my $entry (@sessions)
	{
		my $done = eval {
			if ($entry->{pending})
			{
				# Only this fixture's unfinished caller is cancelled for cleanup.
				$writer->safe_psql('postgres', "SELECT pg_cancel_backend($entry->{pid})", timeout => 10);
				my ($out, $error) = $entry->{client}->query('');
				save_json("$name-cleanup-$entry->{pid}",
					{output => $out, error => $error, stderr => $entry->{client}{stderr}});
				$entry->{pending} = 0;
				$entry->{client}{stderr} = '';
			}
			end_owner($entry, 'ROLLBACK') if $entry->{transaction};
			1;
		};
		push @errors, $@ unless $done;
		my $closed = eval { $entry->{client}->quit; 1; };
		push @errors, $@ unless $closed;
	}
	@sessions = ();
	die join('', @errors) if @errors;
}

sub stable_supply
{
	my ($before, $after) = @_;
	for my $key (qw(segment_switch_count autoextend_count segment_reuse_count))
	{
		die "missing or changed original supply counter $key" unless
		  defined($before->{$key}) && defined($after->{$key}) && $before->{$key} eq $after->{$key};
	}
}

sub release_and_resume
{
	my ($name, $mode, $owners, $chosen, $waiter, $before, $old_counters) = @_;
	end_owner($chosen, $mode eq 'commit' ? 'COMMIT' : 'ROLLBACK');
	my ($out, $error) = $waiter->{client}->query('');
	save_json("$name-completion", {output => $out, error => $error,
		stderr => $waiter->{client}{stderr}});
	$waiter->{pending} = 0;
	die "native waiter did not complete successfully: $out" unless
	  $error == 0 && $out =~ /^G_SLOT_SQLSTATE 00000\s*$/m;
	my $new_binding = binding($waiter);
	my ($old_binding) = grep { $_->{pid} == $chosen->{pid} } @{$before->{bindings}};
	die 'waiter did not acquire the exact retired slot' unless $new_binding->{available}
	  && !$new_binding->{wait_scope} && $new_binding->{segment_id} == $old_binding->{segment_id}
	  && $new_binding->{slot_offset} == $old_binding->{slot_offset}
	  && $new_binding->{xid} == $waiter->{xid}
	  && $new_binding->{slot_wrap} != $old_binding->{slot_wrap};
	my @remaining = (grep { $_->{pid} != $chosen->{pid} } @$owners);
	my $resumed = census([@remaining, $waiter]);
	judge("$name-resumed", $resumed);
	is_deeply([grep { $_->{pid} != $waiter->{pid} } @{$resumed->{bindings}}],
		[grep { $_->{pid} != $chosen->{pid} } @{$before->{bindings}}],
		'all 47 other published canonical bindings remain unchanged');
	die 'CURRENT changed while satisfying the native waiter' unless
	  $resumed->{snapshot}{binding_generation} eq $before->{snapshot}{binding_generation}
	  && $resumed->{snapshot}{segment_id} == $before->{snapshot}{segment_id};
	my $new_counters = counters();
	stable_supply($old_counters, $new_counters);
	save_json("$name-resumed-counters", $new_counters);
	end_owner($waiter, 'COMMIT');
	push @expected, "$chosen->{table}|$chosen->{trial}" if $mode eq 'commit';
	push @expected, "48|$waiter->{trial}";
}

sub cancel_waiter
{
	my ($name, $owners, $waiter, $before, $old_counters) = @_;
	my $cancel = $writer->safe_psql('postgres', "SELECT pg_cancel_backend($waiter->{pid})", timeout => 10);
	my ($out, $error) = $waiter->{client}->query('');
	my $stderr = $waiter->{client}{stderr};
	save_json("$name-completion", {cancel => $cancel, output => $out, error => $error,
		stderr => $stderr});
	$waiter->{pending} = 0;
	die 'caller cancellation did not preserve native 57014' unless
	  $cancel eq 't' && $error == 1 && $out =~ /^G_SLOT_SQLSTATE 57014\s*$/m
	  && $stderr =~ /canceling statement due to user request/;
	# Preserve the complete expected error above before using the same backend.
	$waiter->{client}{stderr} = '';
	end_owner($waiter, 'ROLLBACK');
	is($waiter->{client}->query_safe('SELECT pg_backend_pid()'), "$waiter->{pid}",
		'cancelled caller remains the same native backend');
	is_deeply(binding($waiter), {available => JSON::PP::false, wait_scope => JSON::PP::false},
		'cancellation clears the original wait scope without publishing a binding');
	my $after = census($owners);
	is_deeply($after, $before, 'cancellation leaves all 48 original ACTIVE owners unchanged');
	my $new_counters = counters();
	save_json("$name-cancelled-counters", $new_counters);
	stable_supply($old_counters, $new_counters);
	die 'cancellation allocated an undo record' unless
	  $new_counters->{record_alloc_count} eq $old_counters->{record_alloc_count};
	# Counter arithmetic stays in the exact uint64-aware evidence evaluator.
	my $input = {before => $old_counters->{tt_capacity_cancel_count},
		after => $new_counters->{tt_capacity_cancel_count}};
	my ($stdout, $err) = ('', '');
	my $json = encode_json($input);
	my $code = 'import json,sys; from undo_slot_wait import decimal; '
	  . 'r=json.load(sys.stdin); assert decimal(r["after"])==decimal(r["before"])+1';
	local $ENV{PYTHONPATH} = "$FindBin::RealBin/../pre2";
	run([$ENV{PYTHON} // 'python3', '-c', $code], '<', \$json, '>', \$stdout,
		'2>', \$err, timeout(10)) or die "native cancellation count mismatch: $err";
	pass('native cancellation advances the original counter exactly once');
}

sub data_ledger
{
	my ($name) = @_;
	my $sql = join(' UNION ALL ', map { "SELECT $_ AS table_no,trial FROM g_slot_heap_$_" } 0 .. 48)
	  . ' ORDER BY table_no,trial';
	my $expected = join("\n", sort {
		my @left = split /\|/, $a;
		my @right = split /\|/, $b;
		$left[0] <=> $right[0] || $left[1] <=> $right[1]
	} @expected);
	my @actual = map { $_->safe_psql('postgres', $sql, timeout => 30) } @nodes;
	save_json("$name-data-ledger", {expected => $expected, actual => \@actual});
	for my $index (0 .. $#nodes)
	{
		is($actual[$index], $expected, "$name: node $index agrees with the independent commit ledger");
	}
}

sub run_case
{
	my ($mode, $trial) = @_;
	my $name = "$trial-$mode";
	# Existing cleaner GC must finish before owners can pin the old horizon.
	my $idle = wait_idle($name);
	my @owners;
	for my $index (0 .. 47)
	{
		my $entry = open_session("g_slot_${mode}_owner_$index");
		$entry->{table} = $index;
		$entry->{trial} = $trial;
		$entry->{transaction} = 1;
		$entry->{client}->query_safe("BEGIN; INSERT INTO g_slot_heap_$index VALUES($trial)");
		push @owners, $entry;
	}
	my $before = census(\@owners);
	die 'CURRENT changed while filling the prepared segment' unless
	  $before->{snapshot}{segment_id} == $idle->{segment_id}
	  && $before->{snapshot}{binding_generation} eq $idle->{binding_generation};
	my $full = judge("$name-full", $before);
	my ($chosen) = grep { $_->{pid} == $full->{selected_pid} } @owners;
	die 'the selected slot has no actual owner session' unless defined($chosen);
	my $old_counters = counters();
	my $waiter = open_session("g_slot_${mode}_waiter");
	$waiter->{trial} = $trial;
	$waiter->{transaction} = 1;
	$waiter->{xid} = 0 + $waiter->{client}->query_safe('BEGIN; SELECT pg_current_xact_id()::text');
	$waiter->{pending} = 1;
	my $sent = $waiter->{client}->query_until(qr/G_SLOT_SENT\r?\n/,
		"\\echo G_SLOT_SENT\nINSERT INTO g_slot_heap_48 VALUES($trial);\n\\echo G_SLOT_SQLSTATE :SQLSTATE\n");
	save_json("$name-sent", {output => $sent, pid => $waiter->{pid}, xid => $waiter->{xid}});
	my $wait_activity = wait_witness($name, $waiter);
	my $blocked = {stage => 'blocked', before => $before, after => census(\@owners),
		counters_before => $old_counters, counters_after => counters(),
		waiter_pid => $waiter->{pid}, waiter_xid => $waiter->{xid}, activity => $wait_activity,
		locks => lock_rows($waiter->{pid}, $chosen->{pid})};
	judge("$name-blocked", $blocked);
	if ($mode eq 'cancel')
	{
		cancel_waiter($name, \@owners, $waiter, $before, $old_counters);
	}
	else
	{
		release_and_resume($name, $mode, \@owners, $chosen, $waiter, $before, $old_counters);
	}
	finish_sessions($name);
	data_ledger($name);
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('undo_active_slots', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			'log_error_verbosity=verbose', q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	$writer = $nodes[0];
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members');
	save_json('formation', {open => [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3],
		producer_node => 0, peer_readers => [1, 2, 3]});
	for my $suffix (qw(snapshot binding))
	{
		my $function = "test_pgrac_undo_slot_$suffix";
		$writer->safe_psql('postgres', "CREATE FUNCTION $function() RETURNS text AS "
			. "'\$libdir/test_pgrac_shared_config', '$function' "
			. "LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE; REVOKE ALL ON FUNCTION $function() FROM PUBLIC");
	}
	$writer->safe_psql('postgres', join('; ',
		map { "CREATE TABLE g_slot_heap_$_ (trial integer)" } 0 .. 48), timeout => 90);
	run_case('commit', 1);
	run_case('abort', 2);
	run_case('cancel', 3);
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
eval { finish_sessions('final'); 1; } or $cleanup_error .= $@;
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	$stop_attempted = 1;
	eval {
		my $receipt = $cluster->stop_cluster;
		save_json('normal-stop', $receipt);
		die 'full normal-stop proof missing' unless $cluster->{normal_stop_proven};
		pass('all four native members complete the original normal-stop proof');
		1;
	} or $cleanup_error .= $@;
}
save_json('execution', {completed => $execution ? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error});
if (!$execution || $cleanup_error ne '')
{
	fail('native full-slot execution and cleanup complete');
	diag($first_error . $cleanup_error);
}
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/PANIC|checksum verification failed|invalid page in block/,
		'closed native member has no corruption or PANIC');
}
done_testing();
