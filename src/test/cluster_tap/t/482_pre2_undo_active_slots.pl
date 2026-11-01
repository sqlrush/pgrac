#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 482_pre2_undo_active_slots.pl
#    Native empty-undo preflight, full-segment wait, and terminal cleanup.
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
#    Four normal members; empty-undo cases and all 49 slot transactions run
#    on node 0. Independent heaps avoid page ITL and row-lock pressure.
#    No allocator state is set and no terminal cleanup is forced.
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
	my ($sample, $script) = @_;
	$script //= 'undo_slot_wait.py';
	my $input = encode_json($sample);
	my ($out, $err) = ('', '');
	run([$ENV{PYTHON} // 'python3', "$FindBin::RealBin/../pre2/$script"],
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

sub run_full_preflight_cancellations
{
	my ($name, $owners, $chosen, $waiter, $before) = @_;
	my @cases = (
		['zero_update', 'UPDATE g_empty_undo SET value=999 WHERE false;'],
		['empty_copy', "COPY g_empty_undo FROM STDIN;\n\\.\n"],
		['conflict_nothing', 'INSERT INTO g_empty_undo VALUES(1,999) ON CONFLICT(id) DO NOTHING;'],
		['zero_row_lock', 'SELECT * FROM g_empty_undo WHERE false FOR UPDATE;'],
		['statement_trigger_error', 'UPDATE g_empty_trigger SET value=999 WHERE false;']);
	for my $case (@cases)
	{
		my $label = "$name-full-$case->[0]";
		my $old_counters = counters();
		$waiter->{transaction} = 1;
		$waiter->{xid} = 0 + $waiter->{client}->query_safe('BEGIN; SELECT pg_current_xact_id()::text');
		$waiter->{pending} = 1;
		my $sent = $waiter->{client}->query_until(qr/G_SLOT_SENT\r?\n/,
			"\\echo G_SLOT_SENT\n$case->[1]\n\\echo G_SLOT_SQLSTATE :SQLSTATE\n");
		save_json("$label-sent", {output => $sent, pid => $waiter->{pid}, xid => $waiter->{xid}});
		my $wait_activity = wait_witness($label, $waiter);
		judge("$label-blocked", {stage => 'blocked', before => $before, after => census($owners),
			counters_before => $old_counters, counters_after => counters(),
			waiter_pid => $waiter->{pid}, waiter_xid => $waiter->{xid}, activity => $wait_activity,
			locks => lock_rows($waiter->{pid}, $chosen->{pid})});
		cancel_waiter($label, $owners, $waiter, $before, $old_counters);
	}
}

sub run_full_slot_deadlines
{
	my ($name, $owners, $chosen, $waiter, $before) = @_;
	my $settings_sql = q{SELECT current_setting('lock_timeout')||'|'||current_setting('statement_timeout')};
	my $original_settings = $waiter->{client}->query_safe($settings_sql);
	$waiter->{client}->query_safe("\\set VERBOSITY verbose");
	for my $case (['lock', 'lock_timeout', '55P03', 0],
		['statement', 'statement_timeout', '57014', 1])
	{
		my ($kind, $setting, $state, $cancel_delta) = @$case;
		my $label = "$name-deadline-$kind";
		my $old_counters = counters();
		$waiter->{transaction} = 1;
		$waiter->{xid} = 0 + $waiter->{client}->query_safe(
			"BEGIN; SET LOCAL lock_timeout=0; SET LOCAL statement_timeout=90000; "
			. "SET LOCAL $setting=30000; SELECT pg_current_xact_id()::text");
		$waiter->{pending} = 1;
		my $sent = $waiter->{client}->query_until(qr/G_SLOT_SENT\r?\n/,
			"\\echo G_SLOT_SENT\nUPDATE g_empty_undo SET value=999 WHERE false;\n"
			. "\\echo G_SLOT_SQLSTATE :SQLSTATE\n");
		save_json("$label-sent", {output => $sent, pid => $waiter->{pid}, xid => $waiter->{xid},
			setting => $setting, deadline_ms => 30000, original_settings => $original_settings});
		my $wait_activity = wait_witness($label, $waiter);
		judge("$label-blocked", {stage => 'blocked', before => $before, after => census($owners),
			counters_before => $old_counters, counters_after => counters(),
			waiter_pid => $waiter->{pid}, waiter_xid => $waiter->{xid}, activity => $wait_activity,
			locks => lock_rows($waiter->{pid}, $chosen->{pid})});
		# No signal or owner completion: the caller's original native timer ends this wait.
		my ($out, $error) = $waiter->{client}->query('');
		my $stderr = $waiter->{client}{stderr};
		save_json("$label-completion", {output => $out, error => $error, stderr => $stderr});
		$waiter->{pending} = 0;
		my @states = $out =~ /^G_SLOT_SQLSTATE ([A-Z0-9]{5})\r?$/mg;
		my $reason = "$kind timeout";
		die "native deadline did not preserve $state/$reason\n" unless
		  $error == 1 && @states == 1 && $states[0] eq $state
		  && $stderr =~ /ERROR:\s+$state: canceling statement due to $reason/;
		$waiter->{client}{stderr} = '';
		end_owner($waiter, 'ROLLBACK');
		is($waiter->{client}->query_safe('SELECT pg_backend_pid()'), "$waiter->{pid}",
			'deadline caller remains the same native backend') or die 'deadline backend changed';
		is_deeply(binding($waiter), {available => JSON::PP::false, wait_scope => JSON::PP::false},
			'deadline clears wait scope without a canonical binding') or die 'deadline binding leaked';
		my $after = census($owners);
		is_deeply($after, $before, 'deadline leaves all 48 original owners unchanged')
		  or die 'deadline changed original owners';
		my $new_counters = counters();
		save_json("$label-after", {counters => $new_counters, census => $after});
		stable_supply($old_counters, $new_counters);
		# Reuse the original exact uint64 parser; 55P03 is not caller cancellation.
		my $input = encode_json({before => $old_counters, after => $new_counters,
			cancel_delta => $cancel_delta});
		my ($stdout, $err) = ('', '');
		my $code = q{import json,sys
from undo_slot_wait import decimal
r=json.load(sys.stdin)
b,a=r['before'],r['after']
assert b.keys()==a.keys()
for key in b:
    if key=='tt_slot_active_full_wait_count':
        assert decimal(a[key])==decimal(b[key])+1
    elif key=='tt_capacity_cancel_count':
        assert decimal(a[key])==decimal(b[key])+r['cancel_delta']
    else:
        assert a[key]==b[key],key
};
		local $ENV{PYTHONPATH} = "$FindBin::RealBin/../pre2";
		run([$ENV{PYTHON} // 'python3', '-c', $code], '<', \$input, '>', \$stdout,
			'2>', \$err, timeout(10)) or die "native deadline counter mismatch: $err";
		pass("$label preserves exact original deadline/cancellation counters");
		my $restored = $waiter->{client}->query_safe($settings_sql);
		save_json("$label-settings", {before => $original_settings, after => $restored});
		is($restored, $original_settings, 'transaction-local deadline restores original settings')
		  or die 'deadline settings leaked';
	}
	$waiter->{client}->query_safe("\\set VERBOSITY default");
}

sub wait_preflight_retirement
{
	my ($name, $entry, $offset) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 30;
	my $sample;
	my $first = 1;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		$sample = empty_observation($entry);
		save_json("$name-retirement-first", $sample) if $first;
		$first = 0;
		if ($sample->{snapshot}{available})
		{
			my @slots = grep { $_->{offset} == $offset } @{$sample->{snapshot}{slots}};
			return $sample if @slots == 1 && $slots[0]{status} == 0;
		}
		sleep(0.03);
	}
	save_json("$name-retirement-last", $sample);
	die "NOT_REACHED $name: original waiter slot cleanup did not finish\n";
}

sub resume_full_preflight
{
	my ($name, $mode, $owners, $chosen, $waiter, $blocked, $case) = @_;
	my ($original) = grep { $_->{pid} == $chosen->{pid} } @{$blocked->{before}{bindings}};
	die 'selected owner has no original binding' unless defined($original);
	end_owner($chosen, $mode eq 'commit' ? 'COMMIT' : 'ROLLBACK');
	push @expected, "$chosen->{table}|$chosen->{trial}" if $mode eq 'commit';
	my $trigger = $case->[0] eq 'statement_trigger_error';
	my $first = empty_statement("$name-first", $waiter, undef, $trigger);
	my $repeated = $trigger ? undef : empty_statement("$name-repeated", $waiter, $case->[1], 0);
	end_owner($waiter, $trigger || $mode eq 'abort' ? 'ROLLBACK' : 'COMMIT');
	my $after = wait_preflight_retirement($name, $waiter, $original->{slot_offset});
	my $data_sql = q{SELECT 'data',id,value FROM g_empty_undo }
	  . q{UNION ALL SELECT 'trigger',id,value FROM g_empty_trigger ORDER BY 1,2};
	my $sample = {kind => $case->[0], mode => $mode, blocked => $blocked,
		first => $first, repeated => $repeated, after => $after,
		remaining => [map { binding($_) } grep { $_->{pid} != $chosen->{pid} } @$owners],
		data => [map { $_->safe_psql('postgres', $data_sql, timeout => 30) } @nodes]};
	save_json("$name-resume-input", $sample);
	my $result = evaluate_sample($sample, 'full_preflight_resume.py');
	save_json("$name-resume-result", $result);
	die "NOT_REACHED $name: $result->{reason}\n" unless $result->{passed};
	pass("$name: full-slot caller resumes and retires the exact original slot");
}

sub run_case
{
	my ($mode, $trial, $case) = @_;
	my $name = "$trial-$mode";
	$name .= "-$case->[0]" if defined($case);
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
	$waiter->{client}->query_safe("\\set VERBOSITY verbose\nSET client_min_messages=notice")
	  if defined($case);
	if ($mode eq 'cancel')
	{
		# Reuse this cohort and backend; every cancelled preflight keeps all48 owners.
		run_full_preflight_cancellations($name, \@owners, $chosen, $waiter, $before);
		run_full_slot_deadlines($name, \@owners, $chosen, $waiter, $before);
		$old_counters = counters();
	}
	$waiter->{trial} = $trial;
	$waiter->{transaction} = 1;
	$waiter->{xid} = 0 + $waiter->{client}->query_safe('BEGIN; SELECT pg_current_xact_id()::text');
	$waiter->{pending} = 1;
	my $sent = defined($case) ? $waiter->{client}->query_until(qr/G_SLOT_SENT\r?\n/,
		"\\echo G_SLOT_SENT\n$case->[1]\n\\echo G_EMPTY_SQLSTATE :SQLSTATE\n")
	  : $waiter->{client}->query_until(qr/G_SLOT_SENT\r?\n/,
		"\\echo G_SLOT_SENT\nINSERT INTO g_slot_heap_48 VALUES($trial);\n\\echo G_SLOT_SQLSTATE :SQLSTATE\n");
	save_json("$name-sent", {output => $sent, pid => $waiter->{pid}, xid => $waiter->{xid}});
	my $wait_activity = wait_witness($name, $waiter);
	my $blocked = {stage => 'blocked', before => $before, after => census(\@owners),
		counters_before => $old_counters, counters_after => counters(),
		waiter_pid => $waiter->{pid}, waiter_xid => $waiter->{xid}, activity => $wait_activity,
		locks => lock_rows($waiter->{pid}, $chosen->{pid})};
	judge("$name-blocked", $blocked);
	if (defined($case))
	{
		resume_full_preflight($name, $mode, \@owners, $chosen, $waiter, $blocked, $case);
	}
	elsif ($mode eq 'cancel')
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

sub empty_observation
{
	my ($entry) = @_;
	my $sample = decode_json($entry->{client}->query_safe(
		q{SELECT json_build_object('pid',pg_backend_pid(),}
		. q{'top',coalesce(pg_current_xact_id_if_assigned()::text,'none'),}
		. q{'binding',test_pgrac_undo_slot_binding()::json)}));
	$sample->{snapshot} = decode_json($writer->safe_psql('postgres',
		'SELECT test_pgrac_undo_slot_snapshot()', timeout => 10));
	$sample->{counters} = counters();
	return $sample;
}

sub empty_statement
{
	my ($name, $entry, $sql, $trigger) = @_;
	my ($out, $error) = $entry->{client}->query(
		defined($sql) ? "$sql\n\\echo G_EMPTY_SQLSTATE :SQLSTATE\n" : '');
	$entry->{pending} = 0 unless defined($sql);
	my $stderr = $entry->{client}{stderr};
	save_json("$name-statement", {output => $out, error => $error, stderr => $stderr});
	my @states = $out =~ /^G_EMPTY_SQLSTATE ([A-Z0-9]{5})\r?$/mg;
	die "$name: missing or ambiguous native SQLSTATE\n" unless @states == 1;
	my $result = {error => $error, sqlstate => $states[0]};
	if ($trigger)
	{
		die "$name: expected statement-trigger P0001 was not observed\n" unless
		  $error == 1 && $states[0] eq 'P0001'
		  && $stderr =~ /ERROR:\s+P0001: G_EMPTY_EXPECTED_TRIGGER_ERROR/;
		my @observations = $stderr =~ /^(?:psql:[^\r\n]*:\d+:[ \t]+)?NOTICE:.*G_EMPTY_OBSERVATION (\{.*\})\r?$/mg;
		die "$name: missing or ambiguous pre-error native observation\n" unless @observations == 1;
		my $sample = decode_json($observations[0]);
		$sample->{result} = $result;
		$sample->{counters} = counters();
		# Preserve the complete expected NOTICE/ERROR before reusing this backend.
		$entry->{client}{stderr} = '';
		return $sample;
	}
	die "$name: empty statement failed (SQLSTATE $states[0])\n" unless
	  $error == 0 && $states[0] eq '00000';
	my $sample = empty_observation($entry);
	$sample->{result} = $result;
	return $sample;
}

sub run_empty_case
{
	my ($kind, $finish, $sql) = @_;
	my $name = "empty-$kind-$finish";
	wait_idle($name);
	my $entry = open_session("g_empty_${kind}_$finish");
	$entry->{client}->query_safe("\\set VERBOSITY verbose\nSET client_min_messages=notice");
	$entry->{transaction} = 1;
	$entry->{client}->query_safe('BEGIN');
	my $before = empty_observation($entry);
	save_json("$name-before", $before);
	my $trigger = $kind eq 'statement_trigger_error';
	my $first = empty_statement("$name-first", $entry, $sql, $trigger);
	save_json("$name-first-observation", $first);
	my $repeated = $trigger ? undef
	  : empty_statement("$name-repeated", $entry, $sql, 0);
	save_json("$name-repeated-observation", $repeated) unless $trigger;
	end_owner($entry, $finish eq 'commit' ? 'COMMIT' : 'ROLLBACK');
	wait_idle("$name-finished");
	my $after = empty_observation($entry);
	my $data_sql = q{SELECT 'data',id,value FROM g_empty_undo }
	  . q{UNION ALL SELECT 'trigger',id,value FROM g_empty_trigger ORDER BY 1,2};
	my $sample = {kind => $kind, finish => $finish, pid => $entry->{pid},
		before => $before, first => $first, repeated => $repeated, after => $after,
		data => [map { $_->safe_psql('postgres', $data_sql, timeout => 30) } @nodes]};
	save_json("$name-input", $sample);
	my $result = evaluate_sample($sample, 'empty_undo_preflight.py');
	save_json("$name-result", $result);
	die "NOT_REACHED $name: $result->{reason}\n" unless $result->{passed};
	pass("$name: original preflight and terminal cleanup join exact native evidence");
	finish_sessions($name);
}

sub run_empty_cases
{
	$writer->safe_psql('postgres', <<'SQL', timeout => 90);
CREATE TABLE g_empty_undo (id integer PRIMARY KEY, value integer);
CREATE TABLE g_empty_trigger (id integer, value integer);
INSERT INTO g_empty_undo VALUES (1,100);
CREATE FUNCTION g_empty_statement_error() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
  RAISE NOTICE 'G_EMPTY_OBSERVATION %', json_build_object(
    'pid', pg_backend_pid(),
    'top', coalesce(pg_current_xact_id_if_assigned()::text,'none'),
    'binding', test_pgrac_undo_slot_binding()::json,
    'snapshot', test_pgrac_undo_slot_snapshot()::json);
  RAISE EXCEPTION USING ERRCODE='P0001', MESSAGE='G_EMPTY_EXPECTED_TRIGGER_ERROR';
END;
$body$;
CREATE TRIGGER g_empty_raise BEFORE UPDATE ON g_empty_trigger
FOR EACH STATEMENT EXECUTE FUNCTION g_empty_statement_error();
SQL
	my @cases = (
		['zero_update', 'UPDATE g_empty_undo SET value=999 WHERE false;'],
		['empty_copy', "COPY g_empty_undo FROM STDIN;\n\\.\n"],
		['conflict_nothing', 'INSERT INTO g_empty_undo VALUES(1,999) ON CONFLICT(id) DO NOTHING;'],
		['zero_row_lock', 'SELECT * FROM g_empty_undo WHERE false FOR UPDATE;']);
	for my $case (@cases)
	{
		for my $finish (qw(commit abort))
		{
			run_empty_case($case->[0], $finish, $case->[1]);
		}
	}
	run_empty_case('statement_trigger_error', 'abort',
		'UPDATE g_empty_trigger SET value=999 WHERE false;');
	run_empty_case('pure_select', 'commit', 'SELECT * FROM g_empty_undo WHERE false;');
	run_empty_case('explain_only', 'commit', 'EXPLAIN UPDATE g_empty_undo SET value=999 WHERE false;');
}

sub run_full_preflight_resumes
{
	my @cases = (
		['zero_update', 'UPDATE g_empty_undo SET value=999 WHERE false;'],
		['empty_copy', "COPY g_empty_undo FROM STDIN;\n\\.\n"],
		['conflict_nothing', 'INSERT INTO g_empty_undo VALUES(1,999) ON CONFLICT(id) DO NOTHING;'],
		['zero_row_lock', 'SELECT * FROM g_empty_undo WHERE false FOR UPDATE;'],
		['statement_trigger_error', 'UPDATE g_empty_trigger SET value=999 WHERE false;']);
	my $trial = 100;
	for my $case (@cases)
	{
		for my $mode (qw(commit abort))
		{
			run_case($mode, $trial++, $case);
		}
	}
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('undo_active_slots', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			# Leave maintenance opportunities within the unchanged 30s idle budget.
			'cluster.undo_cleaner_interval_ms=1000',
			'log_error_verbosity=verbose', q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	$writer = $nodes[0];
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members');
	my @cleaner_intervals = map {
		$_->safe_psql('postgres', 'SHOW cluster.undo_cleaner_interval_ms', timeout => 10)
	} @nodes;
	is_deeply(\@cleaner_intervals, [('1000') x 4],
		'all four members use the explicit terminal-cleanup cadence')
	  or die 'terminal-cleanup cadence was not applied to every member';
	save_json('formation', {open => [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3],
		cleaner_interval_ms => \@cleaner_intervals,
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
	run_empty_cases();
	run_case('commit', 1);
	run_case('abort', 2);
	run_case('cancel', 3);
	run_full_preflight_resumes();
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
