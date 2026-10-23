#!/usr/bin/env perl
#-------------------------------------------------------------------------
# Exercise native four-member PRE2 formation, all-instance crash recovery,
# committed/loser reconciliation, and invalid-input refusal.  Each negative
# starts from its own proven OPEN cohort; no mocked adapter or skipped path.
#-------------------------------------------------------------------------

use strict;
use warnings;

use IPC::Run qw(run);
use JSON::PP;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my @CONF = (
	'autovacuum = off',
	'restart_after_crash = off',
	'shared_buffers = 16MB',
	'log_error_verbosity = verbose',
	'cluster.merged_recovery = on',
	'cluster.gcs_block_retransmit_max_retries = 8',
	'cluster.gcs_reply_timeout_ms = 3000',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.quorum_poll_interval_ms = 2000',
	'cluster.cssd_heartbeat_interval_ms = 2000',
	'cluster.cssd_dead_deadband_factor = 10',
);
my $json = JSON::PP->new->canonical;
# The native controller supplies a fresh three-device set for each cohort.
# Reusing a written voting device must remain a native fresh-init refusal.
my $entries = $json->decode($ENV{PGRAC_PRE2_E1_ENTRY_FILES} // 'null');
BAIL_OUT('462 requires three native PRE2 entry profiles')
  unless ref($entries) eq 'ARRAY' && @$entries == 3;
my %votes;
for my $entry (@$entries)
{
	BAIL_OUT('462 entry profile is not a readable file') unless !ref($entry) && -f $entry && -r $entry;
	my $profile = $json->decode(PostgreSQL::Test::Utils::slurp_file($entry));
	my $devices = $profile->{fixture}{votes};
	BAIL_OUT('462 requires three distinct voting devices per cohort')
	  unless ref($devices) eq 'ARRAY' && @$devices == 3;
	for my $device (@$devices)
	{
		BAIL_OUT('462 cohorts must not reuse voting devices')
		  if ref($device) || !defined($device) || $device eq '' || $votes{$device}++;
	}
}
sub now { return clock_gettime(CLOCK_MONOTONIC); }

sub log_offsets
{
	my ($cluster) = @_;
	return [map { -e $_->logfile ? -s $_->logfile : 0 } $cluster->nodes];
}

sub log_since
{
	my ($node, $offset) = @_;
	return '' unless -e $node->logfile;
	return PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset);
}

sub process_table
{
	my $out = '';
	run(['ps', '-axo', 'pid=,ppid=,stat=,lstart=,comm='], '>', \$out)
	  or die 'cannot inventory crash-owned processes';
	my %table;
	for my $line (split /\n/, $out)
	{
		next unless $line =~ /^\s*(\d+)\s+(\d+)\s+(\S+)\s+(.+)$/;
		my ($pid, $parent, $state, $start_and_command) = ($1, $2, $3, $4);
		my @start = split /\s+/, $start_and_command;
		die 'incomplete process birth timestamp' unless @start >= 6;
		$table{$pid} = {parent => $parent, state => $state, birth => join(' ', @start[0 .. 4])};
	}
	return \%table;
}

sub crash_all
{
	my ($cluster) = @_;
	my $table = process_table();
	my %owned;
	my @roots;
	for my $node ($cluster->nodes)
	{
		$node->_update_pid(-1);
		my $pid = $node->{_pid};
		die 'crash requires all four live postmasters'
		  unless defined($pid) && exists($table->{$pid});
		push @roots, $pid;
		$owned{$pid} = $table->{$pid}{birth};
	}
	while (1)
	{
		my @children = grep { !exists($owned{$_}) && exists($owned{$table->{$_}{parent}}) } keys %$table;
		last unless @children;
		$owned{$_} = $table->{$_}{birth} for @children;
	}
	diag('SIGKILL owned process inventory: ' . $json->encode(\%owned));
	# Kill all postmasters before waiting for any member.  Also terminate only
	# the captured descendants whose birth identity still matches.
	for my $pid (@roots)
	{
		kill(9, $pid) == 1 or die "SIGKILL postmaster $pid failed: $!";
	}
	my $current = process_table();
	for my $pid (keys %owned)
	{
		next unless exists($current->{$pid}) && $current->{$pid}{birth} eq $owned{$pid};
		kill(9, $pid) unless $current->{$pid}{state} =~ /^Z/;
	}
	my $deadline = now() + 30;
	while (1)
	{
		$current = process_table();
		my @live = grep { exists($current->{$_}) && $current->{$_}{birth} eq $owned{$_}
			&& $current->{$_}{state} !~ /^Z/ } keys %owned;
		last unless @live;
		die 'crash-owned processes survived: ' . join(',', @live) if now() >= $deadline;
		sleep(0.05);
	}
	$_->_update_pid(-1) for $cluster->nodes;
	# This resets test bookkeeping only after observed process exit.  The next
	# start still consumes the original disks through native shared_start and
	# every PRE2 identity/ROOT/OPEN check; no clean-stop receipt is fabricated.
	$cluster->{running} = 0;
	$cluster->{normal_stop_proven} = 0;
	return 1;
}

sub clean_after_failure
{
	my ($cluster) = @_;
	return unless defined $cluster;
	for my $node ($cluster->nodes)
	{
		$node->_update_pid(-1);
		$node->stop('immediate', fail_ok => 1) if defined $node->{_pid};
		$node->_update_pid(-1);
		BAIL_OUT('failed cohort cleanup; refusing to reformat shared votes') if defined $node->{_pid};
	}
	$cluster->{running} = 0;
}

sub with_cohort
{
	my ($name, $entry_index, $body) = @_;
	subtest $name => sub {
		local $ENV{PGRAC_PRE2_ENTRY_FILE} = $entries->[$entry_index];
		my $cluster;
		my $completed = eval {
			$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster($name,
				nodes => 4, blackbox => 1, required_operations => [], extra_conf => \@CONF);
			$cluster->start_cluster;
			pass('four members reach native OPEN with matching ROOT and writer identities');
			$body->($cluster);
			1;
		};
		my $error = $@;
		if (!$completed)
		{
			fail('real crash/recovery path completes');
			diag($error);
			clean_after_failure($cluster);
		}
	};
}

sub schema_environment
{
	my ($cluster, $stage) = @_;
	# Diagnostic failures must not replace the original schema-write error.
	my $collected = eval {
		my @paths = ((map { $_->data_dir } $cluster->nodes),
			$cluster->shared_root, $cluster->wal_root);
		my %evidence = (stage => $stage, monotonic => now(), paths => \@paths);
		my $settings_ok = eval {
			my ($rc, $stdout, $stderr) = $cluster->node0->psql('postgres', q{
				SELECT name, setting, unit, source, sourcefile, sourceline
				FROM pg_settings
				WHERE name LIKE 'cluster.%undo%'
				OR name IN ('data_directory', 'cluster.shared_data_dir',
					'cluster.shared_storage_backend') ORDER BY name;
			}, timeout => 5, on_error_die => 0);
			$evidence{settings} = {rc => $rc, stdout => $stdout, stderr => $stderr};
			1;
		};
		$evidence{settings_error} = "$@" unless $settings_ok;
		for my $mode ('-Pk', '-Pi')
		{
			my ($stdout, $stderr) = ('', '');
			my $df_ok = eval {
				my $ok = run(['df', $mode, @paths], '>', \$stdout, '2>', \$stderr,
					IPC::Run::timeout(5));
				$evidence{df}{$mode} = {success => $ok ? 1 : 0,
					wait_status => $?, stdout => $stdout, stderr => $stderr};
				1;
			};
			$evidence{df}{$mode} = {error => "$@", stdout => $stdout, stderr => $stderr}
			  unless $df_ok;
		}
		diag('E1_SCHEMA_ENVIRONMENT ' . $json->encode(\%evidence));
		1;
	};
	diag("E1_SCHEMA_ENVIRONMENT capture failed: $@") unless $collected;
}

sub seed_committed
{
	my ($cluster) = @_;
	schema_environment($cluster, 'before_create');
	my $created = eval {
		$cluster->node0->safe_psql('postgres',
			'CREATE TABLE e1_reconcile(id integer PRIMARY KEY, payload text NOT NULL)', timeout => 20);
		1;
	};
	my $error = $@;
	if (!$created)
	{
		schema_environment($cluster, 'create_failed');
		die $error;
	}
	my @nodes = $cluster->nodes;
	for my $i (0 .. $#nodes)
	{
		my ($lo, $hi) = ($i * 16 + 1, ($i + 1) * 16);
		$nodes[$i]->safe_psql('postgres', qq{
			BEGIN; INSERT INTO e1_reconcile SELECT g, 'committed-' || g
			FROM generate_series($lo, $hi) AS g; COMMIT;}, timeout => 20);
	}
	pass('all four members acknowledge committed writes');
}

with_cohort('e1_pre2_crash_462', 0, sub {
	my ($cluster) = @_;
	seed_committed($cluster);
	my @sessions;
	my @nodes = $cluster->nodes;
	for my $i (0 .. $#nodes)
	{
		my $session = $nodes[$i]->background_psql('postgres', on_error_die => 1, timeout => 20);
		$session->query_safe('BEGIN');
		my $id = 1001 + $i;
		$session->query_safe("INSERT INTO e1_reconcile VALUES ($id, 'uncommitted-$id')");
		push @sessions, $session;
	}
	pass('all four members hold an acknowledged but uncommitted INSERT');
	# Make the loser effects eligible for real on-disk recovery; disappearing
	# backend buffers alone must not satisfy the reconciliation assertions.
	$_->safe_psql('postgres', 'CHECKPOINT', timeout => 30) for @nodes;
	pass('all four members checkpoint while loser transactions remain open');
	my $data_identity = $cluster->_data_identity;
	my @old_writers = map { +{%$_} } @{$cluster->{writers}};
	my $old_generation = $cluster->{selected_root}{generation};
	ok(crash_all($cluster), 'SIGKILL all four instances and observe owned process exit');
	# Finish dead client pipes before creating any new postmaster; errors are
	# expected here and never count as a recovery result.
	for my $session (@sessions) { eval { $session->quit; }; }
	@sessions = ();
	my $offsets = log_offsets($cluster);
	is_deeply($cluster->_data_identity, $data_identity, 'cold start retains the same DATA and WAL objects');
	$cluster->start_cluster;
	pass('cold native shared_start returns all four members to OPEN');
	cmp_ok($cluster->{selected_root}{generation}, '>', $old_generation, 'cold recovery advances the selected ROOT');
	for my $i (0 .. $#nodes)
	{
		my $new = $cluster->{writers}[$i];
		isnt($new->{boot}, $old_writers[$i]{boot}, "node$i publishes a new writer boot");
		cmp_ok($new->{wal_generation}, '>', $old_writers[$i]{wal_generation},
			"node$i advances its WAL generation");
		for my $field ('thread', 'incarnation', 'wal_generation', 'boot')
		{
			is($new->{predecessor}{$field}, $old_writers[$i]{$field},
				"node$i consumes the crashed writer's exact $field");
		}
		my $rows = $nodes[$i]->safe_psql('postgres',
			'SELECT id, payload FROM e1_reconcile ORDER BY id', timeout => 20);
		is($rows, join("\n", map { "$_|committed-$_" } 1 .. 64),
			"node$i sees every committed row and no uncommitted row");
		unlike(log_since($nodes[$i], $offsets->[$i]), qr/PANIC:/,
			"node$i emits no PANIC during cold recovery and reconciliation");
	}
	$cluster->stop_cluster;
	pass('all four recovered writers close through native shared_stop');
});

for my $case ('identity', 'missing_control')
{
	with_cohort("e1_pre2_${case}_462", $case eq 'identity' ? 1 : 2, sub {
		my ($cluster) = @_;
		seed_committed($cluster);
		ok(crash_all($cluster), 'negative input is applied only after a proven live cohort crashes');
		if ($case eq 'identity')
		{
			# A valid-range but wrong node identity, not a rejected GUC value.
			$cluster->node1->append_conf('postgresql.conf', 'cluster.node_id = 0');
		}
		else
		{
			my $control = $cluster->shared_data_root . '/global/pg_control';
			-f $control or die "missing-control fixture never had $control";
			unlink($control) or die "cannot remove shared control record: $!";
		}
		my $offsets = log_offsets($cluster);
		my $accepted = eval { $cluster->start_cluster; 1; };
		my $error = $@;
		ok(!$accepted, "$case cold startup is refused");
		diag($error) if $error;
		my @nodes = $cluster->nodes;
		my $all_logs = join("\n", map { log_since($nodes[$_], $offsets->[$_]) } 0 .. $#nodes);
		like($all_logs, qr/FATAL:/, "$case has a native startup rejection, not just a harness exception");
		for my $i (0 .. $#nodes)
		{
			unlike(log_since($nodes[$i], $offsets->[$i]), qr/OPEN_APPLIED/,
				"$case node$i does not publish semantic OPEN");
			# An observer exception is not a negative result: if SQL still
			# answers, require a real unavailable/zero admission observation.
			my ($out, $err) = ('', '');
			my $rc = $nodes[$i]->psql('postgres',
				q{SELECT value FROM cluster_dump_state() WHERE category='lifecycle' AND key='native_writer'},
				stdout => \$out, stderr => \$err, timeout => 5);
			if ($rc == 0)
			{
				my $runtime = $out =~ /\Aunavailable\s*\z/ ? undef : $json->decode($out);
				ok(!defined($runtime) || !($runtime->{resource_x_formation} // 0)
					|| !($runtime->{r4_generation} // 0), "$case node$i has no admitted native writer");
			}
			else
			{
				is($rc, 2, "$case node$i refuses a SQL connection (not a SQL execution error)");
				diag($err) if $rc != 2;
			}
			unlike(log_since($nodes[$i], $offsets->[$i]), qr/PANIC:/,
				"$case node$i rejects without PANIC");
		}
		clean_after_failure($cluster);
	});
}

done_testing();
