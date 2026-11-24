#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Cold recovery at acknowledged SQL boundaries, with old readers before crash.
# Internal mark/cursor cuts and exact source-segment reuse need separate tests.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use IPC::Run qw(run);
use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my $json = JSON::PP->new->canonical;
my $base = '0:base,1:base,2:base,3:base';
my $winners = $base . ',100:parent,102:parent,400:after,402:after,602:handler';
my $rows_sql = q{SELECT coalesce(string_agg(id::text || ':' || v, ',' ORDER BY id),'')
	FROM p013_recovery_data};

sub session
{
	my ($node) = @_;
	my $s = $node->background_psql('postgres', timeout => 60);
	$s->set_query_timer_restart;
	return $s;
}

sub all_rows
{
	my ($cluster, $expected, $label) = @_;
	for my $node ($cluster->nodes)
	{
		is($node->safe_psql('postgres', $rows_sql, timeout => 30), $expected,
			$node->name . " $label") or die "$label row mismatch";
	}
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
		$table{$pid} = {parent => $parent, state => $state,
			birth => join(' ', @start[0 .. 4])};
	}
	return \%table;
}

# Same owned-process crash shape as 462; never signal arbitrary server PIDs.
sub crash_all
{
	my ($cluster) = @_;
	my $table = process_table();
	my (%owned, @roots);
	for my $node ($cluster->nodes)
	{
		$node->_update_pid(-1);
		my $pid = $node->{_pid};
		die 'crash requires all four live postmasters'
			unless defined($pid) && exists($table->{$pid}) && $table->{$pid}{state} !~ /^Z/;
		push @roots, $pid;
		$owned{$pid} = $table->{$pid}{birth};
	}
	while (1)
	{
		my @children = grep { !exists($owned{$_}) && exists($owned{$table->{$_}{parent}}) }
			keys %$table;
		last unless @children;
		$owned{$_} = $table->{$_}{birth} for @children;
	}
	diag('P013_CRASH_OWNED ' . $json->encode(\%owned));
	my $current = process_table();
	for my $pid (@roots)
	{
		die "postmaster $pid changed before crash"
			unless exists($current->{$pid}) && $current->{$pid}{birth} eq $owned{$pid};
	}
	for my $pid (@roots)
	{
		kill(9, $pid) == 1 or die "SIGKILL postmaster $pid failed: $!";
	}
	$current = process_table();
	for my $pid (keys %owned)
	{
		next unless exists($current->{$pid}) && $current->{$pid}{birth} eq $owned{$pid};
		kill(9, $pid) unless $current->{$pid}{state} =~ /^Z/;
	}
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 30;
	while (1)
	{
		$current = process_table();
		my @live = grep { exists($current->{$_}) && $current->{$_}{birth} eq $owned{$_}
			&& $current->{$_}{state} !~ /^Z/ } keys %owned;
		last unless @live;
		die 'crash-owned processes survived: ' . join(',', @live)
			if clock_gettime(CLOCK_MONOTONIC) >= $deadline;
		sleep(0.05);
	}
	$_->_update_pid(-1) for $cluster->nodes;
	# Only bookkeeping after observed exit; this grants no clean-stop receipt.
	$cluster->{running} = 0;
	$cluster->{normal_stop_proven} = 0;
}

sub undo_observation
{
	my ($node, $stage) = @_;
	my $state = $node->safe_psql('postgres', q{
		SELECT key || '=' || value FROM cluster_dump_state()
		WHERE category='undo' AND key IN ('retention_horizon_scn',
			'tt_slot_retain_skip_count','retention_recycle_count',
			'tt_retention_rollover_count','segment_retain_skip_count') ORDER BY key
	}, timeout => 30);
	# Global counters are diagnostic, not a specific source-UBA release proof.
	diag($json->encode({node => $node->name, stage => $stage, undo => $state}));
}

my ($cluster, @readers, @parents);
my $completed = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_recovery_retention',
		nodes => 4, blackbox => 1, extra_conf => [
			'autovacuum = off', 'restart_after_crash = off',
			'log_error_verbosity = verbose', 'shared_buffers = 16MB',
			'cluster.merged_recovery = on', 'cluster.undo_retention_horizon_enabled = on',
			'cluster.cf_enqueue_timeout_ms = 30000',
		]);
	$cluster->start_cluster;
	pass('four members reach native OPEN with actual ROOT and writer identities');
	my @nodes = $cluster->nodes;
	$nodes[0]->safe_psql('postgres', q{
		CREATE TABLE p013_recovery_data(id int PRIMARY KEY,v text NOT NULL);
		INSERT INTO p013_recovery_data SELECT g,'base' FROM generate_series(0,3) g;
		CREATE TABLE p013_recovery_churn(id int PRIMARY KEY,v int NOT NULL);
		INSERT INTO p013_recovery_churn SELECT g,0 FROM generate_series(0,3) g;
	}, timeout => 30);
	all_rows($cluster, $base, 'sees the committed seed before any child');
	for my $i (0 .. 3)
	{
		my $s = session($nodes[$i]);
		push @readers, $s;
		$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
		is($s->query_safe($rows_sql), $base, "node$i pins the original snapshot")
			or die 'old snapshot not materialized';
		is($nodes[$i]->safe_psql('postgres', 'SHOW cluster.undo_retention_horizon_enabled'),
			'on', "node$i keeps the real retention gate enabled") or die 'retention disabled';
		undo_observation($nodes[$i], 'before_writes');
	}

	my @top_xids;
	for my $i (0 .. 3)
	{
		my $s = session($nodes[$i]);
		push @parents, $s;
		my ($parent, $child, $nested, $after) = (100 + $i, 200 + $i, 300 + $i, 400 + $i);
		$s->query_safe(qq{
			BEGIN; INSERT INTO p013_recovery_data VALUES ($parent,'parent');
			SAVEPOINT mutate;
			UPDATE p013_recovery_data SET v='discard' WHERE id=$i;
			DELETE FROM p013_recovery_data WHERE id=$parent;
			INSERT INTO p013_recovery_data VALUES ($child,'discard');
		});
		my $identity = $s->query_safe(qq{
			SELECT pg_current_xact_id_if_assigned()::text || '|' || xmin::text || '|' ||
			(xmin::text::numeric <> pg_current_xact_id_if_assigned()::text::numeric % 4294967296)::text
			FROM p013_recovery_data WHERE id=$child
		});
		like($identity, qr/^\d+\|\d+\|true$/, "node$i really installed a distinct child")
			or die 'child identity witness missing';
		($top_xids[$i]) = split /\|/, $identity;
		diag("node$i top|child|distinct=$identity");
		$s->query_safe(qq{
			SAVEPOINT nested; INSERT INTO p013_recovery_data VALUES ($nested,'discard');
			RELEASE nested; ROLLBACK TO mutate; ROLLBACK TO mutate;
		});
		is($s->query_safe("SELECT v FROM p013_recovery_data WHERE id=$parent"),
			'parent', "node$i rollback restores the parent's deleted row") or die 'parent row lost';
		is($s->query_safe("SELECT v FROM p013_recovery_data WHERE id=$i"),
			'base', "node$i rollback restores the updated base row") or die 'base row lost';
		is($s->query_safe("SELECT count(*) FROM p013_recovery_data WHERE id IN ($child,$nested)"),
			'0', "node$i rollback removes child and RELEASEd grandchild") or die 'child leaked';
		$s->query_safe("INSERT INTO p013_recovery_data VALUES ($after,'after'); RELEASE mutate;");
		if ($i == 2)
		{
			$s->query_safe(q{
				DO $$ DECLARE progress int := 0; message text; BEGIN
					UPDATE p013_recovery_data SET v='exception' WHERE id=2;
					INSERT INTO p013_recovery_data VALUES (702,'exception');
					progress := 1; RAISE EXCEPTION 'p013 recovery child';
				EXCEPTION WHEN raise_exception THEN
					GET STACKED DIAGNOSTICS message=MESSAGE_TEXT;
					IF progress <> 1 OR message <> 'p013 recovery child' THEN
						RAISE EXCEPTION 'p013 unexpected earlier error';
					END IF;
					INSERT INTO p013_recovery_data VALUES (602,'handler');
				END $$;
			});
		}
		if ($i % 2 == 0)
		{
			$s->query_safe('COMMIT');
			$s->quit;
			$parents[$i] = undef;
			pass("node$i acknowledges surviving parent COMMIT after child rollback");
		}
	}
	# One loser has a newly installed child; the other has completed rollback
	# but still owns an uncommitted parent. Neither may become an independent top.
	$parents[1]->query_safe(q{
		SAVEPOINT live_child;
		UPDATE p013_recovery_data SET v='live-child' WHERE id=1;
		INSERT INTO p013_recovery_data VALUES (501,'live-child');
	});
	my $live = $parents[1]->query_safe(q{
		SELECT pg_current_xact_id_if_assigned()::text || '|' || xmin::text || '|' ||
		(xmin::text::numeric <> pg_current_xact_id_if_assigned()::text::numeric % 4294967296)::text
		FROM p013_recovery_data WHERE id=501
	});
	like($live, qr/^\Q$top_xids[1]\E\|\d+\|true$/,
		'node1 holds a real live child of the original loser before crash') or die 'live child missing';
	diag("node1 live top|child|distinct=$live");
	all_rows($cluster, $winners, 'sees acknowledged winners and no active loser');

	# Bounded functional history churn, not a performance measurement. Each
	# update is a separate top transaction; no configured retention bypass.
	for my $i (0 .. 3)
	{
		for (1 .. 12)
		{
			$nodes[$i]->safe_psql('postgres',
				"UPDATE p013_recovery_churn SET v=v+1 WHERE id=$i", timeout => 30);
		}
		is($nodes[$i]->safe_psql('postgres', "SELECT v FROM p013_recovery_churn WHERE id=$i"),
			'12', "node$i completed twelve distinct top transactions") or die 'churn incomplete';
		undo_observation($nodes[$i], 'readers_still_held');
		for my $j (0 .. 3)
		{
			is($readers[$j]->query_safe($rows_sql), $base,
				"node$j old snapshot survives node$i history churn") or die 'protected history lost';
		}
	}
	$_->safe_psql('postgres', 'CHECKPOINT', timeout => 60) for @nodes;
	for my $i (0 .. 3)
	{
		is($readers[$i]->query_safe($rows_sql), $base,
			"node$i old snapshot still reads protected versions after checkpoint")
			or die 'checkpoint lost protected history';
	}
	for my $i (1, 3)
	{
		my $id = 100 + $i;
		is($parents[$i]->query_safe(qq{
			SELECT pg_current_xact_id_if_assigned()::text || '|' || v
			FROM p013_recovery_data WHERE id=$id
		}), "$top_xids[$i]|parent", "node$i original loser and parent row survive checkpoint")
			or die 'loser not held at crash';
	}
	is($parents[1]->query_safe('SELECT v FROM p013_recovery_data WHERE id=501'),
		'live-child', 'node1 live child remains installed after checkpoint') or die 'live child not held';
	my $data_identity = $cluster->_data_identity;
	my @writers = map { +{%$_} } @{$cluster->{writers}};
	my $generation = $cluster->{selected_root}{generation};
	my @offsets = map { -s $_->logfile } @nodes;
	for my $i (0 .. 3)
	{
		my $lsn = $nodes[$i]->safe_psql('postgres',
			'SELECT pg_current_wal_insert_lsn(),pg_current_wal_flush_lsn()', timeout => 30);
		diag('P013_PRECRASH ' . $json->encode({node => $i, writer => $writers[$i],
			data_identity => $data_identity, root_generation => $generation, sql_wal_lsn => $lsn}));
	}
	crash_all($cluster);
	pass('all four captured postmasters and their owned descendants have exited');
	# Dead-client pipe errors are expected, never a recovery verdict.
	for my $s (@parents, @readers) { eval { $s->quit; } if defined($s); }
	@parents = @readers = ();
	is($cluster->_data_identity, $data_identity, 'cold recovery uses the original DATA and WAL objects')
		or die 'DATA was replaced';
	$cluster->start_cluster;
	cmp_ok($cluster->{selected_root}{generation}, '>', $generation, 'cold recovery advances ROOT')
		or die 'ROOT did not advance';
	for my $i (0 .. 3)
	{
		my $new = $cluster->{writers}[$i];
		isnt($new->{boot}, $writers[$i]{boot}, "node$i has a new writer boot");
		cmp_ok($new->{wal_generation}, '>', $writers[$i]{wal_generation},
			"node$i advances its WAL generation");
		for my $field ('thread', 'incarnation', 'wal_generation', 'boot')
		{
			is($new->{predecessor}{$field}, $writers[$i]{$field},
				"node$i recovery consumes the exact original $field");
		}
		unlike(slurp_file($nodes[$i]->logfile, $offsets[$i]), qr/PANIC:|TRAP:/,
			"node$i has no PANIC or assertion through cold recovery");
	}
	all_rows($cluster, $winners, 'cold recovery keeps winners, discards loser tops and never revives rolled-back children');
	for my $node (@nodes)
	{
		is($node->safe_psql('postgres',
			'SELECT string_agg(id::text || chr(58) || v, chr(44) ORDER BY id) FROM p013_recovery_churn',
			timeout => 30), '0:12,1:12,2:12,3:12', $node->name . ' retains every acknowledged churn write');
		$node->safe_psql('postgres', 'CHECKPOINT', timeout => 60);
	}
	$cluster->stop_cluster;
	$cluster->restart_cluster;
	is($cluster->_data_identity, $data_identity, 'normal restart still uses the same recovered disks');
	all_rows($cluster, $winners, 'checkpoint and normal restart preserve recovered operation dispositions');
	$cluster->stop_cluster;
	for my $i (0 .. 3)
	{
		unlike(slurp_file($nodes[$i]->logfile, $offsets[$i]), qr/PANIC:|TRAP:/,
			"node$i has no PANIC or assertion through recovered normal stop and restart");
	}
	pass('recovered cohort finishes through proven native shared_stop');
	1;
};
if (!$completed)
{
	my $error = $@ || 'recovery fixture did not complete';
	# The standard node END cleanup stops only owned nodes and keeps failed
	# DATA. Never start a fresh cohort or report an earlier refusal as success.
	fail('real savepoint crash/retention path completes');
	diag($error);
	diag('Preserve tmp_check and PRE2 DATA/shared/WAL before another make check');
}
done_testing();
