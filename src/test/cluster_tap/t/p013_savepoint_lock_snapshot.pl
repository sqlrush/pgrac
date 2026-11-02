#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Native row lock scopes and old read views across partial rollback.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

sub session
{
	my ($node) = @_;
	my $s = $node->background_psql('postgres', timeout => 180);
	$s->set_query_timer_restart;
	return $s;
}

sub rows_sql
{
	my ($table) = @_;
	return "SELECT coalesce(string_agg(id::text || ':' || v, chr(10) ORDER BY id), '') FROM $table";
}

sub all_rows
{
	my ($cluster, $table, $expected, $label) = @_;
	for my $node ($cluster->nodes)
	{
		is($node->safe_psql('postgres', rows_sql($table), timeout => 30),
			$expected, $node->name . " $label");
	}
}

sub await_tx_wait
{
	my ($node, $pid, $label) = @_;
	$pid =~ /^\d+$/ or BAIL_OUT("invalid waiter pid: $pid");
	my $deadline = time() + 30;
	my $seen = '';
	do
	{
		$seen = $node->safe_psql('postgres', qq{
			SELECT coalesce((SELECT state || '|' || coalesce(wait_event,'')
			FROM pg_stat_activity WHERE pid=$pid), 'missing')
		}, timeout => 30);
		if ($seen eq 'active|GesTxEnqueueWait')
		{
			pass($label);
			diag("$label node=" . $node->name . " pid=$pid state=$seen");
			return;
		}
		usleep(100_000);
	} while (time() < $deadline);
	fail($label);
	BAIL_OUT("expected exact TX waiter pid=$pid, last state=$seen");
}

sub start_waiter
{
	my ($s, $sql) = @_;
	$s->query_until(qr/P013_WAIT_SENT\r?\n/, "\\echo P013_WAIT_SENT\n$sql;\n");
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_lock_snapshot',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
$nodes[0]->safe_psql('postgres', q{
	CREATE TABLE p013_scope_lock (id int PRIMARY KEY, v text NOT NULL);
	INSERT INTO p013_scope_lock VALUES (1,'base'),(2,'base'),(3,'base');
	CREATE TABLE p013_scope_snapshot (id int PRIMARY KEY, v text NOT NULL);
	INSERT INTO p013_scope_snapshot VALUES (1,'base'),(2,'base');
}, timeout => 30);

# A read-only child has no assigned top xid until it first locks a row.
my $first = session($nodes[3]);
$first->query_safe('BEGIN; SAVEPOINT first_lock');
is($first->query_safe('SELECT pg_current_xact_id_if_assigned() IS NULL'), 't',
	'initial read-only child has no assigned xid');
is($first->query_safe('SELECT id FROM p013_scope_lock WHERE id=3 FOR UPDATE'), '3',
	'first child row lock succeeds');
my $identity = $first->query_safe(q{
	SELECT pg_current_xact_id_if_assigned()::text, xmax::text,
		xmax::text::numeric >= 3 AND
		xmax::text::numeric <> (pg_current_xact_id_if_assigned()::text::numeric % 4294967296)
	FROM p013_scope_lock WHERE id=3
});
like($identity, qr/^\d+\|\d+\|t$/, 'first lock records a real child distinct from its top');
diag("first child lock top|raw xmax|distinct=$identity");
my $first_top = $first->query_safe('SELECT pg_current_xact_id_if_assigned()::text');
$first->query_safe('ROLLBACK TO first_lock');
is($first->query_safe('SELECT pg_current_xact_id_if_assigned()::text'), $first_top,
	'rollback-to keeps the original top transaction');
is($nodes[2]->safe_psql('postgres', q{
	UPDATE p013_scope_lock SET v='after-child-lock' WHERE id=3 RETURNING v
}, timeout => 30), 'after-child-lock', 'fresh writer acquires the discarded first child lock');
$first->query_safe('COMMIT');
$first->quit;

my $holder = session($nodes[0]);
$holder->query_safe("BEGIN; UPDATE p013_scope_lock SET v='parent-keep' WHERE id=1;
	SAVEPOINT child_lock; UPDATE p013_scope_lock SET v='child-discard' WHERE id=2;");
diag('holder top=' . $holder->query_safe('SELECT pg_current_xact_id_if_assigned()::text'));
my $old = session($nodes[1]);
$old->query_safe('BEGIN');
my $old_pid = $old->query_safe('SELECT pg_backend_pid()');
start_waiter($old, "UPDATE p013_scope_lock SET v=v || ':old-waiter' WHERE id=2 RETURNING v");
await_tx_wait($nodes[1], $old_pid, 'old row-2 request enters exact TX wait before rollback');
$holder->query_safe('ROLLBACK TO child_lock');
await_tx_wait($nodes[1], $old_pid, 'old request remains subscribed to the active top after rollback');

my $parent_waiter = session($nodes[3]);
$parent_waiter->query_safe('BEGIN');
my $parent_pid = $parent_waiter->query_safe('SELECT pg_backend_pid()');
start_waiter($parent_waiter, "UPDATE p013_scope_lock SET v=v || ':parent-waiter' WHERE id=1 RETURNING v");
await_tx_wait($nodes[3], $parent_pid, 'surviving parent row lock still blocks a new requester');
is($nodes[2]->safe_psql('postgres', q{
	UPDATE p013_scope_lock SET v='new-visitor' WHERE id=2 RETURNING v
}, timeout => 30), 'new-visitor', 'fresh requester can use the released child row');
await_tx_wait($nodes[1], $old_pid, 'old top subscription survives a third transaction committing');
await_tx_wait($nodes[3], $parent_pid, 'parent lock survives the third transaction committing');
$holder->query_safe('COMMIT');
$holder->quit;
is($old->query_safe("SELECT 'old-drained'"), "new-visitor:old-waiter\nold-drained",
	'old waiter rechecks and updates the third writer version after top commit');
$old->query_safe('COMMIT');
$old->quit;
is($parent_waiter->query_safe("SELECT 'parent-drained'"), "parent-keep:parent-waiter\nparent-drained",
	'parent waiter resumes only after the original top commits');
$parent_waiter->query_safe('COMMIT');
$parent_waiter->quit;
my $lock_expected = "1:parent-keep:parent-waiter\n2:new-visitor:old-waiter\n3:after-child-lock";
all_rows($cluster, 'p013_scope_lock', $lock_expected, 'exact surviving lock/update results');

my (@readers, @snapshots);
for my $node (@nodes)
{
	my $reader = session($node);
	$reader->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
	push @snapshots, $reader->query_safe('SELECT pg_current_snapshot()::text');
	is($reader->query_safe(rows_sql('p013_scope_snapshot')), "1:base\n2:base",
		$node->name . ' establishes the original read view');
	diag($node->name . ' original snapshot=' . $snapshots[-1]);
	push @readers, $reader;
}
my $writer = session($nodes[0]);
$writer->query_safe("BEGIN; UPDATE p013_scope_snapshot SET v='parent-keep' WHERE id=1;
	SAVEPOINT snapshot_child; UPDATE p013_scope_snapshot SET v='discard' WHERE id=1;
	DELETE FROM p013_scope_snapshot WHERE id=2;");
for my $i (0 .. 3)
{
	is($readers[$i]->query_safe(rows_sql('p013_scope_snapshot')), "1:base\n2:base",
		"node$i old view excludes both active parent and child effects");
}
$writer->query_safe("ROLLBACK TO snapshot_child; INSERT INTO p013_scope_snapshot VALUES (3,'released-keep');
	RELEASE snapshot_child; COMMIT;");
$writer->quit;
all_rows($cluster, 'p013_scope_snapshot', "1:parent-keep\n2:base\n3:released-keep",
	'fresh views see surviving parent and released child only');
for my $i (0 .. 3)
{
	is($readers[$i]->query_safe(rows_sql('p013_scope_snapshot')), "1:base\n2:base",
		"node$i old view remains unchanged after top commit");
}
# A finite history chain is a correctness case, with no throughput assertion.
for my $step (1 .. 16)
{
	$nodes[($step - 1) % 4]->safe_psql('postgres', qq{
		BEGIN; UPDATE p013_scope_snapshot SET v='history-$step' WHERE id=1;
		SAVEPOINT history_child; UPDATE p013_scope_snapshot SET v='discard-$step' WHERE id=1;
		ROLLBACK TO history_child; RELEASE history_child; COMMIT;
	}, timeout => 30);
}
$nodes[0]->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
my $snapshot_expected = "1:history-16\n2:base\n3:released-keep";
all_rows($cluster, 'p013_scope_snapshot', $snapshot_expected, 'fresh views after cross-origin history');
for my $i (0 .. 3)
{
	is($readers[$i]->query_safe('SELECT pg_current_snapshot()::text'), $snapshots[$i],
		"node$i retains its original native snapshot identity");
	is($readers[$i]->query_safe(rows_sql('p013_scope_snapshot')), "1:base\n2:base",
		"node$i old view survives history and checkpoint without discarded versions");
	$readers[$i]->query_safe('COMMIT');
	$readers[$i]->quit;
}
$cluster->stop_cluster;
$cluster->restart_cluster;
all_rows($cluster, 'p013_scope_lock', $lock_expected, 'lock results after same-DATA restart');
all_rows($cluster, 'p013_scope_snapshot', $snapshot_expected, 'snapshot results after same-DATA restart');
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
		$node->name . ' has no assertion, PANIC or signal termination');
}
done_testing();
