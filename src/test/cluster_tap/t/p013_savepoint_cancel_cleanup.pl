#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Caller cancellation must clean the child scope without ending its parent.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

sub rows_sql
{
	return q{SELECT coalesce(string_agg(id::text || ':' || v,chr(10) ORDER BY id),'') FROM p013_cancel_rows};
}

sub all_rows
{
	my ($nodes, $expected, $label) = @_;
	my $want = join("\n", map { "$_:$expected->{$_}" } sort { $a <=> $b } keys %$expected);
	for my $node (@$nodes)
	{
		is($node->safe_psql('postgres', rows_sql(), timeout => 30), $want,
			$node->name . " $label exact surviving keys and values");
	}
}

sub sleep_filter
{
	my ($pid, $start, $prefix) = @_;
	$pid =~ /^[1-9][0-9]*$/ && $start =~ /^[0-9]+(?:\.[0-9]+)?$/
		or BAIL_OUT('invalid native backend identity');
	return "pid=$pid AND extract(epoch FROM backend_start)=${start}::numeric AND query LIKE '$prefix%'";
}

sub cancel_sleep
{
	my ($node, $pid, $start, $prefix, $label) = @_;
	my $filter = sleep_filter($pid, $start, $prefix);
	my $deadline = time() + 30;
	my $last = '';
	my $seen = 0;
	do
	{
		$last = $node->safe_psql('postgres', "SELECT state || '|' || coalesce(wait_event_type,'') || '|' ||
			coalesce(wait_event,'') || '|' || (backend_xmin IS NOT NULL)::text
			FROM pg_stat_activity WHERE $filter", timeout => 30);
		if ($last eq 'active|Timeout|PgSleep|true')
		{
			$seen = 1;
			last;
		}
		usleep(100_000);
	} while (time() < $deadline);
	ok($seen, "$label exact backend is sleeping with a real snapshot")
		or BAIL_OUT("$label did not reach the cancel point: pid=$pid start=$start last=$last");
	diag("$label cancel target node=" . $node->name . " pid=$pid start=$start state=$last");
	is($node->safe_psql('postgres', "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE $filter
		AND state='active' AND wait_event_type='Timeout' AND wait_event='PgSleep'", timeout => 30),
		't', "$label caller cancels only the same sleeping backend")
		or BAIL_OUT("$label cancel target changed");
}

sub portal_names
{
	return q{SELECT string_agg(name,',' ORDER BY name) FROM pg_cursors
		WHERE name IN ('parent_cursor','child_cursor','pl_child_cursor')};
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_cancel_cleanup',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
$nodes[0]->safe_psql('postgres', q{
	CREATE TABLE p013_cancel_rows(id int PRIMARY KEY,v text NOT NULL);
	INSERT INTO p013_cancel_rows VALUES (1,'base-1'),(2,'base-2'),(3,'base-3'),(4,'base-4');
	CREATE TABLE p013_cancel_cursor(id int PRIMARY KEY);
	INSERT INTO p013_cancel_cursor VALUES (11),(22),(33);
}, timeout => 30);
my %expected = (1 => 'base-1', 2 => 'base-2', 3 => 'base-3', 4 => 'base-4');
for my $i (0 .. 3)
{
	my ($id, $child_id, $after_id, $handler_id) = ($i+1, 1000+$i, 200+$i, 400+$i);
	my $node = $nodes[$i];
	my $s = $node->background_psql('postgres', on_error_stop => 0, timeout => 180,
		extra_params => [ '-v', 'VERBOSITY=verbose' ]);
	$s->set_query_timer_restart;
	my $identity = $s->query_safe(q{SELECT pid::text || '|' || extract(epoch FROM backend_start)::text
		FROM pg_stat_activity WHERE pid=pg_backend_pid()});
	my ($pid, $start) = split /\|/, $identity;
	$s->query_safe("BEGIN; UPDATE p013_cancel_rows SET v='parent-$i' WHERE id=$id;
		DECLARE parent_cursor NO SCROLL CURSOR FOR SELECT id FROM p013_cancel_cursor ORDER BY id;");
	is($s->query_safe('FETCH NEXT FROM parent_cursor'), '11', "origin$i parent cursor starts normally");
	my $top = $s->query_safe('SELECT pg_current_xact_id_if_assigned()::text');
	like($top, qr/^[1-9][0-9]*$/, "origin$i parent has a real FullXid");
	$s->query_safe("SAVEPOINT cancel_point;
		UPDATE p013_cancel_rows SET v='discard-$i' WHERE id=$id;
		INSERT INTO p013_cancel_rows VALUES ($child_id,'child-$i');
		DECLARE child_cursor NO SCROLL CURSOR FOR SELECT id FROM p013_cancel_cursor ORDER BY id;");
	is($s->query_safe('FETCH NEXT FROM child_cursor'), '11', "origin$i child cursor was actually used");
	like($s->query_safe("SELECT xmin::text || '|' ||
		(xmin::text::numeric <> pg_current_xact_id_if_assigned()::text::numeric % 4294967296)::text
		FROM p013_cancel_rows WHERE id=$child_id"), qr/^\d+\|true$/,
		"origin$i really wrote under a distinct child xid");
	is($s->query_safe("SELECT string_agg(id::text || ':' || v,chr(10) ORDER BY id)
		FROM p013_cancel_rows WHERE id IN ($id,$child_id)"), "$id:discard-$i\n$child_id:child-$i",
		"origin$i child effects exist before cancellation");
	$s->query_until(qr/P013_SLEEP_SENT\r?\n/, "\\echo P013_SLEEP_SENT\nSELECT pg_sleep(120);\n");
	cancel_sleep($node, $pid, $start, 'SELECT pg_sleep(120)', "origin$i explicit savepoint");
	my ($out, $error) = $s->query("\\echo CANCEL_STATE=:SQLSTATE\n");
	my $err = $s->{stderr};
	diag("origin$i original cancel stdout=$out stderr=$err");
	is($error, 1, "origin$i original cancellation reports an error");
	like($out, qr/^CANCEL_STATE=57014$/m, "origin$i reports the exact cancellation state");
	like($err, qr/ERROR:\s+57014:.*canceling statement due to user request/,
		"origin$i retains the original caller cancellation diagnostic");
	my @errors = $err =~ /^.*ERROR:.*$/mg;
	is(scalar(@errors), 1, "origin$i has exactly the intended error");
	# Same expected-error buffer reset as original 453; retain the bytes above.
	$s->{stderr} = '';
	$s->query_safe("\\set ON_ERROR_STOP on\nROLLBACK TO cancel_point; ROLLBACK TO cancel_point;");
	is($s->query_safe('SELECT pg_current_xact_id_if_assigned()::text'), $top,
		"origin$i repeated cleanup leaves the same parent active");
	is($s->query_safe("SELECT string_agg(id::text || ':' || v,chr(10) ORDER BY id)
		FROM p013_cancel_rows WHERE id IN ($id,$child_id)"), "$id:parent-$i",
		"origin$i rollback restores the parent and removes child insert");
	is($s->query_safe(portal_names()), 'parent_cursor', "origin$i drops only the failed child portal");
	is($s->query_safe('FETCH NEXT FROM parent_cursor'), '22', "origin$i parent cursor keeps its position");
	$s->query_safe("INSERT INTO p013_cancel_rows VALUES ($after_id,'after-$i'); RELEASE cancel_point; COMMIT;");
	is($s->query_safe(q{SELECT count(*) FROM pg_cursors WHERE name='parent_cursor'}), '0',
		"origin$i parent commit closes the surviving cursor");
	$expected{$id} = "parent-$i";
	$expected{$after_id} = "after-$i";
	all_rows(\@nodes, \%expected, "origin$i explicit cancellation commit");

	$s->query_safe("BEGIN; UPDATE p013_cancel_rows SET v='pl-parent-$i' WHERE id=$id;
		DECLARE parent_cursor NO SCROLL CURSOR FOR SELECT id FROM p013_cancel_cursor ORDER BY id;");
	is($s->query_safe('FETCH NEXT FROM parent_cursor'), '11', "origin$i PL parent cursor starts normally");
	my $pl_top = $s->query_safe('SELECT pg_current_xact_id_if_assigned()::text');
	my $pl = qq{DO \$\$ DECLARE progress int := 0; state text;
		child refcursor := 'pl_child_cursor'; seen int;
	BEGIN
		UPDATE p013_cancel_rows SET v='pl-discard-$i' WHERE id=$id;
		INSERT INTO p013_cancel_rows VALUES ($child_id,'pl-child-$i');
		OPEN child FOR SELECT id FROM p013_cancel_cursor ORDER BY id;
		FETCH child INTO seen;
		IF seen <> 11 THEN RAISE EXCEPTION 'child cursor did not run'; END IF;
		progress := 1;
		PERFORM pg_sleep(120);
		RAISE EXCEPTION 'caller cancellation did not arrive';
	EXCEPTION WHEN query_canceled THEN
		GET STACKED DIAGNOSTICS state=RETURNED_SQLSTATE;
		IF progress <> 1 OR state <> '57014' THEN RAISE EXCEPTION 'wrong cancel point'; END IF;
		INSERT INTO p013_cancel_rows VALUES ($handler_id,'handler-' || state);
	END; \$\$;};
	$s->query_until(qr/P013_PL_SENT\r?\n/, "\\echo P013_PL_SENT\n$pl\n");
	cancel_sleep($node, $pid, $start, 'DO $$', "origin$i PL exception");
	is($s->query_safe("\\echo PL_STATE=:SQLSTATE\n"), 'PL_STATE=00000',
		"origin$i explicit query_canceled handler returns normally");
	is($s->query_safe('SELECT pg_current_xact_id_if_assigned()::text'), $pl_top,
		"origin$i handler keeps the same outer transaction");
	is($s->query_safe(portal_names()), 'parent_cursor', "origin$i PL child portal is cleaned");
	is($s->query_safe('FETCH NEXT FROM parent_cursor'), '22', "origin$i PL parent cursor remains usable");
	is($s->query_safe("SELECT string_agg(id::text || ':' || v,chr(10) ORDER BY id)
		FROM p013_cancel_rows WHERE id IN ($id,$child_id,$handler_id)"), "$id:pl-parent-$i\n$handler_id:handler-57014",
		"origin$i PL handler preserves only outer and handler effects");
	$s->query_safe('COMMIT');
	is($s->query_safe('SELECT pg_backend_pid()'), $pid, "origin$i uses the same backend after both cancellations");
	ok($s->quit, "origin$i session exits normally");
	$expected{$id} = "pl-parent-$i";
	$expected{$handler_id} = 'handler-57014';
	all_rows(\@nodes, \%expected, "origin$i PL cancellation commit");
}
$nodes[0]->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
$cluster->stop_cluster;
$cluster->restart_cluster;
all_rows(\@nodes, \%expected, 'same-DATA restart');
for my $node (@nodes)
{
	is($node->safe_psql('postgres', q{SELECT string_agg(id::text,',' ORDER BY id) FROM p013_cancel_cursor}, timeout => 30),
		'11,22,33', $node->name . ' original cursor source remains intact');
}
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal|does not hold buffer slot/,
		$node->name . ' has no assertion, PANIC, signal termination or bad unlock');
}
done_testing();
