#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Trigger effects roll back with their scope; issued sequence values do not.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

my %state_queries;
for my $table (qw(p013_event_source p013_event_mirror p013_event_deferred))
{
	$state_queries{$table} = "SELECT coalesce(string_agg(id::text || ':' || v, chr(10) ORDER BY id), '') FROM $table";
}
$state_queries{p013_event_log} = q{
	SELECT coalesce(string_agg(kind || ':' || id || ':' || v, chr(10) ORDER BY kind,id), '') FROM p013_event_log
};
$state_queries{p013_sequence_effect} = q{
	SELECT coalesce(string_agg(id::text || ':' || stamp || ':' || label, chr(10) ORDER BY id), '') FROM p013_sequence_effect
};
$state_queries{p013_sequence_handler} = q{
	SELECT coalesce(string_agg(label || ':' || state, chr(10) ORDER BY label), '') FROM p013_sequence_handler
};

sub trigger_state
{
	my ($fired, @rows) = @_;
	my $values = join("\n", map { $_->[0] . ':' . $_->[1] . ':before' } @rows);
	my $log = join("\n", map {
		my $kind = $_;
		map { $kind . ':' . $_->[0] . ':' . $_->[1] . ($kind eq 'before' ? '' : ':before') } @rows
	} qw(before recursive statement));
	return {
		p013_event_source => $values, p013_event_mirror => $values,
		p013_event_log => $log, p013_event_deferred => $fired ? $values : ''
	};
}

sub local_state
{
	my ($session, $expected, $label) = @_;
	for my $table (sort keys %$expected)
	{
		is($session->query_safe($state_queries{$table}), $expected->{$table}, "$label $table");
	}
}

sub all_state
{
	my ($cluster, $expected, $label) = @_;
	for my $node ($cluster->nodes)
	{
		for my $table (sort keys %$expected)
		{
			is($node->safe_psql('postgres', $state_queries{$table}, timeout => 30),
				$expected->{$table}, $node->name . " $label $table");
		}
	}
}

my (%issued, %last_issued);
sub observe_value
{
	my ($group, $value, $label) = @_;
	$value =~ /^\d+$/ or BAIL_OUT("invalid sequence result: $value");
	ok(!exists $issued{$group}{$value}, "$label never reissues a previously returned value");
	cmp_ok($value, '>', $last_issued{$group} // 0, "$label advances beyond earlier returned values");
	$issued{$group}{$value} = 1;
	$last_issued{$group} = $value;
	diag("$label sequence=$group value=$value");
}

sub observe_pair
{
	my ($out, $label) = @_;
	my ($id, $stamp) = $out =~ /^(\d+)\|(\d+)$/;
	defined($id) or BAIL_OUT("invalid default/trigger sequence pair: $out");
	observe_value('default', $id, "$label default");
	observe_value('trigger', $stamp, "$label trigger");
	return "$id:$stamp:$label";
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_events',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
$nodes[0]->safe_psql('postgres', q{
	CREATE TABLE p013_event_source (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_event_mirror (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_event_log (kind text, id int, v text NOT NULL, PRIMARY KEY(kind,id));
	CREATE TABLE p013_event_deferred (id int PRIMARY KEY, v text NOT NULL);
	CREATE FUNCTION p013_event_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		INSERT INTO p013_event_log VALUES ('before',NEW.id,NEW.v);
		NEW.v := NEW.v || ':before'; RETURN NEW;
	END; $$;
	CREATE FUNCTION p013_event_after() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		INSERT INTO p013_event_mirror VALUES (NEW.id,NEW.v); RETURN NULL;
	END; $$;
	CREATE FUNCTION p013_event_recursive() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		INSERT INTO p013_event_log VALUES ('recursive',NEW.id,NEW.v); RETURN NEW;
	END; $$;
	CREATE FUNCTION p013_event_statement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		INSERT INTO p013_event_log SELECT 'statement',id,v FROM inserted_rows; RETURN NULL;
	END; $$;
	CREATE FUNCTION p013_event_defer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		INSERT INTO p013_event_deferred VALUES (NEW.id,NEW.v); RETURN NULL;
	END; $$;
	CREATE TRIGGER p013_before BEFORE INSERT ON p013_event_source FOR EACH ROW EXECUTE FUNCTION p013_event_before();
	CREATE TRIGGER p013_after AFTER INSERT ON p013_event_source FOR EACH ROW EXECUTE FUNCTION p013_event_after();
	CREATE TRIGGER p013_recursive BEFORE INSERT ON p013_event_mirror FOR EACH ROW EXECUTE FUNCTION p013_event_recursive();
	CREATE TRIGGER p013_statement AFTER INSERT ON p013_event_source REFERENCING NEW TABLE AS inserted_rows
		FOR EACH STATEMENT EXECUTE FUNCTION p013_event_statement();
	CREATE CONSTRAINT TRIGGER p013_defer AFTER INSERT ON p013_event_source DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION p013_event_defer();
	CREATE SEQUENCE p013_seq_direct CACHE 1;
	CREATE SEQUENCE p013_seq_default CACHE 1;
	CREATE SEQUENCE p013_seq_trigger CACHE 1;
	CREATE TABLE p013_sequence_effect (id bigint PRIMARY KEY DEFAULT nextval('p013_seq_default'),
		stamp bigint NOT NULL, label text UNIQUE NOT NULL);
	CREATE TABLE p013_sequence_handler (label text PRIMARY KEY, state text NOT NULL);
	CREATE FUNCTION p013_sequence_before() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		NEW.stamp := nextval('p013_seq_trigger');
		IF NEW.label = 'exception-discard' THEN RAISE division_by_zero; END IF;
		RETURN NEW;
	END; $$;
	CREATE TRIGGER p013_sequence BEFORE INSERT ON p013_sequence_effect FOR EACH ROW EXECUTE FUNCTION p013_sequence_before();
}, timeout => 30);

my $events = $nodes[0]->background_psql('postgres', timeout => 30);
$events->query_safe("BEGIN; INSERT INTO p013_event_source VALUES (1,'parent');
	SAVEPOINT event_point; INSERT INTO p013_event_source VALUES (2,'discard'),(3,'batch-discard');
	SAVEPOINT inner_point; INSERT INTO p013_event_source VALUES (4,'released-inner'); RELEASE inner_point;");
local_state($events, trigger_state(0,[1,'parent'],[2,'discard'],[3,'batch-discard'],[4,'released-inner']),
	'BEFORE, statement and recursive effects really ran before rollback');
$events->query_safe('ROLLBACK TO event_point');
local_state($events, trigger_state(0,[1,'parent']), 'rollback removes only child effects and queued events');
$events->query_safe("INSERT INTO p013_event_source VALUES (5,'fired-discard'); SET CONSTRAINTS p013_defer IMMEDIATE;");
local_state($events, trigger_state(1,[1,'parent'],[5,'fired-discard']),
	'child fires both the original parent event and its own event');
$events->query_safe('ROLLBACK TO event_point');
local_state($events, trigger_state(0,[1,'parent']), 'rollback undoes fired effects and restores parent event state');
$events->query_safe("INSERT INTO p013_event_source VALUES (6,'last-keep'); RELEASE event_point;");
local_state($events, trigger_state(0,[1,'parent'],[6,'last-keep']),
	'constraint mode is deferred again after rollback and release');
$events->query_safe('COMMIT');
$events->quit;
my $event_expected = trigger_state(1,[1,'parent'],[6,'last-keep']);
all_state($cluster, $event_expected, 'commit fires surviving events exactly once');
$nodes[1]->safe_psql('postgres', q{
	BEGIN; SAVEPOINT parent_abort; INSERT INTO p013_event_source VALUES (7,'parent-abort');
	RELEASE parent_abort; ROLLBACK;
}, timeout => 30);
all_state($cluster, $event_expected, 'parent abort removes RELEASEd effects and events');
$nodes[2]->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		INSERT INTO p013_event_source VALUES (8,'exception-discard'); PERFORM 1/0;
	EXCEPTION WHEN division_by_zero THEN
		INSERT INTO p013_event_source VALUES (9,'handler-' || SQLSTATE);
	END; $$;
	COMMIT;
}, timeout => 30);
$event_expected = trigger_state(1,[1,'parent'],[6,'last-keep'],[9,'handler-22012']);
all_state($cluster, $event_expected, 'anonymous rollback discards events while handler effects survive');

# One origin per sequence before restart; values may have allocation gaps.
my $seq = $nodes[3]->background_psql('postgres', timeout => 30);
$seq->query_safe('BEGIN');
observe_value('direct', $seq->query_safe("SELECT nextval('p013_seq_direct')"), 'direct parent');
$seq->query_safe('SAVEPOINT seq_point');
observe_value('direct', $seq->query_safe("SELECT nextval('p013_seq_direct')"), 'direct discarded child');
$seq->query_safe('ROLLBACK TO seq_point');
observe_value('direct', $seq->query_safe("SELECT nextval('p013_seq_direct')"), 'direct after child rollback');
$seq->query_safe('RELEASE seq_point; ROLLBACK');
observe_value('direct', $seq->query_safe("SELECT nextval('p013_seq_direct')"), 'direct after parent abort');
$seq->query_safe('BEGIN; SAVEPOINT set_point');
is($seq->query_safe("SELECT setval('p013_seq_direct',10000,true)"), '10000', 'forward SETVAL true executes');
$seq->query_safe('ROLLBACK TO set_point');
my $after_set = $seq->query_safe("SELECT nextval('p013_seq_direct')");
cmp_ok($after_set, '>', 10000, 'child rollback does not undo SETVAL true');
observe_value('direct', $after_set, 'direct after SETVAL true rollback');
$seq->query_safe('RELEASE set_point; COMMIT; BEGIN; SAVEPOINT false_point');
is($seq->query_safe("SELECT setval('p013_seq_direct',20000,false)"), '20000', 'forward SETVAL false executes');
$seq->query_safe('ROLLBACK TO false_point');
my $false_next = $seq->query_safe("SELECT nextval('p013_seq_direct')");
is($false_next, '20000', 'rollback preserves the explicit not-yet-called value');
observe_value('direct', $false_next, 'direct SETVAL false first issue');
$seq->query_safe('RELEASE false_point; ROLLBACK');
observe_value('direct', $seq->query_safe("SELECT nextval('p013_seq_direct')"), 'direct after SETVAL parent abort');

my @kept;
$seq->query_safe('BEGIN');
push @kept, observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('parent') RETURNING id,stamp"), 'parent');
$seq->query_safe('SAVEPOINT default_point');
observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('child-discard') RETURNING id,stamp"), 'child-discard');
$seq->query_safe('ROLLBACK TO default_point');
push @kept, observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('after-rollback') RETURNING id,stamp"), 'after-rollback');
$seq->query_safe(q{
	RELEASE default_point;
	DO $$ BEGIN
		INSERT INTO p013_sequence_effect(label) VALUES ('exception-discard');
	EXCEPTION WHEN division_by_zero THEN
		INSERT INTO p013_sequence_handler VALUES ('caught',SQLSTATE);
	END; $$;
});
observe_pair($seq->query_safe("SELECT currval('p013_seq_default'),currval('p013_seq_trigger')"), 'exception-discard');
push @kept, observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('after-exception') RETURNING id,stamp"), 'after-exception');
$seq->query_safe('COMMIT; BEGIN; SAVEPOINT release_point');
observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('parent-abort') RETURNING id,stamp"), 'parent-abort');
$seq->query_safe('RELEASE release_point; ROLLBACK');
push @kept, observe_pair($seq->query_safe("INSERT INTO p013_sequence_effect(label) VALUES ('after-parent-abort') RETURNING id,stamp"), 'after-parent-abort');
$seq->quit;
my $sequence_expected = { p013_sequence_effect => join("\n",@kept), p013_sequence_handler => 'caught:22012' };
all_state($cluster, $sequence_expected, 'default/trigger allocation survives while rejected row effects do not');

$nodes[0]->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
$cluster->stop_cluster;
$cluster->restart_cluster;
all_state($cluster, $event_expected, 'trigger effects after same-DATA restart');
all_state($cluster, $sequence_expected, 'sequence-consuming rows after same-DATA restart');
# Fresh caches may skip reserved numbers; a returned number must never be reissued.
for my $group (qw(direct default trigger))
{
	observe_value($group, $nodes[0]->safe_psql('postgres', "SELECT nextval('p013_seq_$group')", timeout => 30),
		"$group after restart");
}
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
		$node->name . ' has no assertion, PANIC or signal termination');
}
done_testing();
