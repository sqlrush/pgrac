# Copyright (c) 2026, pgrac contributors.
# Author: SqlRush <sqlrush@gmail.com>
# Shared DROP/TRUNCATE must refuse before any user DDL/trigger effect when
# the caller cannot finish one standalone top-level command.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use IPC::Run qw(run timeout);
use JSON::PP;
use Test::More;
use Time::HiRes qw(time);

my $result = { started_unix => time, queries => [], checks => [] };
my $path = "$ENV{TESTLOGDIR}/shared-ddl-transaction.json";
sub save_result
{
	open(my $out, '>', "$path.tmp") or die $!;
	print {$out} JSON::PP->new->canonical->pretty->encode($result);
	close($out) or die $!;
	rename("$path.tmp", $path) or die $!;
}
sub check
{
	my ($passed, $label) = @_;
	push @{$result->{checks}}, { passed => $passed ? JSON::PP::true : JSON::PP::false,
		label => $label };
	save_result();
	ok($passed, $label);
}
my @nodes;
sub sql
{
	my ($node, $label, $query) = @_;
	my ($stdout, $stderr) = ('', '');
	my @command = ("$ENV{PGRAC_PRE2_BINDIR}/psql", '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
		'-v', 'VERBOSITY=verbose', '-d', $nodes[$node]->connstr('postgres'), '-c', $query);
	my $record = { node => $node, label => $label, sql => $query, started_unix => time };
	push @{$result->{queries}}, $record;
	save_result();
	my $completed = eval {
		my $run_ok = run(\@command, '>', \$stdout, '2>', \$stderr, timeout(20));
		my $status = $?;
		$record->{raw_wait_status} = $status;
		$record->{signal} = $status & 127;
		$record->{rc} = $status >> 8;
		$record->{run_ok} = $run_ok ? JSON::PP::true : JSON::PP::false;
		die "owned SQL client terminated by signal $record->{signal}" if $record->{signal};
		die 'owned SQL client did not complete normally' if !$run_ok && $status == 0;
		1;
	};
	my $transport_error = "$@";
	$record->{transport_error} = $transport_error unless $completed;
	$record->{stdout} = $stdout;
	$record->{stderr} = $stderr;
	$record->{finished_unix} = time;
	save_result();
	die "$label transport failure: $transport_error" unless $completed;
	return $record;
}
sub success
{
	my ($node, $label, $query) = @_;
	my $record = sql($node, $label, $query);
	die "$label failed: $record->{stderr}" if $record->{rc};
	my $value = $record->{stdout};
	chomp($value);
	return $value;
}

my $cluster;
my $complete = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('shared_ddl_transaction',
		nodes => 4, blackbox => 1, required_operations => [],
		extra_conf => ['autovacuum=off', "shared_buffers='16MB'",
			'log_error_verbosity=verbose', "log_statement='all'"]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	for my $node (0..3) {
		check(success($node, "shared_catalog_$node", 'SHOW cluster.shared_catalog') eq 'on',
			"node $node uses shared catalog");
	}

	# Every setup command is a separate query string. No rejected command
	# creates a new shared object that would add abort-time storage work.
	for my $query ('CREATE ROLE p133_owner', 'CREATE SCHEMA p133_schema',
		'CREATE TABLE p133_audit(v integer)',
		'CREATE TABLE p133_drop(id integer PRIMARY KEY, v integer)',
		'CREATE TABLE p133_truncate(id integer PRIMARY KEY, v integer)',
		'CREATE TABLE p133_schema.t(id integer PRIMARY KEY, v integer)',
		'CREATE TABLE p133_owned(id integer PRIMARY KEY, v integer)',
		'ALTER TABLE p133_owned OWNER TO p133_owner') {
		success(0, 'setup', $query);
	}
	my @operations = (
		['drop', 'p133_drop', 'DROP TABLE p133_drop'],
		['truncate', 'p133_truncate', 'TRUNCATE p133_truncate'],
		['schema', 'p133_schema.t', 'DROP SCHEMA p133_schema CASCADE'],
		['owned', 'p133_owned', 'DROP OWNED BY p133_owner CASCADE']);
	my %before;
	sub identity_query {
		my ($table) = @_;
		return "SELECT c.oid,c.relfilenode,(SELECT count(*) FROM $table),"
			. "(SELECT sum(v) FROM $table) FROM pg_class c WHERE c.oid='$table'::regclass";
	}
	for my $operation (@operations) {
		my ($name, $table, $ddl) = @$operation;
		success(0, "seed_$name", "INSERT INTO $table VALUES (1,10),(2,20)");
		$before{$name} = success(0, "before_$name", identity_query($table));
		for my $language ('plpgsql', 'sql') {
			my $body = $language eq 'sql' ? "INSERT INTO p133_audit VALUES (1); $ddl;"
				: "BEGIN INSERT INTO p133_audit VALUES (1); EXECUTE '$ddl'; END";
			success(0, "function_${name}_$language",
				"CREATE FUNCTION p133_${name}_$language() RETURNS void LANGUAGE $language AS \$\$$body\$\$");
		}
	}
	# A wrong preflight order produces P0001 instead of 25001. This tests the
	# original event/statement-trigger entry, including the cascade commands.
	success(0, 'event_function', q{CREATE FUNCTION p133_event_guard() RETURNS event_trigger
		LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'p133 event effect ran'; END$$});
	success(0, 'event_trigger', q{CREATE EVENT TRIGGER p133_event_guard ON ddl_command_start
		WHEN TAG IN ('DROP TABLE','DROP SCHEMA','DROP OWNED') EXECUTE FUNCTION p133_event_guard()});
	success(0, 'truncate_function', q{CREATE FUNCTION p133_truncate_guard() RETURNS trigger
		LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'p133 truncate effect ran'; END$$});
	success(0, 'truncate_trigger', q{CREATE TRIGGER p133_truncate_guard BEFORE TRUNCATE
		ON p133_truncate FOR EACH STATEMENT EXECUTE FUNCTION p133_truncate_guard()});

	for my $operation (@operations) {
		my ($name, $table, $ddl) = @$operation;
		my @contexts = (
			['block', "BEGIN; INSERT INTO p133_audit VALUES(1); $ddl; COMMIT"],
			['plpgsql', "SELECT p133_${name}_plpgsql()"],
			['sql_function', "SELECT p133_${name}_sql()"],
			['do', "DO \$\$BEGIN INSERT INTO p133_audit VALUES(1); EXECUTE '$ddl'; END\$\$"],
			['multi_first', "$ddl; SELECT 1"],
			['multi_after_dml', "INSERT INTO p133_audit VALUES(1); $ddl"]);
		for my $context (@contexts) {
			my ($kind, $query) = @$context;
			my $record = sql(0, "${name}_$kind", $query);
			check($record->{rc} != 0 && $record->{stderr} =~ /ERROR:\s+25001:/,
				"$name $kind refuses with 25001 before triggers");
			check($record->{stderr} =~ /HINT:\s+Execute shared DROP or TRUNCATE/,
				"$name $kind supplies a standalone-statement hint");
			check(success(0, "${name}_${kind}_unchanged", identity_query($table)) eq $before{$name},
				"$name $kind preserves identity and data");
			check(success(0, "${name}_${kind}_not_committed", 'SELECT count(*) FROM p133_audit') eq '0',
				"$name $kind does not silently commit preceding DML");
		}
		for my $node (0..3) {
			check(success($node, "${name}_all_nodes_$node", identity_query($table)) eq $before{$name},
				"$name unchanged on node $node after all refusals");
		}
	}

	success(0, 'remove_event_guard', 'DROP EVENT TRIGGER p133_event_guard');
	success(0, 'remove_truncate_guard', 'DROP TRIGGER p133_truncate_guard ON p133_truncate');
	check(success(0, 'temporary_control', q{BEGIN; CREATE TEMP TABLE p133_temp(v integer);
		INSERT INTO p133_temp VALUES(7); TRUNCATE p133_temp; DROP TABLE p133_temp; ROLLBACK}) eq '',
		'temporary storage retains native transactional DDL');
	for my $operation (@operations) {
		my ($name, $table, $ddl) = @$operation;
		success(0, "${name}_standalone", $ddl);
		for my $node (0..3) {
			my $query = $name eq 'truncate' ? "SELECT count(*) FROM $table"
				: "SELECT to_regclass('$table') IS NULL";
			check(success($node, "${name}_standalone_$node", $query) eq ($name eq 'truncate' ? '0' : 't'),
				"standalone $name completes on node $node");
		}
	}
	$result->{stop_attempted} = JSON::PP::true;
	save_result();
	$result->{shutdown_receipt} = $cluster->stop_cluster;
	1;
};
unless ($complete) {
	$result->{error} = "$@";
	if ($cluster && $cluster->{running} && !$result->{stop_attempted}) {
		$result->{stop_attempted} = JSON::PP::true;
		save_result();
		eval { $result->{failure_cleanup_receipt} = $cluster->stop_cluster; 1 };
		$result->{cleanup_error} = "$@" if $@;
	}
}
$result->{complete} = $complete ? JSON::PP::true : JSON::PP::false;
$result->{failed_checks} = scalar grep { !$_->{passed} } @{$result->{checks}};
$result->{status} = $complete && !$result->{failed_checks} ? 'PASS' : 'FAIL';
$result->{finished_unix} = time;
save_result();
ok($complete, 'original four-node normal stop completes');
diag($result->{error}) unless $complete;
done_testing();
