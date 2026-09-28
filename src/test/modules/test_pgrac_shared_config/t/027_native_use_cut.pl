# PGRAC: a held native producer cut, including future transactions/children.
# Raw test control is NOT a CF/member/service closure certificate.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_use_cut');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\ntest_pgrac_shared_config.delivery=on\n"
	. "autovacuum=off\nmax_connections=12\nmax_parallel_workers_per_gather=2\n");
append_to_file($node->data_dir . '/test_config.input', "common.cluster.read_scache='off'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', q{SELECT count(*) FROM pg_stat_cluster_wait_events
 WHERE name='ReconfigSharedConfigWait'}), '1', 'held native wait is registered for observation');
$node->safe_psql('postgres', 'CREATE TABLE config_cut_parallel AS SELECT generate_series(1,10000) AS n; '
	. 'ALTER TABLE config_cut_parallel SET (parallel_workers=2); ANALYZE config_cut_parallel');
$node->safe_psql('postgres', q{
 CREATE TABLE utility_target(id int, payload text);
 INSERT INTO utility_target SELECT i, repeat(md5(i::text), 300) FROM generate_series(1,100) i;
 CREATE PROCEDURE cut_commit() LANGUAGE plpgsql AS $$BEGIN COMMIT; END$$;
 CREATE PROCEDURE cut_nested() LANGUAGE plpgsql AS $$BEGIN CALL cut_commit(); COMMIT; END$$;
});
my $request = 0;
sub control
{
	my ($mode) = @_;
	++$request;
	++$request while $request % 3 != $mode;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.gate_request=$request\n");
	$node->reload;
	my $path = $node->data_dir . '/test_config.gate';
	for (1..200) {
		if (-e $path) {
			my $text = slurp_file($path);
			if ($text =~ /^$request:(\d+):(\d+):(\d+):(\d+):(\d+)\n$/) {
				return [$1, $2, $3, $4, $5];
			}
		}
		select(undef, undef, undef, 0.05);
	}
	die "native PM did not acknowledge test control $request";
}
sub wait_native
{
	my ($expected) = @_;
	for (1..80) {
		my $observed = control(2);
		return $observed if $observed->[4] == $expected;
		select(undef, undef, undef, 0.05);
	}
	die "native configuration waiters did not become $expected";
}
my $old = $node->background_psql('postgres', on_error_stop => 0);
$old->query_safe('BEGIN');
my $state = control(2);
is($state->[2], 1, 'real transaction owns the held native cut');
# Bound the pre-consumer RED: the primitive alone must not masquerade as the
# native integration. Stop this fixture after the actual missing ownership.
if ($state->[2] != 1) {
	$old->query_safe('ROLLBACK');
	$old->quit;
	$node->stop;
	done_testing();
	exit;
}
$state = control(1);
is_deeply([$state->[0], $state->[2], $state->[3]], [1, 1, 1],
	'CLOSE holds old transaction without retiring it');
is(control(0)->[3], 0, 'OPEN refuses while real old owner survives');
is($old->query_safe('SELECT 42'), '42', 'old transaction continues through the closed cut');
$old->query_safe('COMMIT');
is(control(2)->[2], 0, 'COMMIT retires actual resources before native owner');
ok(control(0)->[3], 'exact empty cut opens');
$old->query_safe('BEGIN');
control(1);
$old->query_safe('ROLLBACK');
is(control(2)->[2], 0, 'ROLLBACK retires the actual owner');
ok(control(0)->[3], 'rollback cut opens');

# Existing idle connection: the next transaction must wait, not just new
# sockets. The PM remains runnable and reads the real native wait event.
my $pid = $old->query_safe('SELECT pg_backend_pid()');
control(1);
$old->query_until(qr/cut_query_started/, "\\echo cut_query_started\nSELECT 71;\n");
$state = wait_native(1);
is($state->[2], 0, 'new transaction waits without manufacturing an old owner');
ok(control(0)->[3], 'controller opens exact empty transaction cut');
like($old->query_until(qr/cut_query_done/, "\\echo cut_query_done\n"), qr/71/,
	'waiting transaction really completes after OPEN');

# Same-message native AND CHAIN must not sneak through the old ownership.
for my $ending ('COMMIT', 'ROLLBACK') {
	$old->query_safe('BEGIN');
	control(1);
	$old->query_until(qr/chain_started/, "\\echo chain_started\n$ending AND CHAIN; SELECT 72;\n");
	$state = wait_native(1);
	is($state->[2], 0, "$ending AND CHAIN retires old owner and waits before the successor");
	ok(control(0)->[3], "$ending chain can open without a false old owner");
	like($old->query_until(qr/chain_done/, "\\echo chain_done\n"), qr/72/, 'chained transaction completes');
	$old->query_safe('ROLLBACK');
}

# ERROR does not masquerade as resource retirement while the aborted block
# still needs native cleanup. ROLLBACK remains usable through the held cut.
$old->query_safe('BEGIN');
control(1);
my (undef, $error_rc) = $old->query('SELECT 1/0');
is($error_rc, 1, 'intentional native ERROR aborts existing work');
like($old->{stderr}, qr/division by zero/, 'ERROR is the fixture arithmetic failure');
$old->{stderr} = '';
is(control(2)->[2], 1, 'aborted block retains owner until actual cleanup');
$old->query_safe('ROLLBACK');
is(control(2)->[2], 0, 'explicit cleanup retires aborted owner');
ok(control(0)->[3], 'aborted block leaves an openable cut');

# Session locks outlive COMMIT: owner must still execute the real unlock.
$old->query_safe('SELECT pg_advisory_lock(910001); SELECT pg_advisory_lock(910001)');
control(1);
is(control(2)->[2], 1, 'idle session-lock owner is counted across commands');
is($old->query_safe('SELECT pg_advisory_unlock(910001)'), 't', 'reentrant owner can unlock once');
is(control(2)->[2], 1, 'first unlock does not retire the remaining native hold');
is($old->query_safe('SELECT pg_advisory_unlock(910001)'), 't', 'last unlock is not deadlocked by the cut');
is(control(2)->[2], 0, 'last actual lock and transaction retirement releases owner');
ok(control(0)->[3], 'session-lock cut opens after cleanup');

# The real utility hook pauses BEFORE entering the command; once released,
# actual VACUUM/CIC/CALL internal commits must not become independent entries.
sub work_ready
{
	my $path = $node->data_dir . '/test_config.work_ready';
	for (1..400) {
		return if -f $path;
		select(undef, undef, undef, 0.025);
	}
	die 'real native command did not reach its fixture pause';
}
sub work_reset
{
	for my $suffix (qw(ready go result)) {
		my $path = $node->data_dir . "/test_config.work_$suffix";
		unlink($path) or die $! if -e $path;
	}
}
for my $case (
	['VACUUM utility_target', 'VACUUM/TOAST'],
	['CREATE INDEX CONCURRENTLY cut_idx ON utility_target(id)', 'CIC'],
	['CALL cut_nested()', 'nested CALL'],
	[q{DO $$BEGIN COMMIT; RAISE EXCEPTION 'expected cut command error'; END$$}, 'nonatomic ERROR'])
{
	work_reset();
	$old->query_safe('SET test_pgrac_shared_config.probe_utility=on');
	$old->query_until(qr/utility_started/, "\\echo utility_started\n$case->[0];\n");
	work_ready();
	is(control(1)->[2], 1, "$case->[1] owns the cut across its whole command");
	append_to_file($node->data_dir . '/test_config.work_go', "go\n");
	$old->query_until(qr/utility_done/, "\\echo utility_done\n");
	if ($case->[1] eq 'nonatomic ERROR') {
		like($old->{stderr}, qr/expected cut command error/, 'real nonatomic ERROR reaches native cleanup');
		$old->{stderr} = '';
	}
	is(control(2)->[2], 0, "$case->[1] completes all internal transactions without being stranded");
	ok(control(0)->[3], "$case->[1] really retires before OPEN");
}
work_reset();
my $worker_pid = $old->query_safe('SELECT test_pgrac_config_work_launch()');
work_ready();
is(control(1)->[2], 1, 'direct native vacuum worker is counted before the cut');
append_to_file($node->data_dir . '/test_config.work_go', "go\n");
for (1..400) {
	last if -f $node->data_dir . '/test_config.work_result';
	select(undef, undef, undef, 0.025);
}
ok(-f $node->data_dir . '/test_config.work_result', 'real direct vacuum returns across internal commits');
for (1..80) {
	$state = control(2);
	last if $state->[2] == 0;
	select(undef, undef, undef, 0.05);
}
is($state->[2], 0, 'direct vacuum owner retires through actual transaction/exit');
ok(control(0)->[3], 'direct vacuum leaves an openable cut');

# A genuine lock-group continuation can start after CLOSE. Roles and a
# positive unrelated count alone are not what the production binding checks.
$old->query_safe('SET parallel_setup_cost=0; SET parallel_tuple_cost=0; '
	. 'SET min_parallel_table_scan_size=0; SET parallel_leader_participation=off; BEGIN');
my $parallel = 'SELECT min(test_pgrac_config_parallel_observe()), count(*) FROM config_cut_parallel';
like($old->query_safe('EXPLAIN (COSTS OFF) ' . $parallel), qr/Gather.*Partial Aggregate/s,
	'parallel probe executes in actual worker partial aggregates');
control(1);
like($old->query_safe($parallel), qr/^1:1:0:\d+\|10000$/,
	'new real parallel workers finish existing leader work through CLOSE');
is(control(2)->[2], 1, 'parallel workers retire without dropping their leader');
$old->query_safe('COMMIT');
is(control(2)->[2], 0, 'leader retirement closes the parallel family');
ok(control(0)->[3], 'parallel cut opens');

# A child born after CLOSE waits before InitPostgres catalog use.
control(1);
my $new = $node->background_psql('postgres', wait => 0);
$state = wait_native(1);
is($state->[2], 0, 'new native child waits before dependent initialization');
ok(control(0)->[3], 'new-child cut opens');
$new->wait_connect;
is($new->query_safe('SELECT 73'), '73', 'new child initializes and works after OPEN');
$new->quit;

# Caller cancellation is not a manufactured gate timeout and leaves no debt.
control(1);
$old->query_until(qr/cancel_started/, "\\echo cancel_started\nSELECT 74;\n");
wait_native(1);
ok(kill('INT', $pid), 'send real cancellation to the waiting backend');
$old->query_until(qr/cancel_done/, "\\echo cancel_done\n");
like($old->{stderr}, qr/canceling statement due to user request/, 'native cancellation exits wait');
$old->{stderr} = '';
is(control(2)->[2], 0, 'cancelled admission does not leak a native owner');
ok(control(0)->[3], 'cancelled wait does not poison the held cut');

# Final exit must release a real session lock before dropping its marker.
$old->query_safe('SELECT pg_advisory_lock(910002)');
control(1);
$old->quit;
for (1..80) {
	$state = control(2);
	last if $state->[2] == 0;
	select(undef, undef, undef, 0.05);
}
is($state->[2], 0, 'native exit retires the session-lock owner after lock cleanup');
ok(control(0)->[3], 'native exit leaves an openable cut');
$new = $node->background_psql('postgres');
is($new->query_safe('SELECT pg_try_advisory_lock(910002)'), 't', 'old session lock really disappeared');
$new->query_safe('SELECT pg_advisory_unlock_all()');
# A native temporary namespace owns a later catalog-cleanup transaction even
# after all visible transactions commit. Exit callbacks disable interrupts;
# they must inherit counted ownership, not independently wait on CLOSED.
$new->query_safe('CREATE TEMP TABLE cut_temp AS SELECT 1 AS n');
$state = control(1);
is($state->[2], 1, 'committed temporary namespace retains native cleanup ownership');
if ($state->[2] != 1) {
	# Preserve a bounded real RED without stranding an uncancellable native
	# exit callback. This is fixture cleanup, not accepted product behavior.
	control(0);
	$new->quit;
} else {
	$new->quit;
	for (1..80) {
		$state = control(2);
		last if $state->[2] == 0;
		select(undef, undef, undef, 0.05);
	}
	is($state->[2], 0, 'native temporary-relation cleanup finishes before owner retirement');
	ok(control(0)->[3], 'temporary-session exit leaves an openable cut');
}
$node->stop('fast');
done_testing();
