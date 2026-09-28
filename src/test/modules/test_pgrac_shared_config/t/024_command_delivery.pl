# PGRAC: native commands retaining ownership across their own transactions.
# Removing either the utility or direct-vacuum bracket must prematurely change
# the consumed ref. This tests real commands and a real background worker,
# not shared DATA admission, distributed convergence or autovacuum scheduling.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('command_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\ntest_pgrac_shared_config.delivery=on\n"
	. "logging_collector=on\nautovacuum=off\nwork_mem='6MB'\n");
sub body
{
	my ($value, $memory) = @_;
	return "common.cluster.ges_handoff='$value'\ncommon.work_mem='$memory'\n";
}
append_to_file($node->data_dir . '/test_config.input', body('off', '6MB'));
$node->start;
$node->safe_psql('postgres', q{
 CREATE EXTENSION test_pgrac_shared_config;
 CREATE TABLE utility_target(id int, payload text);
 INSERT INTO utility_target SELECT i, repeat(md5(i::text), 300) FROM generate_series(1,100) i;
 CREATE PROCEDURE utility_commit() LANGUAGE plpgsql AS $$BEGIN COMMIT; END$$;
 CREATE PROCEDURE utility_nested() LANGUAGE plpgsql AS $$BEGIN CALL utility_commit(); COMMIT; END$$;
});
my $client = $node->background_psql('postgres', on_error_stop => 0);
my $generation = 1;
my $value = 'off';
sub fixture_reset
{
	for my $suffix (qw(ready go result)) {
		my $path = $node->data_dir . "/test_config.work_$suffix";
		unlink($path) or die $! if -e $path;
	}
}
sub release_after_publish
{
	my ($next_value, $memory, $keep_waiting) = @_;
	$memory //= '6MB';
	my $ready = $node->data_dir . '/test_config.work_ready';
	for (1 .. 1000) { last if -f $ready; usleep(10000); }
	ok(-f $ready, 'actual command owner reached fixture pause') or BAIL_OUT('no command owner');
	++$generation;
	$value = $next_value;
	open(my $file, '>', $node->data_dir . '/test_config.reload') or die $!;
	print {$file} body($value, $memory);
	close($file) or die $!;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=$generation\n");
	$node->reload;
	ok($node->poll_query_until('postgres', qq{
	 SELECT split_part(test_pgrac_config_delivery(), ':', 1) = '$generation'
	}), "actual parent publishes generation $generation");
	append_to_file($node->data_dir . '/test_config.work_go', "go\n") unless $keep_waiting;
}

for my $case (
	['VACUUM utility_target', 'VACUUM with TOAST'],
	['CREATE INDEX CONCURRENTLY utility_idx ON utility_target(id)', 'concurrent index'],
	['CALL utility_commit()', 'nonatomic CALL'],
	['CALL utility_nested()', 'nested nonatomic CALL'],
	[q{DO $$BEGIN COMMIT; RAISE EXCEPTION 'expected command error'; END$$}, 'nonatomic ERROR'])
{
	fixture_reset();
	my $before = $generation;
	$client->query_safe('SET test_pgrac_shared_config.probe_utility=on');
	$client->query_until(qr/command sent/, "\\echo command sent\n$case->[0];\n");
	release_after_publish($value eq 'off' ? 'on' : 'off');
	if ($case->[1] eq 'nonatomic ERROR') {
		my (undef, $rc) = $client->query('SELECT 1');
		like($client->{stderr}, qr/expected command error/, 'real command ERROR reached native cleanup');
		$client->{stderr} = '';
	} else {
		$client->query_safe('SELECT 1');
	}
	is($client->query_safe('SELECT test_pgrac_config_work_state()'), "$before:$before:0",
		"$case->[1] retains old configuration across internal transactions");
	is($client->query_safe(q{SELECT split_part(test_pgrac_config_process(0, ''), ':', 1)}),
		"$generation", "$case->[1] releases ownership and native retry after command completion");
}
is($node->safe_psql('postgres', 'SELECT count(*) FROM utility_target'), '100', 'real table remains complete');
is($node->safe_psql('postgres', q{SELECT indisvalid FROM pg_index WHERE indexrelid='utility_idx'::regclass}),
	't', 'concurrent index really completed');

fixture_reset();
my $before = $generation;
my $worker_pid = $node->safe_psql('postgres', 'SELECT test_pgrac_config_work_launch()');
release_after_publish($value eq 'off' ? 'on' : 'off');
my $result_path = $node->data_dir . '/test_config.work_result';
for (1 .. 1000) { last if -f $result_path; usleep(10000); }
ok(-f $result_path, 'real direct-vacuum worker completed') or BAIL_OUT('worker did not complete');
my $result = slurp_file($result_path);
chomp($result);
is($result, "$before:$before:0", 'direct vacuum cannot reload in its internal transaction gap');
ok($node->poll_query_until('postgres', "SELECT NOT EXISTS (SELECT FROM pg_stat_activity WHERE pid=$worker_pid)"),
	'direct worker exits through native cleanup');

# The bracket is not a blanket native reload prohibition.
fixture_reset();
$client->query_safe('SELECT 1');
$before = $generation;
$client->query_safe('SET test_pgrac_shared_config.probe_utility=on');
$client->query_until(qr/default sent/, "\\echo default sent\nVACUUM utility_target;\n");
release_after_publish($value, '9MB');
$client->query_safe('SELECT 1');
is($client->query_safe('SELECT test_pgrac_config_work_state()'), "$before:$generation:1",
	'ordinary default-only image still applies within command ownership');

# Cancellation while the real utility hook is still on stack must release the
# enclosing bracket. It must not leave a surviving backend stuck forever.
fixture_reset();
my $pid = $client->query_safe('SELECT pg_backend_pid()');
$client->query_safe('SET test_pgrac_shared_config.probe_utility=on');
$client->query_until(qr/cancel sent/, "\\echo cancel sent\nVACUUM utility_target;\n");
release_after_publish($value eq 'off' ? 'on' : 'off', '9MB', 1);
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($pid)"), 't', 'real command is cancelled');
$client->query('SELECT 1');
like($client->{stderr}, qr/canceling statement due to user request/, 'native cancellation reached cleanup');
$client->{stderr} = '';
is($client->query_safe(q{SELECT split_part(test_pgrac_config_process(0, ''), ':', 1)}),
	"$generation", 'cancelled command does not leak configuration work ownership');
$client->quit;
$node->stop('fast');
done_testing();
