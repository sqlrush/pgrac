# PGRAC: actual native process-lifetime configuration observations.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('config_enrollment');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\nwork_mem='6MB'\nmax_connections=5\n");
append_to_file($node->data_dir . '/test_config.input', "common.work_mem='6MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
like($node->safe_psql('postgres', 'SELECT test_pgrac_config_enrollment(-1)'),
	qr/^\d+:[1-9]\d*:1:0:0$/, 'actual PM registered its own startup outcome');
my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on');
my $pid = $old->query_safe('SELECT pg_backend_pid()');
my $before = $node->safe_psql('postgres', "SELECT test_pgrac_config_enrollment($pid)");
like($before, qr/^\d+:[1-9]\d*:1:0:0$/, 'observer sees actual existing child registration');

append_to_file($node->data_dir . '/test_config.reload', "common.work_mem='9MB'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres',
	q{SELECT current_setting('test_pgrac_shared_config.reload_generation')='2'}),
	'actual PM advanced its defaults');
like($node->safe_psql('postgres', 'SELECT test_pgrac_config_enrollment(-1)'),
	qr/^\d+:[1-9]\d*:2:0:0$/, 'PM observation advances only after its actual application');
is($node->safe_psql('postgres', "SELECT test_pgrac_config_enrollment($pid)"), $before,
	'new parent state cannot rewrite the existing child observation');
$old->query_safe(q{SELECT test_pgrac_config_process(2,E'common.work_mem=''9MB''\n')});
my $after = $node->safe_psql('postgres', "SELECT test_pgrac_config_enrollment($pid)");
(my $expected = $before) =~ s/:1:0:0$/:2:0:0/;
is($after, $expected, 'same lifetime reports actual child application to another backend');
$old->query_safe(q{DO $$ BEGIN
	PERFORM test_pgrac_config_process(3,
	 E'common.check_function_bodies=''off''\ncommon.cluster.native_config_process_failure=''2''\ncommon.work_mem=''9MB''\n');
EXCEPTION WHEN OTHERS THEN
	IF SQLERRM <> 'test native reload assignment failure' THEN RAISE; END IF;
END $$});
$expected =~ s/:2:0:0$/:2:1:0/;
is($node->safe_psql('postgres', "SELECT test_pgrac_config_enrollment($pid)"), $expected,
	'partial native hook failure is visible to another actual process');
$old->quit;
is($node->safe_psql('postgres', "SELECT test_pgrac_config_enrollment($pid)"), 'absent',
	'actual normal exit removes the old live process observation');

my ($slot, $lifetime) = split /:/, $before;
my $replacement;
for (1..10)
{
	my $seen = $node->safe_psql('postgres', 'SELECT test_pgrac_config_enrollment(pg_backend_pid())');
	my ($index, $serial) = split /:/, $seen;
	if (defined $slot && $slot =~ /^\d+$/ && $index eq $slot)
	{
		$replacement = $serial;
		last;
	}
}
ok(defined $replacement && $replacement > ($lifetime // 0),
	'reused native slot has a strictly new registration, never predecessor proof');

my $aux = $node->safe_psql('postgres', q{SELECT test_pgrac_config_enrollment(pid)
 FROM pg_stat_activity WHERE backend_type='checkpointer'});
like($aux, qr/^\d+:[1-9]\d*:2:0:0$/,
	'actual native auxiliary participates outside client backend sessions');
for my $mode (0..4)
{
	is($node->safe_psql('postgres', "SELECT test_pgrac_config_slot_probe($mode)"), 't',
		'slot snapshot boundary: ' . ('coherent', 'writer in progress', 'read-only alias refusal',
			'exhausted sequence', 'relocated new shmem preserves actual native image')[$mode]);
}
$node->stop('fast');
my $plain = PostgreSQL::Test::Cluster->new('unseeded_enrollment');
$plain->init;
$plain->start;
$plain->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
like($plain->safe_psql('postgres', 'SELECT test_pgrac_config_enrollment(pg_backend_pid())'),
	qr/^\d+:[1-9]\d*:0:0:0$/, 'unseeded native PG process has no selected configuration proof');
$plain->stop('fast');
done_testing();
