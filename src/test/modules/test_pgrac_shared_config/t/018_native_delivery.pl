# PGRAC: real native PM/fork/detached logger reload, without application census.
# Selected source is injected by a test-only hook; this is not CF qualification.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nwork_mem='6MB'\n"
	. "log_filename='native-start.log'\nmax_connections=10\n");
append_to_file($node->data_dir . '/test_config.input',
	"common.work_mem='6MB'\nnode000.log_filename='native-start.log'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
like($node->safe_psql('postgres', 'SELECT pg_current_logfile()'), qr/native-start\.log$/,
	'actual native logger opened the initial file');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_delivery_refuse()'), 't',
	'an actual backend cannot publish for LMON or the parent');
my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on');
append_to_file($node->data_dir . '/test_config.reload',
	"common.max_connections='14'\ncommon.work_mem='9MB'\nnode000.log_filename='native-reloaded.log'\n");
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres', q{
 SELECT pg_current_logfile() ~ 'native-reloaded[.]log$'
}), 'detached logger actually reloaded and rotated without a registration ACK');
is($node->safe_psql('postgres', 'SHOW work_mem'), '9MB', 'new child inherits new parent value');
is($old->query_safe('SHOW work_mem'), '6MB', 'delayed child still owns its actual old value');
is($old->query_safe(q{SELECT test_pgrac_config_process(0, '')}), '1:0:0:0:1',
	'delayed child retains old defaults independently of native parent progress');
is($node->safe_psql('postgres',
	q{SELECT setting || ':' || pending_restart FROM pg_settings WHERE name='max_connections'}),
	'10:true', 'native pg_settings reports pending static value, not an application census');
$old->quit;

# A second image must produce another real native logger rotation; no process
# registration or census is used to decide whether SIGHUP is necessary.
open(my $fixture, '>', $node->data_dir . '/test_config.reload') or die $!;
print {$fixture} "common.max_connections='14'\ncommon.work_mem='12MB'\nnode000.log_filename='native-later.log'\n";
close($fixture) or die $!;
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=3\n");
$node->reload;
ok($node->poll_query_until('postgres', q{
 SELECT pg_current_logfile() ~ 'native-later[.]log$'
}), 'later image reaches the actual detached logger');
is($node->safe_psql('postgres', 'SHOW work_mem'), '12MB', 'later native parent defaults inherited');
$node->stop('fast');
done_testing();
