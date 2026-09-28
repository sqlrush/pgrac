# PGRAC: actual postmaster/child native inheritance, not a distributed ACK.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub quote_sql
{
	my ($s) = @_;
	$s =~ s/'/''/g;
	return "'$s'";
}
sub consume
{
	my ($generation, $body) = @_;
	return 'SELECT test_pgrac_config_process(' . $generation . ',' . quote_sql($body) . ')';
}
my $observe = consume(0, '');
my $node = PostgreSQL::Test::Cluster->new('process_inheritance');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\nwork_mem='6MB'\n");
my $original = "common.log_connections='off'\ncommon.work_mem='6MB'\n";
append_to_file($node->data_dir . '/test_config.input', $original);
$node->start;
if ($node->safe_psql('postgres', q{SELECT count(*) FROM pg_settings WHERE name='cluster.node_id'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; no process inheritance qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', $observe), '1:0:0:0:1',
	'child inherits exact startup state and actual parent applier PID');

my $old = $node->background_psql('postgres');
$old->query_safe('SET test_pgrac_shared_config.defer_process=on');
my $new = "common.log_connections='on'\ncommon.work_mem='9MB'\n";
my $early = $node->background_psql('postgres');
is($early->query_safe(consume(2, $new)), 'ok:2:0:1:0:0',
	'one existing child applies dynamically but keeps its BACKEND setting deferred');
is($early->query_safe('SHOW work_mem'), '9MB', 'early child has actual new dynamic value');
is($node->safe_psql('postgres', $observe), '1:0:0:0:1',
	'new child born from old parent cannot claim another child\'s target');
$early->quit;

# The test-only assign hook drives native application from the actual PM's
# normal SIGHUP context. No role globals, fake proc slots or generation ACKs.
append_to_file($node->data_dir . '/test_config.reload', $new);
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=2\n");
$node->reload;
ok($node->poll_query_until('postgres',
	q{SELECT current_setting('test_pgrac_shared_config.reload_generation') = '2'}),
	'actual parent processed its SIGHUP');
is($node->safe_psql('postgres', $observe), '2:0:0:0:1',
	'fresh child inherits new exact ref only after actual parent application');
is($node->safe_psql('postgres', 'SHOW work_mem; SHOW log_connections'), "9MB\non",
	'new child inherits actual dynamic and BACKEND defaults');
is($old->query_safe($observe), '1:0:0:0:1',
	'parent current does not make a delayed existing child current');
is($old->query_safe('SHOW work_mem; SHOW log_connections'), "6MB\noff",
	'delayed child still has its actual old values');
is($old->query_safe(consume(2, $new)), 'ok:2:0:1:0:0',
	'existing child consumes from its own retained old image');
is($old->query_safe('SHOW work_mem; SHOW log_connections'), "9MB\noff",
	'existing child defers session-start value without pretending it is on');
is($old->query_safe(consume(2, $new)), 'ok:2:0:1:0:0',
	'duplicate exact target preserves classified obligations');
is($old->query_safe('BEGIN; SET LOCAL work_mem=\'13MB\'; '
	. consume(4, "common.log_connections='on'\n") . '; SHOW work_mem; COMMIT; SHOW work_mem'),
	"ok:4:0:1:0:0\n13MB\n4MB", 'skipped generation RESET uses actual old image and preserves SET LOCAL');
is($old->query_safe(consume(3, $new)), 'refused:4:0:1:0:0',
	'late old target cannot replace successful process identity');
is($old->query_safe(consume(4, $new)), 'refused:4:0:1:0:0',
	'same generation with different bytes cannot overwrite current identity');
$old->quit;

my $fresh = $node->background_psql('postgres');
my $other_port = $node->port == 6543 ? 6544 : 6543;
is($fresh->query_safe(consume(2, $new)), 'ok:2:0:0:0:1',
	'duplicate inherited target creates no false deferred debt or child assignment');
is($fresh->query_safe(consume(3, $new . "node000.port='$other_port'\n")), 'ok:3:1:0:0:0',
	'actual static change stays pending in exact process outcome');
is($fresh->query_safe(consume(4, $new)), 'ok:4:1:0:0:0',
	'removed static value retains restart obligation');
is($fresh->query_safe(consume(5, $new)), 'ok:5:1:0:0:0',
	'later unrelated ref cannot retire restart obligation');
is($fresh->query_safe(consume(6, "common.log_connections='on'\ncommon.work_mem='10MB'\n")),
	'ok:6:1:0:0:0', 'changing only a dynamic value creates no inherited BACKEND debt');
is($fresh->query_safe('SHOW work_mem; SHOW log_connections'), "10MB\non",
	'unchanged inherited BACKEND value is actually active, not deferred');
$fresh->quit;

my $failed = $node->background_psql('postgres', on_error_stop => 0);
$failed->query_safe(q{DO $$ BEGIN
	PERFORM test_pgrac_config_process(3,
	 E'common.check_function_bodies=''off''\ncommon.cluster.native_config_process_failure=''2''\ncommon.log_connections=''on''\ncommon.work_mem=''9MB''\n');
EXCEPTION WHEN OTHERS THEN
	IF SQLERRM <> 'test native reload assignment failure' THEN RAISE; END IF;
END $$});
is($failed->query_safe($observe), '2:0:0:1:1',
	'partial native failure keeps last successful identity but marks process failed');
is($failed->query_safe('SHOW check_function_bodies'), 'off',
	'actual earlier native assignment was not speculatively rolled back');
is($failed->query_safe(consume(4, $new)), 'refused:2:0:0:1:1',
	'later target cannot clear potentially partial assign-hook state');
$failed->quit;
is($node->safe_psql('postgres', $observe), '2:0:0:0:1',
	'an actually new child inherits healthy parent state, not failed predecessor');
$node->stop('fast');

my $plain = PostgreSQL::Test::Cluster->new('process_unseeded');
$plain->init;
$plain->start;
$plain->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($plain->safe_psql('postgres', consume(2, $new)), 'refused:unseeded',
	'unseeded backend cannot manufacture startup proof through reload');
$plain->stop('fast');
done_testing();
