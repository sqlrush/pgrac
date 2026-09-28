# PGRAC: actual common native values, never desired-file or admission proof.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub consume
{
	my ($generation, $body) = @_;
	$body =~ s/'/''/g;
	return "SELECT test_pgrac_config_process($generation,'$body')";
}
my $node = PostgreSQL::Test::Cluster->new('active_profile');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\nmax_connections=10\nwork_mem='6MB'\n");
my $original = "common.cluster.ges_handoff='off'\ncommon.max_connections='10'\ncommon.work_mem='6MB'\n";
append_to_file($node->data_dir . '/test_config.input', $original);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $backend = $node->background_psql('postgres');
my $actual = 'SELECT test_pgrac_config_active(false)';
my $registered = 'SELECT test_pgrac_config_active(true)';
my $base = $backend->query_safe($actual);
like($base, qr/^1:[1-9][0-9]*:[1-9][0-9]*:[0-9a-f]{64}:[0-9a-f]{64}$/,
	'actual native typed static and dynamic observations exist');
is($backend->query_safe($registered), $base,
	'native backend attachment publishes its actual values');
$backend->query_safe('SET work_mem=8192; SET statement_timeout=1000');
is($backend->query_safe($actual), $base, 'ordinary session settings do not change common profile');
is($backend->query_safe($registered), $base, 'session-only SET does not invalidate observation');
$backend->query_safe('RESET work_mem; RESET statement_timeout');
$backend->query_safe(consume(2,
	"common.cluster.ges_handoff='false'\ncommon.max_connections='010'\ncommon.work_mem='6MB'\n"));
is($backend->query_safe($actual), $base, 'native equality ignores equivalent literal spellings');
$backend->query_safe(consume(3,
	"common.cluster.ges_handoff='off'\ncommon.max_connections='14'\ncommon.work_mem='6MB'\n"));
is($backend->query_safe('SHOW max_connections'), '10', 'static target is really pending');
is($backend->query_safe($actual), $base, 'pending static target does not replace actual active hash');
$backend->query_safe(consume(4, "common.cluster.ges_handoff='off'\ncommon.work_mem='6MB'\n"));
is($backend->query_safe($actual), $base, 'RESET retains actual static value in the profile');
$backend->query_safe(consume(5, "common.cluster.ges_handoff='off'\ncommon.work_mem='9MB'\n"));
is($backend->query_safe($actual), $base, 'later generation does not erase omitted static identity');
my $body6 = "common.cluster.ges_handoff='on'\ncommon.work_mem='9MB'\n";
$backend->query_safe(consume(6, $body6));
my $on = $backend->query_safe($actual);
my @base_fields = split /:/, $base;
my @on_fields = split /:/, $on;
is($on_fields[3], $base_fields[3], 'dynamic change preserves independent static profile');
isnt($on_fields[4], $base_fields[4], 'actual dynamic safety value changes its profile');
is($backend->query_safe($registered), $on, 'successful native reload publishes new actual values');
$backend->query_safe("SELECT test_pgrac_config_entry(-1,'cluster.ges_handoff','off',true)");
is($backend->query_safe($registered), $on, 'check-only policy does not invalidate active observation');
$backend->query_safe('SET cluster.ges_handoff=off');
is($backend->query_safe($registered), 'unavailable', 'real SET revokes old registration before reuse');
is($backend->query_safe($actual), $base, 'actual observation sees the native session override');
$backend->query_safe(consume(6, $body6));
is($backend->query_safe($registered), $base, 'safe duplicate application observes actual overlay, not file literal');
$backend->query_safe('RESET ALL');
is($backend->query_safe($registered), 'unavailable', 'RESET ALL revokes the former active observation');
is($backend->query_safe($actual), $on, 'RESET ALL restored real system defaults');
$backend->query_safe(consume(6, $body6));
$backend->query_safe('BEGIN; SET LOCAL cluster.ges_handoff=off');
is($backend->query_safe($registered), 'unavailable', 'SET LOCAL invalidates before native assignment');
$backend->query_safe(consume(6, $body6));
is($backend->query_safe($registered), $base, 'safe observation includes actual LOCAL overlay');
$backend->query_safe('ROLLBACK');
is($backend->query_safe($registered), 'unavailable', 'native transaction restoration invalidates old observation');
is($backend->query_safe($actual), $on, 'native rollback restores dynamic value');
$backend->query_safe(consume(6, $body6));
$backend->query_safe(q{DO $$ BEGIN
 PERFORM test_pgrac_config_process(7,
 E'common.check_function_bodies=''off''\ncommon.cluster.native_config_process_failure=''2''\ncommon.work_mem=''9MB''\n');
EXCEPTION WHEN OTHERS THEN
 IF SQLERRM <> 'test native reload assignment failure' THEN RAISE; END IF;
END $$});
is($backend->query_safe($actual), 'unavailable', 'partial shared assignment cannot issue an active profile');
is($backend->query_safe($registered), 'unavailable', 'failed native process revokes its published profile');
$backend->quit;
$node->stop('fast');

my $restarted = PostgreSQL::Test::Cluster->new('new_static_profile');
$restarted->init;
$restarted->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\nmax_connections=14\nwork_mem='6MB'\n");
my $new_static = "common.cluster.ges_handoff='off'\ncommon.max_connections='14'\ncommon.work_mem='6MB'\n";
append_to_file($restarted->data_dir . '/test_config.input', $new_static);
$restarted->start;
$restarted->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $new_backend = $restarted->background_psql('postgres');
$new_backend->query_safe(consume(3, $new_static));
is($new_backend->query_safe('SHOW max_connections'), '14', 'new native lifetime really activated static target');
my @new_fields = split /:/, $new_backend->query_safe($actual);
isnt($new_fields[3], $base_fields[3], 'same consumed generation cannot hide old versus restarted static values');
is($new_fields[4], $base_fields[4], 'different instance port and static capacity leave dynamic profile equal');
$new_backend->query_safe('SELECT test_pgrac_config_define_common()');
is($new_backend->query_safe($registered), 'unavailable', 'late native common registry change revokes older observation');
my @late_fields = split /:/, $new_backend->query_safe($actual);
is($late_fields[2], ($new_fields[2] // 0) + 1, 'new common scalar is conservatively included');
$new_backend->quit;
$restarted->stop('fast');
done_testing();
