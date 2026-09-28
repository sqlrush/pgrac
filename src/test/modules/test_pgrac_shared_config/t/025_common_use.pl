# PGRAC: actual session overlays versus mandatory common-value observations.
# Raw equality used as common equality must fail these native counterexamples.
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
my $node = PostgreSQL::Test::Cluster->new('common_use');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nmax_connections=10\n");
my $body = "common.cluster.ges_handoff='off'\ncommon.max_connections='10'\n";
append_to_file($node->data_dir . '/test_config.input', $body);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $common = 'SELECT test_pgrac_config_active(false,true)';
my $registered = 'SELECT test_pgrac_config_active(true,true)';
my $raw = 'SELECT test_pgrac_config_active(false)';
my $census = 'SELECT test_pgrac_config_active_census(true)';
my $backend = $node->background_psql('postgres');
my $base = $backend->query_safe($common);
like($base, qr/^\d+:[1-9][0-9]*:[1-9][0-9]*:[0-9a-f]{64}:[0-9a-f]{64}$/,
	'actual native common profile exists');
my $raw_base = $backend->query_safe($raw);
ok($node->poll_query_until('postgres', q{
	SELECT test_pgrac_config_active_census(true) ~ '^[0-9]+:0:0:0$'
}), 'actual native family initially agrees');

$backend->query_safe('SET cluster.ges_handoff=on');
isnt($backend->query_safe($raw), $raw_base, 'raw diagnostics still see legal session override');
is($backend->query_safe($common), $base, 'legal session override cannot change mandatory common values');
is($backend->query_safe($registered), $base, 'session override does not revoke unrelated common proof');
like($node->safe_psql('postgres', $census), qr/^[0-9]+:0:0:0$/,
	'node common census does not mistake SET for missing application');
$backend->query_safe(consume(1, $body));
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_active_census()'), '1:0:0:1',
	'original raw census retains its truthful dynamic disagreement');
like($node->safe_psql('postgres', $census), qr/^[0-9]+:0:0:0$/,
	'raw disagreement is not mandatory common disagreement');
$backend->query_safe('RESET cluster.ges_handoff; BEGIN; SET LOCAL cluster.ges_handoff=on');
is($backend->query_safe($common), $base, 'LOCAL overlay preserves actual mandatory values');
is($backend->query_safe($registered), $base, 'LOCAL stack does not invalidate common observation');
$backend->query_safe('ROLLBACK');
is($backend->query_safe($registered), $base, 'rollback cannot revoke unchanged mandatory values');
$backend->query_safe('RESET ALL');
is($backend->query_safe($registered), $base, 'RESET ALL preserves independent common observation');

$backend->query_safe(consume(2, "common.cluster.ges_handoff='off'\ncommon.max_connections='14'\n"));
is($backend->query_safe('SHOW max_connections'), '10', 'static change is really pending');
is($backend->query_safe($common), $base, 'pending static target hashes running value, not desired value');
$backend->query_safe('SELECT test_pgrac_config_define_common()');
is($backend->query_safe($registered), $base, 'new session-settable registry entry is not mandatory equality');
$backend->query_safe('SELECT test_pgrac_config_define_common(true)');
is($backend->query_safe($registered), 'unavailable', 'new common SIGHUP entry revokes old mandatory observation');
my @before = split /:/, $base;
my @after = split /:/, $backend->query_safe($common);
is($after[2], $before[2] + 1, 'new common SIGHUP entry is included without an allowlist edit');
$backend->query_safe(consume(2, "common.cluster.ges_handoff='off'\ncommon.max_connections='14'\n"));
is($backend->query_safe($registered), $backend->query_safe($common),
	'actual successful observation refreshes complete mandatory profile');
$backend->query_safe(q{DO $$ BEGIN
 PERFORM test_pgrac_config_process(3,
 E'common.check_function_bodies=''off''\ncommon.cluster.native_config_process_failure=''2''\n');
EXCEPTION WHEN OTHERS THEN
 IF SQLERRM <> 'test native reload assignment failure' THEN RAISE; END IF;
END $$});
is($backend->query_safe($common), 'unavailable', 'failed native application cannot issue mandatory proof');
is($backend->query_safe($registered), 'unavailable', 'failed native application retires registered common proof');
$backend->quit;
$node->stop('fast');

# Fresh native lifetimes with the same selected object but different actual
# SIGHUP/static values prove that the required profile did not just omit them.
for my $variant (
	['sighup', "cluster.read_scache=on\n", 4],
	['static', "max_connections=14\n", 3])
{
	my ($label, $setting, $field) = @$variant;
	my $fresh = PostgreSQL::Test::Cluster->new("common_$label");
	$fresh->init;
	$fresh->append_conf('postgresql.conf',
		"shared_preload_libraries='test_pgrac_shared_config'\n"
		. "test_pgrac_shared_config.apply_node=0\nmax_connections=10\n$setting");
	append_to_file($fresh->data_dir . '/test_config.input',
		"common.cluster.ges_handoff='off'\n");
	$fresh->start;
	$fresh->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
	my @values = split /:/, $fresh->safe_psql('postgres', $common);
	isnt($values[$field], $before[$field], "actual $label difference cannot be hidden");
	is($values[7 - $field], $before[7 - $field], "unrelated profile remains unchanged for $label");
	$fresh->stop('fast');
}
done_testing();
