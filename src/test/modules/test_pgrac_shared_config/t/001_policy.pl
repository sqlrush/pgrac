# PGRAC: exercise production policy against the real registered native GUCs.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('config_policy');
$node->init;
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name = 'cluster.node_id'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; no PRE2 policy qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
sub entry
{
	my ($scope, $name, $value, $online) = @_;
	for ($name, $value) { s/'/''/g; }
	return $node->safe_psql('postgres',
		"SELECT test_pgrac_config_entry($scope, '$name', '$value', $online)");
}
sub object
{
	my ($body, $prepare) = @_;
	$body =~ s/'/''/g;
	return $node->safe_psql('postgres',
		"SELECT test_pgrac_config_object('$body', $prepare)");
}
my $before = $node->safe_psql('postgres', q{
	SELECT name, setting, reset_val, source, pending_restart
	FROM pg_settings WHERE name IN ('port','statement_timeout','cluster.node_id',
	'cluster.shared_data_dir','cluster.ges_request_timeout_ms','shared_buffers')
	ORDER BY name});

for my $enabled ('on', 'true', '1', 'yes')
{
	like(entry(-1, 'track_commit_timestamp', $enabled, 'false'), qr/^0:4:/,
		"shared commit-ts enable $enabled is outside the supported recovery profile");
	like(entry(-1, 'track_commit_timestamp', $enabled, 'true'), qr/^0:4:/,
		"online shared publication cannot stage commit-ts enable $enabled");
}
like(entry(-1, 'track_commit_timestamp', 'off', 'true'), qr/^1:0:1:1:/,
	'shared commit-ts off remains an explicit static default');
like(entry(-1, 'track_commit_timestamp', 'garbage', 'false'), qr/^0:8:/,
	'invalid boolean is distinguished from an unsupported true value');
for my $level ('logical', 'LOGICAL')
{
	like(entry(-1, 'wal_level', $level, 'false'), qr/^0:4:/,
		"shared WAL level $level is outside the physical recovery profile");
	like(entry(-1, 'wal_level', $level, 'true'), qr/^0:4:/,
		"shared publication cannot stage WAL level $level");
}
like(entry(-1, 'wal_level', 'replica', 'false'), qr/^1:0:/,
	'physical WAL level remains supported');

like(entry(-1, 'statement_timeout', '5s', 'true'), qr/^1:0:1:0:0:/,
	'native session parameter may have a shared default');
like(entry(0, 'port', '6543', 'true'), qr/^1:0:1:1:0:/,
	'instance listener remains restart-only');
like(entry(1, 'shared_buffers', '32MB', 'true'), qr/^1:0:1:1:0:/,
	'native memory units accepted without applying');
like(entry(-1, 'cluster.ges_request_timeout_ms', '-1', 'true'), qr/^1:0:1:/,
	'native cluster check hook permits perpetual wait with existing retransmission');
like(entry(0, 'cluster.node_id', '0', 'false'), qr/^1:0:1:1:1:/,
	'cold instance identity accepted for inspection');
like(entry(0, 'cluster.node_id', '1', 'false'), qr/^0:5:/,
	'node identity must equal the instance stanza, not merely fit its GUC range');
like(entry(0, 'cluster.node_id', '0', 'true'), qr/^0:6:/,
	'changed instance identity cannot be published online');
like(entry(-1, 'cluster.shared_data_dir', '/srv/pgrac', 'false'), qr/^1:0:1:1:1:/,
	'cold shared absolute reference accepted');
like(entry(-1, 'cluster.shared_data_dir', '/srv/pgrac', 'true'), qr/^0:6:/,
	'storage-layout change requires migration');
like(entry(0, 'cluster.external_fence_socket_path', '/run/pgrac/fenced.sock', 'false'),
	qr/^1:0:1:1:0:/, 'local credential socket is a reference, not an embedded credential');
like(entry(-1, 'port', '5432', 'false'), qr/^0:5:/, 'listener cannot be common');
like(entry(0, 'cluster.quorum_poll_interval_ms', '2000', 'false'), qr/^0:5:/,
	'common protocol settings cannot be overridden per node');
like(entry(0, 'statement_timeout', '1s', 'false'), qr/^0:5:/,
	'global default is not an instance protocol override');
like(entry(-1, 'cluster.unregistered_policy_probe', 'on', 'false'), qr/^0:2:/,
	'unknown custom name rejected');
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.unregistered_policy_probe'}),
	'0', 'no placeholder registered by validation');
like(entry(-1, 'server_version', '16', 'false'), qr/^0:3:/, 'internal setting rejected');
like(entry(-1, 'primary_conninfo', 'password=not-a-real-secret', 'false'), qr/^0:4:/,
	'connection strings cannot be persisted as shared credentials');
like(entry(-1, 'shared_preload_libraries', 'untrusted', 'false'), qr/^0:4:/,
	'library commands are not configuration references');
like(entry(-1, 'cluster.injection_points', 'test', 'false'), qr/^0:4:/,
	'fault controls are not durable shared parameters');
like(entry(-1, 'cluster.shared_data_dir', '/srv/../other', 'false'), qr/^0:7:/,
	'parent traversal reference rejected');
like(entry(0, 'cluster.external_fence_socket_path', 'relative.sock', 'false'), qr/^0:7:/,
	'relative local reference rejected');
like(entry(0, 'port', '99999', 'false'), qr/^0:8:/, 'native integer range enforced');
like(entry(-1, 'wal_level', 'impossible', 'false'), qr/^0:8:/, 'native enum enforced');
like(entry(-1, 'statement_timeout', 'bad', 'false'), qr/^0:8:/, 'native units/value enforced');
like(entry(-1, 'cluster.smart_fusion', 'on', 'false'), qr/^0:8:/,
	'existing native safety check hook still rejects unsafe enablement');

like(object("common.statement_timeout='5s'\nnode000.port='5432'\n", 'false'),
	qr/^1:0:2:1:0:/, 'whole canonical object traverses native policy');
like(object("common.cluster.shared_storage_uuid='01010101-0101-0101-0101-010101010101'\n",
	'false'), qr/^1:0:1:1:1:/, 'configured storage UUID matches object identity');
like(object("common.cluster.shared_storage_uuid='02020202-0202-0202-0202-020202020202'\n",
	'false'), qr/^0:7:/, 'well-formed foreign storage UUID cannot enter the bound object');
like(entry(-1, 'cluster.shared_storage_uuid', 'password=test', 'false'), qr/^0:7:/,
	'storage identity is not an arbitrary secret-bearing string');
like(object("common.primary_conninfo='password=test'\n", 'true'), qr/^0:4:/,
	'policy-invalid object rejected before inaccessible staging path');
like(object("common.statement_timeout='5s'\nnode000.port='bad'\n", 'false'),
	qr/^0:8:/, 'invalid later entry rejects the whole policy check');
like(object("common.statement_timeout='5s'\nnode000.port='5432'", 'false'),
	qr/^0:1:0:/, 'malformed later entry reaches no native checker');

is($node->safe_psql('postgres', q{
	SELECT name, setting, reset_val, source, pending_restart
	FROM pg_settings WHERE name IN ('port','statement_timeout','cluster.node_id',
	'cluster.shared_data_dir','cluster.ges_request_timeout_ms','shared_buffers')
	ORDER BY name}), $before, 'inspection preserves current/reset/source/restart state');
is($node->safe_psql('postgres', q{SET statement_timeout='1s'; SHOW statement_timeout}),
	'1s', 'legal PG session SET remains unchanged');
$node->stop('fast');
done_testing();
