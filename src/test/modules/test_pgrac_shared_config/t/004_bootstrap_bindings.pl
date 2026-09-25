# PGRAC: exact bootstrap bindings against the real native GUC registry.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('bootstrap_bindings');
$node->init;
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.shared_config'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; not PRE2 profile qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my @enabled = qw(cluster.controlfile_shared_authority cluster.enabled
	cluster.merged_recovery cluster.shared_catalog cluster.shared_config
	cluster.smgr_user_relations);
my %base = (
	(map { ("common.$_", 'on') } @enabled),
	'common.cluster.shared_storage_backend' => 'cluster_fs',
	'common.cluster.shared_storage_uuid' => '01010101-0101-0101-0101-010101010101',
	'common.cluster.shared_data_dir' => '/srv/pgrac/data',
	'common.cluster.wal_threads_dir' => '/srv/pgrac/wal',
	'common.cluster.undo_tablespace_path' => '/srv/pgrac/undo',
	'node000.cluster.node_id' => '0',
	'node001.cluster.node_id' => '1');
sub inspect
{
	my ($entries, $id, $paths, $bad_hash, $malformed) = @_;
	$id //= 0;
	$paths //= ['/srv/pgrac/data', '/srv/pgrac/wal', '/srv/pgrac/undo'];
	my $body = join('', map {
		my $value = $entries->{$_}; $value =~ s/'/''/g;
		"$_='$value'\n"
	} sort keys %$entries);
	chop($body) if $malformed;
	my @args = ($body, @$paths);
	for (@args) { s/'/''/g; }
	return $node->safe_psql('postgres',
		"SELECT test_pgrac_config_bootstrap('$args[0]', $id, '$args[1]', "
		. "'$args[2]', '$args[3]', " . ($bad_hash ? 'true' : 'false') . ')');
}
my $settings_sql = q{
SELECT name,setting,reset_val,source,pending_restart FROM pg_settings
WHERE name LIKE 'cluster.%' OR name IN ('port','shared_buffers','wal_level')
ORDER BY name};
my $before = $node->safe_psql('postgres', $settings_sql);
like(inspect(\%base), qr/^1:0:13:/, 'exact complete binding for node zero');
like(inspect(\%base, 1), qr/^1:0:13:/, 'configured node one selects same shared roots');
for my $key (sort keys %base)
{
	my %copy = %base;
	delete $copy{$key};
	(my $name = $key) =~ s/^(common|node\d{3})\.//;
	like(inspect(\%copy), qr/^0:9:.*:\Q$name\E$/, "required $key cannot use a local default");
}
for my $name (@enabled)
{
	my %copy = (%base, "common.$name" => 'off');
	like(inspect(\%copy), qr/^0:8:.*:\Q$name\E$/, "$name must be enabled");
	$copy{"common.$name"} = 'not-bool';
	like(inspect(\%copy), qr/^0:8:.*:\Q$name\E$/, "$name uses native boolean parsing");
}
for my $pair (
	['cluster.shared_storage_backend', 'block_device'],
	['cluster.shared_storage_uuid', '02020202-0202-0202-0202-020202020202'],
	['cluster.shared_data_dir', '/srv/other'],
	['cluster.wal_threads_dir', '/srv/other'],
	['cluster.undo_tablespace_path', '/srv/other'])
{
	my ($name, $value) = @$pair;
	my %copy = (%base, "common.$name" => $value);
	like(inspect(\%copy), qr/^0:7:.*:\Q$name\E$/, "$name cannot redirect bootstrap");
}
for my $id (-1, 2, 128)
{
	like(inspect(\%base, $id), qr/^0:5:/, "unbound local node $id rejected");
}
for my $path ('', 'relative', '/', '/srv/../data', '/srv//data', '/srv/data/')
{
	for my $field (0 .. 2)
	{
		my @paths = ('/srv/pgrac/data', '/srv/pgrac/wal', '/srv/pgrac/undo');
		$paths[$field] = $path;
		like(inspect(\%base, 0, \@paths), qr/^0:7:/, "bad expected path[$field] $path");
	}
}
my %wrong_node = (%base, 'node001.cluster.node_id' => '0');
like(inspect(\%wrong_node), qr/^0:5:/, 'other configured node identity is also checked');
my %unknown = (%base, 'common.cluster.unknown_bootstrap_probe' => 'on');
like(inspect(\%unknown), qr/^0:2:/, 'unknown native name rejected');
my %secret = (%base, 'common.primary_conninfo' => 'password=not-a-real-secret');
my $secret_result = inspect(\%secret);
like($secret_result, qr/^0:4:/, 'credential-bearing entry forbidden');
unlike($secret_result, qr/not-a-real-secret/, 'diagnostic never copies a value');
my %scope = (%base, 'node001.cluster.shared_config' => 'on');
like(inspect(\%scope), qr/^0:5:/, 'common feature cannot be node overridden');
like(inspect(\%base, 0, undef, 1), qr/^0:1:0:/, 'hash-corrupt object reaches no policy visitor');
like(inspect(\%base, 0, undef, 0, 1), qr/^0:1:0:/, 'malformed object reaches no policy visitor');
is($node->safe_psql('postgres', $settings_sql), $before,
	'checking bindings performs no assignment, source/reset change or pending restart');
$node->stop('fast');
done_testing();
