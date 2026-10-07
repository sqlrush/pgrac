# PGRAC: storage-quorum strings use the actual shared GUC policy and applier.
# These native fixtures do not activate storage or a Corosync service.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub literal
{
	my ($value) = @_;
	return 'NULL' if !defined $value;
	$value =~ s/'/''/g;
	return "'$value'";
}
my @settings = (
	['cluster.storage_quorum_cluster', 'storage-test', 'storage-other'],
	['cluster.storage_quorum_nodes', '0:11,1:12', '0:21,1:22']);
my $body = join('', map { "common.$_->[0]='$_->[1]'\n" } @settings);
my $node = PostgreSQL::Test::Cluster->new('storage_policy');
$node->init;
$node->append_conf('postgresql.conf', 'fsync=on');
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');

for my $setting (@settings)
{
	my ($name, $value, $changed) = @$setting;
	my $args = literal($name) . ',' . literal($value);
	like($node->safe_psql('postgres', "SELECT test_pgrac_config_entry(-1,$args,false)"),
		qr/^1:0:1:1:1:/, "$name is a common cold string");
	like($node->safe_psql('postgres', "SELECT test_pgrac_config_entry(0,$args,false)"),
		qr/^0:5:/, "$name cannot be an instance override");
	like($node->safe_psql('postgres', "SELECT test_pgrac_config_entry(-1,$args,true)"),
		qr/^0:6:/, "$name cannot be changed online");
	for my $replacement ($changed, undef)
	{
		like($node->safe_psql('postgres', 'SELECT test_pgrac_config_change('
			. literal($body) . ',-1,' . literal($name) . ',' . literal($replacement) . ',false)'),
			qr/^0:6:0:0$/, "$name replacement/removal refuses before staging");
	}
	is($node->safe_psql('postgres', 'SELECT test_pgrac_config_change('
		. literal($body) . ',-1,' . literal($name) . ',' . literal($value) . ',false)'),
		'1:0:0:0', "$name exact retry preserves the selected object");
}
like($node->safe_psql('postgres', 'SELECT test_pgrac_config_object('
	. literal($body) . ',false)'), qr/^1:0:2:2:2:/,
	'both strings round-trip through the complete encoded object and native policy');
is($node->safe_psql('postgres', 'SELECT test_pgrac_config_change('
	. literal($body) . q{,-1,'statement_timeout','5s',false)}), '1:0:1:1',
	'unchanged cold strings pass actual replacement-file staging and digest readback');
is($node->safe_psql('postgres', 'SHOW cluster.storage_quorum_cluster; SHOW cluster.storage_quorum_nodes'),
	"\n", 'policy/preparation does not assign native values');
$node->stop('fast');

sub fixture
{
	my ($label, $bytes) = @_;
	my $n = PostgreSQL::Test::Cluster->new($label);
	$n->init;
	$n->append_conf('postgresql.conf',
		"shared_preload_libraries='test_pgrac_shared_config'\n"
		. "test_pgrac_shared_config.apply_node=0\n"
		. "test_pgrac_shared_config.delivery=on\n"
		. "cluster.storage_quorum_cluster='local-shadow'\n"
		. "cluster.storage_quorum_nodes='0:99,1:98'\n");
	append_to_file($n->data_dir . '/test_config.input', $bytes);
	return $n;
}
my $applied = fixture('storage_applied', $body);
my $started = $applied->start(fail_ok => 1);
ok($started, 'real postmaster applies the shared cold strings');
if (!$started)
{
	diag(slurp_file($applied->logfile));
	done_testing();
	exit 0;
}
$applied->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($applied->safe_psql('postgres', q{
	SELECT name,setting,reset_val,source,pending_restart FROM pg_settings
	WHERE name LIKE 'cluster.storage_quorum_%' ORDER BY name}),
	"cluster.storage_quorum_cluster|storage-test|storage-test|configuration file|f\n"
	. 'cluster.storage_quorum_nodes|0:11,1:12|0:11,1:12|configuration file|f',
	'current and reset values come from shared input, not local file shadows');
my $common_sql = 'SELECT test_pgrac_config_active(false,true)';
my $base = $applied->safe_psql('postgres', $common_sql);
like($base, qr/^\d+:[1-9][0-9]*:[1-9][0-9]*:[0-9a-f]{64}:[0-9a-f]{64}$/,
	'actual shared common profile is available');
is($applied->safe_psql('postgres', 'SELECT test_pgrac_config_active(true,true)'), $base,
	'child common profile matches its actual parent');
$applied->stop('fast');

for my $setting (@settings)
{
	my ($name, $value, $changed) = @$setting;
	my $changed_body = $body;
	$changed_body =~ s/\Q$value\E/$changed/;
	my $label = $name =~ /nodes$/ ? 'nodes' : 'cluster';
	my $different = fixture("storage_changed_$label", $changed_body);
	$different->start;
	$different->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
	my @before = split /:/, $base;
	my @after = split /:/, $different->safe_psql('postgres', $common_sql);
	isnt($after[3], $before[3], "$name participates in actual common static fingerprint");
	is($after[4], $before[4], "$name change preserves unrelated dynamic fingerprint");
	$different->stop('fast');
	my $priority = fixture("storage_priority_$label", $body);
	command_fails_like(['postgres', '-D', $priority->data_dir, '-c', "$name=$changed"],
		qr/shared configuration conflicts with a higher-priority source/,
		"$name cannot be shadowed by command-line input");
}
done_testing();
