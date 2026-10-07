# PGRAC: exact configuration change preparation, using native GUC checks.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('config_change');
$node->init;
$node->append_conf('postgresql.conf', 'fsync=on');
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.node_id'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; not PRE2 qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $body = "common.cluster.shared_data_dir='/srv/pgrac'\n"
	. "common.statement_timeout='5s'\nnode000.port='5432'\nnode001.port='5433'\n";
sub quote_sql
{
	my ($value) = @_;
	return 'NULL' if !defined $value;
	$value =~ s/'/''/g;
	return "'$value'";
}
sub change
{
	my ($scope, $name, $value, $bad, $original) = @_;
	return $node->safe_psql('postgres', 'SELECT test_pgrac_config_change('
		. quote_sql(defined $original ? $original : $body) . ",$scope,"
		. quote_sql($name) . ',' . quote_sql($value) . ','
		. ($bad ? 'true' : 'false') . ')');
}
my $state_sql = q{SELECT name,setting,reset_val,source,pending_restart
	FROM pg_settings WHERE name IN ('port','statement_timeout','cluster.shared_data_dir')
	ORDER BY name};
my $before = $node->safe_psql('postgres', $state_sql);
is(change(-1, 'statement_timeout', '6s'), '1:0:1:1',
	'changed common value produces actual staged bytes');
is(change(1, 'port', '6543'), '1:0:1:1',
	'instance change preserves the other node and cold common input');
is(change(-1, 'statement_timeout', undef), '1:0:1:1',
	'RESET of mutable entry stages a complete replacement object');
is(change(-1, 'statement_timeout', '5s'), '1:0:0:0',
	'identical SET does not stage or advance generation');
is(change(-1, 'work_mem', undef), '1:0:0:0',
	'absent RESET does not stage or advance generation');
is(change(-1, 'cluster.shared_data_dir', '/srv/pgrac'), '1:0:0:0',
	'unchanged cold entry is a no-op, not an online layout change');
like(change(-1, 'cluster.shared_data_dir', undef), qr/^0:6:0:0$/,
	'RESET cannot erase cold identity by bypassing changed-entry policy');
like(change(-1, 'cluster.shared_data_dir', '/other'), qr/^0:6:0:0$/,
	'online layout replacement refused before staging');
like(change(-1, 'port', '6543'), qr/^0:5:0:0$/,
	'instance parameter cannot acquire a common override');
like(change(1, 'port', '99999'), qr/^0:8:0:0$/,
	'native range refusal occurs before staging');
like(change(-1, 'primary_conninfo', 'password=test-only'), qr/^0:4:0:0$/,
	'unsupported credential-bearing string never reaches staging');
like(change(-1, 'statement_timeout', '6s', 1), qr/^0:1:0:0$/,
	'wrong selected hash cannot be repaired by amendment');
like(change(-1, 'statement_timeout', '6s', 0,
	"common.statement_timeout='5s'\nnode001.port='bad'\n"), qr/^0:8:0:0$/,
	'invalid unchanged remote-node entry prevents publication preparation');
is($node->safe_psql('postgres', $state_sql), $before,
	'preparation changes no current/reset/source/pending-restart setting');
$node->stop('fast');
done_testing();
