# PGRAC: historical recovery minima against real applied native settings.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('recovery_capacity');
$node->init;
$node->append_conf('postgresql.conf', qq{
max_connections=60
max_worker_processes=10
max_wal_senders=4
wal_level=replica
max_prepared_transactions=2
max_locks_per_transaction=50
});
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.shared_config'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; not recovery qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
sub inspect
{
	my ($field, $value) = @_;
	return $node->safe_psql('postgres', "SELECT test_pgrac_recovery_capacity('$field', $value)");
}
my $settings_sql = q{SELECT name,setting,reset_val,source,pending_restart FROM pg_settings ORDER BY name};
my $before = $node->safe_psql('postgres', $settings_sql);
is(inspect('valid', 0), '1:0:5:', 'all five exact minima accepted without changing settings');
for my $case (
	['max_connections', 60], ['max_worker_processes', 10], ['max_wal_senders', 4],
	['max_prepared_transactions', 2], ['max_locks_per_transaction', 50])
{
	my ($name, $setting) = @$case;
	for my $need ($setting - 1, $setting)
	{
		is(inspect($name, $need), '1:0:5:', "$name applied value covers $need");
	}
	like(inspect($name, $setting + 1), qr/^0:10:\d+:\Q$name\E$/,
		"$name insufficient for history is refused, not silently raised");
	like(inspect($name, 2147483647), qr/^0:10:\d+:\Q$name\E$/,
		"$name valid large minimum does not wrap");
	like(inspect($name, 2147483648), qr/^0:1:/, "$name out-of-range input rejected");
}
for my $name ('max_connections', 'max_locks_per_transaction')
{
	like(inspect($name, 0), qr/^0:1:/, "$name requires positive minimum");
}
for my $case (['current', 0], ['current', 129], ['history', 257], ['null', 0], ['alias', 0])
{
	like(inspect(@$case), qr/^0:1:/, "malformed capacity $case->[0]=$case->[1] refused");
}
is(inspect('current', 128), '1:0:5:', 'maximum current set supported');
is(inspect('history', 256), '1:0:5:', 'maximum retained count for two origins supported');
is(inspect('report-null', 0), 'refused', 'NULL report rejected');
is($node->safe_psql('postgres', $settings_sql), $before,
	'no assignment, reset/source change or pending_restart side effect');
$node->stop('fast');
done_testing();
