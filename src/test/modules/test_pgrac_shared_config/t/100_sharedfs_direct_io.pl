# PGRAC: direct data I/O participates in the common cold configuration.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('direct_io_policy');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
like($node->safe_psql('postgres', q{
  SELECT test_pgrac_config_entry(-1,'debug_io_direct','data',false)}),
  qr/^1:0:1:1:1:/, 'direct I/O is a common cold string with native value validation');
like($node->safe_psql('postgres', q{
  SELECT test_pgrac_config_entry(0,'debug_io_direct','data',false)}),
  qr/^0:5:/, 'an instance cannot override shared direct I/O');
like($node->safe_psql('postgres', q{
  SELECT test_pgrac_config_entry(-1,'debug_io_direct','data',true)}),
  qr/^0:6:/, 'direct I/O cannot be changed online');
like($node->safe_psql('postgres', q{
  SELECT test_pgrac_config_entry(-1,'debug_io_direct','invalid-io',false)}),
  qr/^0:8:/, 'invalid direct I/O options remain rejected by the native check hook');
is($node->safe_psql('postgres', 'SHOW debug_io_direct'), '',
  'validation does not enable direct I/O in the caller');
$node->stop('fast');

my $applied = PostgreSQL::Test::Cluster->new('direct_io_applied');
$applied->init;
$applied->append_conf('postgresql.conf',
  "shared_preload_libraries='test_pgrac_shared_config'\n"
  . "test_pgrac_shared_config.apply_node=0\n"
  . "test_pgrac_shared_config.delivery=on\n"
  . "debug_io_direct=''\n");
append_to_file($applied->data_dir . '/test_config.input', "common.debug_io_direct='data'\n");
my $started = $applied->start(fail_ok => 1);
ok($started, 'startup applies the shared direct I/O option');
if ($started)
{
  is($applied->safe_psql('postgres', q{
    SELECT setting,reset_val,source,pending_restart FROM pg_settings
    WHERE name='debug_io_direct'}), 'data|data|configuration file|f',
    'shared direct I/O replaces the local shadow at startup');
  $applied->stop('fast');
}
else
{
  diag(slurp_file($applied->logfile));
  fail('shared direct I/O startup did not produce a readable native setting');
}
done_testing();
