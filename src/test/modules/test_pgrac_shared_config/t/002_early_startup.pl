# PGRAC: native startup registration/control-read ordering, not PRE2 admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('early_config');
$node->init;
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name = 'cluster.node_id'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; no PRE2 startup qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', q{
	SET statement_timeout='1234ms';
	SELECT test_pgrac_config_registration();
	SHOW statement_timeout}), "t\n1234ms",
	'repeated registration preserves native phase and existing session values');
is($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name = 'cluster.shared_config'}),
	'1', 'one native definition, not duplicated at preload');
$node->stop('fast');
ok($node->start, 'default-off node restarts through unchanged native control');
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'default-off backend remains usable');
$node->stop('fast');

# This is a new isolated fixture, not any retained cluster or user data.
my $bad = PostgreSQL::Test::Cluster->new('projection_guard');
$bad->init;
$bad->append_conf('postgresql.conf', "cluster.shared_config = on\n");
my $control = $bad->data_dir . '/global/pg_control';
my $bytes = slurp_file($control);
substr($bytes, 0, 1) = chr(ord(substr($bytes, 0, 1)) ^ 1);
open(my $fh, '>:raw', $control) or die "open test control: $!";
print {$fh} $bytes;
close($fh) or die "close test control: $!";
ok(!$bad->start(fail_ok => 1), 'incomplete shared profile refuses startup');
like(slurp_file($bad->logfile),
	qr/invalid native bootstrap preparation input/,
	'profile is bound before reading the corrupt non-authoritative projection');
unlike(slurp_file($bad->logfile), qr/incorrect checksum in control file/,
	'legacy projection is not consulted for PRE2 sizing');
is(slurp_file($control), $bytes, 'startup refusal does not rewrite control bytes');
command_fails_like(['postgres', '--single', '-D', $bad->data_dir, 'postgres'],
	qr/PRE2 shared-control startup is not yet available/,
	'standalone entry also binds profile before control read');
is(slurp_file($control), $bytes, 'standalone refusal leaves the projection untouched');
done_testing();
