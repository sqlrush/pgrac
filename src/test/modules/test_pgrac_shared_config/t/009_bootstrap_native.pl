# PGRAC: actual early native composition, no PRE2 serving/admission claim.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('native_bootstrap');
$node->init;
$node->append_conf('postgresql.conf', 'wal_buffers=32kB');
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.shared_config'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; not PRE2 startup qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my ($rc, $out, $err) = $node->psql('postgres', 'SELECT test_pgrac_bootstrap_late()');
ok($rc != 0, 'backend cannot apply startup configuration');
like($err, qr/native bootstrap preparation requires early startup/, 'native late-entry boundary');
for my $install ('true', 'false')
{
	($rc, $out, $err) = $node->psql('postgres',
		"SELECT test_pgrac_bootstrap_control_late($install)");
	ok($rc != 0, "native control initialization $install cannot run in a backend");
	like($err, qr/native control initialization requires early startup/,
		"native control initialization $install rejects before mutation");
}
my $log_offset = 0;
for my $case (
	['valid', qr/test native bootstrap prepared; no admission or storage initialization/],
	['geometry', qr/test native bootstrap prepared; no admission or storage initialization/],
	['wal-flat', qr/native bootstrap WAL routing is not exact/],
	['wal-missing', qr/native bootstrap WAL routing is not exact/],
	['side-missing', qr/native bootstrap side routing is not exact/],
	['side-literal', qr/native bootstrap side routing is not exact/],
	['side-foreign', qr/native bootstrap side routing is not exact/],
	['side-mx-symlink', qr/native bootstrap side routing is not exact/],
	['prefix-missing', qr/native bootstrap WAL routing is not exact/],
	['prefix-corrupt', qr/native bootstrap WAL routing is not exact/],
	['prefix-identity', qr/native bootstrap WAL routing is not exact/],
	['recheck-root', qr/native WAL bootstrap preparation changed/],
	['recheck-binding', qr/native WAL bootstrap preparation changed/],
	['recheck-route', qr/native WAL bootstrap route changed/],
	['recheck-prefix', qr/native WAL bootstrap route changed/],
	['recheck-side', qr/native side bootstrap route changed/],
	['recheck-pgdata', qr/native WAL bootstrap preparation changed/],
	['wal-min', qr/"min_wal_size" must be at least twice "wal_segment_size"/],
	['wal-max', qr/"max_wal_size" must be at least twice "wal_segment_size"/],
	['capacity', qr/native bootstrap recovery capacity is insufficient.*max_connections/s],
	['profile', qr/native bootstrap profile is not applicable/],
	['native-format', qr/BLCKSZ/],
	['checksum-version', qr/unsupported PRE2 data checksum version/],
	['identity', qr/native bootstrap observation failed/],
	['root-race', qr/native bootstrap observation changed during configuration application/],
	['binding-race', qr/native bootstrap observation changed during configuration application/],
	['hook-error', qr/shared configuration startup callback failed/])
{
	my ($mutation, $expected) = @$case;
	is($node->safe_psql('postgres', "SELECT test_pgrac_bootstrap_fixture('$mutation')"), 't',
		"$mutation disposable fixture encoded by production codecs");
	$node->stop('fast');
	my $data = $node->data_dir;
	my $control_before = sha256_hex(slurp_file("$data/global/pg_control"));
	$node->append_conf('postgresql.conf', qq{
shared_preload_libraries='test_pgrac_shared_config'
test_pgrac_shared_config.prepare_bootstrap=on
});
	$log_offset = -s $node->logfile;
	ok(!$node->start(fail_ok => 1), "$mutation never admits the fixture as a database");
	my $log = substr(slurp_file($node->logfile), $log_offset);
	like($log, $expected, "$mutation reaches its exact native boundary");
	is(sha256_hex(slurp_file("$data/global/pg_control")), $control_before,
		"$mutation leaves compatibility control bytes unchanged");
	$node->append_conf('postgresql.conf', qq{
shared_preload_libraries=''
test_pgrac_shared_config.prepare_bootstrap=off
});
	ok($node->start, "$mutation native noncluster server still starts");
}
$node->stop('fast');
done_testing();
