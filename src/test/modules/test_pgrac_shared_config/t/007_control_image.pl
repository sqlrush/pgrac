# PGRAC: native validation of an already-selected image, without control I/O.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('control_image');
$node->init;
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
is($node->safe_psql('postgres', "SELECT test_pgrac_control_image('valid', 0)"),
	't', 'real initialized control image is compatible without changing native state');
for my $size (1048576, 16777216, 1073741824)
{
	is($node->safe_psql('postgres', "SELECT test_pgrac_control_image('walsize', $size)"),
		't', "valid $size byte segment does not change native WAL geometry or GUCs");
}
my @bad = (
	['version', 0, qr/PG_CONTROL_VERSION/],
	['endian', 0, qr/mismatched byte ordering/],
	['crc', 0, qr/incorrect checksum in control file/],
	['catalog', 0, qr/CATALOG_VERSION_NO/],
	['align', 0, qr/MAXALIGN/],
	['float', 0, qr/floating-point number format/],
	['block', 0, qr/BLCKSZ/],
	['relseg', 0, qr/RELSEG_SIZE/],
	['walblock', 0, qr/XLOG_BLCKSZ/],
	['name', 0, qr/NAMEDATALEN/],
	['keys', 0, qr/INDEX_MAX_KEYS/],
	['toast', 0, qr/TOAST_MAX_CHUNK_SIZE/],
	['lob', 0, qr/LOBLKSIZE/],
	['float8', 0, qr/USE_FLOAT8_BYVAL/],
	['null', 0, qr/control image is required/]);
push @bad, map { ['walsize', $_, qr/WAL segment size must be a power of two/] }
	(0, 1, 524288, 3145728, 2147483648, 4294967295);
for my $case (@bad)
{
	my ($field, $value, $reason) = @$case;
	my ($rc, $stdout, $stderr) = $node->psql('postgres',
		"SELECT test_pgrac_control_image('$field', $value)");
	ok($rc != 0, "$field=$value is refused by native validation");
	like($stderr, $reason, "$field=$value reports the precise native mismatch");
}
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'invalid caller-owned images do not damage the running instance');
$node->stop('fast');
ok($node->start, 'native file reader still initializes geometry after common validation');
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'normal restart remains usable');
$node->stop('fast');
done_testing();
