#-------------------------------------------------------------------------
#
# 332_block_device_backend.pl
#	  spec-6.0a block_device backend end-to-end smoke.
#
#	  Exercises the raw block_device ClusterSharedFs provider through a
#	  running postmaster using a regular-file raw image.  O_DIRECT is disabled
#	  for this CI leg so the test is portable across GitHub Linux runners; the
#	  coverage target is backend activation, raw layout namespace separation,
#	  logical EOF, checkpoint barrier, and crash-restart replay plumbing.  The
#	  O_DIRECT/PR hardware legs remain external/manual per spec-6.0a.
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/332_block_device_backend.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Cwd qw(abs_path);
use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

sub make_raw_image
{
	my ($path, $size_mb) = @_;

	open(my $fh, '>', $path) or die "open $path: $!";
	truncate($fh, $size_mb * 1024 * 1024)
	  or die "truncate $path: $!";
	close($fh) or die "close $path: $!";
}

my $node = PgracClusterNode->new('spec6_0a_block_device');
$node->init;

my $raw_image = abs_path($node->data_dir) . '/spec6_0a_raw_device.img';
make_raw_image($raw_image, 96);

(my $raw_image_conf = $raw_image) =~ s/'/''/g;
$node->append_conf(
	'postgresql.conf',
	"cluster.shared_storage_backend = block_device\n"
	  . "cluster.block_device_path = '$raw_image_conf'\n"
	  . "cluster.block_device_use_odirect = off\n"
	  . "cluster.smgr_user_relations = on\n");

$node->start;

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'active_backend'}),
	'block_device',
	'L1 active shared-storage backend is block_device');

$node->safe_psql('postgres', q{
	CREATE TABLE bd_a (id int PRIMARY KEY, payload text);
	CREATE TABLE bd_b (id int PRIMARY KEY, payload text);
	INSERT INTO bd_a SELECT g, 'a-' || repeat('x', 80) || '-' || g
	  FROM generate_series(1, 600) g;
	INSERT INTO bd_b SELECT g, 'b-' || repeat('y', 80) || '-' || g
	  FROM generate_series(1, 600) g;
});

is($node->safe_psql('postgres', 'SELECT count(*), min(left(payload, 2)) FROM bd_a'),
	'600|a-', 'L2 table A rows round-trip through raw block_device');
is($node->safe_psql('postgres', 'SELECT count(*), min(left(payload, 2)) FROM bd_b'),
	'600|b-', 'L2 table B rows round-trip through a distinct raw extent map');

ok($node->safe_psql(
	'postgres',
	"SELECT count(*) FROM bd_a \\g /dev/null\n"
	  . q{SELECT value::int > 0 FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'smgr_active_relations'})
  eq 't',
   'L3 block_device user relation is open in cluster_smgr state');

$node->safe_psql('postgres', q{
	CHECKPOINT;
});
$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT sum(id), min(left(payload, 2)) FROM bd_a'),
	'180300|a-',
	'L4 table A survives checkpoint plus immediate stop/start on block_device');
is($node->safe_psql('postgres', 'SELECT sum(id), min(left(payload, 2)) FROM bd_b'),
	'180300|b-',
	'L4 table B survives checkpoint plus immediate stop/start on block_device');

$node->safe_psql('postgres', q{
	TRUNCATE bd_b;
	CHECKPOINT;
});
$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT count(*) FROM bd_b'),
	'0',
	'L5 truncate state survives checkpoint plus immediate stop/start');

$node->safe_psql('postgres', q{
	DROP TABLE bd_b;
	CREATE TABLE bd_b (id int PRIMARY KEY, payload text);
	INSERT INTO bd_b VALUES (1, 'fresh');
	CHECKPOINT;
});
$node->stop('immediate');
$node->start;

is($node->safe_psql('postgres', 'SELECT id, payload FROM bd_b'),
	'1|fresh',
	'L6 drop/recreate observes the fresh raw layout mapping after restart');

$node->stop;

done_testing();
