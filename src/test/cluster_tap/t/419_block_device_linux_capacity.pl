#-------------------------------------------------------------------------
#
# 419_block_device_linux_capacity.pl
#	  Linux real-block capacity and O_DIRECT startup gate for M5.
#
# The caller must provide a disposable, zeroed block device through
# PGRAC_TEST_BLOCK_DEVICE.  A regular file is rejected: this test exists to
# prove the S_ISBLK -> BLKGETSIZE64 production path and direct-I/O startup.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use IPC::Run ();
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

BAIL_OUT("BLOCKED: " . 'Linux-only real block-device test')
  unless $^O eq 'linux';

my $device = $ENV{PGRAC_TEST_BLOCK_DEVICE};
BAIL_OUT("BLOCKED: " . 'set PGRAC_TEST_BLOCK_DEVICE to a disposable block device')
  unless defined($device) && length($device) > 0;
BAIL_OUT("BLOCKED: " . "$device is not a block device")
  unless -b $device;

my ($capacity_out, $capacity_err) = ('', '');
ok(IPC::Run::run(['blockdev', '--getsize64', $device], '>', \$capacity_out,
		'2>', \$capacity_err),
	'runner preflight reads real block-device capacity');
chomp($capacity_out);
cmp_ok($capacity_out, '>=', 8 * 1024 * 1024,
	'runner preflight reports at least 8 MiB');

(my $device_conf = $device) =~ s/'/''/g;
my $node = PgracClusterNode->new('m5_linux_block_capacity');
$node->init;
$node->append_conf(
	'postgresql.conf',
	"cluster.shared_storage_backend = block_device\n"
	  . "cluster.block_device_path = '$device_conf'\n"
	  . "cluster.block_device_use_odirect = on\n"
	  . "cluster.storage_fence_driver = disabled\n"
	  . "cluster.smgr_user_relations = on\n");

$node->start;

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'active_backend'}),
	'block_device',
	'postmaster attaches the real block_device backend');
is($node->safe_psql('postgres', q{SHOW "cluster.block_device_use_odirect"}),
	'on',
	'real block-device startup keeps O_DIRECT enabled');

$node->safe_psql('postgres', q{
	CREATE TABLE m5_block_probe (id int PRIMARY KEY, payload text);
	INSERT INTO m5_block_probe VALUES (1, 'blkgetsize64');
});
is($node->safe_psql('postgres',
		q{SELECT id || ':' || payload FROM m5_block_probe}),
	'1:blkgetsize64',
	'user relation round-trips after real block-device startup');

$node->stop;

done_testing();
