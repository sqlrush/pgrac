# Real initdb creation and refusal boundaries; no synthetic pg_control or HW file.
use strict;
use warnings;
use Cwd qw(realpath);
use File::Path qw(make_path);
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);
my $root = "$temp/shared";
my $data = "$temp/data";
mkdir $root or die "mkdir $root: $!";

sub pair
{
	my ($dir, $owner) = @_;
	return ("--pgrac-hw-snapshot-root=$dir", "--pgrac-hw-snapshot-owner=$owner");
}

sub refused
{
	my ($name, $reason, @args) = @_;
	my $target = "$temp/rejected-$name";
	command_fails_like([ 'initdb', '-D', $target, @args ], qr/$reason/,
		$name);
	ok(!-e $target, "$name has no PGDATA side effects");
}

refused('missing-owner', 'HW_OPTIONS_PAIRED', "--pgrac-hw-snapshot-root=$root");
refused('missing-root', 'HW_OPTIONS_PAIRED', '--pgrac-hw-snapshot-owner=7');
for my $owner ('-1', '128', '7x', '18446744073709551616')
{
	refused("owner-$owner", 'HW_OWNER_INVALID', pair($root, $owner));
}
refused('duplicate-root', 'HW_OPTION_DUPLICATE', pair($root, 7),
	"--pgrac-hw-snapshot-root=$root");
refused('duplicate-owner', 'HW_OPTION_DUPLICATE', pair($root, 7),
	'--pgrac-hw-snapshot-owner=8');
refused('no-sync', 'HW_SYNC_REQUIRED', pair($root, 7), '--no-sync');
refused('sync-only', 'HW_SYNC_REQUIRED', pair($root, 7), '--sync-only');
refused('cluster-override', 'HW_UNSAFE_OVERRIDE', pair($root, 7),
	'-c', 'Cluster.Enabled=false');
refused('data-override', 'HW_UNSAFE_OVERRIDE', pair($root, 7),
	'-c', "data_directory=$data");
refused('config-override', 'HW_UNSAFE_OVERRIDE', pair($root, 7),
	'-c', "config_file=$temp/custom.conf");
refused('input-override', 'HW_UNSAFE_OVERRIDE', pair($root, 7), '-L', $temp);

for my $case (
	[ 'space-cluster', ' cluster.enabled=false' ],
	[ 'tab-cluster', "\tcluster.enabled=false" ],
	[ 'space-data', " data_directory=$temp/never-created-data" ],
	[ 'tab-config', "\tconfig_file=$temp/never-created.conf" ])
{
	my ($name, $guc) = @$case;
	my $separate_root = "$temp/$name-root";
	mkdir $separate_root or die "mkdir: $!";
	refused($name, 'HW_UNSAFE_OVERRIDE', pair($separate_root, 7), '-c', $guc);
}

my $alias = "$temp/alias";
symlink $root, $alias or die "symlink: $!";
refused('root-alias', 'HW_ROOT_CANONICAL', pair($alias, 7));
refused('dotdot-alias', 'HW_ROOT_CANONICAL', pair("$root/../shared", 7));
refused('root-wal-overlap', 'HW_ROOT_OVERLAP', pair($root, 7),
	'-X', "$root/wal");
command_fails_like([ 'initdb', '-D', "$root/data", pair($root, 7) ],
	qr/HW_ROOT_OVERLAP/, 'root must not contain PGDATA');
ok(!-e "$root/data", 'overlap rejection does not create PGDATA');
command_fails_like([ 'initdb', '-D', $root, pair($root, 7) ],
	qr/HW_ROOT_OVERLAP/, 'root must not equal PGDATA');

my $occupied = "$temp/occupied";
make_path("$occupied/global");
append_to_file("$occupied/global/retained", "old authority must survive\n");
refused('existing-global', 'HW_ROOT_NOT_EMPTY', pair($occupied, 7));
is(slurp_file("$occupied/global/retained"), "old authority must survive\n",
	'old shared data unchanged');
is_deeply([ glob("$root/*") ], [], 'all refused requests leave fresh root empty');

command_ok([ 'initdb', '-D', $data, '-A', 'trust', '--no-locale', pair($root, 7) ],
	'real bootstrap plus sync produces seed snapshot');
my $snapshot = "$root/global/pg_hw_snapshot.7";
ok(-f $snapshot, 'own snapshot exists after real initdb succeeds');
if (-f $snapshot)
{
	my $bytes = slurp_file($snapshot);
	is(length($bytes), 52, 'empty v1 image is exactly 52 bytes');
	my ($magic, $version, $sysid, $owner, $partition, $kind, $generation,
		$lsn, $entries, $reserved, $crc) = unpack('L L Q L L L L Q L L L', $bytes);
	is_deeply([$magic, $version, $owner, $partition, $kind, $generation,
		$entries, $reserved], [0x48575350, 1, 7, 7, 0, 0, 0, 0],
		'one self CHECKPOINT, no invented owner, entry, or generation');
	my $computed = 0xffffffff;
	for my $byte (unpack('C*', substr($bytes, 0, 48)))
	{
		$computed ^= $byte;
		for (1 .. 8)
		{
			$computed = ($computed >> 1) ^ (($computed & 1) ? 0x82f63b78 : 0);
		}
	}
	is($crc, $computed ^ 0xffffffff, 'real bytes have valid CRC32C');
	my ($control, $control_err);
	ok(IPC::Run::run([ 'pg_controldata', $data ], '>', \$control, '2>', \$control_err),
		'read final native control file');
	my ($native_sysid) = $control =~ /Database system identifier:\s*(\d+)/;
	my ($high, $low) = $control =~ /Latest checkpoint's REDO location:\s*([0-9A-F]+)\/([0-9A-F]+)/;
	is("$sysid", $native_sysid, 'identity comes from final actual control');
	ok(defined($high) && $lsn == hex($high) * 4294967296 + hex($low),
		'snapshot LSN matches actual final checkpoint REDO');
	like($control, qr/Database cluster state:\s*shut down\s*$/m,
		'producer has real clean bootstrap completion');
	is_deeply([ glob("$root/global/*") ], [$snapshot],
		'only seed owner snapshot, no other owners or temp files');

	command_fails([ 'initdb', '-D', $data, pair($root, 7) ],
		'repeated initdb cannot bless existing data');
	is(slurp_file($snapshot), $bytes, 'repeated initdb cannot overwrite snapshot');
	my $empty_root = "$temp/empty-retry-root";
	mkdir $empty_root or die "mkdir: $!";
	command_fails([ 'initdb', '-D', $data, pair($empty_root, 7) ],
		'existing PGDATA with empty root is not a new cluster');
	is_deeply([ glob("$empty_root/*") ], [],
		'cannot sign empty authority for existing PGDATA');
}

# The actual child and strict sync must consume canonical paths even when
# operator-facing PGDATA/WAL names pass through an existing parent alias.
my $parent_alias = "$temp/parent-alias";
my $external_root = "$temp/external-wal-root";
symlink $temp, $parent_alias or die "symlink: $!";
mkdir $external_root or die "mkdir: $!";
command_ok([
	'initdb', '-D', "$parent_alias/alias-data", '-X', "$parent_alias/alias-wal",
	'-A', 'trust', '--no-locale', pair($external_root, 0)
], 'actual new seed through PGDATA and WAL parent aliases');
is(realpath("$temp/alias-data/pg_wal"), "$temp/alias-wal",
	'new WAL link names the canonical created directory');
ok(-f "$external_root/global/pg_hw_snapshot.0",
	'actual external-WAL seed produces only its own snapshot');

done_testing();
