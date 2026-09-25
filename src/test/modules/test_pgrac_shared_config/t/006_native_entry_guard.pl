# PGRAC: a local PRE2 binding cannot fall back to native server startup.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Digest::SHA;
use Fcntl qw(:mode);
use File::Find;
use IPC::Run;
use POSIX ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $guard = qr/PGRAC_CONTROL_BINDING_REQUIRED|PGRAC_CONTROL_BINDING_UNKNOWN/;

sub write_bytes
{
	my ($path, $bytes) = @_;
	open(my $fh, '>:raw', $path) or die "open fixture $path: $!";
	print {$fh} $bytes;
	close($fh) or die "close fixture $path: $!";
}

sub snapshot
{
	my ($root) = @_;
	my @entries;
	File::Find::find({no_chdir => 1, wanted => sub {
		my $path = $File::Find::name;
		my @st = lstat($path);
		die "stat fixture $path: $!" unless @st;
		my $entry = substr($path, length($root)) . ':' . $st[2];
		if (S_ISLNK($st[2])) { $entry .= ':' . readlink($path); }
		elsif (S_ISREG($st[2]))
		{
			open(my $fh, '<:raw', $path) or die "read fixture $path: $!";
			$entry .= ':' . Digest::SHA->new(256)->addfile($fh)->hexdigest;
			close($fh) or die "close fixture $path: $!";
		}
		push @entries, $entry;
	}}, $root);
	return Digest::SHA::sha256_hex(join("\n", sort @entries));
}

sub refuses_unchanged
{
	my ($cmd, $data, $name, $input) = @_;
	$input //= '';
	my $before = snapshot($data);
	my ($out, $err) = ('', '');
	my $ok = IPC::Run::run($cmd, '<', \$input, '>', \$out, '2>', \$err,
		IPC::Run::timeout(60));
	ok(!$ok, "$name refuses native entry");
	like($out . $err, $guard, "$name reports the binding boundary");
	is(snapshot($data), $before, "$name changes no data bytes or directory entries");
}

# Each entry gets its own disposable initialized database: the RED bootstrap
# can overwrite native WAL/control. Never reuse that fixture for startup.
for my $mode ('postmaster', '--single', '--boot', '--check', 'runtime-C')
{
	my $node = PostgreSQL::Test::Cluster->new("entry_$mode");
	$node->init;
	my $data = $node->data_dir;
	$node->append_conf('postgresql.conf', "cluster.shared_config = off\n")
		if $mode ne 'postmaster';
	write_bytes("$data/global/pgrac_control_binding", 'damaged binding');
	if ($mode eq 'postmaster')
	{
		my $before = snapshot($data);
		my $started = $node->start(fail_ok => 1);
		ok(!$started, 'default-off postmaster cannot ignore a PRE2 binding');
		$node->stop('fast') if $started;
		like(slurp_file($node->logfile), $guard, 'postmaster reports binding boundary');
		is(snapshot($data), $before, 'postmaster refusal precedes all data writes');
	}
	elsif ($mode eq '--single')
	{
		refuses_unchanged(['postgres', '--single', '-D', $data, 'postgres'],
			$data, 'single user with explicit off', "SELECT 1;\n");
	}
	elsif ($mode eq 'runtime-C')
	{
		refuses_unchanged(['postgres', '-D', $data, '-C', 'shared_memory_size'],
			$data, 'runtime sizing with explicit off');
	}
	else
	{
		refuses_unchanged(['postgres', $mode, '-D', $data], $data, "$mode with explicit off");
	}
}

my $node = PostgreSQL::Test::Cluster->new('entry_shapes');
$node->init;
my $data = $node->data_dir;
my $global = "$data/global";
my $binding = "$global/pgrac_control_binding";
my $check = ['postgres', '--check', '-D', $data];
for my $bytes ('', "PGCB\0\0unsupported-or-corrupt")
{
	write_bytes($binding, $bytes);
	refuses_unchanged($check, $data, 'binding length ' . length($bytes));
	unlink($binding) or die "unlink fixture binding: $!";
}
mkdir($binding, 0700) or die "mkdir fixture binding: $!";
refuses_unchanged($check, $data, 'directory binding');
rmdir($binding) or die "rmdir fixture binding: $!";

SKIP:
{
	skip 'POSIX link/FIFO fixtures', 18 if $windows_os;
	symlink('missing-marker', $binding) or die "symlink fixture binding: $!";
	refuses_unchanged($check, $data, 'dangling binding');
	unlink($binding) or die "unlink fixture link: $!";
	POSIX::mkfifo($binding, 0600) == 0 or die "mkfifo fixture binding: $!";
	refuses_unchanged($check, $data, 'FIFO binding must not block on read');
	unlink($binding) or die "unlink fixture FIFO: $!";
	rename($global, "$data/saved-global") or die "move fixture global: $!";
	write_bytes($global, 'not a directory');
	refuses_unchanged($check, $data, 'uninspectable binding parent');
	unlink($global) or die "unlink fixture global: $!";
	symlink('missing-global', $global) or die "link missing global: $!";
	refuses_unchanged($check, $data, 'dangling parent is not binding absence');
	unlink($global) or die "unlink missing global: $!";
	symlink('global', $global) or die "link looping global: $!";
	refuses_unchanged($check, $data, 'looping parent is not binding absence');
	unlink($global) or die "unlink looping global: $!";
	symlink('saved-global', $global) or die "link fixture global: $!";
	write_bytes($binding, 'binding behind global link');
	refuses_unchanged($check, $data, 'binding through parent link');
	unlink($binding) or die "unlink fixture binding: $!";
	unlink($global) or die "unlink fixture global link: $!";
	rename("$data/saved-global", $global) or die "restore fixture global: $!";
}

# Marker absence keeps native initdb/check/standalone/postmaster behavior.
command_ok($check, 'unmarked native check still works');
my ($out, $err, $input) = ('', '', "SELECT 8127;\n");
ok(IPC::Run::run(['postgres', '--single', '-D', $data, 'postgres'],
	'<', \$input, '>', \$out, '2>', \$err), 'unmarked single user starts');
like($out, qr/8127/, 'unmarked single user executes real SQL');
ok($node->start, 'unmarked postmaster starts');
is($node->safe_psql('postgres', 'SELECT 8291'), '8291', 'unmarked postmaster serves SQL');
$node->stop('fast');

# The backend guard is narrower than the destructive frontend guard. Existing
# shared-control symlinks and legacy root names alone are not a PRE2 binding.
SKIP:
{
	skip 'POSIX compatibility symlink', 2 if $windows_os;
	rename("$global/pg_control", "$global/native-control") or die "move control: $!";
	symlink('native-control', "$global/pg_control") or die "link control: $!";
	write_bytes("$global/pgrac_control_root", 'legacy name, not a PRE2 binding');
	write_bytes("$global/pgrac_control_root.bak", 'legacy backup name');
	ok($node->start, 'unmarked compatibility control symlink is not rejected');
	is($node->safe_psql('postgres', 'SELECT 8311'), '8311', 'native compatibility entry serves SQL');
	$node->stop('fast');
}

done_testing();
