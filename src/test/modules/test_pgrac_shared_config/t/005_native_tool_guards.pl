# PGRAC: actual native tools must not mutate shared-control deployments.
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

# No real shared deployment is touched: all targets are disposable initdbs.
# Malformed markers deliberately cannot be mistaken for a decoded authority.
my $node = PostgreSQL::Test::Cluster->new('tool_guard');
my $other = PostgreSQL::Test::Cluster->new('tool_other');
$node->init;
$other->init;
my $data = $node->data_dir;
my $peer = $other->data_dir;
my $global = "$data/global";
my $guard = qr/PGRAC_SHARED_CONTROL|PGRAC_CONTROL_GUARD_UNKNOWN/;

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
	my ($cmd, $name, @dirs) = @_;
	my @before = map { snapshot($_) } @dirs;
	my ($out, $err) = ('', '');
	my $ok = IPC::Run::run($cmd, '>', \$out, '2>', \$err,
		IPC::Run::timeout(60));
	ok(!$ok, "$name refuses");
	like($out . $err, $guard, "$name reports shared-control boundary");
	for my $i (0 .. $#dirs)
	{
		is(snapshot($dirs[$i]), $before[$i], "$name leaves directory $i unchanged");
	}
}

my $binding = "$global/pgrac_control_binding";
write_bytes($binding, 'not a valid binding');
refuses_unchanged(['pg_resetwal', '-f', $data], 'forced reset with regular projection', $data);
for my $mode ('--enable', '--disable', '--check')
{
	refuses_unchanged(['pg_checksums', $mode, '-D', $data], "checksums $mode", $data);
}
refuses_unchanged(['pg_rewind', '-D', $data, '--source-pgdata', $peer],
	'rewind marked target', $data, $peer);
refuses_unchanged(['pg_rewind', '-D', $peer, '--source-pgdata', $data],
	'rewind marked local source', $data, $peer);
refuses_unchanged(['pg_rewind', '-D', $data,
	'--source-server=host=127.0.0.1 port=1 connect_timeout=1'],
	'rewind target guard precedes remote connection', $data);

# Deliberately nonexistent binary directories: protection must run even before
# setup attempts to create pg_upgrade_output or start either database.
for my $side ('old', 'new')
{
	my ($old, $new) = $side eq 'old' ? ($data, $peer) : ($peer, $data);
	refuses_unchanged(['pg_upgrade', '--check', '-d', $old, '-D', $new,
		'-b', "$data/absent-bin", '-B', "$peer/absent-bin"],
		"upgrade marked $side side", $data, $peer);
}
refuses_unchanged(['initdb', '-D', $data, '--no-sync'], 'initdb marked target', $data);
refuses_unchanged(['initdb', '-D', $data, '--sync-only'], 'initdb sync marked target', $data);
unlink($binding) or die "remove fixture binding: $!";

for my $marker ('pgrac_control_binding', 'pgrac_control_root', 'pgrac_control_root.bak')
{
	my $path = "$global/$marker";
	for my $contents ('', 'damaged or unsupported format')
	{
		write_bytes($path, $contents);
		refuses_unchanged(['pg_resetwal', '-f', $data],
			"reset with $marker length " . length($contents), $data);
		unlink($path) or die "remove fixture marker: $!";
	}
	mkdir($path, 0700) or die "mkdir fixture marker: $!";
	refuses_unchanged(['pg_resetwal', '-f', $data], "reset with directory $marker", $data);
	rmdir($path) or die "rmdir fixture marker: $!";
	SKIP:
	{
		skip 'symlink fixture unavailable on Windows', 3 if $windows_os;
		symlink('missing-marker-target', $path) or die "symlink fixture marker: $!";
		refuses_unchanged(['pg_resetwal', '-f', $data], "reset with dangling $marker", $data);
		unlink($path) or die "unlink fixture marker: $!";
	}
}

SKIP:
{
	skip 'POSIX directory and symlink fixtures', 12 if $windows_os;
	my $saved = "$data/saved-global";
	rename($global, $saved) or die "move fixture global: $!";
	write_bytes($global, 'not a directory');
	refuses_unchanged(['pg_resetwal', '-f', $data], 'uninspectable global', $data);
	unlink($global) or die "remove fixture global file: $!";
	rename($saved, $global) or die "restore fixture global: $!";
	# No content decode or FIFO open is needed to preserve the boundary.
	POSIX::mkfifo($binding, 0600) == 0 or die "create fixture FIFO: $!";
	refuses_unchanged(['pg_resetwal', '-f', $data], 'nonblocking FIFO marker refusal', $data);
	unlink($binding) or die "remove fixture FIFO: $!";
	rename($global, $saved) or die "move fixture global: $!";
	symlink('saved-global', $global) or die "link fixture global: $!";
	refuses_unchanged(['pg_resetwal', '-f', $data], 'indirect global is not absent', $data);
	unlink($global) or die "unlink fixture global: $!";
	rename($saved, $global) or die "restore fixture global: $!";
	my $control = "$global/pg_control";
	rename($control, "$global/control-target") or die "move fixture control: $!";
	symlink('control-target', $control) or die "link fixture control: $!";
	refuses_unchanged(['pg_checksums', '--enable', '-D', $data],
		'shared projection symlink without local marker', $data);
	unlink($control) or die "remove fixture control symlink: $!";
	rename("$global/control-target", $control) or die "restore fixture control: $!";
}

# Remote source enumeration must run before target auto-recovery or copying.
my $remote = PostgreSQL::Test::Cluster->new('tool_remote');
$remote->init;
$remote->start;
for my $marker ('pgrac_control_binding', 'pgrac_control_root', 'pgrac_control_root.bak')
{
	my $path = $remote->data_dir . "/global/$marker";
	write_bytes($path, 'damaged marker');
	refuses_unchanged(['pg_rewind', '-D', $peer, '--source-server', $remote->connstr('postgres')],
		"rewind remote source with $marker", $peer);
	unlink($path) or die "remove remote fixture: $!";
}
$remote->stop('fast');

# Ordinary unmarked PG remains supported, including actual native writes.
my $plain = PostgreSQL::Test::Cluster->new('tool_plain');
$plain->init;
command_ok(['pg_checksums', '--enable', '-D', $plain->data_dir], 'plain PG enables checksums');
command_ok(['pg_checksums', '--check', '-D', $plain->data_dir], 'plain PG checks checksums');
command_ok(['pg_checksums', '--disable', '-D', $plain->data_dir], 'plain PG disables checksums');
command_ok(['pg_resetwal', '-f', $plain->data_dir], 'plain PG resets WAL');
command_ok(['initdb', '--sync-only', '-D', $plain->data_dir], 'plain PG sync remains supported');
done_testing();
