# Original initdb writers produce their own WAL, before any shared admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(realpath);
use Digest::SHA qw(sha256_hex);
use Fcntl qw(F_SETFD);
use POSIX ();
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);

sub rejected
{
	my ($name, $reason, @args) = @_;
	my $data = "$temp/rejected-$name";
	my $wal = "$temp/wal-$name";
	command_fails_like(['initdb', '-D', $data, '-X', $wal, '-k', @args],
		qr/$reason/, $name);
	ok(!-e $data && !-e $wal, "$name refuses before creating directories");
}

for my $thread ('0', '129', '-1', '7x', '65537')
{
	rejected("thread-$thread", 'INITDB_WAL_OPTION', "--pgrac-initdb-thread=$thread");
}
rejected('no-sync', 'INITDB_WAL_SYNC', '--pgrac-initdb-thread=7', '-N');
rejected('sync-only', 'INITDB_WAL_SYNC', '--pgrac-initdb-thread=7', '-S');
{
	local $ENV{PGRAC_INITDB_WAL_CONTEXT_FD} = '123456';
	rejected('inherited-context', 'INITDB_WAL_CONTEXT', '--pgrac-initdb-thread=7');
}
rejected('sysid-without-thread', 'INITDB_WAL_OPTION',
	'--pgrac-initdb-system-identifier=123456');
rejected('duplicate-thread', 'INITDB_WAL_OPTION',
	'--pgrac-initdb-thread=7', '--pgrac-initdb-thread=8');
rejected('duplicate-sysid', 'INITDB_WAL_OPTION', '--pgrac-initdb-thread=7',
	'--pgrac-initdb-system-identifier=7', '--pgrac-initdb-system-identifier=8');
for my $id ('0', '-1', '123x', '18446744073709551616')
{
	rejected("sysid-$id", 'INITDB_WAL_OPTION', '--pgrac-initdb-thread=7',
		"--pgrac-initdb-system-identifier=$id");
}
for my $setting ('cluster.enabled=on', 'shared_preload_libraries=bad',
	"data_directory=$temp/foreign")
{
	rejected('override-' . (split /=/, $setting)[0], 'INITDB_WAL_OVERRIDE',
		'--pgrac-initdb-thread=7', '-c', $setting);
}
for my $args (
	['no-checksums', 'INITDB_WAL_SYNC', '-X', "$temp/no-checksums-wal"],
	['no-waldir', 'INITDB_WAL_OPTION', '-k'])
{
	my ($name, $reason, @options) = @$args;
	command_fails_like(['initdb', '-D', "$temp/$name", '--pgrac-initdb-thread=7', @options],
		qr/$reason/, $name);
	ok(!-e "$temp/$name" && !-e "$temp/$name-wal", "$name creates neither directory");
}
mkdir "$temp/occupied" or die "mkdir: $!";
append_to_file("$temp/occupied/keep", 'unchanged');
for my $occupied ('data', 'wal')
{
	my ($data, $wal) = $occupied eq 'data'
		? ("$temp/occupied", "$temp/unused-wal") : ("$temp/unused-data", "$temp/occupied");
	command_fails_like(['initdb', '-D', $data, '-X', $wal, '-k', '--pgrac-initdb-thread=7'],
		qr/INITDB_WAL_PATH/, "occupied $occupied refuses before changing either target");
}
is(slurp_file("$temp/occupied/keep"), 'unchanged', 'occupied target is preserved');
ok(!-e "$temp/unused-data" && !-e "$temp/unused-wal", 'other target remains absent');
mkdir "$temp/empty" or die "mkdir: $!";
symlink "$temp/empty", "$temp/alias" or die "symlink: $!";
command_fails_like(['initdb', '-D', "$temp/alias", '-X', "$temp/alias-wal", '-k',
	'--pgrac-initdb-thread=7'], qr/INITDB_WAL_PATH/, 'data directory alias refuses');
ok(!-e "$temp/alias-wal", 'alias rejection leaves WAL absent');
command_fails_like(['initdb', '-D', "$temp/missing/child", '-X', "$temp/missing-wal", '-k',
	'--pgrac-initdb-thread=7'], qr/INITDB_WAL_PATH/, 'uncreated parent refuses');
ok(!-e "$temp/missing" && !-e "$temp/missing-wal", 'parent refusal creates neither target');

sub control
{
	my ($data) = @_;
	my ($out, $err);
	my $ok = IPC::Run::run ['pg_controldata', $data], '>', \$out, '2>', \$err;
	ok($ok, 'actual native control is readable');
	like($out, qr/Database cluster state:\s+shut down/, 'actual initializer shut down');
	my ($id) = $out =~ /Database system identifier:\s+(\d+)/;
	my ($checkpoint) = $out =~ /Latest checkpoint location:\s+([0-9A-F]+\/[0-9A-F]+)/;
	ok(defined $id && defined $checkpoint, 'native identity and checkpoint exist');
	return ($id, $checkpoint);
}

sub wal_identity
{
	my ($wal, $thread, $id, $checkpoint) = @_;
	my $pages = 0;
	my $wrong = 0;
	my $long_headers = 0;
	for my $file (sort grep { /\/[0-9A-F]{24}$/ } glob("$wal/*"))
	{
		open my $fh, '<:raw', $file or die "open $file: $!";
		while (read($fh, my $page, 8192) == 8192)
		{
			next if $page eq "\0" x 8192;
			my ($magic, $flags) = unpack('SS', $page);
			my ($actual, $reserved) = unpack('SS', substr($page, 20, 4));
			$wrong++ if $actual != $thread || $reserved != 0;
			if ($flags & 2)
			{
				$long_headers++;
				$wrong++ if unpack('Q', substr($page, 24, 8)) ne $id;
			}
			$pages++;
		}
		close $fh or die "close $file: $!";
	}
	ok($pages > 1 && $long_headers > 0, 'scan real first and subsequent WAL pages');
	is($wrong, 0, "all generated WAL pages belong to thread $thread and the native sysid");
	command_like(['pg_waldump', '-p', $wal, '-s', $checkpoint, '-n', '1'],
		qr/CHECKPOINT_SHUTDOWN/, 'last checkpoint is an actual CRC-checked native record');
}

my $data0 = "$temp/first";
my $wal0 = "$temp/first-wal";
command_ok(['initdb', '-D', $data0, '-X', $wal0, '-A', 'trust', '-k', '--no-locale',
	'--pgrac-initdb-thread=7'], 'original creator writes its first native thread');
my ($id, $cp) = control($data0);
wal_identity($wal0, 7, $id, $cp);
ok(!-e "$data0/global/pgrac_control_binding", 'native preparation does not manufacture PGCB');
ok(!-e "$data0/global/pgrac_control_root", 'native preparation does not grant ROOT authority');

my $data1 = "$temp/second";
my $wal1 = "$temp/second-wal";
command_ok(['initdb', '-D', $data1, '-X', $wal1, '-A', 'trust', '-k', '--no-locale',
	'--pgrac-initdb-thread=128', "--pgrac-initdb-system-identifier=$id"],
	'a second original writer uses the common database identity without copying WAL');
my ($id1, $cp1) = control($data1);
is($id1, $id, 'native system identifiers agree');
wal_identity($wal1, 128, $id1, $cp1);

my $before = sha256_hex(slurp_file("$data0/global/pg_control"));
sub wal_digest
{
	my $digest = Digest::SHA->new(256);
	for my $file (sort grep { /\/[0-9A-F]{24}$/ } glob("$wal0/*"))
	{
		open my $fh, '<:raw', $file or die "open WAL: $!";
		$digest->addfile($fh);
		close $fh or die "close WAL: $!";
	}
	return $digest->hexdigest;
}
my $wal_before = wal_digest();
command_fails(['initdb', '-D', $data0, '-X', $wal0, '-k',
	'--pgrac-initdb-thread=8', "--pgrac-initdb-system-identifier=$id"],
	'existing data cannot be provisioned as another native writer');
is(sha256_hex(slurp_file("$data0/global/pg_control")), $before,
	'old native control is unchanged');

{
	local $ENV{PGRAC_INITDB_WAL_CONTEXT_FD} = '123456';
	command_fails_like(['postgres', '--single', '-D', $data0, 'template1'],
		qr/INITDB_WAL_CONTEXT/, 'a stale inherited context refuses before standalone WAL writes');
}
is(sha256_hex(slurp_file("$data0/global/pg_control")), $before,
	'failed context cannot rewrite the existing native checkpoint');

# Exercise the real pre-write consumer, including a structurally valid carrier
# that tries to rerun post-bootstrap on a completed database.  No server starts.
sub refused_context
{
	my ($name, $payload, $bootstrap) = @_;
	pipe my $reader, my $writer or die "pipe: $!";
	syswrite($writer, $payload) == length($payload) or die "write context: $!";
	close $writer or die "close context: $!";
	my $pid = fork();
	die "fork: $!" unless defined $pid;
	if ($pid == 0)
	{
		open STDIN, '<', '/dev/null' or POSIX::_exit(126);
		open STDOUT, '>', "$temp/context-$name.out" or POSIX::_exit(126);
		open STDERR, '>', "$temp/context-$name.err" or POSIX::_exit(126);
		fcntl($reader, F_SETFD, 0) or POSIX::_exit(126);
		$ENV{PGRAC_INITDB_WAL_CONTEXT_FD} = fileno($reader);
		my @command = $bootstrap
			? ('postgres', '--boot', '-D', $data0)
			: ('postgres', '--single', '-D', $data0, 'template1');
		exec(@command) or POSIX::_exit(126);
	}
	close $reader or die "close reader: $!";
	waitpid($pid, 0) == $pid or die "waitpid: $!";
	ok($? != 0, "$name context refuses");
	like(slurp_file("$temp/context-$name.err"), qr/INITDB_WAL_CONTEXT/, "$name reason is explicit");
	is(sha256_hex(slurp_file("$data0/global/pg_control")), $before,
		"$name context cannot change native control");
}
my @data_stat = stat($data0);
my @wal_stat = stat($wal0);
my @context = (0x50474957, 7, 2, $id, @data_stat[0,1], @wal_stat[0,1]);
my $context = pack('LSSQQQQQ', @context);
is(length($context), 48, 'native carrier layout is exact');
refused_context('short', substr($context, 0, 47));
refused_context('trailing', $context . 'x');
$context[2] = 1;
refused_context('wrong-phase', pack('LSSQQQQQ', @context));
refused_context('existing-bootstrap', pack('LSSQQQQQ', @context), 1);
$context[2] = 2;
$context[5]++;
refused_context('wrong-directory', pack('LSSQQQQQ', @context));
refused_context('completed-database', $context);
is(wal_digest(), $wal_before, 'all refused contexts preserve existing WAL bytes');

my $ordinary = "$temp/ordinary";
command_ok(['initdb', '-D', $ordinary, '-A', 'trust', '--no-locale'],
	'ordinary initdb remains available');
my ($native_id, $native_cp) = control($ordinary);
wal_identity("$ordinary/pg_wal", 0, $native_id, $native_cp);

done_testing();
