# The original creator makes a new typed shared DATA base, without ROOT admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(realpath);
use File::Copy qw(copy);
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);
my $data = "$temp/data";
my $wal = "$temp/wal";
my $shared = "$temp/shared";
my @namespace = ('--pgrac-initdb-storage-uuid=0123456789abcdef0123456789abcdef',
	'--pgrac-initdb-database-incarnation=1');

sub rejected
{
	my ($name, $base, @options) = @_;
	command_fails_like(['initdb', '-D', "$temp/$name-data", '-X', "$temp/$name-wal",
		'-k', "--pgrac-initdb-shared-base=$base", @options], qr/INITDB_BASE_/,
		"$name refuses");
	ok(!-e "$temp/$name-data" && !-e "$temp/$name-wal",
		"$name refuses before creating native files");
}

rejected('no-thread', "$temp/no-thread", @namespace);
rejected('wrong-thread', "$temp/wrong-thread", '--pgrac-initdb-thread=2', @namespace);
rejected('incomplete-namespace', "$temp/incomplete", '--pgrac-initdb-thread=1');
rejected('bad-uuid', "$temp/bad-uuid", '--pgrac-initdb-thread=1',
	'--pgrac-initdb-storage-uuid=00000000000000000000000000000000',
	'--pgrac-initdb-database-incarnation=1');
mkdir "$temp/occupied" or die "mkdir: $!";
append_to_file("$temp/occupied/keep", 'unchanged');
rejected('occupied', "$temp/occupied", '--pgrac-initdb-thread=1', @namespace);
is(slurp_file("$temp/occupied/keep"), 'unchanged', 'occupied DATA is unchanged');
mkdir "$temp/empty" or die "mkdir: $!";
symlink "$temp/empty", "$temp/alias" or die "symlink: $!";
rejected('alias', "$temp/alias", '--pgrac-initdb-thread=1', @namespace);
rejected('overlap', "$temp/overlap-data", '--pgrac-initdb-thread=1', @namespace);

my ($creation, $creation_error);
ok(IPC::Run::run(['initdb', '-D', $data, '-X', $wal, '-k', '-A', 'trust', '--no-locale',
	'--pgrac-initdb-thread=1', "--pgrac-initdb-shared-base=$shared", @namespace],
	'>', \$creation, '2>', \$creation_error),
	'original creator logs and persists its new shared base') or diag $creation_error;
like($creation, qr/shared startup authority is not published/,
	'creation result does not claim shared startup is ready');
BAIL_OUT('native creator did not produce its targets') unless -d "$shared/global" && -d "$data/base";

my ($control, $stderr);
ok(IPC::Run::run(['pg_controldata', $data], '>', \$control, '2>', \$stderr),
	'original checkpoint is readable');
my ($sysid) = $control =~ /Database system identifier:\s+(\d+)/;
my ($checkpoint) = $control =~ /Latest checkpoint location:\s+([0-9A-F]+\/[0-9A-F]+)/;
like($control, qr/Database cluster state:\s+shut down/, 'original writer shut down');
my ($files, $pages, $wrong, $ordinary, $space) = (0, 0, 0, 0, 0);
for my $dir ("$data/global", sort grep { -d $_ && /\/\d+$/ } glob("$data/base/*"))
{
	(my $rel = $dir) =~ s/^\Q$data\E\///;
	my %relations;
	for my $source (sort glob("$dir/*"))
	{
		next unless $source =~ /\/([1-9][0-9]*)(?:_(fsm|vm))?(?:\.([1-9][0-9]*))?$/;
		my ($number, $fork, $segment) = ($1, $2 // '', $3 // 0);
		(my $name = $source) =~ s/.*\///;
		$relations{$number} += $fork eq '' ? (-s $source) / 8192 : 0;
		$files++;
		my $original = slurp_file($source);
		my $result = slurp_file("$shared/$rel/$name");
		$wrong++ if length($original) != length($result) || length($result) % 8192;
		for (my $off = 0; $off < length($original); $off += 8192)
		{
			my $before = substr($original, $off, 8192);
			my $after = substr($result, $off, 8192);
			$pages++;
			$wrong++ if substr($before, 12, 12) ne substr($after, 12, 12)
				|| substr($before, 32) ne substr($after, 32);
			if ($fork ne 'fsm')
			{
				$ordinary++;
				$wrong++ unless unpack('Q', substr($after, 24, 8)) > 0
					&& (unpack('S', substr($after, 10, 2)) & 0x7c0) == 0x40;
			}
		}
	}
	for my $number (sort keys %relations)
	{
		my $bytes = slurp_file("$shared/$rel/${number}_space");
		$space++;
		$wrong++ unless length($bytes) == 2 * 8192
			&& substr($bytes, 32, 4) eq 'PSI1'
			&& substr($bytes, 8192 + 32, 4) eq 'PSR1'
			&& unpack('Q<', substr($bytes, 40, 8)) eq $sysid
			&& unpack('Q<', substr($bytes, 48, 8)) == 1
			&& unpack('H*', substr($bytes, 56, 16)) eq '0123456789abcdef0123456789abcdef'
			&& unpack('L<', substr($bytes, 8192 + 48, 4)) == $relations{$number}
			&& substr($bytes, 32, 128) eq substr($bytes, 8192 + 56, 128);
	}
}
ok($files > 100 && $ordinary > 100 && $space > 100, 'real catalog corpus was copied');
is($wrong, 0, 'payloads preserved, all ordinary pages versioned, exact SPACE namespace/HWM');
ok(!-e "$shared/global/pgrac_control_root" && !-e "$data/global/pgrac_control_binding",
	'base creation grants no ROOT or local startup binding');
command_ok(['pg_checksums', '--check', '-D', $data], 'original native DATA checksums remain valid');
# Only the isolated verifier tree receives a compatibility control copy.
# It is not a product authority or input to any startup.
copy("$data/global/pg_control", "$shared/global/pg_control") or die "copy verifier control: $!";
mkdir "$shared/pg_tblspc" or die "create verifier tablespace directory: $!";
command_ok(['pg_checksums', '--check', '-D', $shared], 'actual shared DATA and SPACE checksums verify');
unlink "$shared/global/pg_control" or die "remove verifier control: $!";
rmdir "$shared/pg_tblspc" or die "remove verifier tablespace directory: $!";
my ($records, $err);
ok(IPC::Run::run(['pg_waldump', '-p', $wal, '-s', '0/1000000', '-e', $checkpoint,
	'-r', 'Storage'], '>', \$records, '2>', \$err),
	'native WAL decoder accepts original typed creation records') or diag $err;
my $creates = () = $records =~ /SPACE_IDENTITY/g;
my $advances = () = $records =~ /SPACE_RESERVATION/g;
is($creates, $space, 'every relation has one actual typed CREATE/INIT record');
ok($advances > 0, 'nonempty relations have native reservation WAL');
unlike($records, qr/invalid SPACE/, 'all typed SPACE records pass production codecs');

done_testing();
