# Original fixed-member creation owns every new native writer.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(realpath);
use Digest::SHA qw(sha256_hex);
use File::Copy qw(copy);
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);
my $sysid = '7584383251700000001';
my $storage = '0123456789abcdef0123456789abcdef';

sub request
{
	my ($name, $mask) = @_;
	my %values = (
		'cluster.controlfile_shared_authority' => 'on',
		'cluster.enabled' => 'on', 'cluster.merged_recovery' => 'on',
		'cluster.shared_catalog' => 'on', 'cluster.shared_config' => 'on',
		'cluster.shared_data_dir' => "$temp/$name-data",
		'cluster.shared_storage_backend' => 'cluster_fs',
		'cluster.shared_storage_uuid' => '01234567-89ab-cdef-0123-456789abcdef',
		'cluster.smgr_user_relations' => 'on',
		'cluster.undo_tablespace_path' => "$temp/$name-undo",
		'cluster.wal_threads_dir' => "$temp/$name-wal");
	my $text = "\@authority_uuid=123456789abc4ef0923456789abcdef0\n"
		. sprintf("\@configured_0=%016x\n\@configured_1=0000000000000000\n", $mask)
		. "\@database_incarnation=1\n\@format=1\n\@generation=1\n"
		. "\@storage_uuid=$storage\n\@system_identifier=$sysid\n";
	for my $key (sort keys %values)
	{
		(my $value = $values{$key}) =~ s/'/''/g;
		$text .= "common.$key='$value'\n";
	}
	$text .= sprintf("node%03d.cluster.node_id='%d'\n", $_, $_)
		for grep { $mask & (1 << $_) } 0 .. 7;
	return $text;
}

sub options
{
	my ($name) = @_;
	return ('initdb', '-D', "$temp/$name-caches", '-k', '-A', 'trust', '--no-locale',
		'--pgrac-initdb-cohort', "--pgrac-initdb-shared-config=$temp/$name.conf");
}

sub input
{
	my ($name, $bytes) = @_;
	append_to_file("$temp/$name.conf", $bytes);
	chmod 0600, "$temp/$name.conf" or die "chmod: $!";
}

input('valid', request('valid', 15));
my ($out, $err);
ok(IPC::Run::run([options('valid')], '>', \$out, '2>', \$err),
	'one original creator prepares all four native origins') or diag $err;
BAIL_OUT('cohort not created') unless -f "$temp/valid-caches/node_3/global/pg_control";
like($out, qr/shared startup authority is not published/,
	'preparation does not claim shared startup or OPEN');
ok(!-e "$temp/valid-data/global/pgrac_control_root", 'no premature ROOT');
my %generations;
my $founder_scn;
for my $node (0 .. 3)
{
	my $data = "$temp/valid-caches/node_$node";
	my $thread = $node + 1;
	my $wal = readlink "$data/pg_wal";
	like($wal, qr{^\Q$temp/valid-wal/thread_$thread/\Egeneration_[1-9][0-9]*$},
		'origin has its own canonical generation');
	$generations{$wal}++;
	my ($ctl, $stderr);
	ok(IPC::Run::run(['pg_controldata', $data], '>', \$ctl, '2>', \$stderr),
		'actual native control reads');
	like($ctl, qr/Database system identifier:\s+$sysid\b/, 'common native system identity');
	like($ctl, qr/Database cluster state:\s+shut down/, 'actual native child exited');
	my ($checkpoint) = $ctl =~ /Latest checkpoint location:\s+([0-9A-F]+\/[0-9A-F]+)/;
	my $record;
	ok(IPC::Run::run(['pg_waldump', '-p', $wal, '-s', $checkpoint, '-n', '1'],
		'>', \$record, '2>', \$stderr), 'actual final checkpoint decodes') or diag $stderr;
	like($record, qr/CHECKPOINT_SHUTDOWN/, 'record is the actual final shutdown checkpoint');
	my ($scn) = $record =~ /scn: ([0-9]+),/;
	if ($node == 0)
	{
		$founder_scn = $scn;
		ok(defined($scn) && $scn > 0 && $scn < 2**56,
			'founder checkpoint retains its real shared-base SCN');
	}
	else
	{
		is($scn, 0, 'peer without shared-base allocation retains a true zero SCN');
	}
	my ($pages, $wrong) = (0, 0);
	for my $file (grep { /\/[0-9A-F]{24}$/ } glob("$wal/*"))
	{
		open my $fh, '<:raw', $file or die "open WAL: $!";
		while (read($fh, my $page, 8192) == 8192)
		{
			next if $page eq "\0" x 8192;
			$pages++;
			$wrong++ if unpack('S', substr($page, 20, 2)) != $thread;
		}
		close $fh or die "close WAL: $!";
	}
	ok($pages > 0 && !$wrong, "all origin $node WAL pages were generated with its thread");
	ok(!-e "$data/global/pgrac_control_binding", 'no PGCB before complete ROOT publication');
	for my $family ('pg_xact', 'pg_subtrans', 'pg_multixact/offsets', 'pg_multixact/members', 'pg_commit_ts')
	{
		my $target = "$temp/valid-data/native_side/origin_$node/$family";
		ok(-d $target, "origin $node has its native $family directory");
		my @source = sort map { s{.*/}{}r } glob("$data/$family/*");
		my @actual = sort map { s{.*/}{}r } glob("$target/*");
		is_deeply(\@actual, \@source, 'native SIDE file set is exact');
		for my $name (@actual)
		{
			is(sha256_hex(slurp_file("$target/$name")), sha256_hex(slurp_file("$data/$family/$name")),
				'copied SIDE bytes match this origin');
		}
	}
	my ($generation) = $wal =~ /generation_([0-9]+)$/;
	my $claim_path = "$wal/pgrac_thread.claim";
	ok(-f $claim_path, 'original writer has its immutable claim');
	my @anchors = glob("$temp/valid-data/global/anchor_images/thread_$thread/generation_$generation/anchor_1-*.bin");
	is(scalar @anchors, 1, 'original checkpoint has one immutable native anchor');
	if (-f $claim_path && @anchors == 1)
	{
		my $claim = slurp_file($claim_path);
		my $anchor = slurp_file($anchors[0]);
		is(length $claim, 112, 'claim uses the original v2 encoding');
		is(length $anchor, 512, 'anchor uses the original v2 encoding');
		is(unpack('v', substr($claim, 4, 2)), 2, 'claim version is v2');
		is(unpack('v', substr($anchor, 208, 2)), $thread, 'anchor names its generated native thread');
		is(unpack('Q<', substr($anchor, 144, 8)), $generation, 'anchor binds original creation generation');
		is(unpack('H*', substr($anchor, 216, 32)), sha256_hex($claim), 'anchor selects exact original claim');
		like($anchors[0], qr/anchor_1-\Q@{[sha256_hex($anchor)]}\E\.bin$/, 'immutable name selects complete anchor bytes');
		my ($high, $low) = split '/', $checkpoint;
		is(unpack('Q<', substr($anchor, 24, 8)), hex($high) * 4294967296 + hex($low),
			'anchor retains this native checkpoint, not a peer checkpoint');
	}
}
is(scalar keys %generations, 4, 'four independent writer directories');
my ($versioned, $beyond_checkpoint) = (0, 0);
for my $dir ("$temp/valid-data/global", grep { -d $_ } glob("$temp/valid-data/base/*"))
{
	for my $file (grep { /\/[1-9][0-9]*(?:_(?:vm|space))?(?:\.[1-9][0-9]*)?$/ } glob("$dir/*"))
	{
		open my $fh, '<:raw', $file or die "open shared DATA: $!";
		while (read($fh, my $page, 8192) == 8192)
		{
			my $scn = unpack('Q', substr($page, 24, 8));
			$versioned++ if $scn > 0;
			$beyond_checkpoint++ if !defined($founder_scn) || $scn > $founder_scn;
		}
		close $fh or die "close shared DATA: $!";
	}
}
ok($versioned > 1000, 'SCN bound covers a real shared DATA/SPACE corpus');
is($beyond_checkpoint, 0, 'final native checkpoint bounds every created page SCN');
my @controls = glob("$temp/valid-data/global/control_images/1-*.bin");
is(scalar @controls, 1, 'cohort has one immutable common control image');
my @catalogs = glob("$temp/valid-data/global/catalog_checkpoints/1-*.json");
is(scalar @catalogs, 1, 'cohort has an explicit initial catalog manifest');
if (@controls == 1)
{
	my $bytes = slurp_file($controls[0]);
	is(length $bytes, 8192, 'common native control has exact original size');
	like($controls[0], qr/1-\Q@{[sha256_hex($bytes)]}\E\.bin$/, 'common control name selects exact bytes');
	mkdir "$temp/common-read" or die "mkdir: $!";
	mkdir "$temp/common-read/global" or die "mkdir: $!";
	copy($controls[0], "$temp/common-read/global/pg_control") or die "copy: $!";
	command_like(['pg_controldata', "$temp/common-read"], qr/Database system identifier:\s+$sysid\b/,
		'original native reader validates the common image');
}
if (@catalogs == 1)
{
	my $bytes = slurp_file($catalogs[0]);
	my $expected = {version => 1, generation => '1', entries => [], database_identity => {
		authority_uuid => '123456789abc4ef0923456789abcdef0', database_incarnation => '1',
		storage_uuid => $storage, system_identifier => $sysid }};
	is_deeply(decode_json($bytes), $expected, 'empty catalog manifest has original database identity');
	is($bytes, JSON::PP->new->canonical->encode($expected) . "\n", 'initial manifest uses canonical JSON');
	like($catalogs[0], qr/1-\Q@{[sha256_hex($bytes)]}\E\.json$/, 'catalog manifest name selects exact bytes');
}
my $source = sha256_hex(slurp_file("$temp/valid-caches/node_0/global/pg_control"));
command_fails_like([options('valid')], qr/INITDB_COHORT_/,
	'new invocation refuses prior complete or partial preparation');
is(sha256_hex(slurp_file("$temp/valid-caches/node_0/global/pg_control")), $source,
	'refusal preserves prior native bytes');

for my $bad ('missing-member', 'missing-setting', 'occupied', 'overlap', 'alias')
{
	my $bytes = request($bad, 15);
	$bytes =~ s/node003[^\n]*\n// if $bad eq 'missing-member';
	$bytes =~ s/common.cluster.shared_catalog[^\n]*\n// if $bad eq 'missing-setting';
	$bytes =~ s{\Q$temp/$bad-wal\E}{$temp/$bad-data/inside} if $bad eq 'overlap';
	if ($bad eq 'occupied')
	{
		mkdir "$temp/$bad-undo" or die "mkdir: $!";
		append_to_file("$temp/$bad-undo/keep", 'unchanged');
	}
	if ($bad eq 'alias')
	{
		mkdir "$temp/alias-real" or die "mkdir: $!";
		symlink "$temp/alias-real", "$temp/$bad-wal" or die "symlink: $!";
	}
	input($bad, $bytes);
	command_fails_like([options($bad)], qr/INITDB_(?:CONFIG|COHORT)_/,
		"$bad refuses before cohort creation");
	ok(!-e "$temp/$bad-caches" && !-e "$temp/$bad-data",
		"$bad creates no native writer or DATA");
}
is(slurp_file("$temp/occupied-undo/keep"), 'unchanged', 'occupied bytes preserved');
command_fails_like(['postgres', '--pgrac-initdb-cohort'], qr/INITDB_COHORT_/,
	'ordinary direct invocation lacks the original frontend context');
done_testing();
