# Original fixed-member creation owns every new native writer.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(realpath);
use Digest::SHA qw(sha256 sha256_hex);
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
like($out, qr/shared startup remains closed/,
	'creation publication does not claim shared OPEN');
ok(-f "$temp/valid-data/global/pgrac_control_root", 'complete creator publishes ROOT last');
for my $name ('pgrac_oid_authority', 'pgrac_catalog_authority',
	'pgrac_xid_authority', 'pgrac_xid_authority.bak',
	'pgrac_xid_prehistory', 'pgrac_xid_prehistory.bak')
{
	ok(-f "$temp/valid-data/global/$name" && !-l "$temp/valid-data/global/$name",
		"original creator persists $name before ROOT");
}
if (-f "$temp/valid-data/global/pgrac_xid_prehistory")
{
	my $history = slurp_file("$temp/valid-data/global/pgrac_xid_prehistory");
	my $clog = slurp_file("$temp/valid-caches/node_0/pgrac_initdb_native_side/pg_xact/0000");
	is(substr($history, 32), $clog, 'prehistory retains the complete actual founder CLOG');
	is($history, slurp_file("$temp/valid-data/global/pgrac_xid_prehistory.bak"),
		'original prehistory backup contains identical bytes');
	is(slurp_file("$temp/valid-data/global/pgrac_xid_authority"),
		slurp_file("$temp/valid-data/global/pgrac_xid_authority.bak"),
		'original XID backup contains identical bytes');
}
my %generations;
my $founder_scn;
for my $node (0 .. 3)
{
	my $data = "$temp/valid-caches/node_$node";
	ok(-f "$data/global/pgrac_cf_contract", 'original creator supplies storage identity without qualification');
	if (-f "$data/global/pgrac_cf_contract")
	{
		my $contract = slurp_file("$data/global/pgrac_cf_contract");
		is(length $contract, 64, 'original storage contract retains the existing layout');
		is(unpack('Q', substr($contract, 8, 8)), $sysid, 'storage contract binds the original system');
		is(substr($contract, 16, 33), "$storage\0", 'storage contract binds the full compact UUID');
		is(unpack('L', substr($contract, 52, 4)), 0, 'creation does not invent cross-node storage verification');
	}
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
	ok(-f "$data/global/pgrac_control_binding", 'original local binding follows complete ROOT publication');
	my $before_bootstrap = sha256_hex(slurp_file("$temp/valid-data/global/pgrac_control_root"));
	my @bootstrap = ('postgres', '-D', $data, '-C', 'shared_memory_size',
		'-c', 'cluster.enabled=on', '-c', 'cluster.shared_config=on',
		'-c', 'cluster.controlfile_shared_authority=on', '-c', "cluster.node_id=$node",
		'-c', "cluster.shared_data_dir=$temp/valid-data",
		'-c', "cluster.wal_threads_dir=$temp/valid-wal",
		'-c', "cluster.undo_tablespace_path=$temp/valid-undo");
	command_fails_like(\@bootstrap, qr/shared configuration conflicts with a higher-priority source/,
		'command-line settings cannot override ROOT-selected configuration') if $node == 0;
	# Bootstrap discovery is local FILE input. ROOT remains the authority for
	# common/instance values; even identical higher-priority overrides refuse.
	append_to_file("$data/postgresql.conf", "\ncluster.enabled=on\ncluster.shared_config=on\n"
		. "cluster.controlfile_shared_authority=on\ncluster.node_id=$node\n"
		. "cluster.shared_data_dir='$temp/valid-data'\n"
		. "cluster.wal_threads_dir='$temp/valid-wal'\n"
		. "cluster.undo_tablespace_path='$temp/valid-undo'\n");
	@bootstrap = ('postgres', '-D', $data, '-C', 'shared_memory_size');
	my ($bootstrap_out, $bootstrap_err);
	ok(IPC::Run::run(\@bootstrap, '>', \$bootstrap_out, '2>', \$bootstrap_err),
		'ordinary early native bootstrap consumes original creation') or diag $bootstrap_err;
	like($bootstrap_out, qr/^\d+\s*$/, 'native preparation reaches shared memory sizing');
	is(sha256_hex(slurp_file("$temp/valid-data/global/pgrac_control_root")), $before_bootstrap,
		'early native preparation does not publish ROOT');
	for my $family ('pg_xact', 'pg_subtrans', 'pg_multixact', 'pg_commit_ts')
	{
		is(readlink("$data/$family"), "$temp/valid-data/native_side/origin_$node/$family",
			'original creator installs exact native SIDE routing');
	}
	for my $family ('pg_xact', 'pg_subtrans', 'pg_multixact/offsets', 'pg_multixact/members', 'pg_commit_ts')
	{
		my $target = "$temp/valid-data/native_side/origin_$node/$family";
		my $original = "$data/pgrac_initdb_native_side/$family";
		ok(-d $target, "origin $node has its native $family directory");
		ok(-d $original && !-l $original, 'original native SIDE directory is retained');
		my @source = sort map { s{.*/}{}r } glob("$original/*");
		my @actual = sort map { s{.*/}{}r } glob("$target/*");
		is_deeply(\@actual, \@source, 'native SIDE file set is exact');
		for my $name (@actual)
		{
			is(sha256_hex(slurp_file("$target/$name")), sha256_hex(slurp_file("$original/$name")),
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
sub entries
{
	my ($path) = @_;
	opendir(my $dir, $path) or return undef;
	my @names = sort grep { $_ ne '.' && $_ ne '..' } readdir($dir);
	closedir $dir or die "close directory: $!";
	return \@names;
}
is_deeply(entries("$temp/valid-data/pg_undo"), [map { "instance_$_" } 0 .. 3],
	'canonical shared UNDO contains the complete original owner directories');
for my $node (0 .. 3)
{
	my $directory = "$temp/valid-data/pg_undo/instance_$node";
	ok(-d $directory && !-l $directory, 'original undo owner directory exists');
	is_deeply(entries($directory), [], 'no bootstrap segment or shared TT is fabricated before live allocation');
}
ok(!-e "$temp/valid-data/pg_undo/pgrac_undo_root.control", 'creation does not grant online PGRD authority');
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
# Independently derive both creation digests from actual persisted bytes.
sub tree_material
{
	my ($base, $relative, $entries, $derived) = @_;
	for my $name (@{entries(length($relative) ? "$base/$relative" : $base)})
	{
		my $path = length($relative) ? "$relative/$name" : $name;
		next if $derived && ($path eq 'global/wal_startup'
			|| $path eq 'global/pgrac_control_root' || $path eq 'global/pgrac_control_root.bak');
		die "tree alias $path" if -l "$base/$path";
		my $directory = -d "$base/$path";
		my $bytes = $directory ? '' : slurp_file("$base/$path");
		$entries->{$path} = pack('C V', $directory ? 1 : 2, length($path)) . $path
			. pack('Q<', length($bytes)) . ($directory ? "\0" x 32 : sha256($bytes));
		tree_material($base, $path, $entries, $derived) if $directory;
	}
}
sub tree_digest
{
	my ($base, $derived) = @_;
	my %entries;
	tree_material($base, '', \%entries, $derived);
	return sha256("PGRAC-CREATION-TREE-V1\0" . join('', map { $entries{$_} } sort keys %entries));
}
if (-f "$temp/valid-data/global/pgrac_control_root")
{
	my $root = slurp_file("$temp/valid-data/global/pgrac_control_root");
	is(length($root), 66048, 'complete fixed-size ROOT is durable');
	is($root, slurp_file("$temp/valid-data/global/pgrac_control_root.bak"),
		'original primary and backup have identical bytes');
	is(unpack('v', substr($root, 4, 2)), 3, 'ROOT uses v3');
	is(unpack('Q<', substr($root, 64, 8)), 0x15, 'creation uses the separate flags domain');
	is(unpack('Q<', substr($root, 164, 8)), 1, 'creation lineage is generation one');
	is(unpack('Q<', substr($root, 172, 8)), 0, 'creation never invents an OPEN epoch');
	is(unpack('V', substr($root, 196, 4)), 1, 'new ROOT is mounted, not serving');
	is(substr($root, 232, 16), "\0" x 16, 'creation has no serving members');
	my $origins = "PGRAC-CREATION-ORIGINS-V1\0" . substr($root, 216, 16);
	for my $node (0 .. 3)
	{
		my $wal = readlink "$temp/valid-caches/node_$node/pg_wal";
		my $thread = $node + 1;
		my ($generation) = $wal =~ /generation_([0-9]+)$/;
		my ($anchor) = glob("$temp/valid-data/global/anchor_images/thread_$thread/generation_$generation/anchor_1-*.bin");
		$origins .= pack('V', $node) . sha256(slurp_file("$temp/valid-caches/node_$node/global/pg_control"))
			. tree_digest($wal, 0) . sha256(slurp_file("$wal/pgrac_thread.claim")) . sha256(slurp_file($anchor))
			. slurp_file("$temp/valid-caches/node_$node/global/pgrac_cf_contract");
		my $binding = slurp_file("$temp/valid-caches/node_$node/global/pgrac_control_binding");
		is(length($binding), 256, 'local binding has exact length');
		is(unpack('v', substr($binding, 4, 2)), 3, 'PGCB uses v3');
		is(unpack('V', substr($binding, 12, 4)), 1, 'PGCB selects creation domain');
		is(unpack('V', substr($binding, 64, 4)), $node, 'PGCB binds its actual local node');
		is(substr($binding, 152, 64), substr($root, 100, 64), 'PGCB selects both original ROOT digests');
		is(substr($binding, 216, 16), pack('Q< Q<', 1, 0), 'PGCB cannot assert online OPEN');
		my ($startup) = glob("$temp/valid-data/global/wal_startup/thread_$thread/startup_1-*.bin");
		ok(defined($startup), 'each configured origin has an initialized input');
		my $input = slurp_file($startup);
		is(length($input), 1536, 'initialized input has exact length');
		is(unpack('V', substr($input, 12, 4)), 4, 'kind4 is initialized, never CLEAN');
		is(unpack('Q<', substr($input, 48, 8)), 0, 'kind4 awaits the actual formation');
		is(substr($root, 512 + $node * 512 + 336, 32), sha256($input), 'ROOT selects exact kind4 bytes');
	}
	my $native_hash = sha256($origins);
	is(substr($root, 132, 32), $native_hash, 'ROOT binds all actual native control/WAL/claim/anchor/storage identity bytes');
	my $cohort = "PGRAC-CREATION-COHORT-V1\0" . substr($root, 24, 8) . substr($root, 32, 32)
		. substr($root, 200, 8) . substr($root, 216, 16) . substr($root, 4, 2)
		. substr($root, 64, 8) . substr($root, 188, 8) . substr($root, 248, 40)
		. substr($root, 288, 40) . substr($root, 328, 8) . substr($root, 344, 32)
		. substr($root, 336, 8) . $native_hash . tree_digest("$temp/valid-data", 1)
		. tree_digest("$temp/valid-undo", 0);
	is(substr($root, 100, 32), sha256($cohort), 'ROOT cohort digest independently matches complete original objects');
}
# Exercise the production postmaster source, not just the pure catalog codec.
# An ordinary startup cannot heal a missing or corrupt original authority.
{
	my $marker = "$temp/valid-data/global/pgrac_catalog_authority";
	my $saved = "$marker.saved";
	my @bootstrap = ('postgres', '-D', "$temp/valid-caches/node_0", '-C', 'shared_memory_size');
	my $root = slurp_file("$temp/valid-data/global/pgrac_control_root");
	rename $marker, $saved or die "save marker: $!";
	command_fails_like(\@bootstrap, qr/selected shared catalog inputs are unavailable or inconsistent/,
		'missing creator catalog marker refuses through the actual startup source');
	ok(!-e $marker, 'ordinary startup does not recreate the missing marker');
	append_to_file($marker, 'invalid');
	command_fails_like(\@bootstrap, qr/selected shared catalog inputs are unavailable or inconsistent/,
		'corrupt creator catalog marker refuses through the actual startup source');
	is(slurp_file($marker), 'invalid', 'ordinary startup does not repair the corrupt marker');
	unlink $marker or die "remove corrupt marker: $!";
	rename $saved, $marker or die "restore marker: $!";
	is(slurp_file("$temp/valid-data/global/pgrac_control_root"), $root,
		'catalog refusal never changes the selected ROOT');
	command_ok(\@bootstrap, 'original catalog inputs remain restartable after negative probes');
}
done_testing();
