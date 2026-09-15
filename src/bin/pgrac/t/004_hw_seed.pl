# Exercise the actual wrapper and the three ClusterQuad seed constructors.
# The constructor probes stop at the first postmaster call: initdb/config are
# real, but this file does not certify startup, backup recovery or restart.
use strict;
use warnings;
use Cwd qw(realpath);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);

sub check_snapshot
{
	my ($data, $root, $owner, $name) = @_;
	my $path = "$root/global/pg_hw_snapshot.$owner";
	ok(-f $path, "$name has an own snapshot before first start");
	return unless -f $path;
	my $bytes = slurp_file($path);
	is(length($bytes), 52, "$name has the empty v1 image");
	my ($magic, $version, $sysid, $self, $partition, $kind, $generation,
		$lsn, $entries, $reserved, $crc) = unpack('L L Q L L L L Q L L L', $bytes);
	is_deeply([$magic, $version, $self, $partition, $kind, $generation,
		$entries, $reserved], [0x48575350, 1, $owner, $owner, 0, 0, 0, 0],
		"$name names only the actual seed owner");
	my ($control, $err) = ('', '');
	ok(IPC::Run::run(['pg_controldata', $data], '>', \$control, '2>', \$err),
		"$name reads actual final control");
	my ($native_sysid) = $control =~ /Database system identifier:\s*(\d+)/;
	my ($high, $low) = $control =~ /Latest checkpoint's REDO location:\s*([0-9A-F]+)\/([0-9A-F]+)/;
	is("$sysid", $native_sysid, "$name uses actual bootstrap identity");
	ok(defined($high) && $lsn == hex($high) * 4294967296 + hex($low),
		"$name uses actual bootstrap checkpoint REDO");
	is_deeply([glob("$root/global/pg_hw_snapshot.*")], [$path],
		"$name does not sign for future joiners");
	return $bytes;
}

for my $case (['native', 3, []], ['wal', 5, ["--wal-threads-dir=$temp/wal"]])
{
	my ($name, $owner, $extra) = @$case;
	my $data = "$temp/$name-data";
	my $root = "$temp/$name-root";
	command_ok(['pgrac-init', '-D', $data, '--cluster-seed', "--node-id=$owner",
		"--shared-data-dir=$root",
		'--initdb-options=--no-locale -k -capplication_name=VALID', @$extra],
		"$name real wrapper creates a new seed");
	my $bytes = check_snapshot($data, $root, $owner, $name);
	next unless -f "$data/postgresql.conf";
	my $conf = slurp_file("$data/postgresql.conf");
	like($conf, qr/^cluster\.shared_data_dir\s*=\s*'\Q$root\E'\s*$/m,
		"$name first-start root matches producer");
	like($conf, qr/^cluster\.node_id\s*=\s*\Q$owner\E\s*$/m,
		"$name first-start owner matches producer");
	if (defined $bytes)
	{
		command_ok(['pgrac-init', '-D', $data, '--cluster-seed', "--node-id=$owner",
			"--shared-data-dir=$root"], "$name existing data skips initdb");
		is(slurp_file("$root/global/pg_hw_snapshot.$owner"), $bytes,
			"$name existing-data path leaves snapshot untouched");
	}
}

for my $case (
	['no-sync', '--no-sync'],
	['owner-override', '--pgrac-hw-snapshot-owner=12'],
	['data-override', "-D $temp/unmanaged-data"],
	['clustered-data-override', "-kD$temp/clustered-unmanaged-data"],
	['abbreviated-data-override', "--pgd=$temp/abbreviated-unmanaged-data"],
	['terminator', '--'])
{
	my ($name, $options) = @$case;
	my $data = "$temp/reject-$name";
	my $root = "$temp/reject-$name-root";
	command_fails(['pgrac-init', '-D', $data, '--cluster-seed',
		"--shared-data-dir=$root", "--initdb-options=$options"],
		"wrapper rejects $name");
	ok(!-e $data, "$name cannot create the managed PGDATA");
	ok(!-e "$root/global/pg_hw_snapshot.0", "$name cannot publish a seed snapshot");
}
ok(!-e "$temp/clustered-unmanaged-data",
	'clustered short option cannot create an unmanaged PGDATA');
ok(!-e "$temp/abbreviated-unmanaged-data",
	'abbreviated long option cannot create an unmanaged PGDATA');

my $old = "$temp/old-data";
command_ok(['initdb', '-D', $old, '-N', '--no-locale'],
	'create ordinary non-HW data for existing-data refusal boundary');
command_ok(['pgrac-init', '-D', $old, '--cluster-seed', '--force',
	"--shared-data-dir=$temp/old-root"], 'existing wrapper mode does not rerun initdb');
ok(!-e "$temp/old-root/global/pg_hw_snapshot.0",
	'force does not fabricate missing HW history for existing data');

# No postmaster is launched by these probes. Replacing start with a terminal
# sentinel observes the actual init/config side effects at its first consumer.
for my $case (
	['homogeneous_sql', {shared_data => 1, shared_system_identifier => 1,
		shared_system_identifier_seed_sql => 'SELECT 1'}],
	['homogeneous_empty', {shared_data => 1, shared_system_identifier => 1}],
	['catalog', {shared_catalog => 1}])
{
	my ($name, $opts) = @$case;
	my $reached = 0;
	{
		no warnings 'redefine';
		local *PostgreSQL::Test::Cluster::start = sub {
			my ($node) = @_;
			$reached++;
			my $conf = slurp_file($node->data_dir . '/postgresql.conf');
			my ($root) = $conf =~ /^cluster\.shared_data_dir\s*=\s*'([^']+)'\s*$/m;
			ok(defined($root), "$name supplies metadata before first start");
			check_snapshot($node->data_dir, $root, 0, $name) if defined $root;
			like($conf, qr/^cluster\.node_id\s*=\s*0\s*$/m,
				"$name binds the first-start owner");
			like($conf, qr/^cluster\.enabled\s*=\s*off\s*$/m,
				"$name keeps first start native");
			like($conf, qr/^cluster\.lms_enabled\s*=\s*off\s*$/m,
				"$name has no first-start LMS");
			unlike($conf, qr/^cluster\.shared_catalog\s*=\s*on\s*$/m,
				"$name leaves catalogs local before backup");
			unlike($conf, qr/^cluster\.controlfile_shared_authority\s*=\s*on\s*$/m,
				"$name does not move control authority ahead of backup");
			if ($name eq 'homogeneous_sql')
			{
				like($conf, qr/^cluster\.smgr_user_relations\s*=\s*on\s*$/m,
					'seed SQL retains its shared user-relation storage');
				like($conf, qr/^cluster\.relation_extend_lock_enabled\s*=\s*off\s*$/m,
					'seed SQL retains native extension');
			}
			else
			{
				unlike($conf, qr/^cluster\.smgr_user_relations\s*=\s*on\s*$/m,
					"$name does not move user relations ahead of backup");
			}
			die "HW_PROBE_BEFORE_FIRST_START\n";
		};
		eval { PostgreSQL::Test::ClusterQuad->new_quad("hw_$name", %$opts); };
		like($@, qr/^HW_PROBE_BEFORE_FIRST_START\n/,
			"$name reaches first-start boundary after real initdb");
	}
	is($reached, 1, "$name has exactly one pre-start observation");
}

done_testing();
