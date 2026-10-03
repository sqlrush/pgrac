# Original new-database configuration creation, without ROOT publication.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(realpath);
use Digest::SHA qw(sha256_hex);
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = realpath(PostgreSQL::Test::Utils::tempdir);
my $sysid = '7584383251700000001';
my $uuid = '0123456789abcdef0123456789abcdef';

sub request
{
	my ($base) = @_;
	my %entries = (
		'cluster.controlfile_shared_authority' => 'on',
		'cluster.enabled' => 'on',
		'cluster.merged_recovery' => 'on',
		'cluster.shared_catalog' => 'on',
		'cluster.shared_config' => 'on',
		'cluster.shared_data_dir' => $base,
		'cluster.shared_storage_backend' => 'cluster_fs',
		'cluster.shared_storage_uuid' => '01234567-89ab-cdef-0123-456789abcdef',
		'cluster.smgr_user_relations' => 'on',
		'cluster.undo_tablespace_path' => "$temp/undo",
		'cluster.wal_threads_dir' => "$temp/threads");
	my $text = "\@authority_uuid=123456789abcdef0123456789abcdef0\n"
		. "\@configured_0=000000000000000f\n\@configured_1=0000000000000000\n"
		. "\@database_incarnation=1\n\@format=1\n\@generation=1\n"
		. "\@storage_uuid=$uuid\n\@system_identifier=$sysid\n";
	for my $key (sort keys %entries)
	{
		(my $value = $entries{$key}) =~ s/'/''/g;
		$text .= "common.$key='$value'\n";
	}
	$text .= sprintf("node%03d.cluster.node_id='%d'\n", $_, $_) for 0 .. 3;
	return $text;
}

sub options
{
	my ($name, $file) = @_;
	return ('initdb', '-D', "$temp/$name-data", '-X', "$temp/$name-wal",
		'-k', '-A', 'trust', '--no-locale', '--pgrac-initdb-thread=1',
		"--pgrac-initdb-system-identifier=$sysid",
		"--pgrac-initdb-shared-base=$temp/$name-shared",
		"--pgrac-initdb-storage-uuid=$uuid", '--pgrac-initdb-database-incarnation=1',
		"--pgrac-initdb-shared-config=$file");
}

my $original = request("$temp/valid-shared");
append_to_file("$temp/config", $original);
chmod 0600, "$temp/config" or die "chmod: $!";
my ($out, $err);
ok(IPC::Run::run([options('valid', "$temp/config")], '>', \$out, '2>', \$err),
	'original creator persists its initial common configuration') or diag $err;
my $object = "$temp/valid-shared/global/config_images/1-" . sha256_hex($original) . '.conf';
BAIL_OUT('creator did not produce its configuration object') unless -f $object;
is(slurp_file($object), $original, 'exact canonical bytes at existing immutable object path');
is(slurp_file("$temp/config"), $original, 'request is unchanged');
is((stat($object))[3], 1, 'object is an independent file, not an adopted hard link');
like($out, qr/shared startup authority is not published/, 'result does not claim OPEN');
ok(!-e "$temp/valid-shared/global/pgrac_control_root"
	&& !-e "$temp/valid-data/global/pgrac_control_binding", 'no ROOT or PGCB is published');

sub rejected
{
	my ($name, $edit) = @_;
	my $bytes = request("$temp/$name-shared");
	$edit->(\$bytes);
	my $file = "$temp/$name-config";
	append_to_file($file, $bytes);
	chmod 0600, $file or die "chmod: $!";
	command_fails_like([options($name, $file)], qr/INITDB_CONFIG_/,
		"$name refuses before shared mutation");
	ok(!-e "$temp/$name-shared/global" && !-e "$temp/$name-shared/base",
		"$name leaves the shared target unpopulated");
	is(slurp_file($file), $bytes, "$name request is unchanged");
}

rejected('generation', sub { ${$_[0]} =~ s/\@generation=1/\@generation=2/; });
rejected('system', sub { ${$_[0]} =~ s/\@system_identifier=$sysid/\@system_identifier=17/; });
rejected('incarnation', sub { ${$_[0]} =~ s/\@database_incarnation=1/\@database_incarnation=2/; });
rejected('storage', sub { ${$_[0]} =~ s/\@storage_uuid=$uuid/\@storage_uuid=123456789abcdef0123456789abcdef0/; });
rejected('missing-member', sub { ${$_[0]} =~ s/node003[^\n]*\n//; });
rejected('missing-common', sub { ${$_[0]} =~ s/common.cluster.shared_catalog[^\n]*\n//; });
rejected('unknown-setting', sub { ${$_[0]} .= "node003.nonexistent_setting='on'\n"; });
rejected('wrong-base', sub { ${$_[0]} =~ s/\Q$temp\/wrong-base-shared\E/$temp\/valid-shared/; });
rejected('noncanonical-number', sub { ${$_[0]} =~ s/\@generation=1/\@generation=01/; });
rejected('overflow', sub { ${$_[0]} =~ s/\@generation=1/\@generation=18446744073709551616/; });
rejected('include', sub { ${$_[0]} .= "include='/tmp/config'\n"; });

symlink "$temp/config", "$temp/alias" or die "symlink: $!";
command_fails_like([options('alias', "$temp/alias")], qr/INITDB_CONFIG_/,
	'alias input refuses before native creation');
ok(!-e "$temp/alias-data", 'alias did not create PGDATA');
link "$temp/config", "$temp/hardlink" or die "link: $!";
command_fails_like([options('linked', "$temp/config")], qr/INITDB_CONFIG_/,
	'multiply linked request refuses');
unlink "$temp/hardlink" or die "unlink: $!";
chmod 0660, "$temp/config" or die "chmod: $!";
command_fails_like([options('writable', "$temp/config")], qr/INITDB_CONFIG_/,
	'group writable request refuses');
chmod 0600, "$temp/config" or die "chmod: $!";
my @missing = grep { !/^--pgrac-initdb-system-identifier=/ } options('no-id', "$temp/config");
command_fails_like(\@missing, qr/INITDB_CONFIG_/,
	'configuration request needs the original explicit common identity');
ok(!-e "$temp/no-id-data", 'incomplete request did not create native files');

done_testing();
