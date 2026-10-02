# Shared startup must reject unchecksummed storage before recovery writes.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use File::Find;
use PostgreSQL::Test::Utils;
use Test::More;

my $temp = PostgreSQL::Test::Utils::tempdir;

sub contents
{
	my ($dir) = @_;
	my @files;
	find({ no_chdir => 1, follow => 1, wanted => sub {
		push @files, $_ . ':link:' . readlink($_) if -l $_;
		return unless -f $_;
		push @files, $_ . ':' . sha256_hex(slurp_file($_));
	} }, $dir);
	return join("\n", sort @files);
}

sub shared_refuses_unchanged
{
	my ($data, $name) = @_;
	my $before = contents($data);
	my ($out, $err) = ('', '');
	my $ok = IPC::Run::run(
		[ 'postgres', '-D', $data, '-c', 'cluster.shared_config=on',
		  '-c', 'log_error_verbosity=verbose' ],
		'>', \$out, '2>', \$err);
	ok(!$ok, "$name: shared startup refuses");
	like($err, qr/55000:.*cluster\.shared_config requires data checksums/,
		"$name: exact checksum prerequisite error");
	like($err, qr/initdb -k.*--data-checksums/s, "$name: initialization hint");
	is(contents($data), $before, "$name: all data-directory file bytes unchanged");
}

for my $relocated (0, 1)
{
	my $data = "$temp/wrapper-$relocated";
	my @args = ('pgrac-init', '-D', $data, '--node-id=0',
		'--initdb-options=--no-sync');
	push @args, "--wal-threads-dir=$temp/wal" if $relocated;
	command_ok(\@args, "wrapper initializes checksummed directory (relocated=$relocated)");
	command_like([ 'pg_controldata', $data ], qr/Data page checksum version:\s+1/,
		"wrapper stores checksum version 1 (relocated=$relocated)");
	# This local SQL probe is not shared admission. It uses the original
	# nonshared single-user path and exits cleanly on EOF. Disable the
	# cluster registry GUC along with cluster.enabled; pg_wal stays relocated.
	my ($sql, $out, $err) = ("SHOW data_checksums;\n", '', '');
	ok(IPC::Run::run(
		[ 'postgres', '--single', '-D', $data, '-c', 'cluster.enabled=off',
		  '-c', 'cluster.wal_threads_dir=', 'postgres' ],
		'<', \$sql, '>', \$out, '2>', \$err),
		"native SQL observes wrapper directory (relocated=$relocated)") or diag($err);
	like($out, qr/data_checksums.*"on"/s,
		"SHOW data_checksums is on (relocated=$relocated)");
	command_ok([ 'pg_checksums', '--disable', '--no-sync', '-D', $data ],
		"disable checksums while stopped (relocated=$relocated)");
	shared_refuses_unchanged($data, "disabled checksums, relocated=$relocated");
}

my $plain = "$temp/native";
command_ok([ 'initdb', '-D', $plain, '--no-sync' ], 'native initdb keeps its checksum default');
command_like([ 'pg_controldata', $plain ], qr/Data page checksum version:\s+0/,
	'native directory has no checksums');
shared_refuses_unchanged($plain, 'native unchecked directory');
my ($sql, $out, $err) = ("SHOW data_checksums;\n", '', '');
ok(IPC::Run::run(
	[ 'postgres', '--single', '-D', $plain, '-c', 'cluster.enabled=off', 'postgres' ],
	'<', \$sql, '>', \$out, '2>', \$err), 'nonshared native startup remains allowed');
like($out, qr/data_checksums.*"off"/s, 'nonshared SHOW data_checksums stays off');
done_testing();
