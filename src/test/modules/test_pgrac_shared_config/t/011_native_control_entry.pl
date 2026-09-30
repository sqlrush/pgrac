# PGRAC: actual initial postmaster entry, still no PRE2 admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use File::Copy qw(copy);
use IPC::Run;
use POSIX ();
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub write_bytes
{
	my ($path, $bytes) = @_;
	open(my $fh, '>:raw', $path) or die "open fixture $path: $!";
	print {$fh} $bytes;
	close($fh) or die "close fixture $path: $!";
}

my $node = PostgreSQL::Test::Cluster->new('initial_control');
$node->init;
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.shared_config'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC build required; not PRE2 admission qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $data = $node->data_dir;
my $base = "$data/test_native_bootstrap";
my $later_guard = qr/Keep cluster.shared_config off until the complete root-v2 fresh initialization and startup path is qualified/;
for my $case (
	['valid', 0, $later_guard],
	['valid', 200, $later_guard],
	['valid', 317, qr/superuser_reserved_connections.*must be less than max_connections \(320\)/s],
	['capacity', 0, qr/native bootstrap recovery capacity is insufficient.*max_connections/s],
	['native-format', 0, qr/BLCKSZ/],
	['identity', 0, qr/native bootstrap observation failed/],
	['prefix-missing', 0, $later_guard],
	['root-corrupt', 0, qr/native bootstrap observation failed/],
	['missing-binding', 0, qr/native bootstrap observation failed/])
{
	my ($mutation, $reserved, $reason) = @$case;
	is($node->safe_psql('postgres', "SELECT test_pgrac_bootstrap_fixture('entry-$mutation')"),
		't', "$mutation/$reserved uses production-codec disposable inputs");
	$node->stop('fast');
	my $projection = slurp_file("$data/global/pg_control");
	# The exact reader must not consult or repair this compatibility image.
	my $bad_projection = $projection;
	substr($bad_projection, 0, 1) = chr(ord(substr($bad_projection, 0, 1)) ^ 1);
	write_bytes("$data/global/pg_control", $bad_projection);
	copy("$base/local/global/pgrac_control_binding", "$data/global/pgrac_control_binding")
		or die "copy fixture binding: $!";
	chmod(0600, "$data/global/pgrac_control_binding") == 1
		or die "protect fixture binding: $!";
	if ($mutation eq 'missing-binding')
	{
		unlink("$data/global/pgrac_control_binding") or die "unlink fixture binding: $!";
	}
	if ($mutation eq 'root-corrupt')
	{
		write_bytes("$base/shared/global/pgrac_control_root", 'invalid root');
	}
	rename("$data/pg_wal", "$data/fixture_original_wal") or die "preserve fixture WAL: $!";
	symlink("$base/wal/thread_1/generation_99", "$data/pg_wal") or die "route fixture WAL: $!";
	for my $family (qw(pg_xact pg_subtrans pg_multixact pg_commit_ts))
	{
		rename("$data/$family", "$data/fixture_original_$family")
			or die "preserve fixture $family: $!";
		symlink("$base/shared/native_side/origin_0/$family", "$data/$family")
			or die "route fixture $family: $!";
	}
	$node->append_conf('postgresql.conf', qq{
cluster.shared_config=on
cluster.node_id=0
cluster.shared_data_dir='$base/shared'
cluster.wal_threads_dir='$base/wal'
cluster.undo_tablespace_path='$base/undo'
reserved_connections=$reserved
});
	my $root_before = sha256_hex(slurp_file("$base/shared/global/pgrac_control_root"));
	my $offset = -s $node->logfile;
	ok(!$node->start(fail_ok => 1), "$mutation/$reserved never qualifies startup");
	my $log = substr(slurp_file($node->logfile), $offset);
	like($log, $reason, "$mutation/$reserved reaches its exact native boundary");
	unlike($log, qr/incorrect checksum in control file/,
		"$mutation/$reserved ignores the invalid compatibility projection");
	is(slurp_file("$data/global/pg_control"), $bad_projection,
		"$mutation/$reserved does not overwrite projection");
	is(sha256_hex(slurp_file("$base/shared/global/pgrac_control_root")), $root_before,
		"$mutation/$reserved does not publish root");
	if ($mutation eq 'valid' && $reserved == 0)
	{
		command_fails_like(['postgres', '--single', '-D', $data, 'postgres'],
			qr/PGRAC_CONTROL_BINDING_REQUIRED/, 'standalone cannot acquire initial-postmaster route');
		SKIP:
		{
			skip 'POSIX projection FIFO', 6 if $windows_os;
			for my $shape ('absent', 'fifo')
			{
				unlink("$data/global/pg_control") or die "remove fixture projection: $!";
				if ($shape eq 'fifo')
				{
					POSIX::mkfifo("$data/global/pg_control", 0600) == 0
						or die "mkfifo fixture projection: $!";
				}
				my ($out, $err, $completed) = ('', '', 0);
				my $h = IPC::Run::start(['postgres', '-D', $data],
					'>', \$out, '2>', \$err, IPC::Run::timeout(5));
				eval { $h->finish; $completed = 1; };
				$h->kill_kill unless $completed;
				ok($completed, "$shape projection cannot block exact root observation");
				like($out . $err, $later_guard, "$shape projection reaches unchanged final guard");
				ok($shape eq 'fifo' ? -p "$data/global/pg_control" : !-e "$data/global/pg_control",
					"$shape projection remains untouched");
				unlink("$data/global/pg_control") or die "remove fixture FIFO: $!"
					if $shape eq 'fifo';
				write_bytes("$data/global/pg_control", $bad_projection);
			}
		}
	}
	unlink("$data/pg_wal") or die "remove fixture-only WAL route: $!";
	rename("$data/fixture_original_wal", "$data/pg_wal") or die "restore fixture WAL: $!";
	for my $family (qw(pg_xact pg_subtrans pg_multixact pg_commit_ts))
	{
		unlink("$data/$family") or die "remove fixture-only $family route: $!";
		rename("$data/fixture_original_$family", "$data/$family")
			or die "restore fixture $family: $!";
	}
	unlink("$data/global/pgrac_control_binding") or die "remove fixture binding: $!"
		unless $mutation eq 'missing-binding';
	write_bytes("$data/global/pg_control", $projection);
	$node->append_conf('postgresql.conf', qq{
cluster.shared_config=off
cluster.shared_data_dir=''
cluster.wal_threads_dir=''
cluster.undo_tablespace_path='pg_undo'
reserved_connections=0
});
	ok($node->start, "$mutation/$reserved preserved ordinary native database");
}
$node->stop('fast');
done_testing();
