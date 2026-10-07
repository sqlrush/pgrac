# Native elog/fsync fault smoke. The cluster_fs leg uses the supported storage
# provider mode with local catalogs; it is not a PRE2 cohort/OPEN acceptance.
# No authority or shared-control bytes are manufactured by this fixture.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use Cwd qw(abs_path);
use File::Basename qw(dirname);
use IPC::Run qw(run);
use Text::ParseWords qw(shellwords);
use Time::HiRes qw(time sleep);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($^O ne 'darwin' && $^O ne 'linux')
{
	fail("BLOCKED: fsync syscall fixture requires Darwin or Linux, got $^O");
	done_testing();
	exit 1;
}

my $source = dirname(abs_path(__FILE__));
my $work = PostgreSQL::Test::Utils::tempdir;
my $library = "$work/fsync_fault.so";
my $probe = "$work/fsync_fault_probe";
my @cc = shellwords($ENV{CC} // 'cc');
my @shared = $^O eq 'darwin' ? ('-dynamiclib') : ('-shared', '-fPIC');
my @dl = $^O eq 'darwin' ? () : ('-ldl');
ok(run_log([@cc, '-Wall', '-Wextra', '-Werror', @shared,
	"$source/fsync_fault.c", '-o', $library, @dl]), 'build targeted fsync fault shim')
	or BAIL_OUT('fault shim build failed');
ok(run_log([@cc, '-Wall', '-Wextra', '-Werror', "$source/fsync_fault_probe.c",
	'-o', $probe]), 'build syscall isolation probe')
	or BAIL_OUT('fault probe build failed');
my $preload = $^O eq 'darwin' ? 'DYLD_INSERT_LIBRARIES' : 'LD_PRELOAD';
{
	local $ENV{$preload} = $library;
	local $ENV{PGRAC_FSYNC_FAULT_CONTROL} = "$work/probe.control";
	local $ENV{PGRAC_FSYNC_FAULT_WITNESS} = "$work/probe.witness";
	ok(run_log([$probe, "$work/target", "$work/other", "$work/probe.control"]),
		'only the armed inode fails, disarming restores actual fsync')
		or BAIL_OUT('fault isolation failed');
}

sub quote_shell
{
	my ($value) = @_;
	$value =~ s/'/'"'"'/g;
	return "'$value'";
}

sub native_case
{
	my ($shared, $retry) = @_;
	my $route = $shared ? 'cluster_fs' : 'md';
	if (!defined getpwuid($>))
	{
		fail("BLOCKED: native initdb cannot resolve effective UID $>");
		return;
	}
	if ($> == 0)
	{
		fail('BLOCKED: native initdb requires a non-root OS account');
		return;
	}
	my $node = PostgreSQL::Test::Cluster->new("sync_${route}_${retry}");
	$node->init;
	my $root = PostgreSQL::Test::Utils::tempdir;
	my $control = "$root/fault.control";
	my $witness = "$root/fault.witness";
	my $wrapper = "$root/postgres-with-fault";
	my $bin;
	run ['pg_config', '--bindir'], '>', \$bin or die 'pg_config --bindir failed';
	chomp $bin;
	# pg_ctl starts a shell on Unix. Set DYLD after that shell has started,
	# rather than relying on a protected system shell to propagate it.
	append_to_file($wrapper, "#!/bin/sh\n" .
		"export $preload=" . quote_shell($library) . "\n" .
		'export PGRAC_FSYNC_FAULT_CONTROL=' . quote_shell($control) . "\n" .
		'export PGRAC_FSYNC_FAULT_WITNESS=' . quote_shell($witness) . "\n" .
		'exec ' . quote_shell("$bin/postgres") . ' "$@"' . "\n");
	chmod 0700, $wrapper or die "chmod $wrapper: $!";
	$node->append_conf('postgresql.conf',
		"cluster.enabled=off\nshared_buffers='16MB'\nautovacuum=off\n" .
		"restart_after_crash=off\ncheckpoint_timeout='1h'\nfsync=on\n" .
		"wal_sync_method=fsync\ndata_sync_retry=" . ($retry ? 'on' : 'off') . "\n");
	if ($shared)
	{
		my $sql_root = $root;
		$sql_root =~ s/'/''/g;
		$node->append_conf('postgresql.conf',
			"cluster.shared_storage_backend=cluster_fs\n" .
			"cluster.shared_data_dir='$sql_root'\ncluster.smgr_user_relations=on\n");
	}
	my $start = system_log('pg_ctl', '-w', '-D', $node->data_dir,
		'-l', $node->logfile, '-p', $wrapper, 'start');
	$node->_update_pid(-1);
	is($start, 0, "$route native storage instance starts") or do {
		diag(slurp_file($node->logfile));
		return;
	};
	$node->safe_psql('postgres', q{
CREATE FUNCTION storage_sync(regclass,boolean) RETURNS text
AS '$libdir/test_pgrac_shared_config', 'test_pgrac_storage_sync' LANGUAGE C STRICT;
CREATE TABLE sync_target(id integer);
INSERT INTO sync_target VALUES (42);
CHECKPOINT;
});
	is($node->safe_psql('postgres', "SELECT storage_sync('sync_target',true)"),
		"$route:synced", 'original smgr dispatch reaches the selected backend');
	my $relative = $node->safe_psql('postgres',
		"SELECT pg_relation_filepath('sync_target')");
	my $file = ($shared ? $root : $node->data_dir) . "/$relative";
	ok(-f $file && -s $file > 0, 'fault target is the real nonempty MAIN file');
	append_to_file($control, "$file\n");
	my ($out, $err);
	my $rc = $node->psql('postgres', "SELECT storage_sync('sync_target',true)",
		stdout => \$out, stderr => \$err, timeout => 15);
	if ($retry)
	{
		is($rc, 0, 'explicit native retry policy raises catchable ERROR');
		like($out, qr/^$route:58030\s*$/, 'caught error is exactly the data I/O error');
	}
	else
	{
		isnt($rc, 0, 'default fsync failure escapes native PG_CATCH and loses connection');
		like(slurp_file($node->logfile),
			$shared ? qr/PANIC:.*cluster_shared_fs\.shared_fs: could not fsync/
			        : qr/PANIC:.*could not fsync file/,
			'original backend fsync reports PANIC');
	}
	ok(-f $witness && slurp_file($witness) =~ /fsync=EIO/,
		'the actual target fsync syscall encountered the injected EIO');
	unlink($control) or die "disarm $control: $!";
	if ($retry)
	{
		is($node->safe_psql('postgres', "SELECT storage_sync('sync_target',true)"),
			"$route:synced", 'only explicit retry policy permits a later successful sync');
		$node->stop('fast');
	}
	else
	{
		my $deadline = time() + 15;
		my $pidfile = $node->data_dir . '/postmaster.pid';
		sleep 0.05 while -e $pidfile && time() < $deadline;
		ok(!-e $pidfile, 'PANIC stops the instance without a recovery restart');
		my $state;
		run ['pg_controldata', $node->data_dir], '>', \$state
			or die 'pg_controldata failed';
		like($state, qr/Database cluster state:\s+in production/,
			'error stop is not a CLEAN shutdown');
		# Clear the TAP cached pid after observing the actual crash. A failure
		# above is retained even if cleanup has to stop a still-running node.
		$node->stop('immediate', fail_ok => 1);
	}
}

for my $shared (1, 0)
{
	for my $retry (0, 1)
	{
		my $name = ($shared ? 'cluster_fs' : 'native md') .
			($retry ? ' explicit retry policy' : ' default PANIC');
		subtest $name => sub {
			native_case($shared, $retry);
		};
	}
}
done_testing();
