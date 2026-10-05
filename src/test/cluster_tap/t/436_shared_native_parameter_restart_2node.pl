#-------------------------------------------------------------------------
#
# 436_shared_native_parameter_restart_2node.pl
#    A shared cluster cannot change a parameter that its control file
#    records at creation.  ALTER SYSTEM refuses these parameters with
#    55R07; a start whose value differs anyway (here from each member's own
#    configuration file) is refused with 55R07 before its first durable
#    write, and restoring the value lets the cluster start again.
#
#      L1   ALTER SYSTEM SET/RESET max_connections is refused with 55R07 on
#           both members and changes nothing; another common parameter is
#           still accepted
#      L2   with a recorded parameter changed in every member's own
#           configuration, the start is refused with 55R07, no member
#           reaches readiness or PANICs, and the control files, the control
#           ROOT and every WAL file are byte-for-byte unchanged
#      L3   with the value restored, the cluster starts and serves SQL
#
# IDENTIFICATION
#    src/test/cluster_tap/t/436_shared_native_parameter_restart_2node.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use Digest::SHA;
use File::Find;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(sleep);

my $cluster;
my @nodes;

# A failed leg still stops the members it started.
END
{
	if (defined($cluster) && $cluster->{running})
	{
		my $rc = $?;
		eval { $cluster->stop_cluster; 1 } or diag("cleanup stop failed: $@");
		$? = $rc;
	}
}

sub sql { return $_[0]->safe_psql('postgres', $_[1], timeout => 60); }

sub setting
{
	return sql($_[0], "SELECT setting || '|' || source || '|' || pending_restart "
		  . "FROM pg_settings WHERE name = '$_[1]'");
}

# Durable state a refused start must not touch: every member's control
# file, the shared control ROOT and control file, and every WAL file.
sub durable_state
{
	my %digest;
	my @files = map { $_->data_dir . '/global/pg_control' } @nodes;
	push @files, grep { -f $_ } map { $cluster->shared_root . "/global/$_" }
	  qw(pgrac_control_root pgrac_control_root.bak pg_control);
	find({ no_chdir => 1, wanted => sub { push @files, $_ if -f $_ } }, $cluster->wal_root);
	$digest{$_} = Digest::SHA->new(256)->addfile($_)->hexdigest for @files;
	return \%digest;
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('shared_native_param', nodes => 2,
	blackbox => 1, extra_conf => ['autovacuum = off']);
$cluster->start_cluster;
@nodes = $cluster->nodes;
my @created = map { setting($_, 'max_connections') } @nodes;

# L1: ALTER SYSTEM refuses the recorded parameter before any publication.
for my $i (0 .. $#nodes)
{
	for my $stmt ('SET max_connections = 300', 'RESET max_connections')
	{
		my ($rc, $out, $err) = $nodes[$i]->psql('postgres',
			"\\set VERBOSITY verbose\nALTER SYSTEM $stmt", timeout => 60);
		isnt($rc, 0, "L1 node$i: ALTER SYSTEM $stmt is refused");
		like($err, qr/ERROR:  55R07: parameter "max_connections" cannot be changed in a shared cluster/,
			"L1 node$i: ALTER SYSTEM $stmt reports 55R07");
	}
}
sql($nodes[0], "ALTER SYSTEM SET work_mem = '5MB'");
sql($nodes[0], 'ALTER SYSTEM RESET work_mem');
pass('L1 another common parameter is still accepted by ALTER SYSTEM');
is(setting($nodes[$_], 'max_connections'), $created[$_],
	"L1 node$_: max_connections and its pending state are unchanged") for 0 .. $#nodes;

# L2: change a recorded parameter outside ALTER SYSTEM.  Use one that no
# configuration source sets yet, so that each member's own file decides it.
my ($name, $recorded);
for my $candidate ('max_locks_per_transaction', 'max_wal_senders', 'max_worker_processes')
{
	my @state = map { setting($_, $candidate) } @nodes;
	next if grep { (split /\|/)[1] ne 'default' } @state;
	($name, $recorded) = ($candidate, (split /\|/, $state[0])[0]);
	last;
}
die 'no recorded parameter is left at its default on every member' unless defined($name);
my $changed = $recorded + 7;
$cluster->stop_cluster;
my %conf = map { $_->name => slurp_file($_->data_dir . '/postgresql.conf') } @nodes;
$_->append_conf('postgresql.conf', "$name = $changed\n") for @nodes;
my $before = durable_state();
my @log_start = map { -s $_->logfile // 0 } @nodes;

my $started = eval { $cluster->start_cluster; 1 };
ok(!$started, "L2 a start with $name changed is refused");
$cluster->stop_cluster if $started;
$cluster->{running} = 0;
for (1 .. 600)
{
	last unless grep { -e $_->data_dir . '/postmaster.pid' } @nodes;
	sleep(0.1);
}
for my $i (0 .. $#nodes)
{
	next unless -e $nodes[$i]->data_dir . '/postmaster.pid';
	diag("node$i was still starting after the refusal; stopping it");
	$nodes[$i]->_update_pid(-1);
	$nodes[$i]->stop('immediate', fail_ok => 1);
}
my $refused = 0;
for my $i (0 .. $#nodes)
{
	my $log = slurp_file($nodes[$i]->logfile, $log_start[$i]);
	unlike($log, qr/PANIC:/, "L2 node$i has no PANIC");
	unlike($log, qr/database system is ready to accept connections/,
		"L2 node$i does not reach readiness");
	next unless $log =~ /FATAL:.*shared cluster parameters recorded at creation cannot be changed/;
	$refused++;
	like($log, qr/PGRAC_REASON=NATIVE_PARAMETER_CHANGE \Q$name=$changed (created with $recorded)\E\./,
		"L2 node$i names the change and its creation value");
	like($log, qr/HINT:.*creation values/, "L2 node$i says how to recover");
}
ok($refused >= 1, 'L2 the start ends with the recorded-parameter refusal');
my $after = durable_state();
is_deeply([sort keys %$after], [sort keys %$before], 'L2 no WAL or control file was created or removed');
for my $file (sort keys %$before)
{
	is($after->{$file}, $before->{$file}, "L2 unchanged: $file");
}

# L3: restore each member's configuration and start again.
for my $node (@nodes)
{
	open(my $fh, '>', $node->data_dir . '/postgresql.conf') or die $!;
	print {$fh} $conf{ $node->name };
	close($fh) or die $!;
}
$cluster->start_cluster;
for my $i (0 .. $#nodes)
{
	is((split /\|/, setting($nodes[$i], $name))[0], $recorded,
		"L3 node$i runs with the creation value of $name");
	is(sql($nodes[$i], 'SELECT 1'), '1', "L3 node$i serves SQL after the restored start");
}
$cluster->stop_cluster;

done_testing();
