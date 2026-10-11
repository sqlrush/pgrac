#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: reject unsupported process builds without mutating a fresh shared cohort.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use IPC::Run qw(run);
use Digest::SHA;
use File::Find;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

sub durable_files
{
    my ($cluster) = @_;
    my @paths;
    find({no_chdir => 1, wanted => sub {
        push @paths, $File::Find::name if -f $File::Find::name;
    }}, $cluster->shared_root, $cluster->wal_root);
    push @paths, @{$cluster->{layout}{voting_disks}};
    push @paths, map {$_->data_dir . '/global/pg_control'} $cluster->nodes;
    my %hashes;
    for my $path (sort @paths)
    {
        open my $in, '<', $path or die "$path: $!";
        binmode($in);
        $hashes{$path} = Digest::SHA->new(256)->addfile($in)->hexdigest;
        close $in or die "$path: $!";
    }
    return \%hashes;
}

my $flags = '';
run(['pg_config', '--cppflags'], '>', \$flags) or die 'pg_config failed';
my $exec_backend = $flags =~ /(?:^|\s)-DEXEC_BACKEND(?:\s|=|$)/;
my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('pre2_exec_support',
  nodes => 4, blackbox => 1, required_operations => [],
  extra_conf => ['autovacuum = off', 'log_error_verbosity = verbose']);
my $before = durable_files($cluster);
my $started = eval {$cluster->start_cluster; 1};
my $error = $@;
if ($exec_backend)
{
    ok(!$started, 'EXEC build refuses a valid shared cohort');
    # The shared launcher starts every member without waiting. Its first
    # refusal can return before another member has written its own log.
    my $deadline = clock_gettime(CLOCK_MONOTONIC) + 30;
    for my $node ($cluster->nodes)
    {
        while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
        {
            my $log = slurp_file($node->logfile);
            last if $log =~ /(?:FATAL:|PANIC:|signal 11|Segmentation fault)/
              && !-e $node->data_dir . '/postmaster.pid';
            sleep(0.01);
        }
    }
    for my $node ($cluster->nodes)
    {
        my $log = slurp_file($node->logfile);
        like($log, qr/FATAL:\s+0A000:.*cluster mode is not supported in an EXEC_BACKEND build/,
          'shared node has the explicit unsupported-build refusal');
        like($log, qr/Use a build without EXEC_BACKEND to start this cluster\./,
          'shared node has an actionable build hint');
        unlike($log, qr/(?:PANIC:|signal 11|Segmentation fault|cluster startup:)/,
          'shared refusal precedes cluster phases and child failures');
        ok(!-e $node->data_dir . '/postmaster.pid', 'refused postmaster has exited');
    }
    is_deeply(durable_files($cluster), $before,
      'shared files, WAL, native controls and voting disks are unchanged');
}
else
{
    ok($started, 'fork build starts the valid shared cohort');
    diag($error) unless $started;
    if ($started)
    {
        for my $node ($cluster->nodes)
        {
            is($node->safe_psql('postgres', 'SELECT 42'), '42',
              'fork-based shared node accepts SQL');
        }
        $cluster->stop_cluster;
    }
}
done_testing();
