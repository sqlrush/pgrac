#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: check the process-build support boundary before cluster startup.
use strict;
use warnings;
use IPC::Run qw(run timeout);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;

my ($flags, $err) = ('', '');
run(['pg_config', '--cppflags'], '>', \$flags, '2>', \$err)
  or die "pg_config: $err";
my $exec_backend = $flags =~ /(?:^|\s)-DEXEC_BACKEND(?:\s|=|$)/;
note("EXEC_BACKEND=" . ($exec_backend ? 'yes' : 'no'));

my $native = PostgreSQL::Test::Cluster->new('native_process_build');
$native->init;
$native->append_conf('postgresql.conf',
  "cluster.enabled = off\ncluster.shared_config = off\n");
my $native_started = $native->start(fail_ok => 1);
ok($native_started, 'ordinary PostgreSQL starts in this process build');
if ($native_started)
{
    is($native->safe_psql('postgres', 'SELECT 42'), '42',
      'ordinary PostgreSQL accepts SQL in this process build');
    $native->stop('fast');
}

for my $input ('configuration', 'command_line')
{
    my $node = PostgreSQL::Test::Cluster->new("cluster_$input");
    $node->init;
    $node->append_conf('postgresql.conf',
      "cluster.enabled = " . ($input eq 'configuration' ? 'on' : 'off') .
      "\ncluster.shared_config = off\nlog_error_verbosity = verbose\n");
    my @options = $input eq 'command_line' ? ('-c', 'cluster.enabled=on') : ();
    my ($out, $stderr) = ('', '');
    ok(run(['postgres', '-D', $node->data_dir, @options, '-C', 'cluster.enabled'],
        '>', \$out, '2>', \$stderr, timeout(30)),
      "$input read-only configuration query succeeds");
    like($out, qr/^(?:on|true)\s*$/, "$input query sees the effective setting");

    my @start = ('pg_ctl', '-D', $node->data_dir, '-l', $node->logfile,
      '-w', '-t', '30');
    push @start, ('-o', join(' ', @options)) if @options;
    push @start, 'start';
    my $started = run(\@start, '>', \$out, '2>', \$stderr, timeout(35));
    $node->_update_pid(-1);
    my $log = slurp_file($node->logfile);
    if ($exec_backend)
    {
        ok(!$started, "$input EXEC cluster startup is refused");
        like($log, qr/FATAL:\s+0A000:.*cluster mode is not supported in an EXEC_BACKEND build/,
          "$input refusal has the supported SQLSTATE and message");
        like($log, qr/reason=CLUSTER_EXEC_BACKEND_UNSUPPORTED/,
          "$input refusal has a precise diagnostic reason");
        like($log, qr/Use a build without EXEC_BACKEND to start this cluster\./,
          "$input hint describes the supported build");
        unlike($log, qr/(?:PANIC:|signal 11|Segmentation fault|cluster startup:)/,
          "$input refusal precedes cluster phases and child failures");
    }
    else
    {
        ok($started, "$input fork-based cluster startup remains supported");
        is($node->safe_psql('postgres', 'SHOW cluster.enabled'), 'on',
          "$input fork-based cluster accepts SQL");
    }
    $node->stop('fast') if defined($node->{_pid});
}
done_testing();
