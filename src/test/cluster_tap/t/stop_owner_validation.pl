use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use JSON::PP;
use Test::More;
use Time::HiRes qw(time);

my $result = {started_unix => time, checks => []};
my $path = "$ENV{TESTLOGDIR}/stop-owner-validation-lifecycle.json";
sub save {
    open(my $out, '>', "$path.tmp") or die $!;
    print {$out} JSON::PP->new->canonical->pretty->encode($result);
    close($out) or die $!;
    rename("$path.tmp", $path) or die $!;
}
sub check {
    my ($passed, $label) = @_;
    push @{$result->{checks}}, {passed => $passed ? JSON::PP::true : JSON::PP::false,
                                label => $label};
    ok($passed, $label);
    save();
}

my $cluster;
my $complete = eval {
    $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('stop_owner_validation',
        nodes => 4, blackbox => 1, required_operations => [],
        extra_conf => ['autovacuum=off', "shared_buffers='16MB'",
            'log_error_verbosity=verbose', 'cluster.trace_cleanup=on',
            'cluster.crossnode_runtime_visibility=on',
            'cluster.crossnode_write_write=on',
            'cluster.crossnode_cr_data_plane=on']);
    $cluster->start_cluster;
    my @nodes = $cluster->nodes;
    my $request = {bindir => $ENV{PGRAC_PRE2_BINDIR}, output => $ENV{TESTLOGDIR},
        nodes => [map {+{node => $_, connstr => $nodes[$_]->connstr('postgres')}} 0..3]};
    my $request_path = "$ENV{TESTLOGDIR}/stop-owner-validation-request.json";
    open(my $out, '>', $request_path) or die $!;
    print {$out} JSON::PP->new->canonical->pretty->encode($request);
    close($out) or die $!;
    my $rc = system('python3', "$FindBin::RealBin/../pre2/stop_owner_validation.py", $request_path);
    check($rc == 0, 'client cleanup scenario completes');
    $result->{helper_rc} = $rc;
    $result->{helper_status} = $rc >> 8;
    $result->{helper_signal} = $rc & 127;
    $result->{stop_attempted} = JSON::PP::true;
    save();
    $result->{shutdown_receipt} = $cluster->stop_cluster;
    $result->{node_logs} = [map {
        my $log = $_->logfile;
        my $text = -f $log ? do { open(my $fh, '<', $log); local $/; <$fh> } : '';
        my @fatal_lines = ($text =~ /^.*FATAL:.*$/mg);
        my @expected_disconnect = grep { /connection to client lost/ } @fatal_lines;
        my @unexpected_fatal = grep { !/connection to client lost/ } @fatal_lines;
        my $panic_count = scalar(() = ($text =~ /PANIC:/g));
        my $normal_stop_context_invalid
            = scalar(() = ($text =~ /NORMAL_STOP_CHECKPOINT_CONTEXT_INVALID/g));
        my $si_register_failure
            = scalar(() = ($text =~ /SI broadcaster cannot register normal-stop work/g));
        {node => $_->name, log => $log,
         panic_count => $panic_count,
         fatal_count => scalar @fatal_lines,
         expected_client_disconnect_fatal => scalar @expected_disconnect,
         unexpected_fatal_count => scalar @unexpected_fatal,
         normal_stop_context_invalid => $normal_stop_context_invalid,
         si_register_failure => $si_register_failure}
    } @nodes];
    my $clean_stop_logs = !grep {
        $_->{panic_count} || $_->{unexpected_fatal_count}
            || $_->{normal_stop_context_invalid} || $_->{si_register_failure}
    } @{$result->{node_logs}};
    check($clean_stop_logs,
          'four-node normal stop logs contain no crash or unexpected FATAL');
    1;
};
unless ($complete) {
    $result->{error} = "$@";
    if ($cluster && $cluster->{running} && !$result->{stop_attempted}) {
        $result->{stop_attempted} = JSON::PP::true;
        save();
        eval { $result->{failure_cleanup_receipt} = $cluster->stop_cluster; 1 };
        $result->{cleanup_error} = "$@" if $@;
    }
}
$result->{complete} = $complete ? JSON::PP::true : JSON::PP::false;
$result->{failed_checks} = scalar grep { !$_->{passed} } @{$result->{checks}};
$result->{status} = $complete && !$result->{failed_checks} ? 'PASS' : 'FAIL';
$result->{finished_unix} = time;
save();
ok($complete, 'four-node normal stop completes after client cleanup');
diag($result->{error}) unless $complete;
done_testing();
