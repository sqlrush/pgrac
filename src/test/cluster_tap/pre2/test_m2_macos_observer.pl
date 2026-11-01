#!/usr/bin/env perl
# Opt-in observer component test with two independent local postmasters.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);
use Time::HiRes qw(sleep);

die 'this component test requires Darwin' unless $^O eq 'darwin';
my $base = $ENV{PGRAC_M2_OBSERVER_BASE} // die 'missing isolated test root';
my (@nodes, @sessions);
my $blocker;
END {
    eval { $blocker->query_safe('ROLLBACK') } if defined $blocker;
    eval { $_->quit } for reverse @sessions;
    eval { $_->stop('fast') } for reverse @nodes;
}
for my $id (0, 1) {
    my $node = PostgreSQL::Test::Cluster->new("observer_node$id");
    push @nodes, $node;
    $node->init;
    # Only OS/debugger/endpoint behavior is under test, not shared admission.
    $node->append_conf('postgresql.conf', "cluster.enabled=off\ncluster.node_id=$id\nshared_buffers=16MB\n");
    $node->start;
}
# The empty unlogged relation supplies a real lock wait, not DML qualification.
$nodes[0]->safe_psql('postgres', 'CREATE UNLOGGED TABLE m2_roll(v int)');
$blocker = $nodes[0]->background_psql('postgres', timeout => 90);
push @sessions, $blocker;
$blocker->query_safe('BEGIN; LOCK m2_roll IN ACCESS EXCLUSIVE MODE');
my @identities;
for my $id (0, 1) {
    my $writer = $nodes[$id]->background_psql('postgres', timeout => 90);
    push @sessions, $writer;
    my $identity = $writer->query_safe(q{BEGIN; SELECT pg_backend_pid() || '|' || txid_current()});
    die 'missing native producer identity' unless $identity =~ /^([0-9]+)\|([0-9]+)$/;
    push @identities, {pid => 0+$1, xid => 0+$2};
    if ($id == 0) {
        $writer->query_until(qr/PRODUCER_SENT/, "\\echo PRODUCER_SENT\nUPDATE m2_roll SET v=v+1;\n");
    }
}
my $pid = $identities[0]{pid};
my $waiting = 0;
for (1 .. 100) {
    $waiting = $nodes[0]->safe_psql('postgres',
        "SELECT count(*) FROM pg_stat_activity WHERE pid=$pid AND state='active' AND wait_event_type='Lock'");
    last if $waiting eq '1';
    sleep(0.05);
}
is($waiting, '1', 'real producer is executing the registered UPDATE');
mkdir "$base/native-a08-once/testdata/cohort" or die $!;
open my $layout, '>', "$base/native-a08-once/testdata/cohort/blackbox-layout.json" or die $!;
print {$layout} encode_json({nodes => [map { my $id=$_; +{
    # An advisory layout port is deliberately different from the dynamic
    # port allocated by the same Cluster API used by TAP480.
    id=>$id, data_dir=>$nodes[$id]->data_dir, host=>$nodes[$id]->host,
    port=>$nodes[$id]->port == 65535 ? 65534 : $nodes[$id]->port + 1
}} (1, 0)]});
close $layout or die $!;
for my $id (0, 1) {
    my $number = $id == 0 ? 4 : 8;
    open my $file, '>', "$base/native-a08-once/m2-context-$number.json" or die $!;
    print {$file} encode_json({number=>$number, kind=>$id == 0 ? 'A1' : 'A2',
        caller=>'sample_canonical', stage=>'rollover', producer_pid=>$identities[$id]{pid},
        reader_pid=>$identities[1-$id]{pid}, xid=>$identities[$id]{xid},
        component_fixture=>JSON::PP::true});
    close $file or die $!;
}
my ($out, $err) = ('', '');
my $ok = run([$ENV{PYTHON} // 'python3', '-B', "$FindBin::RealBin/m2_macos_observer.py",
    '--base', $base, '--manifest-sha', $ENV{PGRAC_M2_OBSERVER_MANIFEST_SHA}],
    '>', \$out, '2>', \$err, timeout(50));
diag($err) if $err;
my $result = decode_json($out);
ok($ok, 'native observer component driver completed');
ok($result->{positive_attach}, 'observer attached to the actual producer and retained its stack');
ok($result->{correct_endpoint}, 'reversed multi-postmaster layout selects node0 by identity');
ok($result->{actual_dynamic_port}, 'socket and live SQL confirm the original dynamic port');
ok($result->{layout_ignored}, 'the advisory layout port cannot redirect the observer');
ok($result->{negative_refusal}, 'node1 producer is refused against node0 with compared values retained');
is($result->{attachments}, 1, 'only the one qualified capture was attached');

$blocker->query_safe('ROLLBACK');
for my $session (reverse @sessions[1 .. $#sessions]) { $session->query_safe('ROLLBACK'); $session->quit; }
$blocker->quit;
@sessions = ();
$blocker = undef;
for my $node (reverse @nodes) { $node->stop('fast'); }
ok(!grep({ -e $_->data_dir . '/postmaster.pid' } @nodes), 'both owned postmasters stopped normally');
@nodes = ();
done_testing();
