#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# Four real requester nodes exercise the existing HW-X S1-S7 path. No relation
# extension, new wire message, synthetic grant or timeout override is used.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterQuad;
use Test::More;
use Time::HiRes qw(usleep time);

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
    'd6_hw_handoff', quorum_voting_disks => 3, shared_data => 1,
    shared_system_identifier => 1,
    shared_system_identifier_seed_sql => 'CREATE TABLE hw_probe (id int);',
    extra_conf => [
        'autovacuum = off', 'cluster.read_scache = on', 'cluster.online_join = on',
        'cluster.quorum_poll_interval_ms = 500', 'cluster.join_convergence_timeout_ms = 30000',
        'cluster.xid_striping = on', 'cluster.crossnode_runtime_visibility = on',
        'cluster.page_scn_shortcut = on', 'cluster.past_image = on',
        'cluster.crossnode_write_write = on', 'cluster.undo_gcs_coherence = on',
        'cluster.crossnode_cr_data_plane = on', 'cluster.gcs_reply_timeout_ms = 3000',
        'cluster.cssd_heartbeat_interval_ms = 2000', 'cluster.cssd_dead_deadband_factor = 10',
        'cluster.ges_handoff = on',
    ]);
my $voting_bytes = (8 * 128 + 3) * 512;
my @voting = $quad->voting_disk_paths;
die 'exact three voting disks required' unless @voting == 3;
for my $path (@voting) { truncate($path, $voting_bytes) or die "$path: $!"; }
for my $node ($quad->nodes) {
    $node->append_conf('postgresql.conf', "cluster.voting_disk_size_bytes = $voting_bytes\n");
}
$quad->start_quad;
for my $from (0 .. 3) {
    for my $to (0 .. 3) {
        next if $from == $to;
        $quad->wait_for_peer_state($from, $to, 'connected', 30) or die 'peer not connected';
    }
}
my $undo_root = $quad->shared_data_root . '/pg_undo';
mkdir $undo_root or die "mkdir $undo_root: $!";
for my $node ($quad->nodes) {
    $node->poll_query_until('postgres', 'SELECT in_quorum FROM pg_cluster_quorum_state', 't')
        or die 'voting majority unavailable';
    my $deadline = time() + 15;
    my ($rc, $out, $err);
    while (time() < $deadline) {
        ($rc, $out, $err) = $node->psql('postgres',
            'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 30);
        last if defined($rc) && $rc != 0 && $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
        usleep(100_000);
    }
    die 'bootstrap did not remain deferred' unless defined($rc) && $rc != 0
        && $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
}
for my $round (0, 10) {
    my $deadline = time() + 60;
    my $opened = 0;
    while (time() < $deadline) {
        my ($rc, $out, $err) = $quad->node0->psql('postgres',
            'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL', timeout => 45);
        if (defined($rc) && $rc == 0) { $opened = 1; last; }
        die "untyped activation error: $err" unless defined($rc)
            && $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
        usleep(100_000);
    }
    die "bit$round activation failed" unless $opened;
}
my $functions = q{
 CREATE FUNCTION test_pgrac_hw_lock(regclass) RETURNS boolean
 AS '$libdir/test_pgrac_r4_native_tx', 'test_pgrac_hw_lock' LANGUAGE C STRICT;
 CREATE FUNCTION test_pgrac_hw_unlock() RETURNS boolean
 AS '$libdir/test_pgrac_r4_native_tx', 'test_pgrac_hw_unlock' LANGUAGE C;
 CREATE FUNCTION test_pgrac_hw_master(regclass) RETURNS integer
 AS '$libdir/test_pgrac_r4_native_tx', 'test_pgrac_hw_master' LANGUAGE C STRICT;
};
for my $node ($quad->nodes) { $node->safe_psql('postgres', $functions); }
my $master = $quad->node0->safe_psql('postgres', "SELECT test_pgrac_hw_master('hw_probe')");
die 'invalid HW master' unless $master =~ /\A[0-3]\z/;
my $master_node = $quad->node($master);
my $relation = $quad->node0->safe_psql('postgres', "SELECT pg_relation_filenode('hw_probe')");
my $db = $quad->node0->safe_psql('postgres', 'SELECT oid FROM pg_database WHERE datname=current_database()');
my $selector = "type=242 AND field1=$db AND field2=$relation AND field4=0";
note "exact HW main-fork resource db=$db rel=$relation master=$master";

sub counter {
    my ($node, $key) = @_;
    my $value = $node->safe_psql('postgres',
        "SELECT value FROM pg_cluster_state WHERE category='xnode_lever' AND key='$key'");
    die "missing $key" unless $value =~ /\A[0-9]+\z/;
    return int($value);
}
sub queue_is {
    my ($waiters, $holders) = @_;
    my $want = "$waiters|$holders";
    return $master_node->poll_query_until('postgres',
        "SELECT COALESCE(sum(nwaiters),0)||'|'||COALESCE(sum(ngranted),0) FROM pg_cluster_grd_entries WHERE $selector", $want);
}
my @before = map { counter($_, 'e1_invariant_violation_count') } $quad->nodes;
my $grants_before = counter($master_node, 'e1_grant_count');
my @sessions = map { $_->background_psql('postgres', on_error_die => 1) } $quad->nodes;
for my $session (@sessions) { $session->query_safe('BEGIN'); }
is($sessions[0]->query_safe("SELECT test_pgrac_hw_lock('hw_probe')"), 't', 'node0 owns actual HW-X');
ok(queue_is(0, 1), 'master has exactly one holder before contention');
for my $node (1 .. 3) {
    $sessions[$node]->query_until(qr/HW_FIRED/, "\\echo HW_FIRED\nSELECT test_pgrac_hw_lock('hw_probe');\n");
    ok($quad->node($node)->poll_query_until('postgres', q{
        SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%test_pgrac_hw_lock%'
          AND pid<>pg_backend_pid() AND wait_event='ClusterRelExtendWait'}, '1'),
       "node$node is really waiting on HW-X");
    ok(queue_is($node, 1), "master preserves ordered $node waiters and one holder");
}
for my $node (0 .. 3) {
    is($sessions[$node]->query_safe('SELECT test_pgrac_hw_unlock()'), 't', "node$node releases original exact hold");
    $sessions[$node]->query_safe('COMMIT');
    if ($node < 3) {
        is($sessions[$node + 1]->query_safe(''), 't', 'next waiter returned through real grant/reply/promote');
        ok(queue_is(2 - $node, 1), 'one successor holder; remaining queue preserved');
    }
}
ok(queue_is(0, 0), 'final exact HW resource has no holders or waiters');
cmp_ok(counter($master_node, 'e1_grant_count') - $grants_before, '==', 3,
       'three queued successors received exactly three master grants');

# The test module's transaction-abort cleanup must use the same S6 release.
# This is a focused holder ROLLBACK, not a process-failure or recovery fixture.
$sessions[0]->query_safe('BEGIN');
$sessions[1]->query_safe('BEGIN');
is($sessions[0]->query_safe("SELECT test_pgrac_hw_lock('hw_probe')"), 't', 'abort leg holder acquires');
$sessions[1]->query_until(qr/HW_ABORT_FIRED/,
    "\\echo HW_ABORT_FIRED\nSELECT test_pgrac_hw_lock('hw_probe');\n");
ok(queue_is(1, 1), 'abort leg successor is accepted before holder rollback');
$sessions[0]->query_safe('ROLLBACK');
is($sessions[1]->query_safe(''), 't', 'holder rollback releases and wakes exact successor');
$sessions[1]->query_safe('ROLLBACK');
ok(queue_is(0, 0), 'both aborted test transactions leave no HW ownership');
cmp_ok(counter($master_node, 'e1_grant_count') - $grants_before, '==', 4,
       'rollback adds exactly one successor grant');
for my $node (0 .. 3) {
    is(counter($quad->node($node), 'e1_invariant_violation_count') - $before[$node], 0,
       "node$node real handoff verifier reports no violation");
    $sessions[$node]->quit;
}
$quad->stop_quad;
done_testing();
