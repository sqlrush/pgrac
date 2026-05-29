#!/usr/bin/env perl
# spec-3.8 Fix #376 — class 8 baseline: autoextend latency tax
use strict;
use warnings;
$| = 1;
use FindBin;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep gettimeofday tv_interval);

my $pair = PostgreSQL::Test::ClusterPair->new_pair('class_8_baseline',
    extra_conf => [
        'cluster.pcm_grd_max_entries = 0',
        'cluster.undo_segments_max_per_instance = 256',
        'cluster.undo_segment_create_timeout_ms = 5000',
        'max_prepared_transactions = 4',
    ]);
$pair->start_pair;
print STDERR "DEBUG: pair started\n";
usleep(2_000_000);

my $node = $pair->node0;
print STDERR "DEBUG: creating table\n";
eval { $node->safe_psql('postgres', 'CREATE TABLE t_perf (id int, payload text)'); };
if ($@) { print STDERR "ERR create: $@\n"; $pair->stop_pair; exit 1; }
print STDERR "DEBUG: table created\n";

eval { $node->safe_psql('postgres', "INSERT INTO t_perf VALUES (0, 'seed')"); };
if ($@) { print STDERR "ERR seed: $@\n"; $pair->stop_pair; exit 1; }
print STDERR "DEBUG: seed done\n";

my $pre = $node->safe_psql('postgres',
    q{SELECT format('%s|%s|%s|%s',
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='autoextend_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_switch_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_create_fail_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_hard_cap_fail_count'))});
print "PRE: $pre\n";

my $t0 = [gettimeofday];
my $cycles = 0;
my @latencies;
while (tv_interval($t0) < 30) {
    my $iter_t0 = [gettimeofday];
    if ($cycles % 10 == 0) {
        eval { $node->safe_psql('postgres', q{SELECT cluster_undo_test_force_segment_end()}); };
        last if $@;
    }
    my ($rc, undef, undef) = $node->psql('postgres',
        q{INSERT INTO t_perf VALUES (1, repeat('x', 600))},
        on_error_die => 0);
    last if $rc != 0;
    my $iter_ms = tv_interval($iter_t0) * 1000;
    push @latencies, $iter_ms;
    $cycles++;
}
my $elapsed = tv_interval($t0);
my $tps = $elapsed > 0 ? $cycles / $elapsed : 0;

@latencies = sort { $a <=> $b } @latencies;
my $p50 = @latencies ? $latencies[int(@latencies * 0.50)] : 0;
my $p95 = @latencies ? $latencies[int(@latencies * 0.95)] : 0;
my $p99 = @latencies ? $latencies[int(@latencies * 0.99)] : 0;

my $post = $node->safe_psql('postgres',
    q{SELECT format('%s|%s|%s|%s',
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='autoextend_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_switch_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_create_fail_count'),
        (SELECT value FROM pg_cluster_state WHERE category='undo' AND key='segment_hard_cap_fail_count'))});
print "POST: $post\n";

printf("RESULT cycles=%d elapsed=%.2fs tps=%.2f p50=%.2fms p95=%.2fms p99=%.2fms\n",
       $cycles, $elapsed, $tps, $p50, $p95, $p99);

$pair->stop_pair;
