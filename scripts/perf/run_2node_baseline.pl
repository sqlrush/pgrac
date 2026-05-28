#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run_2node_baseline.pl -- spec-3.4e D2 Perl runner
#
# Owns PostgreSQL::Test::ClusterPair lifecycle + pgbench --log parsing +
# 5 metric collection (TPS / p95 latency / WAL bytes/sec / fail_closed
# rate / lookup_hit_miss ratio).
#
# Modes (spec-3.4e v0.2 §2.1 workload classes 2/3/4):
#   2node-local-affinity          class 2 (warn-only)
#   2node-cross-node-visibility   class 3 (PARTIAL — D5b COMMITTED inject)
#   2node-hot-row-lock            class 4 (PARTIAL — D5b ACTIVE inject)
#
# Output (under --results-dir):
#   3-4e-<class>-<TS>.json   — parsed 5 metric summary
#   3-4e-<class>-<TS>.log    — raw pgbench --log transaction logs
#   3-4e-<class>-<TS>.txt    — pgbench stdout/stderr captured
#
# Invoked by run-2node-baseline.sh; not run directly by users.
#
# Spec: spec-3.4e-stage3-multinode-perf-hardening.md (v0.2 FROZEN 2026-05-25)
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";

use Getopt::Long;
use File::Spec;
use JSON::PP;

# ClusterPair fixture (spec-3.4b/c/d 既有,本 spec 直接复用 — F4 修正)
# PGRAC: load in BEGIN so PostgreSQL::Test::Utils + Cluster INIT blocks fire
# (Hardening v1.0.1 — Too-late-INIT fix for portdir resolution).
# Declare with `our` (no `= 0`) so BEGIN-time assignment isn't overwritten
# by main-time initialization.
our $have_cluster_pair;
BEGIN {
    eval {
        require PostgreSQL::Test::ClusterPair;
        PostgreSQL::Test::ClusterPair->import();
        $have_cluster_pair = 1;
    };
}

my $mode;
my $scale = 10;
my $duration = 30;
my $clients = 4;
my $jobs = 2;
my $enable_install;
my $results_dir;
my $timestamp;

GetOptions(
    'mode=s'            => \$mode,
    'scale=i'           => \$scale,
    'duration=i'        => \$duration,
    'clients=i'         => \$clients,
    'jobs=i'            => \$jobs,
    'enable-install=s'  => \$enable_install,
    'results-dir=s'     => \$results_dir,
    'timestamp=s'       => \$timestamp,
) or die "usage: $0 --mode=<class> --enable-install=DIR --results-dir=DIR --timestamp=TS [--scale=N --duration=SECS --clients=N --jobs=N]\n";

die "--mode required\n" unless $mode;
die "--enable-install required\n" unless $enable_install;
die "--results-dir required\n" unless $results_dir;
die "--timestamp required\n" unless $timestamp;
die "ClusterPair module not available (PostgreSQL::Test::ClusterPair) — perf workflow needs PG TAP perl env\n"
    unless $have_cluster_pair;

my $tag = "3-4e-${mode}-${timestamp}";
my $json_path = File::Spec->catfile($results_dir, "${tag}.json");
my $log_path  = File::Spec->catfile($results_dir, "${tag}.log");
my $txt_path  = File::Spec->catfile($results_dir, "${tag}.txt");

print "run_2node_baseline.pl: mode=$mode scale=$scale duration=${duration}s clients=$clients jobs=$jobs\n";
print "  enable=$enable_install results=$results_dir tag=$tag\n";

# ---- ClusterPair start ------------------------------------------------
local $ENV{PGRAC_ENABLE_INSTALL} = $enable_install;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
    "perf_${mode}",
    extra_conf => [
        'autovacuum = off',
        'cluster.pcm_grd_max_entries = 0',
        'cluster.tt_status_overlay_max_entries = 1048576',
        'shared_buffers = 16MB',
    ],
);
$pair->start_pair;

sleep 2;  # let LMON drain initial heartbeat

my $node0 = $pair->node0;
my $node1 = $pair->node1;

# ---- pgbench init -----------------------------------------------------
$node0->safe_psql('postgres', 'SELECT 1') or die "node0 not alive\n";
$node1->safe_psql('postgres', 'SELECT 1') or die "node1 not alive\n";

print "run_2node_baseline.pl: pgbench -i scale=$scale on node0\n";
$node0->command_ok(
    [ 'pgbench', '-i', '-s', $scale, 'postgres' ],
    'pgbench init scale ok',
);
if ($mode eq '2node-local-affinity' || $mode eq '2node-subxact-nesting') {
    print "run_2node_baseline.pl: pgbench -i scale=$scale on node1\n";
    $node1->command_ok(
        [ 'pgbench', '-i', '-s', $scale, 'postgres' ],
        'pgbench init scale ok on node1',
    );
}

# ---- mode-specific setup ----------------------------------------------
my $script_path;
my @pgbench_extra_args;

if ($mode eq '2node-local-affinity') {
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'local_affinity.sql');
}
elsif ($mode eq '2node-cross-node-visibility') {
    # spec-3.4e §2.3 partial coverage:same-node D5b COMMITTED inject + force GUC
    # Pre-flight inject on node0 + force cluster path
    $node0->safe_psql('postgres', q{
        DROP TABLE IF EXISTS perf_3_4e_visibility;
        CREATE TABLE perf_3_4e_visibility(id int PRIMARY KEY, payload text);
        INSERT INTO perf_3_4e_visibility VALUES (1, 'committed');
    });
    my $xid = $node0->safe_psql('postgres',
        q{SELECT xmin::text::int FROM perf_3_4e_visibility WHERE id = 1});
    $node0->safe_psql('postgres',
        qq{SELECT cluster_test_inject_visibility_tt_ref('$xid'::xid, 7, 3, 42, 0, 100::int8, false)});
    $node0->safe_psql('postgres',
        q{ALTER SYSTEM SET cluster_test_force_visibility_cluster_path = on});
    $node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'cross_node_visibility_inject.sql');
}
elsif ($mode eq '2node-hot-row-lock') {
    # spec-3.4e §2.4 partial coverage:D5b ACTIVE inject + force GUC + SELECT FOR UPDATE
    $node0->safe_psql('postgres', q{
        DROP TABLE IF EXISTS perf_3_4e_hot_row;
        CREATE TABLE perf_3_4e_hot_row(id int PRIMARY KEY, payload text);
        INSERT INTO perf_3_4e_hot_row VALUES (1, 'locked');
    });
    my $xid = $node0->safe_psql('postgres',
        q{SELECT xmin::text::int FROM perf_3_4e_hot_row WHERE id = 1});
    $node0->safe_psql('postgres',
        qq{SELECT cluster_test_inject_visibility_tt_ref('$xid'::xid, 7, 3, 42, 0, 0::int8, true)});
    $node0->safe_psql('postgres',
        q{ALTER SYSTEM SET cluster_test_force_visibility_cluster_path = on});
    $node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'hot_row_select_for_update.sql');
}
elsif ($mode eq '2node-subxact-nesting') {
    # spec-3.5 class 5: savepoint depth=5 nesting workload.  Initialise
    # pgbench baseline tables (UPDATE pgbench_accounts inside the
    # innermost savepoint) so we measure subxact cost rather than IC
    # traffic.  Cap chain depth via GUC for fair comparison; default
    # 32 covers depth=5 with headroom.
    $node0->safe_psql('postgres',
        'ALTER SYSTEM SET cluster.subtrans_max_chain_depth = 32');
    $node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'subxact_nesting.sql');
}
elsif ($mode eq '2node-multixact-shared-lock') {
    # spec-3.6 class 6: concurrent FOR SHARE + UPDATE workload to
    # exercise MultiXact composition + cluster overlay emit/lookup +
    # resolve_visibility truth table.  Default GUC cap (32 members)
    # covers typical share-lock workloads.
    $node0->safe_psql('postgres',
        'ALTER SYSTEM SET cluster.multixact_member_overlay_max_members = 32');
    $node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'multixact_shared_lock.sql');
}
elsif ($mode eq '2node-undo-write-pressure') {
    # spec-3.7 class 7: single-backend 4 op DML mix to exercise
    # cluster_undo_record_alloc + per-instance segment + durable flush
    # (per W2 self-contained no-WAL ordering).  MVP: heap_insert real
    # undo emit; UPDATE / SELECT FOR SHARE / DELETE fall through TT-only
    # path under MVP (Hardening v1.0.2 H-4/5/6 amend pending).
    $node0->safe_psql('postgres',
        'ALTER SYSTEM SET cluster.undo_record_inline_max_bytes = 1024');
    $node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
    $script_path = File::Spec->catfile($FindBin::RealBin, 'scripts', 'undo_write_pressure.sql');
}
else {
    $pair->stop_pair;
    die "unknown mode: $mode\n";
}

die "pgbench script not found: $script_path\n" unless -f $script_path;

# ---- baseline counter snapshot before pgbench -------------------------
sub snapshot_counters {
    my $node = shift;
    my %c;
    for my $name (qw(
        cluster_remote_row_lock_fail_closed_count
        cluster_lock_only_itl_stamp_count
    )) {
        my $v = eval {
            $node->safe_psql('postgres',
                qq{SELECT value::bigint FROM pg_cluster_state WHERE key = '$name'});
        };
        $c{$name} = (defined $v && $v ne '') ? int($v) : 0;
    }
    # cluster_tt_status counters (shmem already — spec-3.1 D2)
    for my $name (qw(lookup_hit_count lookup_miss_count)) {
        my $v = eval {
            $node->safe_psql('postgres',
                qq{SELECT value::bigint FROM pg_cluster_state WHERE category='tt_status' AND key = '$name'});
        };
        $c{"tt_status_$name"} = (defined $v && $v ne '') ? int($v) : 0;
    }
    # pg_stat_wal.wal_bytes
    my $wal = eval {
        $node->safe_psql('postgres',
            q{SELECT wal_bytes::text FROM pg_stat_wal});
    };
    $c{wal_bytes} = (defined $wal && $wal ne '') ? int($wal) : 0;
    return \%c;
}

my $c_before = snapshot_counters($node0);

# ---- pgbench run with --log -------------------------------------------
my $pgbench_log_prefix = File::Spec->catfile($results_dir, "${tag}-pgbench");
print "run_2node_baseline.pl: pgbench -c $clients -j $jobs -T $duration\n";

sub start_pgbench {
    my ($node, $label, $extra_args) = @_;
    my $prefix = "${pgbench_log_prefix}-${label}";
    my $out_path = File::Spec->catfile($results_dir, "${tag}-${label}.txt");
    my @cmd = (
        'pgbench',
        '-f', $script_path,
        '-c', $clients,
        '-j', $jobs,
        '-T', $duration,
        '-P', 5,
        '--log',
        '--sampling-rate=1.0',
        '--log-prefix', $prefix,
        @$extra_args,
        $node->connstr('postgres'),
    );

    my $pid = fork();
    die "fork failed:$!\n" unless defined $pid;
    if ($pid == 0) {
        open(STDOUT, '>', $out_path) or die "cannot write $out_path:$!\n";
        open(STDERR, '>&', \*STDOUT) or die "cannot dup stderr:$!\n";
        exec @cmd;
        die "exec pgbench failed:$!\n";
    }
    return ($pid, $out_path);
}

my @children;
if ($mode eq '2node-local-affinity' || $mode eq '2node-subxact-nesting'
    || $mode eq '2node-multixact-shared-lock')
{
    # spec-3.5 D16 / spec-3.6 D13: subxact-nesting + multixact-shared-lock
    # reuse local-affinity fanout (both nodes + per-node node_id).
    push @children, [ start_pgbench($node0, 'node0', [ '-D', 'node_id=0' ]) ];
    push @children, [ start_pgbench($node1, 'node1', [ '-D', 'node_id=1' ]) ];
} else {
    push @children, [ start_pgbench($node0, 'node0', \@pgbench_extra_args) ];
}

my $pgbench_failed = 0;
for my $child (@children) {
    my ($pid, $out_path) = @$child;
    waitpid($pid, 0);
    if ($? != 0) {
        warn "pgbench child pid=$pid output=$out_path failed with status $?\n";
        $pgbench_failed = 1;
    }
}
Test::More::ok(!$pgbench_failed, "pgbench class $mode ok");

# ---- baseline counter snapshot after pgbench --------------------------
my $c_after = snapshot_counters($node0);

# ---- metric derivation ------------------------------------------------
sub delta { return ($c_after->{$_[0]} || 0) - ($c_before->{$_[0]} || 0); }

my $wal_bytes_per_sec = delta('wal_bytes') / $duration;
my $fail_closed_rate  = delta('cluster_remote_row_lock_fail_closed_count') / $duration;
my $hit  = delta('tt_status_lookup_hit_count');
my $miss = delta('tt_status_lookup_miss_count');
my $hit_miss_ratio = ($hit + $miss) > 0 ? ($hit / ($hit + $miss)) : 0;

# ---- p95 latency from pgbench --log transaction logs ------------------
my @latencies;
opendir(my $dh, $results_dir);
for my $f (readdir $dh) {
    next unless $f =~ /^\Q$tag\E-pgbench/;
    open(my $fh, '<', File::Spec->catfile($results_dir, $f)) or next;
    while (<$fh>) {
        # pgbench --log format: client_id transaction_no time script_no time_epoch time_us latency_us
        if (/^\d+ \d+ (\d+)/) { push @latencies, $1 / 1000.0; }  # us -> ms
    }
    close $fh;
}
closedir $dh;

my $p95_ms = 'TBD';
if (@latencies) {
    my @sorted = sort { $a <=> $b } @latencies;
    my $idx = int(0.95 * scalar(@sorted));
    $p95_ms = $sorted[$idx] || $sorted[-1];
}
my $aggregate_tps = @latencies ? (scalar(@latencies) / $duration) : 0;

# ---- ClusterPair teardown ---------------------------------------------
$pair->stop_pair;

# ---- emit JSON summary ------------------------------------------------
my $summary = {
    spec => 'spec-3.4e v0.2',
    mode => $mode,
    timestamp => $timestamp,
    scale => $scale,
    duration => $duration,
    clients => $clients,
    jobs => $jobs,
    metrics => {
        tps => $aggregate_tps,
        wal_bytes_per_sec => $wal_bytes_per_sec,
        fail_closed_rate_per_sec => $fail_closed_rate,
        tt_lookup_hit_miss_ratio => $hit_miss_ratio,
        tt_lookup_hit_count_delta => $hit,
        tt_lookup_miss_count_delta => $miss,
        p95_latency_ms => $p95_ms,
        latency_sample_count => scalar(@latencies),
    },
    partial_coverage => ($mode eq '2node-cross-node-visibility'
                          || $mode eq '2node-hot-row-lock'
                          || $mode eq '2node-subxact-nesting'
                          || $mode eq '2node-multixact-shared-lock') ? 1 : 0,
    notes => 'spec-3.4e v0.2 §2 — class 3/4 partial coverage:inject-based;真 cross-node shared heap TPS contention 推 feature-117 / Stage 4+',
};

open(my $jfh, '>', $json_path) or die "cannot write $json_path:$!\n";
print $jfh encode_json($summary), "\n";
close $jfh;

print "run_2node_baseline.pl: summary -> $json_path\n";
print "  tps=$aggregate_tps\n";
print "  wal_bytes/sec=$wal_bytes_per_sec\n";
print "  fail_closed_rate=$fail_closed_rate\n";
print "  tt_hit_miss_ratio=$hit_miss_ratio (hit=$hit miss=$miss)\n";
print "  p95_latency_ms=$p95_ms (samples=" . scalar(@latencies) . ")\n";

# PGRAC: declare TAP plan so exit status reflects ok-count, not "no plan"
# (Hardening v1.0.1 — done_testing fix; previously caused exit 254 even when
# both PG TAP ok() calls succeeded).
Test::More::done_testing();
exit 0;
