#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run-stage4-2node-steady.pl -- spec-4.13 D2 Perl runner
#
# Owns the real 2-node steady-state perf bands that the bash driver
# (run-stage4-recovery-baseline.sh) cannot drive, because ClusterPair must
# be instantiated from the PG TAP perl framework (not bash).
#
# Bands:
#   P0-1  2-node steady-state decay vs 1-node cluster baseline.
#         baseline = single cluster-on node (allow_single_node=on);
#         test     = 2-node ClusterPair with quorum (quorum_voting_disks => 1),
#                    local-affinity / shared-nothing workload — pgbench runs on
#                    node0 against its own data while node1 is up in the cluster.
#         Measures the membership/quorum/IC-heartbeat tax of running in a 2-node
#         cluster, NOT cross-node cluster_fs Cache Fusion block contention (that
#         needs Stage 5 and is not activated at Stage 4 — see spec-4.13 D2 note).
#         tax = (tps_1node - tps_2node) / tps_1node, rw + ro.
#   P0-2  2-node feature-on idle tax (--bands=toggles|all):
#         online_block_recovery=on + online_thread_recovery=on
#         (write_fence_enforcement stays off — enforcement=on steady-state
#         is untestable, forward 4.12b).  idle_tax vs feature-off 2-node.
#
# Each leg: pgbench rw + ro, N reps, MEDIAN tps; any rep with a failed
# transaction marks the band POLLUTED; run-to-run spread > 5% marks UNSTABLE.
# Local numbers are exploratory only; closure verdict trusts clean CI
# artifacts (规则 23).  ≤10% on P0-1 is a target, not an expected GREEN
# (clean-CI history sits at a higher structural floor; see closure).
#
# Output (under --results-dir):
#   perf-stage4-2node-<TS>.json   parsed band summary (spec-4.13 §3.4 bands[])
#   perf-stage4-2node-<TS>.txt    raw pgbench stdout/stderr
#
# Invoked by run-stage4-recovery-baseline.sh (nodes=2) or the perf workflow;
# not run directly by users.
#
# Spec: spec-4.13-stage4-recovery-write-fence-perf-baseline.md
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
use IPC::Run qw(run);

# Load Cluster + ClusterPair in BEGIN so the PostgreSQL::Test INIT blocks
# (Utils portdir resolution) fire before main-time use.
our ($have_cluster, $have_cluster_pair);
BEGIN {
    eval {
        require PostgreSQL::Test::Cluster;
        PostgreSQL::Test::Cluster->import();
        $have_cluster = 1;
    };
    eval {
        require PostgreSQL::Test::ClusterPair;
        PostgreSQL::Test::ClusterPair->import();
        $have_cluster_pair = 1;
    };
}

my ($enable_install, $results_dir, $timestamp, $commit);
my $scale    = 10;
my $duration = 30;
my $clients  = 4;
my $jobs     = 2;
my $reps     = 3;
my $bands    = 'steady';   # steady | toggles | all

GetOptions(
    'enable-install=s' => \$enable_install,
    'results-dir=s'    => \$results_dir,
    'timestamp=s'      => \$timestamp,
    'commit=s'         => \$commit,
    'scale=i'          => \$scale,
    'duration=i'       => \$duration,
    'clients=i'        => \$clients,
    'jobs=i'           => \$jobs,
    'reps=i'           => \$reps,
    'bands=s'          => \$bands,
) or die "usage: $0 --enable-install=DIR --results-dir=DIR --timestamp=TS "
       . "[--commit=SHA --scale=N --duration=SECS --clients=N --jobs=N --reps=N "
       . "--bands=steady|toggles|all]\n";

die "--enable-install required\n" unless $enable_install;
die "--results-dir required\n"    unless $results_dir;
die "--timestamp required\n"      unless $timestamp;
die "ClusterPair module not available (PostgreSQL::Test::ClusterPair) — perf "
  . "workflow needs the PG TAP perl env\n" unless $have_cluster_pair;

$commit ||= 'unknown';
my $pgbench = "$enable_install/bin/pgbench";
die "pgbench not found: $pgbench\n" unless -x $pgbench;

local $ENV{PGRAC_ENABLE_INSTALL} = $enable_install;

my $tag       = "perf-stage4-2node-${timestamp}";
my $json_path = File::Spec->catfile($results_dir, "${tag}.json");
my $txt_path  = File::Spec->catfile($results_dir, "${tag}.txt");
open(my $raw, '>', $txt_path) or die "cannot write $txt_path: $!\n";

sub logmsg { my $m = shift; print "run-stage4-2node-steady.pl: $m\n"; print $raw "$m\n"; }

# ---- numeric helpers (mirror the bash driver) -------------------------
sub median {
    my @s = sort { $a <=> $b } @_;
    return @s ? $s[int(@s / 2)] : 0;
}
# tax% = (base - test) / base * 100, 1 decimal; 'NA' when base<=0.
sub tax {
    my ($b, $t) = @_;
    return ($b <= 0) ? 'NA' : sprintf('%.1f', ($b - $t) / $b * 100);
}
# spread (max-min)/median > 5% -> 1 (unstable) else 0.
sub unstable {
    my @v = @_;
    return 0 unless @v;
    my ($mn, $mx, $s) = ($v[0], $v[0], 0);
    for (@v) { $mn = $_ if $_ < $mn; $mx = $_ if $_ > $mx; $s += $_; }
    my $med = $s / scalar(@v);
    return ($med > 0 && ($mx - $mn) / $med > 0.05) ? 1 : 0;
}

# ---- one pgbench rep against a node, returns (tps, failed, err_reason) -
# Non-fatal: a GCS/IC failure under load (cluster_gcs_block retransmit
# exhausted etc.) returns an err_reason instead of dying, so the harness can
# mark the band UNAVAILABLE and still emit JSON for the other bands.
sub pgbench_once {
    my ($node, $readonly) = @_;
    my @cmd = ($pgbench, ($readonly ? ('-S') : ()),
        '-c', $clients, '-j', $jobs, '-T', $duration, '-n',
        '-h', $node->host, '-p', $node->port, 'postgres');
    my ($out, $err) = ('', '');
    eval { run(\@cmd, '>', \$out, '2>', \$err); 1 }
        or return (undef, 0, 'pgbench exec failed');
    print $raw "--- pgbench ", ($readonly ? 'ro' : 'rw'), " ---\n$out$err\n";
    my ($tps)    = $out =~ /^tps = ([\d.]+)/m;
    my ($failed) = $out =~ /number of failed transactions:\s*(\d+)/;
    unless (defined $tps) {
        my ($eline) = ($out . $err) =~ /((?:ERROR|FATAL|PANIC):[^\n]*)/;
        return (undef, 0, 'no tps (pgbench errored): ' . ($eline // 'unknown'));
    }
    return ($tps, $failed // 0, undef);
}

# ---- N reps rw + ro on a node, returns hashref --------------------------
# { ok => 1, rw_med, ro_med, rw_all, ro_all, failed, unstable }  on success;
# { ok => 0, reason => '...' }  when init/load can't run cleanly (e.g. 2-node
# GCS/IC instability under OLTP load — see spec-4.13 D2 note; clean verdict
# requires a clean Linux CI runner per §1.4, not this box).
sub bench_node {
    my ($node, $label) = @_;
    logmsg("bench $label: pgbench -i -s $scale then $reps rep x (rw+ro) "
         . "-c$clients -j$jobs -T${duration}s");
    my ($iout, $iok) = ('', 0);
    $iok = eval {
        run([ $pgbench, '-i', '-s', $scale, '-q',
              '-h', $node->host, '-p', $node->port, 'postgres' ],
            '>', \$iout, '2>&1');
        1;
    };
    if (!$iok || $iout =~ /\b(ERROR|FATAL|PANIC)\b/) {
        my ($eline) = $iout =~ /((?:ERROR|FATAL|PANIC):[^\n]*)/;
        return { ok => 0, reason => "pgbench -i failed on $label (2-node GCS/IC "
               . "under load?): " . ($eline // 'no error line; exec failed') };
    }
    my (@rw, @ro);
    my $failed = 0;
    for my $r (1 .. $reps) {
        my ($t, $f, $e) = pgbench_once($node, 0);
        return { ok => 0, reason => "rw rep $r on $label: $e" } if defined $e;
        push @rw, $t; $failed += $f;
    }
    for my $r (1 .. $reps) {
        my ($t, $f, $e) = pgbench_once($node, 1);
        return { ok => 0, reason => "ro rep $r on $label: $e" } if defined $e;
        push @ro, $t; $failed += $f;
    }
    return {
        ok       => 1,
        rw_med   => median(@rw),
        ro_med   => median(@ro),
        rw_all   => [@rw],
        ro_all   => [@ro],
        failed   => $failed,
        unstable => unstable(@rw),
    };
}

# verdict from tax% + green/yellow thresholds + failed/unstable.
sub verdict {
    my ($taxpct, $green, $yellow, $failed, $unst) = @_;
    return 'POLLUTED' if $failed;
    return 'UNSTABLE' if $unst;
    return 'SKIP'     if $taxpct eq 'NA';
    return 'GREEN'    if $taxpct <= $green;
    return 'YELLOW'   if $taxpct <= $yellow;
    return 'RED';
}

my @bands_json;

# ---- 2-node ClusterPair bench, lifecycle wrapped (non-fatal) ----------
# Returns a bench_node hashref (ok=>1 with medians, or ok=>0 with reason).
# Wraps start/stop in eval so a ClusterPair failure marks the band UNAVAILABLE
# rather than crashing the harness (1-node band + JSON still emit).
sub run_2node_bench {
    my ($cname, $label, $conf) = @_;
    my $pair;
    my $res = eval {
        $pair = PostgreSQL::Test::ClusterPair->new_pair(
            $cname, quorum_voting_disks => 1, extra_conf => $conf);
        # Start each node with fail_ok so a boot failure (e.g. quorum strict-mode
        # not coming up on a given runner) degrades to UNAVAILABLE instead of a
        # TAP BAIL_OUT that exits the whole runner with no JSON (start_pair would
        # BAIL_OUT on "pg_ctl start failed").
        my $ok0 = $pair->node0->start(fail_ok => 1);
        my $ok1 = $pair->node1->start(fail_ok => 1);
        if (!$ok0 || !$ok1) {
            return { ok => 0, reason => "2-node ClusterPair start failed (node0="
                   . ($ok0 ? 1 : 0) . " node1=" . ($ok1 ? 1 : 0)
                   . "; quorum strict-mode boot or runner env; log "
                   . $pair->node0->logfile . ")" };
        }
        sleep 2;   # let LMON / qvotec drain initial heartbeat
        bench_node($pair->node0, $label);
    };
    my $err = $@;
    eval { $pair->stop_pair if $pair; 1 };
    return $res if ref $res;
    return { ok => 0, reason => "2-node setup failed: "
           . substr(defined $err ? $err : 'unknown', 0, 160) };
}

# Steady tax band; UNAVAILABLE (with reason) when either leg can't run cleanly.
my $UNAVAIL_NOTE = '2-node OLTP not cleanly measurable on this runner; '
    . 'authoritative verdict = clean Linux CI (§1.4). If CI is also UNAVAILABLE, '
    . 'P0-1 reduces to: real 2-node steady-state OLTP needs Stage-4 IC/GCS-under-'
    . 'load robustness (peer IC drops under load -> cluster_gcs_block retransmit '
    . 'exhausted); full cross-node serving is Stage 5.';

# ---- common cluster GUCs ---------------------------------------------
my @feature_off = (
    'fsync = on',
    'synchronous_commit = on',
    'autovacuum = off',
    'cluster.online_block_recovery = off',
    'cluster.online_thread_recovery = off',
    'cluster.write_fence_enforcement = off',
);

# ===== P0-1: 1-node baseline vs 2-node cluster_fs + quorum =============
logmsg("P0-1: single cluster-on node baseline");
# Single cluster-on node: default IC tier (STUB), allow_single_node, and a
# pgrac.conf giving the node its own interconnect_addr so LMON can bind
# (mirrors t/066 + ClusterPair; do NOT force tier1 — that needs a peer addr).
my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $n1 = PostgreSQL::Test::Cluster->new('s4_1node');
$n1->init;
$n1->append_conf('postgresql.conf', join("\n",
    'cluster.enabled = on',
    'cluster.node_id = 0',
    'cluster.allow_single_node = on',
    'cluster.pcm_grd_max_entries = 0',
    'shared_buffers = 16MB',
    @feature_off) . "\n");
PostgreSQL::Test::Utils::append_to_file($n1->data_dir . '/pgrac.conf', <<"EOC");
[cluster]
name = s4_1node

[node.0]
interconnect_addr = 127.0.0.1:$ic_port
EOC
$n1->start;
my $b1 = bench_node($n1, '1node');
$n1->stop;

logmsg("P0-1: 2-node ClusterPair with quorum (local-affinity, shared-nothing)");
# No shared_data: stub (shared-nothing) backend — pgbench on node0 hits its own
# local data while node1 is up in the cluster.  Real cross-node cluster_fs
# Cache Fusion under OLTP load is not activated at Stage 4 (Stage 5); see the
# spec-4.13 D2 note.  Keep quorum to capture the real membership/voting tax.
my $b2 = run_2node_bench('s4_2node', '2node-off', [ @feature_off ]);

my %p1 = (
    id   => 'P0-1', name => '2-node steady-state decay vs 1-node cluster baseline',
    config => { baseline => '1-node cluster (allow_single_node)',
                test => '2-node quorum (local-affinity, shared-nothing)',
                workload => 'pgbench-rw+ro on node0',
                scope_note => 'membership/quorum tax; cross-node Cache Fusion = Stage 5 out' },
);
if ($b1->{ok} && $b2->{ok}) {
    my $rwtax  = tax($b1->{rw_med}, $b2->{rw_med});
    my $rotax  = tax($b1->{ro_med}, $b2->{ro_med});
    my $failed = $b1->{failed} + $b2->{failed};
    my $vr     = verdict($rwtax, 10, 20, $failed, $b2->{unstable});
    logmsg("P0-1 rw_tax=$rwtax% ro_tax=$rotax% verdict=$vr "
         . "(1node_rw=$b1->{rw_med} 2node_rw=$b2->{rw_med})");
    $p1{one_node} = { rw_tps => $b1->{rw_med}, ro_tps => $b1->{ro_med}, failed => $b1->{failed} };
    $p1{two_node} = { rw_tps => $b2->{rw_med}, ro_tps => $b2->{ro_med}, failed => $b2->{failed} };
    $p1{rw_tax_pct} = $rwtax; $p1{ro_tax_pct} = $rotax;
    $p1{rw_runs} = $b2->{rw_all}; $p1{verdict} = $vr;
    $p1{note} = '≤10% is a target, not an expected GREEN — clean-CI structural '
              . 'floor sits higher (3.25); RED/YELLOW points at 4.8ab';
} else {
    my $reason = $b1->{ok} ? $b2->{reason} : $b1->{reason};
    logmsg("P0-1 UNAVAILABLE: $reason");
    $p1{one_node} = $b1->{ok}
        ? { rw_tps => $b1->{rw_med}, ro_tps => $b1->{ro_med}, failed => $b1->{failed} }
        : undef;
    $p1{verdict} = 'UNAVAILABLE';
    $p1{reason}  = $reason;
    $p1{note}    = $UNAVAIL_NOTE;
}
push @bands_json, \%p1;

# ===== P0-2: 2-node feature-on idle tax ===============================
if ($bands eq 'toggles' || $bands eq 'all') {
    logmsg("P0-2: 2-node feature-on (block+thread recovery on, fence off)");
    my $b2on = run_2node_bench('s4_2node_on', '2node-on', [
        'fsync = on', 'synchronous_commit = on', 'autovacuum = off',
        'cluster.online_block_recovery = on',
        'cluster.online_thread_recovery = on',
        'cluster.write_fence_enforcement = off',
    ]);
    my %p2 = (
        id => 'P0-2', name => '2-node feature-on idle tax (block+thread recovery on)',
        config => { nodes => 2, write_fence => 'off' },
    );
    if ($b2->{ok} && $b2on->{ok}) {
        my $tax = tax($b2->{rw_med}, $b2on->{rw_med});
        my $vr  = verdict($tax, 5, 10, $b2on->{failed}, $b2on->{unstable});
        logmsg("P0-2 idle_tax=$tax% verdict=$vr "
             . "(off_rw=$b2->{rw_med} on_rw=$b2on->{rw_med})");
        $p2{feature_off_rw_tps} = $b2->{rw_med}; $p2{feature_on_rw_tps} = $b2on->{rw_med};
        $p2{failed} = $b2on->{failed}; $p2{idle_tax_pct} = $tax;
        $p2{rw_runs} = $b2on->{rw_all}; $p2{verdict} = $vr;
    } else {
        my $reason = $b2->{ok} ? $b2on->{reason} : "feature-off baseline unavailable: $b2->{reason}";
        logmsg("P0-2 UNAVAILABLE: $reason");
        $p2{verdict} = 'UNAVAILABLE'; $p2{reason} = $reason; $p2{note} = $UNAVAIL_NOTE;
    }
    push @bands_json, \%p2;
}

# ---- emit JSON --------------------------------------------------------
my $summary = {
    spec   => '4.13',
    leg    => 'D2-2node',
    commit => $commit,
    timestamp => $timestamp,
    runner => { os => $^O },
    params => { scale => $scale, duration_s => $duration, reps => $reps,
                clients => $clients, jobs => $jobs, nodes => 2, bands => $bands },
    bands  => \@bands_json,
};
open(my $jfh, '>', $json_path) or die "cannot write $json_path: $!\n";
print $jfh JSON::PP->new->canonical->pretty->encode($summary);
close $jfh;
close $raw;

logmsg("summary -> $json_path");
logmsg("done. ($^O local numbers exploratory only; closure trusts clean CI — 规则 23)");
exit 0;
