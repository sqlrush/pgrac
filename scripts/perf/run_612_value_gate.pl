#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run_612_value_gate.pl
#    spec-6.12 D0-shared -- per-wave value-gate comparison harness.
#
#    Runs the spec-5.59 four-axis profiling harness twice -- baseline
#    (wave GUC off) and candidate (wave GUC on) -- and reports the
#    per-counter deltas that decide the wave's GO / NO-GO:
#      wave c  cluster.page_scn_shortcut     r_tt_visibility_resolve n/ns
#      wave a  cluster.read_scache           reship rate / sholder_hit
#      wave e1 cluster.ges_handoff           w_ges_wait / w_ges_wake
#      wave e2 cluster.ges_bast              w_ges_wait (on top of e1)
#      wave d  cluster.space_affinity=static w_hw_extend + extend counts
#      wave f  cluster.index_leaf_affinity   i_index_block_xfer /
#                                            i_rightmost_leaf_ping
#
#    Attribution honesty (spec-5.59 contract carried over): decision
#    buckets compare as pp-relevant; service buckets compare raw only.
#    The verdict line reports counter deltas, never invents a tax%.
#
#    Env knobs:
#      VG_WAVE      required: c | a | e1 | e2 | d | f
#      VG_SECS/VG_ROUNDS/VG_SCALE  forwarded as XP_SECS/XP_ROUNDS/XP_SCALE
#      VG_STORAGE   forwarded as XP_STORAGE ('' | block_device)
#      VG_NETEM_MS  forwarded as XP_NETEM_MS (declared; use netem-tier.sh)
#      VG_BASELINE_TSV  reuse an existing baseline TSV (skip the off run)
#      VG_OUT       report path (default scripts/perf/results/
#                   612-value-gate-<wave>-<epoch>.md)
#
#    Run from repo root in a built tree:
#      VG_WAVE=c perl scripts/perf/run_612_value_gate.pl
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (D0-shared)
#
# IDENTIFICATION
#    scripts/perf/run_612_value_gate.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use File::Basename qw(dirname);
use File::Path qw(make_path);
use File::Spec;
use FindBin;

my $REPO_ROOT = File::Spec->rel2abs("$FindBin::RealBin/../..");
my $RUNNER    = "$FindBin::RealBin/run_2node_xnode_profile.pl";

# wave -> [GUC lines to inject on the candidate run]
my %WAVE_GUC = (
    c  => ['cluster.page_scn_shortcut = on'],
    a  => ['cluster.read_scache = on'],
    e1 => ['cluster.ges_handoff = on'],
    e2 => ['cluster.ges_handoff = on', 'cluster.ges_bast = on'],
    d  => ['cluster.space_affinity = static'],
    f  => [],    # wave f is a reloption, not a GUC -- see %WAVE_ENV
);

# wave -> extra env for the CANDIDATE leg only.  Wave f shipped as the
# btree reloption cluster_reverse_key (spec-6.12f), not a GUC: the on-leg
# rebuilds the I-axis right-growing PK index reverse-keyed via
# XP_INDEX_RELOPT (consumed by axis_index in run_2node_xnode_profile.pl).
my %WAVE_ENV = (
    f => { XP_INDEX_RELOPT => 'cluster_reverse_key=on' },
);

# wave -> the counters whose delta decides the gate (TSV keys).
my %WAVE_KEYS = (
    c => [
        'r.post.delta.bucket.r_tt_visibility_resolve.n_events',
        'r.post.delta.bucket.r_tt_visibility_resolve.total_nanos',
        'r.pre.delta.bucket.r_tt_visibility_resolve.n_events',
        'r.pre.delta.bucket.r_tt_visibility_resolve.total_nanos',
    ],
    a => [
        'r.post.reship', 'r.post.sholder',
        'r.pre.reship',  'r.pre.sholder',
        'r.post.delta.bucket.r_readimage_ship.n_events',
    ],
    e1 => [
        'w.d0.bucket.w_ges_wait.total_nanos',
        'w.d0.bucket.w_ges_wait.n_events',
        'w.d0.bucket.w_ges_wake.total_nanos',
        'w.d0.bucket.w_ges_enqueue.total_nanos',
    ],
    e2 => [
        'w.d0.bucket.w_ges_wait.total_nanos',
        'w.d0.bucket.w_ges_wait.n_events',
        'w.d0.bucket.w_ges_enqueue.total_nanos',
    ],
    d => [
        'w.d0.bucket.w_hw_extend.total_nanos',
        'w.d0.bucket.w_hw_extend.n_events',
        'w.d0.hw_extend_local_count',
        'w.d0.hw_extend_remote_count',
    ],
    f => [
        'i.d0.bucket.i_index_block_xfer.n_events',
        'i.d0.bucket.i_index_block_xfer.total_nanos',
        'i.d0.bucket.i_rightmost_leaf_ping.n_events',
        'i.d1.bucket.i_index_block_xfer.n_events',
    ],
);

my $wave = $ENV{VG_WAVE} // '';
die "VG_WAVE must be one of: " . join(' ', sort keys %WAVE_GUC) . "\n"
  unless exists $WAVE_GUC{$wave};

my $stamp  = time();
my $outdir = File::Spec->catfile($REPO_ROOT, 'scripts', 'perf', 'results');
my $VG_OUT = $ENV{VG_OUT}
  // File::Spec->catfile($outdir, "612-value-gate-$wave-$stamp.md");

sub run_leg
{
    my ($leg, $extra_guc) = @_;
    my $tsv = File::Spec->catfile($outdir, "612-$wave-$leg-$stamp.tsv");
    my $md  = File::Spec->catfile($outdir, "612-$wave-$leg-$stamp.md");

    local $ENV{XP_TSV_OUT}   = $tsv;
    local $ENV{XP_OUT}       = $md;
    local $ENV{XP_EXTRA_GUC} = $extra_guc;
    local $ENV{XP_SECS}      = $ENV{VG_SECS}   if defined $ENV{VG_SECS};
    local $ENV{XP_ROUNDS}    = $ENV{VG_ROUNDS} if defined $ENV{VG_ROUNDS};
    local $ENV{XP_SCALE}     = $ENV{VG_SCALE}  if defined $ENV{VG_SCALE};
    local $ENV{XP_STORAGE}   = $ENV{VG_STORAGE} // '';
    local $ENV{XP_NETEM_MS}  = $ENV{VG_NETEM_MS} // '';
    my %wave_env = ($leg eq 'on' && $WAVE_ENV{$wave}) ? %{ $WAVE_ENV{$wave} } : ();
    local @ENV{keys %wave_env} = values %wave_env if %wave_env;

    print "== value-gate $wave: running $leg leg (extra_guc='$extra_guc')\n";
    system($^X, $RUNNER) == 0
      or die "value-gate: $leg leg runner failed ($?)\n";
    die "value-gate: $leg leg produced no TSV ($tsv)\n" unless -s $tsv;
    return $tsv;
}

sub read_tsv
{
    my ($path) = @_;
    my %kv;
    open(my $fh, '<', $path) or die "cannot read $path: $!\n";
    while (my $line = <$fh>)
    {
        chomp $line;
        my ($k, $v) = split /\t/, $line, 2;
        $kv{$k} = $v if defined $k && defined $v;
    }
    close $fh;
    return \%kv;
}

my $base_tsv = $ENV{VG_BASELINE_TSV};
if (defined $base_tsv && length $base_tsv)
{
    die "VG_BASELINE_TSV '$base_tsv' unreadable\n" unless -s $base_tsv;
    print "== value-gate $wave: reusing baseline TSV $base_tsv\n";
}
else
{
    $base_tsv = run_leg('off', '');
}
my $cand_tsv = run_leg('on', join('; ', @{ $WAVE_GUC{$wave} }));

my $base = read_tsv($base_tsv);
my $cand = read_tsv($cand_tsv);

my @rows;
my %seen;
for my $k (@{ $WAVE_KEYS{$wave} }, sort keys %$base)
{
    next if $seen{$k}++;
    my $b = $base->{$k};
    my $c = $cand->{$k};
    next unless defined $b || defined $c;
    # decision keys always shown; other keys only when both sides differ
    my $decision = grep { $_ eq $k } @{ $WAVE_KEYS{$wave} };
    next if !$decision && (!defined $b || !defined $c || $b == $c);
    my $delta =
      (defined $b && defined $c && $b =~ /^-?[\d.]+$/ && $c =~ /^-?[\d.]+$/)
      ? sprintf('%+.6g', $c - $b)
      : 'n/a';
    push @rows,
      sprintf('| %s | %s | %s | %s |%s',
        $k, $b // 'n/a', $c // 'n/a', $delta, $decision ? ' <- gate' : '');
}

make_path(dirname($VG_OUT));
open(my $fh, '>', $VG_OUT) or die "cannot write $VG_OUT: $!\n";
print $fh join("\n",
    "# spec-6.12 value-gate: wave $wave",
    '',
    "- baseline tsv: $base_tsv",
    "- candidate tsv: $cand_tsv",
    '- candidate guc: ' . join('; ', @{ $WAVE_GUC{$wave} }),
    '- candidate env: '
      . ($WAVE_ENV{$wave}
        ? join('; ', map { "$_=$WAVE_ENV{$wave}{$_}" } sort keys %{ $WAVE_ENV{$wave} })
        : '(none)'),
    '- storage: ' . ($ENV{VG_STORAGE} // '(shared-nothing)'),
    '- netem declared: ' . ($ENV{VG_NETEM_MS} // 'none'),
    '',
    '> HONESTY: counter deltas only.  Shared-nothing legs prove',
    '> shipping-count / lookup-count movement, NEVER real coordination',
    '> benefit (that needs VG_STORAGE=block_device + netem, spec §3.5).',
    '> GO / NO-GO is a human read of the gate rows, not an exit code.',
    '',
    '| key | off | on | delta | |',
    '|---|---|---|---|---|',
    @rows, ''), "\n";
close $fh;

print "== value-gate report: $VG_OUT\n";
exit 0;
