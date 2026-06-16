#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run-stage4-recovery-latency.pl -- spec-4.13 D4 block recovery latency runner
#
# Drives the online single-block recovery path (single cluster node) and reads
# the D4 LOG-only latency instrumentation back from the server log:
#   LOG:  online block recovery: rel=.. block=.. outcome={recovered|failclosed}
#         latency_us=N
#
# Bands (recovery_timings[] in the spec-4.13 §3.4 schema):
#   P0-5-near-fpi   corrupt a block whose FPI is close      -> recovered latency
#   P0-6-far-fpi    push a large WAL window after the FPI    -> recovered latency
#   P0-7-failclosed corrupt with no recoverable FPI base     -> failclosed latency
#
# Single-node, locally runnable (mirrors t/257_block_recovery_d1.pl's corrupt-
# block mechanism).  Thread recovery latency (P0-8/9/10) needs a real DONE on a
# 2-node fixture -- forward to the 2-node / CI path; this runner covers the
# block bands only.  Local numbers are exploratory; closure trusts clean CI.
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
use lib "$FindBin::RealBin/../../src/test/cluster_tap/lib";
use lib "$FindBin::RealBin/../../src/test/perl";

use Getopt::Long;
use File::Spec;
use JSON::PP;

our $have_node;
BEGIN {
    eval {
        require PgracClusterNode;
        PgracClusterNode->import();
        require PostgreSQL::Test::Utils;
        $have_node = 1;
    };
}

my ($enable_install, $results_dir, $timestamp, $commit);
my $reps = 3;
my $bands = 'block';

GetOptions(
    'enable-install=s' => \$enable_install,
    'results-dir=s'    => \$results_dir,
    'timestamp=s'      => \$timestamp,
    'commit=s'         => \$commit,
    'reps=i'           => \$reps,
    'bands=s'          => \$bands,
) or die "usage: $0 --enable-install=DIR --results-dir=DIR --timestamp=TS "
       . "[--commit=SHA --reps=N --bands=block]\n";

die "--enable-install required\n" unless $enable_install;
die "--results-dir required\n"    unless $results_dir;
die "--timestamp required\n"      unless $timestamp;
die "PgracClusterNode not available -- needs the PG TAP perl env\n" unless $have_node;
$commit ||= 'unknown';

local $ENV{PGRAC_ENABLE_INSTALL} = $enable_install;

my $tag      = "perf-stage4-recovery-${timestamp}";
my $json_out = File::Spec->catfile($results_dir, "${tag}.json");

# ---- single cluster node with online block recovery on ----------------
my $node = PgracClusterNode->new('s4_blkrec');
$node->init(extra => [ '--data-checksums' ]);
$node->append_conf('postgresql.conf',
        "cluster.enabled = on\n"
      . "cluster.node_id = 0\n"
      . "cluster.allow_single_node = on\n"
      . "autovacuum = off\n"
      . "full_page_writes = on\n"
      . "wal_keep_size = 256MB\n"
      . "cluster.online_block_recovery = on\n");
$node->start;

# ---- helpers ----------------------------------------------------------
sub read_raw  { my ($p) = @_; open my $fh, '<:raw', $p or die "open $p: $!"; local $/; my $d = <$fh>; close $fh; return $d; }
sub write_raw { my ($p, $d) = @_; open my $fh, '>:raw', $p or die "open $p: $!"; print $fh $d; close $fh; }

sub flip_byte_at {
    my ($relpath, $off) = @_;
    my $path = $node->data_dir . '/' . $relpath;
    my $img  = read_raw($path);
    substr($img, $off, 1) = pack('C', unpack('C', substr($img, $off, 1)) ^ 0xFF);
    write_raw($path, $img);
}

# Pull the LAST "online block recovery: ... latency_us=N" of the wanted outcome
# from the live server log (the D4 LOG instrumentation).
sub last_latency_us {
    my ($want) = @_;
    my $log = read_raw($node->logfile);
    my $found;
    while ($log =~ /online block recovery: \S+ \S+ block=\d+ outcome=(\w+) latency_us=(\d+)/g) {
        $found = $2 if $1 eq $want;
    }
    return $found;   # undef if none
}

sub median { my @s = sort { $a <=> $b } @_; return @s ? $s[int(@s / 2)] : undef; }

# Run one block-recovery scenario `$reps` times, return median latency_us.
#   builder->($rel, $i) sets up + corrupts one block and returns (relpath, off);
#   $want is the expected outcome ('recovered' | 'failclosed').
sub run_band {
    my ($id, $name, $want, $builder) = @_;
    my @lat;
    my $failed = 0;
    for my $i (1 .. $reps) {
        my $rel = "s4_${id}_$i";
        $rel =~ s/[^a-z0-9_]/_/gi;
        my ($relpath, $off) = $builder->($rel, $i);
        $node->stop;
        flip_byte_at($relpath, $off);
        $node->start;
        # Trigger: a read of the corrupt block.  recovered -> query succeeds;
        # failclosed -> query errors (expected; psql returns non-zero, never
        # dies -- unlike safe_psql).  Either way the D4 LOG fires.
        my $rc = $node->psql('postgres', "SELECT count(*) FROM $rel");
        $failed++ if ($want eq 'recovered' && $rc != 0);
        $failed++ if ($want eq 'failclosed' && $rc == 0);   # must NOT silently succeed
        my $us = last_latency_us($want);
        push @lat, $us if defined $us;
    }
    my $med = median(@lat);
    my $verdict = (!defined $med) ? 'SKIP'
                : $failed         ? 'POLLUTED'
                :                   'DONE';
    print "run-stage4-recovery-latency.pl: $id ($want) latency_us median="
        . (defined $med ? $med : 'n/a') . " samples=" . scalar(@lat)
        . " failed=$failed verdict=$verdict\n";
    return {
        id => $id, name => $name, outcome => $want,
        block_recovery_us => $med, samples => [@lat],
        failed => $failed, verdict => $verdict,
    };
}

# ---- bands ------------------------------------------------------------
my @recov;

# P0-5 near-FPI: FPI generated right before the corruption (small scan window).
push @recov, run_band('P0-5-near-fpi', 'block recovery near-FPI', 'recovered', sub {
    my ($rel) = @_;
    $node->safe_psql('postgres', "CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled=off)");
    $node->safe_psql('postgres', "INSERT INTO $rel SELECT g,'v'||g FROM generate_series(1,20) g");
    $node->safe_psql('postgres', 'CHECKPOINT');
    $node->safe_psql('postgres', "INSERT INTO $rel VALUES (999,'fpi')");   # block-0 FPI
    return ($node->safe_psql('postgres', "SELECT pg_relation_filepath('$rel')"), 2000);
});

# P0-6 far-FPI: push a large WAL window AFTER the FPI (wide scan to locate it).
if ($bands eq 'block' || $bands eq 'all') {
    push @recov, run_band('P0-6-far-fpi', 'block recovery far-FPI', 'recovered', sub {
        my ($rel) = @_;
        $node->safe_psql('postgres', "CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled=off)");
        $node->safe_psql('postgres', "INSERT INTO $rel SELECT g,'v'||g FROM generate_series(1,20) g");
        $node->safe_psql('postgres', 'CHECKPOINT');
        $node->safe_psql('postgres', "INSERT INTO $rel VALUES (999,'fpi')");
        # Generate a wide WAL window on an UNRELATED relation after the FPI.
        $node->safe_psql('postgres',
            "CREATE TABLE ${rel}_pad (id int); "
          . "INSERT INTO ${rel}_pad SELECT g FROM generate_series(1,200000) g; "
          . "DELETE FROM ${rel}_pad");
        return ($node->safe_psql('postgres', "SELECT pg_relation_filepath('$rel')"), 2000);
    });
}

# P0-7 fail-closed: corrupt a block with no apply-able FPI base (no post-checkpoint
# touch) -> reconstruct cannot rebuild -> failclosed.
push @recov, run_band('P0-7-failclosed', 'block recovery fail-closed', 'failclosed', sub {
    my ($rel) = @_;
    $node->safe_psql('postgres', "CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled=off)");
    $node->safe_psql('postgres', "INSERT INTO $rel SELECT g,'v'||g FROM generate_series(1,20) g");
    $node->safe_psql('postgres', 'CHECKPOINT');   # block-0 flushed; NO post-checkpoint FPI touch
    return ($node->safe_psql('postgres', "SELECT pg_relation_filepath('$rel')"), 2000);
});

$node->stop;

# ---- emit JSON --------------------------------------------------------
my $summary = {
    spec   => '4.13', leg => 'D4-block-latency', commit => $commit,
    timestamp => $timestamp, runner => { os => $^O },
    params => { reps => $reps, bands => $bands, nodes => 1 },
    recovery_timings => \@recov,
};
open(my $jfh, '>', $json_out) or die "cannot write $json_out: $!\n";
print $jfh JSON::PP->new->canonical->pretty->encode($summary);
close $jfh;
print "run-stage4-recovery-latency.pl: summary -> $json_out\n";
print "run-stage4-recovery-latency.pl: done ($^O local numbers exploratory only -- 规则 23)\n";
exit 0;
