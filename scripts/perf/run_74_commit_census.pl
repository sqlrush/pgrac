#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run_74_commit_census.pl
#    spec-7.4 D0 -- commit/SCN propagation census (REPORT-ONLY).
#
#    Decomposes the pgrac commit path into measured components and
#    measures the propagation staleness that spec-7.4's levers target.
#    Five legs, selectable via CS_LEGS (default "abcd"; "e" is the
#    version-comparison leg driven separately per install prefix):
#
#      A  commit component table -- 2-node pair (shared_data + voting
#         disks), node0 runs a single-row-UPDATE commit workload under
#         cluster.xnode_profile = on, once with fsync=off (CPU-side
#         attribution, t/328-comparable) and once with fsync=on (real
#         flush weights).  Per-component nanos/commit from the
#         c_commit_* / c_scn_commit_advance bucket deltas; the
#         denominator is the client-side END-statement latency from
#         pgbench -r (native tier measured alongside for the same
#         workload).  Residual = denominator - sum(components).
#
#      B  single-node serialization-ceiling matrix -- native vs solo
#         cluster node (t/328 topology, fsync=off), interleaved short
#         windows over a client ladder (1..64).  Per-rung median TPS +
#         tax%; at the configured attribution rung the runner samples
#         pg_stat_activity wait events and per-commit bucket means to
#         attribute the ceiling (undo cursor LWLock vs SCN atomics vs
#         aux IPC -- the three suspects named by the spec amendment).
#
#      C  BOC staleness -- 2-node pair; node0 commits via a background
#         psql session, the runner then polls node1's observed SCN
#         (scn_max_observed_remote) on a tight interval and records
#         t(observe) - t(commit-return) per sample, idle (no other
#         traffic: the Lamport-clock-stall case) and busy (background
#         pgbench on node0).  Histogram buckets 1/2/5/10/20/50/100/200ms
#         (spec-7.4 §2.4; same-host same-clock differencing).
#
#      D  hwm freshness co-sample audit -- counters snapshot over the
#         Leg A window: how often the UNDO_TT_FETCH piggyback path (the
#         only live_hwm publication channel today) actually fires
#         relative to the commit rate.
#
#      E  version comparison -- exact t/328 methodology (fsync=off,
#         TPC-B -N, 4 clients, short interleaved native/solo windows,
#         median) intended to be invoked once per install prefix
#         (CS_INSTALL=/tmp/pgrac-{74,v128,v129}-install CS_LEGS=e) to
#         rule the "did 6.14 add write tax" question on one runner.
#
#    REPORT-ONLY: trend datapoints, never a gate; exits 0 on completion,
#    dies only on harness failure (mirrors run_2node_xnode_profile.pl).
#
#    Env knobs: CS_LEGS (abcd) legs to run; CS_SECS (10) Leg A seconds;
#    CS_ROUNDS (3) Leg A rounds; CS_SCALE (10) pgbench scale; CS_LADDER
#    (1,2,4,8,16,32,64) Leg B client rungs; CS_LADDER_SECS (2) window
#    seconds; CS_LADDER_PAIRS (6) interleaved pairs per rung; CS_ATTR_C
#    (32) attribution rung; CS_STALE_N (100) Leg C samples per mode;
#    CS_E_ROUNDS (30) Leg E interleaved pairs; CS_OUT report path
#    (default scripts/perf/results/74-commit-census-<epoch>.md);
#    CS_TSV_OUT optional machine-readable dump; CS_INSTALL optional
#    install prefix prepended to PATH (Leg E per-version invocations);
#    PGBENCH pgbench binary override.
#
#    Run from repo root in a built tree:
#      CS_INSTALL=/tmp/pgrac-74-install perl scripts/perf/run_74_commit_census.pl
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-7.4-commit-scn-propagation-freshness.md (D0)
#
# IDENTIFICATION
#    scripts/perf/run_74_commit_census.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";

use File::Basename qw(dirname);
use File::Path qw(make_path);
use File::Spec;
use File::Temp qw(tempdir);
use POSIX ();
use Time::HiRes ();

our ($REAL_STDOUT, $have_fixtures, $fixtures_err);

BEGIN
{
	open($REAL_STDOUT, '>&', \*STDOUT)
	  or die "cannot dup STDOUT: $!\n";
	my $prev = select($REAL_STDOUT);
	$| = 1;
	select($prev);

	if (defined $ENV{CS_INSTALL} && length $ENV{CS_INSTALL})
	{
		$ENV{PATH} = "$ENV{CS_INSTALL}/bin:$ENV{PATH}";
		for my $var (qw(LD_LIBRARY_PATH DYLD_LIBRARY_PATH))
		{
			$ENV{$var} = join(':', "$ENV{CS_INSTALL}/lib",
				grep { defined && length } $ENV{$var});
		}
	}

	{
		require FindBin;
		FindBin->import();
		my $repo = File::Spec->rel2abs("$FindBin::RealBin/../..");

		$ENV{PG_REGRESS} = "$repo/src/test/regress/pg_regress"
		  unless defined $ENV{PG_REGRESS} && length $ENV{PG_REGRESS};
		$ENV{TESTDATADIR} = "$repo/tmp_check"
		  unless defined $ENV{TESTDATADIR} && length $ENV{TESTDATADIR};
		$ENV{TESTLOGDIR} = "$repo/tmp_check/log"
		  unless defined $ENV{TESTLOGDIR} && length $ENV{TESTLOGDIR};
	}

	eval {
		require PostgreSQL::Test::Utils;
		PostgreSQL::Test::Utils->import();
		require PostgreSQL::Test::Cluster;
		PostgreSQL::Test::Cluster->import();
		require PostgreSQL::Test::ClusterPair;
		PostgreSQL::Test::ClusterPair->import();
		$have_fixtures = 1;
		1;
	} or $fixtures_err = $@;
}

use Test::More;

die "TAP fixtures unavailable (need a built tree; PERL5LIB reaches "
  . "src/test/perl via FindBin): $fixtures_err\n"
  unless $have_fixtures;

# ----------
# knobs + constants
# ----------
my $CS_LEGS        = lc($ENV{CS_LEGS} // 'abcd');
my $CS_SECS        = $ENV{CS_SECS} // 10;
my $CS_ROUNDS      = $ENV{CS_ROUNDS} // 3;
my $CS_SCALE       = $ENV{CS_SCALE} // 10;
my @CS_LADDER      = split /,/, ($ENV{CS_LADDER} // '1,2,4,8,16,32,64');
my $CS_LADDER_SECS = $ENV{CS_LADDER_SECS} // 2;
my $CS_LADDER_PAIRS = $ENV{CS_LADDER_PAIRS} // 6;
my $CS_ATTR_C      = $ENV{CS_ATTR_C} // 16;
my $CS_STALE_N     = $ENV{CS_STALE_N} // 100;
my $CS_E_ROUNDS    = $ENV{CS_E_ROUNDS} // 30;
my $PGBENCH        = $ENV{PGBENCH} // 'pgbench';

my $REPO_ROOT = File::Spec->rel2abs("$FindBin::RealBin/../..");
my $CS_OUT    = $ENV{CS_OUT}
  // File::Spec->catfile($REPO_ROOT, 'scripts', 'perf', 'results',
	'74-commit-census-' . time() . '.md');
my $CS_TSV_OUT = $ENV{CS_TSV_OUT} // '';

# Commit-decomposition buckets (spec-7.4 D0) + the pre-existing SCN
# advance bucket; nanos are requester-exclusive so nanos/commit means are
# comparable against the client END latency denominator.
my @COMMIT_BUCKETS = qw(
	c_commit_quorum_read c_commit_undo_flush c_scn_commit_advance
	c_commit_itl_stamp c_commit_tt_stamp c_commit_wal_flush
);

# Leg C histogram bucket upper bounds, ms (spec-7.4 §2.4).
my @STALE_BOUNDS_MS = (1, 2, 5, 10, 20, 50, 100, 200);

my @REPORT;
my %TSV;

sub progress
{
	my ($msg) = @_;
	print $REAL_STDOUT "[census] $msg\n";
}

sub report
{
	my ($line) = @_;
	push @REPORT, $line;
}

sub median
{
	my @s = sort { $a <=> $b } grep { defined } @_;
	return undef unless @s;
	return $s[int(@s / 2)];
}

sub pctile
{
	my ($p, @vals) = @_;
	my @s = sort { $a <=> $b } @vals;
	return undef unless @s;
	my $idx = int($p / 100 * (@s - 1) + 0.5);
	return $s[$idx];
}

sub poll_sql_eq
{
	my ($node, $sql, $want, $timeout_s) = @_;
	my $deadline = Time::HiRes::time() + ($timeout_s // 15);
	while (Time::HiRes::time() < $deadline)
	{
		my $got = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $got && $got eq $want;
		select(undef, undef, undef, 0.25);
	}
	return 0;
}

# ----------
# xnode_profile dump snapshots + deltas (run_2node_xnode_profile.pl)
# ----------
sub xp_snapshot
{
	my ($node) = @_;
	my %snap;
	my $rows = $node->safe_psql('postgres',
		"SELECT key, value FROM pg_cluster_state WHERE category='xnode_profile'");
	for my $line (split /\n/, ($rows // ''))
	{
		my ($k, $v) = split /\|/, $line, 2;
		$snap{$k} = ($v // 0) + 0 if defined $k && length $k;
	}
	return \%snap;
}

sub xp_delta
{
	my ($before, $after) = @_;
	my %d;
	$d{$_} = ($after->{$_} // 0) - ($before->{$_} // 0) for keys %$after;
	return \%d;
}

sub bkt_n  { my ($d, $b) = @_; return $d->{"bucket.$b.n_events"}    // 0; }
sub bkt_ns { my ($d, $b) = @_; return $d->{"bucket.$b.total_nanos"} // 0; }

# Generic pg_cluster_state counter snapshot for one category.
sub cat_snapshot
{
	my ($node, $cat) = @_;
	my %snap;
	my $rows = $node->safe_psql('postgres',
		"SELECT key, value FROM pg_cluster_state WHERE category='$cat'");
	for my $line (split /\n/, ($rows // ''))
	{
		my ($k, $v) = split /\|/, $line, 2;
		next unless defined $k && length $k;
		$snap{$k} = ($v =~ /^-?\d+$/) ? $v + 0 : $v;
	}
	return \%snap;
}

# ----------
# conf + tier boots
# ----------
# Census isolation knobs.  Unlike the 5.59 profile conf, fsync is a LEG
# PARAMETER here: the commit census must see real flush weights in its
# fsync=on leg, while the fsync=off legs stay t/328-comparable.
sub census_conf
{
	my ($fsync) = @_;
	return (
		"autovacuum = off\n",
		"fsync = $fsync\n",
		"shared_buffers = 256MB\n",
		"max_wal_size = 4GB\n",
	);
}

sub boot_native
{
	my ($fsync, $tag) = @_;
	$tag //= $fsync;
	my $native = PostgreSQL::Test::Cluster->new('census_native_' . $tag);
	$native->init;
	$native->append_conf('postgresql.conf', $_) for census_conf($fsync);
	$native->start;
	return $native;
}

sub boot_solo
{
	my ($fsync, $tag) = @_;
	$tag //= $fsync;
	my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
	my $solo    = PostgreSQL::Test::Cluster->new('census_solo_' . $tag);
	$solo->init;
	$solo->append_conf('postgresql.conf', "$_\n")
	  for ('cluster.enabled = on', 'cluster.interconnect_tier = tier1',
		'cluster.allow_single_node = on', 'cluster.node_id = 0',
		'cluster.xnode_profile = on');
	$solo->append_conf('postgresql.conf', $_) for census_conf($fsync);
	PostgreSQL::Test::Utils::append_to_file(
		$solo->data_dir . '/pgrac.conf',
		"[cluster]\nname = census_solo\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n"
	);
	$solo->start;
	return $solo;
}

sub boot_pair
{
	my ($fsync, $tag) = @_;
	$tag //= $fsync;
	my @pair_conf = map { my $l = $_; chomp $l; $l } census_conf($fsync);
	my $pair      = PostgreSQL::Test::ClusterPair->new_pair(
		'census_pair_' . $tag,
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [
			@pair_conf,
			'cluster.xnode_profile = on',
			'cluster.gcs_block_retransmit_max_retries = 8',
			'cluster.gcs_block_dedup_max_entries = 16384',
			# The Leg A commit storm leaves a GES/GCS backlog; a census
			# harness prefers patience over spurious lock timeouts.
			'cluster.ges_request_timeout_ms = 20000',
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 2000',
			'cluster.cssd_dead_deadband_factor = 10',
		]);
	$pair->start_pair;
	my $ready =
		 $pair->wait_for_peer_state(0, 1, 'connected', 30)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 30)
	  && poll_sql_eq($pair->node0,
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20)
	  && poll_sql_eq($pair->node1,
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20);
	die "harness failure: ClusterPair did not reach connected + in_quorum\n"
	  unless $ready;
	return $pair;
}

# ----------
# pgbench drivers
# ----------
sub conn_args
{
	my ($node) = @_;
	return '-h ' . $node->host . ' -p ' . $node->port . ' postgres';
}

sub pgbench_init
{
	my ($node) = @_;
	system("$PGBENCH -i -s $CS_SCALE -q " . conn_args($node) . " >/dev/null 2>&1");
	return $? == 0;
}

# TPC-B (-N) window; returns tps or undef (t/328 pgbench_one).
sub pgbench_tpcb
{
	my ($node, $secs, $clients) = @_;
	my $out = `$PGBENCH -n -T $secs -c $clients -j 2 -N @{[conn_args($node)]} 2>&1`;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/;
	return $1 + 0.0 if $out =~ /tps\s*=\s*([\d.]+)/;
	return undef;
}

# Commit-census workload: single-row UPDATE in an explicit transaction,
# reported per-statement (-r) so the END line is the client-side commit
# latency.  Returns (tps, xacts, end_latency_ms) or ().
my $COMMIT_SCRIPT_DIR = tempdir(CLEANUP => 1);
my $COMMIT_SCRIPT = "$COMMIT_SCRIPT_DIR/commit_census.sql";
{
	open my $fh, '>', $COMMIT_SCRIPT or die "cannot write $COMMIT_SCRIPT: $!";
	print $fh <<'EOS';
\set aid random(1, 100000 * :scale)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = :aid;
END;
EOS
	close $fh;
}

sub pgbench_commit_run
{
	my ($node, $secs, $clients) = @_;
	my $out = `$PGBENCH -n -T $secs -c $clients -j 2 -r -D scale=$CS_SCALE -f $COMMIT_SCRIPT @{[conn_args($node)]} 2>&1`;
	my ($tps) = $out =~ /tps\s*=\s*([\d.]+)/;
	my ($xacts) = $out =~ /number of transactions actually processed:\s*(\d+)/;
	my ($end_ms);
	# "statement latencies in milliseconds[ and failures]:" table; the
	# END; row is the commit.
	for my $line (split /\n/, $out)
	{
		$end_ms = $1 + 0.0 if $line =~ /^\s*([\d.]+)\s+(?:\d+\s+)?END;/;
	}
	return () unless defined $tps && defined $xacts && $xacts > 0;
	return ($tps + 0.0, $xacts, $end_ms);
}

# ----------
# Leg A -- commit component table (pair node0 + native denominator)
# ----------
sub leg_a
{
	my ($fsync) = @_;
	progress("Leg A (fsync=$fsync): boot native + pair");
	my $native = boot_native($fsync);
	my $pair   = boot_pair($fsync);
	my $node0  = $pair->node0;

	pgbench_init($native) or die "pgbench init (native) failed\n";
	pgbench_init($node0)  or die "pgbench init (pair node0) failed\n";

	my (@nat_end, @clu_end, @nat_tps, @clu_tps);
	my ($xacts_total, %ns_total, %n_total) = (0);
	my $d_snap;

	# Leg D pre-window counter snapshot (publication-channel audit).
	my %d_pre = $CS_LEGS =~ /d/
	  ? (scn0 => cat_snapshot($node0, 'scn'),
		scn1 => cat_snapshot($pair->node1, 'scn'),
		gcs1 => cat_snapshot($pair->node1, 'gcs'))
	  : ();

	for my $round (1 .. $CS_ROUNDS)
	{
		progress("Leg A (fsync=$fsync): round $round/$CS_ROUNDS");
		# c=1 single-writer on the pair (5.59 W-axis / M3 precedent):
		# multi-client UPDATE churn on a peer-online pair trips GES
		# handoff drain fail-closed storms and poisons the census;
		# queueing effects are Leg B's job (solo ladder).
		my ($ntps, $nx, $nend) = pgbench_commit_run($native, $CS_SECS, 1);
		my $before = xp_snapshot($node0);
		my ($ctps, $cx, $cend) = pgbench_commit_run($node0, $CS_SECS, 1);
		my $after = xp_snapshot($node0);
		next unless defined $ntps && defined $ctps;
		push @nat_tps, $ntps;  push @nat_end, $nend;
		push @clu_tps, $ctps;  push @clu_end, $cend;
		my $d = xp_delta($before, $after);
		# Denominator: the SCN-advance event count IS the commit count
		# in this window (exactly 1/commit, probe-verified); pgbench's
		# xact number would also include warm-up outside the snapshot.
		$xacts_total += bkt_n($d, 'c_scn_commit_advance');
		for my $b (@COMMIT_BUCKETS)
		{
			$ns_total{$b} += bkt_ns($d, $b);
			$n_total{$b}  += bkt_n($d, $b);
		}
		$d_snap = $d;
	}

	die "Leg A produced no valid rounds\n" unless @clu_tps;

	my $nat_end_med = median(@nat_end) // 0;
	my $clu_end_med = median(@clu_end) // 0;
	my $denom_ns    = $clu_end_med * 1e6;    # ms -> ns per commit

	report("");
	report("## Leg A -- commit component table (fsync=$fsync)");
	report("");
	report(sprintf(
		"workload: single-row UPDATE + explicit commit, 1 client "
		  . "(5.59 M3 single-writer precedent on the pair), "
		  . "%ds x %d rounds, scale %d; pair node0 (shared_data), medians",
		$CS_SECS, $CS_ROUNDS, $CS_SCALE));
	report("");
	report(sprintf("- native  END latency (median): %.3f ms  (tps %.0f)",
		$nat_end_med, median(@nat_tps) // 0));
	report(sprintf("- cluster END latency (median): %.3f ms  (tps %.0f)",
		$clu_end_med, median(@clu_tps) // 0));
	report(sprintf("- commits measured (xp window): %d", $xacts_total));
	report("");
	report("| component | ns/commit | us/event | events/commit | % of END |");
	report("|---|---:|---:|---:|---:|");

	my $sum_ns_per_commit = 0;
	for my $b (@COMMIT_BUCKETS)
	{
		my $ns_per_commit = $xacts_total ? $ns_total{$b} / $xacts_total : 0;
		my $us_per_event  = $n_total{$b} ? $ns_total{$b} / $n_total{$b} / 1000 : 0;
		my $ev_per_commit = $xacts_total ? $n_total{$b} / $xacts_total : 0;
		my $pct = $denom_ns ? 100 * $ns_per_commit / $denom_ns : 0;
		$sum_ns_per_commit += $ns_per_commit;
		report(sprintf("| %s | %.0f | %.2f | %.3f | %.2f%% |",
			$b, $ns_per_commit, $us_per_event, $ev_per_commit, $pct));
		$TSV{"a.$fsync.$b.ns_per_commit"} = sprintf("%.0f", $ns_per_commit);
		$TSV{"a.$fsync.$b.pct_of_end"}    = sprintf("%.2f", $pct);
	}
	my $resid_ns  = $denom_ns - $sum_ns_per_commit;
	my $resid_pct = $denom_ns ? 100 * $resid_ns / $denom_ns : 0;
	report(sprintf("| (sum of components) | %.0f | | | %.2f%% |",
		$sum_ns_per_commit, $denom_ns ? 100 * $sum_ns_per_commit / $denom_ns : 0));
	report(sprintf("| residual (client rtt, executor, CLOG, ProcArray, ...) "
		  . "| %.0f | | | %.2f%% |", $resid_ns, $resid_pct));
	$TSV{"a.$fsync.end_latency_ms"} = sprintf("%.3f", $clu_end_med);
	$TSV{"a.$fsync.native_end_latency_ms"} = sprintf("%.3f", $nat_end_med);
	$TSV{"a.$fsync.residual_pct"} = sprintf("%.2f", $resid_pct);

	# Leg D piggyback: publication-channel counter audit over the same
	# window (no extra cross-node traffic: a post-storm GES request on
	# this pair can stall 20s+ -- pre-existing, logged as follow-up).
	if ($CS_LEGS =~ /d/)
	{
		my %d_post = (
			scn0 => cat_snapshot($node0, 'scn'),
			scn1 => cat_snapshot($pair->node1, 'scn'),
			gcs1 => cat_snapshot($pair->node1, 'gcs'));
		leg_d($xacts_total, \%d_pre, \%d_post, $fsync);
	}

	$pair->stop_pair if $pair->can('stop_pair');
	$native->stop;
	return;
}

# ----------
# Leg B -- serialization ceiling matrix (native vs solo, t/328 topology)
# ----------
sub leg_b
{
	progress("Leg B: boot native + solo (fsync=off, t/328 topology)");
	my $native = boot_native('off', 'b');
	my $solo   = boot_solo('off', 'b');

	pgbench_init($native) or die "pgbench init (native) failed\n";
	pgbench_init($solo)   or die "pgbench init (solo) failed\n";

	report("");
	report("## Leg B -- single-node serialization ceiling (native vs solo cluster)");
	report("");
	report(sprintf(
		"TPC-B -N interleaved windows, %ds x %d pairs per rung, medians; fsync=off",
		$CS_LADDER_SECS, $CS_LADDER_PAIRS));
	report("");
	report("| clients | native tps | cluster tps | tax %% |");
	report("|---:|---:|---:|---:|");

	my $solo_crashed_at;
	for my $c (@CS_LADDER)
	{
		my (@nat, @clu);
		for my $i (1 .. $CS_LADDER_PAIRS)
		{
			my $n = pgbench_tpcb($native, $CS_LADDER_SECS, $c);
			my $s = pgbench_tpcb($solo,   $CS_LADDER_SECS, $c);
			push @nat, $n if defined $n && $n > 0;
			push @clu, $s if defined $s && $s > 0;
		}
		# Ceiling-by-crash detection: if the solo server died under this
		# rung (undo buffer pool PANIC family), that IS the ceiling
		# finding -- record it and stop climbing.
		my $alive = eval { $solo->safe_psql('postgres', 'SELECT 1'); 1 };
		my $nm = median(@nat);
		my $cm = median(@clu);
		my $tax = (defined $nm && defined $cm && $nm > 0)
		  ? 100 * ($nm - $cm) / $nm : undef;
		my $note = $alive ? '' : ' (SERVER CRASHED under this rung)';
		report(sprintf("| %d | %.0f | %.0f | %s |",
			$c, $nm // 0, $cm // 0,
			(defined $tax ? sprintf("%.2f%%", $tax) : 'n/a') . $note));
		$TSV{"b.c$c.native_tps"}  = sprintf("%.0f", $nm // 0);
		$TSV{"b.c$c.cluster_tps"} = sprintf("%.0f", $cm // 0);
		$TSV{"b.c$c.tax_pct"}     = defined $tax ? sprintf("%.2f", $tax) : '';
		progress(sprintf("Leg B: c=%d native=%.0f cluster=%.0f tax=%s%s",
			$c, $nm // 0, $cm // 0,
			defined $tax ? sprintf("%.2f%%", $tax) : 'n/a', $note));
		if (!$alive)
		{
			$solo_crashed_at = $c;
			report("");
			report(sprintf(
				"**ceiling-by-crash**: the solo cluster server died under "
				  . "c=%d (see census_solo_b.log for the PANIC signature); "
				  . "higher rungs skipped.", $c));
			last;
		}
	}
	if (defined $solo_crashed_at)
	{
		progress("Leg B: rebooting solo for the attribution rung");
		$solo = boot_solo('off', 'b2');
		pgbench_init($solo) or die "pgbench re-init (solo b2) failed\n";
	}

	# Attribution rung: per-commit bucket means at c=1 vs c=CS_ATTR_C +
	# wait-event sampling during a sustained c=CS_ATTR_C window.
	progress("Leg B: attribution rung c=$CS_ATTR_C");
	my %attr;
	for my $c (1, $CS_ATTR_C)
	{
		my $before = xp_snapshot($solo);
		my ($tps, $xacts) = pgbench_commit_run($solo, $CS_LADDER_SECS * 3, $c);
		my $after = xp_snapshot($solo);
		next unless defined $xacts && $xacts > 0;
		my $d = xp_delta($before, $after);
		for my $b (@COMMIT_BUCKETS, 'local_undo_itl_wal', 'ic_send_service')
		{
			$attr{$c}{$b} = {
				ns_per_commit => bkt_ns($d, $b) / $xacts,
				us_per_event  => bkt_n($d, $b)
				  ? bkt_ns($d, $b) / bkt_n($d, $b) / 1000 : 0,
			};
		}
	}
	report("");
	report(sprintf(
		"### attribution: per-commit component means, c=1 vs c=%d (solo, commit workload)",
		$CS_ATTR_C));
	report("");
	report("| component | c=1 us/event | c=$CS_ATTR_C us/event | inflation x |");
	report("|---|---:|---:|---:|");
	for my $b (@COMMIT_BUCKETS, 'local_undo_itl_wal', 'ic_send_service')
	{
		my $u1 = $attr{1}{$b}{us_per_event} // 0;
		my $uN = $attr{$CS_ATTR_C}{$b}{us_per_event} // 0;
		report(sprintf("| %s | %.2f | %.2f | %s |",
			$b, $u1, $uN, $u1 > 0 ? sprintf("%.1f", $uN / $u1) : 'n/a'));
		$TSV{"b.attr.$b.c1_us"}  = sprintf("%.2f", $u1);
		$TSV{"b.attr.$b.cN_us"}  = sprintf("%.2f", $uN);
	}

	# Wait-event sampling during a sustained attribution window.
	progress("Leg B: wait-event sampling under c=$CS_ATTR_C");
	my $sampler_secs = $CS_LADDER_SECS * 3;
	my $pid = fork();
	die "fork failed: $!" unless defined $pid;
	if ($pid == 0)
	{
		# child: exec (NOT perl exit -- that would fire Cluster.pm's END
		# teardown and stop the nodes under the parent)
		exec("$PGBENCH -n -T $sampler_secs -c $CS_ATTR_C -j 2 "
			  . "-D scale=$CS_SCALE -f $COMMIT_SCRIPT "
			  . conn_args($solo)
			  . " >/dev/null 2>&1")
		  or do { POSIX::_exit(1) };
	}
	my %we;
	my $deadline = Time::HiRes::time() + $sampler_secs - 0.5;
	my $samples  = 0;
	while (Time::HiRes::time() < $deadline)
	{
		my $rows = eval {
			$solo->safe_psql('postgres',
				"SELECT coalesce(wait_event_type,'CPU') || ':' || "
				  . "coalesce(wait_event,'running'), count(*) "
				  . "FROM pg_stat_activity WHERE state='active' "
				  . "AND backend_type='client backend' "
				  . "AND query NOT LIKE '%pg_stat_activity%' "
				  . "GROUP BY 1");
		};
		if (defined $rows)
		{
			$samples++;
			for my $line (split /\n/, $rows)
			{
				my ($k, $n) = split /\|/, $line, 2;
				$we{$k} += $n if defined $k && length $k;
			}
		}
		Time::HiRes::sleep(0.05);
	}
	waitpid($pid, 0);
	report("");
	report(sprintf(
		"### wait-event census under c=%d commit workload (%d samples x 50ms)",
		$CS_ATTR_C, $samples));
	report("");
	report("| wait event | backend-samples | share %% |");
	report("|---|---:|---:|");
	my $we_total = 0;
	$we_total += $_ for values %we;
	for my $k (sort { $we{$b} <=> $we{$a} } keys %we)
	{
		my $share = $we_total ? 100 * $we{$k} / $we_total : 0;
		next if $share < 0.5;
		report(sprintf("| %s | %d | %.1f%% |", $k, $we{$k}, $share));
		(my $tk = $k) =~ s/[^A-Za-z0-9]+/_/g;
		$TSV{"b.we.$tk.share_pct"} = sprintf("%.1f", $share);
	}

	$solo->stop;
	$native->stop;
	return;
}

# ----------
# Leg C -- BOC staleness (pair; idle + busy)
# ----------
sub stale_histogram
{
	my ($label, @ms) = @_;
	my @counts = (0) x (@STALE_BOUNDS_MS + 1);
	for my $v (@ms)
	{
		my $i = 0;
		$i++ while $i < @STALE_BOUNDS_MS && $v > $STALE_BOUNDS_MS[$i];
		$counts[$i]++;
	}
	report("");
	report("### $label (n=" . scalar(@ms) . ")");
	report("");
	report(sprintf("p50 %.1f ms | p99 %.1f ms | max %.1f ms",
		median(@ms) // 0, pctile(99, @ms) // 0,
		(sort { $b <=> $a } @ms)[0] // 0));
	report("");
	report("| bucket | count |");
	report("|---|---:|");
	for my $i (0 .. $#STALE_BOUNDS_MS)
	{
		report(sprintf("| <= %d ms | %d |", $STALE_BOUNDS_MS[$i], $counts[$i]));
	}
	report(sprintf("| > %d ms | %d |", $STALE_BOUNDS_MS[-1], $counts[-1]));
	(my $tl = lc $label) =~ s/[^a-z0-9]+/_/g;
	$TSV{"c.$tl.p50_ms"} = sprintf("%.1f", median(@ms) // 0);
	$TSV{"c.$tl.p99_ms"} = sprintf("%.1f", pctile(99, @ms) // 0);
	return;
}

sub leg_c
{
	progress("Leg C: boot pair for BOC staleness");
	my $pair  = boot_pair('off', 'c');
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	$node0->safe_psql('postgres',
		'CREATE TABLE census_stale (id int primary key, v bigint); '
		  . 'INSERT INTO census_stale VALUES (1, 0)');

	my $committer = $node0->background_psql('postgres');
	my $observer  = $node1->background_psql('postgres');

	my $scn_sql =
	  "SELECT value FROM pg_cluster_state WHERE category='scn' AND key='scn_max_observed_remote'";

	my $run_mode = sub {
		my ($mode) = @_;
		my @samples_ms;
		# Busy mode: background TPC-B churn on node0 keeps BOC traffic
		# flowing; idle mode measures the clock-stall case the spec
		# calls out (no traffic -> remote observes only via sweep).
		my $bgpid;
		if ($mode eq 'busy')
		{
			$bgpid = fork();
			die "fork failed: $!" unless defined $bgpid;
			if ($bgpid == 0)
			{
				# exec, not perl exit (Cluster.pm END teardown hazard)
				exec("$PGBENCH -n -T " . (60 + $CS_STALE_N)
					  . " -c 1 -j 1 -N " . conn_args($node0)
					  . " >/dev/null 2>&1")
				  or do { POSIX::_exit(1) };
			}
			sleep 2;    # let the churn establish
		}
		for my $i (1 .. $CS_STALE_N)
		{
			# One committed write on node0 (autocommit), then read the
			# post-commit local SCN in the same session.  t0 is the
			# commit return; the extra SELECT round trip biases scn0
			# upward under concurrent traffic (staleness reads high --
			# the conservative direction; exact in idle mode).
			$committer->query_safe(
				'UPDATE census_stale SET v = v + 1 WHERE id = 1');
			my $t0 = Time::HiRes::time();
			my $scn0 = $committer->query_safe(
				"SELECT value FROM pg_cluster_state WHERE category='scn' AND key='scn_current_local'"
			);
			$scn0 =~ s/\s+//g;
			my $deadline = $t0 + 2.0;
			my $dt_ms;
			while (Time::HiRes::time() < $deadline)
			{
				my $obs = $observer->query_safe($scn_sql);
				$obs =~ s/\s+//g;
				if (length $obs && length $scn0 && $obs + 0 >= $scn0 + 0)
				{
					$dt_ms = (Time::HiRes::time() - $t0) * 1000;
					last;
				}
				Time::HiRes::sleep(0.002);
			}
			push @samples_ms, $dt_ms if defined $dt_ms;
			# Idle spacing: let the sweep window fully close between
			# samples so each commit lands in a fresh window.
			Time::HiRes::sleep($mode eq 'idle' ? 0.15 : 0.02);
		}
		waitpid($bgpid, 0) if defined $bgpid && $bgpid > 0;
		return @samples_ms;
	};

	report("");
	report("## Leg C -- BOC staleness: commit(node0) -> observe(node1)");
	report("");
	report("same-host same-clock differencing; polling granularity ~2ms + one "
		  . "background-psql round trip -- treat sub-5ms readings as bounds, "
		  . "not exact values (spec-7.4 H-2)");

	my @idle = $run_mode->('idle');
	stale_histogram('idle (no other traffic; Lamport clock-stall case)', @idle);

	my @busy = $run_mode->('busy');
	stale_histogram('busy (background TPC-B churn on node0)', @busy);

	$committer->quit;
	$observer->quit;
	$pair->stop_pair if $pair->can('stop_pair');
	return;
}

# ----------
# Leg D -- hwm publication-channel audit (counter deltas over Leg A)
# ----------
sub leg_d
{
	my ($commit_count, $pre, $post, $fsync) = @_;
	progress("Leg D: hwm co-sample channel audit");

	report("");
	report("## Leg D -- live_hwm publication channel audit (fsync=$fsync window)");
	report("");
	report("today's only publication path is the UNDO_TT_FETCH reply-header "
		  . "co-sample (cluster_gcs_block.c) consumed into per-backend "
		  . "memos; there is no node-level hwm cache and no proactive "
		  . "broadcast.  Counter deltas across the Leg A commit window:");
	report("");
	report("| node | category.key | delta |");
	report("|---|---|---:|");
	for my $side (sort keys %$pre)
	{
		my ($cat) = $side =~ /^([a-z]+)/;
		for my $k (sort keys %{ $post->{$side} })
		{
			next unless ($post->{$side}{$k} // '') =~ /^-?\d+$/;
			my $d = $post->{$side}{$k} - ($pre->{$side}{$k} // 0);
			next unless $d != 0;
			report(sprintf("| %s | %s.%s | %d |", $side, $cat, $k, $d));
			(my $tk = "$side.$k") =~ s/[^A-Za-z0-9]+/_/g;
			$TSV{"d.$fsync.$tk"} = $d;
		}
	}
	report("");
	report(sprintf(
		"commit rate context: %d commits on node0 in this window; node1's "
		  . "hwm view moved only by BOC observe bumps (SCN freshness), "
		  . "never by an hwm publication -- the channel is passive "
		  . "(fetch-piggyback) only.", $commit_count // 0));
	return;
}

# ----------
# Leg E -- version-comparison leg (exact t/328 methodology, one install)
# ----------
sub leg_e
{
	my $label = $ENV{CS_E_LABEL} // ($ENV{CS_INSTALL} // 'in-path');
	progress("Leg E ($label): boot native + solo (t/328 parameters)");
	my $native = boot_native('off', 'e');
	my $solo   = boot_solo('off', 'e');

	pgbench_init($native) or die "pgbench init (native) failed\n";
	pgbench_init($solo)   or die "pgbench init (solo) failed\n";

	my (@nat, @clu, @taxes);
	for my $i (1 .. $CS_E_ROUNDS)
	{
		my $n = pgbench_tpcb($native, 2, 4);
		my $c = pgbench_tpcb($solo,   2, 4);
		next unless defined $n && defined $c && $n > 0 && $c > 0;
		push @nat,   $n;
		push @clu,   $c;
		push @taxes, 100 * ($n - $c) / $n;
	}
	die "Leg E produced no valid rounds\n" unless @taxes;

	my ($nm, $cm, $tm) = (median(@nat), median(@clu), median(@taxes));
	report("");
	report("## Leg E -- version leg: $label");
	report("");
	report(sprintf(
		"t/328 methodology (TPC-B -N, 4 clients, 2s x %d interleaved pairs, "
		  . "fsync=off, medians)", scalar(@taxes)));
	report("");
	report(sprintf("- native  tps median: %.0f", $nm));
	report(sprintf("- cluster tps median: %.0f", $cm));
	report(sprintf("- write tax median:   %.2f%%", $tm));
	(my $tl = lc $label) =~ s/[^a-z0-9]+/_/g;
	$TSV{"e.$tl.native_tps"}  = sprintf("%.0f", $nm);
	$TSV{"e.$tl.cluster_tps"} = sprintf("%.0f", $cm);
	$TSV{"e.$tl.tax_pct"}     = sprintf("%.2f", $tm);

	$solo->stop;
	$native->stop;
	return;
}

# ----------
# main
# ----------
report("# spec-7.4 D0 commit census -- " . scalar(localtime));
report("");
report(sprintf("host: %s | legs: %s | install: %s",
	`hostname -s` =~ s/\s+//gr, $CS_LEGS, $ENV{CS_INSTALL} // '(PATH)'));

if ($CS_LEGS =~ /a/)
{
	leg_a('off');
	leg_a('on');
}
leg_b() if $CS_LEGS =~ /b/;
leg_c() if $CS_LEGS =~ /c/;
# Leg D runs inside leg_a (shares the pair); standalone only with A.
leg_e() if $CS_LEGS =~ /e/;

make_path(dirname($CS_OUT));
open my $out, '>', $CS_OUT or die "cannot write $CS_OUT: $!";
print $out "$_\n" for @REPORT;
close $out;
progress("report written: $CS_OUT");

if (length $CS_TSV_OUT)
{
	make_path(dirname($CS_TSV_OUT));
	open my $tsv, '>', $CS_TSV_OUT or die "cannot write $CS_TSV_OUT: $!";
	print $tsv "$_\t$TSV{$_}\n" for sort keys %TSV;
	close $tsv;
	progress("tsv written: $CS_TSV_OUT");
}

print $REAL_STDOUT join("\n", @REPORT), "\n";

ok(1, 'census completed (report-only)');
done_testing();
