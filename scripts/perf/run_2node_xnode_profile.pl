#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# run_2node_xnode_profile.pl
#    spec-5.59 D8 -- four-axis cross-node profiling harness (REPORT-ONLY).
#
#    Boots three tiers under one perf conf (autovacuum off, fsync off,
#    shared_buffers 64MB): (a) native, (b) solo cluster node, (c) 2-node
#    ClusterPair (shared_data + 3 voting disks) -- cluster tiers with
#    cluster.xnode_profile = on.  Drives the four axis scenarios against
#    the pair, snapshotting the full xnode_profile dump (all 51 keys) per
#    node before/after each scenario and reporting per-bucket deltas:
#      W  pgbench -n -N -c 1 on node0 with peer online (M3 single-writer),
#         interleaved with native + solo rounds (three-tier medians =>
#         measured two-node tax %)
#      R  node1 full-table reads of a duplicated phantom-shared plain-heap
#         table (xp_read) written on node0 + the spec §3.6 amortization
#         probe (reship rate before/after VACUUM on node0)
#      I  node0-only right-growing PK index; its blocks hash-master ~50%
#         onto node1, so node0's own index maintenance IS the cross-node
#         traffic
#      C  many tiny single-row commits from node0 (SCN advance + BOC)
#    plus two observe-only legs (never failures): -c 2 write collapse and
#    alternating same-rows updates (fail-closed retry observation).
#
#    Attribution contract: Table 1 folds ONLY the requester decision
#    buckets into pp of the measured tax; Table 2 lists every other
#    bucket (nested / service-side / overlay) as raw nanos/event ONLY.
#    REPORT-ONLY (mirror run-cr-profile.sh): trend datapoints, never a
#    gate; exit 0 on completion; die only on harness failure.
#
#    Env knobs: XP_SECS (8) per-scenario seconds; XP_ROUNDS (5) TPS
#    rounds; XP_SCALE (10) pgbench scale; XP_OUT report path (default
#    scripts/perf/results/xnode-profile-<epoch>.md); XP_INSTALL optional
#    install prefix prepended to PATH (else PATH, as Cluster.pm does);
#    PGBENCH pgbench binary override.  Run from repo root in a built
#    tree: perl scripts/perf/run_2node_xnode_profile.pl
#
#    spec-6.12 D0-shared additive hookups (buckets/attribution untouched):
#      XP_EXTRA_GUC  semicolon-separated GUC lines injected into the solo
#                    + pair cluster tiers (lever wave switches; default
#                    empty = byte-identical 5.59 behaviour)
#      XP_STORAGE    'block_device' boots the PAIR on the spec-6.0a raw
#                    block_device backend (loopback file image) instead
#                    of the shared-nothing phantom harness
#      XP_TSV_OUT    optional machine-readable key<TAB>value dump of all
#                    per-axis bucket deltas + scalars (consumed by
#                    run_612_value_gate.pl off/on comparison)
#      XP_NETEM_MS   report-header honesty note only (netem-tier.sh does
#                    the actual tc qdisc setup; runner never mutates it)
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.59-two-node-crossnode-perf-profile.md (D8)
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (D0-shared)
#
# IDENTIFICATION
#    scripts/perf/run_2node_xnode_profile.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../src/test/perl";

use File::Basename qw(dirname);
use File::Path qw(make_path);
use File::Spec;
use Time::HiRes ();

# One BEGIN block, three compile-time jobs: (1) capture the real STDOUT
# before PostgreSQL::Test::Utils' INIT hijacks it into the regress log,
# so the report can reach the terminal; (2) prepend <XP_INSTALL>/bin to
# PATH so Cluster.pm finds initdb/pg_ctl/psql; (3) load the TAP fixtures
# so their INIT blocks fire in order (run_2node_baseline.pl Hardening
# v1.0.1 too-late-INIT pattern).
our ($REAL_STDOUT, $have_fixtures, $fixtures_err);

BEGIN
{
	open($REAL_STDOUT, '>&', \*STDOUT)
	  or die "cannot dup STDOUT: $!\n";
	my $prev = select($REAL_STDOUT);
	$| = 1;
	select($prev);

	if (defined $ENV{XP_INSTALL} && length $ENV{XP_INSTALL})
	{
		$ENV{PATH} = "$ENV{XP_INSTALL}/bin:$ENV{PATH}";
		for my $var (qw(LD_LIBRARY_PATH DYLD_LIBRARY_PATH))
		{
			$ENV{$var} = join(':', "$ENV{XP_INSTALL}/lib",
				grep { defined && length } $ENV{$var});
		}
	}

	# Cluster.pm shells out to "$PG_REGRESS --config-auth"; when invoked
	# outside a make-check harness (this runner, the perf.yml job) the
	# variable is unset, so default it to the built tree's pg_regress.
	# TESTDATADIR/TESTLOGDIR must be ABSOLUTE for the same reason: the
	# ClusterPair shared_data dir is carved from the tempdir root and
	# cluster.shared_data_dir rejects relative paths.
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
my $XP_SECS   = $ENV{XP_SECS} // 8;
my $XP_ROUNDS = $ENV{XP_ROUNDS} // 5;
my $XP_SCALE  = $ENV{XP_SCALE} // 10;
my $PGBENCH   = $ENV{PGBENCH} // 'pgbench';
my $OBS_SECS  = 4;
our $PAIR_SCALE_USED;    # observe-only write-collapse leg ("a few seconds")

my $REPO_ROOT = File::Spec->rel2abs("$FindBin::RealBin/../..");
my $XP_OUT    = $ENV{XP_OUT}
  // File::Spec->catfile($REPO_ROOT, 'scripts', 'perf', 'results',
	'xnode-profile-' . time() . '.md');

# spec-6.12 D0-shared knobs (all default-off; empty = 5.59 behaviour).
my $XP_EXTRA_GUC = $ENV{XP_EXTRA_GUC} // '';
my $XP_STORAGE   = $ENV{XP_STORAGE} // '';
my $XP_TSV_OUT   = $ENV{XP_TSV_OUT} // '';
my $XP_NETEM_MS  = $ENV{XP_NETEM_MS} // '';

die "XP_STORAGE must be '' or 'block_device' (got '$XP_STORAGE')\n"
  unless $XP_STORAGE eq '' || $XP_STORAGE eq 'block_device';

# The 23 buckets of cluster_xnode_profile.h, dump-key order.
my @ALL_BUCKETS = qw(
	w_gcs_x_request w_gcs_x_receive w_gcs_x_install w_gcs_x_invalidate
	w_ges_enqueue w_ges_convert w_ges_wait w_ges_wake w_hw_extend
	r_gcs_s_request r_gcs_s_receive r_readimage_ship r_sholder_cache_hit
	r_cr_construct r_cr_chain_walk r_tt_visibility_resolve
	i_index_block_xfer i_rightmost_leaf_ping
	c_scn_commit_advance c_scn_boc_broadcast
	ic_send_service ic_inbound_dispatch
	local_undo_itl_wal
);

# Table 1: requester decision buckets -- the ONLY set pp-folding is valid
# for (requester-exclusive-wait; spec-5.59 attribution contract, mirrors
# t/334 @DECISION_NANOS_KEYS).
my @DECISION_BUCKETS = qw(
	w_gcs_x_request r_gcs_s_request w_ges_enqueue w_ges_convert
	w_hw_extend r_cr_construct r_tt_visibility_resolve
	c_scn_commit_advance local_undo_itl_wal
);
my %IS_DECISION = map { $_ => 1 } @DECISION_BUCKETS;

# Table 2 annotations: nested-under / service-side / overlay roles.
my %SERVICE_NOTE = (
	w_gcs_x_receive       => 'nested under w_gcs_x_request',
	w_gcs_x_install       => 'nested under w_gcs_x_request',
	w_gcs_x_invalidate    => 'service-side (invalidate bcast + acks)',
	w_ges_wait            => 'nested under w_ges_enqueue/w_ges_convert',
	w_ges_wake            => 'service-side (master waiter wake)',
	r_gcs_s_receive       => 'nested under r_gcs_s_request',
	r_readimage_ship      => 'service-side (holder read-image ship)',
	r_sholder_cache_hit   => 'reserved lever probe (S-holder cache)',
	r_cr_chain_walk       => 'nested under r_cr_construct',
	i_index_block_xfer    => 'overlay (also counted in GCS buckets)',
	i_rightmost_leaf_ping => 'count-only probe (GUC-gated)',
	c_scn_boc_broadcast   => 'service-side (BOC fanout send)',
	ic_send_service       => 'service-side (IC one-way send)',
	ic_inbound_dispatch   => 'service-side (IC inbound dispatch)',
);

my @perf_conf = (
	"autovacuum = off\n",
	"fsync = off\n",
	"shared_buffers = 64MB\n",
);

my @REPORT;

# ----------
# small helpers
# ----------
sub progress
{
	my ($msg) = @_;
	print $REAL_STDOUT "== $msg\n";
	print "== $msg\n";    # hijacked STDOUT -> regress log copy
}

sub rep { push @REPORT, @_; }

sub median
{
	my @s = sort { $a <=> $b } @_;
	return undef unless @s;
	return $s[int((@s) / 2)];
}

sub ms_str { return sprintf('%.3f', $_[0] / 1e6); }

sub ns_per_event
{
	my ($nanos, $n) = @_;
	return 'n/a' unless defined $n && $n > 0;
	return sprintf('%.0f', $nanos / $n);
}


# Cross-node statements can hit retryable fail-closed / timeout errors
# (rule-8.A behaviour under transient load); bounded retry, never die.
sub psql_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 5;
	for my $i (1 .. $tries)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		Time::HiRes::sleep(0.5);
	}
	return 0;
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
# xnode_profile dump snapshots (all 51 keys) + deltas
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

# reset_generation sanity: a mid-scenario reset invalidates deltas.
sub xp_reset_stable
{
	my ($before, $after) = @_;
	return ($before->{reset_generation} // 0) ==
	  ($after->{reset_generation} // 0);
}

# Snapshot both pair nodes at once; pair_deltas turns two such snapshots
# into the (d0, d1, reset_stable) triple every axis result carries.
sub snap_pair
{
	my ($node0, $node1) = @_;
	return [ xp_snapshot($node0), xp_snapshot($node1) ];
}

sub pair_deltas
{
	my ($before, $after) = @_;
	return (
		d0           => xp_delta($before->[0], $after->[0]),
		d1           => xp_delta($before->[1], $after->[1]),
		reset_stable => (xp_reset_stable($before->[0], $after->[0])
			  && xp_reset_stable($before->[1], $after->[1])),
	);
}

sub bkt_n  { my ($d, $b) = @_; return $d->{"bucket.$b.n_events"}    // 0; }
sub bkt_ns { my ($d, $b) = @_; return $d->{"bucket.$b.total_nanos"} // 0; }

# ----------
# spec-6.12 D0-shared helpers (additive; inert when the knobs are unset)
# ----------

# XP_EXTRA_GUC: semicolon-separated "name = value" lines for the cluster
# tiers (solo + pair).  Whitespace-trimmed; empty items skipped.
sub extra_guc_lines
{
	return () unless length $XP_EXTRA_GUC;
	my @lines;
	for my $item (split /;/, $XP_EXTRA_GUC)
	{
		$item =~ s/^\s+|\s+$//g;
		next unless length $item;
		die "XP_EXTRA_GUC item '$item' is not 'name = value'\n"
		  unless $item =~ /^[A-Za-z0-9_.]+\s*=\s*\S/;
		push @lines, $item;
	}
	return @lines;
}

# XP_STORAGE=block_device: pair boots on the spec-6.0a raw block_device
# backend over a loopback file image (t/333 pattern).
sub make_raw_image
{
	my ($path, $size_mb) = @_;
	open(my $fh, '>', $path) or die "open $path: $!";
	truncate($fh, $size_mb * 1024 * 1024) or die "truncate $path: $!";
	close($fh) or die "close $path: $!";
}

sub storage_conf_lines
{
	return () unless $XP_STORAGE eq 'block_device';
	my $raw_dir   = PostgreSQL::Test::Utils::tempdir();
	my $raw_image = "$raw_dir/xpr_612_raw_device.img";
	make_raw_image($raw_image, 512);
	my $q = $raw_image;
	$q =~ s/'/''/g;
	return (
		'cluster.shared_storage_backend = block_device',
		"cluster.block_device_path = '$q'",
		'cluster.block_device_use_odirect = off',
		'cluster.storage_fence_driver = disabled',
		'cluster.smgr_user_relations = on',
	);
}

# Honesty note for the report header: report the loopback netem state as
# observed (Linux tc only); the runner never mutates qdiscs itself.
sub netem_status
{
	return 'n/a (non-Linux; loopback RTT is NOT representative)'
	  unless $^O eq 'linux';
	my $out = `tc qdisc show dev lo 2>/dev/null`;
	chomp($out //= '');
	return ($out =~ /netem/)
	  ? "active ($out)"
	  : 'none (loopback RTT is NOT representative; see netem-tier.sh)';
}

# XP_TSV_OUT: machine-readable dump of per-axis deltas + scalars for the
# run_612_value_gate.pl off/on comparison.  Keys are stable API.
my %TSV;

sub tsv_put { my ($k, $v) = @_; $TSV{$k} = $v if defined $v; }

sub tsv_axis_deltas
{
	my ($axis, $res) = @_;
	for my $side (qw(d0 d1))
	{
		my $d = $res->{$side} or next;
		tsv_put("$axis.$side.$_", $d->{$_}) for keys %$d;
	}
}

sub write_tsv
{
	my ($w, $r, $i, $c) = @_;
	return unless length $XP_TSV_OUT;

	tsv_axis_deltas('w', $w);
	tsv_axis_deltas('i', $i);
	tsv_axis_deltas('c', $c);
	tsv_put('w.nat_med',      $w->{nat_med});
	tsv_put('w.solo_med',     $w->{solo_med});
	tsv_put('w.pair_med',     $w->{pair_med});
	tsv_put('w.tax_pair_pct', $w->{tax_pair_pct});
	tsv_put('w.wall_ns',      $w->{wall_ns});
	tsv_put('i.batches',      $i->{batches});
	tsv_put('i.errors',       $i->{errors});
	tsv_put('i.wall_ns',      $i->{wall_ns});
	tsv_put('c.txns',         $c->{txns});
	tsv_put('c.errors',       $c->{errors});
	tsv_put('c.wall_ns',      $c->{wall_ns});
	for my $phase (qw(pre post))
	{
		my $p = $r->{$phase} or next;
		tsv_put("r.$phase.$_", $p->{$_})
		  for qw(rounds errors reship sholder wall_ns);
		tsv_put("r.$phase.delta.$_", $p->{delta}{$_}) for keys %{ $p->{delta} };
	}

	make_path(dirname($XP_TSV_OUT));
	open(my $fh, '>', $XP_TSV_OUT) or die "cannot write $XP_TSV_OUT: $!\n";
	print $fh "$_\t$TSV{$_}\n" for sort keys %TSV;
	close $fh;
	progress("tsv written: $XP_TSV_OUT");
}

# ----------
# pgbench (mirror t/328 helpers; -c 1 -N is the M3 single-writer shape)
# ----------
sub pgbench_one
{
	my ($node, $clients, $secs) = @_;
	$clients //= 1;
	$secs    //= $XP_SECS;
	my $conn = "-h '" . $node->host . "' -p " . $node->port . ' postgres';
	my $out  = `$PGBENCH -n -N -T $secs -c $clients $conn 2>&1`;
	return ($1 + 0.0, $out)
	  if $out =~ /tps\s*=\s*([\d.]+)\s*\(without initial/;
	return ($1 + 0.0, $out) if $out =~ /tps\s*=\s*([\d.]+)/;
	return (undef, $out);
}

sub pgbench_init
{
	my ($node, $scale) = @_;
	$scale //= $XP_SCALE;
	my $conn = "-h '" . $node->host . "' -p " . $node->port . ' postgres';
	for my $attempt (1, 2)
	{
		my $out = `$PGBENCH -i -s $scale -q $conn 2>&1`;
		return 1 if $? == 0;
		# Never swallow the failure mode: the CI artifact must show WHY.
		progress("pgbench init attempt $attempt (scale $scale) failed on "
			  . $node->name . ": $out");
		sleep 3;    # let a transient shipping/dedup storm drain
	}
	return 0;
}

# The phantom pair's bulk COPY is storm-prone on slow local machines
# (dedup / transition-denied bursts under PCM-on shipping).  Degrade the
# scale rather than dying: the harness is REPORT-ONLY and records the
# scale actually used; 0 means the pair W-axis TPS stays n/a.
sub pgbench_init_pair_degrading
{
	my ($node) = @_;

	# spec-6.12d: XP_PAIR_INIT_LEASE=1 arms the static space-affinity lease
	# for the DURATION OF THE INIT ONLY (ALTER SYSTEM + reload; PGC_SIGHUP),
	# then restores the boot-conf value before the measurement phase.  The
	# pair bulk COPY storm (extend-per-block HW master round-trips) is what
	# made pair W-axis tax unmeasurable on every platform; leases collapse
	# the extend traffic so init can complete.  Used identically by BOTH
	# value-gate legs, so the A/B still differs only in the measured GUC.
	my $init_lease = ($ENV{XP_PAIR_INIT_LEASE} // '') eq '1';
	if ($init_lease)
	{
		eval {
			$node->safe_psql('postgres',
				"ALTER SYSTEM SET cluster.space_affinity = 'static'; "
				  . 'SELECT pg_reload_conf()');
			1;
		} or progress('pair init lease crutch: arm failed (continuing)');
	}

	my $scale_got = 0;
	for my $scale ($XP_SCALE, 4, 2, 1)
	{
		if (pgbench_init($node, $scale)) { $scale_got = $scale; last; }
		progress("pair pgbench init: degrading below scale $scale");
		# a failed bulk COPY leaves partial tables + storm state behind;
		# clear them (best effort) before the next, smaller attempt.
		eval {
			$node->safe_psql('postgres',
				'DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, '
				  . 'pgbench_history, pgbench_tellers');
			1;
		};
		sleep 2;
	}

	if ($init_lease)
	{
		eval {
			$node->safe_psql('postgres',
				'ALTER SYSTEM RESET cluster.space_affinity; '
				  . 'SELECT pg_reload_conf()');
			1;
		} or progress('pair init lease crutch: restore failed');
	}
	return $scale_got;
}

# Loop one SQL text against a node until $secs elapse; each safe_psql call
# is one round.  Errors are observations (counted), never fatal.  The FIRST
# error text is kept (one line) so an invalid phase self-describes WHY in
# the report instead of just showing an error count.
sub run_sql_rounds
{
	my ($node, $sql, $secs) = @_;
	my $t0       = Time::HiRes::time();
	my $deadline = $t0 + $secs;
	my ($rounds, $errors, $first_err) = (0, 0, undef);
	while (Time::HiRes::time() < $deadline)
	{
		unless (eval { $node->safe_psql('postgres', $sql); 1 })
		{
			$errors++;
			if (!defined $first_err)
			{
				($first_err) = ($@ =~ /(ERROR:[^\n]*)/);
				$first_err //= 'unknown (no ERROR line in psql output)';
			}
		}
		$rounds++;
	}
	return ($rounds, (Time::HiRes::time() - $t0) * 1e9, $errors, $first_err);
}

# ----------
# tier boots
# ----------
sub boot_native
{
	my $native = PostgreSQL::Test::Cluster->new('xpr_native');
	$native->init;
	$native->append_conf('postgresql.conf', $_) for @perf_conf;
	$native->start;
	return $native;
}

sub boot_solo
{
	my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
	my $solo    = PostgreSQL::Test::Cluster->new('xpr_solo');
	$solo->init;
	$solo->append_conf('postgresql.conf', "$_\n")
	  for ('cluster.enabled = on', 'cluster.interconnect_tier = tier1',
		'cluster.allow_single_node = on', 'cluster.node_id = 0',
		'cluster.xnode_profile = on');
	$solo->append_conf('postgresql.conf', $_) for @perf_conf;
	# spec-6.12 D0: lever wave switches apply to both cluster tiers.
	$solo->append_conf('postgresql.conf', "$_\n") for extra_guc_lines();
	PostgreSQL::Test::Utils::append_to_file(
		$solo->data_dir . '/pgrac.conf',
		"[cluster]\nname = xpr_solo\n\n[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n\n"
	);
	$solo->start;
	return $solo;
}

sub boot_pair
{
	my @pair_conf = map { my $l = $_; chomp $l; $l } @perf_conf;
	my $pair      = PostgreSQL::Test::ClusterPair->new_pair(
		'xpr_pair',
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [
			@pair_conf,
			'cluster.xnode_profile = on',
			# Bulk pgbench -i COPY on the pair can exhaust the default
			# retransmit budget on a loaded local machine (cassert +
			# shipping storm); a profiling harness prefers patience.
			'cluster.gcs_block_retransmit_max_retries = 8',
			# Bulk COPY floods many distinct in-flight block requests; the
			# default 1024-entry dedup HTAB TTL-sweeps too slowly on a slow
			# cassert machine -> DENIED_DEDUP_FULL storms.  Profiling
			# harness sizes it out of the way.
			'cluster.gcs_block_dedup_max_entries = 16384',
			'cluster.quorum_poll_interval_ms = 500',
			'cluster.cssd_heartbeat_interval_ms = 2000',
			'cluster.cssd_dead_deadband_factor = 10',
			# spec-6.12 D0: 6.0a raw backend substrate + lever switches.
			storage_conf_lines(),
			extra_guc_lines(),
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


# Create every phantom-shared (coincidence-dependent) table up front,
# IMMEDIATELY after the pair reaches quorum: any node0-only DDL (the
# index/commit axis tables, pgbench init) advances node0's relfilenode
# counter and would break the same-DDL/same-relfilepath premise for any
# later duplicated table (t/334 pattern).
sub setup_phantom_tables
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);
	for my $tbl (qw(xp_read xp_hot))
	{
		$node0->safe_psql('postgres', "CREATE TABLE $tbl (id int, v int)");
		$node1->safe_psql('postgres', "CREATE TABLE $tbl (id int, v int)");
		my $p0 = $node0->safe_psql('postgres',
			"SELECT pg_relation_filepath('$tbl')");
		my $p1 = $node1->safe_psql('postgres',
			"SELECT pg_relation_filepath('$tbl')");
		die "harness failure: $tbl relfilepath coincidence does not hold "
		  . "(n0=$p0 n1=$p1)\n"
		  unless $p0 eq $p1;
	}
}

# ---- W axis: pair single-writer pgbench + interleaved 3-tier baseline ----
sub axis_write
{
	my ($native, $solo, $pair, $pair_scale) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	progress("W axis: $XP_ROUNDS interleaved rounds x ${XP_SECS}s "
		  . '(native / solo / pair-node0, pgbench -n -N -c 1)');

	my $before = snap_pair($node0, $node1);

	my (@nat, @solo_tps, @pair_tps);
	my $pair_wall_ns = 0;
	for my $r (1 .. $XP_ROUNDS)
	{
		my ($n) = pgbench_one($native, 1, $XP_SECS);
		my ($s) = pgbench_one($solo,   1, $XP_SECS);
		my $t0 = Time::HiRes::time();
		my $p;
		if ($pair_scale)
		{
			($p) = pgbench_one($node0, 1, $XP_SECS);
		}
		else
		{
			# pgbench tables unavailable on the pair: drive a plain SQL
			# write loop so the W buckets still accumulate honest deltas
			# (TPS/tax stay n/a in the report).  eval-wrapped: the pair
			# may need a beat to drain the failed-COPY storm state.
			eval {
				$node0->safe_psql('postgres',
					'CREATE TABLE IF NOT EXISTS xp_wfallback (id int, v int)');
				1;
			} or sleep 2;
			run_sql_rounds($node0,
				'INSERT INTO xp_wfallback SELECT g, g FROM generate_series(1,200) g',
				$XP_SECS);
		}
		$pair_wall_ns += (Time::HiRes::time() - $t0) * 1e9;
		push @nat,      $n if defined $n && $n > 0;
		push @solo_tps, $s if defined $s && $s > 0;
		push @pair_tps, $p if defined $p && $p > 0;
		progress(sprintf('W round %d: native=%s solo=%s pair=%s tps',
			$r, map { defined $_ ? sprintf('%.0f', $_) : 'n/a' } $n, $s, $p));
	}

	my $after = snap_pair($node0, $node1);

	my $nat_med  = median(@nat);
	my $solo_med = median(@solo_tps);
	my $pair_med = median(@pair_tps);
	my $tax_pair =
	  (defined $nat_med && $nat_med > 0 && defined $pair_med)
	  ? 100.0 * (1.0 - $pair_med / $nat_med)
	  : undef;
	my $tax_solo =
	  (defined $nat_med && $nat_med > 0 && defined $solo_med)
	  ? 100.0 * (1.0 - $solo_med / $nat_med)
	  : undef;

	return {
		nat_med => $nat_med, solo_med => $solo_med, pair_med => $pair_med,
		tax_pair_pct => $tax_pair, tax_solo_pct => $tax_solo,
		rounds_used => scalar(@pair_tps), wall_ns => $pair_wall_ns,
		pair_deltas($before, $after),
	};
}

# ---- R axis: node1 cross-node reads + §3.6 amortization probe (VACUUM) ----
sub read_phase
{
	my ($node1) = @_;
	my $before = xp_snapshot($node1);
	my ($rounds, $wall_ns, $errors, $first_err) = run_sql_rounds($node1,
		'SELECT count(*), sum(v) FROM xp_read', $XP_SECS);
	my $after = xp_snapshot($node1);
	my $d     = xp_delta($before, $after);
	return {
		rounds => $rounds, wall_ns => $wall_ns, errors => $errors,
		first_err => $first_err,
		delta => $d,
		reship => $d->{read_reship_count} // 0,
		sholder => $d->{read_sholder_hit_count} // 0,
		reset_stable => xp_reset_stable($before, $after),
	};
}

sub axis_read
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	progress('R axis: phantom-shared xp_read seeded + wide-updated on node0, '
		  . "then node1 full-table reads ${XP_SECS}s (pre/post VACUUM)");

	# Phantom-shared-block harness (t/334 pattern): node1 has NO pgbench
	# catalog entries (pgbench -i ran against node0 only), so the read
	# stream needs a dedicated duplicated table.  Identical plain-heap DDL
	# (no PK, no text/TOAST: duplicated index DDL cannot work over
	# coincident files) on BOTH nodes must land the same relfilepath; then
	# node0 seeds rows and CHECKPOINTs them to shared storage so node1's
	# reads go through cross-node coordination.
	# xp_read was created (and coincidence-checked) by
	# setup_phantom_tables right after the pair reached quorum.

	psql_retry($node0,
		'INSERT INTO xp_read SELECT g, g FROM generate_series(1,5000) g')
	  or progress('R seeding failed after retries; reads will be empty');
	# Wide UPDATE from node0: id % 5 touches rows in every heap block, so
	# every block node1 reads carries node0-written versions.
	psql_retry($node0,
		'UPDATE xp_read SET v = v + 1 WHERE id % 5 = 0');
	psql_retry($node0, 'CHECKPOINT');

	my $pre = read_phase($node1);

	# Amortization experiment: VACUUM on node0, then the SAME reads.
	psql_retry($node0, 'VACUUM xp_read');
	my $post = read_phase($node1);

	# S-cache ceiling estimate from the PRE phase: if reship -> 0, saved
	# = reship_count x avg(r_gcs_s_request ns/event), as % of read wall.
	my $s_req_n  = bkt_n($pre->{delta}, 'r_gcs_s_request');
	my $s_req_ns = bkt_ns($pre->{delta}, 'r_gcs_s_request');
	my $avg_s_ns = ($s_req_n > 0) ? $s_req_ns / $s_req_n : undef;
	my $ceiling_pct =
	  (defined $avg_s_ns && $pre->{wall_ns} > 0)
	  ? 100.0 * ($pre->{reship} * $avg_s_ns) / $pre->{wall_ns}
	  : undef;

	return { pre => $pre, post => $post,
		avg_s_ns => $avg_s_ns, ceiling_pct => $ceiling_pct };
}

# ---- I axis: node0-only right-growing PK index INSERT stream ----
sub axis_index
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	progress("I axis: node0-only right-growing PK INSERT stream ${XP_SECS}s "
		  . '(index blocks hash-master ~50% onto node1)');

	# node0-ONLY index table: duplicated index DDL is impossible over
	# coincident files (the second node's btbuild finds the first node's
	# metapage -- t/334 pattern).  The index blocks still hash-master
	# ~50% onto node1, so node0's own index maintenance IS the
	# cross-node traffic being measured.
	# spec-6.12 D0 (wave f): XP_INDEX_RELOPT rebuilds the right-growing PK
	# index with the given storage parameter (candidate leg:
	# cluster_reverse_key=on).  Default '' keeps the 5.59 DDL byte-identical.
	my $relopt = $ENV{XP_INDEX_RELOPT} // '';
	$node0->safe_psql('postgres',
		'DROP TABLE IF EXISTS xp_index; '
		  . 'CREATE TABLE xp_index (id bigserial, v int, PRIMARY KEY (id)'
		  . ($relopt ne '' ? " WITH ($relopt)" : '')
		  . ')');

	my $before = snap_pair($node0, $node1);

	my $ins = 'INSERT INTO xp_index (v) SELECT g FROM generate_series(1,200) g';
	my $t0       = Time::HiRes::time();
	my $deadline = $t0 + $XP_SECS;
	my ($batches, $errors) = (0, 0);
	while (Time::HiRes::time() < $deadline)
	{
		eval { $node0->safe_psql('postgres', $ins); 1 } or $errors++;
		$batches++;
	}

	return {
		batches => $batches,
		errors  => $errors,
		wall_ns => (Time::HiRes::time() - $t0) * 1e9,
		pair_deltas($before, snap_pair($node0, $node1)),
	};
}

# ---- C axis: many tiny single-row commits from node0 (1 INSERT/txn) ----
sub axis_commit
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	progress("C axis: tiny single-row commits from node0 for ${XP_SECS}s");

	$node0->safe_psql('postgres',
		'DROP TABLE IF EXISTS xp_commit; CREATE TABLE xp_commit (v int)');

	my $before = snap_pair($node0, $node1);

	# 100 single-statement INSERTs per psql invocation; psql autocommit
	# makes each its own transaction (=> 100 commits per batch).
	my $batch = join(";\n", ('INSERT INTO xp_commit VALUES (1)') x 100) . ';';
	my ($batches, $wall_ns, $errors, $first_err) =
	  run_sql_rounds($node0, $batch, $XP_SECS);

	return {
		txns      => $batches * 100,
		errors    => $errors,
		first_err => $first_err,
		wall_ns   => $wall_ns,
		pair_deltas($before, snap_pair($node0, $node1)),
	};
}

# ---- observe-only legs (never fail; errors are OBSERVATIONS) ----
sub observe_hot
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);

	progress('observe-only O2: 10 alternating same-rows updates');

	# O2: alternating same-rows UPDATEs node0/node1 (fail-closed retry
	# observation; cross-node conflicts are expected behaviour).  xp_hot
	# is a duplicated phantom-shared plain-heap table (t/334 pattern; no
	# PK -- duplicated index DDL cannot work over coincident files):
	# identical DDL on both nodes must land the same relfilepath, then
	# node0 seeds + CHECKPOINTs so both nodes update the same blocks.
	# MUST run before the pair pgbench init: a degraded/failed bulk init
	# advances node0's relfilenode counter and breaks the coincidence.
	# xp_hot was created (and coincidence-checked) by
	# setup_phantom_tables right after the pair reached quorum.
	# Seeding failure does NOT make the updates error: xp_hot exists but is
	# empty, so every UPDATE trivially succeeds touching zero rows.  Track
	# the seeded state so the report can say NO SIGNAL instead of the
	# misleading "0 of N errored".
	my $hot_seeded = psql_retry($node0,
		'INSERT INTO xp_hot SELECT g, 0 FROM generate_series(1,100) g')
	  ? 1 : 0;
	progress('O2 seeding failed after retries; '
		  . 'empty-table updates are a NO-SIGNAL observation')
	  unless $hot_seeded;
	psql_retry($node0, 'CHECKPOINT');
	my $hot_sql    = 'UPDATE xp_hot SET v = v + 1 WHERE id <= 50';
	my $hot_errors = 0;
	for my $i (1 .. 5)
	{
		for my $node ($node0, $node1)
		{
			eval { $node->safe_psql('postgres', $hot_sql); 1 }
			  or $hot_errors++;
		}
	}

	return {
		hot_errors   => $hot_errors,
		hot_attempts => 10,
		hot_seeded   => $hot_seeded,
	};
}

sub observe_collapse
{
	my ($pair) = @_;
	my $node0 = $pair->node0;

	progress("observe-only O1: pgbench -c 2 ${OBS_SECS}s (write collapse)");

	# O1: -c 2 write pressure on node0 (pgbench tables from the pair
	# init; skipped honestly when that init never succeeded).
	my ($collapse_tps, $collapse_out) = (undef, '');
	($collapse_tps, $collapse_out) = pgbench_one($node0, 2, $OBS_SECS)
	  if $PAIR_SCALE_USED;
	my $collapse_errs = () = ($collapse_out =~ /error|FATAL|aborted/ig);

	return { collapse_tps => $collapse_tps,
		collapse_errs => $collapse_errs };
}

# ---- report sections ----
sub report_header
{
	my $commit = `git -C '$REPO_ROOT' rev-parse --short HEAD 2>/dev/null`;
	chomp($commit //= '');
	rep('# spec-5.59 D8 cross-node four-axis profile (REPORT-ONLY)', '',
		sprintf('- date: %s UTC | commit: %s',
			scalar(gmtime()), $commit || 'unknown'),
		"- knobs: XP_SECS=$XP_SECS XP_ROUNDS=$XP_ROUNDS XP_SCALE=$XP_SCALE"
		  . " pair_scale=" . ($PAIR_SCALE_USED || 'n/a'),
		# spec-6.12 D0 provenance: every value-gate comparison must be
		# attributable to its wave switch / substrate / latency tier.
		'- 6.12 extra_guc: ' . (length $XP_EXTRA_GUC ? $XP_EXTRA_GUC : 'none'),
		'- 6.12 storage: '
		  . ($XP_STORAGE eq 'block_device'
			? 'spec-6.0a raw block_device (pair)'
			: 'shared-nothing phantom harness (shipping-count only, AD-015)'),
		'- 6.12 netem: ' . netem_status()
		  . (length $XP_NETEM_MS ? " (declared: ${XP_NETEM_MS}ms)" : ''),
		'',
		'> HONESTY PREAMBLE: single-machine loopback interconnect; cassert',
		'> builds inflate absolute nanoseconds; background cluster traffic',
		'> leaks small counts into scenario deltas.  Trends and ratios are',
		'> the signal, absolute numbers are not.  Every value below is a',
		'> trend datapoint, never a gate (mirror of run-cr-profile.sh).',
		'');
}

sub report_three_tier
{
	my ($w) = @_;
	my $f = sub { defined $_[0] ? sprintf('%.0f', $_[0]) : 'n/a' };
	my $p = sub { defined $_[0] ? sprintf('%.2f', $_[0]) : 'n/a' };
	rep('## Three-tier TPS baseline (pgbench -n -N -c 1, medians of '
		  . "$w->{rounds_used} interleaved rounds)",
		'',
		'| tier | median tps | tax vs native |',
		'|---|---|---|',
		sprintf('| native (cluster off) | %s | - |', $f->($w->{nat_med})),
		sprintf('| solo cluster node | %s | %s%% |',
			$f->($w->{solo_med}), $p->($w->{tax_solo_pct})),
		sprintf('| 2-node pair, node0 single-writer | %s | %s%% |',
			$f->($w->{pair_med}), $p->($w->{tax_pair_pct})),
		'',
		sprintf('measured two-node tax used for pp folding: %s%%',
			$p->($w->{tax_pair_pct})),
		'',
		'reset_generation stable across W scenario: '
		  . ($w->{reset_stable} ? 'yes' : 'NO (deltas unreliable)'),
		'');
}

# Table 1: pp folding is ONLY valid here (requester exclusive-wait set).
sub report_decision_table
{
	my ($w) = @_;
	rep('## Table 1 -- requester decision buckets (pp-foldable; pair write '
		  . 'scenario, node0)',
		'');
	rep('pp = bucket_nanos / workload_wall_ns x measured_two_node_tax_pct',
		'');
	rep('| bucket | n_events | total_ms | ns/event | pp (of tax) |',
		'|---|---|---|---|---|');
	my $pp_sum = 0;
	for my $b (@DECISION_BUCKETS)
	{
		my $n  = bkt_n($w->{d0}, $b);
		my $ns = bkt_ns($w->{d0}, $b);
		my $pp = (defined $w->{tax_pair_pct} && $w->{wall_ns} > 0)
		  ? ($ns / $w->{wall_ns}) * $w->{tax_pair_pct} : undef;
		$pp_sum += $pp if defined $pp;
		rep(sprintf('| %s | %d | %s | %s | %s |',
			$b, $n, ms_str($ns), ns_per_event($ns, $n),
			defined $pp ? sprintf('%.3f', $pp) : 'n/a'));
	}
	rep('', sprintf('decision-bucket pp sum: %.3f pp of the measured tax '
			  . '(unattributed remainder is outside these buckets)',
			$pp_sum),
		'');
}

# Table 2: raw only.  These buckets accumulate concurrently across
# processes (service/nested/overlay) -- NEVER folded into pp.
sub report_service_table
{
	my ($w) = @_;
	rep('## Table 2 -- service/nested buckets (raw only, NEVER folded to pp)',
		'');
	rep('| bucket | role | n0 events | n1 events | n0 ms | n1 ms | ns/event |',
		'|---|---|---|---|---|---|---|');
	for my $b (@ALL_BUCKETS)
	{
		next if $IS_DECISION{$b};
		my ($n0, $ns0) = (bkt_n($w->{d0}, $b), bkt_ns($w->{d0}, $b));
		my ($n1, $ns1) = (bkt_n($w->{d1}, $b), bkt_ns($w->{d1}, $b));
		rep(sprintf('| %s | %s | %d | %d | %s | %s | %s |',
			$b, $SERVICE_NOTE{$b} // 'service-side',
			$n0, $n1, ms_str($ns0), ms_str($ns1),
			ns_per_event($ns0 + $ns1, $n0 + $n1)));
	}
	my $hw = sub { ($w->{d0}{$_[0]} // 0) + ($w->{d1}{$_[0]} // 0) };
	rep('',
		'HW-extend master locality during W (both nodes): local='
		  . $hw->('hw_extend_local_count')
		  . ' remote=' . $hw->('hw_extend_remote_count'),
		'');
}

sub reship_rate_line
{
	my ($label, $ph) = @_;
	my $total = $ph->{reship} + $ph->{sholder};
	my $per_read = $ph->{rounds} > 0
	  ? sprintf('%.2f', $ph->{reship} / $ph->{rounds}) : 'n/a';
	my $ratio = $total > 0
	  ? sprintf('%.1f%%', 100.0 * $ph->{reship} / $total) : 'n/a';
	return sprintf('| %s | %d | %d | %d | %d | %s | %s |%s',
		$label, $ph->{rounds}, $ph->{errors}, $ph->{reship}, $ph->{sholder},
		$per_read, $ratio,
		$ph->{reset_stable} ? '' : ' (RESET mid-phase, unreliable)');
}

sub report_read_section
{
	my ($r) = @_;
	rep('## Read axis -- node1 cross-node reads + amortization probe '
		  . '(spec §3.6)',
		'');
	rep('| phase | read rounds | errors | reship | sholder_hit | '
		  . 'reship/read | reship ratio |',
		'|---|---|---|---|---|---|---|',
		reship_rate_line('pre-VACUUM',  $r->{pre}),
		reship_rate_line('post-VACUUM', $r->{post}),
		'');
	my ($pre, $post) = ($r->{pre}, $r->{post});
	my $rate =
	  sub { $_[0]{rounds} > 0 ? $_[0]{reship} / $_[0]{rounds} : 0 };
	# A phase with errored reads, or one whose reads produced ZERO
	# cross-node signal (reship + sholder both 0 over > 0 rounds), cannot
	# support an amortization verdict.
	my $no_signal = sub {
		$_[0]{rounds} > 0 && $_[0]{reship} == 0 && $_[0]{sholder} == 0;
	};
	my $verdict;
	if (   $pre->{errors} > 0
		|| $post->{errors} > 0
		|| $no_signal->($pre)
		|| $no_signal->($post))
	{
		$verdict = sprintf(
			'invalid (reads errored): pre errors=%d post errors=%d '
			  . 'pre reship+sholder=%d post reship+sholder=%d',
			$pre->{errors}, $post->{errors},
			$pre->{reship} + $pre->{sholder},
			$post->{reship} + $post->{sholder});
	}
	elsif ($rate->($post) < 0.5 * $rate->($pre))
	{
		$verdict = 'reship rate DROPPED after VACUUM (amortization observed)';
	}
	else
	{
		$verdict = 'no amortization: reship rate did not drop after VACUUM '
		  . '(expected currently: ~1 reship/read, zero amortization -- '
		  . 'spec §3.6)';
	}
	rep("verdict: $verdict");
	for my $ph (['pre-VACUUM', $pre], ['post-VACUUM', $post])
	{
		rep(sprintf('- %s first error: %s', $ph->[0], $ph->[1]{first_err}))
		  if $ph->[1]{errors} > 0 && defined $ph->[1]{first_err};
	}
	rep('');
	for my $b (qw(r_gcs_s_request r_cr_construct r_cr_chain_walk
		r_tt_visibility_resolve))
	{
		my ($n, $ns) = (bkt_n($pre->{delta}, $b), bkt_ns($pre->{delta}, $b));
		rep(sprintf('- pre-VACUUM %s: n=%d total=%sms ns/event=%s',
			$b, $n, ms_str($ns), ns_per_event($ns, $n)));
	}
	rep('',
		sprintf('S-cache ceiling estimate (if reship -> 0): saved = %d '
			  . 'reships x %s ns avg r_gcs_s_request = %s of the read wall',
			$pre->{reship},
			defined $r->{avg_s_ns} ? sprintf('%.0f', $r->{avg_s_ns}) : 'n/a',
			defined $r->{ceiling_pct}
			  ? sprintf('%.2f%%', $r->{ceiling_pct}) : 'n/a'),
		'');
}

# Shared shape for the index + commit axis sections: one lead line, then
# per-bucket n / total_ms / ns-per-event lines summed over both nodes.
sub report_axis_buckets
{
	my ($title, $lead, $res, @buckets) = @_;
	rep("## $title", '');
	rep($lead
		  . ($res->{reset_stable} ? '' : ' (RESET mid-scenario, unreliable)'));
	for my $b (@buckets)
	{
		my $n  = bkt_n($res->{d0}, $b) + bkt_n($res->{d1}, $b);
		my $ns = bkt_ns($res->{d0}, $b) + bkt_ns($res->{d1}, $b);
		rep(sprintf('- %s: n=%d (n0=%d n1=%d) total=%sms ns/event=%s',
			$b, $n, bkt_n($res->{d0}, $b), bkt_n($res->{d1}, $b),
			ms_str($ns), ns_per_event($ns, $n)));
	}
	rep('');
}

sub report_observe_section
{
	my ($o) = @_;
	rep('## Observe-only legs (observations, never failures)', '');
	rep(sprintf('- write collapse (-c 2, %ds): tps=%s, error-ish lines '
			  . 'in pgbench output=%d',
			$OBS_SECS,
			defined $o->{collapse_tps} ? sprintf('%.0f', $o->{collapse_tps})
			: 'n/a (run failed -- observation)',
			$o->{collapse_errs}),
		$o->{hot_seeded}
		? sprintf('- alternating same-rows updates: %d of %d errored '
			  . '(fail-closed retries are expected behaviour, recorded '
			  . 'not judged)',
			$o->{hot_errors}, $o->{hot_attempts})
		: '- alternating same-rows updates: NO SIGNAL (seeding failed; '
		  . 'updates ran against an empty table and succeed trivially)',
		'');
}

# Per-axis consumability.  REPORT-ONLY and informational -- this section
# never fails the run; it exists so a downstream reader (e.g. a lever spec
# consuming these numbers) cannot over-read a partially-failed artifact.
sub report_validity_section
{
	my ($w, $r, $i, $c, $o) = @_;
	my $phase_ok = sub {
		my ($ph) = @_;
		return 0 unless $ph->{rounds} > 0 && $ph->{errors} == 0;
		return ($ph->{reship} + $ph->{sholder}) > 0 ? 1 : 0;
	};
	my ($pre_ok, $post_ok) =
	  ($phase_ok->($r->{pre}), $phase_ok->($r->{post}));
	my $row = sub { rep(sprintf('| %s | %s | %s |', @_)) };
	rep('## Artifact validity (per-axis consumability; informational, '
		  . 'never a gate)',
		'',
		'| axis | consumable | why |',
		'|---|---|---|');
	$row->(
		'W pair tax + Table 1 pp folding',
		defined $w->{tax_pair_pct} ? 'yes' : 'NO',
		defined $w->{tax_pair_pct}
		? sprintf('pair tps measured at pgbench scale %s', $PAIR_SCALE_USED)
		: 'pair pgbench init failed at every scale; tax/pp = n/a');
	$row->(
		'R pre-VACUUM phase',
		$pre_ok ? 'yes' : 'NO',
		sprintf('%d rounds, %d errors, reship+sholder=%d',
			$r->{pre}{rounds}, $r->{pre}{errors},
			$r->{pre}{reship} + $r->{pre}{sholder}));
	$row->(
		'R post-VACUUM phase',
		$post_ok ? 'yes' : 'NO',
		sprintf('%d rounds, %d errors, reship+sholder=%d',
			$r->{post}{rounds}, $r->{post}{errors},
			$r->{post}{reship} + $r->{post}{sholder}));
	$row->(
		'R pre/post amortization verdict + S-cache ceiling',
		($pre_ok && $post_ok) ? 'yes' : 'NO',
		'needs BOTH read phases valid');
	$row->(
		'I index axis',
		($i->{batches} > 0 && $i->{errors} == 0) ? 'yes' : 'NO',
		sprintf('%d batches, %d errors', $i->{batches}, $i->{errors}));
	$row->(
		'C commit axis',
		$c->{errors} == 0 ? 'yes' : 'trend-only',
		sprintf('%d errors of %d txns%s', $c->{errors}, $c->{txns},
			$c->{errors} > 0
			? ' (errored loop: per-event costs stay indicative, '
			  . 'exclusion-grade verdicts do not)'
			: ''));
	$row->(
		'O2 same-rows observe leg',
		$o->{hot_seeded} ? 'yes' : 'NO',
		$o->{hot_seeded}
		? 'seeded; error count is a real conflict observation'
		: 'seeding failed; empty-table updates succeed trivially');
	rep('');
}

sub write_report
{
	make_path(dirname($XP_OUT));
	open(my $fh, '>', $XP_OUT) or die "cannot write $XP_OUT: $!\n";
	print $fh map { "$_\n" } @REPORT;
	close $fh;
	# Echo the whole report to the invoking terminal + the regress log.
	print $REAL_STDOUT map { "$_\n" } @REPORT;
	print map { "$_\n" } @REPORT;
	progress("report written: $XP_OUT");
}

# ---- main ----
progress('spec-5.59 D8 four-axis cross-node profile harness (REPORT-ONLY)');
progress("repo=$REPO_ROOT out=$XP_OUT pgbench=$PGBENCH");

my $native = boot_native();
my $solo   = boot_solo();
my $pair   = boot_pair();
setup_phantom_tables($pair);
ok(1, 'harness: native + solo + 2-node pair booted, pair in quorum, '
	  . 'phantom tables coincident');

pgbench_init($native) or die "harness failure: pgbench init (native)\n";
pgbench_init($solo)   or die "harness failure: pgbench init (solo)\n";
ok(1, "harness: pgbench -i (native/solo scale $XP_SCALE)");

# Axis order matters on a slow local machine: the pair-side pgbench
# bulk COPY is storm-prone and can wound the pair's GCS state, so the
# small-workload axes (R/I/C/observe) run FIRST on a healthy pair and
# the pgbench-based W axis (with its degrading init chain) runs LAST.
my $r = axis_read($pair);
ok(1, 'R axis measured (pre/post-VACUUM amortization probe)');

my $i = axis_index($pair);
ok(1, 'I axis measured');

my $c = axis_commit($pair);
ok(1, 'C axis measured');

my $o = observe_hot($pair);
ok(1, 'observe-only O2 recorded');

$PAIR_SCALE_USED = pgbench_init_pair_degrading($pair->node0);
progress($PAIR_SCALE_USED
	? "pair pgbench init succeeded at scale $PAIR_SCALE_USED"
	: 'pair pgbench init FAILED at every scale; pair W-axis TPS will be n/a');

my $o1 = observe_collapse($pair);
$o->{$_} = $o1->{$_} for keys %$o1;
ok(1, 'observe-only O1 recorded');

my $w = axis_write($native, $solo, $pair, $PAIR_SCALE_USED);
ok(1, 'W axis measured');

# native + solo are only needed for the interleaved W baseline.
$native->stop;
$solo->stop;

$pair->stop_pair if $pair->can('stop_pair');

report_header();
report_three_tier($w);
report_decision_table($w);
report_service_table($w);
report_read_section($r);
report_axis_buckets(
	'Index axis -- node0-only right-growing PK index; its blocks '
	  . 'hash-master ~50% onto node1, so node0\'s own index maintenance '
	  . 'IS the cross-node traffic',
	sprintf('- %d insert batches (200 rows each, node0-only), errors=%d',
		$i->{batches}, $i->{errors}),
	$i, qw(i_index_block_xfer i_rightmost_leaf_ping));
report_axis_buckets(
	'Commit axis -- tiny single-row commits from node0',
	sprintf('- %d committed txns in %.1fs, errors=%d%s',
		$c->{txns}, $c->{wall_ns} / 1e9, $c->{errors},
		($c->{errors} > 0 && defined $c->{first_err})
		? " (first error: $c->{first_err})" : ''),
	$c, qw(c_scn_commit_advance c_scn_boc_broadcast));
report_observe_section($o);
report_validity_section($w, $r, $i, $c, $o);
write_report();
write_tsv($w, $r, $i, $c); # spec-6.12 D0: machine-readable value-gate feed
ok(1, "report emitted: $XP_OUT");

done_testing();
exit 0;
