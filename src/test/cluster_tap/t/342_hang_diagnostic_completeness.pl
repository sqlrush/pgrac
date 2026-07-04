#-------------------------------------------------------------------------
#
# 342_hang_diagnostic_completeness.pl
#    spec-5.20 D4 -- Hang Manager diagnostic completeness (HG#4), single node.
#
#    Content-validates the Hang Manager observability surface (not just "the
#    category is present", L373/L223): every fixed key of the `hang` dump
#    category is emitted with a sane value, the per-sample rows carry the full
#    9-field suffix set, the ProcSignal single-backend dump really writes the
#    target's wait identity to the server log (+ counter), the victims SRF
#    visibility matrix and the manual-resolve superuser gate hold, the first-
#    threshold LOG-once is deduped (not per-round spam), and the honest-degrade
#    operator-alert message exists in the disposition path.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/342_hang_diagnostic_completeness.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::HangChaos;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);
use PgracClusterNode;


my $node = PgracClusterNode->new('hangdiag');
$node->init;
my $chaos = PostgreSQL::Test::HangChaos->new($node);
$chaos->apply_fast_conf;
$node->start;


# Set up ONE real idle-in-tx hang so the dump + per-sample rows have content.
my $root = $chaos->idle_in_tx_blocker;
my $w    = $chaos->waiter_on($root->{table});
ok($chaos->wait_for_lock_wait("%$root->{table}%", 20), 'setup: waiter is blocked');
my $q = $chaos->wait_for_sample_quality($w->{pid}, 20);
is($q, 'complete', 'setup: waiter sampled complete (dump has a per-sample row)');


# ======================================================================
# HG#4 (1) -- full fixed key roster content-validate.  Collect every non-sample
# `hang` key into a hash, then assert each expected key is present with a sane
# value.  (A rename at an emit site would make a key missing here.)
# ======================================================================
my %kv;
{
	# Exclude only per-sample rows (hang_sample<digit>_...), keeping the fixed
	# keys hang_sample_interval_ms / hang_samples_taken / hang_sample_epoch.
	my $rows = $node->safe_psql('postgres',
		"SELECT key||'\t'||value FROM pg_cluster_state WHERE category='hang' "
		. "AND key !~ '^hang_sample[0-9]'");
	for my $line (split /\n/, $rows)
	{
		my ($k, $v) = split /\t/, $line, 2;
		$kv{$k} = defined $v ? $v : '';
	}
}

my @bool_keys = qw(hang_manager_enabled hang_dump_enabled hang_available hang_truncated);
my @posint_keys = qw(hang_threshold_ms hang_sample_interval_ms hang_max_sampled);
my @counter_keys = qw(
	hang_long_wait_count hang_longest_wait_us hang_n_samples
	hang_samples_taken hang_long_waits_seen hang_dumps_emitted
	hang_incomplete_sample_count hang_excluded_deadlock_count
	hang_excluded_idle_count hang_excluded_bgworker_count
	hang_proc_signal_dump_count hang_error_count
	hang_deadlock_confirmed_count hang_cycle_detected_count
	hang_resolve_evaluations hang_victims_selected hang_soft_cancels_issued
	hang_terminates_issued hang_resolved_confirmed hang_resolution_failed
	hang_hard_skipped hang_non_actionable_skipped hang_over_excluded
	hang_unprovable_root_skipped hang_aba_revalidate_failed
	hang_not_confirmed_yet hang_no_safe_victim hang_degraded_to_timeout
	hang_advisory_recommendations);
my @str_keys = qw(
	hang_sample_epoch hang_last_sample_at hang_last_dump_emitted_at
	hang_resolution_mode hang_resolve_last_victim_pid hang_resolve_last_action);

for my $k (@bool_keys)
{
	ok(exists $kv{$k}, "dump key present: $k");
	like($kv{$k} // '', qr/^[tf]$/, "dump key $k is a boolean (t/f)");
}
for my $k (@posint_keys)
{
	ok(exists $kv{$k}, "dump key present: $k");
	cmp_ok($kv{$k} // -1, '>', 0, "dump key $k is a positive int");
}
for my $k (@counter_keys)
{
	ok(exists $kv{$k}, "dump key present: $k");
	like($kv{$k} // '', qr/^-?\d+$/, "dump key $k is numeric");
}
for my $k (@str_keys)
{
	ok(exists $kv{$k}, "dump key present: $k");
	isnt($kv{$k} // '', '', "dump key $k has a value");
}
like($kv{hang_resolution_mode}, qr/^(off|advisory|enforce)$/,
	'hang_resolution_mode is a valid mode string');
# 13 config/aggregate + 9 (5.11) + 2 (5.8) + 18 (5.12) == 42 fixed keys.
is(scalar(keys %kv), 42, 'exactly 42 fixed hang dump keys emitted (roster complete, no extras)');


# ======================================================================
# HG#4 (1b) -- per-sample rows carry the full 9-field suffix set with sane
# values for the sampled waiter.
# ======================================================================
{
	my $idx = $node->safe_psql('postgres', qq{
		SELECT (regexp_match(key, '^hang_sample(\\d+)_pid\$'))[1]
		FROM pg_cluster_state
		WHERE category='hang' AND key ~ '^hang_sample\\d+_pid\$' AND value = '$w->{pid}'
		LIMIT 1});
	isnt($idx, '', 'per-sample: found the sampled waiter row index');
	for my $suf (qw(pid wait_event wait_ms duration_kind source quality
		blocker_pid blocker_remote_node in_confirmed_deadlock))
	{
		my $v = $node->safe_psql('postgres',
			"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='hang_sample${idx}_$suf'");
		isnt($v, '', "per-sample: hang_sample${idx}_$suf present");
	}
	is($node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='hang_sample${idx}_source'"),
		'lock', 'per-sample: source == lock');
	is($node->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='hang_sample${idx}_in_confirmed_deadlock'"),
		'f', 'per-sample: in_confirmed_deadlock == f (v1 fail-OPEN)');
}


# ======================================================================
# HG#4 (2) -- ProcSignal single-backend dump: pg_cluster_hang_dump(pid) makes
# the target log its own wait identity + proc_signal_dump_count increments.
# ======================================================================
{
	my $before = $chaos->hang_num('hang_proc_signal_dump_count');
	my $r = $node->safe_psql('postgres', "SELECT pg_cluster_hang_dump($w->{pid})");
	is($r, 't', 'ProcSignal: pg_cluster_hang_dump() returns true (signal delivered)');
	# The target processes the interrupt and logs its wait identity.
	my $seen = 0;
	my $dl = time() + 15;
	while (time() < $dl)
	{
		my $log = slurp_file($node->logfile);
		if ($log =~ /cluster hang dump: pid $w->{pid} wait/) { $seen = 1; last; }
		usleep(200_000);
	}
	ok($seen, 'ProcSignal: target backend logged "cluster hang dump: pid <pid> wait ..."');
	ok($chaos->wait_for_counter_gt('hang_proc_signal_dump_count', $before, 10),
		'ProcSignal: proc_signal_dump_count incremented');
}


# ======================================================================
# HG#4 (3) -- victims SRF visibility matrix.
# ======================================================================
{
	ok($chaos->wait_for_victim($root->{pid}, 25),
		'victims: root recommended as victim (advisory)');
	$node->safe_psql('postgres', 'CREATE ROLE hdu LOGIN');
	is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $root->{pid}"),
		'1', 'victims: superuser sees the victim row');
	is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $root->{pid}",
		extra_params => [ '-U', 'hdu' ]),
		'0', 'victims: unprivileged user sees no foreign victim rows');
	$node->safe_psql('postgres', 'GRANT pg_read_all_stats TO hdu');
	is($node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $root->{pid}",
		extra_params => [ '-U', 'hdu' ]),
		'1', 'victims: pg_read_all_stats member sees the victim row');
}


# ======================================================================
# HG#4 (4) -- manual resolve superuser gate.
# ======================================================================
{
	my ($rc, $stdout, $stderr) = $node->psql('postgres',
		"SELECT pg_cluster_hang_resolve($root->{pid})", extra_params => [ '-U', 'hdu' ]);
	isnt($rc, 0, 'resolve: non-superuser pg_cluster_hang_resolve() fails');
	like($stderr, qr/must be superuser/, 'resolve: perm error mentions superuser');
}


# ======================================================================
# HG#4 (5) -- LOG-once dedup: the first-threshold hang LOG for one waiter is
# admitted once per exact long-wait, not once per sample round (no spam).
# ======================================================================
{
	my $log = slurp_file($node->logfile);
	my @hits = ($log =~ /cluster hang manager: backend pid $w->{pid} waiting/g);
	cmp_ok(scalar(@hits), '>=', 1, 'LOG-once: the first-threshold hang LOG appeared for the waiter');
	# Deduped: even after many sample rounds it must not have logged per-round.
	# (The waiter has been hanging for well over a dozen 200ms rounds by now.)
	cmp_ok(scalar(@hits), '<=', 3,
		'LOG-once: the first-threshold LOG is deduped (not one line per sample round)');
}


# ======================================================================
# HG#4 (6) -- honest-degrade operator alert: the disposition path emits an
# operator-actionable "escalate to external fencer (AD-013)" LOG when a hang
# survives terminate.  The faithful trigger (a hard-skip / undisposable root
# leaving no_safe_victim) is not deterministically inducible from a single-node
# TAP session (spec §1.4.6); assert the alert message exists in the disposition
# source so the observability contract is pinned.
# ======================================================================
{
	my $src = slurp_file("$FindBin::RealBin/../../../backend/cluster/cluster_hang_resolve.c");
	like($src, qr/honest-degrade \(no SIGKILL\).*external fencer \(AD-013\)/s,
		'honest-degrade: operator-alert message (escalate to external fencer) exists in the disposition path');
}


$chaos->cleanup;
$node->stop;
done_testing();
