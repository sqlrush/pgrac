
#-------------------------------------------------------------------------
#
# HangChaos.pm -- spec-5.20 D1: Hang Manager chaos injection harness.
#
# Faithful hang injection toolbox + seeded chaos campaign driver + hang
# category counter-delta collection, shared by the Hang Manager chaos
# acceptance TAP legs:
#   t/330 detection matrix (HG#1 + HG#2 detection side)
#   t/331 remediation chaos (HG#3)
#   t/332 diagnostic completeness (HG#4)
#   t/333 reconfig non-interference (HG#5)
#
# A HangChaos object wraps ONE PostgreSQL::Test::Cluster node and tracks every
# background psql handle it spawns, so a single ->cleanup() reliably tears the
# scenario down (eval-wrapped quits + external terminate of leftovers -- a
# still-blocked psql quit can die on some platforms, and must never fail the
# test).  Cross-node injection (ges_cross_node_hang) takes the whole cluster
# (ClusterPair/Triple/Quad) and drives two per-node HangChaos handles.
#
# Injection honesty (spec §1.4.5 / L341): the injectors here are all FAITHFUL
# real-path scenarios driven through SQL (idle-in-tx LOCK TABLE holders, real
# blocking chains / convoys, real CPU-bound active queries, real 2PC-prepared
# holders, real >cap sample floods, real cross-node GES waits).  The v1-
# unreachable cells (IO-stall APPROXIMATE verdict, COMPLETE+in-WFG over-exclude,
# in_confirmed_deadlock=true, SKIP_2PC pure-policy) are covered by the C unit
# truth tables (test_cluster_hang / test_cluster_hang_resolve), NOT faked here.
#
# Spec authority: pgrac:specs/spec-5.20-hang-manager-acceptance.md §1.2 D1.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::HangChaos;

use strict;
use warnings;

use Test::More;
use Time::HiRes qw(usleep);

# A GUC block that makes the Hang Manager sample + resolve fast enough for an
# e2e test while still exercising the real path (real threshold cross, real
# confirm rounds, real soft-timeout escalation).  Callers append this to the
# node's postgresql.conf before start; mode defaults to advisory (dry-run) and
# individual tests flip it to enforce as needed.
our $FAST_HANG_CONF = <<'EOC';
# spec-5.20 chaos-acceptance fast-hang knobs (real path, low latency).
cluster.diag_main_loop_interval = 200
cluster.hang_manager_enabled = on
cluster.hang_sample_interval_ms = 200
cluster.hang_threshold_ms = 1500
cluster.hang_dump_enabled = on
cluster.hang_resolution_mode = advisory
cluster.hang_resolution_confirm_rounds = 1
cluster.hang_resolution_soft_timeout_ms = 1000
cluster.hang_resolution_max_per_round = 1
# deadlock.c must break a real deadlock well before the hang threshold so a
# deadlock waiter is never seen as a COMPLETE actionable long-wait.
deadlock_timeout = 500
# warning level keeps LOG-level cluster messages AND ERROR-level "deadlock
# detected" that the reconfig / deadlock legs check.
log_min_messages = warning
max_prepared_transactions = 8
# Hang storm (HT12) parks > hang_max_sampled (64) simultaneous waiters on one
# lock; give enough backend slots for the storm + the test's own sessions.
max_connections = 150
EOC


#-----------------------------------------------------------------------
# new($class, $node)
#
#	Wrap one PostgreSQL::Test::Cluster node.  Does not start it.
#-----------------------------------------------------------------------
sub new
{
	my ($class, $node) = @_;
	return bless {
		node    => $node,
		handles => [],		# tracked background_psql handles (for cleanup)
		leftover_like => {},	# query LIKE patterns to terminate on cleanup
		nseq    => 0,		# monotone table-name sequence (deterministic)
	}, $class;
}

sub node { return $_[0]->{node}; }

# Append the fast-hang GUC block (call before ->start).
sub apply_fast_conf
{
	my ($self, @extra) = @_;
	$self->{node}->append_conf('postgresql.conf', $FAST_HANG_CONF);
	$self->{node}->append_conf('postgresql.conf', "$_\n") for @extra;
	return;
}


# ----------------------------------------------------------------------
# Observability: hang dump category counters.
# ----------------------------------------------------------------------

# A single `hang` category value (string; '' if absent).
sub hang_val
{
	my ($self, $key) = @_;
	my $v = $self->{node}->safe_psql('postgres',
		"SELECT value FROM pg_cluster_state WHERE category='hang' AND key='$key'");
	return defined $v ? $v : '';
}

# Numeric hang counter (0 if absent / non-numeric).
sub hang_num
{
	my ($self, $key) = @_;
	my $v = $self->hang_val($key);
	return ($v =~ /^-?\d+$/) ? $v + 0 : 0;
}

# Snapshot every hang counter as a hashref (delta baseline).
sub counters
{
	my ($self) = @_;
	# Exclude only the per-sample rows (hang_sample<digit>_...), NOT the fixed
	# keys that merely start with hang_sample (hang_sample_interval_ms /
	# hang_samples_taken / hang_sample_epoch).
	my $rows = $self->{node}->safe_psql('postgres',
		"SELECT key||'='||value FROM pg_cluster_state WHERE category='hang' "
		. "AND key !~ '^hang_sample[0-9]'");
	my %h;
	for my $line (split /\n/, ($rows // ''))
	{
		my ($k, $v) = split /=/, $line, 2;
		$h{$k} = ($v =~ /^-?\d+$/) ? $v + 0 : $v;
	}
	return \%h;
}

# Delta of one counter vs a baseline snapshot.
sub delta
{
	my ($self, $base, $key) = @_;
	my $now = $self->hang_num($key);
	my $was = ($base->{$key} && $base->{$key} =~ /^-?\d+$/) ? $base->{$key} + 0 : 0;
	return $now - $was;
}


# ----------------------------------------------------------------------
# Internal helpers.
# ----------------------------------------------------------------------

sub _next_tbl
{
	my ($self, $stem) = @_;
	$self->{nseq}++;
	return sprintf('hc_%s_%d', $stem, $self->{nseq});
}

# Spawn a tracked background psql handle.
sub _bg
{
	my ($self) = @_;
	my $h = $self->{node}->background_psql('postgres', on_error_die => 0);
	push @{ $self->{handles} }, $h;
	return $h;
}

# Fire a query that is expected to BLOCK, without waiting for it to return.
sub _fire_blocking
{
	my ($self, $h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
	return;
}

sub _create_table
{
	my ($self, $tbl) = @_;
	$self->{node}->safe_psql('postgres', "CREATE TABLE IF NOT EXISTS $tbl (i int)");
	$self->{leftover_like}{"%$tbl%"} = 1;
	return;
}


# ----------------------------------------------------------------------
# Faithful injectors.  Each returns pids / structure the caller asserts on.
# ----------------------------------------------------------------------

# HT1 -- idle-in-tx blocker: BEGIN; LOCK TABLE t AEL (assigns NO xid); idle.
# Returns { pid, table, holder }.
sub idle_in_tx_blocker
{
	my ($self, $tbl) = @_;
	$tbl //= $self->_next_tbl('idle');
	$self->_create_table($tbl);
	my $h = $self->_bg;
	$h->query_safe('BEGIN');
	$h->query_safe("LOCK TABLE $tbl IN ACCESS EXCLUSIVE MODE");
	my $pid = $h->query_safe('SELECT pg_backend_pid()');
	return { pid => $pid, table => $tbl, holder => $h };
}

# A waiter blocked on the AEL of $tbl.  Returns { pid, waiter }.
sub waiter_on
{
	my ($self, $tbl) = @_;
	my $h = $self->_bg;
	my $pid = $h->query_safe('SELECT pg_backend_pid()');
	$self->_fire_blocking($h, "BEGIN; LOCK TABLE $tbl IN ACCESS EXCLUSIVE MODE");
	return { pid => $pid, waiter => $h };
}

# HT2 -- long blocking chain R <- M1 <- ... of depth $depth (>=2).  R is the
# idle-in-tx root holder; each Mi holds its own table then blocks on the prior.
# Returns { root_pid, mid_pids => [...], waiter_pid, tables => [...] }.
sub blocking_chain
{
	my ($self, $depth) = @_;
	$depth //= 3;
	$depth = 2 if $depth < 2;

	my @tables = map { $self->_next_tbl("chain${_}") } (0 .. $depth - 1);
	$self->_create_table($_) for @tables;

	# Root: idle-in-tx holder of tables[0].
	my $root = $self->idle_in_tx_blocker($tables[0]);

	# Middle links: each holds tables[i] then blocks waiting on tables[i-1].
	my @mid_pids;
	for my $i (1 .. $depth - 2)
	{
		my $h = $self->_bg;
		$h->query_safe('BEGIN');
		$h->query_safe("LOCK TABLE $tables[$i] IN ACCESS EXCLUSIVE MODE");
		my $pid = $h->query_safe('SELECT pg_backend_pid()');
		$self->_fire_blocking($h, "LOCK TABLE $tables[$i-1] IN ACCESS EXCLUSIVE MODE");
		push @mid_pids, $pid;
	}

	# Deepest waiter: blocks on the last middle link's table.
	my $w = $self->waiter_on($tables[$depth - 2]);

	return {
		root_pid   => $root->{pid},
		mid_pids   => \@mid_pids,
		waiter_pid => $w->{pid},
		tables     => \@tables,
	};
}

# HT3 -- lock convoy: one idle-in-tx root holder + $n waiters on one table.
# Returns { root_pid, waiter_pids => [...], table }.
sub lock_convoy
{
	my ($self, $n) = @_;
	$n //= 4;
	my $root = $self->idle_in_tx_blocker($self->_next_tbl('convoy'));
	my @wpids;
	for (1 .. $n)
	{
		my $w = $self->waiter_on($root->{table});
		push @wpids, $w->{pid};
	}
	return { root_pid => $root->{pid}, waiter_pids => \@wpids, table => $root->{table} };
}

# HT7 -- healthy-but-slow ACTIVE query (CPU-bound, NOT a wait).  Must never be
# sampled as a long-wait.  Returns { pid }.  The query runs a large in-memory
# aggregate so it stays 'active' with no wait_event for $secs-ish.
sub healthy_slow_query
{
	my ($self, $rows) = @_;
	$rows //= 80_000_000;
	my $h = $self->_bg;
	my $pid = $h->query_safe('SELECT pg_backend_pid()');
	# Fire-and-don't-wait: a CPU-bound aggregate (no locks, no waits).
	$h->query_until(qr/PGRAC_FIRED/,
		"\\echo PGRAC_FIRED\nSELECT count(*) FROM generate_series(1, $rows) g WHERE (g % 7) = 0;\n");
	return { pid => $pid, worker => $h };
}

# HT11 -- 2PC-prepared holder: PREPARE TRANSACTION leaves the AEL held by NO
# live backend.  Returns { table, gid }.  Pair with waiter_on($table).
sub twopc_holder
{
	my ($self, $gid) = @_;
	$gid //= 'hc_2pc_' . (++$self->{nseq});
	my $tbl = $self->_next_tbl('twopc');
	$self->_create_table($tbl);
	$self->{node}->safe_psql('postgres', qq{
		BEGIN;
		LOCK TABLE $tbl IN ACCESS EXCLUSIVE MODE;
		PREPARE TRANSACTION '$gid';
	});
	$self->{prepared}{$gid} = 1;
	return { table => $tbl, gid => $gid };
}

# HT12 -- hang storm: ONE idle-in-tx root holding a single table, blocking $n
# waiters, so the sample store (cap 64) overflows and reports truncated=true.
# Uses 1 + $n connections (convoy-shaped) to stay under max_connections while
# still producing > cap simultaneous long-waits.  Returns { root_pid,
# waiter_pids }.  Use $n > cluster.hang_max_sampled (default 64).
sub hang_storm
{
	my ($self, $n) = @_;
	$n //= 70;
	my $root = $self->idle_in_tx_blocker($self->_next_tbl('storm'));
	my @waiters;
	for (1 .. $n)
	{
		my $w = $self->waiter_on($root->{table});
		push @waiters, $w->{pid};
	}
	return { root_pid => $root->{pid}, waiter_pids => \@waiters };
}

# HT9 -- ABA: an idle-in-tx blocker that will self-resolve (ROLLBACK) after the
# waiter has been sampled, so a late disposition round must re-validate and NOT
# kill the reused slot.  Returns the blocker struct; caller calls
# ->release_holder($struct) at the ABA moment.
sub aba_blocker
{
	my ($self, $tbl) = @_;
	my $b = $self->idle_in_tx_blocker($tbl);
	return $b;
}

sub release_holder
{
	my ($self, $b) = @_;
	eval { $b->{holder}->query_safe('ROLLBACK'); };
	return;
}

# HT10 (best-effort faithful): a system backend (autovacuum) holding a lock is
# not deterministically inducible from SQL, so the SKIP_SYSTEM / no_safe_victim
# behaviour is proven by the C policy unit (test_cluster_hang_resolve).  This
# helper documents that boundary for callers that want to assert honestly.
sub hard_skip_note
{
	return 'HT10 hard-skip (SKIP_SYSTEM/no_safe_victim) is covered by the '
		. 'test_cluster_hang_resolve policy unit; a live autovacuum lock holder '
		. 'is not deterministically inducible from a TAP session (spec §1.4.5).';
}


# ----------------------------------------------------------------------
# Cross-node (HT5/HT6) note: there is deliberately NO faithful cross-node-hang
# injector here.  A sustained cross-node ACTIONABLE hang is not deterministically
# inducible in pgrac: cross-node ROW conflicts fail-closed (53R98, spec-3.4d /
# t/209) instead of blocking, and cross-node BLOCK conflicts make progress via
# Cache Fusion.  The genuine REMOTE_BOUNDARY case only arises transiently during
# reconfig instability (a survivor waiting on a GES reply from a dead node),
# which t/343 exercises for NON-INTERFERENCE + honesty (the local manager never
# disposes a non-local waiter).  The REMOTE_BOUNDARY *classification* itself
# (cross-node blocker -> remote_boundary -> non-actionable) is faithfully unit-
# tested at the policy level in test_cluster_hang.c (cluster_hang_quality with
# blocker_remote_node >= 0) + test_cluster_hang_acceptance.c L6.  Injecting a
# fake FOR-UPDATE "cross-node hang" would contradict the fail-closed contract, so
# it is not provided (spec §1.4.5 / L341 honesty).
# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
# Seeded chaos campaign: deterministically mix injectors over a window.  The
# seed derives the sequence (no Math::random) so re-runs are identical.  @mix
# is a list of injector-name strings drawn round-robin, perturbed by the seed.
# Returns a summary { injected => { kind => count }, roots => [...] }.
# ----------------------------------------------------------------------
sub run_chaos_campaign
{
	my ($self, $seed, $steps, $mix) = @_;
	$seed  //= 1;
	$steps //= 6;
	$mix   //= [qw(idle chain convoy healthy)];

	my %injected;
	my @roots;
	for my $step (0 .. $steps - 1)
	{
		# Deterministic pick: seed + step, no RNG.
		my $kind = $mix->[($seed + $step) % scalar(@$mix)];
		$injected{$kind}++;
		if ($kind eq 'idle')
		{
			my $r = $self->idle_in_tx_blocker;
			$self->waiter_on($r->{table});
			push @roots, $r->{pid};
		}
		elsif ($kind eq 'chain')
		{
			my $c = $self->blocking_chain(3);
			push @roots, $c->{root_pid};
		}
		elsif ($kind eq 'convoy')
		{
			my $c = $self->lock_convoy(3);
			push @roots, $c->{root_pid};
		}
		elsif ($kind eq 'healthy')
		{
			$self->healthy_slow_query(40_000_000);
		}
		usleep(150_000);
	}
	return { injected => \%injected, roots => \@roots };
}


# ----------------------------------------------------------------------
# Wait helpers (mirror t/305).
# ----------------------------------------------------------------------

sub wait_for_lock_wait
{
	my ($self, $qlike, $secs) = @_;
	$secs //= 20;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $self->{node}->safe_psql('postgres', qq{
			SELECT coalesce(wait_event_type,'') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq 'Lock';
		usleep(200_000);
	}
	return 0;
}

sub wait_for_victim
{
	my ($self, $pid, $secs) = @_;
	$secs //= 25;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $self->{node}->safe_psql('postgres',
			"SELECT count(*) FROM pg_cluster_hang_victims() WHERE victim_pid = $pid");
		return 1 if defined $n && $n eq '1';
		usleep(200_000);
	}
	return 0;
}

sub wait_for_gone
{
	my ($self, $pid, $secs) = @_;
	$secs //= 40;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $self->{node}->safe_psql('postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE pid = $pid");
		return 1 if defined $n && $n eq '0';
		usleep(200_000);
	}
	return 0;
}

# Poll a hang counter until it exceeds $base (returns 1) or times out (0).
sub wait_for_counter_gt
{
	my ($self, $key, $base, $secs) = @_;
	$secs //= 20;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		return 1 if $self->hang_num($key) > $base;
		usleep(200_000);
	}
	return 0;
}

# Poll until a per-sample row for $pid reports the expected quality (dump str).
# Returns the observed quality string (or '' if never seen).
sub wait_for_sample_quality
{
	my ($self, $pid, $secs) = @_;
	$secs //= 20;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $q = $self->sample_field($pid, 'quality');
		return $q if defined $q && $q ne '';
		usleep(200_000);
	}
	return '';
}

# Read a per-sample field (source/quality/wait_ms/...) for the sample row whose
# _pid matches $pid.  Returns the dump-string value or '' if not sampled.
sub sample_field
{
	my ($self, $pid, $field) = @_;
	# Resolve the index whose _pid == $pid AND read <field> for it in a SINGLE
	# pg_cluster_state snapshot (self-join), so a 200ms sample-epoch rewrite
	# between two queries cannot make us read a different pid's slot.
	my $v = $self->{node}->safe_psql('postgres', qq{
		SELECT f.value
		FROM pg_cluster_state p
		JOIN pg_cluster_state f
		  ON f.category='hang'
		 AND f.key = 'hang_sample'
			|| (regexp_match(p.key, '^hang_sample(\\d+)_pid\$'))[1] || '_$field'
		WHERE p.category='hang'
		  AND p.key ~ '^hang_sample\\d+_pid\$'
		  AND p.value = '$pid'
		LIMIT 1});
	return defined $v ? $v : '';
}


# ----------------------------------------------------------------------
# Assertion helpers (spec §2).
# ----------------------------------------------------------------------

# Assert the sample row for $pid matches expected {source?, quality?,
# actionable?}.  Skips a field if not provided.
sub assert_hang_sample
{
	my ($self, $pid, $exp, $label) = @_;
	my $q = $self->sample_field($pid, 'quality');
	Test::More::isnt($q, '', "$label: pid $pid was sampled");
	if (defined $exp->{quality})
	{
		Test::More::is($q, $exp->{quality}, "$label: quality == $exp->{quality}");
	}
	if (defined $exp->{source})
	{
		my $src = $self->sample_field($pid, 'source');
		Test::More::is($src, $exp->{source}, "$label: source == $exp->{source}");
	}
	if (defined $exp->{actionable})
	{
		# actionable == (quality==complete && in_confirmed_deadlock==false).
		# The dump serialises bools via fmt_bool() -> 't' / 'f' (not true/false).
		my $dl = $self->sample_field($pid, 'in_confirmed_deadlock');
		my $act = ($q eq 'complete' && $dl eq 'f') ? 1 : 0;
		Test::More::is($act, $exp->{actionable} ? 1 : 0,
			"$label: actionable == " . ($exp->{actionable} ? 1 : 0));
	}
	return;
}

# Assert no disposition counter that represents an UNSAFE kill moved vs baseline
# for the given pid set (they must still be alive) -- used by false-positive
# control (HG#2).  Only checks liveness (safe direction).
sub assert_all_alive
{
	my ($self, $pids, $label) = @_;
	for my $pid (@$pids)
	{
		my $n = $self->{node}->safe_psql('postgres',
			"SELECT count(*) FROM pg_stat_activity WHERE pid = $pid");
		Test::More::is($n, '1', "$label: pid $pid still alive (no unsafe disposition)");
	}
	return;
}


# ----------------------------------------------------------------------
# Cleanup: quit tracked handles (eval-wrapped), rollback prepared xacts,
# terminate any leftover backends still holding injected-table locks.
# ----------------------------------------------------------------------
sub cleanup
{
	my ($self) = @_;

	# Roll back any prepared 2PC transactions we created.
	for my $gid (keys %{ $self->{prepared} || {} })
	{
		eval { $self->{node}->safe_psql('postgres', "ROLLBACK PREPARED '$gid'"); };
	}

	# Terminate backends still parked on injected tables (release locks so the
	# tracked psql quits do not hang), then eval-wrap the quits.
	for my $like (keys %{ $self->{leftover_like} })
	{
		eval {
			$self->{node}->safe_psql('postgres',
				"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
				. "WHERE pid <> pg_backend_pid() AND query LIKE '$like'");
		};
	}
	for my $h (@{ $self->{handles} })
	{
		eval { $h->quit; };
	}
	$self->{handles} = [];
	return;
}


1;
