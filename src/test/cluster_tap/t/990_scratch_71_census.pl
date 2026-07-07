# spec-7.1 D0 scratch census driver (NOT part of the test schedule; run by hand).
#
#   53R97 per-leg census over S3-shaped (100% write) and S5-shaped (99/1)
#   dual-node loads, plus a deterministic multixact cross-read probe.
#   Attribution channels: (a) vis53r97_leg_* / rtvis_* counters via
#   pg_cluster_state, snapshotted per phase per node; (b) client-observed
#   SQLSTATE+message histogram via the census_try_* plpgsql wrappers'
#   RAISE LOG lines ("CENSUS;<W|R>;<sqlstate>;<msg>") grepped per phase
#   from each node's log.
#
# Spec: spec-7.1-cross-instance-positive-interread.md (D0-①)

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;
use IPC::Run;

my $PHASE_SECS = $ENV{CENSUS_SECS} // 60;
my $CLIENTS    = $ENV{CENSUS_CLIENTS} // 4;
my $NROWS      = 500;
my $PHASE      = $ENV{CENSUS_PHASE} // 'ALL';    # S3 | S5 | MULTI | ALL

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

my @KEYS = qw(
  vis53r97_leg_invalid_scn_refuse_count
  vis53r97_leg_zero_match_refuse_count
  vis53r97_leg_srv_other_refuse_count
  vis53r97_leg_covers_refuse_count
  vis53r97_leg_multi_unresolvable_count
  vis53r97_leg_xmax_unprovable_count
  rtvis_underivable_failclosed_count
  rtvis_resolve_committed_count
  rtvis_resolve_aborted_count
  rtvis_resolve_failclosed_count
  rtvis_verdict_wire_count
  rtvis_verdict_failclosed_count
  rtvis_verdict_exact_count
  rtvis_verdict_below_horizon_count
  rtvis_verdict_inadmissible_count
  cr_server_verdict_served_count
  cr_server_verdict_denied_count
  rtvis_undo_fetch_wire_count
  rtvis_undo_fetch_failclosed_count
);

sub snap_counters
{
	my ($node) = @_;
	my %s;
	$s{$_} = state_val($node, 'cr', $_) for @KEYS;
	return \%s;
}

sub diff_counters
{
	my ($before, $after) = @_;
	my %d;
	for my $k (@KEYS)
	{
		my $delta = $after->{$k} - $before->{$k};
		$d{$k} = $delta if $delta != 0;
	}
	return \%d;
}

sub log_offset { my ($node) = @_; return -s $node->logfile || 0; }

sub census_lines
{
	my ($node, $from) = @_;
	open(my $fh, '<', $node->logfile) or die "open log: $!";
	seek($fh, $from, 0);
	my %hist;
	while (my $l = <$fh>)
	{
		if ($l =~ /CENSUS;([WR]);([0-9A-Z]{5});(.*)$/)
		{
			my ($op, $state, $msg) = ($1, $2, $3);
			$msg =~ s/\d+/N/g;          # fold ids
			$msg = substr($msg, 0, 90);
			$hist{"$op;$state;$msg"}++;
		}
	}
	close $fh;
	return \%hist;
}

sub report_phase
{
	my ($tag, $pair, $snap0, $snap1, $off0, $off1) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);
	my $d0 = diff_counters($snap0, snap_counters($node0));
	my $d1 = diff_counters($snap1, snap_counters($node1));
	diag("==== CENSUS PHASE $tag: counter deltas node0 ====");
	diag(sprintf("  %-46s %12d", $_, $d0->{$_})) for sort keys %$d0;
	diag("==== CENSUS PHASE $tag: counter deltas node1 ====");
	diag(sprintf("  %-46s %12d", $_, $d1->{$_})) for sort keys %$d1;
	my $h0 = census_lines($node0, $off0);
	my $h1 = census_lines($node1, $off1);
	diag("==== CENSUS PHASE $tag: client error histogram node0 ====");
	diag(sprintf("  %6d  %s", $h0->{$_}, $_)) for sort { $h0->{$b} <=> $h0->{$a} } keys %$h0;
	diag("==== CENSUS PHASE $tag: client error histogram node1 ====");
	diag(sprintf("  %6d  %s", $h1->{$_}, $_)) for sort { $h1->{$b} <=> $h1->{$a} } keys %$h1;
}

# ============================================================
# Setup: t/346-shaped pair + census schema (mirrored DDL, OID-aligned).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'c71census',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'log_min_messages = log',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

sub nodes_alive
{
	my ($tag) = @_;
	my $dead = 0;
	for my $n ($node0, $node1)
	{
		my $ok = eval {
			my ($rc, $o, $e) = $n->psql('postgres', 'SELECT 1', timeout => 10);
			$rc == 0;
		};
		if (!$ok)
		{
			$dead = 1;
			diag("CRASH at $tag: node " . $n->name . " unreachable; crash signature:");
			my $lf = $n->logfile;
			my @tail = split(/\n/,
				`grep -E 'TRAP|PANIC|was terminated by signal' $lf | tail -4`);
			diag("  $_") for @tail;
		}
	}
	return !$dead;
}

my $fn_w = q{CREATE FUNCTION census_try_write(p int) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  UPDATE census_acc SET v = v + 1 WHERE aid = p;
  RETURN 'OK';
EXCEPTION WHEN OTHERS THEN
  RAISE LOG 'CENSUS;W;%;%', SQLSTATE, SQLERRM;
  RETURN SQLSTATE;
END $$};
my $fn_r = q{CREATE FUNCTION census_try_read(p int) RETURNS text LANGUAGE plpgsql AS $$
DECLARE x int;
BEGIN
  SELECT v INTO x FROM census_acc WHERE aid = p;
  RETURN 'OK';
EXCEPTION WHEN OTHERS THEN
  RAISE LOG 'CENSUS;R;%;%', SQLSTATE, SQLERRM;
  RETURN SQLSTATE;
END $$};

# Mirrored DDL in identical order; then verify the data table coincides,
# lo_create-aligning the lagging node if the counters diverged (t/347 recipe).
sub mirrored_coincident_create
{
	my ($name, $ddl) = @_;
	my ($q0, $q1) = ('', '');
	for my $attempt (1 .. 8)
	{
		return 0 unless write_retry($node0, $ddl);
		return 0 unless write_retry($node1, $ddl);
		$q0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$name')});
		$q1 = $node1->safe_psql('postgres', qq{SELECT pg_relation_filepath('$name')});
		return 1 if $q0 eq $q1;
		my ($n0) = $q0 =~ /(\d+)$/;
		my ($n1) = $q1 =~ /(\d+)$/;
		my ($lag, $burn) = $n0 < $n1 ? ($node0, $n1 - $n0) : ($node1, $n0 - $n1);
		return 0 unless write_retry($lag,
			"SELECT lo_unlink(lo_create(0)) FROM generate_series(1, $burn)");
		return 0 unless write_retry($node0, "DROP TABLE $name");
		return 0 unless write_retry($node1, "DROP TABLE $name");
	}
	return 0;
}
ok(mirrored_coincident_create('census_acc', 'CREATE TABLE census_acc (aid int, v int)'),
	'census_acc relfilepath coincidence holds');
$node0->safe_psql('postgres', $fn_w);
$node0->safe_psql('postgres', $fn_r);
$node1->safe_psql('postgres', $fn_w);
$node1->safe_psql('postgres', $fn_r);

ok(write_retry($node0, "INSERT INTO census_acc SELECT g, 0 FROM generate_series(1, $NROWS) g"),
	'seeded');
ok(write_retry($node0, 'CHECKPOINT'), 'checkpoint after seed');

# pgbench script files
my $dir = PostgreSQL::Test::Utils::tempdir();
sub write_file_local
{
	my ($p, $c) = @_;
	open(my $fh, '>', $p) or die $!;
	print $fh $c;
	close $fh;
}
# Plain SQL, no plpgsql EXCEPTION wrappers: a subxact-per-call under the
# cluster error storm trips pre-existing subxact bookkeeping asserts
# (pgstat_relation.c:574 / trigger.c:5434 — registered census findings).
# Errors abort pgbench clients instead; the loop below relaunches pgbench
# until the phase wall-time elapses and histograms the client-side ERROR
# lines — which is exactly the 7.0 per-transaction error accounting shape.
write_file_local("$dir/s3.sql",
	"\\set aid random(1, $NROWS)\nUPDATE census_acc SET v = v + 1 WHERE aid = :aid;\n");
write_file_local("$dir/s5r.sql",
	"\\set aid random(1, $NROWS)\nSELECT v FROM census_acc WHERE aid = :aid;\n");
write_file_local("$dir/s5w.sql",
	"\\set aid random(1, $NROWS)\nUPDATE census_acc SET v = v + 1 WHERE aid = :aid;\n");

sub run_load
{
	my ($tag, @bench_args) = @_;
	my ($s0, $s1) = (snap_counters($node0), snap_counters($node1));
	my ($o0, $o1) = (log_offset($node0), log_offset($node1));
	my $deadline = time() + $PHASE_SECS;
	my (%errhist0, %errhist1);
	my ($ntx0, $ntx1) = (0, 0);
	my $alive = 1;

	while (time() < $deadline)
	{
		my $remain = $deadline - time();
		last if $remain < 2;
		my @cmd0 = ('pgbench', '-n', '-c', $CLIENTS, '-j', 2, '-T', $remain,
			'-h', $node0->host, '-p', $node0->port, @bench_args, 'postgres');
		my @cmd1 = ('pgbench', '-n', '-c', $CLIENTS, '-j', 2, '-T', $remain,
			'-h', $node1->host, '-p', $node1->port, @bench_args, 'postgres');
		my ($out0, $err0, $out1, $err1) = ('', '', '', '');
		my $h0 = IPC::Run::start(\@cmd0, '>', \$out0, '2>', \$err0);
		my $h1 = IPC::Run::start(\@cmd1, '>', \$out1, '2>', \$err1);
		eval { $h0->finish };
		eval { $h1->finish };
		$ntx0 += $1 if $out0 =~ /actually processed: (\d+)/;
		$ntx1 += $1 if $out1 =~ /actually processed: (\d+)/;
		for (split /\n/, $err0)
		{
			if (/ERROR:\s+(.*)/) { my $m = $1; $m =~ s/\d+/N/g; $errhist0{substr($m, 0, 90)}++; }
		}
		for (split /\n/, $err1)
		{
			if (/ERROR:\s+(.*)/) { my $m = $1; $m =~ s/\d+/N/g; $errhist1{substr($m, 0, 90)}++; }
		}
		if (!nodes_alive("load-$tag-loop")) { $alive = 0; last; }
	}

	diag("==== CENSUS PHASE $tag: node0 committed_tx=$ntx0 / client ERROR histogram ====");
	diag(sprintf("  %6d  %s", $errhist0{$_}, $_))
	  for sort { $errhist0{$b} <=> $errhist0{$a} } keys %errhist0;
	diag("==== CENSUS PHASE $tag: node1 committed_tx=$ntx1 / client ERROR histogram ====");
	diag(sprintf("  %6d  %s", $errhist1{$_}, $_))
	  for sort { $errhist1{$b} <=> $errhist1{$a} } keys %errhist1;
	if (!$alive)
	{
		# A node died under load: the client histograms above are the census
		# data; counters are not reachable.  End cleanly with what we have.
		ok(1, "phase $tag ended in a node crash (signature above) -- census data recorded");
		done_testing();
		exit 0;
	}
	report_phase($tag, $pair, $s0, $s1, $o0, $o1);
	ok(1, "phase $tag completed");
}

# ============================================================
# Phase S3-shaped: 100% write, both nodes, same row range.
# ============================================================
run_load('S3', '-f', "$dir/s3.sql") if $PHASE eq 'ALL' || $PHASE eq 'S3';

# ============================================================
# Phase S5-shaped: 99% read / 1% write, both nodes.
# ============================================================
run_load('S5', '-f', "$dir/s5r.sql\@99", '-f', "$dir/s5w.sql\@1")
  if $PHASE eq 'ALL' || $PHASE eq 'S5';

# ============================================================
# Multi probe: node0 composes a local-all-member multixact (two FOR SHARE
# sessions, then one with an updater); node1 reads / writes the row after.
# Deterministic; captures the actual cross-node behavior of the multi leg.
# ============================================================
if ($PHASE eq 'ALL' || $PHASE eq 'MULTI')
{
	# Quiesce after the load phases, then compose multis on a FRESH mirrored
	# table only these probe sessions touch (the storm-battered census_acc
	# pages may be held remotely; probes must not inherit that contention).
	usleep(10_000_000);
	ok(mirrored_coincident_create('census_multi', 'CREATE TABLE census_multi (aid int, v int)'),
		'census_multi relfilepath coincidence holds');
	ok(write_retry($node0, 'INSERT INTO census_multi SELECT g, 0 FROM generate_series(1, 20) g'),
		'census_multi seeded');

	my ($s0, $s1) = (snap_counters($node0), snap_counters($node1));
	my ($o0, $o1) = (log_offset($node0), log_offset($node1));

	sub bq
	{
		my ($bg, $tag, $sql) = @_;
		my $ok = eval { $bg->query_safe($sql); 1 };
		diag("PROBE-STEP $tag FAILED: $@") if !$ok;
		return $ok;
	}

	# Fresh session pair per probe: a query_safe timeout permanently desyncs
	# a BackgroundPsql's banner protocol, so sessions are never reused across
	# probe groups.
	sub probe_sessions
	{
		my ($s1, $s2) = ($node0->background_psql('postgres', timeout => 25),
			$node0->background_psql('postgres', timeout => 25));
		return ($s1, $s2);
	}

	# Probe A: lockers-only multi (share + share), committed, then remote read.
	{
		my ($bg1, $bg2) = probe_sessions();
		bq($bg1, 'A1', 'BEGIN');
		bq($bg1, 'A2', 'SELECT v FROM census_multi WHERE aid = 7 FOR SHARE');
		bq($bg2, 'A3', 'BEGIN');
		bq($bg2, 'A4', 'SELECT v FROM census_multi WHERE aid = 7 FOR SHARE');
		bq($bg1, 'A5', 'COMMIT');
		bq($bg2, 'A6', 'COMMIT');
		eval { $bg1->quit };
		eval { $bg2->quit };
		my ($rc_a, $out_a, $err_a) = $node1->psql('postgres',
			'SELECT aid, v FROM census_multi WHERE aid = 7', timeout => 15);
		diag("MULTI-PROBE A (lockers-only, committed): rc=$rc_a out=[$out_a] err=[$err_a]");
	}

	# Probe B: KEY SHARE + own-node non-key UPDATE (compatible locks -> a
	# real updater-member multixact), both committed, then the remote read.
	{
		my ($bg1, $bg2) = probe_sessions();
		bq($bg1, 'B1', 'BEGIN');
		bq($bg1, 'B2', 'SELECT v FROM census_multi WHERE aid = 11 FOR KEY SHARE');
		bq($bg2, 'B3', 'BEGIN');
		bq($bg2, 'B4', 'UPDATE census_multi SET v = v + 100 WHERE aid = 11');
		bq($bg2, 'B5', 'COMMIT');
		bq($bg1, 'B6', 'COMMIT');
		eval { $bg1->quit };
		eval { $bg2->quit };
		my ($rc_b, $out_b, $err_b) = $node1->psql('postgres',
			'SELECT aid, v FROM census_multi WHERE aid = 11', timeout => 15);
		diag("MULTI-PROBE B (keyshare+update multi, committed): rc=$rc_b out=[$out_b] err=[$err_b]"
			 . " -- CORRECT answer is v=100");
	}

	# Probe C/D: multi still LIVE (uncommitted lockers) during the remote ops.
	{
		my ($bg1, $bg2) = probe_sessions();
		bq($bg1, 'C1', 'BEGIN');
		bq($bg1, 'C2', 'SELECT v FROM census_multi WHERE aid = 13 FOR SHARE');
		bq($bg2, 'C3', 'BEGIN');
		bq($bg2, 'C4', 'SELECT v FROM census_multi WHERE aid = 13 FOR SHARE');
		my ($rc_c, $out_c, $err_c) = $node1->psql('postgres',
			'SELECT aid, v FROM census_multi WHERE aid = 13', timeout => 15);
		diag("MULTI-PROBE C (lockers-only, LIVE): rc=$rc_c out=[$out_c] err=[$err_c]");
		my ($rc_d, $out_d, $err_d) = $node1->psql('postgres',
			'UPDATE census_multi SET v = v + 1 WHERE aid = 13', timeout => 15);
		diag("MULTI-PROBE D (remote UPDATE against live multi): rc=$rc_d out=[$out_d] err=[$err_d]");
		bq($bg1, 'C5', 'COMMIT');
		bq($bg2, 'C6', 'COMMIT');
		eval { $bg1->quit };
		eval { $bg2->quit };
	}

	# Probe E: probe B's row again after checkpoint + idle (durable-stamped
	# window) -- separates the delayed-cleanout dimension from the multi one.
	{
		usleep(2_000_000);
		write_retry($node0, 'CHECKPOINT');
		my ($rc_e, $out_e, $err_e) = $node1->psql('postgres',
			'SELECT aid, v FROM census_multi WHERE aid = 11', timeout => 15);
		diag("MULTI-PROBE E (B row after checkpoint): rc=$rc_e out=[$out_e] err=[$err_e]"
			 . " -- CORRECT answer is v=100");
	}

	# Probe F: the multi-id ALIAS direction.  node1 first advances its OWN
	# local multixact space past node0's ids by composing lockers-only multis
	# on a node1-resident table; the native decode of node0's updater-multi on
	# the aid=11 tuple then RESOLVES against node1's unrelated local members
	# instead of erroring -- the silent-wrong direction the error in probes
	# B/E only masks by accident of id-space position.
	{
		ok(mirrored_coincident_create('census_local1',
				'CREATE TABLE census_local1 (aid int, v int)'),
			'census_local1 relfilepath coincidence holds');
		ok(write_retry($node1,
				'INSERT INTO census_local1 SELECT g, 0 FROM generate_series(1, 5) g'),
			'census_local1 seeded by node1');
		for my $round (1 .. 3)
		{
			my $bgx = $node1->background_psql('postgres', timeout => 25);
			my $bgy = $node1->background_psql('postgres', timeout => 25);
			bq($bgx, "F$round-1", 'BEGIN');
			bq($bgx, "F$round-2", "SELECT v FROM census_local1 WHERE aid = $round FOR SHARE");
			bq($bgy, "F$round-3", 'BEGIN');
			bq($bgy, "F$round-4", "SELECT v FROM census_local1 WHERE aid = $round FOR SHARE");
			bq($bgx, "F$round-5", 'COMMIT');
			bq($bgy, "F$round-6", 'COMMIT');
			eval { $bgx->quit };
			eval { $bgy->quit };
		}
		my $mx1 = $node1->safe_psql('postgres',
			q{SELECT next_multixact_id FROM pg_control_checkpoint()});
		diag("PROBE F: node1 checkpointed next_multixact_id=$mx1 (in-memory is ahead)");
		my ($rc_f, $out_f, $err_f) = $node1->psql('postgres',
			'SELECT aid, v FROM census_multi WHERE aid = 11', timeout => 15);
		diag("MULTI-PROBE F (node1 owns aliasing multi ids; reads node0 updater-multi row): "
			 . "rc=$rc_f out=[$out_f] err=[$err_f] -- CORRECT answer is v=100; v=0 = SILENT WRONG");
	}
	report_phase('MULTI', $pair, $s0, $s1, $o0, $o1);

	# Ground truth for the multi table (from the composing node).
	my $mt = $node0->safe_psql('postgres',
		'SELECT count(*), sum(v) FROM census_multi');
	diag("MULTI GROUND TRUTH node0 count/sum: $mt (expect 20|101 after B+D outcomes noted above)");
	ok(1, 'multi probes completed');
}

# Ground truth for the report: expected sum(v).
my $sum0 = $node0->safe_psql('postgres', 'SELECT count(*), sum(v) FROM census_acc');
diag("GROUND TRUTH node0 count/sum: $sum0");
my ($rc_g, $out_g, $err_g) = $node1->psql('postgres', 'SELECT count(*), sum(v) FROM census_acc');
diag("GROUND TRUTH node1 count/sum: rc=$rc_g out=[$out_g] err=[$err_g]");

$pair->stop_pair;
done_testing();
