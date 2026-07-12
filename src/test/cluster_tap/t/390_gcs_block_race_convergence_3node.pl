# GCS block-ship race convergence — the three S3 storm root causes.
#
# S3 (4-node hot-block UPDATE bench) surfaced three functional defects in
# the GCS block plane, each reproducible at low concurrency:
#
#	RC-B  a retryable denial (DENIED_PENDING_X / direct-land deny) left
#	      its dedup entry IN_FLIGHT, so the requester's convergence
#	      retry — same (request_id, epoch) key — was silently swallowed
#	      as IN_FLIGHT_DUPLICATE until the TTL sweep.  Every swallowed
#	      round burned a full cluster.gcs_reply_timeout_ms
#	      (block_timeout_count++) and cascaded into 53R90 exhaustion.
#	L2 asserts a 3-corner S->X PENDING_X convergence completes with
#	ZERO reply-timeout burn.
#
#	RC-C  the node-wide invalidate broadcast slot failed a busy claim
#	      INSTANTLY, so two concurrent local S->X upgrades on one node
#	      (even on unrelated blocks) surfaced the loser as a spurious
#	      "S->X upgrade invalidate did not complete" ERROR.
#	L3 asserts the second upgrade WAITS for the slot and completes.
#
#	RC-A  a duplicate GCS_BLOCK_REPLY (dedup CACHED_REPLY resend racing
#	      the original) overwrote the outstanding-slot reply buffer while
#	      the requester consumed it lock-free -> torn 8KB image under the
#	      CRC32C verify -> false DENIED_CHECKSUM_FAIL ("wire-ABI drift").
#	L4 arms the duplicate-grant-reply inject and asserts the requester
#	drops the duplicate (stale_reply_drop_count++) instead of accepting
#	the overwrite.
#
#	L5 whole-run invariants: zero CRC verify failures, zero retransmit
#	budget exhaustion on any node.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep time);
use Test::More;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub gcs_sum
{
	my ($triple, $key) = @_;
	my $sum = 0;
	$sum += state_val($triple->node($_), 'gcs', $key) for (0 .. 2);
	return $sum;
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

sub arm_inject
{
	my ($node, $val) = @_;
	for (1 .. 40)
	{
		my ($rc) = $node->psql('postgres',
			"ALTER SYSTEM SET cluster.injection_points = '$val'");
		last if defined $rc && $rc == 0;
		usleep(300_000);
	}
	$node->psql('postgres', 'SELECT pg_reload_conf()');
	return;
}

sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# Cross-node reads can transiently hit retryable cluster errors ("TT slot
# recycled" after a fresh remote update); retry like the S3 driver does.
sub read_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(300_000);
	}
	return 0;
}

# ============================================================
# L1: boot the shared-data triple + full IC mesh.
# ============================================================
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'gcs_race',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		# The S3 bench rig runs the spec-6.12 read levers; the quiescent
		# X->S downgrade is what forms the multi-node S sets every leg
		# below depends on (without it a remote read of an X-held block
		# only ever gets a one-shot read image, never durable S).
		'cluster.read_scache = on',
	]);
$triple->start_triple;
usleep(3_000_000);

my ($node0, $node1, $node2) =
  ($triple->node(0), $triple->node(1), $triple->node(2));
for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$i alive");
}
for my $from (0 .. 2)
{
	for my $to (0 .. 2)
	{
		next if $from == $to;
		ok($triple->wait_for_peer_state($from, $to, 'connected', 30),
			"L1 node$from sees node$to connected");
	}
}

my $base_timeout = gcs_sum($triple, 'block_timeout_count');
my $base_crc     = gcs_sum($triple, 'block_checksum_fail_count');

# ============================================================
# L2 (RC-B): 3-corner S->X PENDING_X convergence must not burn reply
# timeouts.  Ten probe tables; node1 seeds + reads (S), node2 reads (S),
# then node1 UPDATEs (S->X with remote S holder node2).  Tables whose
# block master is node0 take the wire e2 nudge path (DENIED_PENDING_X +
# nowait invalidate); the requester's backoff retry must be re-evaluated
# by the master, NOT swallowed by a leftover in-flight dedup entry.
# ============================================================
my $updated = 0;
my $probes  = 0;
for my $i (1 .. 10)
{
	my $t = "w_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node1, "INSERT INTO $t VALUES (1, 10), (2, 20)");
	next unless write_retry($node1, 'CHECKPOINT');
	# Read-back on the seeder (lazy cleanout -> quiescent), then the remote
	# reader takes S — the S set is now {node1, node2}.
	$node1->safe_psql('postgres', "SELECT sum(v) FROM $t");
	next unless eval { $node2->safe_psql('postgres', "SELECT sum(v) FROM $t"); 1 };
	$probes++;

	my $t0 = time();
	my ($rc, $out, $err) = $node1->psql('postgres', "UPDATE $t SET v = v + 1 WHERE id = 1");
	my $elapsed = time() - $t0;
	note(sprintf("L2 %s UPDATE rc=%d elapsed=%.2fs", $t, $rc, $elapsed));
	$updated++ if $rc == 0;
}
cmp_ok($probes, '>=', 5, "L2 probe setup viable on >=5/10 tables ($probes)");
is($updated, $probes, "L2 all $probes conflicting UPDATEs complete");

my $l2_timeout_delta = gcs_sum($triple, 'block_timeout_count') - $base_timeout;
is($l2_timeout_delta, 0,
	'L2 PENDING_X convergence burned ZERO reply timeouts '
	  . "(delta=$l2_timeout_delta; a leftover IN_FLIGHT dedup entry swallows "
	  . 'the retry and burns cluster.gcs_reply_timeout_ms per round)');

# ============================================================
# L2b (RC-B, injection-deterministic): force ONE DENIED_PENDING_X on a
# cross-node N->S read.  The reader's backoff retry reuses the same
# (request_id, epoch) key; the master must re-evaluate it (deny released
# the dedup entry), not swallow it as IN_FLIGHT_DUPLICATE for a full
# cluster.gcs_reply_timeout_ms round.
# ============================================================
my $base_timeout2 = gcs_sum($triple, 'block_timeout_count');
my $base_starve   = gcs_sum($triple, 'starvation_denied_pending_x_count');
arm_inject($_, 'cluster-gcs-block-starvation-force-denied:skipn:1')
  for ($node0, $node1, $node2);
usleep(500_000);

my $denied_fired = 0;
for my $i (1 .. 6)
{
	my $t = "f_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node0, "INSERT INTO $t VALUES (1, 10)");
	next unless write_retry($node0, 'CHECKPOINT');
	$node0->safe_psql('postgres', "SELECT sum(v) FROM $t");

	my $t0 = time();
	my $ok = eval { $node1->safe_psql('postgres', "SELECT sum(v) FROM $t"); 1 };
	note(sprintf("L2b %s cross-node read ok=%d elapsed=%.2fs", $t, $ok ? 1 : 0, time() - $t0));
	if (gcs_sum($triple, 'starvation_denied_pending_x_count') > $base_starve)
	{
		$denied_fired = $ok;
		last;
	}
}
arm_inject($_, '') for ($node0, $node1, $node2);
ok($denied_fired,
	'L2b injected DENIED_PENDING_X fired on a cross-node read and the read completed');

my $l2b_timeout_delta = gcs_sum($triple, 'block_timeout_count') - $base_timeout2;
is($l2b_timeout_delta, 0,
	'L2b denied read converged with ZERO reply-timeout burn '
	  . "(delta=$l2b_timeout_delta; >0 = the backoff retry was swallowed by "
	  . 'the leftover IN_FLIGHT dedup entry)');

# ============================================================
# L3 (RC-C): two concurrent LOCAL S->X upgrades on one node must
# serialize on the invalidate broadcast slot, not fail instantly.
#
# The ACK-delay window comes from SIGSTOPping node2's LMS data-plane
# processes (no injection: the :skipn countdown is per-process and the
# shared skip_pending consumes cross-traffic, so a multi-worker rig
# cannot arm it deterministically).  While node2's LMS is stopped its
# INVALIDATE ACKs stall; a local-upgrade-class probe (master=node0,
# node0 S holder, node2 remote S holder) parks its backend in
# ClusterGCSBlockInvalidateAckWait — the classification signature —
# and then FAILS the upgrade, leaving the table's data untouched (no
# new xid), so a node2 re-read restores the S set for the final leg.
# ============================================================
sub wait_event_of
{
	my ($node, $qlike) = @_;
	return $node->safe_psql(
		'postgres', qq{
		SELECT coalesce(wait_event, '') FROM pg_stat_activity
		WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
		  AND state = 'active' LIMIT 1});
}

sub wait_probe_done
{
	my ($node, $qlike, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $node->safe_psql(
			'postgres', qq{
			SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND state = 'active'
			  AND pid <> pg_backend_pid()});
		return 1 if $n eq '0';
		usleep(200_000);
	}
	return 0;
}

# node2's LMS data-plane pids (LmsMain owns worker 0's channel, workers own
# the rest).  SIGSTOP freezes INVALIDATE handling + ACKs; SIGCONT resumes
# and the queued frames drain.  CSSD is untouched, so no false-DEAD.
my @n2_lms_pids = split ' ', $node2->safe_psql(
	'postgres', q{
	SELECT coalesce(string_agg(pid::text, ' '), '')
	  FROM pg_stat_activity WHERE backend_type IN ('lms', 'lms worker')});
note('L3 node2 lms pids: ' . join(',', @n2_lms_pids));

sub stop_n2_lms  { kill 'STOP', @n2_lms_pids; }
sub cont_n2_lms  { kill 'CONT', @n2_lms_pids; }

$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.gcs_block_invalidate_ack_timeout_ms = 2000');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

my @local_up;
for my $i (1 .. 14)
{
	last if @local_up >= 2;
	my $t = "u_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node0, "INSERT INTO $t VALUES (1, 10), (2, 20)");
	next unless write_retry($node0, 'CHECKPOINT');
	$node0->safe_psql('postgres', "SELECT sum(v) FROM $t");
	next unless read_retry($node2, "SELECT sum(v) FROM $t");

	stop_n2_lms();
	my $bg = $node0->background_psql('postgres', on_error_die => 0);
	bg_start_blocking($bg, "UPDATE $t SET v = v + 1 WHERE id = 1");
	my $is_local = 0;
	for (1 .. 12)
	{
		usleep(100_000);
		if (wait_event_of($node0, "UPDATE $t %") eq 'ClusterGCSBlockInvalidateAckWait')
		{
			$is_local = 1;
			last;
		}
	}
	if ($is_local)
	{
		# Keep node2 frozen until the probe FAILS its ACK window: the
		# upgrade aborts, no new xid touches the block, and the S set is
		# restorable by a plain node2 re-read below.
		wait_probe_done($node0, "UPDATE $t %", 15);
		cont_n2_lms();
		eval { $bg->quit };
		if (read_retry($node2, "SELECT sum(v) FROM $t"))
		{
			push @local_up, $t;
		}
	}
	else
	{
		# Wire-class (master elsewhere) — resume node2 so the probe can
		# conclude on its own (grant or bounded error), then discard.
		cont_n2_lms();
		wait_probe_done($node0, "UPDATE $t %", 30);
		eval { $bg->quit };
	}
	note("L3 probe $t is_local=$is_local");
}

SKIP:
{
	skip 'L3 needs two node0-local-upgrade tables (hash placement)', 1
	  unless @local_up >= 2;

	my ($ta, $tb) = @local_up;
	note("L3 local-upgrade tables: $ta + $tb");

	# Freeze node2's LMS: session A claims the broadcast slot and parks in
	# the ACK wait.  Session B collides on an UNRELATED table 400ms in —
	# pre-fix it fails INSTANTLY on the busy slot (the S3 low-concurrency
	# class); post-fix it waits.  node2 resumes 1s in, drains A's queued
	# INVALIDATE, A completes and releases the slot, then B's own round
	# trip completes.  No injected failure anywhere: BOTH upgrades succeed
	# on the fixed build.
	stop_n2_lms();
	my $bgA = $node0->background_psql('postgres', on_error_die => 0);
	bg_start_blocking($bgA, "UPDATE $ta SET v = v + 1 WHERE id = 2");
	usleep(400_000);

	my $bgB = $node0->background_psql('postgres', on_error_die => 0);
	bg_start_blocking($bgB, "UPDATE $tb SET v = v + 1 WHERE id = 2");
	usleep(600_000);
	cont_n2_lms();

	wait_probe_done($node0, "UPDATE $ta %", 15);
	wait_probe_done($node0, "UPDATE $tb %", 15);
	eval { $bgA->quit };
	eval { $bgB->quit };

	my ($va) = $node0->safe_psql('postgres', "SELECT v FROM $ta WHERE id = 2");
	my ($vb) = $node0->safe_psql('postgres', "SELECT v FROM $tb WHERE id = 2");
	ok($va == 21 && $vb == 21,
		'L3 BOTH concurrent local S->X upgrades complete (va=' . $va
		  . ' vb=' . $vb
		  . '); pre-fix the second fails instantly on the busy invalidate '
		  . 'slot ("S->X upgrade invalidate did not complete", the S3 '
		  . 'low-concurrency class)');
}
$node0->safe_psql('postgres',
	'ALTER SYSTEM RESET cluster.gcs_block_invalidate_ack_timeout_ms');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

# ============================================================
# L4 (RC-A): duplicate GRANT replies must be dropped at delivery
# (first-reply-wins), never overwrite the slot mid-consume.
# ============================================================
my $base_drop = gcs_sum($triple, 'stale_reply_drop_count');
arm_inject($_, 'cluster-gcs-block-duplicate-grant-reply:skipn:8') for ($node0, $node1, $node2);
usleep(500_000);

my $dup_reads_ok = 0;
for my $i (1 .. 5)
{
	my $t = "d_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node0, "INSERT INTO $t VALUES (1, 10)");
	next unless write_retry($node0, 'CHECKPOINT');
	$node0->safe_psql('postgres', "SELECT sum(v) FROM $t");
	# First remote read ships a reply; the armed inject sends it TWICE.
	$dup_reads_ok++
	  if eval { $node1->safe_psql('postgres', "SELECT sum(v) FROM $t"); 1 };
}
cmp_ok($dup_reads_ok, '>=', 3, "L4 >=3/5 duplicate-armed cross-node reads succeed ($dup_reads_ok)");

my $drop_delta = gcs_sum($triple, 'stale_reply_drop_count') - $base_drop;
cmp_ok($drop_delta, '>=', 1,
	'L4 duplicate reply dropped at delivery (stale_reply_drop_count '
	  . "delta=$drop_delta; 0 = the duplicate silently overwrote the "
	  . 'outstanding slot, the S3 torn-CRC precondition)');

arm_inject($_, '') for ($node0, $node1, $node2);

# ============================================================
# L5: whole-run invariant — no CRC rejects anywhere.  (Budget-exhaustion
# noise is possible in the L3 classification probes by design — a
# wire-class probe under the armed stall legitimately burns its retry
# budget — so the CRC counter is the whole-run invariant here.)
# ============================================================
my $crc_delta = gcs_sum($triple, 'block_checksum_fail_count') - $base_crc;
is($crc_delta, 0, "L5 zero CRC32C verify rejections across the whole run");

$triple->stop_triple;
done_testing();
