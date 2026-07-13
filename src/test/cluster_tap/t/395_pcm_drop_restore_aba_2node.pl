# W2 ownership-generation: GCS drop restore N->X->N ABA (2-node).
#
# SCOPE: exactly ONE defect -- the bounded GCS drop's restore-ABA.  The drop
# stages pcm_state=N, then releases the header spinlock for the bounded
# InvalidateBufferTry.  On a raced pin the Try fails and the drop re-locks to
# restore the staged state.  The old ==N-only guard blocked a plain overwrite
# but NOT an N->X->N ABA: a concurrent ownership round completing inside that
# window leaves pcm_state==N again with a NEW ownership generation, and the
# blind restore then resurrected the stale pre-drop X/S over the re-owned
# block (Rule 8.A stale holder).  The fix gates the restore on the generation
# being unchanged and bumps pcm.restore_aba_detected_count when it moved.
#
# WHY TWO INJECTS (audited + verified live by the RED subagent):
#  * the restore arm is reached ONLY when InvalidateBufferTry returns false,
#    which requires a foreign pin appearing in the sub-microsecond gap between
#    its UnlockBufHdr and the partition-lock recheck -- a continuously-held
#    pin is refused at the drop's ENTRY refcount gates and never reaches the
#    restore arm.  cluster-pcm-drop-prepin-window (sleep, GCS-drop-gated)
#    holds that exact gap open so a reader can place the pin: ReadBuffer pins
#    BEFORE LockBuffer's PCM acquire, so the pin lands instantly even while
#    the reader's S acquire then queues behind the in-flight transfer.
#  * a real concurrent N->X->N round inside the restore window is not
#    SQL-schedulable (the grant would queue behind the same in-flight
#    transfer).  cluster-pcm-restore-aba-force-round (:skip) completes the
#    simulated round at the exact window point -- one coherent transition to
#    N with a generation bump, indistinguishable to the restore guard from a
#    real round.  Same force-behavior inject pattern as
#    cluster-gcs-block-duplicate-grant-reply.
#
# The counter fire is a FULL-CHAIN proof: it is reachable only via
# drop entry gates passed (no pin) -> stage N -> prepin window pin ->
# Try==false -> force-round gen move -> guard refuses the stale restore.
#
#	L1  pair boots, peers connected.
#	L2  the ABA was detected: pcm.restore_aba_detected_count delta >= 1
#	    (old code silently restored the stale pre-drop X here) and the
#	    reader (the pinner) completes cleanly.
#	L3  liveness + convergence: the requester's UPDATE lands after retries
#	    (the PINNED drop is a retryable deny, never a silent stale grant);
#	    both nodes converge to the same final value.
#	L4  zero TT noise (53R97 / recycled / unknown) over the whole test.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-2.36-gcs-block-transfer.md
# Spec: spec-5.2-cross-node-tx-wait.md
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep time);
use IPC::Run ();

my ($n0, $n1);

sub state_int {
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Summed 53R97 / TT-recycled / TT-unknown "noise fired" counters (same set as
# t/394).
my $TT_SQL = q{
	SELECT COALESCE(SUM(value::bigint), 0) FROM pg_cluster_state
	 WHERE (category = 'cr' AND key IN (
			'vis53r97_leg_invalid_scn_refuse_count',
			'vis53r97_leg_zero_match_refuse_count',
			'vis53r97_leg_srv_other_refuse_count',
			'vis53r97_leg_covers_refuse_count',
			'vis53r97_leg_multi_unresolvable_count',
			'vis53r97_leg_xmax_unprovable_count',
			'cr_xmax_recycled_invisible_count'))
	    OR (category = 'tt_status_hint' AND key = 'drop_unknown_version_count')
	    OR (category = 'tt_recovery'    AND key = 'recycled_liveness_relaxed')
};

sub tt_noise_sum {
	return int($n0->safe_psql('postgres', $TT_SQL))
		+ int($n1->safe_psql('postgres', $TT_SQL));
}

sub arm {
	my ($node, $val) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$val'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

sub disarm {
	my ($node) = @_;
	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pcm_restore_aba',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_bast = on',
		'cluster.read_scache = on',
		'cluster.crossnode_runtime_visibility = off',
		'cluster.gcs_block_local_cache = on',
		# The requester must survive one PINNED deny + a clean re-serve.
		# The invalidate-ACK window must outlast the 2.5s prepin sleep: a
		# directive whose execute is stalled by the sleep ACKs ~2.5s after
		# broadcast, and a shorter window turns that late-but-correct ACK
		# into a stale_reply_drop (a test-parameter artifact, not a defect).
		'cluster.gcs_block_invalidate_ack_timeout_ms = 4000',
		'cluster.gcs_reply_timeout_ms = 2000',
		'cluster.gcs_block_retransmit_max_retries = 8',
		'cluster.gcs_block_starvation_max_retries = 60' ]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 peers 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 peers 1->0 connected');

# Coinciding-filepath fixture: node0 seeds (first toucher = block master AND
# X holder), quiesced (frozen + hint-clean, no ACTIVE ITL) so node1's UPDATE
# takes the destructive X-transfer serve -> node0-side drop.
my $tbl;
for my $i (1 .. 12) {
	my $t = "ra_t$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int) WITH (fillfactor=10)")
		for ($n0, $n1);
	my $p0 = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $n1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $tbl = $t; last; }
}
die 'no coinciding filepath found' unless defined $tbl;
diag("table=$tbl");

$n0->safe_psql('postgres', "INSERT INTO $tbl VALUES (1, 1)");
$n0->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n0->safe_psql('postgres', 'CHECKPOINT');
$n0->safe_psql('postgres', "SELECT count(*) FROM $tbl");
usleep(300_000);

my $aba_b = state_int($n0, 'pcm', 'restore_aba_detected_count');
my $tt_b = tt_noise_sum();

# Hold the InvalidateBufferTry pin gap open on the serving node (node0) and
# arm the simulated round at the restore window.
arm($n0, 'cluster-pcm-drop-prepin-window:sleep:2500000,'
	   . 'cluster-pcm-restore-aba-force-round:skip');
usleep(400_000);

# Requester: node1 UPDATE (X transfer request -> node0 serve -> drop).
my ($rin, $rout, $rerr) = ('', '', '');
my $t_req = time();
my $reqh = IPC::Run::start(
	[ 'psql', '-X', '-q', '-d', $n1->connstr('postgres'),
		'-c', "UPDATE $tbl SET v = v + 10 WHERE k = 1" ],
	\$rin, \$rout, \$rerr);

# Give the request time to reach the serve, pass the drop's entry gates and
# enter the prepin sleep -- THEN pin: the reader's ReadBuffer pins the block
# instantly (before its PCM S acquire queues behind the in-flight transfer),
# and the pin is held while the acquire waits, covering the Try recheck.
usleep(800_000);
my ($pin_in, $pin_out, $pin_err) = ('', '', '');
my $t_pin = time();
my $pinh = IPC::Run::start(
	[ 'psql', '-X', '-A', '-t', '-q', '-d', $n0->connstr('postgres'),
		'-c', "SELECT count(*) FROM $tbl" ],
	\$pin_in, \$pin_out, \$pin_err);

# Wait for the chain to complete: prepin sleep expires -> Try sees the pin ->
# restore arm -> force-round moves the generation -> guard refuses + counts.
my $aba = $aba_b;
for (1 .. 40) {
	$aba = state_int($n0, 'pcm', 'restore_aba_detected_count');
	last if $aba - $aba_b >= 1;
	usleep(250_000);
}

$pinh->finish;
my $pin_elapsed = time() - $t_pin;
$reqh->finish;
my $req_elapsed = time() - $t_req;
disarm($n0);

# L2 — the W2 fix contract, full-chain proven (review blocker: these MUST be
# hard assertions, not just a polled diag — without them a broken restore
# guard regresses silently).
my $aba_d = state_int($n0, 'pcm', 'restore_aba_detected_count') - $aba_b;
diag(sprintf("aba delta=%d; reader elapsed=%.2fs out=[%s] err=[%s]",
		$aba_d, $pin_elapsed, $pin_out // '',
		($pin_err // '') =~ s/\n.*//sr));
cmp_ok($aba_d, '>=', 1,
	'L2 restore-ABA detected (restore_aba_detected_count advanced; old code '
	  . 'silently restored the stale pre-drop X here)');
like(($pin_out // ''), qr/^1$/m,
	'L2 the pinning reader completed cleanly (count=1)');

# L3 — convergence.  With ruling ②'s RETRYABLE_BUSY the requester no longer
# burns its ACK budget against the circular wait (reader's in-flight S acquire
# holds GRANT_PENDING -> INVALIDATE parks -> upgrade waits): the holder answers
# BUSY, the master aborts the round, clears pending_x (unblocking that very
# reader) and retries with a fresh round identity after a short backoff — so
# the SAME statement completes.  The pre-crit VM content lock (heapam +
# visibilitymap_clear_locked) keeps every failure-capable PCM acquire out of
# the critical section, so no ERROR->PANIC escalation
# either.  A bounded number of statement-level retries is tolerated (the
# prepin sleep can still eat one serve); a wedge or a PANIC is a failure.
my $att_ok = ($reqh->result // 1) == 0 && ($rerr // '') =~ /^\s*$/;
my $tries = 0;
while (!$att_ok && $tries < 3) {
	$tries++;
	my ($rc2, $o2, $e2) = $n1->psql('postgres',
		"UPDATE $tbl SET v = v + 10 WHERE k = 1");
	if ($rc2 == 0) { $att_ok = 1; last; }
	diag("retry $tries err=[" . (($e2 // '') =~ s/\n.*//sr) . "]");
	usleep(400_000);
}
ok($att_ok,
	'L3 requester UPDATE completed within bounded retries (BUSY broke the '
	  . 'circular wait; no timeout-mediated wedge, no PANIC)');

# The BUSY protocol actually participated: the holder (node0) answered at
# least one RETRYABLE_BUSY and the master (node1) consumed it.
my $busy_sent = state_int($n0, 'gcs', 'invalidate_busy_sent_count');
my $busy_recv = state_int($n1, 'gcs', 'invalidate_busy_received_count');
diag("busy_sent(node0)=$busy_sent busy_received(node1)=$busy_recv");
cmp_ok($busy_sent, '>=', 1,
	'L3 holder answered RETRYABLE_BUSY instead of a silent park');
cmp_ok($busy_recv, '>=', 1,
	'L3 master consumed the BUSY (round aborted, not timed out)');

# Convergence: writer-side own-xid read first (no cross-node resolve), then
# freeze ONCE at the very end (no lower-xid write follows, so the shared
# relfrozenxid cannot be outrun) so node0's native read is TT-clean too.
is($n1->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 1"), '11',
	'L3 writer node reads its committed +10 (own-xid)');
$n1->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n1->safe_psql('postgres', 'CHECKPOINT');
$n1->safe_psql('postgres', "SELECT count(*) FROM $tbl");
is($n0->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 1"), '11',
	'L3 both nodes converge to the same final value');

# L4 — zero TT noise over the whole test.
is(tt_noise_sum() - $tt_b, 0,
	'L4 zero TT noise (53R97 / recycled / unknown) over the whole test');

$pair->stop_pair;
done_testing();
