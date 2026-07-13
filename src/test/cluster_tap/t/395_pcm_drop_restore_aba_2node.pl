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
		'cluster.gcs_block_retransmit_max_retries = 12',
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
# Quiesce before the convergence leg: the requester's timeout broke the
# round's circular wait (in-flight reader S acquire holds GRANT_PENDING ->
# parks the INVALIDATE -> upgrade holds pending_x -> blocks that same S
# grant); the reader then finalizes, and the parked directive's park_tick
# retry executes the drop and sends the catch-up ACK (~2.5s, observed),
# whose slotless arm clears the master's S bit.  Wait that chain out so the
# retry below starts from a settled directory.
usleep(8_000_000);

my $aba_d = state_int($n0, 'pcm', 'restore_aba_detected_count') - $aba_b;
diag(sprintf(
	"aba delta=%d; reader elapsed=%.2fs out=[%s] err=[%s]; "
	  . "requester attempt1 elapsed=%.2fs err=[%s]",
	$aba_d, $pin_elapsed, $pin_out // '',
	($pin_err // '') =~ s/\n.*//sr, $req_elapsed,
	($rerr // '') =~ s/\n.*//sr));

# L2 — the fix contract, full-chain proven.
cmp_ok($aba_d, '>=', 1,
	'L2 restore-ABA detected (restore_aba_detected_count advanced; old code '
	  . 'silently restored the stale pre-drop X here)');
like(($pin_out // ''), qr/^1$/m,
	'L2 the pinning reader completed cleanly (count=1)');

# L3 — liveness + convergence.  The guard left the block at N (fail-safe: N
# claims nothing) while the force-round moved only the LOCAL generation, so
# the master still books node0 as the X holder — an artificial master/mirror
# asymmetry a REAL N->X->N round would not leave (its grant/drop synchronize
# the master).  The fix's recovery contract is that the next local access
# simply re-acquires: node0's own write re-establishes mirror X (the master
# already books node0, so the re-grant is idempotent), after which the
# requester's transfer serves normally.  Drive exactly that.
my $healed = 0;
for (1 .. 20) {
	my ($rch, $oh, $eh) = $n0->psql('postgres',
		"UPDATE $tbl SET v = v + 100 WHERE k = 1");
	if ($rch == 0) { $healed = 1; last; }
	usleep(400_000);
}
ok($healed,
	'L3 node0 local re-acquire healed the N mirror (the fail-safe N is '
	  . 'transparently recoverable, not a wedge)');

# FREEZE the heal's new tuple version before node1 touches it (same recipe
# as the fixture seed): a frozen xmin is natively visible on any node, so
# node1's scan never routes node0's low xid to the cluster TT-resolve path
# and cannot trip the ORTHOGONAL fresh-cluster 53R97 fail-close (task ⑤/⑥
# territory, not this window; hint bits alone do NOT help — the remote-xid
# dimension still routes to the cluster path).
$n0->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n0->safe_psql('postgres', 'CHECKPOINT');
$n0->safe_psql('postgres', "SELECT count(*) FROM $tbl");

my $v1;
for (1 .. 20) {
	my ($rc2, $o2, $e2) = $n1->psql('postgres',
		"UPDATE $tbl SET v = v + 10 WHERE k = 1 RETURNING v");
	if ($rc2 == 0 && defined $o2 && $o2 ne '') { $v1 = int($o2); last; }
	usleep(400_000);
}
ok(defined $v1, 'L3 requester UPDATE landed (retryable deny, not a wedge)');
# node1's +10 tuple needs the same freeze treatment before either node's
# final read (node0 would otherwise TT-resolve node1's low xid).
$n1->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n1->safe_psql('postgres', "SELECT count(*) FROM $tbl");
my $v0 = $n0->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 1");
my $v1r = $n1->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 1");
diag("final v: node0=$v0 node1=$v1r");
is($v0, $v1r, 'L3 both nodes converge to the same final value');
cmp_ok(int($v0), '>=', 111,
	'L3 no lost write (the heal +100 AND at least one +10 both present)');

# L4 — zero TT noise over the whole test.
is(tt_noise_sum() - $tt_b, 0,
	'L4 zero TT noise (53R97 / recycled / unknown) over the whole test');

$pair->stop_pair;
done_testing();
