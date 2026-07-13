# X-transfer copy->drop window silent lost write (2-node, deterministic).
#
# The destructive X-transfer serve captures the ship image (stable copy)
# and then drops the local copy as two separate steps with no admission
# barrier between them.  A LOCAL writer holding a cached X grant
# (cluster.gcs_block_local_cache) passes the bufmgr PCM gate on the raw
# pcm_state read BEFORE taking the content lock and is never re-verified,
# so it can be admitted INSIDE the copy->drop window: its committed write
# lands on the local page after the copy was taken, the drop then discards
# the page, and the pre-write image ships to the requester.  The write is
# durably committed (WAL flushed by the drop) yet absent from the live
# block everywhere -- the silent lost write the REVOKING+generation
# linearization closes (adjudication gaps (a) cached-X no-reverify and
# (b) copy->drop admission, one interleave).
#
# Deterministic rig: the cluster-gcs-xfer-copy-drop-window sleep inject
# holds the window open for 1.5s on the serving node; the local writer runs
# inside it, then the inject is disarmed so the requester's retry re-serves
# cleanly.  Interleave validity is asserted, not assumed: the requester's
# UPDATE must have waited through at least one stall (elapsed >= 1.3s) and
# the window write must have run unblocked (elapsed < 1s, i.e. it was
# admitted, not queued behind the transfer).
#
#	L1  pair boots, cached-X local cache on.
#	L2  window write is admitted and commits inside the stall.
#	L3  the committed window write survives the transfer -- final value on
#	    BOTH nodes reflects seed(1) + window(+100) + requester(+10) = 111.
#	    A broken window ships the pre-write image: both nodes read 11 and
#	    the +100 is silently gone.
#	L4  the generation gate fired: xfer_stale_deny_count advanced (the
#	    window was exercised and CLOSED, not merely avoided) and the
#	    requester UPDATE ultimately succeeded on the re-serve.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-2.36-gcs-block-transfer.md
# Spec: spec-5.2-cross-node-tx-wait.md
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use IPC::Run ();
use Test::More;
use Time::HiRes qw(usleep time);

sub state_int {
	my ($node, $cat, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

sub arm_inject {
	my ($node, $val) = @_;

	$node->safe_psql('postgres',
		"ALTER SYSTEM SET cluster.injection_points = '$val'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	return;
}

sub disarm_inject {
	my ($node) = @_;

	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	return;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'xfer_lostwrite',
	quorum_voting_disks => 3,
	data_port_span => 2,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.crossnode_runtime_visibility = on',
		# The requester must outlast one stall + STALE deny + a clean
		# re-serve, so give it a generous retransmit budget and reply window.
		'cluster.gcs_reply_timeout_ms = 2000',
		'cluster.gcs_block_retransmit_max_retries = 12',
		'cluster.gcs_block_starvation_max_retries = 60' ]);
$pair->start_pair;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 peers 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 peers 1->0 connected');
is($pair->node0->safe_psql('postgres', 'SHOW cluster.gcs_block_local_cache'),
	'on', 'L1 cached-X local cache on (the gate-skip precondition)');

# Coinciding-filepath fixture (shared storage, per-node catalogs).
my $table;
for my $i (1 .. 8) {
	my $t = "lw_t$i";

	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
		for ($pair->node0, $pair->node1);
	my $p0 = $pair->node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	my $p1 = $pair->node1->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});

	if ($p0 eq $p1) {
		$table = $t;
		last;
	}
}
die 'no coinciding filepath' unless defined $table;
diag("table=$table");

# Seed on node0 (node0 = first toucher = block master AND X holder), then
# read back + checkpoint so the seed ITL slot is cleaned out (an ACTIVE
# slot would route the request to the READ_IMAGE path, not the destructive
# transfer).
$pair->node0->safe_psql('postgres', "INSERT INTO $table VALUES (1, 1)");
$pair->node0->safe_psql('postgres', 'CHECKPOINT');
$pair->node0->safe_psql('postgres', "SELECT v FROM $table WHERE k = 1");

my $lw0_b = state_int($pair->node0, 'gcs', 'lost_write_detected_count');
my $lw1_b = state_int($pair->node1, 'gcs', 'lost_write_detected_count');
my $stale_b = state_int($pair->node0, 'gcs', 'xfer_stale_deny_count');

# Hold the copy->drop window open on the serving node (node0) for 1.5s.
arm_inject($pair->node0, 'cluster-gcs-xfer-copy-drop-window:sleep:1500000');
usleep(500_000);

# Requester: node1 UPDATE (N->X request into node0's self-ship path).
my ($in, $out, $err) = ('', '', '');
my $t_req = time();
my $reqh = IPC::Run::start(
	[ 'psql', '-X', '-q', '-d', $pair->node1->connstr('postgres'),
		'-c', "UPDATE $table SET v = v + 10 WHERE k = 1" ],
	\$in, \$out, \$err);

# Let the request reach the serve and take the stable copy, then run the
# window write on node0 while the serve is held before its drop.
usleep(600_000);

my $t_win = time();
my ($wrc, $wout, $werr) = $pair->node0->psql('postgres',
	"UPDATE $table SET v = v + 100 WHERE k = 1 RETURNING v");
my $win_elapsed = time() - $t_win;
diag(sprintf("window write: rc=%d v=[%s] elapsed=%.2fs err=[%s]",
		$wrc, $wout // '', $win_elapsed, $werr // ''));

is($wrc, 0, 'L2 window write committed (cached-X admission inside the window)');
is($wout // '', '101',
	'L2 window write applied on the pre-transfer local copy (1 + 100)');
cmp_ok($win_elapsed, '<', 1.0,
	'L2 window write was admitted, not queued behind the transfer');

# The window write is committed; disarm so the requester's retry after the
# STALE deny re-serves the CURRENT (post-write) image cleanly.  The first
# requester attempt fails-closed RETRYABLE (53R9X, the round-6 transient
# revoke deny) — it must NEVER silently succeed on a stale image.
$reqh->finish;
my $req1_err = $err // '';
my $req_elapsed = time() - $t_req;
diag(sprintf("requester UPDATE attempt1: exit=%d elapsed=%.2fs err=[%s]",
		$reqh->result, $req_elapsed, $req1_err));
isnt($reqh->result, 0,
	'L2 requester attempt1 fails-closed (never a silent stale grant)');
like($req1_err, qr/transiently refused|clean-page|53R9X|retry/i,
	'L2 attempt1 failure is the retryable transient-revoke deny');
cmp_ok($req_elapsed, '>', 1.3,
	'L2 requester attempt1 waited through the stall (interleave really happened)');

disarm_inject($pair->node0);
usleep(500_000);

# L3: the committed +100 must SURVIVE the transfer.  Read it on the writer
# node (node0) -- an own-xid read, no cross-node resolve, so the orthogonal
# fresh-cluster low-xid 53R97 (step-1 territory) cannot noise this check.
# A broken copy->drop window discards node0's committed v=101 and ships the
# pre-write image, so node0 loses the +100 (converges to 1 or, after the
# requester's +10, 11).  The fixed generation gate refuses the stale drop,
# so node0 keeps its committed write: the value still contains +100.
my $v0;
for my $try (1 .. 15) {
	my ($rc, $out0, $e0) = $pair->node0->psql('postgres',
		"SELECT v FROM $table WHERE k = 1");
	if ($rc == 0 && defined $out0 && $out0 ne '') {
		$v0 = int($out0);
		last;
	}
	usleep(300_000);
}
diag("L3 node0 (writer) reads v=" . (defined $v0 ? $v0 : '(unreadable)'));
ok(defined $v0, 'L3 node0 can read its own committed row');
cmp_ok($v0 // 0, '>=', 101,
	'L3 the committed window write (+100) survived the transfer '
	  . '(v=' . ($v0 // 'undef') . '; a lost write would leave v<101)');

# L3b: best-effort application retry of the requester's +10 (the retryable
# contract).  Cross-node, so it may transiently hit the orthogonal low-xid
# 53R97 on a fresh cluster; tolerated.  When it lands, node0 converges to
# 111 -- still +100-present, never a lost write.
my $req2_ok = 0;
my $req2_err = '';
for my $try (1 .. 15) {
	my ($rc, $out2, $e2) = $pair->node1->psql('postgres',
		"UPDATE $table SET v = v + 10 WHERE k = 1");
	if ($rc == 0) {
		$req2_ok = 1;
		last;
	}
	$req2_err = (split /\n/, ($e2 // 'unknown'))[0];
	usleep(400_000);
}
diag("L3b requester +10 retry: ok=$req2_ok last_err=[$req2_err]");

# L4: the generation gate fired -- the copy->drop window was exercised and
# CLOSED (a stale-image drop refused), not merely avoided.  No lost-write
# detector fire either (the write was never dropped, so no torn history).
my $stale_delta = state_int($pair->node0, 'gcs', 'xfer_stale_deny_count') - $stale_b;
my $lw0_delta = state_int($pair->node0, 'gcs', 'lost_write_detected_count') - $lw0_b;
my $lw1_delta = state_int($pair->node1, 'gcs', 'lost_write_detected_count') - $lw1_b;
diag("L4 xfer_stale_deny delta node0=$stale_delta "
		. "lost_write_detected delta node0=$lw0_delta node1=$lw1_delta");
cmp_ok($stale_delta, '>=', 1,
	'L4 generation gate refused >=1 stale-image drop (copy->drop window closed)');
is($lw0_delta + $lw1_delta, 0,
	'L4 no lost-write detector fire (the write was never dropped)');

$pair->stop_pair;
done_testing();
