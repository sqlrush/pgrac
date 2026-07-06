#-------------------------------------------------------------------------
#
# 350_cluster_6_12h_pi_recovery_e2e.pl
#    spec-6.12h D-h3c — the Past Image consumed as the ONLINE thread-recovery
#    base, end to end through the real inject -> lmon GRD recovery FSM ->
#    executor worker -> replay engine wiring (the t/267 fixture shape).
#
#    Shape (same ping-pong as the t/349 differential, on one heap block,
#    INSERT-only, never read):
#      r1 + CHECKPOINT  node1 creates block 0 (XLOG_HEAP_INIT_PAGE stays
#                       BEFORE the checkpoint redo = the replay window's
#                       lower bound), and the checkpoint publishes the
#                       thread_2 wal-state slot.  Block 0 has no Past Image
#                       anywhere yet, so the D-h2 discard ride cannot fire.
#      segment A        node1 INSERTs (FPI + delta in thread_2).
#      middle           node0 INSERTs an alien row (X transfers to node0).
#      hand-back + B    node1 INSERTs again — node0's copy converts to a
#                       stamped Past Image; segment B records are post-ship.
#                       NO further checkpoint: shared storage stays at the
#                       r1-era page, and the PI survives.
#      inject           a synthetic node1-dead reconfig on node0 drives the
#                       REAL FSM -> worker -> replay_one(AUTO).  The engine
#                       meets block 0 with a resident stamped PI: lineage
#                       records gate out (ship-SCN), the first post-ship
#                       record consumes the PI base (write-back + discard),
#                       and the rest ride the normal LSN-gated storage path.
#
#    Legs:
#      L1  inject accepted (t/267 mechanism-completion pattern: node1 stays
#          alive but quiescent; the fenced-death faithful path is the
#          spec-4.11 forward link).
#      L2  the worker drove the dead-thread replay to a terminal DONE.
#      L3  h_pi_recovery_base_used advanced (the PI really was the base).
#      L4  h_pi_recovery_base_fallback did NOT advance (the only PI in the
#          run was usable; fail-safe path untraveled).
#      L5  shared-storage bytes after the worker == the digest the D-h3b
#          differential SRF computed BEFORE the inject from the very same
#          Past Image (a snapshot never consumes the PI): two independent
#          implementations of the PI rebuild — the detached SRF and the
#          live worker path — agree byte-for-byte, and t/349 already
#          proved the SRF digest equals the origin's live content.
#      L6  the PI was consumed: the D-h3b differential SRF now reports
#          'no-pi' for the block on node0.
#      L7  the declared-dead node1 is write-fenced (spec-4.12): its
#          CHECKPOINT is REJECTED on the shared-storage write — the
#          survivor's recovered bytes cannot be raced by the "dead" node
#          (harder than the t/267 mechanism-completion note, which
#          predates default-ON write fencing).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/350_cluster_6_12h_pi_recovery_e2e.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Digest::MD5 qw(md5_hex);
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

use constant BLCKSZ => 8192;

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pih3c',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [
		'autovacuum = off',
		'full_page_writes = on',
		'wal_keep_size = 256MB',
		'cluster.past_image = on',
		'cluster.online_thread_recovery = on',
	]);
$pair->start_pair;

# Let the IC tier1 mesh settle before the first cross-node round trip.
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# same-DDL/same-relfilenode harness (t/355 convention).
$n0->safe_psql('postgres', 'CREATE TABLE pp_t (id int, v int)');
$n1->safe_psql('postgres', 'CREATE TABLE pp_t (id int, v int)');

# pre-window: r1 (INIT_PAGE, off the apply matrix) + node1 CHECKPOINT — the
# replay window's lower bound (the dead thread's checkpoint redo) lands
# AFTER r1, and the wal-state slot is published.  No PI exists yet.
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (1,1)');
$n1->safe_psql('postgres', 'CHECKPOINT');
my $start_win = $n1->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

# segment A (FPI + delta) -> middle (X to node0) -> hand-back + segment B
# (PI on node0; post-ship records in thread_2).
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (2,2)');
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (3,3)');
settle();
$n0->safe_psql('postgres', 'INSERT INTO pp_t VALUES (100,100)');
settle();
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (4,4)');
$n1->safe_psql('postgres', 'INSERT INTO pp_t VALUES (5,5)');
settle();
wal_flush_stable($n1);

# Seal thread_2 with a clean segment switch: node1 is only DECLARED dead
# (t/267 mechanism-completion), so its WAL tail can otherwise end
# mid-record while the wal-state watermark points past it — the D4
# validated-end pass would fail closed on a boundary a REAL dead thread
# can never present.  The SWITCH record completes the segment (no data
# block touched: the Past Image survives, unlike a CHECKPOINT which
# would retire it through the D-h2 ride).
$n1->safe_psql('postgres', 'SELECT pg_switch_wal()');
wal_flush_stable($n1);

# Give the cluster_stats refresh loop a beat to advance the thread_2
# wal-state watermark past segment B before the window is derived.
usleep(2_000_000);

# Expected truth, computed BEFORE the inject by the D-h3b differential SRF
# from the very same Past Image (a snapshot never consumes the PI).  t/349
# proved this digest equals the origin's live page byte-for-byte, so it is
# the reference the worker's independent rebuild must reproduce.
my $end_win = $n1->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
my ($est, undef, $eap, $esk, $expected_md5) = pi_srf($n0, $start_win, $end_win, 'true');
is($est, 'ok', 'L0 pre-inject SRF rebuild over the same PI succeeds');
cmp_ok($eap, '>=', 1, 'L0b post-ship records applied in the reference rebuild');
cmp_ok($esk, '>=', 1, 'L0c lineage records gated out in the reference rebuild');

my $used0     = lever0('h_pi_recovery_base_used_count');
my $fallback0 = lever0('h_pi_recovery_base_fallback_count');
my %pi0       = pi_counters();

# ----------------------------------------------------------------------
# L1/L2: synthetic node1-dead reconfig on node0 -> REAL FSM -> worker ->
# replay_one(AUTO) -> terminal DONE (slot state 2).
# ----------------------------------------------------------------------
is($n0->safe_psql('postgres', 'SELECT cluster_reconfig_inject_dead_node_test(1)'),
	't', 'L1 synthetic node1-dead reconfig accepted on node0');

ok($n0->poll_query_until('postgres', 'SELECT cluster_thread_replay_slot_state_test(2) = 2', 't'),
	'L2 online thread recovery of thread_2 reached terminal DONE');

# ----------------------------------------------------------------------
# L3/L4: the PI was consumed as the recovery base, and the fail-safe
# fallback path stayed untraveled (block 0 held the only PI in the run).
# ----------------------------------------------------------------------
my $used1     = lever0('h_pi_recovery_base_used_count');
my $fallback1 = lever0('h_pi_recovery_base_fallback_count');
if ($used1 <= $used0)
{
	# PI-fate forensics: which of the D-h1/h2/h3a exits took the PI?
	my %pi1 = pi_counters();
	diag("PI counters after recovery (absolute, delta-vs-preinject): "
		  . join(', ',
			map { "$_=$pi1{$_}(" . ($pi1{$_} - ($pi0{$_} // 0)) . ")" }
			  sort keys %pi1));
}
cmp_ok($used1, '>', $used0, 'L3 h_pi_recovery_base_used advanced (PI consumed as base)');
is($fallback1, $fallback0, 'L4 h_pi_recovery_base_fallback unchanged (no PI abandoned)');

# ----------------------------------------------------------------------
# L5: byte truth — the worker's PI-based rebuild (already fsync'd by the
# orchestrator's durability barrier) reproduces the pre-inject reference
# digest computed from the same Past Image by the detached SRF.
# ----------------------------------------------------------------------
my $relpath = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('pp_t')");
my $worker_bytes = read_block($pair->shared_data_root . '/' . $relpath, 0);
ok(defined $worker_bytes, 'L5a shared-storage block readable after the worker');
is(md5_hex($worker_bytes), $expected_md5,
	'L5 worker PI-based rebuild == detached SRF reference, byte-for-byte');

# ----------------------------------------------------------------------
# L6: the PI was discarded on consumption — the D-h3b differential SRF
# reports no-pi for the block now.
# ----------------------------------------------------------------------
my $probe = $n0->safe_psql('postgres',
	"SELECT cluster_pi_apply_redo_test('pp_t', 0, 0, 2, '0/1', '0/1', true)");
like($probe, qr/^no-pi:/, 'L6 the consumed Past Image is gone (no-pi)');

# ----------------------------------------------------------------------
# L7: the declared-dead node1 is write-fenced (spec-4.12): a CHECKPOINT
# that would flush its live copy of the recovered block over the
# survivor's rebuild is REJECTED at the shared-storage write.
# ----------------------------------------------------------------------
my ($ret, $out, $err) = $n1->psql('postgres', 'CHECKPOINT');
isnt($ret, 0, 'L7 checkpoint on the declared-dead node fails');
like($err, qr/checkpoint request failed/,
	'L7b checkpoint failure surfaced to the client');
like(slurp_node1_log(), qr/cluster shared-storage write rejected: this node is write-fenced/,
	'L7c the rejection is the spec-4.12 write fence');

$pair->stop_pair;
done_testing();


# --- helpers -------------------------------------------------------------

sub settle { usleep(700_000); return; }

sub wal_flush_stable
{
	my ($node) = @_;
	my $prev = '';
	for my $i (1 .. 50)
	{
		my $cur =
		  $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');
		return if $cur eq $prev;
		$prev = $cur;
		usleep(200_000);
	}
	diag('wal_flush_stable: stream still moving after 10s; proceeding');
	return;
}

sub lever0
{
	my ($key) = @_;
	return $n0->safe_psql('postgres',
			"SELECT value::bigint FROM cluster_dump_state() "
		  . "WHERE category = 'xnode_lever' AND key = '$key'");
}

# every h_pi_* lever on node0, as a hash (PI-fate forensics on failure)
sub pi_counters
{
	my $rows = $n0->safe_psql('postgres',
			"SELECT key || '=' || value FROM cluster_dump_state() "
		  . "WHERE category = 'xnode_lever' AND key LIKE 'h_pi_%' ORDER BY key");
	return map { split(/=/, $_, 2) } split(/\n/, $rows);
}

# cluster_pi_apply_redo_test wrapper -> (status, ship_scn, applied, skipped, md5)
sub pi_srf
{
	my ($node, $start, $end, $use_pi, $rel) = @_;
	$rel //= 'pp_t';
	my $out = $node->safe_psql('postgres',
			"SELECT cluster_pi_apply_redo_test('$rel', 0, 0, 2, "
		  . "'$start', '$end', $use_pi)");
	return split(/:/, $out, 5);
}

sub slurp_node1_log
{
	my $log = $n1->logfile;
	open(my $fh, '<', $log) or die "open $log: $!";
	local $/;
	my $all = <$fh>;
	close($fh);
	return $all;
}

sub read_block
{
	my ($path, $block_no) = @_;
	open(my $fh, '<:raw', $path) or die "open $path: $!";
	sysseek($fh, $block_no * BLCKSZ, 0) or die "seek $path: $!";
	my $buf = '';
	my $n = sysread($fh, $buf, BLCKSZ);
	close($fh);
	return undef if !defined $n || $n != BLCKSZ;
	return $buf;
}
