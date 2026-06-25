#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 248_shared_merged_recovery.pl
#    spec-4.5a -- true cold-crash k-way merged recovery on a REAL
#    shared-data backend (cluster_fs) + the full cross-instance CR/TT
#    read closure (G1-G6).
#
#    Substrate (L92 honesty): both postmasters run on one host and
#    share one real directory tree for WAL threads AND data files (the
#    voting-disk harness pattern) -- a true shared filesystem, not
#    cross-host shared storage.  Same-DDL/same-relfilenode is the
#    harness naming premise (L0 preflight pins it; production naming
#    is feature #11).
#
#      L0   preflight: pg_relation_filepath identical on both nodes
#           (else skip_all) + shared-root sentinel carries BOTH node
#           ids as participants
#      L13  torn/corrupt candidate WAL below the validated end ->
#           merged recovery refused 53RA3 (never silent truncation);
#           restore -> engages.  (Cursor xl_scn-vs-LSN ordering is
#           unit-locked in cluster_recovery_merge engine tests.)
#      L1   A merged: BOTH row sets (A's 100 + B's 100) visible at A,
#           content checks via sums
#      L2   engage PASSED in the log; merged_records_applied > 0;
#           materialized_remote_instances contains B's origin
#      L3   B's undo materialized at A: pg_undo/instance_1/seg exists
#           (the dir uses node_id = owner_instance-1, so B=node 1)
#           and its segment-header TT slots carry COMMITTED+commit_scn
#      L4   remote UBA resolver: injected snapshot (cr_force_read_scn,
#           the F4 mechanism -- a cold crash leaves no natural old
#           snapshot; runtime/warm consumers are 4.6/4.7) older than
#           B's UPDATE -> CR construction returns the OLD value from
#           materialized undo
#      L4b  F1: A/B raw-xid collision -- B COMMITS at the same 32-bit
#           xid value A ABORTED.  After merge: A's aborted tuple stays
#           invisible, A's local pg_xact still says 'aborted'
#           (txid_status), B's row is visible via the per-origin
#           outcome authority.  B's commit never polluted A's pg_xact.
#      L5   outcome authority: falsifying B's xid in A's LOCAL pg_xact
#           does not change the remote verdict (A pg_xact never
#           consulted -- AD-012 ex.9)
#      L6   in-doubt: B crashed AFTER stamping its durable TT slot but
#           BEFORE the commit record (cluster-scn-wal-write-pre crash
#           injection) -> reading that row at A raises 53R97 -- never
#           visible, never silently invisible
#      L7   authority missing link: materialized instance_<B> segment
#           removed -> remote reads fail closed 53R97; restored -> ok
#      L8   UPDATE chain: B updated the same row twice; injected
#           snapshots at both intermediate SCNs return each historical
#           version (2-deep inverse-apply chain walk)
#      L9   TT slot identity: materialized slot xid patched (models a
#           slot recycled to a later owner) -> fail-closed 53R97, the
#           old owner's commit_scn is never returned
#      L9b/L10  wrap/epoch identity family: materialized slot
#           commit_scn patched -> the outcome-vs-TT-stamp cross-check
#           (G5) fails closed 53R97.  Finer wrap-generation
#           discrimination (record-header tt_wrap_plus1 vs slot wrap)
#           is unit-locked (G4 unit suite).
#      L11  same shared page, both writers (serialized, SCN observed
#           across nodes): page ends with both rows after the merge
#      L12  B restarts post-merge while A is live: own-LSN-bound skip
#           fires (merged_own_bound_skips > 0), B's own rows intact
#           (pages not rolled back), and B's read of A-origin rows
#           fails closed (A was never materialized AT B -- the gate is
#           per-origin, not global).  L12 runs BEFORE the data-read
#           legs: a lone survivor cannot be granted GCS keys mastered
#           by a still-down peer (pre-existing Stage-2 topology
#           boundary, t/243 L4 scope note; remastering is 4.6/4.7,
#           and the GCS failure is fail-closed ERROR, not wrong data)
#      L4c  P1-1 side-effect fail-closed: B's crash window carries a
#           DROP TABLE commit (nrels > 0) -> merged recovery FATALs
#           53RA3 rather than silently skipping the peer's drop;
#           merged_recovery=off recovers A's own stream
#      L14  dump surface: recovery category = 39 keys; remote outcome
#           counters live
#
#    NB: this is a Perl TAP file -- never run clang-format on it.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D13)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PgracWalState qw(read_file_raw write_file_raw);
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

my $TT_SLOTS_OFF   = 112;    # UndoSegmentHeaderData.tt_slots
my $TT_SLOT_SIZE   = 32;
my $TT_SLOT_COUNT  = 48;
my $TT_COMMITTED   = 2;

# ----------------------------------------------------------------
# helpers
# ----------------------------------------------------------------

sub dumpkey
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value FROM pg_cluster_state
		WHERE category='recovery' AND key='$key'});
}

# Retry a scalar query while the post-recovery CSSD/no-peer-fastpath
# settles (the first data access after a lone restart can race the
# dead-peer declaration).
sub query_retry
{
	my ($node, $sql, $timeout) = @_;
	$timeout //= 60;
	my $deadline = time() + $timeout;
	my ($ret, $out, $err);
	while (time() < $deadline)
	{
		($ret, $out, $err) = $node->psql('postgres', $sql);
		return $out if $ret == 0;
		sleep 1;
	}
	diag("query_retry timed out: $sql\nlast stderr: " . ($err // ''));
	return undef;
}

# Expect the query to fail closed; return the stderr text.
sub query_fails
{
	my ($node, $sql) = @_;
	my ($ret, $out, $err) = $node->psql('postgres', $sql);
	return $ret == 0 ? undef : $err;
}

# Parse the shared-root sentinel: (uuid, [participant ids]).
sub read_sentinel
{
	my ($path) = @_;
	my $img = read_file_raw($path);
	my ($magic, $version) = unpack('LL', substr($img, 0, 8));
	die 'bad sentinel magic' unless $magic == 0x50475343;
	my $count = unpack('L', substr($img, 44, 4));
	my @ids = unpack("l$count", substr($img, 48, 4 * $count));
	return \@ids;
}

# Read TT slots from a materialized undo segment header.
# Returns list of {idx, xid, wrap, status, scn}.
sub read_tt_slots
{
	my ($segfile) = @_;
	my $img = read_file_raw($segfile);
	my @slots;
	for my $i (0 .. $TT_SLOT_COUNT - 1)
	{
		my $raw = substr($img, $TT_SLOTS_OFF + $i * $TT_SLOT_SIZE, $TT_SLOT_SIZE);
		my ($xid, $wrap, $status) = unpack('LSC', substr($raw, 0, 7));
		my $scn = unpack('Q', substr($raw, 8, 8));
		push @slots, { idx => $i, xid => $xid, wrap => $wrap,
			status => $status, scn => $scn };
	}
	return @slots;
}

# Patch one field of every COMMITTED TT slot in a segment file.
# $field: 'xid' (+= delta) or 'scn' (+= delta).  Returns saved image.
sub patch_committed_slots
{
	my ($segfile, $field, $delta) = @_;
	my $img = read_file_raw($segfile);
	my $saved = $img;
	for my $i (0 .. $TT_SLOT_COUNT - 1)
	{
		my $base = $TT_SLOTS_OFF + $i * $TT_SLOT_SIZE;
		my $status = unpack('C', substr($img, $base + 6, 1));
		next unless $status == $TT_COMMITTED;
		if ($field eq 'xid')
		{
			my $xid = unpack('L', substr($img, $base, 4));
			substr($img, $base, 4) = pack('L', ($xid + $delta) & 0xFFFFFFFF);
		}
		else
		{
			my $scn = unpack('Q', substr($img, $base + 8, 8));
			substr($img, $base + 8, 8) = pack('Q', $scn + $delta);
		}
	}
	write_file_raw($segfile, $img);
	return $saved;
}

# Flip B's xid status in A's LOCAL pg_xact to ABORTED (10b).
sub patch_local_pg_xact_aborted
{
	my ($datadir, $xid) = @_;
	my $file = "$datadir/pg_xact/0000";
	my $img = read_file_raw($file);
	my $saved = $img;
	my $byte = int($xid / 4);
	my $shift = ($xid % 4) * 2;
	my $old = unpack('C', substr($img, $byte, 1));
	my $new = ($old & ~(0x3 << $shift)) | (0x2 << $shift);
	substr($img, $byte, 1) = pack('C', $new);
	write_file_raw($file, $img);
	return $saved;
}

# Rewrite a node's pgrac.conf to declare ONLY itself (single-node
# disaster-recovery form).  declared_count == 1 makes block-master
# lookup resolve to self, so post-recovery data reads bypass GCS
# Cache Fusion (a live 2-node read of a peer-mastered block is blocked
# by a pre-existing Stage-2 GCS gap, t/243 L4 / roadmap 4.7).  Merged
# recovery still finds the peer's crashed stream because candidate
# discovery scans the WAL-state registry, not pgrac.conf membership.
sub make_single_node
{
	my ($node, $node_id, $ic_port) = @_;
	my $conf = $node->data_dir . '/pgrac.conf';
	open my $fh, '>', $conf or die "open $conf: $!";
	print $fh "[cluster]\nname = sharedmerge\n\n"
	  . "[node.$node_id]\ninterconnect_addr = 127.0.0.1:$ic_port\n";
	close $fh;
}

# Wait until the postmaster of $node has fully exited.
sub wait_postmaster_gone
{
	my ($node, $timeout) = @_;
	$timeout //= 60;
	my $pidfile = $node->data_dir . '/postmaster.pid';
	my $deadline = time() + $timeout;
	while (time() < $deadline)
	{
		return 1 unless -f $pidfile;
		sleep 1;
	}
	return 0;
}

# ----------------------------------------------------------------
# pair setup -- build the on-disk layout (initdb both nodes, shared
# data root, per-thread WAL, sentinel) but DRIVE EACH NODE AS A SINGLE
# NODE, serialized.  Active 2-node DML on a cluster_fs shared table
# goes through PCM/GCS Cache Fusion, which is not yet mature for a live
# 2-node topology (pre-existing Stage-2 gap, t/243 L4 / roadmap 4.7).
# The cold-cluster disaster-recovery scenario this spec verifies is
# inherently serialized -- the nodes never run concurrently -- so each
# runs alone (declared_count == 1 -> PCM inactive -> writes/reads
# bypass GCS).  The shared data root + WAL registry + sentinel are the
# cross-node coupling the merge actually consumes.
# ----------------------------------------------------------------
my $pair = PostgreSQL::Test::ClusterPair->new_pair('sharedmerge',
	quorum_voting_disks => 3,
	wal_threads_root => 1,
	shared_data      => 1,
	extra_conf       => [
		'autovacuum = off',
		'restart_after_crash = off',
		'cluster.merged_recovery = on',
		'cluster.recovery_workers_max = 0',
		'cluster.recovery_stale_active_ms = 1000',
	]);
my $walroot  = $pair->wal_threads_root;
my $dataroot = $pair->shared_data_root;
my $na        = $pair->node0;    # node_id 0, thread 1, instance 1
my $nb        = $pair->node1;    # node_id 1, thread 2, instance 2

# Both nodes single-node before any start (no concurrent run at all).
make_single_node($na, 0, $pair->ic_port(0));
make_single_node($nb, 1, $pair->ic_port(1));

my @tables = qw(t_a t_b t_pp t_doubt t_l4 t_slot);
my $ddl = join('; ',
	'CREATE TABLE t_a (v int)',
	'CREATE TABLE t_b (v int)',
	'CREATE TABLE t_pp (v int)',
	'CREATE TABLE t_doubt (v int)',
	'CREATE TABLE t_l4 (k int, v int)',
	'CREATE TABLE t_slot (v int)');

# ================================================================
# Phase 1: node A alone -- own rows + L4b abort bait + L11 first write.
# A checkpoints (its own data flushes to the shared root; A's own
# recovery baseline) and then crashes.
# ================================================================
$na->start;
$na->safe_psql('postgres', $ddl);

my %relpath;
$relpath{$_} = $na->safe_psql('postgres', "SELECT pg_relation_filepath('$_')")
  for @tables;

$na->safe_psql('postgres', 'INSERT INTO t_a SELECT generate_series(1, 100)');
$na->safe_psql('postgres', 'SELECT txid_current()') for (1 .. 5);

my ($r4b, $out4b, $err4b) = $na->psql('postgres',
	"BEGIN;\nSELECT txid_current();\nINSERT INTO t_a VALUES (999);\nROLLBACK;");
my ($k4b) = $out4b =~ /(\d+)/;
ok(defined $k4b && $k4b > 0, "L4b A captured aborted xid K=$k4b");

$na->safe_psql('postgres', 'INSERT INTO t_pp VALUES (1)');
$na->safe_psql('postgres', 'CHECKPOINT');
my $a_scn = $na->safe_psql('postgres', 'SELECT cluster_scn_current()');
$na->stop('immediate');

# ================================================================
# Phase 2: node B alone -- adopts A's relfiles (same DDL), writes the
# rest of the data in ONE crash window (NO checkpoint after, so every
# B commit replays during A's merge and its outcome is materialized),
# and dies mid-commit on the in-doubt txn.
# ================================================================
$nb->start;
$nb->safe_psql('postgres', $ddl);    # adopt A's relfiles (owner-agnostic create)

# L0 preflight: same DDL -> same relfilenode on both nodes.
my $mismatch = 0;
for my $t (@tables)
{
	my $pb = $nb->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	$mismatch = 1 if $pb ne $relpath{$t};
}
if ($mismatch)
{
	plan skip_all =>
	  'same-DDL relfilepath coincidence does not hold on this build '
	  . '(harness premise; production naming is feature #11)';
}
ok(1, 'L0 pg_relation_filepath identical on both nodes for all 5 tables');
ok(-f "$dataroot/pgrac_shared.control", 'L0 shared-root sentinel exists');
my $ids = read_sentinel("$dataroot/pgrac_shared.control");
is_deeply([ sort { $a <=> $b } @$ids ], [ 0, 1 ],
	'L0 sentinel records both nodes as participants');

# One EARLY checkpoint publishes thread_2's redo start into the WAL
# registry (the merge needs a candidate redo origin).  It precedes ALL
# of B's data writes, so every B commit below lands in the merge window
# (checkpoint -> crash) and its outcome materializes during A's merge.
$nb->safe_psql('postgres', 'CHECKPOINT');

# Deterministic Lamport push: B observes A's last SCN so every B-side
# commit orders after A's writes (AD-008).
$nb->safe_psql('postgres', "SELECT cluster_scn_observe($a_scn)");

# L4b: burn B's xid counter to K-1, then commit a row AT xid K.
my $bnext = $nb->safe_psql('postgres', 'SELECT txid_current()');
my $burn = $k4b - 1 - $bnext;
ok($burn >= 0, "L4b B can reach A's xid (burn $burn)");
if ($burn > 0)
{
	my $sql = "SELECT txid_current();\n" x $burn;
	$nb->safe_psql('postgres', $sql);
}
$nb->safe_psql('postgres', 'INSERT INTO t_b VALUES (101)');
my $bxmin = $nb->safe_psql('postgres', 'SELECT xmin FROM t_b WHERE v = 101');
is($bxmin, $k4b, 'L4b B committed at exactly the xid value A aborted');

# L16 setup: B fills t_slot's first heap page with all 8 ITL slots, each
# claimed by a SEPARATE committed transaction (one INSERT per txn).  The 8
# rows are tiny and pack onto page 0, so its 8-slot ITL array is fully
# occupied by COMMITTED B-origin slots with 0 FREE.  Post-merge these are
# pinned foreign slots; A's first write to that page must hit ITL OVERFLOW
# (fail-closed) rather than recycle one (which would strip a live B tuple's
# origin -> alias into A's CLOG, P1 #1).
$nb->safe_psql('postgres', "INSERT INTO t_slot VALUES ($_)") for (501 .. 508);

# Rest of B's row set + the L4/L8 update-chain material.
$nb->safe_psql('postgres', 'INSERT INTO t_b SELECT generate_series(102, 200)');
$nb->safe_psql('postgres', q{INSERT INTO t_l4 VALUES (1, 10), (2, 21)});
my $scn_mid1 = $nb->safe_psql('postgres', 'SELECT cluster_scn_current()');

# Kc = the committed updater xid used by the L5 pg_xact falsification.
my ($r5, $out5, $err5) = $nb->psql('postgres',
	"BEGIN;\nSELECT txid_current();\nUPDATE t_l4 SET v = 11 WHERE k = 1;\nCOMMIT;");
my ($kc) = $out5 =~ /(\d+)/;
ok(defined $kc && $kc > 0, "L5 B captured committed updater xid Kc=$kc");

$nb->safe_psql('postgres', q{UPDATE t_l4 SET v = 22 WHERE k = 2});
my $scn_mid2 = $nb->safe_psql('postgres', 'SELECT cluster_scn_current()');
$nb->safe_psql('postgres', q{UPDATE t_l4 SET v = 23 WHERE k = 2});

# Second shared-page writer for L11 (B's row on the t_pp page).
$nb->safe_psql('postgres', 'INSERT INTO t_pp VALUES (2)');

# ----------------------------------------------------------------
# L6 staging: one psql session opens the in-doubt txn, forces a WAL
# switch so the INSERT's WAL (heap + RM_CLUSTER_UNDO) is durable
# WITHOUT committing, arms the crit-section crash, and COMMITs.  The
# crash fires before the commit record lands, so A's merge replays the
# INSERT (the shared page + the materialized undo slot, left ACTIVE)
# but finds no commit outcome -> 53R97.  A regular psql returns the
# moment the backend's connection drops (no background-session hang).
# No checkpoint anywhere in phase 2, so every B commit stays in the
# merge window.
# ----------------------------------------------------------------
$nb->psql('postgres', join(";\n",
		'BEGIN',
		'INSERT INTO t_doubt VALUES (1)',
		'SELECT pg_switch_wal()',
		q{SELECT cluster_inject_fault('cluster-scn-wal-write-pre', 'crash', 0)},
		'COMMIT')
	  . ';');
ok(wait_postmaster_gone($nb), 'L6 B died before writing the commit record');
# The inject crash (PANIC) bypasses ->stop, so clear the harness's stale
# pid bookkeeping or a later ->start would BAIL_OUT "already running".
$nb->{_pid} = undef;

sleep 2;    # > recovery_stale_active_ms (both nodes now cold)

# ----------------------------------------------------------------
# L13: corrupt B's stream at the exact LSN the merge reads from --
# thread_2's checkpoint_redo_lsn, published in the WAL-state registry
# (slot for thread 2 = byte 512 + (2-1)*512 = 1024; checkpoint_redo_lsn
# is at slot offset 56).  Corrupting the record AT the merge start makes
# the pre-replay decode produce zero complete records -> the foreign
# candidate's "corrupt below the validated end" fail-closed fires,
# instead of silently treating it as an empty torn tail and dropping
# the whole peer stream.
# ----------------------------------------------------------------
my $regimg = read_file_raw("$walroot/pgrac_wal_state");
my $redo = unpack('Q<', substr($regimg, 1024 + 56, 8));
ok($redo > 0, "L13 thread_2 checkpoint_redo_lsn from registry = $redo");

my $segsz   = 16 * 1024 * 1024;
my $segno   = int($redo / $segsz);
my $segoff  = $redo % $segsz;
my $walfile = sprintf("%s/thread_2/%08X%08X%08X",
	$walroot, 1, int($segno / 256), $segno % 256);
ok(-f $walfile, "L13 B WAL segment present ($walfile)");

my $walimg  = read_file_raw($walfile);
my $savedwal = $walimg;
substr($walimg, $segoff, 64) = "\xde\xad\xbe\xef" x 16;
write_file_raw($walfile, $walimg);

my $log_off = -s $na->logfile;
is($na->start(fail_ok => 1), 0,
	'L13 merged recovery refused on a corrupt candidate stream');
my $log = PostgreSQL::Test::Utils::slurp_file($na->logfile, $log_off);
like($log,
	qr/corrupt below the validated end|merged k-way recovery refused|WAL decode failed before the validated end/,
	'L13 refusal is the fail-closed 53RA3 surface, not silent truncation');

write_file_raw($walfile, $savedwal);

# ----------------------------------------------------------------
# main merge: A engages, drives both streams.  B stays DOWN through
# the engage + materialization asserts (cold premise), then restarts
# (L12) BEFORE the data-read legs: a lone survivor cannot be granted
# GCS keys mastered by a still-down peer (pre-existing Stage-2
# topology boundary, t/243 L4 scope note; remastering is roadmap
# 4.6/4.7) -- the cross-instance visibility machinery under test is
# orthogonal to which live node masters the block grant.
# ----------------------------------------------------------------
$log_off = -s $na->logfile;
$na->start;
$log = PostgreSQL::Test::Utils::slurp_file($na->logfile, $log_off);
like($log, qr/cluster merged recovery: engage decision PASSED/,
	'L2 engage decision PASSED');
like($log, qr/cluster merged recovery: replay complete/,
	'L2 merged replay completed');

# L2: dump surface (catalog-only SQL; no shared-block reads yet).
my $applied = query_retry($na, q{SELECT value FROM pg_cluster_state
	WHERE category='recovery' AND key='merged_records_applied'});
ok(defined $applied && $applied > 0, "L2 merged_records_applied = $applied > 0");
is(dumpkey($na, 'materialized_remote_instances'), '1',
	'L2 materialized_remote_instances contains B (origin 1)');
ok(dumpkey($na, 'remote_outcome_committed') > 0,
	'L2 remote_outcome_committed > 0 (diverted commits materialized)');

# L3: B's undo materialized at A with stamped TT slots (file-level).
my @segs = glob($na->data_dir . '/pg_undo/instance_1/seg_*.dat');
ok(@segs > 0, 'L3 instance_1 (B=node 1) undo segment materialized at A');
my @slots = read_tt_slots($segs[0]);
my @committed = grep { $_->{status} == $TT_COMMITTED && $_->{scn} > 0 } @slots;
ok(@committed > 0,
	'L3 materialized segment header carries COMMITTED TT slots with commit_scn');

# ----------------------------------------------------------------
# L12: B self-recovers (single-node form too).  A's thread is ALIVE
# in the registry, so B does not re-merge it (n_alive>0 -> not cold);
# B replays only its own thread_2 and the own-LSN-bound skip drops
# the shared records A already merged.  B's own-origin reads bypass
# GCS; reading an A-origin row never materialized AT B is the
# per-origin fail-closed path.
# ----------------------------------------------------------------
make_single_node($nb, 1, $pair->ic_port(1));
$nb->start;
is(query_retry($nb, 'SELECT count(*) FROM t_b'), '100',
	'L12 B own rows intact: merged-driven shared pages were not rolled back');
ok(dumpkey($nb, 'merged_own_bound_skips') > 0,
	'L12 own-LSN-bound skip fired during B self-recovery');
my $err = query_fails($nb, 'SELECT count(*) FROM t_a');
ok(defined $err && $err =~ /cluster TT status unknown/,
	'L12 B cannot read A-origin rows (A never materialized AT B; per-origin gate)');
$nb->stop;

# ----------------------------------------------------------------
# L1: both row sets visible at A + content equality.
# ----------------------------------------------------------------
is(query_retry($na, 'SELECT count(*) FROM t_a'), '100',
	'L1 A sees its own 100 rows');
is(query_retry($na, 'SELECT count(*) FROM t_b'), '100',
	"L1 A sees B's 100 rows through the remote authority");
is($na->safe_psql('postgres', 'SELECT sum(v) FROM t_a'), '5050',
	'L1 own content intact');
is($na->safe_psql('postgres', 'SELECT sum(v) FROM t_b'), '15050',
	'L1 remote content intact');

# L11: same shared page, both writers, both visible post-merge.
is($na->safe_psql('postgres', 'SELECT count(*), sum(v) FROM t_pp'), '2|3',
	'L11 same-page cross-stream rows both present (1 own + 1 remote)');

# L4: injected old snapshot -> CR inverse-apply returns the old value.
# cluster_inject_fault arm state is PER-BACKEND (spec-0.27 §3.6; see
# t/015_inject.pl), so the arm and the read MUST share one psql session: a
# DO block arms with no output, then the SELECT in the same backend sees it.
is($na->safe_psql('postgres',
	"DO \$\$ BEGIN PERFORM cluster_inject_fault('cr_force_read_scn', 'skip', $scn_mid1); END \$\$;\n"
	  . 'SELECT v FROM t_l4 WHERE k = 1'),
	'10', "L4 read at injected scn=$scn_mid1 sees B's pre-update value");
is($na->safe_psql('postgres',
	"DO \$\$ BEGIN PERFORM cluster_inject_fault('cr_force_read_scn', 'skip', $scn_mid1); END \$\$;\n"
	  . 'SELECT v FROM t_l4 WHERE k = 2'),
	'21', 'L4 two-step chain reconstructs the first version');

# L8: chain walk at the second intermediate SCN, then disarm.
is($na->safe_psql('postgres',
	"DO \$\$ BEGIN PERFORM cluster_inject_fault('cr_force_read_scn', 'skip', $scn_mid2); END \$\$;\n"
	  . 'SELECT v FROM t_l4 WHERE k = 2'),
	'22', 'L8 UPDATE chain middle version reconstructed');
# Disarmed: a plain session is born with no arm, so the latest versions show.
is($na->safe_psql('postgres', q{SELECT v FROM t_l4 WHERE k = 1 OR k = 2 ORDER BY k}),
	"11\n23", 'L8 disarmed snapshot sees the final versions');

# L4b: the raw-xid collision -- A's abort survives B's commit.
is($na->safe_psql('postgres', 'SELECT count(*) FROM t_a WHERE v = 999'), '0',
	'L4b A aborted tuple stays invisible after the merge');
is($na->safe_psql('postgres', 'SELECT count(*) FROM t_b WHERE v = 101'), '1',
	"L4b B's row at the SAME xid value is visible via the outcome authority");
is($na->safe_psql('postgres', "SELECT txid_status($k4b)"), 'aborted',
	"L4b A's local pg_xact still says aborted for xid $k4b (F1: not polluted)");

# L6: the in-doubt row fails closed.
$err = query_fails($na, 'SELECT count(*) FROM t_doubt');
ok(defined $err, 'L6 in-doubt read raises an error');
like($err, qr/cluster TT status unknown/,
	'L6 53R97 fail-closed: stamped-then-crashed is neither visible nor invisible');
ok(dumpkey($na, 'remote_authority_53ra') > 0,
	'L6 remote_authority_53ra counted the in-doubt verdict');

# L14: dump surface totals.
is($na->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'recovery'}),
	'39', 'L14 recovery category exposes 39 keys (spec-4.11 D5 +4 thread-recovery)');
ok(dumpkey($na, 'remote_uba_resolved') > 0,
	'L14 remote_uba_resolved > 0 (CR legs consumed materialized undo)');

# ----------------------------------------------------------------
# L5: falsify B's xid in A's LOCAL pg_xact -> verdict unchanged.
# (Restart required: pg_xact pages are SLRU-cached in shmem.)
# ----------------------------------------------------------------
$na->stop;
my $saved_xact = patch_local_pg_xact_aborted($na->data_dir, $kc);
$na->start;
is(query_retry($na, "SELECT txid_status($kc)"), 'aborted',
	'L5 the falsified local pg_xact reads back aborted (patch took)');
is($na->safe_psql('postgres', 'SELECT v FROM t_l4 WHERE k = 1'), '11',
	"L5 B's committed update stays visible: A's pg_xact is never consulted");
$na->stop;
write_file_raw($na->data_dir . '/pg_xact/0000', $saved_xact);
$na->start;

# ----------------------------------------------------------------
# L7: authority missing link -- materialized segment removed.
# (Live patch: every psql connection opens fresh segment fds and the
# durable TT reads are raw preads, so no server restart is needed.)
# ----------------------------------------------------------------
my $seg = $segs[0];
rename($seg, "$seg.bak") or die "rename: $!";
$err = query_fails($na, 'SELECT count(*) FROM t_b');
ok(defined $err && $err =~ /cluster TT status unknown/,
	'L7 remote read fails closed 53R97 when the materialized authority is gone');
rename("$seg.bak", $seg) or die "rename: $!";
is(query_retry($na, 'SELECT count(*) FROM t_b'), '100',
	'L7 restored authority resolves again');

# ----------------------------------------------------------------
# L9: slot recycled to another owner (xid patched) -> fail-closed.
# ----------------------------------------------------------------
my $saved_seg = patch_committed_slots($seg, 'xid', 1000000);
$err = query_fails($na, 'SELECT count(*) FROM t_b');
ok(defined $err && $err =~ /cluster TT status unknown/,
	'L9 xid-mismatched TT slot never returns the old owner commit_scn (53R97)');
write_file_raw($seg, $saved_seg);
is(query_retry($na, 'SELECT count(*) FROM t_b'), '100',
	'L9 restored slots resolve again');

# ----------------------------------------------------------------
# L9b/L10: identity cross-check (outcome SCN vs TT stamp) mismatch.
# ----------------------------------------------------------------
$saved_seg = patch_committed_slots($seg, 'scn', 1);
$err = query_fails($na, 'SELECT count(*) FROM t_b');
ok(defined $err && $err =~ /cluster TT status unknown/,
	'L9b/L10 outcome-vs-stamp cross-check mismatch fails closed (53R97)');
write_file_raw($seg, $saved_seg);
is(query_retry($na, 'SELECT count(*) FROM t_b'), '100',
	'L9b/L10 restored stamp resolves again');

# ----------------------------------------------------------------
# L4c: P1-1 -- a foreign commit with side effects fails the merge.
# B (single node too) writes a checkpoint-fenced DROP so A's re-merge
# window contains only the drop commit (nrels>0).
# ----------------------------------------------------------------
$nb->start;
$nb->safe_psql('postgres', 'CREATE TABLE drop_t (v int)');
$nb->safe_psql('postgres', 'INSERT INTO drop_t VALUES (1)');
$nb->safe_psql('postgres', 'CHECKPOINT');    # fence the CREATE out of the window
$nb->safe_psql('postgres', 'DROP TABLE drop_t');
$nb->stop('immediate');
$na->stop('immediate');
sleep 2;

$log_off = -s $na->logfile;
is($na->start(fail_ok => 1), 0,
	'L4c merged recovery refused on a foreign commit with side effects');
$log = PostgreSQL::Test::Utils::slurp_file($na->logfile, $log_off);
like($log, qr/foreign commit record carries an unsupported side effect/,
	'L4c P1-1 53RA3: the DROP is never silently skipped');

$na->adjust_conf('postgresql.conf', 'cluster.merged_recovery', 'off');
$na->start;
is(query_retry($na, 'SELECT count(*) FROM t_a'), '100',
	'L4c own-stream recovery completes with merged_recovery=off');
is($na->safe_psql('postgres', 'SELECT count(*) FROM t_b'), '100',
	'L4c previously materialized remote rows remain readable');

# ----------------------------------------------------------------
# L15: post-merge LOCAL DML on materialized remote rows is ordinary
# operation (the survivor cleaning up after disaster recovery).  The
# steady-state xmax gate must resolve the OWN deleter through native
# snapshot semantics (alias-free for own xids) instead of failing
# closed -- without that, the first committed local UPDATE/DELETE of
# a remote row turns every later read of the table into a 53R97.
# ----------------------------------------------------------------
is($na->safe_psql('postgres',
		"UPDATE t_b SET v = 1101 WHERE v = 101;\n"
	  . 'SELECT v FROM t_b WHERE v = 1101'),
	'1101', 'L15 A updates a materialized remote row and reads the new version');
is($na->safe_psql('postgres', 'SELECT count(*) FROM t_b'), '100',
	'L15 the superseded remote version is invisible (own committed updater)');
is($na->safe_psql('postgres',
		"DELETE FROM t_b WHERE v = 200;\n"
	  . 'SELECT count(*) FROM t_b'),
	'99', 'L15 A deletes a materialized remote row; the row stays gone');

# ----------------------------------------------------------------
# L16 (P1 #1): the materialized foreign ITL slot pin.  t_slot's first
# page holds 8 COMMITTED B-origin ITL slots (filled by 8 separate B
# transactions in phase 2) with no FREE slot.  Those slots are the ONLY
# origin evidence for the live B tuples on the page -- a PG heap tuple's
# bare 32-bit xmin carries no origin.  A local write to that page needs
# an ITL slot; with the pin it must fail closed (ITL OVERFLOW) instead of
# recycling a foreign slot (old behaviour), which would strip a live B
# tuple's origin and alias its xid into A's CLOG.  Either way the 8 B
# rows must stay readable by B's authority.
# ----------------------------------------------------------------
is(query_retry($na, 'SELECT count(*) FROM t_slot'), '8',
	'L16 all 8 materialized B rows on the pinned page are visible');
my $ovf = query_fails($na, 'UPDATE t_slot SET v = v + 1000 WHERE v = 501');
ok(defined $ovf && $ovf =~ /ITL slot OVERFLOW/,
	'L16 a local write to the fully-pinned foreign page fails closed (ITL OVERFLOW), '
	  . 'never recycling a foreign slot');
is($na->safe_psql('postgres', 'SELECT count(*), sum(v) FROM t_slot'), '8|4036',
	'L16 after the refused write every B row still reads by B authority (origin intact)');
$pair->stop_pair;

done_testing();
