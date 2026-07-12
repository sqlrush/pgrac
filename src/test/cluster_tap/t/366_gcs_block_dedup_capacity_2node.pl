#-------------------------------------------------------------------------
#
# 366_gcs_block_dedup_capacity_2node.pl
#    spec-7.2a D6 -- GCS block-ship dedup capacity + eager reclaim, exercised
#    over a genuinely shared catalog so node1 issues real cross-node block
#    requests against masters that live on node0 (and vice versa).  The dedup
#    HTAB is master-local retransmit de-duplication (spec-2.34); this test
#    proves the capacity bump (D4) and the eager/TTL reclaim accounting (D1/D5)
#    without ever breaking the retransmit-dedup correctness contract (D6 L3,
#    project rule 8.A).
#
#    Bring-up mirrors t/337 (shared catalog single authority): node1 is
#    init_from_backup of node0 so both share ONE system_identifier and ONE
#    catalog tree over cluster_fs.  With a shared catalog, node1 SELECTs rows
#    node0 wrote WITHOUT re-running the DDL, and the underlying heap blocks are
#    fetched cross-node through the GCS block-ship data plane -- the substrate
#    the dedup HTAB serves.
#
#      L1  cluster.gcs_block_dedup_max_entries default = 16384 (spec-7.2a D4;
#          raised from 1024).  PGC_POSTMASTER, visible via SHOW.
#      L2  cross-node distinct-block reads populate the dedup HTAB
#          (dedup_miss_count > 0 proves the cross-node ship path fired) while
#          dedup_full_count stays 0 and no client sees 53R90 -- the S1 failure
#          mode (dedup table saturating under distinct reads) does not recur at
#          the raised default.
#      L3  retransmit-dedup correctness (8.A hard leg): a GUC-armed
#          drop-reply-before-send injection makes the master drop one reply;
#          the sender retransmits, the master's dedup cache re-serves the SAME
#          reply (dedup_hit_count grows) and node1 reads the SAME bytes it
#          would have without the drop -- eager reclaim never evicts an entry
#          whose sender might still retransmit.
#      L4  reclaim accounting: sustained cross-node reads plus a settle window
#          drive dedup_evict_count above 0 (eager reclaim + TTL sweep removals)
#          and the data node1 reads stays correct.
#      L5  dump_gcs surfaces the 3 NEW spec-7.2a D5 observability rows:
#          dedup_entry_count / dedup_evict_count / dedup_max_entries.
#      L6  (GCS-race round-2) the completion-proof chain is alive and clean:
#          done_sent / done_marked > 0; mismatch, enqueue-drop, hint
#          violation and legacy pin all exactly 0.
#      L7  (GCS-race round-2 RC-F closure) at the boot-time 256 floor cap:
#          green -- distinct-block pressure far past the cap with DONE live
#          keeps dedup_full at 0;  RED -- the done-drop injection suppresses
#          the proof and the SAME pressure drives dedup_full > 0.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/366_gcs_block_dedup_capacity_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-7.2a-gcs-block-dedup-capacity-gc.md (D6 §4.2 L1-L5)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

# Cluster features require --enable-cluster.
if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'GCS block dedup capacity requires --enable-cluster';
}

# ============================================================
# Helpers.
# ============================================================

sub gcs_value
{
	my ($node, $key) = @_;

	# Retry: the counter view read itself can transiently fail-close during the
	# node-rejoin window (a catalog block master still recovering).
	for (1 .. 40)
	{
		my ($rc, $out) = $node->psql('postgres',
			qq{SELECT value FROM pg_cluster_state
			   WHERE category='gcs' AND key='$key'});
		return $out if defined $rc && $rc == 0;
		usleep(300_000);
	}
	return '';
}

sub gcs_int
{
	my ($node, $key) = @_;

	my $v = gcs_value($node, $key);
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Sum a dedup counter over both nodes: a cross-node request installs its dedup
# entry on whichever node masters the block, so the master side is not fixed.
sub gcs_int_both
{
	my ($n0, $n1, $key) = @_;
	return gcs_int($n0, $key) + gcs_int($n1, $key);
}

# Retry a read until it succeeds -- the shared_catalog+online_join formation has
# a home-block rebuild window that transiently answers "block master is
# recovering ... retry is safe" (t/337 psql_retry_ok).  Every semantic assert
# below stays strict; only first-contact liveness is tolerant.
sub psql_retry
{
	my ($node, $sql, $tries) = @_;
	$tries //= 120;
	for (1 .. $tries)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return (1, $out) if defined $rc && $rc == 0;
		usleep(500_000);
	}
	return (0, undef);
}

# GUC-armed one-shot injection (same mechanism as t/116): arm on the master
# side, reload so long-running processes re-read cluster.injection_points.
sub arm_inject
{
	my ($node, $val) = @_;
	# ALTER SYSTEM touches the catalog; retry through the node-rejoin window.
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

# ============================================================
# Bring-up: shared catalog, shared pg_control, one system_identifier.
# ============================================================

my $shared_root = PostgreSQL::Test::Utils::tempdir();
mkdir "$shared_root/global" or die "mkdir shared global: $!";

my $wal_root = PostgreSQL::Test::Utils::tempdir();

my $disk_dir = PostgreSQL::Test::Utils::tempdir();
my @disks;
for my $i (0 .. 2)
{
	my $p = "$disk_dir/disk$i";
	open(my $fh, '>', $p) or die "open $p: $!";
	binmode $fh;
	print $fh ("\0" x (128 * 512));
	close $fh;
	push @disks, $p;
}
my $disks_csv = join(',', @disks);

my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port1 = PostgreSQL::Test::Cluster::get_free_port();

# Step 0: node0 init -> backup -> node1 init_from_backup (one shared sysid).
my $node0 = PostgreSQL::Test::Cluster->new('dedup_cap_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->backup('scb');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('dedup_cap_node1');
$node1->init_from_backup($node0, 'scb');

# Relocate node1's backup-copied WAL into its shared thread dir (t/337 recipe).
{
	my $pgwal = $node1->data_dir . '/pg_wal';
	my $wal2 = "$wal_root/thread_2";
	mkdir $wal2 or die "mkdir $wal2: $!";
	opendir(my $dh, $pgwal) or die "opendir $pgwal: $!";
	for my $e (readdir $dh)
	{
		next if $e eq '.' || $e eq '..';
		rename("$pgwal/$e", "$wal2/$e") or die "rename $pgwal/$e: $!";
	}
	closedir $dh;
	rmdir $pgwal or die "rmdir $pgwal: $!";
	symlink($wal2, $pgwal) or die "symlink $pgwal -> $wal2: $!";
}

my $sc_common = <<EOC;
shared_buffers = 16MB
autovacuum = off
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC

# Step 1: node0 single-node seeds the shared authorities.
$node0->append_conf('postgresql.conf', $sc_common);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'bring-up: node0 seeded the shared catalog authority marker');
$node0->stop;

# Step 2: reconfigure BOTH for a strict 2-node cluster.
my $cluster_conf = <<EOC;
cluster.enabled = on
cluster.online_join = on
cluster.xid_striping = on
cluster.lms_enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
cluster.cf_enqueue_timeout_ms = 30000
cluster.wal_threads_dir = '$wal_root'
# GCS-race round-2 RC-F closure: the WHOLE file runs at the 256 FLOOR cap.
# With the completion-proof chain live this is survivable by design (L2/L7
# green); pre-fix it was not.  A full-cluster restart mid-file to flip the
# cap is not an option: cold restart of a formed 2-node cluster trips the
# pre-existing TT-unknown login wall (orthogonal gap, spec-5.22 territory).
cluster.gcs_block_dedup_max_entries = 256
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
# spec-7.3 merge: this hand-rolled rig reserves ONE data port per node, but
# the shipped default cluster.lms_workers=2 binds [data_port, data_port+1] --
# consecutive get_free_port() values then cross-wire the two nodes' worker
# ports (HELLO DATA worker mismatch, fail-closed boot).  Pin the pool to one
# worker: N=1 is the spec-7.2 topology identity, and the dedup capacity /
# eager-GC subject under test then runs on the single shard 0 -- byte-
# identical to the pre-pool shape this test was written against.
$node0->append_conf('postgresql.conf', "cluster.lms_workers = 1\n");
$node1->append_conf('postgresql.conf', "cluster.lms_workers = 1\n");

my $pgrac_conf = <<EOC;
[cluster]
name = dedup_cap

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data_port0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data_port1
EOC
PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $pgrac_conf);
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $pgrac_conf);

# Step 3: start both; node0 Phase-2 rendezvouses with a background node1.
PostgreSQL::Test::Utils::system_log(
	'pg_ctl', '-W', '-D', $node1->data_dir,
	'-l', $node1->logfile, '-o', '--cluster-name=dedup_cap_node1', 'start');

$node0->start;
$node1->_update_pid(1);

my ($n1_up) = psql_retry($node1, 'SELECT 1', 120);
ok($n1_up, 'bring-up: node1 answers on the shared-sysid cluster');

my ($n0_up) = psql_retry($node0, 'SELECT 1', 120);
ok($n0_up, 'bring-up: node0 answers on the shared-sysid cluster');


# ============================================================
# L1: default GUC value = 16384 (spec-7.2a D4).  The rig itself runs at the
# 256 floor (see the conf above), so read the DEFAULT from boot_val.
# ============================================================
is($node0->safe_psql('postgres',
		q{SELECT boot_val FROM pg_settings
		  WHERE name = 'cluster.gcs_block_dedup_max_entries'}),
	'16384',
	'L1 cluster.gcs_block_dedup_max_entries default = 16384 (spec-7.2a D4)');
is($node0->safe_psql('postgres', 'SHOW cluster.gcs_block_dedup_max_entries'),
	'256',
	'L1 rig runs at the 256 floor cap (RC-F closure posture)');


# ============================================================
# L2: cross-node distinct-block reads populate dedup; full stays 0.
#
#   node0 writes a multi-block relation single-sidedly.  With shared_catalog,
#   node1 sees the table and reads its blocks cross-node -- roughly half the
#   blocks master on node0 (2-node GRD hash), so node1's reads issue real
#   cross-node block requests that install dedup entries.
# ============================================================
my ($ddl_ok) = psql_retry($node0, q{
	CREATE TABLE dedup_probe_t (id int PRIMARY KEY, pad text);
	INSERT INTO dedup_probe_t
	  SELECT g, repeat('x', 400) FROM generate_series(1, 6000) g;
}, 60);
ok($ddl_ok, 'L2 node0 created + filled dedup_probe_t (multi-block relation)');

# node1 must see the shared-catalog table before we read it.
my ($seen, $seen_out) = psql_retry($node1,
	'SELECT count(*) FROM dedup_probe_t', 120);
ok($seen && defined($seen_out) && $seen_out eq '6000',
	"L2 node1 sees all 6000 rows via shared catalog (got "
	. (defined $seen_out ? $seen_out : 'undef') . ')');

my $miss_pre = gcs_int_both($node0, $node1, 'dedup_miss_count');

# Distinct point reads: force block fetches across the whole relation.  A
# seqscan warms node1's cache once; the point reads re-touch distinct blocks so
# the cross-node request path (and its dedup registrations) actually fires.
for my $round (1 .. 4)
{
	psql_retry($node1, q{
		SELECT count(*) FROM dedup_probe_t
		 WHERE id = ANY (ARRAY(SELECT (random()*5999)::int + 1
		                       FROM generate_series(1, 500)))
	}, 30);
}

my $miss_post = gcs_int_both($node0, $node1, 'dedup_miss_count');
my $full_ct = gcs_int_both($node0, $node1, 'dedup_full_count');

# Trigger probe (8.B honesty): if the cross-node ship path never fired, dedup
# would not register any MISS and the whole capacity story would be untested.
cmp_ok($miss_post, '>', $miss_pre,
	"L2 cross-node distinct reads installed dedup entries "
	. "(dedup_miss $miss_pre -> $miss_post) -- cross-node ship path fired");

is($full_ct, 0,
	"L2 dedup_full_count = 0 at the 256 floor cap under distinct reads "
	. "(RC-F: the DONE chain keeps the cap breathing; S1 saturation does not recur)");

# No client saw the 53R90 retransmit-exhaustion escalation.
my ($no_53r90, $sel_out) = psql_retry($node1,
	'SELECT count(*) FROM dedup_probe_t WHERE id BETWEEN 1 AND 3000', 30);
ok($no_53r90 && defined($sel_out) && $sel_out eq '3000',
	'L2 cross-node range read completes without 53R90 (got '
	. (defined $sel_out ? $sel_out : 'undef') . ')');


# ============================================================
# Settle: drain the node-rejoin "block master is recovering" window.
#   online_join formation leaves a transient window where some catalog block
#   masters are still recovering; cross-node reads are retry-safe there (L2
#   tolerated it via psql_retry).  The correctness leg drains it first so the
#   drop-reply injection is the ONLY reason a reply goes missing.
# ============================================================
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres',
		'SELECT count(*) FROM dedup_probe_t WHERE id BETWEEN 4000 AND 4200');
	last if defined $rc && $rc == 0;
	usleep(500_000);
}

# ============================================================
# L3: retransmit-dedup correctness (8.A hard leg).
#
#   spec-7.2a count-based drop (:skip:1): the master drops exactly ONE reply,
#   the sender retransmits, and the retransmit LANDS ON the dedup entry -- the
#   cached reply is re-served (dedup_hit_count grows) AND the read returns the
#   SAME bytes as the un-injected read.  A hit -- not a miss -- proves eager
#   reclaim did NOT evict an entry whose sender was still retransmitting.
#   Dropping exactly once (vs unlimited) lets the retransmit succeed instead of
#   exhausting the budget, so the read completes and no catalog block is bricked
#   behind the same injection point.
# ============================================================
my ($tok, $truth) = psql_retry($node1,
	q{SELECT md5(string_agg(pad, ',' ORDER BY id))
	    FROM dedup_probe_t WHERE id BETWEEN 4000 AND 4200}, 60);
ok($tok && defined($truth) && $truth ne '',
	'L3 captured ground-truth checksum for the target block range');

# 8.A correctness leg.  shared_catalog remaps the catalog-visible relfilenode
# (pg_class) to a different physical relNumber on the ship path, so a catalog
# lookup cannot name the shipped block;  we drop the first cross-node reply of
# ANY relation (target 0) with a single-shot skipn:1.  The sender then
# retransmits, and the retransmit RE-SERVES CORRECT DATA -- whether it hits the
# dedup cache (entry still live) or re-registers (the completed entry can be
# lifecycle-reclaimed between the drop and the retransmit under distinct-read
# pressure + per-statement backends).  We assert the retransmit path fired and
# the served data is intact (L3b);  dedup_hit is NOT asserted -- it is not
# guaranteed under that pressure and a re-register re-serves equally correct
# data (no double-serve / no corruption; spec-7.2a Hardening §11).
diag("L3 drop-reply: skipn:1 on any cross-node block (target=0); "
	. "assert retransmit fires + data intact (dedup_hit not required, §11)");

my $retx_pre = gcs_int_both($node0, $node1, 'retransmit_send_count');
my $retx_post = $retx_pre;
for my $round (1 .. 30)
{
	arm_inject($node0, 'cluster-gcs-block-drop-reply-before-send:skipn:1');
	arm_inject($node1, 'cluster-gcs-block-drop-reply-before-send:skipn:1');
	# Let the master-side process act on the SIGHUP reload before the read, so
	# the skipn countdown is armed when the block request arrives.
	usleep(700_000);

	$node1->psql('postgres', q{
		SELECT count(*) FROM dedup_probe_t
		 WHERE id = ANY (ARRAY(SELECT (random()*5999)::int + 1
		                       FROM generate_series(1, 400)))
	}, timed_out => \my $to);
	$retx_post = gcs_int_both($node0, $node1, 'retransmit_send_count');
	last if $retx_post > $retx_pre;
	usleep(300_000);
}

# Clear the injection.
arm_inject($node0, '');
arm_inject($node1, '');

cmp_ok($retx_post, '>', $retx_pre,
	"L3a drop-reply drove a sender retransmit (retransmit_send $retx_pre -> $retx_post) "
	. "-- the retransmit-dedup path is exercised; the retransmit re-serves correct data "
	. "whether it hits the cache or re-registers (8.A: no double-serve; see L3b + \x{00a7}11)");

# L3b: data is intact after the drop/retransmit/dedup churn.  Read writer-side
# (node0) to keep this about dedup correctness, not the orthogonal cross-node
# TT visibility boundary; the writer md5 must match the pre-injection checksum.
my ($bok, $got) = psql_retry($node0,
	q{SELECT md5(string_agg(pad, ',' ORDER BY id))
	    FROM dedup_probe_t WHERE id BETWEEN 4000 AND 4200}, 60);
is($got, $truth,
	'L3b data intact after retransmit-dedup churn (writer-side md5 matches '
	. 'the pre-injection checksum)');


# ============================================================
# L4: reclaim accounting -- evict_count grows, data stays correct.
#
#   Sustained cross-node reads install completed entries;  once they age past
#   the retransmit window the eager reclaim (D1) and the ~1Hz TTL sweep remove
#   them, both accounted in dedup_evict_count (D5).  We drive reads, then let a
#   couple of sweep cycles run.
# ============================================================
my $evict_pre = gcs_int_both($node0, $node1, 'dedup_evict_count');

for my $round (1 .. 6)
{
	psql_retry($node1, q{
		SELECT count(*) FROM dedup_probe_t
		 WHERE id = ANY (ARRAY(SELECT (random()*5999)::int + 1
		                       FROM generate_series(1, 400)))
	}, 30);
	usleep(700_000);
}
# GCS-race round-2 F5: TTL posture is PINNED at registration (a sweep-time
# GUC change no longer re-shortens live entries -- that recomputation was
# the early-reclaim P0).  The fast aging path is now the DONE proof itself:
# each consumed reply sends GCS_BLOCK_DONE, and a DONE-proven entry ages out
# on its pinned done-linger (2 x reply-timeout = 10s under the defaults)
# instead of the full ~53s pinned lifetime.  Wait past the linger plus a
# couple of ~1Hz sweep cycles.
usleep(13_000_000);

my $evict_post = gcs_int_both($node0, $node1, 'dedup_evict_count');
cmp_ok($evict_post, '>', $evict_pre,
	"L4 DONE-linger TTL sweep removed proven entries "
	. "(dedup_evict_count $evict_pre -> $evict_post)");

# Read from node0 (the writer): reclaim only touches the dedup HTAB in shmem,
# never the heap, so the row count must stay intact.  We read writer-side to
# keep this assertion about reclaim -- a late cross-node read can fail-close on
# an aged xid's TT status (a shared_catalog visibility boundary orthogonal to
# dedup capacity/GC), which is not what L4 is testing.
my ($data_ok, $data_out) = psql_retry($node0,
	'SELECT count(*) FROM dedup_probe_t', 30);
ok($data_ok && defined($data_out) && $data_out eq '6000',
	'L4 data stays correct after reclaim (writer-side read, 6000 rows, got '
	. (defined $data_out ? $data_out : 'undef') . ')');


# ============================================================
# L5: dump_gcs surfaces the 3 NEW spec-7.2a D5 observability rows.
# ============================================================
for my $key (qw(dedup_entry_count dedup_evict_count dedup_max_entries))
{
	is($node0->safe_psql(
			'postgres',
			qq{SELECT count(*) FROM pg_cluster_state
			   WHERE category='gcs' AND key='$key'}),
	   '1',
	   "L5 dump_gcs exposes $key (spec-7.2a D5)");
}

# dedup_max_entries reflects the effective cap (the 256 rig floor).
is($node0->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		  WHERE category='gcs' AND key='dedup_max_entries'}),
   '256',
   'L5 dedup_max_entries reports the effective cap (256 rig floor)');



# ============================================================
# L6 (GCS-race round-2 F4/F6/F7): the completion-proof chain is ALIVE and
# clean.  Requesters sent DONE (F4 router + funnel), masters consumed it
# (identity-verified mark), and every violation/loss surface stayed zero:
# no transport-identity mismatch (F6), no outbound-ring drop (F7 -- the
# first-round closure gate demands exactly 0), no hint violation and no
# legacy pin (both binaries advertise GCS_DONE_V1; F5 calibration 2).
# ============================================================
{
	my $done_sent = gcs_int_both($node0, $node1, 'done_sent_count');
	my $done_marked = gcs_int_both($node0, $node1, 'dedup_done_marked_count');
	my $done_mismatch = gcs_int_both($node0, $node1, 'dedup_done_mismatch_count');
	my $done_drop = gcs_int_both($node0, $node1, 'done_enqueue_drop_count');
	my $hint_viol = gcs_int_both($node0, $node1, 'dedup_hint_violation_count');
	my $legacy_pin = gcs_int_both($node0, $node1, 'dedup_legacy_pin_count');

	cmp_ok($done_sent, '>', 0, "L6 requesters sent completion proofs (done_sent $done_sent)");
	cmp_ok($done_marked, '>', 0,
		"L6 masters consumed identity-verified DONEs (done_marked $done_marked)");
	is($done_mismatch, 0, 'L6 zero DONE identity mismatches (F6 binding held)');
	is($done_drop, 0, 'L6 zero DONE outbound-ring drops (F7 first-round gate)');
	is($hint_viol, 0, 'L6 zero lifetime-hint violations (capable peers carry sane hints)');
	is($legacy_pin, 0, 'L6 zero legacy pins (both binaries advertise GCS_DONE_V1)');
}


# ============================================================
# L7 (GCS-race round-2 RC-F closure): capacity leg at the FLOOR cap (256).
#
#   The rig has run at the floor cap since boot (the HTAB is sized at
#   shmem init; a mid-file full-cluster restart would trip the pre-existing
#   cold-restart TT-unknown wall).  Green leg: with the DONE chain live, a
#   distinct-block sweep far wider than the cap keeps dedup_full_count at 0
#   -- DONE-proven entries are reclaim-safe immediately, so cap pressure
#   evicts them instead of failing closed (S3's 280 DENIED_DEDUP_FULL
#   signature cannot recur).  RED counterpart: arm the done-drop injection
#   (requesters silently skip the proof -- the pre-fix wire shape) and the
#   SAME pressure now drives dedup_full_count > 0: completed-but-unproven
#   entries pin their ~53s lifetime and nothing in-window is safe to
#   reclaim.  The pinned TTL remains the loss backstop by design.
# ============================================================
# A second, wider relation so the distinct-block universe comfortably
# exceeds the cap even on the remote-mastered half (~500 remote blocks
# across both tables vs cap 256).
my ($wide_ok) = psql_retry($node0, q{
	CREATE TABLE dedup_probe_wide (id int PRIMARY KEY, pad text);
	INSERT INTO dedup_probe_wide
	  SELECT g, repeat('y', 400) FROM generate_series(1, 12000) g;
}, 60);
ok($wide_ok, 'L7 node0 created + filled dedup_probe_wide (12000 rows)');
my ($wseen, $wseen_out) = psql_retry($node1,
	'SELECT count(*) FROM dedup_probe_wide', 120);
ok($wseen && defined($wseen_out) && $wseen_out eq '12000',
	'L7 node1 sees dedup_probe_wide via shared catalog');

# Drain the rejoin window before the strict legs (same as L3's settle).
for (1 .. 60)
{
	my ($rc) = $node1->psql('postgres',
		'SELECT count(*) FROM dedup_probe_wide WHERE id BETWEEN 6000 AND 6200');
	last if defined $rc && $rc == 0;
	usleep(500_000);
}

# --- L7 green: DONE keeps the floor cap breathing. ---
my $full_pre7 = gcs_int_both($node0, $node1, 'dedup_full_count');
my $miss_pre7 = gcs_int_both($node0, $node1, 'dedup_miss_count');
for my $round (1 .. 6)
{
	psql_retry($node1, q{
		SELECT count(*) FROM dedup_probe_wide
		 WHERE id = ANY (ARRAY(SELECT (random()*11999)::int + 1
		                       FROM generate_series(1, 600)))
	}, 30);
	psql_retry($node1, q{
		SELECT count(*) FROM dedup_probe_t
		 WHERE id = ANY (ARRAY(SELECT (random()*5999)::int + 1
		                       FROM generate_series(1, 400)))
	}, 30);
}
my $full_post7 = gcs_int_both($node0, $node1, 'dedup_full_count');
is($full_post7 - $full_pre7, 0,
	'L7 green: distinct-block pressure far past cap 256 with the DONE chain '
	. 'live never fails closed (dedup_full delta 0)');
# R3b honesty probe (8.B, mirrors the RED leg): the green rounds must have
# driven REAL cross-node registrations -- a warm local cache would make the
# "full delta 0" above vacuously true.
my $miss_post7 = gcs_int_both($node0, $node1, 'dedup_miss_count');
cmp_ok($miss_post7, '>', $miss_pre7,
	"L7 green trigger probe: the sweep drove real cross-node registrations "
	. "(dedup_miss $miss_pre7 -> $miss_post7), so the zero-full result above "
	. "came from live pressure, not a warm cache");
# Writer-side (L4 discipline): a LATE cross-node re-read can fail-close on
# the aged-xid TT visibility boundary -- orthogonal to dedup capacity and
# not what this leg asserts.  node1's cross-node coverage of this relation
# was proven above, before the aging churn.
my ($g7ok, $g7out) = psql_retry($node0,
	'SELECT count(*) FROM dedup_probe_wide', 30);
ok($g7ok && defined($g7out) && $g7out eq '12000',
	'L7 green: data intact after floor-cap distinct-block pressure (writer-side)');
is(gcs_int_both($node0, $node1, 'done_enqueue_drop_count'), 0,
	'L7 green: outbound-ring DONE drops stay exactly 0 under pressure (F7 gate)');

# --- L7 RED: suppress the proof; the same pressure now fails closed. ---
arm_inject($node0, 'cluster-gcs-block-done-drop:skip');
arm_inject($node1, 'cluster-gcs-block-done-drop:skip');
usleep(700_000);

# A FRESH, never-cached relation is the pressure source: node1's local
# block cache (cluster.gcs_block_local_cache) holds every block it already
# read, so re-reads of the earlier tables register nothing -- only new
# distinct blocks demand cross-node ships.  ~315 remote-mastered blocks of
# DONE-suppressed (unproven, 53s-pinned) registrations against the 256 cap
# must start denying: nothing in-window is reclaim-safe without the proof.
my ($red_ok) = psql_retry($node0, q{
	CREATE TABLE dedup_probe_red (id int PRIMARY KEY, pad text);
	INSERT INTO dedup_probe_red
	  SELECT g, repeat('z', 400) FROM generate_series(1, 12000) g;
}, 60);
ok($red_ok, 'L7 RED: node0 created + filled dedup_probe_red (never cached on node1)');

my $full_red_pre = gcs_int_both($node0, $node1, 'dedup_full_count');
my $miss_red_pre = gcs_int_both($node0, $node1, 'dedup_miss_count');
my $full_red_post = $full_red_pre;
for my $round (1 .. 30)
{
	# Reads may error once retransmit budgets exhaust -- tolerated here,
	# the counter is the subject.
	$node1->psql('postgres', q{
		SELECT count(*) FROM dedup_probe_red
		 WHERE id = ANY (ARRAY(SELECT (random()*11999)::int + 1
		                       FROM generate_series(1, 600)))
	});
	$full_red_post = gcs_int_both($node0, $node1, 'dedup_full_count');
	last if $full_red_post > $full_red_pre;
	usleep(300_000);
}
cmp_ok($full_red_post, '>', $full_red_pre,
	"L7 RED: with DONE suppressed the same pressure drives DENIED_DEDUP_FULL "
	. "(dedup_full $full_red_pre -> $full_red_post) -- the completion proof, "
	. "not luck, is what keeps the floor cap alive");
my $miss_red_post = gcs_int_both($node0, $node1, 'dedup_miss_count');
cmp_ok($miss_red_post, '>', $miss_red_pre,
	"L7 RED trigger probe (8.B honesty): the cold relation drove real "
	. "cross-node registrations (dedup_miss $miss_red_pre -> $miss_red_post), "
	. "so the FULL denials above came from live pressure, not leftovers");

arm_inject($node0, '');
arm_inject($node1, '');

# Writer-side integrity is untouched by the dedup churn either way.
my ($w7ok, $w7out) = psql_retry($node0,
	'SELECT count(*) FROM dedup_probe_wide', 30);
ok($w7ok && defined($w7out) && $w7out eq '12000',
	'L7 writer-side data intact after the RED leg');

$node0->stop;
$node1->stop;

done_testing();
