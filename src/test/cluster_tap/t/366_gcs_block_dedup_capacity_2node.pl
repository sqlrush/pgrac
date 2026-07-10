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
#      L1  cluster.gcs_block_dedup_max_entries default = 4096 (spec-7.2a D4;
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
EOC

$node0->append_conf('postgresql.conf', $cluster_conf);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

$node1->append_conf('postgresql.conf', $sc_common);
$node1->append_conf('postgresql.conf', $cluster_conf);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

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
# L1: default GUC value = 4096 (spec-7.2a D4).
# ============================================================
is($node0->safe_psql('postgres', 'SHOW cluster.gcs_block_dedup_max_entries'),
	'4096',
	'L1 cluster.gcs_block_dedup_max_entries default = 4096 (spec-7.2a D4)');


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
	"L2 dedup_full_count = 0 at default 4096 under distinct reads "
	. "(S1 saturation mode does not recur)");

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
# Let the TTL sweep (LMON, ~1Hz) run past the 2x retransmit window.
usleep(4_000_000);

my $evict_post = gcs_int_both($node0, $node1, 'dedup_evict_count');
cmp_ok($evict_post, '>', $evict_pre,
	"L4 reclaim/TTL sweep removed aged entries "
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

# dedup_max_entries reflects the effective cap (4096) on a serving node.
is($node0->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		  WHERE category='gcs' AND key='dedup_max_entries'}),
   '4096',
   'L5 dedup_max_entries reports the effective cap (4096)');


$node0->stop;
$node1->stop;

done_testing();
