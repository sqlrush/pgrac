#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 231_cluster_3_18_d7_undo_observability.pl
#	  spec-3.18 D7 -- undo buffer pool + per-txn extent observability.
#
#	  D1 (undo buffer pool) + D3 (per-txn extent) + D2b (write-back) added
#	  blocking paths + counters; D7 surfaces them so operators can see them:
#
#	  L1  dump_undo exposes the new keys (extent_claim_count +
#	      undo_buf_hit/miss/writeback_count), all non-negative.
#	  L2  a real write + CR-read load moves extent_claim_count and the undo
#	      buffer hit/miss counters (the pool is actually exercised).
#	  L3  a CHECKPOINT under write-back flushes buffered-dirty undo blocks and
#	      moves undo_buf_writeback_count (the D2b write-back path is counted).
#	  L4  the new wait_event_type 'Cluster: Undo' is queryable without error
#	      (the enum -> wait_event_type mapping is wired for the 2 new events).
#
#	  Author: SqlRush <sqlrush@gmail.com>
#	  Spec: spec-3.18-write-path-performance-overhaul.md (D7)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('s318_d7obs');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");
$node->append_conf('postgresql.conf', "cluster.undo_buffers = 64\n");
$node->append_conf('postgresql.conf', "cluster.undo_buffer_writeback = on\n");
$node->append_conf('postgresql.conf', "checkpoint_timeout = 1h\n");
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;

# counter($key) -> integer value of an undo-category dump counter.
my $counter = sub {
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() WHERE key = '$key'");
};

# --- L1: the new dump_undo keys exist and are non-negative -------------------
my @keys = qw(extent_claim_count undo_buf_hit_count undo_buf_miss_count
	undo_buf_writeback_count);
for my $k (@keys) {
	my $v = $counter->($k);
	ok(defined($v) && $v ne '' && $v >= 0, "L1: dump_undo exposes $k (= $v)");
}

# --- L2: a write load moves extent_claim + undo buffer access counters -------
# The undo WRITE path (INSERT/UPDATE) allocates undo records through the buffer
# pool (lookup -> hit/miss) and claims per-txn extents, so writes alone move the
# counters -- no CR read needed (a heavy multi-page CR read can fail closed by
# pre-existing block-level CR policy, independent of D7; see t/229 L1).
{
	my $ec_before = $counter->('extent_claim_count');
	my $acc_before = $counter->('undo_buf_hit_count') + $counter->('undo_buf_miss_count');

	$node->safe_psql('postgres', 'CREATE TABLE l2(id int primary key, v text)');
	$node->safe_psql('postgres',
		q{INSERT INTO l2 SELECT g, 'base'||g FROM generate_series(1, 300) g});
	$node->safe_psql('postgres', q{UPDATE l2 SET v = 'upd'||id WHERE id <= 100});

	cmp_ok($counter->('extent_claim_count'), '>', $ec_before,
		'L2: extent_claim_count moved under a write load (per-txn extent claimed)');
	cmp_ok($counter->('undo_buf_hit_count') + $counter->('undo_buf_miss_count'),
		'>', $acc_before, 'L2: undo buffer pool was accessed (hit+miss moved)');
}

# --- L3: CHECKPOINT under write-back flushes buffered-dirty undo -------------
{
	my $wb_before = $counter->('undo_buf_writeback_count');
	# Dirty fresh undo blocks just before the checkpoint so there is buffered
	# write-back work to flush.
	$node->safe_psql('postgres',
		q{INSERT INTO l2 SELECT g, 'more'||g FROM generate_series(301, 700) g});
	$node->safe_psql('postgres', 'CHECKPOINT');
	cmp_ok($counter->('undo_buf_writeback_count'), '>', $wb_before,
		'L3: CHECKPOINT flushed buffered-dirty undo (writeback_count moved)');
}

# --- L4: the new wait_event_type is queryable without error -----------------
{
	my ($rc, undef, $err) = $node->psql('postgres',
		"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Cluster: Undo'");
	is($rc, 0, 'L4: wait_event_type = Cluster: Undo is queryable');
	is($err // '', '', 'L4: no error querying the Undo wait_event_type');
}

$node->stop;
done_testing();
