#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 244_wal_state_registry.pl
#    spec-4.2 -- ClusterWalState registry, single-node surface.
#
#      L1   first boot creates <root>/pgrac_wal_state (66048 bytes);
#           registry_ready=t; own slot reaches 'active' only after the
#           node serves (phase -> RUNNING publish)
#      L2   cluster_stats refresh advances registry_last_updated and
#           registry_highest_lsn while the node runs
#      L3   clean stop publishes STOPPED (verified via raw slot bytes
#           and via the dump key after restart)
#      L4   kill -9 leaves the slot ACTIVE (crash never writes STOPPED;
#           restart re-publishes ACTIVE with a new started_at)
#      L5   own-slot corruption self-heals: the next ACTIVE publish
#           overwrites the torn slot (owner repairs its own slot)
#      L6   header corruption -> startup FATAL 53RA2 (never rebuilt
#           automatically); restoring the header recovers
#      L7   chmod-based publish failure -> startup FATAL 53RA2 (the
#           registered injection point cannot be armed before first
#           boot; same real-fault pattern as t/242 L11)
#      L8   foreign/corrupt NEIGHBOUR slots never block this node and
#           surface as their own slots only (no cross-slot bleed)
#      L9   wal_threads_dir unset -> registry_ready=f, no file
#      L9b  startup failure before recovery does not publish ACTIVE: a
#           mis-linked pg_wal FATALs in the spec-4.1 claim validation
#           (pre-StartupXLOG) and the slot keeps its previous content.
#           NB: this does NOT exercise a mid-StartupXLOG failure; the
#           publish site (phase -> RUNNING) is after both.
#      L10  dump keys 10/10 under wal_thread; wait events 2/2
#      L11  own slot owned by a FOREIGN node_id (valid CRC) -> startup
#           FATAL 53RA2; the slot is never overwritten (round-2 P1)
#      L12  registry truncated to 512B -> startup FATAL 53RA2 (fixed
#           66048; never resized in place) (round-2 P1)
#      L2b  checkpoint redo publish also advances highest_lsn in the same
#           owner slot write, so readers never see redo > highest between
#           checkpoint and the next cluster_stats tick.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.2-wal-thread-metadata-catalog.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use File::Path qw(make_path);
use PgracClusterNode;
use PgracWalState qw(crc32c read_file_raw write_file_raw read_slot_raw patch_byte forge_slot_node_id);
use PostgreSQL::Test::Utils;
use Test::More;

sub dumpkey
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value FROM pg_cluster_state
		WHERE category='wal_thread' AND key='$key'});
}

my $wroot = PostgreSQL::Test::Utils::tempdir();
my $regfile = "$wroot/pgrac_wal_state";

my $node = PgracClusterNode->new('wal_state_a');
$node->init(extra => [ '-X', "$wroot/thread_4" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "cluster.cluster_stats_main_loop_interval = '500ms'\n"
	  . "autovacuum = off\n");
$node->start;

# ============================================================
# L1: registry created; ACTIVE published at the RUNNING transition.
# ============================================================
ok(-f $regfile, 'L1 registry file exists after first boot');
is(-s $regfile, 66048, 'L1 registry file is the fixed 66048 bytes');
is(dumpkey($node, 'registry_ready'), 't', 'L1 registry_ready');
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L1 own slot is ACTIVE once the node serves SQL');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{thread_id}, 4, 'L1 slot self-describes thread 4');
	is($slot->{node_id},   3, 'L1 slot records node_id 3');
	is($slot->{state},     1, 'L1 raw state == ACTIVE(1)');
}

# ============================================================
# L2: stats tick refreshes liveness stamp + watermarks.
# ============================================================
my $ts0  = dumpkey($node, 'registry_last_updated');
my $lsn0 = dumpkey($node, 'registry_highest_lsn');
$node->safe_psql('postgres',
	q{CREATE TABLE t244 AS SELECT g FROM generate_series(1, 2000) g});
my $deadline = time() + 15;
my ($ts1, $lsn1) = ($ts0, $lsn0);
while (time() < $deadline) {
	$ts1  = dumpkey($node, 'registry_last_updated');
	$lsn1 = dumpkey($node, 'registry_highest_lsn');
	last if $ts1 ne $ts0 && $lsn1 ne $lsn0;
	select(undef, undef, undef, 0.25);
}
cmp_ok($ts1, '>', $ts0, "L2 registry_last_updated advances ($ts0 -> $ts1)");
isnt($lsn1, $lsn0, 'L2 registry_highest_lsn advances with WAL volume');

# ============================================================
# L2b: checkpoint redo publish keeps the slot internally usable immediately.
# ============================================================
$node->safe_psql('postgres',
	q{CREATE TABLE t244_ckpt AS SELECT g FROM generate_series(1, 1000) g});
$node->safe_psql('postgres', q{CHECKPOINT});
{
	my $slot = read_slot_raw($regfile, 4);
	cmp_ok($slot->{checkpoint_redo_lsn}, '>', 0,
		'L2b checkpoint_redo_lsn published after CHECKPOINT');
	cmp_ok($slot->{highest_lsn}, '>', $slot->{checkpoint_redo_lsn},
		'L2b checkpoint publish leaves highest_lsn past checkpoint_redo_lsn');
}

# ============================================================
# L3: clean stop publishes STOPPED.
# ============================================================
$node->stop;    # fast = clean
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 2, 'L3 raw state == STOPPED(2) after clean stop');
}
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L3 restart republishes ACTIVE');

# ============================================================
# L4: crash leaves ACTIVE; restart re-publishes with new started_at.
# ============================================================
my $started_before = read_slot_raw($regfile, 4)->{started_at};
$node->stop('immediate');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 1, 'L4 immediate shutdown leaves the slot ACTIVE');
	is($slot->{started_at}, $started_before,
		'L4 crash did not rewrite the slot (same incarnation stamp)');
}
$node->start;
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 1, 'L4 restart publishes ACTIVE again');
	cmp_ok($slot->{started_at}, '>', $started_before,
		'L4 new incarnation has a newer started_at');
}

# ============================================================
# L5: own-slot corruption self-heals on the next publish.
# ============================================================
$node->stop;
# slot 4 starts at 512 + (4-1)*512 = 2048; flip one body byte -> bad CRC
patch_byte($regfile, 2048 + 41);
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L5 corrupt own slot overwritten by the ACTIVE publish (owner self-heal)');

# ============================================================
# L6: header corruption -> FATAL 53RA2, never auto-rebuilt.
# ============================================================
$node->stop;
my $hdr_orig;
{
	open my $fh, '<:raw', $regfile or die;
	sysread($fh, $hdr_orig, 512) == 512 or die;
	close $fh;
}
patch_byte($regfile, 0);    # magic
my $log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L6 start refused on corrupt registry header');
my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/WAL state registry .* failed validation/,
	'L6 FATAL names the registry validation failure (53RA2)');
{
	open my $fh, '+<:raw', $regfile or die;
	syswrite($fh, $hdr_orig, 512) == 512 or die;
	close $fh;
}
$node->start;
is(dumpkey($node, 'registry_ready'), 't', 'L6 restored header validates again');

# ============================================================
# L7: publish failure (read-only registry) -> FATAL 53RA2.
# ============================================================
$node->stop;
chmod(0444, $regfile) or die "chmod: $!";
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L7 start refused when ACTIVE publish cannot write');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/could not publish ACTIVE to the WAL state registry/,
	'L7 FATAL names the ACTIVE publish failure (53RA2)');
chmod(0644, $regfile) or die "chmod: $!";
$node->start;

# ============================================================
# L8: corrupt neighbour slots never block this node.
# ============================================================
$node->stop;
patch_byte($regfile, 512 + (9 - 1) * 512 + 4);    # slot 9 garbage
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L8 own slot unaffected by a corrupt neighbour slot');

# ============================================================
# L9: no wal_threads_dir -> no registry.
# ============================================================
my $flat = PgracClusterNode->new('wal_state_flat');
$flat->init;
$flat->append_conf('postgresql.conf',
	"cluster.enabled = on\ncluster.node_id = 5\ncluster.allow_single_node = on\n");
$flat->start;
is(dumpkey($flat, 'registry_ready'), 'f', 'L9 flat layout has no registry');
is(dumpkey($flat, 'registry_slot_state'), '-', 'L9 slot state placeholder');
$flat->stop;

# ============================================================
# L9b: pre-recovery startup failure does not publish ACTIVE.
# (The 4.1 claim validation FATALs before StartupXLOG; ACTIVE is
# published only at phase -> RUNNING, after recovery succeeded.)
# ============================================================
$node->stop;
my $stopped_before = read_slot_raw($regfile, 4);
is($stopped_before->{state}, 2, 'L9b precondition: clean STOPPED on disk');
make_path("$wroot/thread_9");
my $pg_wal = $node->data_dir . '/pg_wal';
unlink($pg_wal) or die "unlink: $!";
symlink("$wroot/thread_9", $pg_wal) or die "symlink: $!";
is($node->start(fail_ok => 1), 0, 'L9b mis-linked pg_wal still refused (4.1)');
{
	my $slot = read_slot_raw($regfile, 4);
	is($slot->{state}, 2,
		'L9b failed startup never published ACTIVE (slot keeps STOPPED)');
}
unlink($pg_wal) or die;
symlink("$wroot/thread_4", $pg_wal) or die;
$node->start;

# ============================================================
# L10: dump keys + wait events self-enumeration.
# ============================================================
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'wal_thread'}),
	'10', 'L10 wal_thread category has exactly 10 keys (spec-4.2 +5)');
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		  WHERE name IN ('ClusterWalStateRead', 'ClusterWalStateWrite')}),
	'2', 'L10 registry I/O wait events registered');

# ============================================================
# L11: valid own slot owned by a FOREIGN node_id -> startup refused;
# the slot is evidence and is never overwritten (round-2 P1).
# ============================================================
$node->stop;
my $full_image = read_file_raw($regfile);
is(length($full_image), 66048, 'L11 precondition: full registry image saved');
forge_slot_node_id($regfile, 4, 7);    # valid CRC, foreign owner
is(read_slot_raw($regfile, 4)->{node_id}, 7, 'L11 crafted slot says node 7');
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L11 start refused: own slot owned by another node');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/slot 4 is owned by node 7, but this node is 3/,
	'L11 FATAL names the foreign owner (53RA2)');
is(read_slot_raw($regfile, 4)->{node_id}, 7,
	'L11 foreign slot left untouched (evidence preserved)');
write_file_raw($regfile, $full_image);
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L11 restored registry starts and republishes ACTIVE');

# ============================================================
# L12: truncated registry -> startup refused (fixed 66048 bytes,
# never resized in place) (round-2 P1).
# ============================================================
$node->stop;
$full_image = read_file_raw($regfile);
truncate($regfile, 512) or die "truncate: $!";
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L12 start refused on truncated registry');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/unexpected size 512, expected 66048/,
	'L12 FATAL names the size mismatch (53RA2)');
is(-s $regfile, 512, 'L12 registry never auto-resized');
{
	open my $fh, '>:raw', $regfile or die;
	syswrite($fh, $full_image) == 66048 or die;
	close $fh;
}
$node->start;
is(dumpkey($node, 'registry_slot_state'), 'active',
	'L12 restored registry starts and republishes ACTIVE');

$node->stop;

done_testing();
