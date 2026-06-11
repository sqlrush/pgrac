#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 242_wal_thread_routing.pl
#    spec-4.1 -- per-thread WAL routing, single-node surface.
#
#      L1  routed node starts; dump keys: thread_id=4 / dir_configured=t
#          / dir_validated=t / claim_created=t (first boot claims)
#      L2  pg_waldump over an explicit [lsn0,lsn1) window (L227) shows
#          `thread: 4` on every record, never `thread: 0`
#      L3  page_stamp_count accumulator grows with WAL volume
#      L4  kill -9 + restart: own-stream strict crash recovery succeeds,
#          data intact, claim NOT re-created (claim_created=f)
#      L5  pg_wal re-linked at another thread's directory -> FATAL
#          (53RA0 routing mismatch), restore -> starts again
#      L6  claim corruption: magic flip -> "bad magic"; body flip ->
#          "bad crc"; both FATAL (53RA1), restore -> starts again
#      L7  cluster.enabled=off + wal_threads_dir set -> FATAL (53RA0
#          configuration contradiction)
#      L8  flat-layout node with identity (Q3-A orthogonality): no
#          wal_threads_dir, stamps thread 6, dir_configured=f
#      L9  RL1: physical standby replays the primary's stamped stream
#          (thread 4) without rejection -- local node_id never rejects
#          upstream WAL
#      L10 cluster.enabled=off control: stamps stay LEGACY (`thread: 0`),
#          page_stamp_count stays 0 (L29 silence)
#      L11 first-boot claim-create failure (read-only thread dir) ->
#          FATAL (53RA1); restore -> claim created.  [Exercised with a
#          real EACCES rather than the registered injection point: the
#          inject shmem arming cannot survive into a first boot.]
#      L12 pg_resetwal writes LEGACY pages; mixed legacy/real segments
#          recover and serve queries
#      L13 dump category self-enumeration: wal_thread has exactly 10 keys
#      L14 wait events registered: 2 ClusterWalThreadClaim* rows
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.1-per-thread-wal-routing.md (FROZEN v1.0)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use File::Path qw(make_path);
use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

sub dumpkey
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', qq{
		SELECT value FROM pg_cluster_state
		WHERE category='wal_thread' AND key='$key'});
}

sub wal_window_dump
{
	my ($node, $lsn0, $lsn1) = @_;
	# L227: explicit --start/--end window captured at quiesced points;
	# never let pg_waldump auto-position inside a live pg_wal.
	my ($out, $err) = run_command([
		$node->installed_command('pg_waldump'),
		'--path' => $node->data_dir . '/pg_wal',
		'--start' => $lsn0,
		'--end' => $lsn1 ]);
	return $out;
}

sub read_bytes
{
	my ($path) = @_;
	open my $fh, '<:raw', $path or die "open $path: $!";
	local $/;
	my $data = <$fh>;
	close $fh;
	return $data;
}

sub write_bytes
{
	my ($path, $data) = @_;
	open my $fh, '>:raw', $path or die "open $path: $!";
	print $fh $data;
	close $fh;
}

sub patch_byte
{
	my ($path, $off) = @_;
	open my $fh, '+<:raw', $path or die "open $path: $!";
	sysseek($fh, $off, 0) or die "seek: $!";
	sysread($fh, my $old, 1) or die "read: $!";
	sysseek($fh, $off, 0) or die "seek: $!";
	syswrite($fh, chr(ord($old) ^ 0xFF), 1) or die "write: $!";
	close $fh;
	return;
}

my $wroot = PostgreSQL::Test::Utils::tempdir();

# ============================================================
# L1: routed node (node_id=3 -> thread 4) starts + dump keys.
# ============================================================
my $node = PgracClusterNode->new('wal_thread_a');
$node->init(allows_streaming => 1, extra => [ '-X', "$wroot/thread_4" ]);
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 3\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n"
	  . "autovacuum = off\n");
$node->start;

is(dumpkey($node, 'thread_id'),      '4', 'L1 thread_id = node_id + 1');
is(dumpkey($node, 'dir_configured'), 't', 'L1 dir_configured');
is(dumpkey($node, 'dir_validated'),  't', 'L1 dir_validated');
is(dumpkey($node, 'claim_created'),  't', 'L1 first boot created the claim');
ok(-f "$wroot/thread_4/pgrac_thread.claim", 'L1 claim file exists in thread dir');

# ============================================================
# L2: windowed pg_waldump shows real thread id on every record.
# ============================================================
$node->safe_psql('postgres', 'CREATE TABLE t242 (id int primary key, v text)');
my $lsn0 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres',
	q{INSERT INTO t242 SELECT g, 'v'||g FROM generate_series(1, 500) g});
my $lsn1 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

my $wd = wal_window_dump($node, $lsn0, $lsn1);
like($wd, qr/thread: 4/, 'L2 records stamped with thread 4');
like($wd, qr/scn: \d+/, 'L2 waldump prints the spec-4.5 scn field');
unlike($wd, qr/thread: 0\b/, 'L2 no legacy-stamped records inside the window');

# ============================================================
# L3: page_stamp_count accumulator grows with WAL volume.
# ============================================================
my $stamp0 = dumpkey($node, 'page_stamp_count');
cmp_ok($stamp0, '>', 0, "L3 page_stamp_count > 0 after workload ($stamp0)");
$node->safe_psql('postgres',
	q{INSERT INTO t242 SELECT g, 'w'||g FROM generate_series(501, 2000) g});
my $stamp1 = dumpkey($node, 'page_stamp_count');
cmp_ok($stamp1, '>', $stamp0, "L3 page_stamp_count grows ($stamp0 -> $stamp1)");

# ============================================================
# L4: kill -9 + restart -- own-stream strict crash recovery.
# ============================================================
$node->stop('immediate');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM t242'), '2000',
	'L4 data intact after own-stream strict crash recovery');
is(dumpkey($node, 'dir_validated'), 't', 'L4 routing re-validated');
is(dumpkey($node, 'claim_created'), 'f', 'L4 claim read back, not re-created');

# ============================================================
# L5: pg_wal re-linked at a foreign thread directory -> FATAL 53RA0.
# ============================================================
$node->stop;
make_path("$wroot/thread_5");
my $pg_wal = $node->data_dir . '/pg_wal';
unlink($pg_wal) or die "unlink symlink $pg_wal: $!";
symlink("$wroot/thread_5", $pg_wal) or die "symlink: $!";

my $log_off = -s $node->logfile;
my $ret = $node->start(fail_ok => 1);
is($ret, 0, 'L5 start refused on mis-linked pg_wal');
my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/does not resolve to this node's WAL thread directory/,
	'L5 FATAL names the routing mismatch (53RA0)');

unlink($pg_wal) or die "unlink: $!";
symlink("$wroot/thread_4", $pg_wal) or die "symlink: $!";
$node->start;
is(dumpkey($node, 'dir_validated'), 't', 'L5 restored link validates again');

# ============================================================
# L6: claim corruption -> FATAL 53RA1 (magic, then crc).
# ============================================================
$node->stop;
my $claim_path = "$wroot/thread_4/pgrac_thread.claim";
my $claim_orig = read_bytes($claim_path);
is(length($claim_orig), 40, 'L6 claim file is the 40-byte v1 layout');

patch_byte($claim_path, 0);    # magic
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L6 start refused on corrupt claim (magic)');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/Claim validation failed: bad magic/, 'L6 errdetail: bad magic');

write_bytes($claim_path, $claim_orig);
patch_byte($claim_path, 16);    # created_at -> crc no longer matches
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L6 start refused on corrupt claim (crc)');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/Claim validation failed: bad crc/, 'L6 errdetail: bad crc');

write_bytes($claim_path, $claim_orig);
$node->start;
is(dumpkey($node, 'dir_validated'), 't', 'L6 restored claim validates again');

# ============================================================
# L7: cluster.enabled=off + wal_threads_dir set -> FATAL 53RA0.
# ============================================================
$node->stop;
$node->append_conf('postgresql.conf', "cluster.enabled = off\n");
$log_off = -s $node->logfile;
is($node->start(fail_ok => 1), 0, 'L7 start refused on contradictory config');
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile, $log_off);
like($log, qr/cluster\.wal_threads_dir is set but cluster\.enabled is off/,
	'L7 FATAL names the contradiction (53RA0)');
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->start;

# ============================================================
# L8: identity without layout (Q3-A orthogonality).
# ============================================================
my $flat = PgracClusterNode->new('wal_thread_flat');
$flat->init;
$flat->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 5\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n");
$flat->start;
is(dumpkey($flat, 'thread_id'),      '6', 'L8 flat node still has identity');
is(dumpkey($flat, 'dir_configured'), 'f', 'L8 no layout configured');
my $flsn0 = $flat->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$flat->safe_psql('postgres',
	q{CREATE TABLE tf AS SELECT g FROM generate_series(1, 500) g});
my $flsn1 = $flat->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
like(wal_window_dump($flat, $flsn0, $flsn1), qr/thread: 6/,
	'L8 flat pg_wal pages stamped with thread 6');

# ============================================================
# L9 (RL1): physical standby replays the primary's stamped stream.
# ============================================================
$node->backup('bk242');
my $standby = PgracClusterNode->new('wal_thread_standby');
$standby->init_from_backup($node, 'bk242', has_streaming => 1);
# The backup copies the primary's postgresql.conf: drop the routing (the
# backup's pg_wal is a plain directory) and take a different identity.
# RL1: replay must accept thread-4 pages regardless of local node_id.
$standby->append_conf('postgresql.conf',
	    "cluster.wal_threads_dir = ''\n"
	  . "cluster.node_id = 9\n");
$standby->start;
$node->safe_psql('postgres',
	q{INSERT INTO t242 VALUES (9001, 'standby-visible')});
$node->wait_for_catchup($standby);
is($standby->safe_psql('postgres',
		q{SELECT v FROM t242 WHERE id = 9001}),
	'standby-visible',
	'L9 standby replayed the primary thread-4 stream (RL1)');
is(dumpkey($standby, 'thread_id'), '10',
	'L9 standby own identity is independent of replayed stream');
# spec-4.3 gate: standby mode never generates a recovery plan (the
# plan pass shares the RL1 plain-local gate).
is($standby->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		WHERE category='recovery' AND key='plan_state'}),
	'none', 'L9 standby mode produces no recovery plan (spec-4.3 gate)');
$standby->stop;

# ============================================================
# L10: cluster.enabled=off control -- LEGACY stamps, silent counter.
# ============================================================
my $off = PgracClusterNode->new('wal_thread_off');
$off->init;
$off->append_conf('postgresql.conf', "cluster.enabled = off\n");
$off->start;
my $olsn0 = $off->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$off->safe_psql('postgres',
	q{CREATE TABLE toff AS SELECT g FROM generate_series(1, 500) g});
my $olsn1 = $off->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my $owd = wal_window_dump($off, $olsn0, $olsn1);
like($owd, qr/thread: 0\b/, 'L10 enabled=off stamps stay LEGACY');
unlike($owd, qr/thread: [1-9]/, 'L10 no real ids under enabled=off');
is(dumpkey($off, 'page_stamp_count'), '0', 'L10 stamp counter silent (L29)');
$off->stop;

# ============================================================
# L11: first-boot claim-create failure (EACCES) -> FATAL 53RA1.
# ============================================================
my $noclaim = PgracClusterNode->new('wal_thread_noclaim');
$noclaim->init(extra => [ '-X', "$wroot/thread_8" ]);
$noclaim->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 7\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.wal_threads_dir = '$wroot'\n");
chmod(0555, "$wroot/thread_8") or die "chmod: $!";
$log_off = -s $noclaim->logfile;
is($noclaim->start(fail_ok => 1), 0, 'L11 start refused when claim cannot be created');
$log = PostgreSQL::Test::Utils::slurp_file($noclaim->logfile, $log_off);
like($log, qr/could not create WAL thread claim file/,
	'L11 FATAL names the claim-create failure (53RA1)');
chmod(0755, "$wroot/thread_8") or die "chmod: $!";
$noclaim->start;
is(dumpkey($noclaim, 'claim_created'), 't', 'L11 claim created once writable');
$noclaim->stop;

# ============================================================
# L12: pg_resetwal writes LEGACY pages; mixed segments are legal.
# ============================================================
$node->stop;
PostgreSQL::Test::Utils::system_or_bail(
	$node->installed_command('pg_resetwal'), '-D', $node->data_dir);
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM t242'), '2001',
	'L12 queries served after pg_resetwal (mixed legacy/real stream)');
my $rlsn0 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$node->safe_psql('postgres', q{INSERT INTO t242 VALUES (9002, 'post-reset')});
my $rlsn1 = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
like(wal_window_dump($node, $rlsn0, $rlsn1), qr/thread: 4/,
	'L12 new pages stamp thread 4 again after pg_resetwal');

# ============================================================
# L13: dump category self-enumeration (L103 anchor).
# ============================================================
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category = 'wal_thread'}),
	'10', 'L13 wal_thread category has exactly 10 keys (spec-4.2 +5 registry)');

# ============================================================
# L14: wait events registered.
# ============================================================
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		  WHERE name IN ('ClusterWalThreadClaimRead', 'ClusterWalThreadClaimWrite')}),
	'2', 'L14 claim I/O wait events registered');

$node->stop;
$flat->stop;

done_testing();
