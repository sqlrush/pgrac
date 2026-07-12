#-------------------------------------------------------------------------
#
# 361_xid_authority_native_era_2node.pl
#    spec-6.15b BUG-A1/A2 -- shared_catalog native-era XID authority.
#
#    node1 is a cold pre-seed clone of node0: it has the same sysid and a
#    pre-seed pg_control/pg_xact horizon, but it does not contain node0's
#    cluster.enabled=off seed transactions.  node0 then seeds shared_catalog,
#    consumes native xids, cleanly shuts down, and publishes the sealed XID
#    authority plus pg_xact prehistory.  On first cluster boot node1 must use
#    its per-node recovery anchor's pre-seed nextXid as the adoption proof,
#    copy the native-era prehistory into local pg_xact before StartupCLOG, and
#    max-merge nextXid with the sealed authority before ShmemVariableCache is
#    seeded.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/361_xid_authority_native_era_2node.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use File::Copy ();
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if ($ENV{with_pgrac_cluster} && $ENV{with_pgrac_cluster} eq 'no')
{
	plan skip_all => 'shared catalog requires --enable-cluster';
}

sub make_shared_root
{
	my $root = PostgreSQL::Test::Utils::tempdir();
	mkdir "$root/global" or die "mkdir $root/global: $!";
	return $root;
}

sub make_voting_disks
{
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
	return join(',', @disks);
}

sub cold_clone_data_dir
{
	my ($src, $dst, $wal_root, $thread_no) = @_;

	PostgreSQL::Test::RecursiveCopy::copypath(
		$src->data_dir,
		$dst->data_dir,
		filterfn => sub {
			my $rel = shift;
			# With a shared WAL root, pg_wal is a symlink into the source's
			# thread dir (initdb -X); the clone gets its own thread below.
			return 0 if defined($wal_root) && $rel eq 'pg_wal';
			return ($rel ne 'log' && $rel ne 'postmaster.pid');
		});
	chmod(0700, $dst->data_dir) or die "chmod clone data dir: $!";

	if (defined $wal_root)
	{
		# spec-4.6a D10 recipe: the clone's WAL baseline is a copy of the
		# source thread's files, relocated into the clone's own shared
		# thread dir with pg_wal resolving there (the WAL thread validator
		# requires pg_wal -> thread_N).
		my $src_thread = readlink($src->data_dir . '/pg_wal')
		  // ($src->data_dir . '/pg_wal');
		my $dst_thread = "$wal_root/thread_$thread_no";
		my $copied = 0;
		mkdir $dst_thread or die "mkdir $dst_thread: $!";
		opendir(my $dh, $src_thread) or die "opendir $src_thread: $!";
		for my $e (readdir $dh)
		{
			# skip the source's thread-claim marker: the clone must claim
			# its own thread fresh (a foreign claim makes the WAL thread
			# machinery treat the dir as stale residue), matching the
			# pg_basebackup shape where pg_wal carries segments only.
			# File::Copy, NOT RecursiveCopy::copypath -- the latter
			# silently no-ops on single-FILE sources (its internal
			# "$src/" join defeats the -f test).
			next
			  if $e eq '.'
			  || $e eq '..'
			  || $e eq 'archive_status'
			  || $e eq 'pgrac_thread.claim';
			File::Copy::copy("$src_thread/$e", "$dst_thread/$e")
			  or die "copy $src_thread/$e: $!";
			$copied++ if -s "$dst_thread/$e";
		}
		closedir $dh;
		die "cold clone copied no WAL segments from $src_thread" if $copied == 0;
		mkdir "$dst_thread/archive_status";
		symlink($dst_thread, $dst->data_dir . '/pg_wal')
		  or die "symlink pg_wal -> $dst_thread: $!";
	}

	# The cold copy carries node0's port; append node1's port so the last
	# setting wins, matching init_from_backup's post-copy rewrite.
	$dst->append_conf('postgresql.conf', 'port = ' . $dst->port . "\n");
}

sub append_pgrac_conf
{
	my ($node, $name, $ic0, $ic1, $data0, $data1) = @_;
	# spec-7.2: multi-node clusters must declare the LMS-owned DATA plane.
	$data0 //= PostgreSQL::Test::Cluster::get_free_port();
	$data1 //= PostgreSQL::Test::Cluster::get_free_port();
	my $pgrac_conf = <<EOC;
[cluster]
name = $name

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data1
EOC
	PostgreSQL::Test::Utils::append_to_file($node->data_dir . '/pgrac.conf', $pgrac_conf);
}

sub start_background
{
	my ($node) = @_;
	PostgreSQL::Test::Utils::system_log(
		'pg_ctl', '-W', '-D', $node->data_dir,
		'-l', $node->logfile, '-o', '--cluster-name=' . $node->name, 'start');
}

sub wait_sql_eq
{
	my ($node, $sql, $want, $name, $attempts) = @_;
	$attempts //= 80;
	my $got = '';

	for (1 .. $attempts)
	{
		# eval: some IPC::Run versions die with 'ack Broken pipe' when the
		# server drops the connection mid-boot (cluster phases still
		# starting); treat that as one more retryable non-answer.
		my ($rc, $out);
		eval { ($rc, $out) = $node->psql('postgres', $sql); };
		if (!$@ && defined $rc && $rc == 0 && defined $out)
		{
			$got = $out;
			last if $got eq $want;
		}
		usleep(250_000);
	}
	is($got, $want, $name);
}

sub read_xid_authority
{
	my ($shared_root) = @_;
	my $path = "$shared_root/global/pgrac_xid_authority";
	open(my $fh, '<:raw', $path) or die "open $path: $!";
	read($fh, my $buf, 40) or die "read $path: $!";
	close $fh;
	my ($magic, $version, $flags, $reserved, $native_hw, $next_multi, $crc) =
	  unpack('L< L< L< L< Q< Q< L<', $buf);
	return {
		magic => $magic,
		version => $version,
		flags => $flags,
		native_hw => $native_hw,
		next_multi => $next_multi,
		crc => $crc,
	};
}

sub corrupt_file
{
	my ($path) = @_;
	open(my $fh, '>:raw', $path) or die "open $path: $!";
	print $fh "not-a-valid-xid-authority";
	close $fh;
}

sub append_common_shared_catalog_conf
{
	my ($node, $shared_root) = @_;
	$node->append_conf('postgresql.conf', <<EOC);
shared_buffers = 16MB
autovacuum = off
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared_root'
cluster.smgr_user_relations = on
cluster.controlfile_shared_authority = on
cluster.shared_catalog = on
cluster.merged_recovery = on
EOC
}

sub append_strict_two_node_conf
{
	my ($node, $disks_csv, $wal_root) = @_;
	my $wal_line = defined($wal_root) ? "cluster.wal_threads_dir = '$wal_root'\n" : '';
	$node->append_conf('postgresql.conf', <<EOC);
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
cluster.relation_extend_lock_enabled = on
cluster.lms_workers = 1
$wal_line
EOC
}

# -------------------------------------------------------------------------
# D6 negative: shared_catalog multi-node startup refuses xid_striping=off.
# -------------------------------------------------------------------------
{
	my $shared_root = make_shared_root();
	my $disks_csv = make_voting_disks();
	my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $n = PostgreSQL::Test::Cluster->new('sxid_no_stripe');

	$n->init;
	append_common_shared_catalog_conf($n, $shared_root);
	$n->append_conf('postgresql.conf', <<EOC);
cluster.enabled = on
cluster.online_join = on
cluster.lms_enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.node_id = 0
EOC
	append_pgrac_conf($n, 'sxid_no_stripe', $ic0, $ic1);

	my $start_failed = !$n->start(fail_ok => 1);
	ok($start_failed, 'D6: shared_catalog multi-node refuses cluster.xid_striping=off');
	like(PostgreSQL::Test::Utils::slurp_file($n->logfile),
		qr/requires cluster\.xid_striping/,
		'D6: startup log names the xid_striping requirement');
}

# -------------------------------------------------------------------------
# Main green path: pre-seed clone adopts node0 native-era pg_xact truth.
# -------------------------------------------------------------------------
my $shared_root = make_shared_root();
my $disks_csv = make_voting_disks();
my $wal_root = PostgreSQL::Test::Utils::tempdir();
my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();

my $node0 = PostgreSQL::Test::Cluster->new('sxid_node0');
$node0->init(allows_streaming => 1, extra => [ '-X', "$wal_root/thread_1" ]);
$node0->start;
$node0->safe_psql('postgres', 'CHECKPOINT');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sxid_node1');
cold_clone_data_dir($node0, $node1, $wal_root, 2);
# F3: real scram TCP login leg needs a host auth line AND a TCP listener on
# the joiner (test nodes default to unix sockets only).
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pg_hba.conf',
	"host all sxid_login 127.0.0.1/32 scram-sha-256\n");
$node1->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'\n");

append_common_shared_catalog_conf($node0, $shared_root);
# relation_extend_lock off during the seed: the gate otherwise fail-closes
# because CSSD is not ready under enabled=off (mac-harness.sh seed posture);
# append_strict_two_node_conf restores the default at formation.
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
cluster.relation_extend_lock_enabled = off
EOC

$node0->start;
ok(-e "$shared_root/global/pgrac_catalog_authority",
	'G1: node0 seeded the shared catalog authority marker');
ok(-e "$shared_root/global/pg_control",
	'G1: node0 seeded the shared pg_control authority');
ok(-e "$shared_root/global/pgrac_xid_authority",
	'G1: node0 seeded the shared XID authority before native-era load');

my $abort_xid = $node0->safe_psql('postgres', q{
BEGIN;
SELECT txid_current();
CREATE SCHEMA sxid_abort_shadow;
ROLLBACK;
});

$node0->safe_psql('postgres', q{
CREATE SCHEMA sxid;
CREATE ROLE sxid_login LOGIN PASSWORD 'sxidpass';
});

my $seed_schema_xid = $node0->safe_psql('postgres',
	q{SELECT xmin::text FROM pg_namespace WHERE nspname = 'sxid'});
my $seed_role_xid = $node0->safe_psql('postgres',
	q{SELECT xmin::text FROM pg_authid WHERE rolname = 'sxid_login'});

# G7 prep (GCS-race round-2 RC-E): a native-era user table.  Its rows carry
# a native xmin and NO ITL binding; after formation a cluster-era UPDATE
# rebinds the old versions' t_itl_slot_idx to the deleter's ITL slot
# (heap_update "last writer of this version"), so a peer resolving the raw
# native xmin reaches classify_ref with a mismatching ref -- the exact S3
# pg_statistic shape (raw xmin below the stripe floor, recycled-looking ref).
$node0->safe_psql('postgres', q{
CREATE TABLE sxid.reuse (k int PRIMARY KEY, v int);
INSERT INTO sxid.reuse SELECT g, 0 FROM generate_series(1, 20) g;
});
my $native_row_xid = $node0->safe_psql('postgres',
	q{SELECT max((xmin::text)::bigint)::text FROM sxid.reuse});

is($node0->safe_psql('postgres', "SELECT txid_status($seed_schema_xid)"),
	'committed', 'G1: seed schema catalog xid is committed on the seed');
is($node0->safe_psql('postgres', "SELECT txid_status($abort_xid)"),
	'aborted', 'G1: native-era aborted xid is aborted on the seed');

$node0->stop;

my $auth = read_xid_authority($shared_root);
ok(($auth->{flags} & 0x0001) != 0, 'G2: clean seed shutdown sealed the XID authority');
cmp_ok($auth->{native_hw}, '>', $seed_schema_xid,
	'G2: native high-water is above the seed schema catalog xid');
cmp_ok($auth->{native_hw}, '>', $seed_role_xid,
	'G2: native high-water is above the seed role xid');
ok(-e "$shared_root/global/pgrac_xid_prehistory",
	'G2: clean seed shutdown published XID prehistory');

append_strict_two_node_conf($node0, $disks_csv, $wal_root);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

append_common_shared_catalog_conf($node1, $shared_root);
append_strict_two_node_conf($node1, $disks_csv, $wal_root);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

my $data0 = PostgreSQL::Test::Cluster::get_free_port();
my $data1 = PostgreSQL::Test::Cluster::get_free_port();
append_pgrac_conf($node0, 'sxid', $ic0, $ic1, $data0, $data1);
append_pgrac_conf($node1, 'sxid', $ic0, $ic1, $data0, $data1);

start_background($node1);
$node0->start;
$node1->_update_pid(1);

wait_sql_eq($node1, 'SELECT 1', '1', 'G3: node1 is up after XID prehistory adopt');
wait_sql_eq($node0,
	"SELECT COALESCE(bool_or(heartbeat_recv_count > 0), false) "
	. "FROM pg_cluster_ic_peers WHERE node_id = 1",
	't', 'G3: node0 sees node1 heartbeats');

wait_sql_eq($node1,
	q{SELECT count(*) FROM pg_namespace WHERE nspname = 'sxid'},
	'1', 'G4: node1 sees the native-era seed schema catalog row');
wait_sql_eq($node1,
	q{SELECT count(*) FROM pg_authid WHERE rolname = 'sxid_login'},
	'1', 'G4: node1 sees the seed role catalog row');
wait_sql_eq($node1,
	q{SET ROLE sxid_login; SELECT current_user},
	'sxid_login', 'G4: node1 can resolve and SET ROLE to the seed role');
# F3: the seed role's scram password must survive adoption -- a real TCP
# login exercises rolpassword visibility at connection time.
$node1->connect_ok(
	'host=127.0.0.1 port=' . $node1->port
	  . ' user=sxid_login password=sxidpass dbname=postgres',
	'G4: seed role logs in over scram TCP on node1');

wait_sql_eq($node1, "SELECT txid_status($seed_schema_xid)", 'committed',
	'G5: node1 txid_status sees the native-era committed schema xid');
wait_sql_eq($node1, "SELECT txid_status($seed_role_xid)", 'committed',
	'G5: node1 txid_status sees the native-era committed role xid');
wait_sql_eq($node1, "SELECT txid_status($abort_xid)", 'aborted',
	'G5: node1 txid_status sees the native-era aborted xid');
is($node0->safe_psql('postgres', "SELECT txid_status($abort_xid)"),
	'aborted', 'G5: node0 still sees the aborted xid after node1 reads');
# F4: the poison-stamp proof must re-read the shared HEAP rows on node0 --
# a wrong HEAP_XMIN_INVALID hint written by node1 lives in the shared
# catalog block, not in node0's CLOG.
is($node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_namespace WHERE nspname = 'sxid'}),
	'1', 'G5: node0 still sees the seed schema row after node1 scans (no poison stamp)');
is($node0->safe_psql('postgres',
	q{SELECT count(*) FROM pg_authid WHERE rolname = 'sxid_login'}),
	'1', 'G5: node0 still sees the seed role row after node1 scans (no poison stamp)');

my $state_hw = $node1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_authority_native_hw'});
my $state_sealed = $node1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_authority_sealed'});
my $state_adopted = $node1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_prehistory_adopted'});

is($state_hw, "$auth->{native_hw}", 'G6: pg_cluster_state exposes authority native high-water');
is($state_sealed, 't', 'G6: pg_cluster_state exposes sealed XID authority');
is($state_adopted, 't', 'G6: pg_cluster_state exposes node1 prehistory adoption');
like(PostgreSQL::Test::Utils::slurp_file($node1->logfile),
	qr/adopted XID prehistory through native high-water/,
	'G6: node1 log records XID prehistory adoption');
like(PostgreSQL::Test::Utils::slurp_file($node1->logfile),
	qr/merged nextXid with XID authority native high-water/,
	'G6: node1 log records StartupXLOG nextXid max-merge');

# G6b (round-2): the joiner must have LATCHED verified native prehistory
# coverage after recovery (the resolver's LOCAL-routing precondition).
my $state_covered = $node1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_native_prehistory_covered_hw'});
is($state_covered, "$auth->{native_hw}",
	'G6b: node1 latched verified native prehistory coverage');

# -------------------------------------------------------------------------
# G7 (round-2 RC-E): cluster-era ITL reuse over native rows.  node0 UPDATEs
# the native-era table, stamping the old versions' ITL slot idx with the
# deleter's slot; node1's scan then resolves the raw native xmin through a
# mismatching ref and must route it to the adopted local CLOG (LOCAL
# evidence) instead of failing closed "TT slot recycled".
# -------------------------------------------------------------------------
$node0->safe_psql('postgres', q{UPDATE sxid.reuse SET v = v + 1});
my ($g7_rc, $g7_out, $g7_err) = $node1->psql('postgres', q{
SET cluster.crossnode_runtime_visibility = on;
SELECT count(*) FROM sxid.reuse});
is($g7_rc, 0, 'G7: node1 scan of the ITL-reused native page succeeds');
is($g7_out, '20', 'G7: node1 sees every committed native row after ITL reuse');
unlike(($g7_err // ''), qr/TT slot recycled/,
	'G7: no recycled-slot fail-closed for a provably native xid');
my $g7_hits = $node1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'cr' AND key = 'rtvis_native_prehistory_local_count'});
ok(($g7_hits // '') =~ /^\d+$/ && $g7_hits > 0,
	'G7: native-prehistory LOCAL routing counter moved');

# -------------------------------------------------------------------------
# G8 (round-2): solo joiner restart.  The second boot has no backup_label
# and an own nextXid >= native_hw, so coverage must re-latch through the
# post-recovery verify (prefix consistency), not through adopt.
# -------------------------------------------------------------------------
$node1->stop;
# node0 is live, so the rejoin rendezvous is immediate: a plain blocking
# start suffices (no background-start + pidfile race).
$node1->start;
wait_sql_eq($node1, 'SELECT 1', '1', 'G8: node1 rejoined after solo restart');
wait_sql_eq($node1,
	q{SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_native_prehistory_covered_hw'},
	"$auth->{native_hw}", 'G8: coverage latch re-established on restart');
my ($g8_rc, $g8_out, $g8_err) = $node1->psql('postgres', q{
SET cluster.crossnode_runtime_visibility = on;
SELECT count(*) FROM sxid.reuse});
is($g8_out, '20', 'G8: native rows stay resolvable after joiner restart');

$node1->stop;
$node0->stop;

# -------------------------------------------------------------------------
# Negative lifecycle legs on the formed shared tree.
# -------------------------------------------------------------------------
$node0->append_conf('postgresql.conf', <<EOC);
# t/361 era re-entry negative: last setting wins.
cluster.enabled = off
cluster.lms_enabled = off
EOC
my $reentry_failed = !$node0->start(fail_ok => 1);
ok($reentry_failed, 'N1: cluster.enabled=off cannot re-enter native seed era after formation');
like(PostgreSQL::Test::Utils::slurp_file($node0->logfile),
	qr/native seed era cannot be re-entered/,
	'N1: startup log names native-era re-entry refusal');

corrupt_file("$shared_root/global/pgrac_xid_authority");
corrupt_file("$shared_root/global/pgrac_xid_authority.bak");
# F9: pin the 53RB5 SQLSTATE contract on one leg (startup FATALs surface in
# the server log only, so %e in log_line_prefix carries the SQLSTATE; the
# other negative legs deliberately match message text).
# %e must precede %q: the refusal FATAL comes from the postmaster (a
# non-session process), and %q stops expansion there.
$node1->append_conf('postgresql.conf',
	"log_line_prefix = '%m [%p] %e %q%a '\n");
my $corrupt_failed = !$node1->start(fail_ok => 1);
ok($corrupt_failed, 'N2: corrupt shared XID authority fails closed');
like(PostgreSQL::Test::Utils::slurp_file($node1->logfile),
	qr/shared XID authority is unavailable|present but corrupt|cluster_xid_authority_unavailable/,
	'N2: startup log names corrupt/unavailable XID authority');
like(PostgreSQL::Test::Utils::slurp_file($node1->logfile),
	qr/53RB5/, 'N2: the refusal carries SQLSTATE 53RB5');

# -------------------------------------------------------------------------
# Unsealed authority negative: seed startup wrote the authority, but the seed
# crashed before clean shutdown could publish prehistory and seal it.
# -------------------------------------------------------------------------
{
	my $u_shared = make_shared_root();
	my $u_disks = make_voting_disks();
	my $u_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $u_ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $u0 = PostgreSQL::Test::Cluster->new('sxid_unsealed_node0');
	my $u1 = PostgreSQL::Test::Cluster->new('sxid_unsealed_node1');

	$u0->init(allows_streaming => 1);
	$u0->start;
	$u0->safe_psql('postgres', 'CHECKPOINT');
	$u0->stop;
	cold_clone_data_dir($u0, $u1);

	append_common_shared_catalog_conf($u0, $u_shared);
	$u0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC
	$u0->start;
	ok(-e "$u_shared/global/pgrac_xid_authority",
		'N3: unsealed fixture created the authority during seed startup');
	$u0->stop('immediate');

	append_common_shared_catalog_conf($u1, $u_shared);
	append_strict_two_node_conf($u1, $u_disks, undef);
	$u1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
	append_pgrac_conf($u1, 'sxid_unsealed', $u_ic0, $u_ic1);

	my $unsealed_failed = !$u1->start(fail_ok => 1);
	ok($unsealed_failed, 'N3: joiner refuses an unsealed XID authority');
	like(PostgreSQL::Test::Utils::slurp_file($u1->logfile),
		qr/shared XID authority is not sealed|shared XID authority is unavailable/,
		'N3: startup log names the unsealed authority');
}

# -------------------------------------------------------------------------
# Multi-pass seed negative: a SECOND native-era run unseals the authority at
# boot, so crashing that run leaves joiners fail-closed instead of silently
# adopting the first pass's stale high-water (spec-6.15b §3.1 multi-pass arm).
# -------------------------------------------------------------------------
{
	my $m_shared = make_shared_root();
	my $m_disks = make_voting_disks();
	my $m_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $m_ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $m0 = PostgreSQL::Test::Cluster->new('sxid_multipass_node0');
	my $m1 = PostgreSQL::Test::Cluster->new('sxid_multipass_node1');

	$m0->init(allows_streaming => 1);
	$m0->start;
	$m0->safe_psql('postgres', 'CHECKPOINT');
	$m0->stop;
	cold_clone_data_dir($m0, $m1);

	append_common_shared_catalog_conf($m0, $m_shared);
	$m0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC

	# pass 1: consume native xids (catalog-only DDL; a native-era relation
	# extend would trip the extend-lock gate), shut down cleanly -> sealed
	$m0->start;
	$m0->safe_psql('postgres', 'CREATE SCHEMA sxid_pass1;');
	$m0->stop;
	my $m_auth = read_xid_authority($m_shared);
	ok(($m_auth->{flags} & 0x0001) != 0,
		'N4: first native pass sealed the XID authority');

	# pass 2: booting a follow-up native run re-opens (unseals) the era
	$m0->start;
	$m_auth = read_xid_authority($m_shared);
	ok(($m_auth->{flags} & 0x0001) == 0,
		'N4: follow-up native pass unsealed the XID authority at boot');
	like(PostgreSQL::Test::Utils::slurp_file($m0->logfile),
		qr/re-opened native seed era/,
		'N4: seed log names the re-opened native era');

	# crash pass 2: the stale pass-1 high-water must NOT become adoptable
	$m0->stop('immediate');
	append_common_shared_catalog_conf($m1, $m_shared);
	append_strict_two_node_conf($m1, $m_disks, undef);
	$m1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
	append_pgrac_conf($m1, 'sxid_multipass', $m_ic0, $m_ic1);

	my $multipass_failed = !$m1->start(fail_ok => 1);
	ok($multipass_failed,
		'N4: joiner refuses adoption after a crashed follow-up native pass');
	like(PostgreSQL::Test::Utils::slurp_file($m1->logfile),
		qr/shared XID authority is not sealed|shared XID authority is unavailable/,
		'N4: startup log fails closed on the unsealed authority');
}

# -------------------------------------------------------------------------
# Divergent-lineage negative (review F2 / spec Q6 amendment): a same-sysid
# clone that ran STANDALONE after cloning holds its own outcomes in the
# native xid range; neither skipping nor adopting is sound -> FATAL 53RB5.
# -------------------------------------------------------------------------
{
	my $d_shared = make_shared_root();
	my $d_disks = make_voting_disks();
	my $d_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $d_ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $d0 = PostgreSQL::Test::Cluster->new('sxid_diverge_node0');
	my $d1 = PostgreSQL::Test::Cluster->new('sxid_diverge_node1');

	$d0->init(allows_streaming => 1);
	$d0->start;
	$d0->safe_psql('postgres', 'CHECKPOINT');
	$d0->stop;
	cold_clone_data_dir($d0, $d1);

	# d1 diverges: a plain standalone run committing 30 transactions writes
	# d1's own outcomes into the xid range the seed is about to consume.
	$d1->start;
	$d1->safe_psql('postgres',
		join('', map { "BEGIN; CREATE TABLE d_t$_ (i int); COMMIT;\n" } 1 .. 30));
	$d1->stop;

	# d0 seeds: one aborted xid guarantees a byte-level contradiction with
	# d1's straight-committed overlap.
	append_common_shared_catalog_conf($d0, $d_shared);
	$d0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
EOC
	$d0->start;
	$d0->safe_psql('postgres', q{
BEGIN;
CREATE SCHEMA d_abort_shadow;
ROLLBACK;
});
	$d0->safe_psql('postgres', 'CREATE SCHEMA d_seed;');
	$d0->stop;

	append_common_shared_catalog_conf($d1, $d_shared);
	append_strict_two_node_conf($d1, $d_disks, undef);
	$d1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
	append_pgrac_conf($d1, 'sxid_diverge', $d_ic0, $d_ic1);

	my $diverge_failed = !$d1->start(fail_ok => 1);
	ok($diverge_failed, 'N5: divergent-lineage joiner is refused');
	like(PostgreSQL::Test::Utils::slurp_file($d1->logfile),
		qr/divergent native-era lineage/,
		'N5: startup log names the divergent lineage');
}

# -------------------------------------------------------------------------
# L9 dormant leg: with cluster.shared_catalog=off nothing of the XID
# authority machinery may touch the shared tree.
# -------------------------------------------------------------------------
{
	my $o_shared = make_shared_root();
	my $o0 = PostgreSQL::Test::Cluster->new('sxid_off_node0');

	$o0->init(allows_streaming => 1);
	$o0->append_conf('postgresql.conf', <<EOC);
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$o_shared'
cluster.enabled = off
cluster.lms_enabled = off
EOC
	$o0->start;
	$o0->safe_psql('postgres', 'CREATE SCHEMA off_mode_probe;');
	$o0->stop;
	ok(!-e "$o_shared/global/pgrac_xid_authority",
		'L9: shared_catalog=off never creates the XID authority');
	ok(!-e "$o_shared/global/pgrac_xid_prehistory",
		'L9: shared_catalog=off never creates the XID prehistory');
}

# -------------------------------------------------------------------------
# B legs (GCS-race round-2 RC-E): pg_basebackup PRE-SEED joiner.  This is
# the RACvsRAC S3 bring-up shape (mac-harness.sh): the joiner is cloned
# with pg_basebackup BEFORE the cluster.enabled=off seed runs, so its
# backup_label first cluster boot must ADOPT the sealed native prehistory
# (a never-booted clone's lineage is a subset of the seed lineage by
# construction) -- not skip it.  Unlike the RecursiveCopy main path above,
# the clone carries a real backup_label, which is exactly the leg the old
# unconditional skip left uncovered.
# -------------------------------------------------------------------------
{
	my $b_shared = make_shared_root();
	my $b_disks = make_voting_disks();
	my $b_wal_root = PostgreSQL::Test::Utils::tempdir();
	my $b_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $b_ic1 = PostgreSQL::Test::Cluster::get_free_port();

	my $b0 = PostgreSQL::Test::Cluster->new('sxid_bkp_node0');
	$b0->init(allows_streaming => 1, extra => [ '-X', "$b_wal_root/thread_1" ]);
	$b0->start;
	$b0->backup('preseed');

	my $b1 = PostgreSQL::Test::Cluster->new('sxid_bkp_node1');
	$b1->init_from_backup($b0, 'preseed');
	ok(-e $b1->data_dir . '/backup_label',
		'B1: pre-seed pg_basebackup clone carries backup_label');
	$b0->stop;

	# Relocate b1's backup-copied WAL into its shared thread dir (t/337
	# recipe): the WAL thread validator requires pg_wal -> thread_N.
	{
		my $pgwal = $b1->data_dir . '/pg_wal';
		my $wal2 = "$b_wal_root/thread_2";
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

	append_common_shared_catalog_conf($b0, $b_shared);
	$b0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
cluster.relation_extend_lock_enabled = off
EOC

	$b0->start;
	my $b_abort_xid = $b0->safe_psql('postgres', q{
BEGIN;
SELECT txid_current();
CREATE SCHEMA sxid_bkp_abort_shadow;
ROLLBACK;
});
	$b0->safe_psql('postgres', q{
CREATE SCHEMA sxid_bkp;
CREATE TABLE sxid_bkp.reuse (k int PRIMARY KEY, v int);
INSERT INTO sxid_bkp.reuse SELECT g, 0 FROM generate_series(1, 20) g;
});
	my $b_seed_xid = $b0->safe_psql('postgres',
		q{SELECT xmin::text FROM pg_namespace WHERE nspname = 'sxid_bkp'});
	$b0->stop;

	my $b_auth = read_xid_authority($b_shared);
	ok(($b_auth->{flags} & 0x0001) != 0,
		'B2: clean seed shutdown sealed the XID authority');
	cmp_ok($b_auth->{native_hw}, '>', $b_seed_xid,
		'B2: native high-water is above the seed schema xid');

	append_strict_two_node_conf($b0, $b_disks, $b_wal_root);
	$b0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	append_common_shared_catalog_conf($b1, $b_shared);
	append_strict_two_node_conf($b1, $b_disks, $b_wal_root);
	$b1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
	my $b_data0 = PostgreSQL::Test::Cluster::get_free_port();
	my $b_data1 = PostgreSQL::Test::Cluster::get_free_port();
	append_pgrac_conf($b0, 'sxid_bkp', $b_ic0, $b_ic1, $b_data0, $b_data1);
	append_pgrac_conf($b1, 'sxid_bkp', $b_ic0, $b_ic1, $b_data0, $b_data1);

	start_background($b1);
	$b0->start;
	$b1->_update_pid(1);

	wait_sql_eq($b1, 'SELECT 1', '1', 'B3: backup_label joiner is up');
	like(PostgreSQL::Test::Utils::slurp_file($b1->logfile),
		qr/adopted XID prehistory through native high-water/,
		'B3: backup_label boot ADOPTED the native prehistory');
	unlike(PostgreSQL::Test::Utils::slurp_file($b1->logfile),
		qr/skipped XID prehistory adopt on backup_label boot/,
		'B3: backup_label boot did not skip the adopt');

	wait_sql_eq($b1, "SELECT txid_status($b_seed_xid)", 'committed',
		'B4: joiner proves the native-era committed xid');
	wait_sql_eq($b1, "SELECT txid_status($b_abort_xid)", 'aborted',
		'B4: joiner proves the native-era aborted xid');

	wait_sql_eq($b1,
		q{SELECT count(*) FROM pg_namespace WHERE nspname = 'sxid_bkp'},
		'1', 'B5: joiner sees the native-era seed schema row');
	wait_sql_eq($b1, q{SELECT count(*) FROM sxid_bkp.reuse}, '20',
		'B5: joiner sees the native-era table rows');

	my $b_covered = $b1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_native_prehistory_covered_hw'});
	is($b_covered, "$b_auth->{native_hw}",
		'B6: backup_label joiner latched verified native coverage');

	# B7 (RC-E core): cluster-era ITL reuse over the native rows, resolved
	# from the backup_label joiner.  The UPDATE can land in the node-rejoin
	# "block master is recovering" window (retry-safe by contract), so retry
	# instead of dying on the transient.
	my $b7u_ok = 0;
	for (1 .. 60)
	{
		my ($b7u_rc) = $b0->psql('postgres', q{UPDATE sxid_bkp.reuse SET v = v + 1});
		if (defined $b7u_rc && $b7u_rc == 0) { $b7u_ok = 1; last; }
		usleep(500_000);
	}
	ok($b7u_ok, 'B7 cluster-era UPDATE over the native rows succeeded (ITL reuse armed)');
	my ($b7_rc, $b7_out, $b7_err) = $b1->psql('postgres', q{
SET cluster.crossnode_runtime_visibility = on;
SELECT count(*) FROM sxid_bkp.reuse});
	is($b7_rc, 0, 'B7: joiner scan of the ITL-reused native page succeeds');
	is($b7_out, '20', 'B7: joiner sees every committed native row');
	unlike(($b7_err // ''), qr/TT slot recycled/,
		'B7: no recycled-slot fail-closed for a provably native xid');

	$b1->stop;
	$b0->stop;
}

# -------------------------------------------------------------------------
# P/C legs (GCS-race round-2 review F2 / calibration 1): the seal is a
# PROOF, not a timestamp.
#   P: a native-era PREPARED transaction survives clean shutdown as an
#	   in-progress CLOG bit that COMMIT PREPARED may later flip, so the
#	   shutdown checkpoint must refuse to seal (WARNING names it) and an
#	   unsealed authority must fail-close the joiner bootstrap.  Resolving
#	   the prepared xact and shutting down cleanly again seals.
#   C: a native-era crash-aborted xid (IN_PROGRESS in pg_xact, below the
#	   sealed high-water) latches normally on the joiner: the seal-side
#	   compound proof (true shutdown checkpoint + zero prepared + zero
#	   active xacts) makes blob-IN_PROGRESS == local-IN_PROGRESS
#	   "crash-aborted forever, never resolvable".
# -------------------------------------------------------------------------
{
	my $p_shared = make_shared_root();
	my $p_disks = make_voting_disks();
	my $p_wal_root = PostgreSQL::Test::Utils::tempdir();
	my $p_ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $p_ic1 = PostgreSQL::Test::Cluster::get_free_port();

	my $p0 = PostgreSQL::Test::Cluster->new('sxid_2pc_node0');
	$p0->init(allows_streaming => 1, extra => [ '-X', "$p_wal_root/thread_1" ]);
	$p0->start;
	$p0->safe_psql('postgres', 'CHECKPOINT');
	$p0->stop;

	my $p1 = PostgreSQL::Test::Cluster->new('sxid_2pc_node1');
	cold_clone_data_dir($p0, $p1, $p_wal_root, 2);

	append_common_shared_catalog_conf($p0, $p_shared);
	$p0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
cluster.relation_extend_lock_enabled = off
max_prepared_transactions = 1
EOC

	# Native seed run: committed content + a crash-aborted xid.
	$p0->start;
	$p0->safe_psql('postgres', q{
CREATE SCHEMA sxid_2pc;
CREATE TABLE sxid_2pc.rows (k int PRIMARY KEY, v int);
INSERT INTO sxid_2pc.rows SELECT g, 0 FROM generate_series(1, 20) g;
});
	my $p_seed_xid = $p0->safe_psql('postgres',
		q{SELECT xmin::text FROM pg_namespace WHERE nspname = 'sxid_2pc'});
	my $crash_bg = $p0->background_psql('postgres', on_error_stop => 0);
	$crash_bg->query_safe('BEGIN');
	my $crash_xid = $crash_bg->query_safe('SELECT txid_current()');
	$crash_bg->query_safe('CREATE TABLE sxid_2pc.shadow_crash (k int)');
	$p0->stop('immediate'); # crash: $crash_xid stays IN_PROGRESS in pg_xact
	eval { $crash_bg->quit };

	# Crash recovery, then a PREPARED xact pending across clean shutdown.
	$p0->start;
	my $prep_xid = $p0->safe_psql('postgres', q{
BEGIN;
SELECT txid_current();
INSERT INTO sxid_2pc.rows VALUES (99, 99);
PREPARE TRANSACTION 'sxid_2pc_pending';
});
	$p0->stop;

	my $p_auth = read_xid_authority($p_shared);
	is($p_auth->{flags} & 0x0001, 0,
		'P1: pending prepared xact keeps the XID authority UNSEALED at clean shutdown');
	like(PostgreSQL::Test::Utils::slurp_file($p0->logfile),
		qr/prepared transactions survive this shutdown/,
		'P2: shutdown checkpoint WARNING names the prepared-xact seal refusal');

	# Unsealed authority fail-closes the joiner bootstrap.
	append_common_shared_catalog_conf($p1, $p_shared);
	append_strict_two_node_conf($p1, $p_disks, $p_wal_root);
	$p1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
	my $p_data0 = PostgreSQL::Test::Cluster::get_free_port();
	my $p_data1 = PostgreSQL::Test::Cluster::get_free_port();
	append_pgrac_conf($p1, 'sxid_2pc', $p_ic0, $p_ic1, $p_data0, $p_data1);
	my $p_join_failed = !$p1->start(fail_ok => 1);
	ok($p_join_failed, 'P3: joiner refuses to boot against an unsealed XID authority');
	like(PostgreSQL::Test::Utils::slurp_file($p1->logfile),
		qr/shared XID authority is not sealed/,
		'P3: joiner log names the unsealed authority');

	# Resolve the prepared xact; the next clean shutdown may seal again.
	$p0->start;
	$p0->safe_psql('postgres', q{ROLLBACK PREPARED 'sxid_2pc_pending'});
	$p0->stop;
	$p_auth = read_xid_authority($p_shared);
	ok(($p_auth->{flags} & 0x0001) != 0,
		'P4: resolving the prepared xact lets the next clean shutdown seal');
	cmp_ok($p_auth->{native_hw}, '>', $crash_xid,
		'P4: sealed native high-water covers the crash-aborted xid');

	# C legs: form the cluster; the crash-aborted IN_PROGRESS bit must
	# latch (blob == local under the seal proof), never block coverage.
	append_strict_two_node_conf($p0, $p_disks, $p_wal_root);
	$p0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	append_pgrac_conf($p0, 'sxid_2pc', $p_ic0, $p_ic1, $p_data0, $p_data1);

	start_background($p1);
	$p0->start;
	$p1->_update_pid(1);
	wait_sql_eq($p1, 'SELECT 1', '1', 'C1: joiner is up despite a crash-aborted native xid');

	my $p_covered = $p1->safe_psql('postgres', q{
SELECT value FROM pg_cluster_state
 WHERE category = 'catalog' AND key = 'xid_native_prehistory_covered_hw'});
	is($p_covered, "$p_auth->{native_hw}",
		'C2: crash-aborted IN_PROGRESS bit latched verified native coverage');

	wait_sql_eq($p1, "SELECT txid_status($crash_xid)", 'aborted',
		'C3: joiner proves the crash-aborted native xid aborted');
	wait_sql_eq($p1, "SELECT txid_status($prep_xid)", 'aborted',
		'C3: joiner proves the rolled-back prepared xid aborted');
	wait_sql_eq($p1, "SELECT txid_status($p_seed_xid)", 'committed',
		'C3: joiner proves the committed native xid');
	wait_sql_eq($p1, q{SELECT count(*) FROM sxid_2pc.rows}, '20',
		'C4: joiner sees the native-era rows (prepared insert rolled back)');

	$p1->stop;
	$p0->stop;
}

done_testing();
