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
	my ($src, $dst) = @_;

	PostgreSQL::Test::RecursiveCopy::copypath(
		$src->data_dir,
		$dst->data_dir,
		filterfn => sub {
			my $rel = shift;
			return ($rel ne 'log' && $rel ne 'postmaster.pid');
		});
	chmod(0700, $dst->data_dir) or die "chmod clone data dir: $!";

	# The cold copy carries node0's port; append node1's port so the last
	# setting wins, matching init_from_backup's post-copy rewrite.
	$dst->append_conf('postgresql.conf', 'port = ' . $dst->port . "\n");
}

sub append_pgrac_conf
{
	my ($node, $name, $ic0, $ic1) = @_;
	my $pgrac_conf = <<EOC;
[cluster]
name = $name

[node.0]
interconnect_addr = 127.0.0.1:$ic0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
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
		my ($rc, $out) = $node->psql('postgres', $sql);
		if (defined $rc && $rc == 0 && defined $out)
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
my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
my $ic1 = PostgreSQL::Test::Cluster::get_free_port();

my $node0 = PostgreSQL::Test::Cluster->new('sxid_node0');
$node0->init(allows_streaming => 1);
$node0->start;
$node0->safe_psql('postgres', 'CHECKPOINT');
$node0->stop;

my $node1 = PostgreSQL::Test::Cluster->new('sxid_node1');
cold_clone_data_dir($node0, $node1);
# F3: real scram TCP login leg needs a host auth line AND a TCP listener on
# the joiner (test nodes default to unix sockets only).
PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pg_hba.conf',
	"host all sxid_login 127.0.0.1/32 scram-sha-256\n");
$node1->append_conf('postgresql.conf', "listen_addresses = '127.0.0.1'\n");

append_common_shared_catalog_conf($node0, $shared_root);
$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
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

append_strict_two_node_conf($node0, $disks_csv, undef);
$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");

append_common_shared_catalog_conf($node1, $shared_root);
append_strict_two_node_conf($node1, $disks_csv, undef);
$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

append_pgrac_conf($node0, 'sxid', $ic0, $ic1);
append_pgrac_conf($node1, 'sxid', $ic0, $ic1);

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

done_testing();
