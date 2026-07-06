#-------------------------------------------------------------------------
#
# 003_cluster_init.pl
#    File-level regression for the pgrac-init shared-catalog modes
#    (--cluster-seed / --cluster-join).
#
#    Sections A-D are filesystem-level (no server start): option
#    validation, seed GUC wiring, shared-tree skeleton, join clone from
#    a backup directory, node-id rewrite, pg_wal relocation and the
#    pgrac.conf topology append.  Section E boots the seeded node
#    single-node and proves the first start publishes the shared
#    authorities (and the second start adopts them).  The live 2-node
#    boot of a seeded + joined pair is covered by
#    src/test/cluster_tap/t/337 (the CLI mirrors that proven recipe).
#
# IDENTIFICATION
#    src/bin/pgrac/t/003_cluster_init.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

# The six postgresql.conf keys --cluster-seed / --cluster-join must wire.
my @sc_gucs = (
	qr/^cluster\.shared_storage_backend\s*=\s*cluster_fs\s*$/m,
	qr/^cluster\.shared_data_dir\s*=\s*'[^']+'\s*$/m,
	qr/^cluster\.smgr_user_relations\s*=\s*on\s*$/m,
	qr/^cluster\.controlfile_shared_authority\s*=\s*on\s*$/m,
	qr/^cluster\.shared_catalog\s*=\s*on\s*$/m,
	qr/^cluster\.merged_recovery\s*=\s*on\s*$/m,
);

# ----------
# A. Mode / option validation (nothing may be created on failure).
# ----------
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a1", '--cluster-seed' ],
	'A1: --cluster-seed without --shared-data-dir is rejected');
ok(!-d "$tempdir/a1", 'A1: rejection bootstrapped nothing');

command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a2", '--cluster-seed',
		'--shared-data-dir=relative/path' ],
	'A2: relative --shared-data-dir is rejected');

command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a3", '--cluster-seed', '--cluster-join',
		"--shared-data-dir=$tempdir/sroot-a" ],
	'A3: --cluster-seed and --cluster-join together are rejected');

command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a4", '--cluster-join',
		"--shared-data-dir=$tempdir/sroot-a",
		"--join-from-backup=$tempdir/nowhere" ],
	'A4: --cluster-join without an explicit --node-id is rejected');

command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a5", '--cluster-join', '--node-id=1',
		"--shared-data-dir=$tempdir/sroot-a" ],
	'A5: --cluster-join without a join source is rejected');

command_fails(
	[ 'pgrac-init', '-D', "$tempdir/a6", '--cluster-join', '--node-id=1',
		"--shared-data-dir=$tempdir/sroot-a",
		'--join-from=host=localhost',
		"--join-from-backup=$tempdir/nowhere" ],
	'A6: --join-from and --join-from-backup together are rejected');

# ----------
# B. Seed mode end-to-end (file level).
# ----------
my $sroot_b = "$tempdir/sroot-b";
my $seed_dir = "$tempdir/seed";

command_ok(
	[ 'pgrac-init', '-D', $seed_dir, '--node-id=0', '--cluster-seed',
		"--shared-data-dir=$sroot_b" ],
	'B1: --cluster-seed bootstraps a fresh PGDATA');
ok(-f "$seed_dir/PG_VERSION", 'B1: PG-shaped data directory produced');

my $seedconf = PostgreSQL::Test::Utils::slurp_file("$seed_dir/postgresql.conf");
my $i = 0;
for my $re (@sc_gucs)
{
	$i++;
	like($seedconf, $re, "B2.$i: shared-catalog GUC $i wired in postgresql.conf");
}
like($seedconf, qr/^cluster\.shared_data_dir\s*=\s*'\Q$sroot_b\E'\s*$/m,
	'B2: cluster.shared_data_dir points at the shared root');
like($seedconf, qr/^cluster\.node_id\s*=\s*0\s*$/m,
	'B2: cluster.node_id = 0');

ok(-d "$sroot_b/global", 'B3: shared-tree global/ skeleton created');

command_ok(
	[ 'pgrac-init', '-D', $seed_dir, '--node-id=0', '--cluster-seed',
		"--shared-data-dir=$sroot_b" ],
	'B4: seed re-run with the same parameters is a no-op');

# B5: a FRESH PGDATA pointed at a shared tree that already carries a
# catalog authority must be refused (a fresh initdb mints a new system
# identifier that can never match the existing authority).
my $sroot_b5 = "$tempdir/sroot-b5";
mkdir $sroot_b5 or die "mkdir $sroot_b5: $!";
mkdir "$sroot_b5/global" or die "mkdir $sroot_b5/global: $!";
append_to_file("$sroot_b5/global/pgrac_catalog_authority", "x");
my ($rc, $out, $err) = (0, '', '');
$rc = IPC::Run::run(
	[ 'pgrac-init', '-D', "$tempdir/b5", '--node-id=0', '--cluster-seed',
		"--shared-data-dir=$sroot_b5" ],
	'>', \$out, '2>', \$err);
ok(!$rc, 'B5: seeding a fresh PGDATA against an existing authority fails');
like($err, qr/--cluster-join/,
	'B5: the error suggests --cluster-join instead');
ok(!-d "$tempdir/b5", 'B5: rejection bootstrapped nothing');

# ----------
# C. Join mode from a backup directory.
# ----------
# The join preflight only checks that the shared tree carries an
# authority marker + shared pg_control (content vetting is the server's
# boot-time job), so fabricate both on top of the B seed tree.
append_to_file("$sroot_b/global/pgrac_catalog_authority", "x");
append_to_file("$sroot_b/global/pg_control", "x");

# C0: a raw data-directory copy (no backup_label) is NOT a supported
# join source -- the joiner boot contract is label-provisioned
# (spec-5.6a): the anchor semantics of a raw clone are undefined.
($rc, $out, $err) = (0, '', '');
$rc = IPC::Run::run(
	[ 'pgrac-init', '-D', "$tempdir/c0", '--cluster-join', '--node-id=1',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir" ],
	'>', \$out, '2>', \$err);
ok(!$rc, 'C0: join from a raw directory without backup_label fails');
like($err, qr/pg_basebackup/,
	'C0: the error points the operator at pg_basebackup');
ok(!-d "$tempdir/c0", 'C0: rejection cloned nothing');

# All file-level join legs below use the (initdb-shaped) seed dir as a
# stand-in backup source; give it the backup_label the vet demands.
# (Real boots of a joined pair are t/337 territory.)
append_to_file("$seed_dir/backup_label", "START WAL LOCATION: 0/0 (file x)\n");

# C1: joining a shared tree with no catalog authority is refused.
my $sroot_c1 = "$tempdir/sroot-c1";
mkdir $sroot_c1 or die "mkdir $sroot_c1: $!";
($rc, $out, $err) = (0, '', '');
$rc = IPC::Run::run(
	[ 'pgrac-init', '-D', "$tempdir/c1", '--cluster-join', '--node-id=1',
		"--shared-data-dir=$sroot_c1",
		"--join-from-backup=$seed_dir" ],
	'>', \$out, '2>', \$err);
ok(!$rc, 'C1: join against an unseeded shared tree fails');
like($err, qr/first start|authority/i,
	'C1: the error explains the seed node must have completed its first start');
ok(!-d "$tempdir/c1", 'C1: rejection cloned nothing');

# C2: successful join from a backup directory.
my $join_dir = "$tempdir/join";
command_ok(
	[ 'pgrac-init', '-D', $join_dir, '--cluster-join', '--node-id=1',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir" ],
	'C2: --cluster-join clones a backup directory');
ok(-f "$join_dir/PG_VERSION", 'C2: joined PGDATA is PG-shaped');
ok(!-f "$join_dir/postmaster.pid", 'C2: no stale postmaster.pid in the clone');
ok(-f "$join_dir/backup_label",
	'C2: backup_label preserved (label-provisioned first boot, spec-5.6a)');

my $joinconf = PostgreSQL::Test::Utils::slurp_file("$join_dir/postgresql.conf");
like($joinconf, qr/^cluster\.node_id\s*=\s*1\s*$/m,
	'C2: cluster.node_id rewritten to the joiner id');
unlike($joinconf, qr/^cluster\.node_id\s*=\s*0\s*$/m,
	'C2: the clone-source node id is gone');

$i = 0;
for my $re (@sc_gucs)
{
	$i++;
	like($joinconf, $re, "C3.$i: shared-catalog GUC $i present after join");
}

# C4: reusing the clone source's node id is refused.
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/c4", '--cluster-join', '--node-id=0',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir" ],
	'C4: join with the clone source\'s own node id is rejected');

# C5: a non-empty target PGDATA is never clobbered.
command_fails(
	[ 'pgrac-init', '-D', $join_dir, '--cluster-join', '--node-id=2',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir" ],
	'C5: join refuses a non-empty target PGDATA');

# C6: --interconnect-addr appends the joiner's [node.N] topology section.
my $join6_dir = "$tempdir/join6";
command_ok(
	[ 'pgrac-init', '-D', $join6_dir, '--cluster-join', '--node-id=3',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir",
		'--interconnect-addr=127.0.0.1:7433' ],
	'C6: join with --interconnect-addr succeeds');
my $jpgrac = PostgreSQL::Test::Utils::slurp_file("$join6_dir/pgrac.conf");
like($jpgrac, qr/\[node\.0\]/, 'C6: cloned pgrac.conf keeps the seed section');
like($jpgrac, qr/\[node\.3\][^\[]*interconnect_addr\s*=\s*127\.0\.0\.1:7433/s,
	'C6: [node.3] section appended with the given interconnect address');

# C7: without --interconnect-addr the operator is reminded to complete
# the topology by hand (on this node and on every other node).
my $join7_dir = "$tempdir/join7";
command_like(
	[ 'pgrac-init', '-D', $join7_dir, '--cluster-join', '--node-id=4',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir" ],
	qr/\[node\.4\]/,
	'C7: join without --interconnect-addr prints a topology reminder');

# ----------
# D. Join + per-thread WAL relocation (spec-4.1 layout).
# ----------
my $wroot = "$tempdir/walroot";
my $join_d1 = "$tempdir/join-d1";
command_ok(
	[ 'pgrac-init', '-D', $join_d1, '--cluster-join', '--node-id=5',
		"--shared-data-dir=$sroot_b",
		"--join-from-backup=$seed_dir",
		"--wal-threads-dir=$wroot" ],
	'D1: join with --wal-threads-dir succeeds');
ok(-l "$join_d1/pg_wal", 'D1: pg_wal is a symlink after relocation');
{
	my $target = readlink("$join_d1/pg_wal");
	like($target, qr/thread_6/, 'D1: pg_wal symlink targets thread_<id+1>');
}
ok(-d "$wroot/thread_6", 'D1: thread directory created on the WAL root');
opendir(my $dh, "$wroot/thread_6") or die "opendir: $!";
my @wal = grep { /^[0-9A-F]{24}$/ } readdir($dh);
closedir($dh);
ok(scalar(@wal) > 0, 'D1: the clone WAL segments moved into the thread dir');
my $d1conf = PostgreSQL::Test::Utils::slurp_file("$join_d1/postgresql.conf");
like($d1conf, qr/^cluster\.wal_threads_dir\s*=\s*'\Q$wroot\E'\s*$/m,
	'D1: cluster.wal_threads_dir written to postgresql.conf');

# D2: a clone whose pg_wal is a symlink (the source itself uses the
# per-thread layout) must not silently share the source's WAL stream.
# Fresh shared root: the seed preflight refuses a marked tree, and the
# join legs need the clone's cluster.shared_data_dir to match theirs.
my $src_d2 = "$tempdir/src-d2";
my $sroot_d2 = "$tempdir/sroot-d2";
my $wroot_d2 = "$tempdir/walroot-d2";
command_ok(
	[ 'pgrac-init', '-D', $src_d2, '--node-id=0', '--cluster-seed',
		"--shared-data-dir=$sroot_d2",
		"--wal-threads-dir=$wroot_d2" ],
	'D2: seed with a per-thread WAL layout (join source)');
append_to_file("$src_d2/backup_label", "START WAL LOCATION: 0/0 (file x)\n");
append_to_file("$sroot_d2/global/pgrac_catalog_authority", "x");
append_to_file("$sroot_d2/global/pg_control", "x");
command_fails(
	[ 'pgrac-init', '-D', "$tempdir/join-d2", '--cluster-join', '--node-id=6',
		"--shared-data-dir=$sroot_d2",
		"--join-from-backup=$src_d2" ],
	'D2: join from a symlinked-pg_wal source without --wal-threads-dir fails');

# D3: same source, but WITH --wal-threads-dir: the joiner gets its own
# fresh thread directory (never the source's stream).
my $join_d3 = "$tempdir/join-d3";
command_ok(
	[ 'pgrac-init', '-D', $join_d3, '--cluster-join', '--node-id=6',
		"--shared-data-dir=$sroot_d2",
		"--join-from-backup=$src_d2",
		"--wal-threads-dir=$wroot_d2" ],
	'D3: join from a symlinked-pg_wal source with --wal-threads-dir succeeds');
ok(-l "$join_d3/pg_wal", 'D3: pg_wal is a symlink');
{
	my $target = readlink("$join_d3/pg_wal");
	like($target, qr/thread_7/, 'D3: joiner symlink targets its OWN thread_7');
	unlike($target, qr/thread_1\b/, 'D3: joiner does not adopt the source thread');
}

# ----------
# E. Seed first boot: the CLI-wired GUC set must actually publish the
#    shared authorities (t/337 step-1 recipe, driven through the CLI).
# ----------
my $enode = PostgreSQL::Test::Cluster->new('pgrac_003_seed');
my $eport = $enode->port;
my $edata = "$tempdir/eseed";
my $esroot = "$tempdir/sroot-e";
my $elog = "$tempdir/eseed.log";

command_ok(
	[ 'pgrac-init', '-D', $edata, '--node-id=0', '--cluster-seed',
		"--shared-data-dir=$esroot" ],
	'E1: seed bootstrap for the live first start');

append_to_file("$edata/postgresql.conf",
	"port = $eport\n"
	. "unix_socket_directories = '$tempdir'\n"
	. "listen_addresses = ''\n"
	# Single-node first start (the t/337 step-1 recipe): the catalog
	# migration is gated on cluster.shared_catalog, not cluster.enabled.
	. "cluster.enabled = off\n"
	. "cluster.lms_enabled = off\n");

command_ok(
	[ 'pgrac-start', '-D', $edata, '-l', $elog, '-w', '-t', '60' ],
	'E2: seed node first start succeeds single-node');

ok(-f "$esroot/global/pgrac_catalog_authority",
	'E3: shared catalog authority marker seeded');
ok(-e "$esroot/global/pg_control",
	'E3: shared pg_control authority seeded');
ok(-f "$esroot/global/pgrac_oid_authority",
	'E3: shared OID authority seeded');
ok(-f "$esroot/global/pgrac_relmap_authority",
	'E3: shared relmap authority seeded');

command_ok([ 'pg_ctl', '-D', $edata, '-w', 'stop' ],
	'E4: seed node stops cleanly');

# Second start must ADOPT the existing authority (idempotent boot; the
# marker forbids a re-seed).
command_ok(
	[ 'pgrac-start', '-D', $edata, '-l', $elog, '-w', '-t', '60' ],
	'E5: second start adopts the existing shared authority');
command_ok([ 'pg_ctl', '-D', $edata, '-w', 'stop' ],
	'E5: second start stops cleanly');

done_testing();
