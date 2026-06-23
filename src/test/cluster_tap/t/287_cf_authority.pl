#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 287_cf_authority.pl
#    spec-5.6 -- shared pg_control authority: migration + symlink + the
#    bootstrap-window / CF X control-file write path, validated live on a
#    single node.
#
#    This is the first real e2e for the spec-5.6 foundation (5.6a + 5.6b
#    core).  It runs an ordinary node with the opt-in GUC
#    cluster.controlfile_shared_authority = on so postmaster startup
#    migrates $PGDATA/global/pg_control into one shared authority under
#    cluster.shared_data_dir and replaces the local path with a symlink to
#    it; the startup process opens the single-node bootstrap authority
#    window for its recovery writes, and a steady-state CHECKPOINT goes
#    through the CF X authority write.  cluster.enabled stays off so the
#    authority mechanics are exercised without the full cluster machinery
#    (CSSD / voting disks): the authority path keys only on
#    controlfile_shared_authority, not on cluster.enabled.
#
#    Legs (single node, real start/stop -- no SKIP):
#      L1  $PGDATA/global/pg_control is a symlink to the shared authority.
#      L2  pg_controldata reads through the symlink and reports a valid,
#          in-production control file (same system identifier as the
#          authority).
#      L3  a CHECKPOINT succeeds (CF X authority write path) and the
#          authority file stays a valid 8 KB control file.
#      L4  fail-closed gate: a node with controlfile_shared_authority = on
#          but no cluster.shared_data_dir refuses to start (FATAL 58R13).
#      L5  first-migration identity gate: a freshly-initdb'd node (foreign
#          system_identifier) pointed at an existing authority fails closed.
#      L6  per-node identity anchor (P0): an already-migrated node whose shared
#          authority is replaced, at the same path, by a foreign-but-valid
#          control file fails closed (the anchor catches the foreign sysid).
#      L7  per-node identity anchor (P0): a migrated node whose anchor is
#          missing fails closed (never re-trusts the symlink target blindly).
#
#    The two-node "same inode" + concurrent-checkpoint legs are t/288/t/289.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/287_cf_authority.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Da1/Da2/Da3/Db3/Db5)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(make_path);

# The shared authority root.  global/ must exist before the first node
# migrates its control file into <root>/global/pg_control (pgrac-init owns
# this in production; the test creates it directly).
my $shared = PostgreSQL::Test::Utils::tempdir();
make_path("$shared/global");

my $node = PostgreSQL::Test::Cluster->new('cf_auth');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared'
cluster.controlfile_shared_authority = on
});
$node->start;

my $local_ctl = $node->data_dir . "/global/pg_control";
my $authority = "$shared/global/pg_control";

# ---- L1: the local control path is a symlink to the shared authority ----
ok(-l $local_ctl, "L1: \$PGDATA/global/pg_control is a symlink");
is(readlink($local_ctl), $authority,
	"L1: the symlink points at the shared authority");
ok(-f $authority, "L1: the shared authority is a regular file");

# ---- L2: pg_controldata reads through the symlink; identities match ----
my $cd_local = $node->command_checks_all(
	[ 'pg_controldata', $node->data_dir ],
	0, [qr/Database cluster state:\s+in production/], [],
	"L2: pg_controldata reads through the symlink");

my $sysid_via_pgdata = run_pg_controldata_sysid($node->data_dir);
my $sysid_via_shared = run_pg_controldata_sysid_path($authority);
is($sysid_via_pgdata, $sysid_via_shared,
	"L2: same system identifier via \$PGDATA and via the authority file");

# ---- L3: a checkpoint goes through the CF X authority write path ----
my $size_before = -s $authority;
$node->safe_psql('postgres', 'CHECKPOINT');
ok(1, "L3: CHECKPOINT through the CF X authority write succeeded");
is(-s $authority, 8192, "L3: the authority stays a full 8 KB control file");
# still a symlink + still readable after the checkpoint rewrote it
ok(-l $local_ctl, "L3: local path is still a symlink after the checkpoint");
$node->command_ok([ 'pg_controldata', $node->data_dir ],
	"L3: pg_controldata still valid after the checkpoint");

$node->stop;

# ---- L4: fail-closed gate -- authority on but no shared_data_dir ----
my $bad = PostgreSQL::Test::Cluster->new('cf_auth_nodir');
$bad->init;
$bad->append_conf(
	'postgresql.conf', qq{
cluster.enabled = off
cluster.controlfile_shared_authority = on
});
my $ret = $bad->start(fail_ok => 1);
ok(!$ret, "L4: node with authority on but no shared_data_dir fails to start");
my $log = PostgreSQL::Test::Utils::slurp_file($bad->logfile);
like($log, qr/shared_data_dir is not set/,
	"L4: the failure is the fail-closed authority gate (58R13)");

# ---- L5: identity fail-closed (DoD-5) -- a node whose OWN per-node control
#         file has a foreign system_identifier must NOT attach to the shared
#         authority built by a different cluster.  The first node's authority
#         (in $shared) carries its system_identifier; a freshly-initdb'd node
#         (different sysid) pointed at the same authority must fail closed at
#         the migrate/identity gate rather than start on a foreign control file.
my $foreign = PostgreSQL::Test::Cluster->new('cf_auth_foreign');
$foreign->init;    # independent initdb -> a different system_identifier
$foreign->append_conf(
	'postgresql.conf', qq{
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared'
cluster.controlfile_shared_authority = on
});
my $fret = $foreign->start(fail_ok => 1);
ok(!$fret, "L5: a node with a foreign system_identifier fails to attach to the authority");
my $flog = PostgreSQL::Test::Utils::slurp_file($foreign->logfile);
like($flog, qr/could not migrate global\/pg_control|foreign|authority/i,
	"L5: the failure is the fail-closed identity/migration gate (DoD-5)");

# ---- L6: P0 identity anchor -- an ALREADY-MIGRATED node (local path is a
#         symlink, no independent local sysid left) whose shared authority is
#         replaced, AT THE SAME PATH, by another VALID control file with a
#         different system_identifier must fail closed.  The per-node anchor
#         recorded at migration is the only proof the node is reading its own
#         cluster's control file; a same-path storage swap to a foreign-but-
#         valid authority must be rejected, not silently accepted.
my $anchor = $node->data_dir . "/global/pgrac_cf_contract";
ok(-f $anchor, "L6: the migrated node recorded a per-node identity anchor");

# $foreign initdb'd a valid control file with a different system_identifier and
# never migrated (its start failed at L5), so its global/pg_control is a plain
# foreign-but-valid image.  Drop it onto the shared authority's primary path.
require File::Copy;
File::Copy::copy($foreign->data_dir . "/global/pg_control", $authority)
	or die "copy foreign authority: $!";

my $sret = $node->start(fail_ok => 1);
ok(!$sret,
	"L6: the migrated node refuses to start on a foreign authority swapped in at the same path");
my $slog = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($slog, qr/identity check failed|different system identifier|foreign control file/i,
	"L6: the failure is the per-node identity gate (foreign sysid, not a blind accept)");

# ---- L7: P0 identity anchor -- a migrated node (local path is a symlink) whose
#         per-node anchor is MISSING must fail closed, never "re-trust" whatever
#         authority the symlink currently resolves to.  Uses its own fresh shared
#         root so it is independent of L6.
my $shared7 = PostgreSQL::Test::Utils::tempdir();
make_path("$shared7/global");
my $n7 = PostgreSQL::Test::Cluster->new('cf_auth_noanchor');
$n7->init;
$n7->append_conf(
	'postgresql.conf', qq{
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared7'
cluster.controlfile_shared_authority = on
});
$n7->start;    # migrates: writes the anchor + replaces the local path with a symlink
ok(-l($n7->data_dir . "/global/pg_control"),
	"L7: the node migrated (local control path is a symlink)");
$n7->stop;

unlink($n7->data_dir . "/global/pgrac_cf_contract")
	or die "unlink anchor: $!";
my $r7 = $n7->start(fail_ok => 1);
ok(!$r7, "L7: a migrated node with no identity anchor fails to start");
my $l7 = PostgreSQL::Test::Utils::slurp_file($n7->logfile);
like($l7, qr/no identity anchor|identity check failed/i,
	"L7: the failure is the missing-anchor identity gate (no blind re-trust)");

done_testing();

# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------

sub run_pg_controldata_sysid
{
	my ($datadir) = @_;
	my $out = PostgreSQL::Test::Utils::run_command([ 'pg_controldata', $datadir ]);
	return ($out =~ /Database system identifier:\s+(\d+)/) ? $1 : '';
}

sub run_pg_controldata_sysid_path
{
	my ($ctlpath) = @_;
	# pg_controldata takes a data dir; point it at a scratch dir whose
	# global/pg_control is the authority file.
	my $scratch = PostgreSQL::Test::Utils::tempdir();
	make_path("$scratch/global");
	PostgreSQL::Test::Utils::slurp_file($ctlpath);    # ensure it exists
	require File::Copy;
	File::Copy::copy($ctlpath, "$scratch/global/pg_control");
	return run_pg_controldata_sysid($scratch);
}
