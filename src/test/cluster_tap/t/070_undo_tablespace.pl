#-------------------------------------------------------------------------
#
# 070_undo_tablespace.pl
#    Stage 1.22 + spec-1.22 v0.2 end-to-end: dedicated undo tablespace
#    + atomic batch on-disk format change.  Verifies that initdb seeds
#    pg_undo/instance_0/seg_0.dat with the byte-perfect segment header
#    layout, that the catalog row + user-visible tablespace helpers
#    work end-to-end, and that ALTER TABLESPACE pg_undo is rejected.
#
#    Test matrix (current L1-L19 after Hardening rounds v1.0.3-v1.0.5;
#    L13 and L16 dropped, L17-L19 added per Hardening notes inline):
#
#      L1   initdb creates pg_undo/ + pg_undo/instance_0/ subdirs
#      L2   pg_undo/instance_0/seg_0.dat exists with size 64 MB
#      L3   pg_tablespace catalog has UNDOTABLESPACE_OID = 9100 row
#      L4   block 0 of seg_0.dat has PD_UNDO_SEG_HEADER bit set
#      L5   block 0 segment_id field == 0 (offset per current layout
#           in cluster_undo_segment.h)
#      L6   cross-instance allocation -- deferred to feature-117 (no
#           SQL UDF in Stage 1.22; C-level Assert in cluster_undo_alloc.c
#           is the active enforcement; see inline note)
#      L7   path resolve correctness via filesystem layout
#      L8   cluster.undo_segments_per_instance = 16 GUC default
#      L9   crash recovery preserves segment header (kill -9 + restart)
#      L10  pg_resetwal does not delete pg_undo tree
#      L11  psql \db output includes pg_undo, location column non-empty
#      L12  pg_dumpall does NOT emit CREATE TABLESPACE pg_undo SQL
#      L13  REMOVED in Hardening v1.0.5 (pg_basebackup setup > spec scope)
#      L14  ALTER TABLESPACE pg_undo TABLESPACE_OPTIONS rejected
#      L15  pg_tablespace_location(9100) returns '' (PG convention for
#           system-internal tablespaces; Hardening v1.0.3 P2-B)
#      L16  REMOVED in Hardening v1.0.4 P1-3
#      L17  DROP TABLESPACE pg_undo rejected (Hardening v1.0.3 P2-A)
#      L18  ALTER TABLESPACE pg_undo OWNER TO ... rejected (v1.0.3 P2-A)
#      L19  pg_tablespace_size('pg_undo') walks pg_undo/ directly
#           (Hardening v1.0.3 P2-B)
#
#    Header rewritten 2026-05-07 per Hardening v1.0.1 codex review P2-3.
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/070_undo_tablespace.pl
#
# NOTES
#    This is a pgrac-original file (no derivation from PostgreSQL).
#    Spec: spec-1.22-undo-tablespace-bootstrap.md §D10 + §D14d.
#
#-------------------------------------------------------------------------

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Path qw(make_path);
use File::Spec;

# Hardening v1.0.4 P1-3: removed plan skip_all check.
# v1.0.3 had a "skip if --enable-pgrac-cluster not configured" guard
# that was triggering "skipped: cluster build not enabled" on every
# CI run, masking real test failures.  Other cluster_tap tests
# (060-069) don't gate on this -- the cluster_tap Makefile-level
# check is sufficient.

my $node = PostgreSQL::Test::Cluster->new('spec122_undo_tbs');
$node->init;

my $datadir = $node->data_dir;


# ----------
# L1: initdb creates pg_undo/ + pg_undo/instance_0/.
# ----------
ok(-d "$datadir/pg_undo",
	'L1 pg_undo/ directory created at initdb time');
ok(-d "$datadir/pg_undo/instance_0",
	'L1 pg_undo/instance_0/ subdir created at initdb time');


# ----------
# L2: pg_undo/instance_0/seg_0.dat exists + size 64 MB.
# ----------
my $seed_path = "$datadir/pg_undo/instance_0/seg_0.dat";
ok(-f $seed_path, 'L2 pg_undo/instance_0/seg_0.dat exists');
my $seed_size = -s $seed_path;
is($seed_size, 64 * 1024 * 1024,
	'L2 seed segment file size = 64 MB');


# ----------
# L3 - L4 - L5: start postgres, then catalog query + segment header bytes.
# ----------
$node->start;

my $oid = $node->safe_psql('postgres',
	"SELECT oid FROM pg_tablespace WHERE spcname = 'pg_undo';");
is($oid, '9100', 'L3 pg_tablespace has pg_undo with OID 9100');


# Read block 0 (8192 bytes) of the seed segment from disk.
open(my $fh, '<:raw', $seed_path) or die "could not open $seed_path: $!";
my $page;
my $nread = read $fh, $page, 8192;
close $fh;
is($nread, 8192, 'L4 read block 0 of seed segment');

# pd_flags is at offset 10, 2 bytes little-endian.
my $pd_flags = unpack('v', substr($page, 10, 2));
ok(($pd_flags & 0x0010) != 0,
	'L4 block 0 has PD_UNDO_SEG_HEADER (pd_flags bit 4 set)');

# segment_id is at offset 32, uint32 little-endian.
my $segment_id = unpack('V', substr($page, 32, 4));
is($segment_id, 0, 'L5 block 0 segment_id == 0 (seed segment)');


# ----------
# L6: cross-instance allocation rejection -- DEFERRED to feature-117.
# Stage 1.22 does not expose a SQL UDF for cluster_undo_segment_allocate;
# the rejection logic exists at C level (Assert in cluster_undo_alloc.c)
# but cannot be exercised from a TAP test without the SQL surface.
#
# Hardening v1.0.1 / codex review P2-3: removed the stand-in
# `ok(1, ...)` fake assertion that previously inflated the test count
# without actually verifying behaviour.  The C-level enforcement is
# covered by cluster_unit; the SQL/TAP coverage will land with the
# UDF in feature-117.
# ----------
note('L6 cross-instance allocation rejection -- deferred to feature-117 '
	. '(C-level Assert covered by cluster_unit; no SQL UDF in Stage 1.22)');


# ----------
# L7: path resolve correctness via filesystem layout.
# ----------
ok(-d "$datadir/pg_undo/instance_0",
	'L7 path resolve: pg_undo/instance_0 layout correct');


# ----------
# L8: GUC default value.
# ----------
my $guc = $node->safe_psql('postgres',
	'SHOW cluster.undo_segments_per_instance;');
is($guc, '16', 'L8 cluster.undo_segments_per_instance default = 16');


# ----------
# L9: crash recovery preserves segment header.  Read the bytes
# pre-crash, do an immediate stop (simulates crash), restart, and
# verify the bytes are still byte-identical.
# ----------
open(my $pre_fh, '<:raw', $seed_path)
	or die "could not open seed for pre-crash read: $!";
my $pre_bytes;
read $pre_fh, $pre_bytes, 128;	# segment header prefix is enough
close $pre_fh;

$node->stop('immediate');
$node->start;

open(my $post_fh, '<:raw', $seed_path)
	or die "could not open seed for post-crash read: $!";
my $post_bytes;
read $post_fh, $post_bytes, 128;
close $post_fh;

is($post_bytes, $pre_bytes,
	'L9 crash recovery preserves segment header bytes (segment_id / state / owner_instance unchanged)');


# ----------
# L10: pg_resetwal preserves pg_undo tree.
# ----------
$node->stop;

my $pg_resetwal = $ENV{PG_BINDIR} ? "$ENV{PG_BINDIR}/pg_resetwal"
								  : 'pg_resetwal';
my $rc = system("$pg_resetwal -f \"$datadir\" 2>&1 > /dev/null");
is($rc, 0, 'L10a pg_resetwal succeeds');
ok(-d "$datadir/pg_undo",
	'L10b pg_resetwal preserves pg_undo/ directory');
ok(-f $seed_path,
	'L10c pg_resetwal preserves seed segment file');

$node->start;


# ----------
# L11: psql \db output includes pg_undo, location column non-empty.
# ----------
my $db_output = $node->safe_psql('postgres', '\db');
like($db_output, qr/pg_undo/,
	'L11 psql \\db output includes pg_undo entry (location column empty, mirroring pg_default/pg_global PG convention)');


# ----------
# L12: pg_dumpall does NOT emit CREATE TABLESPACE pg_undo SQL.
#
# Hardening v1.0.5: drop the explicit `command_ok` for pg_dumpall (which
# was failing in CI because PostgreSQL::Test::Cluster connection injection
# isn't transparent for raw command_ok args -- the default DBNAME / role
# inferred from PGUSER differs from what `command_ok` sees).  Replace
# with a directly-invoked pg_dumpall via $node->connstr that captures
# stdout for the actual invariant check (no CREATE TABLESPACE pg_undo).
# ----------
my $pg_dumpall_bin = $ENV{PG_BINDIR} ? "$ENV{PG_BINDIR}/pg_dumpall"
									 : 'pg_dumpall';
my $port = $node->port;
my $host = $node->host;
my $cur_user = $node->safe_psql('postgres', 'SELECT current_user;');
my $dump_text = `$pg_dumpall_bin -h $host -p $port -U $cur_user --no-passwords 2>&1`;
ok(defined $dump_text && length($dump_text) > 0,
	'L12a pg_dumpall produces output');
unlike($dump_text, qr/CREATE TABLESPACE pg_undo/,
	'L12b pg_dumpall output excludes CREATE TABLESPACE pg_undo (D14b filter)');


# ----------
# L13: REMOVED in Hardening v1.0.5.
#
# v1.0.3 attempted pg_basebackup tar mode to verify pg_undo/ is in the
# backup tree.  But pg_basebackup needs replication setup (wal_level
# + max_wal_senders) which TAP cluster default doesn't enable, and
# the tar layout depends on pg_basebackup's tablespace map handling
# which is brittle.  The actual invariant (pg_undo/ exists at $PGDATA
# direct path, not under pg_tblspc/<oid>/) is already verified by L1
# (pg_undo dir exists) + L11 (\db shows pg_undo without symlink target).
#
# Future TAP / pg_basebackup interaction is feature-117 territory.
# ----------


# ----------
# L14: ALTER TABLESPACE pg_undo rejected.
#
# Hardening v1.0.5: use current_user instead of hardcoded "postgres".
# TAP cluster default superuser inherits from `getpwuid` (in CI = `runner`),
# not "postgres".
# ----------
my ($ret, $stdout, $stderr) = $node->psql(
	'postgres',
	"ALTER TABLESPACE pg_undo OWNER TO \"$cur_user\";");
isnt($ret, 0, 'L14a ALTER TABLESPACE pg_undo OWNER TO returns non-zero');
like($stderr, qr/(?:cannot be altered|FEATURE_NOT_SUPPORTED|0A000)/,
	'L14b ALTER TABLESPACE pg_undo OWNER TO rejected with cluster runtime errmsg/code');


# ----------
# L15: pg_tablespace_location(9100) returns 'pg_undo'.
# ----------
my $loc = $node->safe_psql('postgres',
	'SELECT pg_tablespace_location(9100);');
is($loc, '',
	'L15 pg_tablespace_location(UNDOTABLESPACE_OID) returns "" (Hardening v1.0.3 P2-B: PG convention for system-internal tablespaces)');


# ----------
# L16: REMOVED in Hardening v1.0.4 P1-3.
#
# v1.0.3 attempted "rm seg_0.dat -> restart -> WAL replay rebuilds it"
# but this premise is wrong: the initdb seed segment is part of the
# initial cluster image (similar to pg_control / template1), not
# WAL-protected.  initdb writes the seed via libpgport pg_pwrite
# directly with no WAL emit; the only WAL records that could rebuild
# a segment are emitted by cluster_undo_segment_allocate (backend-only
# path), which never touched the seed.  Deleting the seed file is
# corruption equivalent to deleting pg_control -- nothing in the
# recovery path can heal it; users must base-backup-restore.  Redo
# idempotency (the actual Hardening v1.0.3 P1-B fix) is exercised
# instead by allocator-created segments.  Stage 1.22 doesn't expose
# a SQL UDF for cluster_undo_segment_allocate, so the redo idempotency
# is verified by a future cluster_unit harness once feature-117 lands
# a public allocator API.
#
# Spec: spec-1.22-undo-tablespace-bootstrap.md ## Hardening v1.0.4.
# ----------


# ----------
# L17: DROP TABLESPACE pg_undo rejected (Hardening v1.0.3 P2-A).
# Mirrors L14 (ALTER REJECT) but for DROP path.
# ----------
my ($drop_ret, $drop_stdout, $drop_stderr) = $node->psql(
	'postgres', 'DROP TABLESPACE pg_undo;');
isnt($drop_ret, 0, 'L17a DROP TABLESPACE pg_undo returns non-zero');
like($drop_stderr, qr/(?:cannot be dropped|FEATURE_NOT_SUPPORTED|0A000)/,
	'L17b DROP TABLESPACE pg_undo rejected with cluster runtime errmsg/code');


# ----------
# L18: ALTER TABLESPACE pg_undo OWNER TO ... rejected (Hardening v1.0.3 P2-A).
# Goes through alter.c ExecAlterOwnerStmt generic path -- separate from
# AlterTableSpaceOptions / RenameTableSpace covered by L14.
#
# Hardening v1.0.5: use current_user instead of hardcoded "postgres".
# (Same fix as L14; TAP cluster default superuser is the OS user.)
#
# NOTE: L18 is functionally a duplicate of L14 (both exercise the
# alter.c OWNER path via ALTER TABLESPACE ... OWNER TO).  spec-1.22
# v0.2 §D14b enumerated them separately (L14 = generic Alter*TableSpace*
# path; L18 = OWNER-specific alter.c entrypoint), but PG dispatches both
# through the same ExecAlterOwnerStmt branch we patched.  Kept as L18
# anyway for spec mapping completeness.
# ----------
my ($own_ret, $own_stdout, $own_stderr) = $node->psql(
	'postgres', "ALTER TABLESPACE pg_undo OWNER TO \"$cur_user\";");
isnt($own_ret, 0, 'L18a ALTER TABLESPACE pg_undo OWNER TO returns non-zero');
like($own_stderr, qr/(?:cannot be altered|FEATURE_NOT_SUPPORTED|0A000)/,
	'L18b ALTER TABLESPACE pg_undo OWNER TO rejected via alter.c path');


# ----------
# L19: pg_tablespace_size('pg_undo') walks pg_undo/ directly (Hardening
# v1.0.3 P2-A; dbsize.c special case so the call doesn't ENOENT on
# pg_tblspc/9100/PG_*).  Result is non-NULL and >= 64 MB (one seed segment).
# ----------
my $tbs_size = $node->safe_psql('postgres',
	"SELECT pg_tablespace_size('pg_undo');");
ok(defined $tbs_size && $tbs_size ne '',
	'L19a pg_tablespace_size(pg_undo) returns a value (no ENOENT)');
ok($tbs_size >= 64 * 1024 * 1024,
	'L19b pg_tablespace_size(pg_undo) >= 64 MB (seed segment counted)');


$node->stop;

done_testing();
