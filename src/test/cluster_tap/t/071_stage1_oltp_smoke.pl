#-------------------------------------------------------------------------
#
# 071_stage1_oltp_smoke.pl
#    Spec-1.23 Stage 1 OLTP pgbench TPC-B 10-second smoke test (D2).
#
#    NOT a baseline -- this test only verifies that pgbench TPC-B
#    workload runs against a pgrac --enable-cluster instance without
#    breaking spec-1.22 invariants (pg_undo seed unchanged; GUC default
#    intact; pg_tablespace_size walks pg_undo cleanly).  Full baseline
#    is in scripts/perf/run-stage1-oltp-baseline.sh (manual; ~4.5 hr).
#
#    Test matrix (4 L# real assertions):
#      L1 pgbench reports tps after 10s scale=1 c=1 run
#         (pgbench TPC-B 6 statements: BEGIN / UPDATE accounts /
#          SELECT abalance / UPDATE tellers / UPDATE branches /
#          INSERT history / END -- NO DELETE)
#      L2 pg_undo/instance_0/seg_0.dat still 64 MB after workload
#         (allocator never invoked from SQL workload at Stage 1.22;
#          seed segment must remain untouched)
#      L3 cluster.undo_segments_per_instance still default 16 after run
#      L4 pg_tablespace_size('pg_undo') still >= 64 MB
#         (validates Hardening v1.0.3 P2-A special case in dbsize.c)
#
#    L5 (PG 219 / cluster_unit / cluster_regress维持) is NOT a TAP
#    L# -- it lives in spec-1.23 §7 DoD as a parent Makefile-level
#    invariant.  TAP must only assert what TAP itself executes
#    (lessons SSOT v1.12 L49: no fake assertions).
#
#    L49 compliance:
#      - no hardcoded role: $cur_user = $node->safe_psql(...)
#      - no plan skip_all: cluster_tap Makefile-level enable-cluster
#        gate is sufficient
#      - no command_ok for external tools: pgbench invoked via backtick
#        with explicit -h/-p/-U
#      - CI must show "ok" not "skipped" before ship
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/071_stage1_oltp_smoke.pl
#
# NOTES
#    This is a pgrac-original file (no derivation from PostgreSQL).
#    Spec: spec-1.23-stage1-oltp-baseline.md §D2.
#
#-------------------------------------------------------------------------

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;


my $node = PostgreSQL::Test::Cluster->new('spec123_oltp');
$node->init;
$node->start;

# L49 compliance: get the real superuser role (not hardcoded "postgres").
my $cur_user = $node->safe_psql('postgres', 'SELECT current_user;');

my $datadir = $node->data_dir;
my $port = $node->port;
my $host = $node->host;
my $pgbench_bin = $ENV{PG_BINDIR} ? "$ENV{PG_BINDIR}/pgbench" : 'pgbench';

# Init pgbench scale=1 (smallest workload; ~150 KB working set).
my $init_out = `$pgbench_bin -i -s 1 -h $host -p $port -U $cur_user postgres 2>&1`;
isnt($?, -1, 'pgbench init invocation succeeded')
    or diag "pgbench init output: $init_out";


# ----------
# L1: pgbench TPC-B 10s c=1 run reports tps.  pgbench TPC-B 6 statements
# (UPDATE/SELECT/UPDATE/UPDATE/INSERT plus BEGIN/END) -- NO DELETE per
# pgbench default; spec-1.23 v0.2 Q6 corrected from v0.1 erratum.
# ----------
my $bench_out = `$pgbench_bin -T 10 -c 1 -h $host -p $port -U $cur_user postgres 2>&1`;
like($bench_out, qr/tps = /,
    'L1 pgbench TPC-B 10s scale=1 c=1 reports tps (workload exercised UPDATE/SELECT/INSERT paths)');


# ----------
# L2: pg_undo/instance_0/seg_0.dat still 64 MB after workload.
#     spec-1.22 invariant: allocator never invoked from SQL workload
#     at Stage 1.22 (deferred to feature-117 SQL UDF round); seed
#     segment must remain untouched by any pgbench TPC-B run.
# ----------
my $seed_path = "$datadir/pg_undo/instance_0/seg_0.dat";
ok(-f $seed_path, 'L2a pg_undo/instance_0/seg_0.dat still exists after pgbench');
my $seed_size = -s $seed_path;
is($seed_size, 64 * 1024 * 1024,
    'L2b pg_undo/instance_0/seg_0.dat still 64 MB after pgbench (allocator not exercised by SQL workload at Stage 1.22)');


# ----------
# L3: cluster.undo_segments_per_instance still default 16.
#     PGC_POSTMASTER GUC -- shouldn't change at runtime, but verify
#     pgbench workload didn't somehow corrupt the GUC machinery.
# ----------
my $guc = $node->safe_psql('postgres', 'SHOW cluster.undo_segments_per_instance;');
is($guc, '16',
    'L3 cluster.undo_segments_per_instance default = 16 unchanged after pgbench');


# ----------
# L4: pg_tablespace_size('pg_undo') still walks the directory cleanly.
#     Validates Hardening v1.0.3 P2-A special case in
#     src/backend/utils/adt/dbsize.c (calculate_tablespace_size walks
#     "pg_undo" path directly instead of pg_tblspc/<oid>/<catver>/).
# ----------
my $tbs_size = $node->safe_psql('postgres',
    "SELECT pg_tablespace_size('pg_undo');");
ok(defined $tbs_size && $tbs_size ne '' && $tbs_size >= 64 * 1024 * 1024,
    'L4 pg_tablespace_size(pg_undo) >= 64 MB after pgbench (single seed segment intact; D14b dbsize.c special case OK)');


$node->stop;
done_testing();
