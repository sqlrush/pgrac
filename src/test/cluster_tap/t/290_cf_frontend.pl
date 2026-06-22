#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 290_cf_frontend.pl
#    spec-5.6 -- frontend-tool behaviour around the shared pg_control authority:
#    basebackup materializes the symlinked control file as a regular file
#    (Dc6), and pg_resetwal refuses to run in cluster mode (Dc3).
#
#    Legs (single node with controlfile_shared_authority = on):
#      L11 basebackup materialize + restore: pg_basebackup of an authority node
#          emits global/pg_control as a regular 8 KB file (NOT a symlink tar
#          entry), so a restore is a clean per-node control file that
#          pg_controldata reads and a node can start from.
#      L12 pg_resetwal cluster reject: pg_resetwal on a node whose
#          global/pg_control is a symlink to the shared authority fails closed
#          (it would corrupt the cluster-wide authority), and the authority file
#          is left intact.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/290_cf_frontend.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (§3.10 Dc3/Dc6)
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

my $shared = PostgreSQL::Test::Utils::tempdir();
make_path("$shared/global");

my $node = PostgreSQL::Test::Cluster->new('cf_fe');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', qq{
cluster.enabled = off
cluster.lms_enabled = off
cluster.shared_data_dir = '$shared'
cluster.controlfile_shared_authority = on
});
$node->start;

my $local_ctl = $node->data_dir . '/global/pg_control';
ok(-l $local_ctl, 'setup: node global/pg_control is a symlink to the shared authority');

# ----------
# L11: basebackup materializes the symlinked pg_control as a regular file.
# ----------
$node->backup('b290');
my $backup_ctl = $node->backup_dir . '/b290/global/pg_control';
ok(-f $backup_ctl && !-l $backup_ctl,
	'L11a: the backup global/pg_control is a regular file, not a symlink (Dc6 materialize)');
is(-s $backup_ctl, 8192, 'L11b: the materialized control file is a full 8 KB image');

# the materialized control file is a valid control file pg_controldata reads.
my $restore = PostgreSQL::Test::Cluster->new('cf_fe_restore');
$restore->init_from_backup($node, 'b290');
$restore->command_ok([ 'pg_controldata', $restore->data_dir ],
	'L11c: pg_controldata reads the restored (materialized) control file');
# the restored node is a plain single-node deployment (authority off) and starts.
$restore->append_conf('postgresql.conf',
	"cluster.controlfile_shared_authority = off\ncluster.shared_data_dir = ''\n");
$restore->start;
is($restore->safe_psql('postgres', 'SELECT 1'), '1',
	'L11d: a node restored from the materialized backup starts and is usable');
$restore->stop;

# ----------
# L12: pg_resetwal refuses to run in cluster (symlinked-authority) mode.
# ----------
my $auth_size_before = -s "$shared/global/pg_control";
$node->stop;    # pg_resetwal requires the server stopped

$node->command_fails_like(
	[ 'pg_resetwal', '-n', $node->data_dir ],
	qr/symlink to a shared pg_control authority|pgrac cluster mode/,
	'L12a: pg_resetwal fails closed on a symlinked (cluster) control file');

# the shared authority must be untouched (pg_resetwal refused before writing).
ok(-l $local_ctl, 'L12b: the local control path is still a symlink after the refusal');
is(-s "$shared/global/pg_control", $auth_size_before,
	'L12c: the shared authority is intact (pg_resetwal did not rewrite it)');

done_testing();
