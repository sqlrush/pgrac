#-------------------------------------------------------------------------
#
# 072_pgrac_conf_multi_node_activation.pl
#    End-to-end regression for spec-2.1: pgrac.conf multi-node activation
#    + cluster.node_id WARNING -> FATAL tighten + cluster.allow_single_node
#    boundary invariant.
#
#    Stage 2.1 introduces the new GUC cluster.allow_single_node (default
#    on, PGC_POSTMASTER) that gates the WARNING -> FATAL dual path:
#
#      - allow_single_node = on  (Stage 2.1 default; backward-compat):
#          conf absent OR cluster.node_id invalid -> WARNING + fallback
#      - allow_single_node = off (Stage 2 strict mode):
#          conf absent OR cluster.node_id invalid -> FATAL
#
#    BOUNDARY INVARIANT (spec-2.1 §3.5; Q1 user 反审 caveat):
#    allow_single_node = on permits fallback ONLY when multi-node config
#    is ABSENT.  Malformed conf (collision / out-of-range / etc) is FATAL
#    regardless of allow_single_node value.  L9 实证 this boundary.
#
#    Test plan (9 L# scenarios):
#      L1: cluster.allow_single_node default value = on
#      L2: conf absent + allow=on (default) -> WARNING + single-node fallback
#      L3: conf absent + allow=off -> FATAL
#      L4: conf present + node_id valid -> no WARNING
#      L5: conf present + node_id invalid + allow=on -> WARNING
#      L6: conf present + node_id invalid + allow=off -> FATAL
#      L7: conf collision + allow=off -> FATAL (regression spec-0.19)
#      L8: runtime SET cluster.allow_single_node = off -> ERROR (PGC_POSTMASTER)
#      L9 ★ v0.2: conf collision + allow=on (default) -> FATAL
#                  实证 §3.5 B2 boundary "allow=on does NOT swallow malformed conf"
#
# IDENTIFICATION
#    src/test/cluster_tap/t/072_pgrac_conf_multi_node_activation.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ============================================================
# L1: cluster.allow_single_node default value
# ============================================================
my $default = $node->safe_psql('postgres', q{SHOW "cluster.allow_single_node"});
is($default, 'on',
	'L1 cluster.allow_single_node default value = on (Stage 2.1 backward-compat)');


# ============================================================
# L2: conf absent + allow=on (default) -> WARNING + single-node fallback
#     This regresses spec-1.23 baseline single-node OLTP path.  No
#     pgrac.conf, cluster.node_id default = -1; postmaster starts;
#     pg_cluster_nodes returns 1 row marked is_self.
# ============================================================
my $conf_path = $node->data_dir . '/pgrac.conf';
ok(!-e $conf_path,
	'L2 setup: pgrac.conf does not exist (clean init)');

my $node_count = $node->safe_psql('postgres', 'SELECT count(*) FROM pg_cluster_nodes');
is($node_count, '1',
	'L2 conf absent + allow=on -> single-node fallback (1 row in pg_cluster_nodes)');

my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/falling back to single-node mode/i,
	'L2 startup log: pgrac.conf-absent fallback path taken');


# ============================================================
# L3: conf absent + allow=off -> FATAL
#     Set allow_single_node = off in postgresql.conf, restart, verify
#     startup fails with the expected errmsg + errhint wording (spec-2.1
#     v0.2 Q3 REVISED).
# ============================================================
$node->stop;
$node->append_conf('postgresql.conf', "cluster.allow_single_node = off\n");
unlink $node->logfile;
my $start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'L3 conf absent + allow=off -> postmaster refuses to start');

$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/pgrac\.conf is required when cluster\.allow_single_node is off/,
	'L3 startup FATAL errmsg matches spec-2.1 v0.2 Q3 wording');
like($log, qr/Create pgrac\.conf with node entries/,
	'L3 startup FATAL errhint suggests creating pgrac.conf');
like($log, qr/single-node compatibility mode/,
	'L3 startup FATAL errhint mentions compatibility mode');


# ============================================================
# L4: conf present + node_id valid -> no WARNING
#     Restore allow=on, write a valid pgrac.conf, set cluster.node_id=0,
#     restart, verify clean startup with no node_id WARNING.
# ============================================================
$node->adjust_conf('postgresql.conf', 'cluster.allow_single_node', 'on');
PostgreSQL::Test::Utils::append_to_file($conf_path, <<'EOC');
[cluster]
name = pgrac-l4

[node.0]
interconnect_addr = 10.0.0.0:6432
EOC
$node->adjust_conf('postgresql.conf', 'cluster.node_id', '0');
unlink $node->logfile;
$node->start;
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
unlike($log, qr/cluster\.node_id .* is outside the valid range/,
	'L4 conf present + node_id valid -> no node_id WARNING in log');

$node_count = $node->safe_psql('postgres', 'SELECT count(*) FROM pg_cluster_nodes');
is($node_count, '1', 'L4 pg_cluster_nodes returns 1 row (the configured node)');


# ============================================================
# L5: conf present + node_id invalid + allow=on -> WARNING
#     Set cluster.node_id back to -1; allow=on (default); expect WARNING
#     in startup log.
# ============================================================
$node->stop;
$node->adjust_conf('postgresql.conf', 'cluster.node_id', '-1');
unlink $node->logfile;
$node->start;
$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/cluster\.node_id \(-1\) is outside the valid range 0\.\.127/,
	'L5 conf present + node_id invalid + allow=on -> WARNING emitted');
like($log, qr/single-node compatibility mode/,
	'L5 WARNING errhint mentions compatibility mode');


# ============================================================
# L6: conf present + node_id invalid + allow=off -> FATAL
#     Same conf, switch allow=off, expect FATAL.
# ============================================================
$node->stop;
$node->adjust_conf('postgresql.conf', 'cluster.allow_single_node', 'off');
unlink $node->logfile;
$start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'L6 conf present + node_id invalid + allow=off -> postmaster refuses to start');

$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/cluster\.node_id \(-1\) is outside the valid range 0\.\.127/,
	'L6 startup FATAL errmsg matches');
like($log, qr/Set cluster\.node_id .* to an integer 0\.\.127/,
	'L6 startup FATAL errhint suggests setting cluster.node_id');


# ============================================================
# L7: conf collision + allow=off -> FATAL (spec-0.19 regression)
#     Write a conf with duplicate [node.0]; allow=off; expect FATAL.
# ============================================================
unlink $conf_path;
PostgreSQL::Test::Utils::append_to_file($conf_path, <<'EOC');
[node.0]
interconnect_addr = 10.0.0.10:6432

[node.0]
interconnect_addr = 10.0.0.11:6432
EOC
$node->adjust_conf('postgresql.conf', 'cluster.node_id', '0');
unlink $node->logfile;
$start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'L7 conf collision + allow=off -> postmaster refuses to start (spec-0.19 regression)');

$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/duplicate \[node\.0\] section/,
	'L7 startup FATAL log mentions duplicate section');


# ============================================================
# L8: runtime SET cluster.allow_single_node -> ERROR (PGC_POSTMASTER)
#     Need a running postmaster.  Restore valid conf + allow=on first.
# ============================================================
unlink $conf_path;
PostgreSQL::Test::Utils::append_to_file($conf_path, <<'EOC');
[node.0]
interconnect_addr = 10.0.0.0:6432
EOC
$node->adjust_conf('postgresql.conf', 'cluster.allow_single_node', 'on');
unlink $node->logfile;
$node->start;

my ($stdout, $stderr);
$node->psql('postgres',
	q{SET "cluster.allow_single_node" = off},
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot be changed without restarting the server/i,
	'L8 runtime SET cluster.allow_single_node rejected (PGC_POSTMASTER)');


# ============================================================
# L9 ★ v0.2: conf collision + allow=on (default) -> FATAL
#            实证 §3.5 B2 boundary "allow_single_node = on does NOT
#            swallow malformed conf".  This is the design integrity
#            anti-backdoor test.  If implementation incorrectly
#            short-circuits "allow=on => fallback on any error", L9
#            will fail because postmaster will start instead of FATAL.
# ============================================================
$node->stop;
unlink $conf_path;
PostgreSQL::Test::Utils::append_to_file($conf_path, <<'EOC');
[node.0]
interconnect_addr = 10.0.0.20:6432

[node.0]
interconnect_addr = 10.0.0.21:6432
EOC
# allow_single_node = on (default; was already left on at L8)
unlink $node->logfile;
$start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'L9 ★ conf collision + allow=on (default) -> postmaster refuses to start '
	. '(spec-2.1 §3.5 B2 boundary: allow=on does NOT swallow malformed conf)');

$log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
like($log, qr/duplicate \[node\.0\] section/,
	'L9 startup FATAL log mentions duplicate section (boundary invariant enforced)');


done_testing();
