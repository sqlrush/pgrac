#-------------------------------------------------------------------------
#
# 434_cluster_prepare_refusal.pl
#    Outside shared mode, cluster mode refuses PREPARE TRANSACTION.
#
#      L1   PREPARE TRANSACTION is refused with SQLSTATE 0A000, the
#           CLUSTER_SCOPE detail and the hint; as for PostgreSQL's own
#           PREPARE refusals the transaction rolls back and its block
#           ends, so the next statement runs without a ROLLBACK
#      L2   COMMIT PREPARED / ROLLBACK PREPARED are not refused: an
#           unknown identifier gets PostgreSQL's own 42704
#      L3   with cluster.enabled = off two-phase commit works unchanged
#      L4   a transaction prepared with cluster.enabled = off is still
#           finished by COMMIT PREPARED after a restart in cluster mode
#
# IDENTIFICATION
#    src/test/cluster_tap/t/434_cluster_prepare_refusal.pl
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

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $cluster_conf = "cluster.enabled = on\n"
  . "cluster.node_id = 0\n"
  . "cluster.allow_single_node = on\n"
  . "max_prepared_transactions = 10\n";

my $node = PgracClusterNode->new('prepare_refusal');
$node->init;
$node->append_conf('postgresql.conf', $cluster_conf);
$node->start;

sub prepared_count {
	my ($n) = @_;
	return $n->safe_psql('postgres', 'SELECT count(*) FROM pg_prepared_xacts');
}

# L1: refused before any prepare work, with the stable code, detail and hint.
my ($rc, $stdout, $stderr) = $node->psql(
	'postgres',
	"\\set VERBOSITY verbose\nBEGIN;\nCREATE TABLE pr_t (i int);\n"
	  . "PREPARE TRANSACTION 'g1';\nSELECT 'after';\n",
	on_error_stop => 0);
like($stderr, qr/ERROR:\s+0A000: two-phase transactions are not supported in cluster mode/,
	'L1 PREPARE TRANSACTION refused with SQLSTATE 0A000');
like($stderr, qr/PGRAC_FAMILY=CLUSTER_SCOPE PGRAC_REASON=OPERATION_UNSUPPORTED/,
	'L1 refusal carries the CLUSTER_SCOPE detail');
like($stderr, qr/HINT:\s+Use COMMIT or ROLLBACK instead/, 'L1 refusal carries the hint');
is(prepared_count($node), '0', 'L1 nothing was prepared');
like($stdout, qr/^after$/m, 'L1 the transaction block ended with the refusal');
unlike($stderr, qr/current transaction is aborted/, 'L1 no aborted block is left behind');
is($node->safe_psql('postgres', "SELECT to_regclass('pr_t') IS NULL"),
	't', 'L1 the refused transaction rolled back');
is($node->safe_psql('postgres', 'SELECT 42'), '42', 'L1 the server keeps serving');

# L2: the finish statements are not refused by the cluster check.
($rc, $stdout, $stderr) = $node->psql('postgres',
	"\\set VERBOSITY verbose\nROLLBACK PREPARED 'nope';\n");
like($stderr, qr/ERROR:\s+42704: prepared transaction with identifier "nope" does not exist/,
	'L2 ROLLBACK PREPARED reaches PostgreSQL');
unlike($stderr, qr/not supported in cluster mode/, 'L2 ROLLBACK PREPARED is not refused');
($rc, $stdout, $stderr) = $node->psql('postgres',
	"\\set VERBOSITY verbose\nCOMMIT PREPARED 'nope';\n");
like($stderr, qr/ERROR:\s+42704: prepared transaction with identifier "nope" does not exist/,
	'L2 COMMIT PREPARED reaches PostgreSQL');
$node->stop;

# L3: without cluster mode, PostgreSQL's two-phase commit is unchanged.
$node->append_conf('postgresql.conf', "cluster.enabled = off\n");
$node->start;
$node->safe_psql('postgres', "BEGIN; SELECT 1; PREPARE TRANSACTION 'native1';");
is(prepared_count($node), '1', 'L3 PREPARE TRANSACTION works with cluster.enabled = off');
$node->safe_psql('postgres', "COMMIT PREPARED 'native1';");
is(prepared_count($node), '0', 'L3 COMMIT PREPARED works with cluster.enabled = off');

# L4: a prepared transaction never strands when cluster mode comes back.
$node->safe_psql('postgres', "BEGIN; SELECT 1; PREPARE TRANSACTION 'carried';");
$node->stop;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->start;
is(prepared_count($node), '1', 'L4 the prepared transaction survives the restart');
$node->safe_psql('postgres', "COMMIT PREPARED 'carried';");
is(prepared_count($node), '0', 'L4 COMMIT PREPARED finishes it in cluster mode');

$node->stop;
done_testing();
