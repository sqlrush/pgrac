# Native ERROR/FATAL/LIFO regression, independent of micro/soak/PRE budgets.
# Every negative has a fresh disposable instance.  No recovery claim is made.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $library = $ENV{PGRAC_ABORT_PROBE_LIB} // '$libdir/test_pgrac_abort_owner';
die 'unsafe module path' unless $library =~ m{\A[\w/.\$\-]+\z};
my @cases = (
	['rollback_error', 1, 0, 'ROLLBACK'],
	['commit_error', 1, 1, 'COMMIT'],
	['rollback_fatal', 2, 0, 'ROLLBACK'],
	['shutdown_fatal', 2, 0, 'SELECT abort_die()'],
	['reentry', 3, 0, 'ROLLBACK'],
);
for my $case (@cases)
{
	my ($name, $mode, $commit, $end) = @$case;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf', qq{
cluster.enabled=on
cluster.node_id=0
cluster.allow_single_node=on
cluster.interconnect_tier=stub
cluster.quorum_poll_interval_ms=2000
autovacuum=off
restart_after_crash=off
});
	$node->start;
	$node->safe_psql('postgres', qq{
		CREATE FUNCTION abort_arm(integer,boolean) RETURNS void
		AS '$library','test_pgrac_abort_arm' LANGUAGE C STRICT;
		CREATE FUNCTION abort_die() RETURNS void
		AS '$library','test_pgrac_abort_die' LANGUAGE C STRICT;
	});
	my ($rc, $out, $err) = $node->psql('postgres',
		"BEGIN; SELECT abort_arm($mode," . ($commit ? 'true' : 'false') . "); $end;",
		timeout => 30);
	isnt($rc, 0, "$name cannot return successful terminal result");
	# Read after the client has observed the terminal process outcome.  The
	# callback and PANIC log are emitted synchronously before disconnection.
	my $log = slurp_file($node->logfile);
	like($log, qr/PGRAC_REASON=ABORT_COMPLETION_FAILED/, "$name has explicit fail-stop owner");
	unlike($log, qr/error stack size exceeded/, "$name has no recursive error-stack overflow");
	unlike($log, qr/abort probe entered callback; mode=\d attempt=2/,
		"$name never retries the failed abort consumer");
	unlike($log, qr/abort probe reached low-level cleanup; fires=[1-9]/,
		"$name does not hand failed ownership to low-level cleanup");
	$node->stop('immediate');
}
done_testing();
