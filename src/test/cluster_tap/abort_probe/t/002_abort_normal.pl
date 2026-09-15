# Author: SqlRush <sqlrush@gmail.com>
# Positive/native mode boundary checks.  No injected authority or return code.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $library = $ENV{PGRAC_ABORT_PROBE_LIB} // '$libdir/test_pgrac_abort_owner';
die 'unsafe module path' unless $library =~ m{\A[\w/.\$\-]+\z};
for my $enabled ('on', 'off')
{
	my $node = PostgreSQL::Test::Cluster->new("normal_$enabled");
	my $node_id = $enabled eq 'on' ? 0 : -1;
	$node->init;
	$node->append_conf('postgresql.conf', qq{
cluster.enabled=$enabled
cluster.node_id=$node_id
cluster.allow_single_node=on
cluster.interconnect_tier=stub
cluster.quorum_poll_interval_ms=2000
autovacuum=off
});
	$node->start;
	$node->safe_psql('postgres', qq{
		CREATE FUNCTION abort_arm(integer,boolean) RETURNS void
		AS '$library','test_pgrac_abort_arm' LANGUAGE C STRICT;
	});
	# Multiple true top-level terminals on the same backend must clear the
	# process-local guard, including a recovered subtransaction.
	my ($rc, $out, $err) = $node->psql('postgres', q{
		BEGIN;
		SELECT abort_arm(0,false);
		SELECT pg_advisory_xact_lock(86601);
		ROLLBACK;
		BEGIN;
		SAVEPOINT s;
		SELECT 1/0;
		ROLLBACK TO SAVEPOINT s;
		SELECT 'SUBXACT_OK';
		COMMIT;
		BEGIN;
		SELECT pg_advisory_xact_lock(86601);
		ROLLBACK;
		BEGIN;
		SELECT 'NEXT_TX_OK';
		COMMIT;
	}, on_error_stop => 0);
	is($rc, 0, "$enabled normal terminals preserve the same backend");
	like($err, qr/division by zero/, "$enabled actually executes subtransaction failure");
	like($out, qr/SUBXACT_OK/, "$enabled savepoint recovers without top-level abort");
	like($out, qr/NEXT_TX_OK/, "$enabled guard does not contaminate the next transaction");
	is($node->safe_psql('postgres', q{SELECT pg_try_advisory_xact_lock(86601)}),
		't', "$enabled transaction lock is released");

	# Actual self-SIGINT through pg_cancel_backend, not a synthetic error.
	($rc, $out, $err) = $node->psql('postgres', q{
		BEGIN;
		SELECT abort_arm(0,false);
		SELECT pg_advisory_xact_lock(86602);
		SELECT pg_cancel_backend(pg_backend_pid());
		SELECT pg_sleep(0.1);
		ROLLBACK;
		BEGIN;
		SELECT 'AFTER_CANCEL_OK';
		COMMIT;
	}, on_error_stop => 0);
	is($rc, 0, "$enabled canceled backend remains usable");
	like($err, qr/canceling statement due to user request/,
		"$enabled actual cancellation reaches normal error recovery");
	like($out, qr/AFTER_CANCEL_OK/, "$enabled same connection commits after cancel");
	is($node->safe_psql('postgres', q{SELECT pg_try_advisory_xact_lock(86602)}),
		't', "$enabled cancellation releases its transaction lock");
	unlike(slurp_file($node->logfile), qr/PANIC:|PGRAC_REASON=ABORT_COMPLETION_FAILED/,
		"$enabled ordinary errors and successful aborts do not fail-stop");
	$node->stop;
}
done_testing();
