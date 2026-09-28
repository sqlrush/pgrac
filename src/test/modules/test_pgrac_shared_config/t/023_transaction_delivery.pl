# PGRAC: actual native transaction boundaries during shared default delivery.
# A mid-transaction common assignment, lost retry, or global default-only hold
# must fail these tests. No fake transaction/process state or distributed ACK.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('transaction_delivery');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\n"
	. "test_pgrac_shared_config.delivery=on\nlogging_collector=on\nmax_connections=10\nwork_mem='6MB'\n");
sub body
{
	my ($handoff, $work_mem, $connections) = @_;
	return (defined $handoff ? "common.cluster.ges_handoff='$handoff'\n" : '')
		. "common.max_connections='$connections'\ncommon.work_mem='$work_mem'\n";
}
append_to_file($node->data_dir . '/test_config.input', body('off', '6MB', 10));
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config; CREATE TABLE tx_marker(v int)');
sub publish
{
	my ($generation, $image) = @_;
	open(my $fixture, '>', $node->data_dir . '/test_config.reload') or die $!;
	print {$fixture} $image;
	close($fixture) or die $!;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=$generation\n");
	$node->reload;
	ok($node->poll_query_until('postgres', qq{
	 SELECT split_part(test_pgrac_config_delivery(), ':', 1) = '$generation'
	}), "actual parent and detached logger consume generation $generation");
}
my $old = $node->background_psql('postgres', on_error_stop => 0);
my $receive = 'SELECT test_pgrac_config_delivery_receive()';
my $ref = q{SELECT split_part(test_pgrac_config_process(0, ''), ':', 1)};
$old->query_safe('BEGIN; INSERT INTO tx_marker VALUES (1); SAVEPOINT s');
publish(2, body('on', '9MB', 10));
is($old->query_safe($receive), 'f', 'real in-progress transaction defers common dynamic application');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'old transaction keeps old protocol setting');
is($old->query_safe('SHOW work_mem'), '6MB', 'whole delayed image is unapplied, not partly assigned');
is($old->query_safe($ref), '1', 'no new consumed-reference proof while the transaction owns old work');
is($node->safe_psql('postgres', 'SHOW cluster.ges_handoff'), 'on',
	'new child inherits actual parent value; this is not a DATA admission test');
$old->query_safe('ROLLBACK TO s; RELEASE s');
is($old->query_safe($receive), 'f', 'subtransaction completion is not top-level quiescence');
$old->query_safe('COMMIT');
is($old->query_safe('SHOW cluster.ges_handoff'), 'on', 'native loop retries after COMMIT without another SIGHUP');
is($old->query_safe('SHOW work_mem'), '9MB', 'delayed ordinary default also applied after COMMIT');
is($old->query_safe($ref), '2', 'actual consumed proof advances only after application');
is($node->safe_psql('postgres', 'SELECT sum(v) FROM tx_marker'), '1', 'old transaction really committed');

$old->query_safe("BEGIN; UPDATE tx_marker SET v=2; SET LOCAL work_mem='13MB'");
publish(3, body('off', '9MB', 10));
is($old->query_safe($receive), 'f', 'second real transaction also retains its old protocol');
is($old->query_safe('SHOW work_mem'), '13MB', 'LOCAL stack is preserved during deferral');
$old->query_safe('ROLLBACK');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'native loop retries after ROLLBACK without another SIGHUP');
is($node->safe_psql('postgres', 'SELECT sum(v) FROM tx_marker'), '1', 'rollback was not blocked or fabricated');

$old->query_safe('BEGIN');
publish(4, body('off', '11MB', 10));
is($old->query_safe($receive), 't', 'default-only generation needs no transaction hold');
is($old->query_safe('SHOW work_mem'), '11MB', 'ordinary default changes retain native reload behavior');
publish(5, body('off', '11MB', 14));
is($old->query_safe($receive), 't', 'static-only target is native pending, not a dynamic cutover');
is($old->query_safe('SHOW max_connections'), '10', 'pending static target never becomes active online');
is($old->query_safe(q{SELECT pending_restart FROM pg_settings WHERE name='max_connections'}), 't',
	'actual native pending_restart remains visible');
$old->query_safe('SET LOCAL cluster.ges_handoff=on');
publish(6, body('off', '12MB', 14));
is($old->query_safe($receive), 't', 'unrelated defaults do not prohibit a legal native SET LOCAL');
is($old->query_safe('SHOW cluster.ges_handoff'), 'on', 'native LOCAL override is not replaced by file default');
$old->query_safe('COMMIT');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'native LOCAL restoration is unchanged');

$old->query_safe('BEGIN');
publish(7, body('on', '12MB', 14));
is($old->query_safe($receive), 'f', 'next common dynamic target waits for the old transaction');
publish(8, body('off', '15MB', 14));
is($old->query_safe($receive), 't', 'new accepted target supersedes the delayed one using actual old image');
$old->query_safe('COMMIT');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'retired delayed generation cannot apply later');
is($old->query_safe('SHOW work_mem'), '15MB', 'latest target wins, not a stale retained callback');
is($old->query_safe($ref), '8', 'consumed ref is the real newest target');

$old->query_safe('BEGIN');
publish(9, body(undef, '15MB', 14));
is($old->query_safe($receive), 'f', 'RESET/removal of a common dynamic default also waits');
is($old->query_safe($ref), '8', 'removal cannot advance proof before actual application');
$old->query_safe('ROLLBACK');
is($old->query_safe($ref), '9', 'native pending retry consumes RESET after rollback');

my $old_pid = $old->query_safe('SELECT pg_backend_pid()');
$old->query_safe('BEGIN');
my (undef, $error_rc) = $old->query('SELECT 1/0');
is($error_rc, 1, 'fixture enters a real aborted transaction block');
like($old->{stderr}, qr/division by zero/, 'expected native arithmetic error, not an unrelated failure');
$old->{stderr} = '';
publish(10, body('off', '17MB', 14));
my (undef, $still_aborted) = $old->query('SELECT 1');
is($still_aborted, 1, 'aborted block remains native, not an idle permission');
like($old->{stderr}, qr/current transaction is aborted/, 'native failed-block error is retained');
$old->{stderr} = '';
is($node->safe_psql('postgres', qq{
 SELECT split_part(test_pgrac_config_enrollment($old_pid), ':', 3)
}), '9', 'aborted block did not consume common configuration during command dispatch');
is($node->safe_psql('postgres', qq{
 SELECT split_part(test_pgrac_config_enrollment($old_pid), ':', 4)
}), '0', 'deferred application is not a sticky failed process');
$old->query_safe('ROLLBACK');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'aborted-block rollback releases the native retry');
is($old->query_safe($ref), '10', 'native proof advances after aborted-block cleanup');

# AND CHAIN starts another top-level transaction inside the same native command,
# without returning to postgres.c's protocol-message reload check.
$old->query_safe('BEGIN');
publish(11, body('on', '17MB', 14));
is($old->query_safe($receive), 'f', 'chain fixture holds a real pending dynamic target');
is($old->query_safe(q{SELECT 'COMMIT AND CHAIN; SHOW cluster.ges_handoff' \gexec}), 'on',
	'COMMIT AND CHAIN consumes target before its new transaction');
$old->query_safe('ROLLBACK');
$old->query_safe('BEGIN');
publish(12, body('off', '17MB', 14));
is($old->query_safe($receive), 'f', 'rollback-chain fixture owns the previous actual target');
$old->query_safe('ROLLBACK AND CHAIN');
is($old->query_safe('SHOW cluster.ges_handoff'), 'off', 'ROLLBACK AND CHAIN also reaches the real idle retry boundary');
is($old->query_safe($ref), '12', 'rollback-chain proof comes from application, not a coincidentally equal value');
$old->query_safe('ROLLBACK');

$old->query_safe('BEGIN');
publish(13, body('on', '17MB', 14));
is($old->query_safe($receive), 'f', 'multi-query message starts with delayed configuration');
is($old->query_safe(q{SELECT 'COMMIT; BEGIN; SHOW cluster.ges_handoff' \gexec}), 'on',
	'one simple-query message cannot skip the post-COMMIT retry boundary');
$old->query_safe('ROLLBACK');
$old->quit;
$node->stop('fast');
done_testing();
