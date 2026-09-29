# PGRAC: native persistent common values and ordinary online reload.
# The selected source is injected; actual postmaster, logger, children, native
# GUC assignment and clean restart run. This is not cluster DATA admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('static_common_delivery');
$node->init;
$node->append_conf('postgresql.conf',
 "shared_preload_libraries='test_pgrac_shared_config'\n"
 . "test_pgrac_shared_config.apply_node=0\n"
 . "test_pgrac_shared_config.delivery=on\nlogging_collector=on\n"
 . "max_connections=10\nwork_mem='6MB'\n");
sub body
{
 my ($cache, $memory, $connections) = @_;
 return (defined $cache ? "common.cluster.read_scache='$cache'\n" : '')
  . "common.max_connections='$connections'\ncommon.work_mem='$memory'\n";
}
sub write_fixture
{
 my ($name, $bytes) = @_;
 open(my $out, '>', $node->data_dir . "/test_config.$name") or die $!;
 print {$out} $bytes;
 close($out) or die $!;
}
write_fixture('input', body('off', '6MB', 10));
$node->start;
$node->safe_psql('postgres',
 'CREATE EXTENSION test_pgrac_shared_config; CREATE TABLE tx_marker(v int)');
sub publish
{
 my ($generation, $image) = @_;
 write_fixture('reload', $image);
 $node->append_conf('postgresql.conf',
  "test_pgrac_shared_config.reload_generation=$generation\n");
 $node->reload;
 ok($node->poll_query_until('postgres', qq{
  SELECT split_part(test_pgrac_config_process(0, ''), ':', 1) = '$generation'
 }), "new child inherits native parent generation $generation");
}
my $old = $node->background_psql('postgres');
my $receive = 'SELECT test_pgrac_config_delivery_receive()';
my $cache = q{SELECT setting,pending_restart,context FROM pg_settings WHERE name='cluster.read_scache'};
my $ref = q{SELECT split_part(test_pgrac_config_process(0,''),':',1)};
is($old->query_safe($cache), 'off|f|postmaster',
 'managed common SIGHUP parameter exposes its effective static context');
$old->query_safe('BEGIN; INSERT INTO tx_marker VALUES(1); SAVEPOINT s');
publish(2, body('on', '9MB', 10));
is($old->query_safe($receive), 't', 'active transaction consumes persistent target without a cut');
is($old->query_safe($cache), 'off|t|postmaster', 'old transaction retains actual common value');
is($old->query_safe('SHOW work_mem'), '9MB', 'ordinary default reload remains online during work');
is($old->query_safe($ref), '2', 'consumed durable reference is not proof of active common target');
is($node->safe_psql('postgres', $cache), 'off|t|postmaster',
 'new child inherits old running common value and pending restart');
$old->query_safe('ROLLBACK TO s; RELEASE s; COMMIT');
is($old->query_safe($cache), 'off|t|postmaster', 'COMMIT cannot turn pending into an online apply');
is($node->safe_psql('postgres', 'SELECT sum(v) FROM tx_marker'), '1',
 'real transaction committed without coordinator holds');

$old->query_safe("BEGIN; UPDATE tx_marker SET v=2; SET LOCAL work_mem='13MB'");
publish(3, body('on', '11MB', 14));
is($old->query_safe($receive), 't', 'next persistent target applies ordinary reset default');
is($old->query_safe('SHOW work_mem'), '13MB', 'native SET LOCAL remains authoritative');
$old->query_safe('ROLLBACK');
is($old->query_safe('SHOW work_mem'), '11MB', 'rollback reveals new FILE default');
is($node->safe_psql('postgres', 'SELECT sum(v) FROM tx_marker'), '1', 'native rollback is preserved');
is($old->query_safe($cache), 'off|t|postmaster', 'ROLLBACK cannot apply pending common protocol');

publish(4, body(undef, '12MB', 14));
is($old->query_safe($receive), 't', 'RESET removal is consumed without changing common memory');
is($old->query_safe($cache), 'off|t|postmaster', 'RESET of common FILE entry remains pending');
publish(5, body(undef, '15MB', 14));
is($old->query_safe($receive), 't', 'later ordinary-only target is consumed');
is($old->query_safe($cache), 'off|t|postmaster', 'unrelated target cannot erase pending removal');
publish(6, body('off', '15MB', 14));
is($old->query_safe($receive), 't', 'same running common value is consumed');
is($old->query_safe($cache), 'off|f|postmaster', 'canonical same value clears only its pending bit');
is($old->query_safe(q{SELECT setting,pending_restart FROM pg_settings WHERE name='max_connections'}),
 '10|t', 'independent original static restart debt remains');
is($old->query_safe(q{DO $$ BEGIN
 PERFORM set_config('cluster.read_scache','on',false);
 RAISE EXCEPTION 'unexpected online static assignment';
EXCEPTION WHEN SQLSTATE '55P02' THEN NULL; END $$; SELECT 1}), '1',
 'direct memory assignment cannot bypass the static contract');

publish(7, body('on', '15MB', 14));
is($old->query_safe($receive), 't', 'final durable change is ready for a coordinated restart');
$old->quit;
$node->stop('fast');
# Standalone clean-restart consumer proof, not a four-node shutdown certificate.
write_fixture('input', body('on', '15MB', 14));
$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=0\n");
$node->start;
is($node->safe_psql('postgres', $cache), 'on|f|postmaster',
 'new native lifetime applies selected common value and clears pending');
is($node->safe_psql('postgres', 'SHOW max_connections'), '14',
 'new lifetime applies original static value as well');
is($node->safe_psql('postgres', 'SELECT sum(v) FROM tx_marker'), '1',
 'normal restart keeps the same data and committed result');
$node->stop('fast');
done_testing();
