# PGRAC: process-local shared configuration reload through the native GUC engine.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('config_reload');
$node->init;
$node->start;
if ($node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.node_id'}) eq '0')
{
	$node->stop('fast');
	plan skip_all => 'PGRAC cluster build required; not PRE2 qualification';
}
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my $old = "common.work_mem='8MB'\n";
my $new = "common.work_mem='12MB'\n";
my $state = q{SELECT setting,reset_val,source FROM pg_settings WHERE name='work_mem'};
sub quote_sql
{
	my ($value) = @_;
	$value =~ s/'/''/g;
	return "'$value'";
}
sub call_sql
{
	my ($before, $after, $fault, $id) = @_;
	return 'SELECT test_pgrac_config_reload(' . quote_sql($before) . ','
		. quote_sql($after) . ',' . (defined $id ? $id : 0) . ','
		. quote_sql(defined $fault ? $fault : '') . ');';
}
sub reload_state
{
	my ($before, $after, $fault) = @_;
	return $node->safe_psql('postgres', call_sql($before, $after, $fault) . $state);
}
is(reload_state($old, $new), "1:0:1:0:0:0\n12288|12288|configuration file",
	'newer generation can skip intermediates and sets native FILE default');
is(reload_state($old, ''), "1:0:0:1:0:0\n4096|4096|default",
	'RESET removes the FILE source instead of restoring its old reset value');
is(reload_state($old, $new, 'session'), "1:0:1:0:0:0\n21504|12288|session",
	'legal session SET wins while new FILE reset value is installed');
is(reload_state($old, '', 'session'), "1:0:0:1:0:0\n21504|4096|session",
	'RESET preserves session override and removes its underlying FILE default');
is($node->safe_psql('postgres', 'BEGIN;' . call_sql($old, $new, 'local')
	. $state . ';COMMIT;' . $state),
	"1:0:1:0:0:0\n21504|12288|session\n12288|12288|configuration file",
	'SET LOCAL remains until commit then reveals the new FILE value');
is($node->safe_psql('postgres', 'BEGIN;' . call_sql($old, '', 'local')
	. $state . ';ROLLBACK;' . $state),
	"1:0:0:1:0:0\n21504|4096|session\n4096|4096|default",
	'SET LOCAL rollback cannot resurrect the removed FILE value');
is(reload_state($old, $old, 'equal'), "1:0:1:0:0:0\n8192|8192|configuration file",
	'same generation and hash is an idempotent application');
is(reload_state($old, $new, 'equal'), "0:1:0:0:0:0\n8192|8192|configuration file",
	'same generation with different object refuses before mutation');
for my $fault ('oldhash', 'newhash', 'backwards')
{
	is(reload_state($old, $new, $fault), "0:1:0:0:0:0\n8192|8192|configuration file",
		"$fault refuses before assignment and has no receipt");
}
is(reload_state($old, $new . "node001.port='99999'\n"),
	"0:8:0:0:0:0\n8192|8192|configuration file",
	'invalid remote-node entry cannot partially apply the valid local change');
my $port = $node->port;
my $other_port = $port == 6543 ? 6544 : 6543;
is($node->safe_psql('postgres', call_sql("node000.port='$port'\n",
	"node000.port='$other_port'\n")
	. q{SELECT setting,pending_restart FROM pg_settings WHERE name='port'}),
	"1:0:0:0:1:0\n$port|t",
	'POSTMASTER change is pending restart, not active or applied');
is($node->safe_psql('postgres', call_sql("node000.port='$port'\n", '')
	. q{SELECT setting,pending_restart FROM pg_settings WHERE name='port'}),
	"1:0:0:0:1:0\n$port|t",
	'POSTMASTER removal also remains explicitly pending restart');
is($node->safe_psql('postgres', call_sql("node000.port='$port'\n", '')
	. call_sql('', $new)
	. q{SELECT setting,pending_restart FROM pg_settings WHERE name='port'}),
	"1:0:0:0:1:0\n1:0:1:0:0:0\n$port|t",
	'operation counts are not cumulative ACK: later unrelated change retains restart obligation');
is($node->safe_psql('postgres', call_sql("common.cluster.enabled='off'\n", '')),
	'0:6:0:0:0:0', 'cold identity deletion cannot enter the reload path');
is($node->safe_psql('postgres', call_sql('', "node001.port='$other_port'\n")
	. q{SELECT setting,pending_restart FROM pg_settings WHERE name='port'}),
	"1:0:0:0:0:0\n$port|f", 'other node value is validated but not locally applied');
is($node->safe_psql('postgres', call_sql('', "common.ignore_system_indexes='on'\n")
	. q{SELECT setting FROM pg_settings WHERE name='ignore_system_indexes'}),
	"1:0:0:0:0:1\noff", 'BACKEND setting is deferred, never marked active');
is($node->safe_psql('postgres', call_sql($old, $new, '', 2)),
	'0:1:0:0:0:0', 'unconfigured node cannot consume a configuration');

# A per-operation count cannot prove that an older removed setting has become
# active. Keep each sequence in one actual backend, including the empty diff.
my $old_port = "node000.port='$port'\n";
my $new_port = "node000.port='$other_port'\n";
is($node->safe_psql('postgres', call_sql($old_port, $new_port, 'totals')),
	'1:0:1:0', 'cumulative status includes a POSTMASTER value not yet active');
is($node->safe_psql('postgres', call_sql($old_port, '', 'totals')
	. call_sql('', $new, 'totals')),
	"1:0:1:0\n1:0:1:0", 'removed restart obligation survives an unrelated generation');
is($node->safe_psql('postgres', call_sql($old_port, '', 'totals')
	. call_sql('', '', 'totals') . call_sql('', $old_port, 'totals')),
	"1:0:1:0\n1:0:1:0\n1:0:0:0",
	'empty reload retains pending removal until the actual value is restored');
is($node->safe_psql('postgres', call_sql('', $new_port, 'totals')
	. call_sql('', "common.max_connections='103'\n", 'totals')
	. call_sql('', $old_port, 'totals')),
	"1:0:1:0\n1:0:2:0\n1:0:1:0", 'restoring one static value cannot clear a different pending setting');
is($node->safe_psql('postgres', call_sql('', "common.ignore_system_indexes='on'\n", 'totals')
	. call_sql("common.ignore_system_indexes='on'\n", '', 'totals')
	. call_sql('', $new, 'totals')),
	"1:0:0:1\n1:0:0:1\n1:0:0:1",
	'existing child retains deferred removal across unrelated generations');
is($node->safe_psql('postgres', call_sql('', "node001.port='$other_port'\n", 'totals')),
	'1:0:0:0', 'remote-only pending change does not create a local obligation');
is($node->safe_psql('postgres', call_sql(
	"common.check_function_bodies='on'\ncommon.cluster.native_config_reload_failure='1'\n",
	"common.check_function_bodies='off'\ncommon.cluster.native_config_reload_failure='2'\n",
	'assign') . q{SELECT setting FROM pg_settings WHERE name='check_function_bodies'}),
	"0:8:0:0:0:0\noff",
	'late native hook error preserves no receipt, restores ownership, and admits partial assignment');

# The factoring also runs in ordinary ProcessConfigFile. Prove its native
# reload/remove path without shared configuration, not just the new consumer.
$node->append_conf('postgresql.conf', "work_mem='9MB'");
$node->reload;
$node->poll_query_until('postgres', q{SELECT reset_val='9216' FROM pg_settings WHERE name='work_mem'})
	or die 'ordinary reload did not apply FILE value';
is($node->safe_psql('postgres', $state), '9216|9216|configuration file',
	'ordinary non-shared file reload still applies native settings');
my $conf = $node->data_dir . '/postgresql.conf';
my $contents = slurp_file($conf);
$contents =~ s/^work_mem='9MB'\n//m or die 'ordinary test setting missing';
open my $file, '>', $conf or die "cannot rewrite disposable test config: $!";
print {$file} $contents;
close $file or die "cannot close disposable test config: $!";
$node->reload;
$node->poll_query_until('postgres', q{SELECT reset_val='4096' FROM pg_settings WHERE name='work_mem'})
	or die 'ordinary reload did not remove FILE value';
is($node->safe_psql('postgres', $state), '4096|4096|default',
	'ordinary non-shared file removal still restores the native default');
$node->stop('fast');
done_testing();
