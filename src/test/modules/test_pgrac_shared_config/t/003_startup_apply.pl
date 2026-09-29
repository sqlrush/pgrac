# PGRAC: real early-postmaster configuration application, not serving admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $probe = PostgreSQL::Test::Cluster->new('apply_capability');
$probe->init;
$probe->start;
my $capable = $probe->safe_psql('postgres',
	q{SELECT count(*) FROM pg_settings WHERE name='cluster.node_id'});
$probe->stop('fast');
plan skip_all => 'PGRAC cluster build required; no PRE2 application qualification'
	if $capable eq '0';

sub fixture
{
	my ($name, $body, $node_id, $extra) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf',
		"shared_preload_libraries='test_pgrac_shared_config'\n"
		. "test_pgrac_shared_config.apply_node=$node_id\n"
		. "statement_timeout='7s'\n" . ($extra // ''));
	open(my $fh, '>:raw', $node->data_dir . '/test_config.input') or die $!;
	print {$fh} $body;
	close($fh) or die $!;
	return $node;
}

my $body = "common.statement_timeout='5s'\ncommon.work_mem='6MB'\n"
	. "node000.shared_buffers='32MB'\nnode001.shared_buffers='64MB'\n";
my $node = fixture('apply_node0', $body, 0);
my $started = $node->start(fail_ok => 1);
ok($started, 'production adapter applies common and local settings in real postmaster');
if (!$started)
{
	diag(slurp_file($node->logfile));
	done_testing();
	exit 0;
}
is($node->safe_psql('postgres', 'SHOW statement_timeout'), '5s',
	'shared file overrides local file at same source priority');
is($node->safe_psql('postgres', 'SHOW work_mem'), '6MB', 'native memory units applied');
is($node->safe_psql('postgres', 'SHOW shared_buffers'), '32MB',
	'other node instance stanza does not overwrite selected node');
is($node->safe_psql('postgres', q{
	SELECT setting || '|' || reset_val || '|' || source || '|' || pending_restart
	FROM pg_settings WHERE name='statement_timeout'}),
	'5000|5000|configuration file|false', 'current/reset/source refer to applied native value');
like(slurp_file($node->logfile), qr/node=0 entries=3 generation=1/,
	'exact local receipt counts only applied entries');
is($node->safe_psql('postgres', q{SET statement_timeout='1s'; SHOW statement_timeout;
	RESET statement_timeout; SHOW statement_timeout}), "1s\n5s",
	'legal session SET and RESET still use native semantics');
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my ($stdout, $stderr);
my $rc = $node->psql('postgres', 'SELECT test_pgrac_config_backend_apply()',
	stdout => \$stdout, stderr => \$stderr);
isnt($rc, 0, 'backend cannot reuse startup-only application');
like($stderr, qr/shared configuration application requires early startup/,
	'backend refusal is classified before assigning');
is($node->safe_psql('postgres', 'SHOW statement_timeout'), '5s',
	'refusal leaves the postmaster and new sessions intact');
$node->stop('fast');

my $other = fixture('apply_node1', $body, 1);
$other->start;
is($other->safe_psql('postgres', 'SHOW shared_buffers'), '64MB',
	'same object selects a different exact local node');
$other->stop('fast');

sub refused
{
	my ($name, $bytes, $node_id, $extra, $reason) = @_;
	my $bad = fixture($name, $bytes, $node_id, $extra);
	ok(!$bad->start(fail_ok => 1), "$name refuses startup");
	my $log = slurp_file($bad->logfile);
	like($log, $reason, "$name reports the exact refusal");
	unlike($log, qr/test shared configuration applied:/, "$name emits no application receipt");
}
refused('apply_commit_ts_on', "common.track_commit_timestamp='on'\n", 0, '',
	qr/shared configuration object is not applicable/);
refused('apply_inherited_commit_ts_on', "common.work_mem='6MB'\n", 0,
	"track_commit_timestamp=on\n",
	qr/shared configuration requires track_commit_timestamp=off/);
my $standalone = PostgreSQL::Test::Cluster->new('native_commit_ts_on');
$standalone->init;
$standalone->append_conf('postgresql.conf', "track_commit_timestamp=on\n");
$standalone->start;
is($standalone->safe_psql('postgres', 'SHOW track_commit_timestamp'), 'on',
	'ordinary non-shared native PG can still enable commit-ts');
$standalone->stop('fast');

refused('apply_bad_hash', $body, 0, "test_pgrac_shared_config.bad_hash=on\n",
	qr/shared configuration object is not applicable/);
refused('apply_bad_node', $body, 2, '', qr/shared configuration node is not configured/);
refused('apply_unknown', "common.statement_timeout='5s'\ncommon.zzz_no_such_parameter='1'\n",
	0, '', qr/shared configuration object is not applicable/);
refused('apply_bad_value', "common.statement_timeout='bad'\n", 0, '',
	qr/shared configuration assignment failed/);
refused('apply_foreign_value', "common.statement_timeout='5s'\nnode001.port='99999'\n",
	0, '', qr/shared configuration native validation failed/);
refused('apply_bad_format', "common.statement_timeout='5s'", 0, '',
	qr/shared configuration object is not applicable/);
refused('apply_secret', "common.primary_conninfo='password=not-a-real-secret'\n",
	0, '', qr/shared configuration object is not applicable/);

my $priority = fixture('apply_priority', $body, 0);
command_fails_like(['postgres', '-D', $priority->data_dir, '-c', 'work_mem=8MB'],
	qr/shared configuration conflicts with a higher-priority source/,
	'command-line source cannot silently overrule shared configuration');

# Verify cross-GUC ordering, then exit before any storage initialization. These
# are inert isolated fixtures, not a permitted shared undo/storage deployment.
my $dependent = fixture('apply_dependent',
	"common.cluster.shared_data_dir='/pgrac-test-path-not-created'\n"
	. "common.cluster.undo_gcs_coherence='on'\n", 0,
	"test_pgrac_shared_config.stop_after_apply=on\n");
ok(!$dependent->start(fail_ok => 1), 'dependent fixture stops before storage initialization');
like(slurp_file($dependent->logfile), qr/node=0 entries=2 generation=1/,
	'dependent native check sees the newly applied shared path');
like(slurp_file($dependent->logfile), qr/test application completed; no storage initialization requested/,
	'test-only stop follows complete application, not product refusal');
ok(!-e '/pgrac-test-path-not-created', 'dependent fixture creates no shared storage path');
done_testing();
