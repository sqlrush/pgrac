# PGRAC: actual pre-use configuration for an old fork and future native work.
# Local test binding is not a distributed configuration application proof.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('future_native_use');
$node->init;
$node->append_conf('postgresql.conf', "shared_preload_libraries='test_pgrac_shared_config'\n"
	. "test_pgrac_shared_config.apply_node=0\ntest_pgrac_shared_config.delivery=on\n"
	. "autovacuum=off\nmax_connections=12\ncluster.read_scache=off\n");
append_to_file($node->data_dir . '/test_config.input', "common.cluster.read_scache='off'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_pgrac_shared_config');
my ($request, $bind) = (0, 0);
sub await_file
{
	my ($name, $pattern) = @_;
	my $path = $node->data_dir . "/test_config.$name";
	for (1..400) {
		if (-e $path) {
			my $text = slurp_file($path);
			return $text if $text =~ $pattern;
		}
		select(undef, undef, undef, 0.025);
	}
	die "native fixture did not reach $name";
}
sub control
{
	my ($mode) = @_;
	++$request;
	++$request while $request % 3 != $mode;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.gate_request=$request\n");
	$node->reload;
	my $value = await_file('gate', qr/^$request:/);
	chomp $value;
	my @values = split /:/, $value;
	return \@values;
}
sub bind_parent
{
	my ($generation) = @_;
	++$bind;
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.gate_bind=$bind\n");
	$node->reload;
	is(await_file('future_bind', qr/^$bind:/), "$bind:1:$generation\n",
		"real parent common values bound at generation $generation");
}
sub publish
{
	my ($generation, $body) = @_;
	open(my $file, '>', $node->data_dir . '/test_config.reload') or die $!;
	print {$file} $body;
	close($file) or die $!;
	# This isolated native fixture does not activate shared-config startup.
	# Its control SIGHUP still reads the native local file. Mirror the selected
	# defaults there so a later control reload cannot remove/reset a PGC_S_FILE
	# value behind the real consumer. Production shared-mode reload never uses
	# that local file as a second authority. All admission observations below
	# still come from actual processes and actual GUC values.
	(my $native = $body) =~ s/^common\.//mg;
	$node->append_conf('postgresql.conf', $native);
	$node->append_conf('postgresql.conf', "test_pgrac_shared_config.reload_generation=$generation\n");
	$node->reload;
	for (1..400) {
		return if slurp_file($node->logfile) =~ /test process configuration consume: generation=$generation ok=1 pid=/;
		select(undef, undef, undef, 0.025);
	}
	die "actual postmaster did not consume generation $generation";
}

my $worker = $node->safe_psql('postgres', 'SELECT test_pgrac_config_future_launch()');
is(await_file('future_ready', qr/^$worker:/), "$worker:1\n",
	'real worker fork inherited old defaults before database initialization');
is(control(1)->[3], 0, 'pre-initialization worker has no native use owner');
publish(2, "common.cluster.read_scache='on'\n");
bind_parent(2);
is(control(0)->[4], 1, 'exact bound local cut opens');
append_to_file($node->data_dir . '/test_config.future_go', "go\n");
my $result = await_file('future_result', qr/^\d+:/);
is($result, "2:on\n", 'old fork consumes actual new common values before native database use');
# The bounded real RED before wiring the consumer must remain a failure,
# rather than hanging later fixtures or passing because the worker exited.
if ($result ne "2:on\n") {
	$node->stop('fast');
	done_testing();
	exit;
}

my $old = $node->background_psql('postgres');
$old->query_safe('BEGIN; SET LOCAL cluster.ges_handoff=on');
is($old->query_safe('SELECT 11'), '11', 'legal native session overlay is not a common-value mismatch');
is(control(1)->[3], 1, 'already-bound transaction remains counted across CLOSE');
is($old->query_safe('SHOW cluster.read_scache'), 'on', 'retained old owner can still finish through CLOSE');
$old->query_safe('ROLLBACK');
is(control(2)->[3], 0, 'actual rollback retires the bound old owner');
bind_parent(2);
is(control(0)->[4], 1, 'unchanged actual target can bind the next empty cut');
# Deliberately publish without a common-use cut, while the client is idle.
# This is a negative controller fixture, not an allowed application protocol.
# Independent native work must reject the mismatching profile even though
# this raw local gate is open. The native mid-work deferral is covered in023.
publish(3, "common.cluster.read_scache='off'\n");
$old->query_until(qr/future_wait_start/, "\\echo future_wait_start\nSELECT 79;\n");
my $waiting = 0;
for (1..100) {
	$waiting = control(2)->[5];
	last if $waiting >= 1;
	select(undef, undef, undef, 0.025);
}
ok($waiting >= 1, 'independent work with a changed common profile waits even when gate is open');
is(control(1)->[3], 0, 'profile mismatch retires its provisional owner before waiting');
bind_parent(3);
is(control(0)->[4], 1, 'next exact local cut publishes matching profile');
like($old->query_until(qr/future_wait_done/, "\\echo future_wait_done\n"), qr/79/,
	'waiting real transaction completes only after matching binding');
is($old->query_safe('SHOW cluster.read_scache'), 'off', 'actual resumed common value matches');

publish(4, "common.cluster.read_scache='off'\ncommon.work_mem='11MB'\n");
is($old->query_safe('SHOW work_mem'), '11MB', 'later default-only generation needs no new common cut');
is($node->safe_psql('postgres', 'SHOW work_mem'), '11MB', 'new child also uses later native defaults');
publish(5, "common.cluster.read_scache='off'\ncommon.max_connections='16'\ncommon.work_mem='11MB'\n");
is($old->query_safe('SHOW max_connections'), '12', 'pending static target does not change actual common use');
is($node->safe_psql('postgres', 'SELECT 83'), '83', 'actual unchanged static profile permits a future child');
$old->quit;
$node->stop('fast');
done_testing();
