#!/usr/bin/env perl
# Manually selected native 2.12 prerequisite. CC schedules it only after the
# same-binary Rule29 gate; this fixture neither starts a VM nor grants a budget.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);
use Cwd qw(abs_path);

my $cluster;
my $stop_attempted = 0;
my @clients;
my $evidence = {version => 1, writer_node => 2, normal_stop => JSON::PP::false};
my $directory = $ENV{TESTLOGDIR} // '.';
my $judge = "$FindBin::RealBin/k_vm_native_chain.py";
my $root = abs_path("$FindBin::RealBin/../../../..");

sub save_evidence
{
	open my $file, '>', "$directory/k-vm-native-chain.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($evidence);
	close $file or die $!;
}

sub close_clients
{
	my @errors;
	for my $client (reverse @clients)
	{
		eval { $client->quit; 1 } or push @errors, $@;
	}
	@clients = ();
	die join('', @errors) if @errors;
}

END
{
	eval { close_clients(); 1 } or diag("client exit failed: $@");
	if (defined($cluster) && !$stop_attempted)
	{
		$stop_attempted = 1;
		eval { $cluster->stop_cluster; 1 } or diag("original normal stop failed: $@");
	}
}

sub require_true
{
	die "native evidence refused: $_[1]\n" unless $_[0];
}

sub invoke_judge
{
	my ($input, @args) = @_;
	my ($out, $err) = ('', '');
	my $ok = run([$ENV{PYTHON} // 'python3', '-B', $judge, @args],
		'<', \$input, '>', \$out, '2>', \$err, timeout(10));
	require_true($out ne '', "judge returned no result: $err");
	return ($ok, decode_json($out));
}

sub vm_flags
{
	my ($node) = @_;
	return decode_json($node->safe_psql('postgres', q{SELECT row_to_json(v)
		FROM pg_visibility_map('k_vm_native'::regclass, 0) AS v}));
}

sub metrics
{
	my ($node) = @_;
	return decode_json($node->safe_psql('postgres', q{SELECT json_object_agg(key, value)
		FROM pg_cluster_state WHERE category='pcm' AND key LIKE 'vm_%'}));
}

sub session
{
	my ($node) = @_;
	my $client = $node->background_psql('postgres', timeout => 120);
	push @clients, $client;
	return $client;
}

my ($ready, $source) = invoke_judge('', '--source-root', $root);
$evidence->{source_precheck} = $source;
save_evidence();
BAIL_OUT('original source chain is incomplete: ' . encode_json($source)) unless $ready;
pass('original source entries exist; this is not a native completion verdict');

my $ok = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('k_vm_native',
		nodes => 4, blackbox => 1, required_operations => [],
		extra_conf => ['autovacuum=off', 'cluster.trace_block_io=on',
			'cluster.trace_tx_identity=on', 'cluster.read_scache=off']);
	$cluster->start_cluster;
	my @nodes = $cluster->nodes;
	my ($publisher, $reader, $writer_node) = @nodes[0, 1, 2];
	$publisher->safe_psql('postgres', q{CREATE EXTENSION pg_visibility;
		CREATE EXTENSION pg_walinspect;
		CREATE TABLE k_vm_native(id int PRIMARY KEY, v int);
		INSERT INTO k_vm_native VALUES(1,1);
		DELETE FROM k_vm_native WHERE id=1});
	# Keep the physically initialized empty target instead of truncating it.
	$publisher->safe_psql('postgres', 'VACUUM(TRUNCATE OFF) k_vm_native');
	my $locator = $publisher->safe_psql('postgres', q{SELECT
		COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::text || '/' ||
		d.oid::text || '/' || pg_relation_filenode(c.oid)::text
		FROM pg_class c, pg_database d WHERE c.oid='k_vm_native'::regclass
		AND d.datname=current_database()});
	require_true($locator =~ /^[1-9][0-9]*\/[1-9][0-9]*\/[1-9][0-9]*$/, 'exact relation locator');
	$evidence->{heap_tag} = "$locator/0/0";
	$evidence->{vm_tag} = "$locator/2/0";
	$evidence->{index_locator} = $publisher->safe_psql('postgres', q{SELECT
		COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::text || '/' ||
		d.oid::text || '/' || pg_relation_filenode(c.oid)::text
		FROM pg_class c, pg_database d WHERE c.oid='k_vm_native_pkey'::regclass
		AND d.datname=current_database()});
	require_true($publisher->safe_psql('postgres', q{SELECT pg_relation_size('k_vm_native')}) eq '8192',
		'one initialized empty heap block');
	$evidence->{published} = vm_flags($publisher);
	$evidence->{retained} = vm_flags($reader);
	for my $stage (qw(published retained))
	{
		require_true($evidence->{$stage}{all_visible} && $evidence->{$stage}{all_frozen},
			"empty qualified publication must be present at $stage");
	}
	pass('original empty publisher and remote VM reader both observe AV/AF');
	$evidence->{before} = metrics($writer_node);
	my $writer = session($writer_node);
	$evidence->{writer_pid} = $writer->query_safe('SELECT pg_backend_pid()');
	my $start = $writer->query_safe('SELECT pg_current_wal_insert_lsn()');
	require_true($start =~ /^[0-9A-F]+\/[0-9A-F]+$/, 'native WAL start');
	$writer->query_safe('BEGIN');
	my $tid = $writer->query_safe('INSERT INTO k_vm_native VALUES(42,7) RETURNING ctid');
	require_true($tid =~ /^\(0,[1-9][0-9]*\)$/, 'INSERT uses the original target block');
	$evidence->{xid} = $writer->query_safe(q{SELECT
		(pg_current_xact_id()::text::numeric % 4294967296)::text});
	require_true($evidence->{xid} =~ /^[1-9][0-9]*$/, 'original WAL transaction id');
	$evidence->{uncommitted_count} = $reader->safe_psql('postgres', 'SELECT count(*) FROM k_vm_native');
	$evidence->{cleared} = vm_flags($reader);
	$writer->query_safe('COMMIT');
	my $end = $writer->query_safe('SELECT pg_current_wal_insert_lsn()');
	require_true($end =~ /^[0-9A-F]+\/[0-9A-F]+$/, 'native WAL end');
	$evidence->{wal_start} = $start;
	$evidence->{wal_end} = $end;
	$evidence->{after} = metrics($writer_node);
	my $xid = $evidence->{xid};
	$evidence->{wal} = decode_json($writer->query_safe(qq{SELECT COALESCE(json_agg(
		json_build_object('start',(start_lsn-'0/0'::pg_lsn)::text,
		'end',(end_lsn-'0/0'::pg_lsn)::text,'xid',xid::text,'rm',resource_manager,
		'type',record_type,'description',description,'block_ref',block_ref)
		ORDER BY start_lsn), '[]'::json)
		FROM pg_get_wal_records_info('$start'::pg_lsn,'$end'::pg_lsn)
		WHERE xid::text='$xid'}));
	$evidence->{peers} = [];
	for my $ordinal (0 .. 3)
	{
		my $client = session($nodes[$ordinal]);
		$client->query_safe('SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=off');
		push @{$evidence->{peers}}, {node => $ordinal,
			plan => $client->query_safe('EXPLAIN(COSTS OFF) SELECT id,v FROM k_vm_native WHERE id=42'),
			rows => $client->query_safe('SELECT id,v FROM k_vm_native WHERE id=42')};
	}
	save_evidence();
	close_clients();
	$stop_attempted = 1;
	$cluster->stop_cluster;
	$evidence->{normal_stop} = JSON::PP::true;
	undef $cluster;
	save_evidence();
	my ($accepted, $verdict) = invoke_judge(encode_json($evidence));
	$evidence->{verdict} = $verdict;
	save_evidence();
	require_true($accepted, encode_json($verdict));
	pass('native remote VM X completion and clear precede exact heap/VM WAL, index WAL and COMMIT');
	pass('uncommitted row stays hidden, all four ordinary index reads agree, original normal stop succeeds');
	1;
};
if (!$ok)
{
	$evidence->{error} = "$@";
	save_evidence();
	BAIL_OUT($evidence->{error});
}
done_testing();
