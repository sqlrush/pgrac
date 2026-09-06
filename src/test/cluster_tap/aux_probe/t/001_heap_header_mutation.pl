# Copyright (c) 2026, pgrac contributors
# Native, single-worker functional proof. Not a cluster throughput test.
use strict;
use warnings FATAL => 'all';
use JSON::PP qw(decode_json);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $symbol = $ENV{PGRAC_HEAP_PROBE_SYMBOL} // 'test_pgrac_micro_heap_lock_step';
die 'unknown heap probe candidate'
	unless $symbol eq 'test_pgrac_micro_heap_lock_step'
	|| $symbol eq 'test_pgrac_micro_heap_step';
my $node = PostgreSQL::Test::Cluster->new('heap_header');
$node->init;
$node->append_conf('postgresql.conf', "autovacuum=off\nfsync=on\n");
$node->start;
$node->safe_psql('postgres', qq{
	CREATE EXTENSION pageinspect;
	CREATE TABLE micro_heap (id integer, value bigint);
	INSERT INTO micro_heap SELECT g,0 FROM generate_series(1,16) g;
	CREATE FUNCTION header_step(regclass,integer) RETURNS void
	AS '\$libdir/test_pgrac_aux_consumer','$symbol' LANGUAGE C STRICT;
});

sub snapshot
{
	return decode_json($node->safe_psql('postgres', q{
		SELECT json_build_object(
			'bytes',pg_relation_size('micro_heap'),
			'lower',p.lower,'upper',p.upper,'special',p.special,
			'slots',(SELECT count(*) FROM heap_page_items(get_raw_page('micro_heap',0)) WHERE lp_flags=1),
			'payload',(SELECT array_agg(value ORDER BY id) FROM micro_heap),
			'tids',(SELECT array_agg(ctid::text ORDER BY id) FROM micro_heap),
			'xmax',(SELECT t_xmax::text FROM heap_page_items(get_raw_page('micro_heap',0)) WHERE lp=1))
		FROM page_header(get_raw_page('micro_heap',0)) p
	}));
}

my $before = snapshot();
is($before->{slots}, 16, 'exact sixteen-row native fixture');
is($before->{bytes}, 8192, 'one physical block before modification');
my $worker = $node->background_psql('postgres');
$worker->query_safe(q{SELECT header_step('micro_heap',1)});
my $first = snapshot();
my $first_ok = is_deeply($first->{payload}, [(0) x 16], 'real header mutation preserves row payload');
$first_ok &= is_deeply($first->{tids}, $before->{tids}, 'real header mutation preserves every TID');
$first_ok &= is_deeply([@$first{qw(bytes lower upper special slots)}],
	[@$before{qw(bytes lower upper special slots)}], 'no new tuple version or page-space consumption');
$first_ok &= isnt($first->{xmax}, $before->{xmax}, 'actual first tuple header changes');

# A wrong operation fails here; do not run a long tail after a known RED.
if (!$first_ok)
{
	$worker->quit;
	$node->stop;
	done_testing();
	exit 1;
}

for my $ordinal (2 .. 256)
{
	my $id = (($ordinal - 1) % 16) + 1;
	$worker->query_safe("SELECT header_step('micro_heap',$id)");
}
$worker->quit;
my $after = snapshot();
is_deeply($after->{payload}, [(0) x 16], '256 committed calls preserve all payloads');
is_deeply($after->{tids}, $before->{tids}, '256 committed calls preserve exact TIDs');
is_deeply([@$after{qw(bytes lower upper special slots)}],
	[@$before{qw(bytes lower upper special slots)}], '256 calls cross the old capacity bound without growth');
isnt($after->{xmax}, $first->{xmax}, 'later committed transactions really replace the lock header');

for my $id (0,17)
{
	my ($rc, $out, $err) = $node->psql('postgres', "SELECT header_step('micro_heap',$id)");
	isnt($rc, 0, "out-of-range worker $id is rejected");
	like($err, qr/fixed worker id/, 'worker rejection is an explicit runtime check');
}

$node->safe_psql('postgres', q{
	CREATE TABLE other_heap (id integer,value bigint);
	INSERT INTO other_heap SELECT g,0 FROM generate_series(1,16) g;
	CREATE SCHEMA wrong_shape;
	CREATE TABLE wrong_shape.micro_heap (id integer,value text);
	INSERT INTO wrong_shape.micro_heap SELECT g,'zero' FROM generate_series(1,16) g;
	CREATE SCHEMA wrong_slot;
	CREATE TABLE wrong_slot.micro_heap (id integer,value bigint);
	INSERT INTO wrong_slot.micro_heap SELECT g+1,0 FROM generate_series(1,16) g;
});
for my $relation ('other_heap', 'wrong_shape.micro_heap', 'wrong_slot.micro_heap')
{
	my ($rc, $out, $err) = $node->psql('postgres', "SELECT header_step('$relation',1)");
	isnt($rc, 0, "$relation is not a valid fixed heap fixture");
	like($err, qr/heap lock probe/, 'fixture rejection comes from the probe guard');
}
my ($rc, $out, $err) = $node->psql('postgres', q{
	BEGIN;
	SELECT header_step('micro_heap',1);
	SELECT header_step('micro_heap',1);
	ROLLBACK;
});
isnt($rc, 0, 'same-transaction second lock is not a new physical mutation');
like($err, qr/fresh transaction/, 'no-op rejected instead of reported as completed');
is($node->safe_psql('postgres', 'SELECT sum(value) FROM micro_heap'), '0',
	'normal error cleanup preserves payload');
$node->stop;
done_testing();
