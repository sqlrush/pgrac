#-------------------------------------------------------------------------
#
# 036_undo_slot_observation.pl
#    Native unavailable/privilege contract for the read-only slot probes.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/modules/test_pgrac_shared_config/t/036_undo_slot_observation.pl
#
# NOTES
#    This standalone instance does not provide shared admission or a full
#    transaction segment. The four-member test exercises those paths.
#-------------------------------------------------------------------------
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use JSON::PP;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('undo_slot_observation');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = off\n");
$node->start;
is($node->safe_psql('postgres', 'SHOW cluster.enabled'), 'off',
	'the local probe contract is tested without cluster admission');

for my $name (qw(snapshot binding))
{
	my $function = "test_pgrac_undo_slot_$name";
	my ($rc, $out, $err) = $node->psql('postgres',
		"CREATE FUNCTION $function() RETURNS text AS "
		. "'\$libdir/test_pgrac_shared_config', '$function' "
		. 'LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE');
	is($rc, 0, "$name exports a native read-only probe") or diag($err);

	my $sql = "SELECT coalesce(pg_current_xact_id_if_assigned()::text,'none'), "
		. "$function(), coalesce(pg_current_xact_id_if_assigned()::text,'none')";
	($rc, $out, $err) = $node->psql('postgres', $sql);
	is($rc, 0, "$name observation executes in the native backend") or diag($err);
	my ($before, $json, $after) = split /\|/, $out;
	is($before, 'none', "$name starts with an unassigned native transaction");
	is($after, 'none', "$name does not assign a transaction or binding");
	my $sample = eval { decode_json($json // '') };
	is_deeply($sample,
		$name eq 'snapshot' ? { available => JSON::PP::false }
		: { available => JSON::PP::false, wait_scope => JSON::PP::false },
		"$name reports unavailable explicitly rather than an empty positive sample");

	($rc, $out, $err) = $node->psql('postgres',
		"BEGIN; SELECT pg_current_xact_id()::text; SELECT $function(); "
		. 'SELECT pg_current_xact_id_if_assigned()::text; ROLLBACK');
	is($rc, 0, "$name also permits an already assigned native transaction") or diag($err);
	my @rows = split /\n/, $out;
	like($rows[0] // '', qr/^[1-9][0-9]*$/, "$name sees a real assigned XID");
	is($rows[-1], $rows[0], "$name preserves that assigned XID");
}

$node->safe_psql('postgres', 'CREATE ROLE slot_probe_reader');
for my $name (qw(snapshot binding))
{
	my ($rc, $out, $err) = $node->psql('postgres',
		"\\set VERBOSITY verbose\nSET ROLE slot_probe_reader; "
		. "SELECT test_pgrac_undo_slot_$name()");
	cmp_ok($rc, '!=', 0, "$name refuses a non-superuser");
	like($err, qr/42501:/, "$name uses the native insufficient-privilege error");
}
$node->stop;
done_testing();
