# Author: SqlRush <sqlrush@gmail.com>
# SQL operations for caller-owned, persistent four-node test sessions.
package WorkloadSql;
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use JSON::PP;
use Scalar::Util qw(refaddr);

my $json = JSON::PP->new->canonical;

sub new
{
	my ($class, %args) = @_;
	die "four persistent SQL sessions required\n"
	  unless ref($args{sessions}) eq 'ARRAY' && @{$args{sessions}} == 4;
	my %seen;
	for my $session (@{$args{sessions}})
	{
		die "distinct query_safe sessions required\n"
		  unless ref($session) && $session->can('query_safe') && !$seen{refaddr($session)}++;
	}
	die "invalid workload schema\n"
	  unless defined($args{schema}) && $args{schema} =~ /\A[a-z][a-z0-9_]{0,47}\z/;
	die "journal emitter and between-lanes hook required\n"
	  unless ref($args{emit}) eq 'CODE' && ref($args{between_lanes}) eq 'CODE';
	return bless {%args, seq => [0, 0, 0, 0], open => {}, lanes => {}, identities => []}, $class;
}

sub _query
{
	my ($self, $node, $operation, $sql) = @_;
	my $entry = {
		kind => 'sql_result', node => $node, operation => $operation,
		client_seq => ++$self->{seq}[$node], sql => $sql, sql_sha256 => sha256_hex($sql)};
	$entry->{sql_identity} = $self->{identities}[$node] if $self->{identities}[$node];
	my $output;
	my $ok = eval { $output = $self->{sessions}[$node]->query_safe($sql); 1 };
	if (!$ok)
	{
		my $error = $@ || "SQL session failed\n";
		$self->{emit}->({%{$entry}, kind => 'sql_error', error => $error});
		die $error;
	}
	die "undefined SQL output\n" unless defined($output);
	$self->{emit}->({%{$entry}, stdout => $output, stdout_sha256 => sha256_hex($output)});
	return $output;
}

sub _json_query
{
	my ($self, @args) = @_;
	return $json->decode($self->_query(@args));
}

sub _identity
{
	my ($self, $node) = @_;
	my $identity = $self->_json_query($node, 'identity', q{
SELECT jsonb_build_object(
 'node_id', current_setting('cluster.node_id')::integer,
 'pid', pg_backend_pid(), 'backend_start', a.backend_start::text,
 'postmaster_start', pg_postmaster_start_time()::text,
 'database_oid', (SELECT oid FROM pg_database WHERE datname = current_database()),
 'system_identifier', (pg_control_system()).system_identifier::text)
FROM pg_stat_activity a WHERE a.pid = pg_backend_pid()});
	die "missing SQL identity\n" unless ref($identity) eq 'HASH';
	for my $field (qw(node_id pid backend_start postmaster_start database_oid system_identifier))
	{
		die "missing SQL identity $field\n"
		  unless defined($identity->{$field}) && length($identity->{$field});
	}
	die "wrong node identity\n" unless $identity->{node_id} eq "$node";
	return $identity;
}

sub _table
{
	my ($self, $name) = @_;
	return qq{"$self->{schema}"."$name"};
}

sub _locator
{
	my ($self, $node, $table) = @_;
	my $loc = $self->_json_query($node, 'relation_locator', qq{
SELECT jsonb_build_object(
 'spcOid', COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace),
 'dbOid', d.oid, 'relNumber', pg_relation_filenode(c.oid), 'forkNum', 0,
 'blocks', pg_relation_size(c.oid, 'main') / current_setting('block_size')::integer,
 'path', pg_relation_filepath(c.oid))
FROM pg_class c CROSS JOIN pg_database d
WHERE c.oid = '$table'::regclass AND d.datname = current_database()});
	die "missing relation locator\n" unless ref($loc) eq 'HASH';
	for my $field (qw(spcOid dbOid relNumber blocks))
	{
		die "invalid relation $field\n"
		  unless defined($loc->{$field}) && $loc->{$field} =~ /\A[0-9]+\z/ && $loc->{$field} > 0;
	}
	die "wrong relation fork or database\n"
	  unless defined($loc->{forkNum}) && $loc->{forkNum} == 0
	  && $loc->{dbOid} == $self->{identities}[$node]{database_oid};
	return $loc;
}

sub _rows
{
	my ($rows) = @_;
	die "invalid row result\n" unless ref($rows) eq 'ARRAY';
	my @visible;
	for my $row (@{$rows})
	{
		die "invalid visible row\n" unless ref($row) eq 'HASH'
		  && defined($row->{id}) && $row->{id} =~ /\A[0-9]+\z/
		  && defined($row->{v}) && !ref($row->{v});
		push @visible, {id => 0 + $row->{id}, v => $row->{v}};
	}
	return [sort { $a->{id} <=> $b->{id} || $a->{v} cmp $b->{v} } @visible];
}

sub _change
{
	my ($self, $node, $operation, $sql, $expected) = @_;
	my $rows = $self->_json_query($node, $operation, qq{
WITH changed AS ($sql RETURNING id, v, ctid)
SELECT COALESCE(jsonb_agg(jsonb_build_object('id', id, 'v', v, 'ctid', ctid::text)
 ORDER BY id), '[]'::jsonb) FROM changed});
	die "wrong affected row count for $operation\n" unless @{_rows($rows)} == $expected;
	return $rows;
}

sub _read
{
	my ($self, $node, $table) = @_;
	return _rows($self->_json_query($node, 'visible_rows', qq{
SELECT COALESCE(jsonb_agg(jsonb_build_object('id', id, 'v', v) ORDER BY id),
 '[]'::jsonb) FROM $table}));
}

sub _hold_snapshot
{
	my ($self, $node) = @_;
	# Mark first: a client error after BEGIN was sent still needs cleanup.
	$self->{open}{$node} = 1;
	$self->_query($node, 'begin_reader', 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
	my $export = $self->_json_query($node, 'export_snapshot', q{
WITH exported AS MATERIALIZED (SELECT pg_export_snapshot() AS id)
SELECT jsonb_build_object('export_id', id,
 'contents', pg_read_file('pg_snapshots/' || id)) FROM exported});
	die "missing exported snapshot\n" unless ref($export) eq 'HASH'
	  && defined($export->{export_id}) && $export->{export_id} =~ /\A[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+\z/
	  && defined($export->{contents});
	my %fields;
	for my $line (split /\n/, $export->{contents})
	{
		die "invalid snapshot line\n" unless $line =~ /\A([^:]+):(.*)\z/;
		push @{$fields{$1}}, $2;
	}
	for my $key (qw(pid dbid iso ro clu_src clu_scn clu_epoch))
	{
		die "missing or repeated snapshot $key\n" unless $fields{$key} && @{$fields{$key}} == 1;
	}
	my %value = map { $_ => $fields{$_}[0] } keys %fields;
	die "snapshot identity mismatch\n"
	  unless $value{pid} eq "$self->{identities}[$node]{pid}"
	  && $value{dbid} eq "$self->{identities}[$node]{database_oid}"
	  && $value{iso} eq '2' && $value{ro} eq '1' && $value{clu_src} eq '1';
	for my $key (qw(clu_scn clu_epoch))
	{
		die "invalid snapshot $key\n" unless $value{$key} =~ /\A[1-9][0-9]*\z/
		  && (length($value{$key}) < 19 || (length($value{$key}) == 19
		  && $value{$key} le '9223372036854775807'));
	}
	return {%{$export}, read_scn => $value{clu_scn}, read_epoch => $value{clu_epoch}};
}

sub _compare
{
	my ($self, $lane) = @_;
	my ($reader, $writer, $table) = @{$lane}{qw(reader writer table)};
	my $after = $self->_locator($writer, $table);
	for my $field (qw(spcOid dbOid relNumber forkNum))
	{
		die "relation identity changed\n" unless $after->{$field} == $lane->{locator}{$field};
	}
	my $blocks = $after->{blocks} > $lane->{locator}{blocks}
	  ? $after->{blocks} : $lane->{locator}{blocks};
	my $counter_sql = q{SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb)
FROM pg_cluster_state WHERE category = 'cr' AND key = 'cr_tuple_verdict_count'};
	$lane->{counter_before} = $self->_json_query($reader, 'counter_before', $counter_sql);
	$lane->{visible} = $self->_read($reader, $table);
	$lane->{counter_after} = $self->_json_query($reader, 'counter_after', $counter_sql);
	my $last = $blocks - 1;
	my $scn = $lane->{snapshot}{read_scn};
	$lane->{full} = $self->_json_query($reader, 'full_rows', qq{
SELECT COALESCE(jsonb_agg(jsonb_build_object('id', id, 'v', v,
 'block', b, 'cr_off', cr_off) ORDER BY id, b, cr_off), '[]'::jsonb)
FROM generate_series(0, $last) AS blocks(b)
CROSS JOIN LATERAL cluster_cr_test_image('$table'::regclass, b, $scn)
 AS img(cr_off int2, id int, v text)});
	$lane->{locator_after} = $after;
	$lane->{full_blocks} = $blocks;
	die "row comparison mismatch\n"
	  unless $json->encode($lane->{baseline}) eq $json->encode($lane->{visible})
	  && $json->encode($lane->{visible}) eq $json->encode(_rows($lane->{full}));
	$lane->{sql_comparison} = 'MATCH';
}

sub run
{
	my ($self) = @_;
	die "driver attempt already consumed\n" if $self->{attempted}++;
	my $ok = eval {
		for my $node (0 .. 3)
		{
			$self->{identities}[$node] = $self->_identity($node);
			for my $key (qw(database_oid system_identifier))
			{
				die "four-node database identity mismatch\n"
				  unless $self->{identities}[$node]{$key} eq $self->{identities}[0]{$key};
			}
		}
		$self->_query(0, 'create_schema', qq{CREATE SCHEMA "$self->{schema}"});
		for my $spec (['R/IMG', 'r_img', 0, 1], ['H/HOT', 'h_hot', 2, 3])
		{
			my ($name, $short, $writer, $reader) = @{$spec};
			my $table = $self->_table($short);
			$self->_query($writer, 'create_relation',
				"CREATE TABLE $table (id integer PRIMARY KEY, v text) WITH (fillfactor = 50)");
			my $seed = $self->_change($writer, 'seed',
				"INSERT INTO $table SELECT g, 'seed_' || g FROM generate_series(1, 64) g", 64);
			my $expected = [map { +{id => $_, v => "seed_$_"} } 1 .. 64];
			die "seed row comparison mismatch\n" unless $json->encode(_rows($seed)) eq $json->encode($expected);
			my $lane = $self->{lanes}{$name} = {
				table => $table, writer => $writer, reader => $reader,
				locator => $self->_locator($writer, $table)};
			$lane->{snapshot} = $self->_hold_snapshot($reader);
			$lane->{baseline} = $self->_read($reader, $table);
			die "baseline row comparison mismatch\n"
			  unless $json->encode($lane->{baseline}) eq $json->encode($expected);
		}
		my ($r, $h) = @{$self->{lanes}}{'R/IMG', 'H/HOT'};
		die "lane relation collision\n" if $r->{locator}{relNumber} == $h->{locator}{relNumber}
		  && $r->{locator}{spcOid} == $h->{locator}{spcOid};
		$self->_change(0, 'read_image_update', "UPDATE $r->{table} SET v = 'remote_' || id", 64);
		$self->_compare($r);
		$self->{emit}->({kind => 'sql_lane_complete', lane => 'R/IMG', evidence => $r});
		# The caller owns the I/TT operations and their actual evidence seals.
		$self->{between_lanes}->();
		for my $pass (1 .. 3)
		{
			$self->_change(2, 'hot_candidate_update', "UPDATE $h->{table} SET v = 'hot_${pass}_' || id", 64);
		}
		$self->_change(2, 'indexed_key_update', "UPDATE $h->{table} SET id = id + 64 WHERE id % 4 = 0", 16);
		$self->_change(2, 'delete', "DELETE FROM $h->{table} WHERE id % 8 = 1", 8);
		$self->_query(2, 'vacuum', "VACUUM $h->{table}");
		$self->_compare($h);
		$self->{emit}->({kind => 'sql_lane_complete', lane => 'H/HOT', evidence => $h});
		for my $node (0 .. 3)
		{
			die "SQL session identity changed\n" unless $json->encode($self->_identity($node))
			  eq $json->encode($self->{identities}[$node]);
		}
		1;
	};
	my $error = $ok ? '' : ($@ || "SQL driver failed\n");
	for my $node (sort keys %{$self->{open}})
	{
		my $clean = eval { $self->_query($node, 'release_reader', 'ROLLBACK'); 1 };
		$error .= $@ || "reader cleanup failed\n" unless $clean;
	}
	if (length($error))
	{
		$self->{emit}->({kind => 'driver_error', error => $error, field_root => 'OPEN'});
		die $error;
	}
	my $result = {lanes => $self->{lanes}, field_root => 'OPEN', ready_to_run => JSON::PP::false,
		trigger_status => 'UNBOUND', itt_owner => 'caller', receipt_stop_seal => 'NOT_CHECKED'};
	$self->{emit}->({kind => 'sql_driver_complete', result => $result});
	return $result;
}

1;
