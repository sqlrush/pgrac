#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# Offline SQL-boundary tests. No database, client, or VM is started here.
use strict;
use warnings;
use FindBin;
use lib $FindBin::RealBin;
use JSON::PP;
use Test::More;

my $loaded = eval { require WorkloadSql; 1 };
ok($loaded, 'read/image and HOT SQL driver is available');
if (!$loaded)
{
	diag($@);
	done_testing();
	exit 1;
}

my $json = JSON::PP->new->canonical;
sub seed_rows { return [map { +{id => $_, v => "seed_$_"} } 1 .. 64]; }

{
	package SqlBoundary;
	sub query_safe
	{
		my ($self, $sql) = @_;
		push @{$self->{calls}}, $sql;
		my $n = $self->{node};
		my $rows = main::seed_rows();
		my $identity = {
			node_id => $n, pid => 100 + $n, database_oid => 16384,
			backend_start => '2026-10-21 10:00:00+08',
			postmaster_start => '2026-10-21 09:00:00+08',
			system_identifier => '7600000000000000001'};
		if ($sql =~ /pg_stat_activity/)
		{
			$identity->{pid}++ if $self->{identity_reads}++ && $self->{reconnect};
			return $json->encode($identity);
		}
		if ($sql =~ /pg_export_snapshot/)
		{
			my $text = "pid:" . (100 + $n) . "\ndbid:16384\niso:2\nro:1\n"
			  . "clu_src:1\nclu_scn:4097\nclu_epoch:9\n";
			$text =~ s/clu_scn:4097/clu_scn:0/ if $self->{bad_scn};
			$text .= "clu_scn:8193\n" if $self->{duplicate_scn};
			$text =~ s/clu_src:1/clu_src:0/ if $self->{local_snapshot};
			$text =~ s/pid:\d+/pid:999/ if $self->{wrong_pid};
			return $json->encode({export_id => "0000000$n-00000001-1", contents => $text});
		}
		if ($sql =~ /pg_relation_size/)
		{
			return $json->encode({spcOid => 1663, dbOid => 16384,
				relNumber => $sql =~ /r_img/ || $self->{locator_collision} ? 501 : 502,
				forkNum => 0, blocks => $self->{blocks} // 7,
				path => $sql =~ /r_img/ ? 'base/16384/501' : 'base/16384/502'});
		}
		if ($sql =~ /pg_cluster_state/)
		{
			return $json->encode({cr_tuple_verdict_count => 19});
		}
		if ($sql =~ /cluster_cr_test_image/)
		{
			push @{$rows}, {id => 64, v => 'seed_64'} if $self->{duplicate_full};
			return $json->encode([map { +{%$_, block => 0, cr_off => $_->{id}} } @{$rows}]);
		}
		if ($sql =~ /WITH changed AS/)
		{
			die "injected SQL failure\n" if $sql =~ /UPDATE/ && $self->{fail_update};
			$rows = [grep { $_->{id} % 4 == 0 } @{$rows}] if $sql =~ /id % 4 = 0/;
			$rows = [grep { $_->{id} % 8 == 1 } @{$rows}] if $sql =~ /id % 8 = 1/;
			return $json->encode([map { +{%$_, ctid => '(0,' . $_->{id} . ')'} } @{$rows}]);
		}
		if ($sql =~ /jsonb_agg/)
		{
			$rows = [] if $self->{empty_read};
			return $json->encode($rows);
		}
		if ($sql =~ /\A(?:CREATE|BEGIN|ROLLBACK|VACUUM)/)
		{
			die "injected rollback failure\n" if $sql =~ /ROLLBACK/ && $self->{fail_rollback};
			return '';
		}
		die "unexpected SQL: $sql\n";
	}
}

sub fixture
{
	my ($hook) = @_;
	my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
	my @journal;
	my $between = 0;
	my $driver = WorkloadSql->new(
		sessions => \@sessions, schema => 'read_hot_1',
		emit => sub { push @journal, shift },
		between_lanes => sub { $between++; $hook->() if $hook; });
	return ($driver, \@sessions, \@journal, \$between);
}

subtest 'real SQL operations retain exact snapshot and all FULL blocks' => sub {
	my ($driver, $sessions, $journal, $between) = fixture();
	my $result = $driver->run();
	is($result->{lanes}{'R/IMG'}{sql_comparison}, 'MATCH', 'R rows agree');
	is($result->{lanes}{'H/HOT'}{sql_comparison}, 'MATCH', 'H rows agree');
	is($$between, 1, 'caller barrier hook runs once between R and H');
	for my $node (1, 3)
	{
		my @full = grep { /cluster_cr_test_image/ } @{$sessions->[$node]{calls}};
		is(scalar @full, 1, 'one FULL comparison query, no retry');
		like($full[0], qr/generate_series\(0, 6\)/, 'all seven relation blocks included');
		like($full[0], qr/\b4097\b/, 'FULL uses exported snapshot SCN');
		ok(grep(/\AROLLBACK/, @{$sessions->[$node]{calls}}), 'held snapshot released');
	}
	is($result->{field_root}, 'OPEN', 'SQL agreement does not close field root');
	ok(!$result->{ready_to_run}, 'partial driver does not certify a joint run');
	is($result->{trigger_status}, 'UNBOUND', 'no HOT/remote/protocol credit from SQL alone');
	my @queries = grep { $_->{kind} eq 'sql_result' } @{$journal};
	is(scalar @queries, scalar(map { @{$_->{calls}} } @{$sessions}), 'every query result retained');
	is(scalar(grep { !exists($_->{sql}) || !exists($_->{stdout}) || $_->{client_seq} <= 0 } @queries),
		0, 'every query and raw result retains a client-local sequence');
	my $again = eval { $driver->run(); 1 };
	ok(!$again, 'same driver cannot start a second attempt');
};

for my $case (qw(bad_scn duplicate_scn local_snapshot wrong_pid))
{
	subtest "invalid exported snapshot: $case" => sub {
		my ($driver, $sessions) = fixture();
		$sessions->[1]{$case} = 1;
		my $ok = eval { $driver->run(); 1 };
		ok(!$ok, 'bad snapshot refuses comparison');
		unlike(join('\n', map { @{$_->{calls}} } @{$sessions}), qr/UPDATE /,
			'no post-snapshot mutation after refusal');
		ok(grep(/\AROLLBACK/, @{$sessions->[1]{calls}}), 'reader transaction released');
	};
}

subtest 'duplicate FULL rows cannot disappear in set comparison' => sub {
	my ($driver, $sessions, $journal) = fixture();
	$sessions->[1]{duplicate_full} = 1;
	my $ok = eval { $driver->run(); 1 };
	ok(!$ok, 'extra copy is a mismatch');
	like($@, qr/row comparison/, 'reports the actual comparison failure');
	ok(grep($_->{kind} eq 'driver_error', @{$journal}), 'failure retained in journal');
};

subtest 'empty reader is not vacuous agreement' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[1]{empty_read} = 1;
	ok(!eval { $driver->run(); 1 }, 'empty baseline rejected');
};

subtest 'lane locator collision prevents post-snapshot mutations' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[2]{locator_collision} = 1;
	ok(!eval { $driver->run(); 1 }, 'shared physical relation refused');
	like($@, qr/lane relation collision/, 'collision is explicit');
	unlike(join('\n', map { @{$_->{calls}} } @{$sessions}), qr/UPDATE /,
		'no workload UPDATE after collision');
};

subtest 'caller barrier failure stops H and releases both readers' => sub {
	my ($driver, $sessions) = fixture(sub { die "external barrier failed\n" });
	ok(!eval { $driver->run(); 1 }, 'failed external barrier rejects continuation');
	like($@, qr/external barrier failed/, 'caller failure preserved');
	is(scalar(grep(/UPDATE/, @{$sessions->[2]{calls}})), 0, 'H has not started');
	for my $node (1, 3)
	{
		is(scalar(grep(/\AROLLBACK/, @{$sessions->[$node]{calls}})), 1,
			'each held reader receives one cleanup attempt');
	}
};

subtest 'SQL error and cleanup error both survive without retry' => sub {
	my ($driver, $sessions, $journal) = fixture();
	$sessions->[2]{fail_update} = 1;
	$sessions->[3]{fail_rollback} = 1;
	ok(!eval { $driver->run(); 1 }, 'SQL failure rejects run');
	like($@, qr/injected SQL failure/, 'original error retained');
	like($@, qr/injected rollback failure/, 'cleanup error also retained');
	is(scalar(grep(/UPDATE/, @{$sessions->[2]{calls}})), 1, 'failed UPDATE attempted once');
	for my $node (1, 3)
	{
		is(scalar(grep(/\AROLLBACK/, @{$sessions->[$node]{calls}})), 1,
			'cleanup tried once for each held reader');
	}
	ok(grep($_->{kind} eq 'sql_error', @{$journal}), 'SQL failure output retained');
};

subtest 'session replacement invalidates final SQL identity' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[3]{reconnect} = 1;
	ok(!eval { $driver->run(); 1 }, 'changed backend PID rejected');
	like($@, qr/identity/, 'identity mismatch is explicit');
};

subtest 'unsafe schema is rejected before SQL' => sub {
	my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
	ok(!eval { WorkloadSql->new(sessions => \@sessions, schema => 'x;DROP SCHEMA y',
		emit => sub {}, between_lanes => sub {}); 1 }, 'schema cannot inject SQL');
	is(scalar(map { @{$_->{calls}} } @sessions), 0, 'no SQL sent');
};

done_testing();
