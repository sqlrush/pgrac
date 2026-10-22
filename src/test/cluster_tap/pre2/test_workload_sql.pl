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
		my $pid = $self->{pid} // (100 + $n);
		my $is_q = $sql =~ /"q_receipt"/;
		my $rows = $is_q ? [{id => 1, v => 'q_seed'}] : main::seed_rows();
		my $identity = {
			node_id => $n, pid => $pid, database_oid => 16384,
			backend_start => '2026-10-21 10:00:00+08',
			postmaster_start => $self->{postmaster_start} // '2026-10-21 09:00:00+08',
			system_identifier => '7600000000000000001'};
		if ($sql =~ /pg_stat_activity/)
		{
			$identity->{pid}++ if $self->{identity_reads}++ && $self->{reconnect};
			return $json->encode($identity);
		}
		if ($sql =~ /pg_export_snapshot/)
		{
			my $text = "pid:$pid\ndbid:16384\niso:2\nro:1\n"
			  . "clu_src:1\nclu_scn:4097\nclu_epoch:9\n";
			$text =~ s/clu_scn:4097/clu_scn:0/ if $self->{bad_scn};
			$text .= "clu_scn:8193\n" if $self->{duplicate_scn};
			$text =~ s/clu_src:1/clu_src:0/ if $self->{local_snapshot};
			$text =~ s/pid:\d+/pid:999/ if $self->{wrong_pid};
			return $json->encode({export_id => "0000000$n-00000001-1", contents => $text});
		}
		if ($sql =~ /pg_relation_size/)
		{
			my $rel = $sql =~ /r_img/ || $self->{locator_collision} ? 501 : $is_q ? 503 : 502;
			$rel = 501 if $is_q && $self->{q_locator_collision};
			return $json->encode({spcOid => 1663, dbOid => 16384,
				relNumber => $rel,
				forkNum => 0, blocks => $self->{blocks} // 7,
				path => "base/16384/$rel"});
		}
		if ($sql =~ /pg_current_xact_id/)
		{
			return $json->encode(exists($self->{full_xid}) ? $self->{full_xid} : '18446744073709551614');
		}
		if ($sql =~ /pg_cluster_state/)
		{
			return $json->encode({cr_tuple_verdict_count => 19});
		}
		if ($sql =~ /cluster_cr_test_(?:target_)?image/)
		{
			die "target FULL refused\n" if $self->{fail_full};
			push @{$rows}, {id => 64, v => 'seed_64'} if $self->{duplicate_full};
			return $json->encode([map { +{%$_, block => 0, cr_off => $_->{id}} } @{$rows}]);
		}
		if ($sql =~ /WITH changed AS/)
		{
			return undef if $is_q && $sql =~ /UPDATE/ && $self->{undefined_q_update};
			die "injected Q mutation failure\n" if $is_q && $sql =~ /UPDATE/ && $self->{fail_q_update};
			die "injected SQL failure\n" if $sql =~ /UPDATE/ && $self->{fail_update};
			$rows->[0]{v} = 'q_committed' if $is_q && $sql =~ /UPDATE/;
			$rows = [grep { $_->{id} % 4 == 0 } @{$rows}] if $sql =~ /id % 4 = 0/;
			$rows = [grep { $_->{id} % 8 == 1 } @{$rows}] if $sql =~ /id % 8 = 1/;
			return $json->encode([map { +{%$_, ctid => '(0,' . $_->{id} . ')'} } @{$rows}]);
		}
		if ($sql =~ /jsonb_agg/)
		{
			$rows = [] if $self->{empty_read};
			$self->{reads}++;
			$rows->[0]{v} = 'wrong_full_path_row' if $self->{bad_off_rows}
			  && defined($self->{fastpath_enabled}) && !$self->{fastpath_enabled}
			  && $self->{reads} > 1;
			return $json->encode($rows);
		}
		if ($sql =~ /\ASET LOCAL cluster\.cr_tuple_level_fastpath = (on|off)\z/)
		{
			die "fastpath enable failed\n" if $1 eq 'on' && $self->{fail_fastpath_enable};
			$self->{fastpath_enabled} = $1 eq 'on';
			return '';
		}
		if ($sql =~ /\A(?:CREATE|BEGIN|COMMIT|ROLLBACK|VACUUM)/)
		{
			die "injected Q begin failure\n" if $sql eq 'BEGIN' && $self->{fail_q_begin};
			die "injected H begin failure\n" if $sql eq 'BEGIN' && $self->{fail_hot_begin};
			die "injected commit failure\n" if $sql eq 'COMMIT' && $self->{fail_commit};
			die "injected rollback failure\n" if $sql =~ /ROLLBACK/ && $self->{fail_rollback};
			return '';
		}
		die "unexpected SQL: $sql\n";
	}
}

sub fixture
{
	my ($hook, $receipt_hook, $emit) = @_;
	my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
	my $hot_reader = bless {node => 2, pid => 150, calls => []}, 'SqlBoundary';
	my @journal;
	my @receipt;
	my $between = 0;
	my $driver = WorkloadSql->new(
		sessions => \@sessions, hot_reader => $hot_reader, schema => 'read_hot_1',
		emit => sub { my ($entry) = @_; push @journal, $entry; $emit->($entry) if $emit; },
		between_lanes => sub { $between++; $hook->() if $hook; },
		receipt_barrier => sub {
			my ($phase, $evidence) = @_;
			push @receipt, {phase => $phase, evidence => $json->decode($json->encode($evidence)),
				commit_calls => scalar(grep { $_ eq 'COMMIT' } @{$sessions[0]{calls}})};
			return $receipt_hook->($phase, $evidence) if $receipt_hook;
		});
	return ($driver, [@sessions, $hot_reader], \@journal, \$between, \@receipt);
}

subtest 'real SQL operations retain exact snapshot and all FULL blocks' => sub {
	my ($driver, $sessions, $journal, $between) = fixture();
	my $result = $driver->run();
	is($result->{lanes}{'R/IMG'}{sql_comparison}, 'MATCH', 'R rows agree');
	is($result->{lanes}{'H/HOT'}{sql_comparison}, 'MATCH', 'H rows agree');
	is($$between, 1, 'caller barrier hook runs once between R and H');
	for my $node (1, 4)
	{
		my @full = grep { /cluster_cr_test_(?:target_)?image/ } @{$sessions->[$node]{calls}};
		is(scalar @full, 1, 'one FULL comparison query, no retry');
		like($full[0], qr/cluster_cr_test_target_image\(/, 'FULL uses the target entry');
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

subtest 'target FULL refusal stops comparison without a source fallback' => sub {
	my ($driver, $sessions, $journal) = fixture();
	$sessions->[1]{fail_full} = 1;
	ok(!eval { $driver->run(); 1 }, 'refusal fails the attempt');
	like($@, qr/target FULL refused/, 'original target refusal preserved');
	is(scalar(grep(/cluster_cr_test_target_image\(/, @{$sessions->[1]{calls}})),
		1, 'target query is sent once');
	is(scalar(grep(/cluster_cr_test_image\(/, @{$sessions->[1]{calls}})),
		0, 'own-instance entry is never a fallback');
	ok(grep(/\AROLLBACK/, @{$sessions->[1]{calls}}), 'held reader is released');
	ok(grep($_->{kind} eq 'driver_error', @{$journal}), 'refusal retained in journal');
};

subtest 'H holds an own-instance snapshot across one modifying transaction' => sub {
	my ($driver, $sessions, $journal) = fixture();
	my $result = $driver->run();
	my $h = $result->{lanes}{'H/HOT'};
	is($h->{writer}, 2, 'H writer is on node 2');
	is($h->{reader}, 2, 'H reader is on the writer instance');
	is($h->{reader_session}, 4, 'H has a separate reader session');
	is($h->{reader_sql_identity}{pid}, 150, 'actual reader birth identity retained');
	is($h->{writer_sql_identity}{pid}, 102, 'separate writer identity retained');
	is($h->{commit_status}, 'ACKNOWLEDGED', 'one modifying transaction committed');
	my @calls = @{$sessions->[2]{calls}};
	my ($begin) = grep { $calls[$_] eq 'BEGIN' } 0 .. $#calls;
	my ($commit) = grep { $calls[$_] eq 'COMMIT' } 0 .. $#calls;
	my ($vacuum) = grep { $calls[$_] =~ /\AVACUUM / } 0 .. $#calls;
	my @changes = grep { $calls[$_] =~ /WITH changed AS/ && $calls[$_] =~ /UPDATE|DELETE/ } 0 .. $#calls;
	is(scalar @changes, 5, 'three nonindexed updates, one indexed update and one delete');
	ok(defined($begin) && defined($commit) && defined($vacuum)
		&& !grep($_ <= $begin || $_ >= $commit, @changes), 'all five mutations share BEGIN/COMMIT');
	ok(defined($commit) && defined($vacuum) && $commit < $vacuum, 'VACUUM follows COMMIT');
	is(scalar(grep { $_ eq 'BEGIN' } @calls), 1, 'no second H modifying transaction');
	is(scalar(grep { $_ eq 'COMMIT' } @calls), 1, 'H COMMIT is sent once');
	is(scalar(grep { $_ eq 'ROLLBACK' } @calls), 0, 'acknowledged H is not rolled back');
	my @h_reader = @{$sessions->[4]{calls}};
	is_deeply([map { / = (on|off)\z/; $1 } grep { /\ASET LOCAL / } @h_reader],
		['off', 'on', 'off'], 'baseline off, tuple read on, ordinary full-path control off');
	is_deeply($h->{visible_full_path}, $h->{visible}, 'ON/OFF reads share the held snapshot');
	is_deeply($h->{counter_before}, $h->{counter_after}, 'unchanged counters are retained');
	is($result->{trigger_status}, 'UNBOUND', 'constructed input is not a proven fastpath trigger');
	for my $slot (2, 4)
	{
		my @entries = grep { $_->{kind} eq 'sql_result' && ($_->{client_session} // -1) == $slot } @{$journal};
		is(scalar @entries, scalar @{$sessions->[$slot]{calls}}, 'each same-node session has its own journal domain');
		is_deeply([map { $_->{client_seq} } @entries], [1 .. @entries], 'client sequence is local to that session');
		is(scalar(grep { $_->{node} != 2 } @entries), 0, 'session slot is not reported as a node id');
	}
};

subtest 'H requires a distinct caller-owned reader before sending SQL' => sub {
	for my $mode (qw(absent aliased))
	{
		my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
		my %extra = $mode eq 'aliased' ? (hot_reader => $sessions[2]) : ();
		my $ok = eval { WorkloadSql->new(sessions => \@sessions, %extra, schema => 'valid_schema',
			emit => sub {}, between_lanes => sub {}, receipt_barrier => sub {}); 1 };
		ok(!$ok, "$mode H reader rejected");
		like($@, qr/HOT reader|distinct/, 'dedicated-session requirement is explicit');
		is(scalar(map { @{$_->{calls}} } @sessions), 0, 'no SQL on invalid session wiring');
	}
};

subtest 'H reader must be a different backend on the same live instance' => sub {
	for my $case (qw(node pid postmaster_start))
	{
		my ($driver, $sessions) = fixture();
		$sessions->[4]{$case} = $case eq 'node' ? 3 : $case eq 'pid' ? 102 : 'other postmaster';
		my $ok = eval { $driver->run(); 1 };
		ok(!$ok, "wrong H reader $case rejected");
		like($@, qr/identity|backend|instance/, 'live identity mismatch is explicit');
		unlike(join('\n', map { @{$_->{calls}} } @{$sessions}), qr/CREATE SCHEMA|UPDATE /,
			'identity refusal precedes table creation and workload');
	}
};

subtest 'H ordinary full-path disagreement fails the comparison' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[4]{bad_off_rows} = 1;
	ok(!eval { $driver->run(); 1 }, 'OFF control mismatch rejects the attempt');
	like($@, qr/row comparison/, 'mismatch is not reported as agreement');
	is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 0, 'Q does not run after H disagreement');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[4]{calls}}), 1, 'held H snapshot cleaned once');
};

subtest 'H fastpath enable refusal is not bypassed' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[4]{fail_fastpath_enable} = 1;
	ok(!eval { $driver->run(); 1 }, 'SET failure rejects attempt');
	like($@, qr/fastpath enable failed/, 'original SET failure preserved');
	is(scalar(grep { /cluster_cr_test_target_image/ } @{$sessions->[4]{calls}}), 0, 'no fallback after SET failure');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[4]{calls}}), 1, 'reader cleaned once');
};

for my $failure (qw(fail_hot_begin fail_commit))
{
	subtest "H transaction failure: $failure" => sub {
		my ($driver, $sessions) = fixture();
		$sessions->[2]{$failure} = 1;
		ok(!eval { $driver->run(); 1 }, 'H transaction failure rejects attempt');
		like($@, qr/injected (?:H begin|commit) failure/, 'original transaction error preserved');
		is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[2]{calls}}), $failure eq 'fail_commit' ? 1 : 0,
			'COMMIT is never retried');
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[2]{calls}}), 1, 'writer gets one cleanup attempt');
		is(scalar(grep { /\AVACUUM/ } @{$sessions->[2]{calls}}), 0, 'no VACUUM after failed transaction');
		is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 0, 'Q has not started');
		is($driver->{lanes}{'H/HOT'}{commit_status}, 'UNKNOWN', 'failure does not invent a transaction outcome')
			if $failure eq 'fail_commit';
	};
}

subtest 'H commit acknowledgement survives a failing result journal' => sub {
	my ($driver, $sessions) = fixture(undef, undef, sub {
		my ($entry) = @_;
		die "H commit journal failed\n" if $entry->{kind} eq 'sql_result' && $entry->{operation} eq 'commit_hot_writer';
	});
	ok(!eval { $driver->run(); 1 }, 'journal failure rejects attempt');
	like($@, qr/H commit journal failed/, 'journal error preserved');
	is($driver->{lanes}{'H/HOT'}{commit_status}, 'ACKNOWLEDGED', 'actual H acknowledgement retained');
	is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[2]{calls}}), 1, 'H COMMIT once');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[2]{calls}}), 0, 'acknowledged H is not rolled back');
	for my $slot (1, 4)
	{
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[$slot]{calls}}), 1, 'each reader still cleaned once');
	}
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
	for my $node (1, 4)
	{
		is(scalar(grep(/\AROLLBACK/, @{$sessions->[$node]{calls}})), 1,
			'each held reader receives one cleanup attempt');
	}
};

subtest 'SQL error and cleanup error both survive without retry' => sub {
	my ($driver, $sessions, $journal) = fixture();
	$sessions->[2]{fail_update} = 1;
	$sessions->[4]{fail_rollback} = 1;
	ok(!eval { $driver->run(); 1 }, 'SQL failure rejects run');
	like($@, qr/injected SQL failure/, 'original error retained');
	like($@, qr/injected rollback failure/, 'cleanup error also retained');
	is(scalar(grep(/UPDATE/, @{$sessions->[2]{calls}})), 1, 'failed UPDATE attempted once');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[2]{calls}}), 1, 'open H writer cleaned once');
	for my $node (1, 4)
	{
		is(scalar(grep(/\AROLLBACK/, @{$sessions->[$node]{calls}})), 1,
			'cleanup tried once for each held reader');
	}
	ok(grep($_->{kind} eq 'sql_error', @{$journal}), 'SQL failure output retained');
};

for my $slot (3, 4)
{
	subtest "session replacement invalidates final SQL identity: $slot" => sub {
		my ($driver, $sessions) = fixture();
		$sessions->[$slot]{reconnect} = 1;
		ok(!eval { $driver->run(); 1 }, 'changed backend PID rejected');
		like($@, qr/identity/, 'identity mismatch is explicit');
	};
}

subtest 'Q uses a disjoint ordinary transaction and caller capture boundaries' => sub {
	my ($driver, $sessions, $journal, undef, $receipt) = fixture(undef, sub {
		my ($phase, $evidence) = @_;
		$evidence->{pg_full_xid} = 'forged';
		$evidence->{commit_status} = 'FORGED';
		return {ready_to_run => JSON::PP::true, receipt_stop_seal => 'PASS'};
	});
	my $result = $driver->run();
	my $q = $result->{lanes}{'Q/RECEIPT'} // {};
	is($q->{pg_full_xid}, '18446744073709551614', 'actual full xid retained exactly above numeric precision limits');
	is($q->{commit_status}, 'ACKNOWLEDGED', 'SQL commit acknowledgement retained');
	is_deeply($q->{baseline}, [{id => 1, v => 'q_seed'}], 'independent Q baseline retained');
	is_deeply($q->{changed}, [{id => 1, v => 'q_committed', ctid => '(0,1)'}], 'actual mutation result retained');
	is_deeply([map { $_->{phase} } @{$receipt}], ['before_commit', 'after_commit'], 'capture hook runs once per boundary');
	is_deeply([map { $_->{commit_calls} } @{$receipt}], [0, 1], 'capture brackets the one COMMIT');
	is($receipt->[0]{evidence}{commit_status}, 'NOT_SENT', 'precommit snapshot claims no acknowledgement');
	is($receipt->[1]{evidence}{commit_status}, 'ACKNOWLEDGED', 'postcommit snapshot retains SQL acknowledgement');
	my @q_sql = grep { /"q_receipt"/ } @{$sessions->[0]{calls}};
	ok(grep(/UPDATE .*WHERE id = 1/, @q_sql), 'Q mutates one deterministic key');
	is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 1, 'ordinary COMMIT once');
	ok(!grep(/PREPARE TRANSACTION/, map { @{$_->{calls}} } @{$sessions}), 'no PostgreSQL two-phase transaction');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[0]{calls}}), 0, 'acknowledged transaction is not rolled back');
	my @operations = map { $_->{operation} } grep { $_->{kind} eq 'sql_result' } @{$journal};
	my ($q_begin) = grep { $operations[$_] eq 'begin_receipt' } 0 .. $#operations;
	my ($vacuum) = grep { $operations[$_] eq 'vacuum' } 0 .. $#operations;
	my ($q_seed) = grep { $operations[$_] eq 'seed_receipt' } 0 .. $#operations;
	my ($r_update) = grep { $operations[$_] eq 'read_image_update' } 0 .. $#operations;
	ok(defined($q_begin) && $q_begin > $vacuum, 'Q transaction follows H');
	ok(defined($q_seed) && $q_seed < $r_update, 'Q seeded before workload mutations');
	is($q->{locator}{relNumber}, 503, 'Q physical relation differs from R and H');
	is($result->{receipt_stop_seal}, 'NOT_CHECKED', 'callback return cannot grant a receipt/stop seal');
	is($result->{trigger_status}, 'UNBOUND', 'SQL xid does not supply ITL or receipt identity');
	ok(!$result->{ready_to_run}, 'callback return cannot certify readiness');
};

subtest 'Q locator collision is refused before workload UPDATE' => sub {
	my ($driver, $sessions) = fixture();
	$sessions->[0]{q_locator_collision} = 1;
	my $ok = eval { $driver->run(); 1 };
	my $error = $@;
	ok(!$ok, 'Q/R physical relation collision rejected');
	like($error, qr/lane relation collision/, 'physical collision is explicit');
	unlike(join('\n', map { @{$_->{calls}} } @{$sessions}), qr/UPDATE /, 'no workload mutation after collision');
};

subtest 'invalid Q full xid cannot be replaced with a derived identity' => sub {
	for my $xid (undef, '0', '18446744073709551616', '7fake')
	{
		my ($driver, $sessions) = fixture();
		$sessions->[0]{full_xid} = $xid;
		my $ok = eval { $driver->run(); 1 };
		my $error = $@;
		ok(!$ok, 'invalid full xid rejected');
		like($error, qr/invalid PostgreSQL full xid/, 'SQL identity refusal is explicit');
		is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 0, 'no COMMIT after invalid identity');
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[0]{calls}}), 1, 'Q gets one cleanup attempt');
	}
};

for my $phase (qw(before_commit after_commit))
{
	subtest "receipt capture failure at $phase is not retried" => sub {
		my ($driver, $sessions, undef, undef, $receipt) = fixture(undef, sub {
			my ($at) = @_;
			die "capture $phase failed\n" if $at eq $phase;
		});
		my $ok = eval { $driver->run(); 1 };
		my $error = $@;
		ok(!$ok, 'capture failure rejects the attempt');
		like($error, qr/capture $phase failed/, 'capture error retained');
		my $committed = $phase eq 'after_commit' ? 1 : 0;
		is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), $committed, 'no COMMIT retry');
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[0]{calls}}), 1 - $committed,
			'only an unacknowledged Q transaction gets cleanup');
		for my $node (1, 4)
		{
			is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[$node]{calls}}), 1, 'held reader cleaned once');
		}
		is(scalar(grep { $_->{phase} eq $phase } @{$receipt}), 1, 'failing capture runs once');
		ok(!eval { $driver->run(); 1 }, 'failed attempt stays consumed');
	};
}

for my $failure (qw(fail_q_begin fail_q_update fail_commit))
{
	subtest "Q SQL failure: $failure" => sub {
		my ($driver, $sessions, $journal, undef, $receipt) = fixture();
		$sessions->[0]{$failure} = 1;
		my $ok = eval { $driver->run(); 1 };
		my $error = $@;
		ok(!$ok, 'Q failure rejects attempt');
		like($error, qr/injected (?:Q begin|Q mutation|commit) failure/, 'original SQL failure retained');
		is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), $failure eq 'fail_commit' ? 1 : 0,
			'COMMIT sent at most once');
		for my $node (0, 1, 4)
		{
			is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[$node]{calls}}), 1, 'open transaction gets one cleanup attempt');
		}
		is(scalar(grep { $_->{phase} eq 'after_commit' } @{$receipt}), 0, 'no postcommit acknowledgement capture');
		ok(grep($_->{kind} eq 'sql_error', @{$journal}), 'failed SQL retained');
	};
}

subtest 'SQL, rollback and aggregate errors survive a throwing journal' => sub {
	my ($driver, $sessions) = fixture(undef, undef, sub {
		my ($entry) = @_;
		die "journal $entry->{kind} failed\n" if $entry->{kind} eq 'sql_error' || $entry->{kind} eq 'driver_error';
	});
	$sessions->[2]{fail_update} = 1;
	$sessions->[4]{fail_rollback} = 1;
	my $ok = eval { $driver->run(); 1 };
	my $error = $@;
	ok(!$ok, 'journal error does not turn failure into success');
	like($error, qr/injected SQL failure/, 'original SQL error survives');
	like($error, qr/injected rollback failure/, 'cleanup error survives');
	like($error, qr/journal sql_error failed/, 'SQL error journal failure survives');
	like($error, qr/journal driver_error failed/, 'aggregate journal failure survives');
	for my $node (1, 4)
	{
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[$node]{calls}}), 1, 'cleanup still attempted once');
	}
};

subtest 'undefined SQL output retains its SQL error entry' => sub {
	my ($driver, $sessions, $journal) = fixture();
	$sessions->[0]{undefined_q_update} = 1;
	my $ok = eval { $driver->run(); 1 };
	my $error = $@;
	ok(!$ok, 'undefined output fails');
	like($error, qr/undefined SQL output/, 'undefined output error retained');
	ok(grep($_->{kind} eq 'sql_error' && $_->{operation} eq 'receipt_update', @{$journal}), 'failed query retained in journal');
	is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 0, 'no COMMIT after missing output');
};

subtest 'COMMIT acknowledgement precedes a failing result journal' => sub {
	my ($driver, $sessions, $journal) = fixture(undef, undef, sub {
		my ($entry) = @_;
		die "commit journal unavailable\n" if $entry->{kind} eq 'sql_result' && $entry->{operation} eq 'commit_receipt';
	});
	my $ok = eval { $driver->run(); 1 };
	my $error = $@;
	ok(!$ok, 'journal failure rejects attempt after SQL commit');
	like($error, qr/commit journal unavailable/, 'journal failure retained');
	is(scalar(grep { $_ eq 'COMMIT' } @{$sessions->[0]{calls}}), 1, 'COMMIT once');
	is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[0]{calls}}), 0, 'does not claim rollback of acknowledged commit');
	for my $node (1, 4)
	{
		is(scalar(grep { $_ eq 'ROLLBACK' } @{$sessions->[$node]{calls}}), 1, 'reader still cleaned once');
	}
};

subtest 'receipt capture hook is required before any SQL' => sub {
	my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
	my $ok = eval { WorkloadSql->new(sessions => \@sessions, schema => 'valid_schema',
		emit => sub {}, between_lanes => sub {}); 1 };
	my $error = $@;
	ok(!$ok, 'absent capture hook rejected');
	like($error, qr/receipt.*hook/, 'missing capture boundary explicit');
	is(scalar(map { @{$_->{calls}} } @sessions), 0, 'no SQL sent');
};

subtest 'unsafe schema is rejected before SQL' => sub {
	my @sessions = map { bless {node => $_, calls => []}, 'SqlBoundary' } 0 .. 3;
	ok(!eval { WorkloadSql->new(sessions => \@sessions, schema => 'x;DROP SCHEMA y',
		emit => sub {}, between_lanes => sub {}); 1 }, 'schema cannot inject SQL');
	is(scalar(map { @{$_->{calls}} } @sessions), 0, 'no SQL sent');
};

done_testing();
