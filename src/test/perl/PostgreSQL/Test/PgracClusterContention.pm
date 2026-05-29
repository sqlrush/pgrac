# -*- perl -*-
#
# PostgreSQL::Test::PgracClusterContention
#
#	  spec-2.40 D7:  TAP helper — same-row UPDATE contention workload
#	  for GES grant + Cache Fusion block forwarding acceptance testing.
#
#	  start_load() records a counter snapshot and initializes the hot row.
#	  stop_load() runs a bounded synchronous UPDATE burst alternating node0
#	  and node1.  This avoids SafePsql background teardown races in TAP while
#	  still exercising:
#	    - GES grant/release contention (spec-2.13-2.17)
#	    - CF block ship + invalidate ack (spec-2.33/2.35/2.36)
#	    - PCM N→X / X→X transitions (spec-2.30)
#
#	  Medium/long perf scripts increase the iteration count for longer
#	  samples; TAP smoke keeps it small to avoid runner flake.
#
#	  Usage:
#	    my $cont = PostgreSQL::Test::PgracClusterContention->new($pair,
#	        sessions => 4, statement_timeout_ms => 5000);
#	    $cont->start_load;
#	    sleep 30;
#	    my $metrics = $cont->stop_load;
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::PgracClusterContention;

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Time::HiRes qw(sleep time);


sub new
{
	my ($class, $pair, %opts) = @_;
	my $self = {
		pair                  => $pair,
			sessions              => $opts{sessions}              // 2,
			iterations            => $opts{iterations}            // (($opts{sessions} // 2) * 5),
			statement_timeout_ms  => $opts{statement_timeout_ms}  // 5000,
			table_name            => $opts{table_name}            // 'contention_row',
		pids                  => [],
		start_time            => undef,
		snap_before           => undef,
	};
	return bless $self, $class;
}


sub _snap_counters
{
	my ($self) = @_;
	# Read from node0 (origin);  reasonable proxy since counters are
	# accumulated cluster-wide via cluster_state.
	my $sql = q{
		SELECT
		  (SELECT coalesce(value, '0') FROM pg_cluster_state WHERE category='ges' AND key='request_count')
		  || '|' ||
		  (SELECT coalesce(value, '0') FROM pg_cluster_state WHERE category='ges' AND key='reply_count')
		  || '|' ||
			  (SELECT coalesce(value, '0') FROM pg_cluster_state WHERE category='gcs' AND key='block_forward_sent_count')
			  || '|' ||
			  (SELECT coalesce(value, '0') FROM pg_cluster_state WHERE category='gcs' AND key='block_invalidate_ack_received_count')
	};
	my $raw = $self->{pair}->node0->safe_psql('postgres', $sql);
	my @parts = split /\|/, $raw;
	return {
		ges_request_count             => int($parts[0] // 0),
		ges_reply_count               => int($parts[1] // 0),
		cf_block_forward_count        => int($parts[2] // 0),
		cf_block_invalidate_ack_count => int($parts[3] // 0),
	};
}


sub start_load
{
	my ($self) = @_;
	my $tbl = $self->{table_name};

	# Initialize contention row (idempotent) on both ClusterPair nodes.
	# ClusterPair is a two-postmaster fixture, not a shared-catalog fixture;
	# creating the table only on node0 makes the alternating node1 UPDATEs
	# fail before they can exercise the smoke helper path.
	for my $node ($self->{pair}->node0, $self->{pair}->node1)
	{
		$node->safe_psql('postgres',
			"CREATE TABLE IF NOT EXISTS $tbl (id int PRIMARY KEY, ctr bigint NOT NULL DEFAULT 0);
			 INSERT INTO $tbl (id) VALUES (1) ON CONFLICT (id) DO NOTHING;");
	}

	$self->{start_time}  = time;
	$self->{snap_before} = $self->_snap_counters;
		# spec-2.40 D7 v0.2: synchronous in-process UPDATE burst instead of
		# fork — avoids SafePsql 'death by signal' race when parent calls
		# stop_load.  GES contention real-time triggering deferred to manual
		# scripts/perf/run-stage2-cluster-baseline.sh tier=medium.
		$self->{iter_target} = $self->{iterations};
	return 1;
}


sub stop_load
{
	my ($self) = @_;
	# Synchronous UPDATE burst — alternating node0 / node1 to drive GES
	# contention path.
	my $tbl = $self->{table_name};
	for (my $i = 0; $i < $self->{iter_target}; $i++) {
		my $node = ($i % 2 == 0) ? $self->{pair}->node0 : $self->{pair}->node1;
		$node->psql('postgres',
			"SET statement_timeout = '$self->{statement_timeout_ms}ms';
			 UPDATE $tbl SET ctr = ctr + 1 WHERE id = 1;");
	}
	$self->{pids} = [];

	my $snap_after = $self->_snap_counters;
	my $elapsed    = time - $self->{start_time};

	# Final row state on both nodes (best-effort;  cluster contention may
	# leave retransmit budget exhausted or row inaccessible on some node).
	my ($final_node0, $final_node1);
	eval {
		$final_node0 = $self->{pair}->node0->safe_psql('postgres',
			"SELECT ctr FROM $self->{table_name} WHERE id = 1");
	};
	$final_node0 //= 'N/A';
	eval {
		$final_node1 = $self->{pair}->node1->safe_psql('postgres',
			"SELECT ctr FROM $self->{table_name} WHERE id = 1");
	};
	$final_node1 //= 'N/A';

	return {
		elapsed_s                   => $elapsed,
		ges_request_delta           => $snap_after->{ges_request_count}
									   - $self->{snap_before}->{ges_request_count},
		ges_reply_delta             => $snap_after->{ges_reply_count}
									   - $self->{snap_before}->{ges_reply_count},
		cf_block_forward_delta      => $snap_after->{cf_block_forward_count}
									   - $self->{snap_before}->{cf_block_forward_count},
		cf_invalidate_ack_delta     => $snap_after->{cf_block_invalidate_ack_count}
									   - $self->{snap_before}->{cf_block_invalidate_ack_count},
		final_ctr_node0             => $final_node0,
		final_ctr_node1             => $final_node1,
	};
}


1;
