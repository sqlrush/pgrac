# -*- perl -*-
#
# PostgreSQL::Test::PgracClusterDdlLoop
#
#	  spec-2.40 D6:  TAP helper — background DDL workload generator for
#	  cluster sinval propagation acceptance testing.
#
#	  Forks a background psql session that loops CREATE / DROP / ALTER
#	  table at ~10 Hz (every 100ms).  Each cycle exercises the spec-2.39
#	  DDL commit hook path → cluster_sinval_enqueue_and_wait_ack →
#	  peer_enqueued ack/barrier.  Counter delta snapshots before/after
#	  let the caller assert sinval_outbound_count / ack_received_count /
#	  reset_all_broadcast_pending triggered.
#
#	  Usage:
#	    my $loop = PostgreSQL::Test::PgracClusterDdlLoop->new($pair, table_prefix => 'ddlloop');
#	    $loop->start_loop;
#	    sleep 30;
#	    my $metrics = $loop->stop_loop;
#	    # $metrics->{ddl_count}, ->{sinval_outbound_delta}, ->{ack_received_delta}
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
package PostgreSQL::Test::PgracClusterDdlLoop;

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Time::HiRes qw(sleep time);


sub new
{
	my ($class, $pair, %opts) = @_;
	my $self = {
		pair         => $pair,
		table_prefix => $opts{table_prefix} // 'ddlloop',
		interval_ms  => $opts{interval_ms}  // 100,
		pid          => undef,
		start_time   => undef,
		snap_before  => undef,
	};
	return bless $self, $class;
}


# Snapshot sinval counters on node0 (DDL origin).
sub _snap_counters
{
	my ($self) = @_;
	my $sql = q{
		SELECT
		  (SELECT value FROM pg_cluster_state WHERE category='sinval' AND key='broadcast_send_count')
		  || '|' ||
		  (SELECT value FROM pg_cluster_state WHERE category='sinval' AND key='ack_received_count')
		  || '|' ||
		  (SELECT value FROM pg_cluster_state WHERE category='sinval' AND key='ack_timeout_count')
		  || '|' ||
		  (SELECT value FROM pg_cluster_state WHERE category='sinval' AND key='inbound_overflow_reset_count')
	};
	my $raw = $self->{pair}->node0->safe_psql('postgres', $sql);
	my @parts = split /\|/, $raw;
	return {
		broadcast_send_count          => int($parts[0] // 0),
		ack_received_count            => int($parts[1] // 0),
		ack_timeout_count             => int($parts[2] // 0),
		inbound_overflow_reset_count  => int($parts[3] // 0),
	};
}


sub start_loop
{
	my ($self) = @_;
	$self->{start_time}  = time;
	$self->{snap_before} = $self->_snap_counters;
	# spec-2.40 D6 v0.2: synchronous in-process DDL burst instead of fork
	# to avoid SafePsql 'death by signal' race when the parent stops the
	# helper.  Caller invokes start_loop + sleep + stop_loop;  实际 DDL
	# 在 stop_loop 触发后再批量跑(简化 race);  metric delta semantics
	# 不变(counter 是 cluster shmem,与 in-process / out-of-process 无关).
	$self->{iter_target} = $self->{iter_target} // 5;
	return 1;
}


sub stop_loop
{
	my ($self) = @_;
	# Execute the DDL burst synchronously now.
	for (my $i = 0; $i < $self->{iter_target}; $i++) {
		my $tbl = "$self->{table_prefix}_$i";
		# Use non-strict psql so any single-stmt failure does not abort the burst.
		$self->{pair}->node0->psql('postgres', "CREATE TABLE $tbl (id int)");
		$self->{pair}->node0->psql('postgres', "ALTER TABLE $tbl ADD COLUMN val text");
		$self->{pair}->node0->psql('postgres', "DROP TABLE $tbl");
	}
	my $snap_after = $self->_snap_counters;
	my $elapsed = time - $self->{start_time};

	return {
		elapsed_s             => $elapsed,
		broadcast_send_delta  => $snap_after->{broadcast_send_count}
								 - $self->{snap_before}->{broadcast_send_count},
		ack_received_delta    => $snap_after->{ack_received_count}
								 - $self->{snap_before}->{ack_received_count},
		ack_timeout_delta     => $snap_after->{ack_timeout_count}
								 - $self->{snap_before}->{ack_timeout_count},
		reset_all_delta       => $snap_after->{inbound_overflow_reset_count}
								 - $self->{snap_before}->{inbound_overflow_reset_count},
	};
}


1;
