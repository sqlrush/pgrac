
# spec-2.2 D15 -- ClusterPair: 2-instance harness for tier1 TAP tests.
#
# Encapsulates the boilerplate that 076 (and future Stage 2.X tests) need
# to spin up two cooperating pgrac instances with mutual interconnect:
#   - allocate 4 random free ports (2 PG + 2 IC)
#   - init both nodes
#   - append cluster.* GUCs (cluster.enabled = on, tier = tier1, node_id)
#     using append_conf (per spec-2.1 v1.0.1 L59 -- adjust_conf is
#     replace-only and would no-op for first-time GUC writes)
#   - write mutually-trusting pgrac.conf to both data dirs
#   - start_pair / stop_pair / heartbeat_seen helpers
#
# Spec authority: pgrac:specs/spec-2.2-* §4.3.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterPair;

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;


#-----------------------------------------------------------------------
# new_pair($class, $cluster_name, %opts)
#
#	Allocate two PG instances that share a pgrac cluster name.
#	Optional %opts:
#	  cluster_name_override  : write a different name into the second
#	                           node's pgrac.conf (used by 076 L6 to
#	                           force HELLO mismatch; default = same)
#	  extra_conf             : arrayref of extra GUC lines appended
#	                           to BOTH nodes' postgresql.conf
#-----------------------------------------------------------------------
sub new_pair
{
	my ($class, $cluster_name, %opts) = @_;

	# Allocate 4 distinct free ports.  get_free_port() reserves the port
	# until the test exits, so successive calls return distinct values.
	my $pg_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $pg_port_1 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_1 = PostgreSQL::Test::Cluster::get_free_port();

	my $node0 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node0", port => $pg_port_0);
	my $node1 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node1", port => $pg_port_1);

	for my $node ($node0, $node1)
	{
		$node->init;

		# spec-2.2 §3.3 -- enable cluster + tier1.  per L59 first-time
		# GUC writes use append_conf (adjust_conf is replace-only).
		$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
		$node->append_conf('postgresql.conf',
			"cluster.interconnect_tier = tier1\n");
		$node->append_conf('postgresql.conf',
			"cluster.allow_single_node = off\n");

		# Keep shared_buffers small so 2 postmasters fit in CI runners
		# (R8 mitigation).
		$node->append_conf('postgresql.conf', "shared_buffers = 16MB\n");

		if ($opts{extra_conf})
		{
			for my $line (@{ $opts{extra_conf} })
			{
				$node->append_conf('postgresql.conf', "$line\n");
			}
		}
	}

	$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

	# Mutual pgrac.conf for node0 -- declares both peers in the same cluster.
	my $pgrac_conf_0 = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	# node1's pgrac.conf -- same except optionally a different cluster_name
	# (L6 HELLO mismatch test).
	my $alt_name = $opts{cluster_name_override} // $cluster_name;
	my $pgrac_conf_1 = <<EOC;
[cluster]
name = $alt_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	PostgreSQL::Test::Utils::append_to_file(
		$node0->data_dir . '/pgrac.conf', $pgrac_conf_0);
	PostgreSQL::Test::Utils::append_to_file(
		$node1->data_dir . '/pgrac.conf', $pgrac_conf_1);

	return bless {
		node0       => $node0,
		node1       => $node1,
		cluster_name => $cluster_name,
		pg_ports    => [ $pg_port_0, $pg_port_1 ],
		ic_ports    => [ $ic_port_0, $ic_port_1 ],
	}, $class;
}


sub start_pair
{
	my ($self, %opts) = @_;
	$self->{node0}->start(%opts);
	$self->{node1}->start(%opts);
	return;
}

sub stop_pair
{
	my ($self) = @_;
	$self->{node0}->stop if $self->{node0};
	$self->{node1}->stop if $self->{node1};
	return;
}

sub node0   { return $_[0]->{node0}; }
sub node1   { return $_[0]->{node1}; }
sub ic_port { return $_[0]->{ic_ports}[ $_[1] ]; }


#-----------------------------------------------------------------------
# heartbeat_seen($self, $from, $to)
#
#	Returns 't' / 'f' as a string -- whether $from instance has sent
#	at least one heartbeat to $to peer (heartbeat_send_count > 0).
#	Caller wraps in a poll loop with timeout.
#-----------------------------------------------------------------------
sub heartbeat_seen
{
	my ($self, $from, $to) = @_;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	return $node->safe_psql(
		'postgres',
		"SELECT heartbeat_send_count > 0 FROM pg_cluster_ic_peers WHERE node_id = $to"
	);
}


#-----------------------------------------------------------------------
# wait_for_peer_state($self, $from, $to, $expected_state, $timeout_s)
#
#	Polls $from's pg_cluster_ic_peers.state for $to until it matches
#	$expected_state or $timeout_s elapses.  Returns 1 on success, 0 on
#	timeout.  Default timeout = 10 s.
#-----------------------------------------------------------------------
sub wait_for_peer_state
{
	my ($self, $from, $to, $expected_state, $timeout_s) = @_;
	$timeout_s //= 10;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		my $state = $node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $to");
		return 1 if defined $state && $state eq $expected_state;
		select(undef, undef, undef, 0.25);
	}
	return 0;
}


1;
