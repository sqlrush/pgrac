# Exercise diagnostic availability through real shared memory and SQL.
use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use Math::BigInt;
use Test::More;

my @retain = qw(active_or_prepared record_writer seal_or_drain horizon_missing
  history_pin reference_pending release_not_durable publisher_pending identity_or_io);
my @reasons = qw(NONE PREPARED_DRAIN RESOURCE_X PAGE_REVALIDATE WAL_DURABILITY
  PARTICIPANT_ACK BLOCK0_CERTIFICATE BLOCKED);
my @locator = qw(old_tt_locator_fallback_count old_tt_locator_fallback_result.found
  old_tt_locator_fallback_result.missing old_tt_locator_fallback_result.ambiguous
  old_tt_locator_fallback_result.scan_unavailable);
for my $family (qw(runtime_active runtime_terminal mx_current_origin mx_projection))
{
	push @locator, map { "old_tt_locator_family.$family.$_" }
	  qw(fallback_count fallback_result.found fallback_result.missing
	  fallback_result.ambiguous fallback_result.scan_unavailable);
}

# Availability is independent for every original producer. The origin domains
# differ: capacity/CTRC use 0..15; durable locator/rollover use 0..127.
my @groups = (
	['undo_cleaner', 'tt_capacity_stats_available', 'tt_capacity_origin_node_id', 16,
		[qw(tt_slot_active_full_wait_count tt_capacity_pool_wait_count
		  tt_capacity_wakeup_recheck_count tt_capacity_cancel_count
		  tt_capacity_refuse_by_reason.context tt_capacity_refuse_by_reason.proof
		  capacity_wait_entered capacity_wait_repolled capacity_wait_refused_context
		  capacity_wait_refused_proof), map { "undo_retain_by_reason.$_" } @retain]],
	['undo', 'old_tt_locator_stats_available', 'old_tt_locator_origin_node_id', 128, \@locator],
	['undo', 'tt_retained_rollover_stats_available', undef, 128,
		[qw(tt_retained_rollover_winner_count tt_retained_rollover_lost_race_count)]],
	['ctrc', 'ctrc_completion_stats_available', 'ctrc_completion_origin_node_id', 16,
		['ctrc_real_completion_count']],
	['ctrc', 'ctrc_phase_no_progress_stats_available', 'ctrc_phase_no_progress_origin_node_id', 16,
		[map { "ctrc_phase_no_progress_by_reason.$_" } @reasons]],
	['ctrc', 'ctrc_publisher_stats_available', 'ctrc_publisher_origin_node_id', 16,
		['ctrc_publisher_pending_count']],
	['ctrc', 'ctrc_unsettled_stats_available', 'ctrc_unsettled_origin_node_id', 16,
		['ctrc_unsettled_count']],
	['ctrc', 'ctrc_oldest_unsettled_age_stats_available', undef, 16,
		['ctrc_oldest_unsettled_age_ms']]);

my $node = PostgreSQL::Test::Cluster->new('undo_capacity_stats');
$node->init;
$node->append_conf('postgresql.conf', qq{
cluster.enabled = off
cluster.shared_config = off
autovacuum = off
shared_buffers = '8MB'
max_connections = 10
});

for my $origin (-1, 0, 15, 16, 127)
{
	$node->append_conf('postgresql.conf', "cluster.node_id = $origin\n");
	$node->start;
	subtest "diagnostic origin $origin" => sub {
		is($node->safe_psql('postgres', 'SHOW cluster.node_id'), "$origin",
			'the postmaster uses the requested origin');
		is($node->safe_psql('postgres', 'SHOW cluster.enabled'), 'off',
			'this diagnostic test does not activate cluster operation');
		# The materialized SRF is intentionally sampled once per postmaster.
		# WHERE filters output only; the original full dump/ensure cost remains.
		my $raw = $node->safe_psql('postgres', q{
SET statement_timeout = '10s';
WITH snapshot AS MATERIALIZED (SELECT category, key, value FROM pg_cluster_state)
SELECT category || E'\t' || key || E'\t' || value FROM snapshot
WHERE category IN ('undo', 'undo_cleaner', 'ctrc')
ORDER BY category, key});
		my %rows;
		for my $line (split /\n/, $raw)
		{
			my ($category, $key, $value) = split /\t/, $line, 3;
			ok(defined($value), 'the snapshot row has three non-NULL columns');
			next unless defined($value);
			push @{ $rows{"$category/$key"} }, $value;
		}
		for my $group (@groups)
		{
			my ($category, $availability, $origin_key, $limit, $counts) = @$group;
			my $available = $origin >= 0 && $origin < $limit;
			my $flag = $rows{"$category/$availability"} // [];
			is_deeply($flag, [$available ? 'true' : 'false'],
				"$availability is explicit and unique");
			if (defined($origin_key))
			{
				is_deeply($rows{"$category/$origin_key"} // [],
					$available ? ["$origin"] : [], "$origin_key follows availability");
			}
			for my $key (@$counts)
			{
				my $values = $rows{"$category/$key"} // [];
				is(scalar(@$values), $available ? 1 : 0,
					"$key is unique when available and absent otherwise");
				next unless $available && @$values == 1;
				like($values->[0], qr/\A(?:0|[1-9][0-9]*)\z/, "$key is unsigned decimal");
				ok(Math::BigInt->new($values->[0])->bcmp('18446744073709551615') <= 0,
					"$key fits uint64");
			}
		}
		if ($origin >= 0 && $origin < 16)
		{
			for my $alias (
				['capacity_wait_entered', 'tt_capacity_pool_wait_count'],
				['capacity_wait_refused_context', 'tt_capacity_refuse_by_reason.context'],
				['capacity_wait_refused_proof', 'tt_capacity_refuse_by_reason.proof'])
			{
				is_deeply($rows{"undo_cleaner/$alias->[0]"}, $rows{"undo_cleaner/$alias->[1]"},
					"$alias->[0] preserves its original scope");
			}
			my $cv = $rows{'undo_cleaner/capacity_wait_repolled'} // [];
			my $total = $rows{'undo_cleaner/tt_capacity_wakeup_recheck_count'} // [];
			if (@$cv == 1 && @$total == 1)
			{
				ok(Math::BigInt->new($total->[0])->bcmp($cv->[0]) >= 0,
					'the combined recheck count includes the original CV count');
			}
		}
	};
	$node->stop;
}

done_testing();
