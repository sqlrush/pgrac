#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 423_external_io_fence.pl
#    RF-ROOT P4 / STOP04 deterministic test-only acceptance witnesses.
#
# The positive provider is the same-host child transport compiled only into
# test_cluster_external_fence.  This TAP aggregates named direct witnesses;
# it neither registers a production provider nor closes deployment
# certification.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use File::Path qw(make_path);
use FindBin;
use IPC::Run ();
use Test::More;

my $root = abs_path("$FindBin::RealBin/../../../..");
my $build_root = $ENV{top_builddir} // $root;
my $unit_dir = "$build_root/src/test/cluster_unit";
my @suites = qw(
	test_cluster_external_fence
	test_pgrac_external_fence_protocol
	test_pgrac_fenced_core
	test_pgrac_fenced_journal
	test_pgrac_fenced_provider
	test_pgrac_fenced_operation
	test_pgrac_fenced_rejoin
	test_pgrac_fenced_rejoin_async
	test_pgrac_fenced_rejoin_coordinator
	test_pgrac_fenced_session
	test_pgrac_fenced_schedule
	test_pgrac_fenced_async
	test_pgrac_fenced_coordinator
	test_pgrac_fenced_ipmi
	test_pgrac_fenced_ipmi_exec
	test_pgrac_fenced_config
	test_pgrac_fenced_ctl
	test_cluster_recovery_serial
	test_cluster_recovery_merge
	test_cluster_reconfig
	test_cluster_control_root
	test_cluster_write_fence_cache
	test_cluster_shared_fs
	test_cluster_guc
);
my ($build_out, $build_err) = ('', '');
my %output;

IPC::Run::run(
	[ 'make', '-C', "$build_root/src/common", 'libpgcommon.a',
	  'libpgcommon_srv.a' ],
	'>', \$build_out, '2>', \$build_err)
	or BAIL_OUT("cannot build STOP04 common libraries: $build_err");

IPC::Run::run(
	[ 'make', '-C', $unit_dir, @suites ],
	'>', \$build_out, '2>', \$build_err)
	or BAIL_OUT("cannot build STOP04 direct witnesses: $build_err");

for my $suite (@suites)
{
	my ($stdout, $stderr) = ('', '');

	IPC::Run::run([ 'prove', '-v', "$unit_dir/$suite" ],
		'>', \$stdout, '2>', \$stderr)
		or BAIL_OUT("$suite failed: $stdout$stderr");
	$output{$suite} = $stdout . $stderr;
}

my $artifact_root = $ENV{TESTLOGDIR} // "$build_root/src/test/cluster_tap/tmp_check";
my $artifact_dir = "$artifact_root/423_external_io_fence";
make_path($artifact_dir);
open(my $artifact, '>', "$artifact_dir/provenance.json")
	or BAIL_OUT("cannot create test-only provenance: $!");
print {$artifact}
	qq|{"schema":1,"test_only":true,"provider_id":1,|,
	qq|"deployment_certified":false,"wire":"PFRQ/PFRS/PFRJ-v1"}\n|;
close($artifact) or BAIL_OUT("cannot close test-only provenance: $!");

my %witness = (
	'L04-01' => [
		[ test_cluster_external_fence =>
			'test_external_fence_provider_zero_is_unavailable_without_admission' ],
		[ test_pgrac_fenced_provider =>
			'test_production_registry_honors_fexecve_gate' ],
	],
	'L04-02' => [
		[ test_pgrac_fenced_provider =>
			'test_recovery_terminal_matrix_is_exact' ],
		[ test_pgrac_fenced_ipmi_exec =>
			'test_result_folding_requires_fresh_guid_before_status' ],
	],
	'L04-03' => [
		[ test_cluster_external_fence =>
			'test_external_fence_nonproof_signals_never_admit' ],
		[ test_pgrac_fenced_ipmi =>
			'test_uncertified_readback_never_claims_io_drained' ],
	],
	'L04-04' => [
		[ test_cluster_external_fence =>
			'test_external_fence_test_only_exact_response_creates_live_admission' ],
	],
	'L04-05' => [
		[ test_pgrac_external_fence_protocol =>
			'test_acquire_echo_binding_rejects_every_identity_mutation' ],
		[ test_pgrac_fenced_operation =>
			'test_protected_set_mismatch_has_zero_request_journal_and_action' ],
	],
	'L04-06' => [
		[ test_cluster_external_fence =>
			'test_external_fence_live_admission_detects_daemon_close' ],
	],
	'L04-07' => [
		[ test_cluster_external_fence =>
			'test_external_fence_admit_set_final_expiry_clears_whole_output' ],
		[ test_cluster_recovery_serial =>
			'test_recovery_serial_p4_revalidate_is_two_phase_fail_closed' ],
	],
	'L04-08' => [
		[ test_cluster_recovery_serial =>
			'test_recovery_serial_p4_revalidate_is_two_phase_fail_closed' ],
	],
	'L04-09' => [
		[ test_cluster_external_fence =>
			'test_external_fence_admit_set_reverses_partial_success' ],
	],
	'L04-10' => [
		[ test_cluster_external_fence =>
			'test_external_fence_admit_set_ands_all_sorted_writers' ],
		[ test_cluster_recovery_serial =>
			'test_recovery_serial_p4_acquire_requires_fresh_fence_then_actual_grant' ],
	],
	'L04-11' => [
		[ test_cluster_recovery_merge =>
			'test_external_fence_plan_native_and_restore_gate' ],
	],
	'L04-12' => [
		[ test_cluster_recovery_merge =>
			'test_external_fence_plan_native_and_restore_gate' ],
	],
	'L04-13' => [
		[ test_pgrac_fenced_journal =>
			'test_reconcile_actions_cover_all_durable_record_kinds' ],
		[ test_pgrac_fenced_provider =>
			'test_worker_crash_and_timeout_never_return_provider_success' ],
		[ test_pgrac_fenced_provider =>
			'test_worker_success_drains_descendants_after_leader_reap' ],
	],
	'L04-14' => [
		[ test_pgrac_fenced_journal =>
			'test_only_active_final_partial_record_is_truncatable' ],
		[ test_pgrac_fenced_journal =>
			'test_journal_rejects_crc_reserved_unknown_and_bad_proof' ],
	],
	'L04-15' => [
		[ test_pgrac_fenced_config =>
			'test_mapping_reload_generation_rules' ],
		[ test_cluster_external_fence =>
			'test_external_fence_mapping_reload_closes_old_and_fresh_admits' ],
		[ test_pgrac_fenced_operation =>
			'test_mapping_reload_is_durable_before_activation' ],
	],
	'L04-16' => [
		[ test_cluster_external_fence =>
			'test_external_fence_rejoin_rejects_unauthenticated_root_peer' ],
		[ test_pgrac_fenced_core => 'test_peer_policy_is_mutual_and_exact' ],
		[ test_pgrac_fenced_ctl =>
			'test_verify_journal_requires_exact_root_owned_regular_file' ],
		[ test_pgrac_fenced_ipmi_exec =>
			'test_runner_uses_fexecve_and_sanitized_environment' ],
	],
	'L04-17' => [
		[ test_cluster_external_fence =>
			'test_external_fence_rejoin_claim_receives_exact_offer' ],
		[ test_cluster_reconfig =>
			'test_external_rejoin_consumes_exact_candidate_before_jcmk_submit' ],
		[ test_pgrac_fenced_rejoin =>
			'test_claim_authorize_refresh_is_exact_and_on_happens_once' ],
		[ test_pgrac_fenced_rejoin_coordinator =>
			'test_socket_rejoin_invalidates_exact_scalar_and_releases_after_close' ],
	],
	'L04-18' => [
		[ test_pgrac_external_fence_protocol =>
			'test_pfrq_rejects_wrong_length_version_and_type' ],
		[ test_cluster_control_root =>
			'test_external_fence_bit24_activation_is_forbidden_without_provider' ],
	],
	'L04-19' => [
		[ test_cluster_external_fence =>
			'test_external_fence_e1_child_owns_independent_admission' ],
	],
	'L04-20' => [
		[ test_pgrac_fenced_core =>
			'test_capacity_rejects_129_without_evicting' ],
		[ test_pgrac_fenced_schedule =>
			'test_capacity_129_denies_without_mutating_existing_operations' ],
	],
	'L04-21' => [
		[ test_cluster_reconfig =>
			'test_external_rejoin_consumes_exact_candidate_before_jcmk_submit' ],
		[ test_cluster_recovery_serial =>
			'test_recovery_serial_acquire_set_zero_before_first_grant' ],
	],
	'L04-22' => [
		[ test_cluster_external_fence =>
			'test_external_rejoin_races_invalidate_sets_and_held_guard' ],
		[ test_cluster_recovery_serial =>
			'test_external_rejoin_new_epoch_invalidates_held_guard' ],
	],
	'L04-23' => [
		[ test_cluster_external_fence =>
			'test_external_fence_need_set_rejects_incomplete_authority' ],
		[ test_cluster_control_root =>
			'test_external_fence_bit24_activation_is_forbidden_without_provider' ],
	],
	'L04-24' => [
		[ test_cluster_external_fence =>
			'test_external_fence_need_set_rejects_incomplete_authority' ],
		[ test_cluster_external_fence =>
			'test_external_fence_admit_set_reverses_partial_success' ],
	],
	'L04-25' => [
		[ test_cluster_recovery_merge =>
			'test_external_fence_plan_native_and_restore_gate' ],
	],
	'L04-26' => [
		[ test_cluster_reconfig =>
			'test_rejoin_failure_snapshot_requires_exact_nonempty_survivors_and_floor' ],
		[ test_cluster_external_fence =>
			'test_external_fence_rejoin_claim_receives_exact_offer' ],
	],
	'L04-27' => [
		[ test_pgrac_fenced_core =>
			'test_queue_and_negative_readback_are_closed' ],
		[ test_pgrac_fenced_core =>
			'test_capacity_rejects_129_without_evicting' ],
		[ test_pgrac_fenced_coordinator =>
			'test_queued_deadline_is_durably_invalidated_without_action' ],
	],
	'L04-28' => [
		[ test_cluster_external_fence =>
			'test_external_fence_admission_pins_call_entry_lease_snapshot' ],
		[ test_cluster_guc => 'test_external_fence_guc_contract' ],
	],
);

plan tests => 28;
for my $number (1 .. 28)
{
	my $id = sprintf('L04-%02d', $number);
	my @missing;

	for my $anchor (@{ $witness{$id} })
	{
		my ($suite, $name) = @$anchor;
		push @missing, "$suite:$name"
			unless $output{$suite} =~ /\Q$name\E/;
	}
	ok(!@missing, "$id direct deterministic witness set is GREEN")
		or diag('missing direct anchor(s): ' . join(', ', @missing));
}
