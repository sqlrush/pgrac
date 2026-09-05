/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_locator.c
 *	  R4 exact caller-selected transaction-locator tests.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/multixact.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tx_resolve.h"

extern ClusterTxOutcome cluster_runtime_visibility_resolve_exact_origin_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome cluster_runtime_visibility_resolve_exact_origin(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	uint64 formation_epoch, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);
extern ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_exact(
	const ClusterTxLocator *locator, SCN retained_commit_scn,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out);

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
int cluster_node_id = 0;
bool cluster_recmerge_window_active = false;

static ClusterSemanticAdmissionResult test_admission_result
	= CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
static ClusterSemanticAdmissionResult test_terminal_census_admission_result
	= CLUSTER_SEMANTIC_ADMISSION_OK;
static bool test_recheck_result = true;
static bool test_terminal_census_recheck_result = true;
static uint64 test_formation_epoch = UINT64_C(41);
static ClusterTxOutcome test_provider_outcome = CLUSTER_TX_COMMITTED;
static ClusterTxResolveReason test_provider_reason = CLUSTER_TX_RESOLVE_NONE;
static ClusterTxResolution test_provider_resolution;
static ClusterTxLocator test_provider_locator;
static ClusterTxResolveMode test_provider_mode;
static uint64 test_provider_epoch;
static int test_enter_calls;
static int test_terminal_census_enter_calls;
static int test_recheck_calls;
static int test_terminal_census_recheck_calls;
static int test_leave_calls;
static int test_provider_calls;
static int test_runtime_exit_hooks_calls;
static int test_legacy_provider_calls;
static int test_admitted_provider_calls;
static bool test_provider_raise;
static bool test_provider_mutates_epoch;
static int test_node_count = 1;
static uint64 test_observed[CLUSTER_R4_OBSERVATION_EVENT_COUNT];

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pfree(void *pointer)
{
	free(pointer);
}

int
GetMultiXactIdMembersWithOffset(MultiXactId multi pg_attribute_unused(),
								MultiXactMember **members pg_attribute_unused(),
								bool from_pgupgrade pg_attribute_unused(),
								bool isLockOnly pg_attribute_unused(),
								MultiXactOffset *start_offset_out pg_attribute_unused())
{
	/* This fixture keeps TARGET admission closed for its dormant Multi test. */
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	test_enter_calls++;
	UT_ASSERT_EQ(feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	if (test_admission_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		memset(token, 0, sizeof(*token));
		token->feature_bit = feature_bit;
		token->record_generation = UINT64_C(73);
		token->formation_epoch = test_formation_epoch;
		token->side = (uint8)side;
		token->entered = true;
	}
	return test_admission_result;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(
	ClusterSemanticAdmissionToken *token)
{
	test_terminal_census_enter_calls++;
	if (test_terminal_census_admission_result
		== CLUSTER_SEMANTIC_ADMISSION_OK) {
		memset(token, 0, sizeof(*token));
		token->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		token->record_generation = UINT64_C(73);
		token->formation_epoch = test_formation_epoch;
		token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
		token->entered = true;
	}
	return test_terminal_census_admission_result;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	test_recheck_calls++;
	UT_ASSERT(token->entered);
	return test_recheck_result;
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(
	const ClusterSemanticAdmissionToken *token)
{
	test_terminal_census_recheck_calls++;
	UT_ASSERT(token->entered);
	UT_ASSERT_EQ(token->feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(token->side, CLUSTER_SEMANTIC_TARGET_SIDE);
	return test_terminal_census_recheck_result;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	test_leave_calls++;
	UT_ASSERT(token->entered);
	token->entered = false;
}

void
cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason pg_attribute_unused(),
				   ClusterCrBuildReason cr_reason pg_attribute_unused())
{
	UT_ASSERT((uint32)event < CLUSTER_R4_OBSERVATION_EVENT_COUNT);
	if ((uint32)event < CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		test_observed[event]++;
}

uint64
cluster_epoch_get_current(void)
{
	return test_formation_epoch;
}

int
cluster_conf_node_count(void)
{
	return test_node_count;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	uint64 formation_epoch, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	test_provider_calls++;
	test_legacy_provider_calls++;
	if (test_provider_raise)
		siglongjmp(*PG_exception_stack, 1);
	test_provider_locator = *locator;
	test_provider_mode = mode;
	test_provider_epoch = formation_epoch;
	*out = test_provider_resolution;
	*reason_out = test_provider_reason;
	return test_provider_outcome;
}

void
cluster_runtime_visibility_ensure_exit_hooks(void)
{
	test_runtime_exit_hooks_calls++;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_exact_origin_admitted(
	const ClusterTxLocator *locator, ClusterTxResolveMode mode,
	const ClusterSemanticAdmissionToken *admission, ClusterTxResolution *out,
	ClusterTxResolveReason *reason_out)
{
	test_provider_calls++;
	test_admitted_provider_calls++;
	UT_ASSERT(mode == CLUSTER_TX_RESOLVE_VISIBILITY
			  || mode == CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
	if (test_provider_raise)
		siglongjmp(*PG_exception_stack, 1);
	test_provider_locator = *locator;
	test_provider_mode = mode;
	UT_ASSERT_NOT_NULL(admission);
	test_provider_epoch = admission->formation_epoch;
	if (test_provider_mutates_epoch)
		test_formation_epoch = UINT64_C(1);
	*out = test_provider_resolution;
	*reason_out = test_provider_reason;
	return test_provider_outcome;
}

ClusterTxOutcome
cluster_runtime_visibility_resolve_terminal_census_retained_exact(
	const ClusterTxLocator *locator pg_attribute_unused(),
	SCN retained_commit_scn pg_attribute_unused(),
	const ClusterSemanticAdmissionToken *admission pg_attribute_unused(),
	ClusterTxResolution *out, ClusterTxResolveReason *reason_out)
{
	memset(out, 0, sizeof(*out));
	*reason_out = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
	return CLUSTER_TX_UNKNOWN;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static char test_page[BLCKSZ];

static Page
build_itl_page(void)
{
	PageHeader header;

	memset(test_page, 0, sizeof(test_page));
	header = (PageHeader)test_page;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	header->pd_upper = header->pd_special;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	return (Page)test_page;
}

static ClusterItlSlotData *
slot_at(Page page, uint8 index)
{
	return &ClusterPageGetItlSlots(page)[index];
}

static bool
bytes_are_zero(const void *ptr, size_t size)
{
	const unsigned char *bytes = ptr;
	size_t i;

	for (i = 0; i < size; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static ClusterTxLocator
exact_locator(void)
{
	ClusterTxLocator locator;

	memset(&locator, 0, sizeof(locator));
	locator.uba.raw[0] = (UINT64_C(408) << 32) | UINT64_C(11);
	locator.uba.raw[1] = (UINT64_C(5) << 16) | UINT64_C(6);
	locator.xid = (TransactionId)798;
	locator.tt_wrap = 7;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 3;
	return locator;
}

static void
reset_exact_resolver_fixture(void)
{
	ClusterTxLocator locator = exact_locator();

	test_formation_epoch = UINT64_C(41);
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	test_terminal_census_admission_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	test_recheck_result = true;
	test_terminal_census_recheck_result = true;
	test_provider_outcome = CLUSTER_TX_COMMITTED;
	test_provider_reason = CLUSTER_TX_RESOLVE_NONE;
	memset(&test_provider_resolution, 0, sizeof(test_provider_resolution));
	test_provider_resolution.locator_echo = locator;
	test_provider_resolution.top_xid = locator.xid;
	test_provider_resolution.outcome = CLUSTER_TX_COMMITTED;
	test_provider_resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	test_provider_resolution.commit_scn = (SCN)101;
	test_provider_resolution.horizon_scn = (SCN)89;
	test_provider_resolution.authority.origin_epoch = test_formation_epoch;
	test_provider_resolution.authority.tt_generation = UINT64_C(17);
	test_provider_resolution.authority.authority_scn = (SCN)103;
	memset(&test_provider_locator, 0, sizeof(test_provider_locator));
	test_provider_mode = (ClusterTxResolveMode)-1;
	test_provider_epoch = 0;
	test_enter_calls = 0;
	test_terminal_census_enter_calls = 0;
	test_recheck_calls = 0;
	test_terminal_census_recheck_calls = 0;
	test_leave_calls = 0;
	test_provider_calls = 0;
	test_legacy_provider_calls = 0;
	test_admitted_provider_calls = 0;
	test_provider_raise = false;
	test_provider_mutates_epoch = false;
	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_recmerge_window_active = false;
	test_node_count = 1;
	memset(test_observed, 0, sizeof(test_observed));
}

static ClusterSemanticAdmissionToken
epoch_zero_terminal_admission(void)
{
	ClusterSemanticAdmissionToken admission;

	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = 0;
	admission.formation_epoch = 0;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	return admission;
}

static void
prepare_epoch_zero_terminal_fixture(ClusterSemanticAdmissionToken *admission)
{
	reset_exact_resolver_fixture();
	test_formation_epoch = 0;
	test_provider_resolution.authority.origin_epoch = 0;
	*admission = epoch_zero_terminal_admission();
}

static ClusterTxLocator
prepare_epoch_zero_partial_visibility_fixture(void)
{
	ClusterTxLocator locator = exact_locator();

	reset_exact_resolver_fixture();
	test_formation_epoch = 0;
	test_node_count = 4;
	locator.tt_wrap = TT_WRAP_INVALID;
	locator.uba.raw[0] = (locator.uba.raw[0] & UINT64_C(0xffffffff00000000))
						 | UINT64_C(257);
	test_provider_resolution.locator_echo = locator;
	test_provider_resolution.locator_echo.tt_wrap = 7;
	test_provider_resolution.authority.origin_epoch = 0;
	return locator;
}

static void
assert_epoch_zero_refused(const ClusterTxLocator *locator,
					  ClusterTxResolveMode mode,
					  const ClusterSemanticAdmissionToken *admission,
					  ClusterTxResolveReason expected_reason)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
		locator, mode, admission, &resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, expected_reason);
	UT_ASSERT_EQ(test_provider_calls, 0);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

static uint64
observed_total(void)
{
	uint64 total = 0;
	int i;

	for (i = 0; i < CLUSTER_R4_OBSERVATION_EVENT_COUNT; i++)
		total += test_observed[i];
	return total;
}

static ClusterItlSlotData *
set_slot(Page page, uint8 index, uint8 flags, TransactionId xid, uint16 wrap, uint32 segment_id,
		 uint32 block_no, uint16 tt_slot_offset, uint16 row_offset)
{
	ClusterItlSlotData *slot = slot_at(page, index);

	slot->flags = flags;
	slot->xid = xid;
	slot->wrap = wrap;
	slot->undo_segment_head.raw[0] = ((uint64)block_no << 32) | segment_id;
	slot->undo_segment_head.raw[1] = ((uint64)row_offset << 16) | tt_slot_offset;
	return slot;
}

static void
assert_locator_failure(Page page, uint8 index, ClusterTxResolveReason expected_reason)
{
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	memset(&locator, 0xA5, sizeof(locator));
	UT_ASSERT(!cluster_tx_locator_from_itl(page, index, &locator, &reason));
	UT_ASSERT_EQ(reason, expected_reason);
	UT_ASSERT(bytes_are_zero(&locator, sizeof(locator)));
}

UT_TEST(test_frozen_identity_layout)
{
	UT_ASSERT_EQ(sizeof(ClusterUndoByteAddress), 16);
	UT_ASSERT_EQ(sizeof(ClusterTxLocator), 24);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, uba), 0);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, xid), 16);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, tt_wrap), 20);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, itl_kind), 22);
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, itl_slot_index), 23);
	UT_ASSERT_EQ(sizeof(ClusterMultiResolutionMember), 24);
}

UT_TEST(test_frozen_closed_domains)
{
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_VISIBILITY, 0);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_CLEANOUT_HINT, 3);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_TERMINAL_CENSUS, 4);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_TX_RESOLVE_PROTOCOL, 25);
	UT_ASSERT_EQ(CLUSTER_TX_UNKNOWN, 0);
	UT_ASSERT_EQ(CLUSTER_TX_ABORTED, 4);
	UT_ASSERT_EQ(CLUSTER_TX_PROOF_NONE, 0);
	UT_ASSERT_EQ(CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON, 7);
}

UT_TEST(test_reason_names_are_stable)
{
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_NONE), "none");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_TARGET_DISABLED),
					 "target_disabled");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_BAD_UBA), "bad_uba");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_XID_MISMATCH),
					 "xid_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_WRAP_MISMATCH),
					 "wrap_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_SLOT_MISMATCH),
					 "slot_mismatch");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name(CLUSTER_TX_RESOLVE_PROTOCOL), "protocol");
	UT_ASSERT_STR_EQ(cluster_tx_resolve_reason_name((ClusterTxResolveReason)26), "invalid_reason");
}

UT_TEST(test_exact_resolver_is_dormant_and_zeroes_output)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(
		cluster_tx_resolve_exact(NULL, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
	UT_ASSERT_EQ(observed_total(), 0);
}

UT_TEST(test_exact_resolver_binds_exact_origin_provider_and_publishes_only_after_recheck)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_ROW_WAIT, &resolution,
										  &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_legacy_provider_calls, 1);
	UT_ASSERT_EQ(test_admitted_provider_calls, 0);
	UT_ASSERT_EQ(test_recheck_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT_EQ(test_provider_locator.uba.raw[0], locator.uba.raw[0]);
	UT_ASSERT_EQ(test_provider_locator.uba.raw[1], locator.uba.raw[1]);
	UT_ASSERT_EQ(test_provider_locator.xid, locator.xid);
	UT_ASSERT_EQ(test_provider_locator.tt_wrap, locator.tt_wrap);
	UT_ASSERT_EQ(test_provider_locator.itl_kind, locator.itl_kind);
	UT_ASSERT_EQ(test_provider_locator.itl_slot_index, locator.itl_slot_index);
	UT_ASSERT_EQ(test_provider_mode, CLUSTER_TX_RESOLVE_ROW_WAIT);
	UT_ASSERT_EQ(test_provider_epoch, test_formation_epoch);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(resolution.proof_kind, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG);
	UT_ASSERT_EQ(resolution.commit_scn, (SCN)101);
	UT_ASSERT_EQ(test_observed[CLUSTER_R4_EVENT_TX_COMMITTED], 1);
	UT_ASSERT_EQ(observed_total(), 1);
}

UT_TEST(test_exact_resolver_observes_each_positive_outcome_once)
{
	static const struct {
		ClusterTxOutcome outcome;
		ClusterTxProofKind proof;
		ClusterR4Event event;
		SCN commit_scn;
	} cases[] = {
		{CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG,
		 CLUSTER_R4_EVENT_TX_IN_PROGRESS, InvalidScn},
		{CLUSTER_TX_PREPARED, CLUSTER_TX_PROOF_ORIGIN_TWOPHASE,
		 CLUSTER_R4_EVENT_TX_PREPARED, InvalidScn},
		{CLUSTER_TX_COMMITTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG,
		 CLUSTER_R4_EVENT_TX_COMMITTED, (SCN)101},
		{CLUSTER_TX_ABORTED, CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG,
		 CLUSTER_R4_EVENT_TX_ABORTED, InvalidScn}
	};
	ClusterTxLocator locator = exact_locator();
	int i;

	for (i = 0; i < lengthof(cases); i++) {
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

		reset_exact_resolver_fixture();
		test_provider_outcome = cases[i].outcome;
		test_provider_resolution.outcome = cases[i].outcome;
		test_provider_resolution.proof_kind = cases[i].proof;
		test_provider_resolution.commit_scn = cases[i].commit_scn;
		UT_ASSERT_EQ(cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_VISIBILITY,
										  &resolution, &reason),
					 cases[i].outcome);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(test_observed[cases[i].event], 1);
		UT_ASSERT_EQ(observed_total(), 1);
	}
}

UT_TEST(test_exact_resolver_rejects_mismatched_locator_echo)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_exact_resolver_fixture();
	test_provider_resolution.locator_echo.tt_wrap++;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution,
										  &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_recheck_calls, 0);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
	UT_ASSERT_EQ(test_observed[CLUSTER_R4_EVENT_TX_UNKNOWN], 1);
	UT_ASSERT_EQ(observed_total(), 1);
}

UT_TEST(test_exact_resolver_discards_provider_result_when_activation_generation_moves)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_exact_resolver_fixture();
	test_recheck_result = false;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_CR_BUILD, &resolution,
										  &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_recheck_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
	UT_ASSERT_EQ(test_observed[CLUSTER_R4_EVENT_TX_UNKNOWN], 1);
	UT_ASSERT_EQ(observed_total(), 1);
}

UT_TEST(test_exact_cleanout_consumer_rejects_live_provider_outcome)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_exact_resolver_fixture();
	test_provider_outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	test_provider_resolution.commit_scn = InvalidScn;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_CLEANOUT_HINT, &resolution,
										  &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_recheck_calls, 0);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
	UT_ASSERT_EQ(test_observed[CLUSTER_R4_EVENT_TX_UNKNOWN], 1);
	UT_ASSERT_EQ(observed_total(), 1);
}

UT_TEST(test_exact_terminal_census_uses_only_scoped_inactive_target_admission)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
				 &locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				 &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_enter_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_enter_calls, 1);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_legacy_provider_calls, 0);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);
	UT_ASSERT_EQ(test_provider_mode, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS);
	UT_ASSERT_EQ(test_recheck_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
}

UT_TEST(test_exact_terminal_census_rejects_prepared_provider_outcome)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	reset_exact_resolver_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	test_provider_outcome = CLUSTER_TX_PREPARED;
	test_provider_resolution.outcome = CLUSTER_TX_PREPARED;
	test_provider_resolution.proof_kind = CLUSTER_TX_PROOF_ORIGIN_TWOPHASE;
	test_provider_resolution.commit_scn = InvalidScn;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
				 &locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				 &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT_EQ(test_enter_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_enter_calls, 1);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_recheck_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 0);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_exact_terminal_census_admitted_uses_caller_token_without_reentry)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_resolver_fixture();
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = UINT64_C(73);
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
				 &locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				 &admission, &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_enter_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_enter_calls, 0);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 0);
	UT_ASSERT(admission.entered);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
}

UT_TEST(test_partial_terminal_census_accepts_one_canonical_provider_echo)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	reset_exact_resolver_fixture();
	locator.tt_wrap = TT_WRAP_INVALID;
	memset(&admission, 0, sizeof(admission));
	admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	admission.record_generation = UINT64_C(73);
	admission.formation_epoch = test_formation_epoch;
	admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	admission.entered = true;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
				 &locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
				 &admission, &resolution, &reason),
				 CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_provider_locator.tt_wrap, TT_WRAP_INVALID);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, 7);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 1);
}

UT_TEST(test_partial_visibility_uses_exact_admitted_provider_and_canonical_echo)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	locator.tt_wrap = TT_WRAP_INVALID;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_terminal_census_enter_calls, 0);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);
	UT_ASSERT_EQ(test_legacy_provider_calls, 0);
	UT_ASSERT_EQ(test_provider_locator.tt_wrap, TT_WRAP_INVALID);
	UT_ASSERT_EQ(test_provider_mode, CLUSTER_TX_RESOLVE_VISIBILITY);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, 7);
	UT_ASSERT_EQ(test_recheck_calls, 1);
	UT_ASSERT_EQ(test_leave_calls, 1);
}

UT_TEST(test_partial_visibility_publishes_exact_in_progress_but_terminal_census_does_not)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	locator.tt_wrap = TT_WRAP_INVALID;
	test_provider_outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.proof_kind
		= CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	test_provider_resolution.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_IN_PROGRESS);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);

	reset_exact_resolver_fixture();
	locator.tt_wrap = TT_WRAP_INVALID;
	test_provider_outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.outcome = CLUSTER_TX_IN_PROGRESS;
	test_provider_resolution.proof_kind
		= CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	test_provider_resolution.commit_scn = InvalidScn;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_PROTOCOL);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_epoch_zero_terminal_census_local_single_node_publishes_exact_terminal)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
		&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, &resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);
	UT_ASSERT_EQ(test_legacy_provider_calls, 0);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 2);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, 0);
	UT_ASSERT_EQ(resolution.outcome, CLUSTER_TX_COMMITTED);
}

UT_TEST(test_epoch_zero_terminal_census_homogeneous_four_node_foreign_publishes_exact_terminal)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	test_node_count = 4;
	/* The homogeneous clean-formation override binds the exact current
	 * admission generation; it is not limited to the pre-PGSA zero sentinel. */
	admission.record_generation = UINT64_C(73);
	locator.uba.raw[0] = (locator.uba.raw[0] & UINT64_C(0xffffffff00000000))
						 | UINT64_C(257);
	test_provider_resolution.locator_echo = locator;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
		&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, &resolution, &reason), CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 2);
	UT_ASSERT_EQ(test_provider_epoch, 0);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, 0);
}

/* Freshref §3.1: only the partial-wrap VISIBILITY/status-22 fallback may use
 * the exact current four-node epoch-zero admission.  It retains regular R4
 * admission/recheck semantics and does not borrow terminal-census authority. */
UT_TEST(test_epoch_zero_partial_visibility_homogeneous_four_node_uses_exact_status22_path)
{
	ClusterTxLocator locator = prepare_epoch_zero_partial_visibility_fixture();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_COMMITTED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_terminal_census_enter_calls, 0);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_admitted_provider_calls, 1);
	UT_ASSERT_EQ(test_legacy_provider_calls, 0);
	UT_ASSERT_EQ(test_recheck_calls, 2);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 0);
	UT_ASSERT_EQ(test_provider_mode, CLUSTER_TX_RESOLVE_VISIBILITY);
	UT_ASSERT_EQ(test_provider_epoch, 0);
	UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, 7);
	UT_ASSERT_EQ(resolution.authority.origin_epoch, 0);
}

UT_TEST(test_epoch_zero_complete_visibility_remains_fail_closed)
{
	ClusterTxLocator locator = prepare_epoch_zero_partial_visibility_fixture();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	locator.tt_wrap = 7;
	test_provider_resolution.locator_echo = locator;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_provider_calls, 0);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_epoch_zero_canonical_row_wait_reuses_partial_provider_without_rebinding)
{
	int leg;

	for (leg = 0; leg < 3; leg++) {
		ClusterTxLocator locator = prepare_epoch_zero_partial_visibility_fixture();
		ClusterTxLocator before;
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		ClusterTxOutcome expected = leg == 0   ? CLUSTER_TX_IN_PROGRESS
									: leg == 1 ? CLUSTER_TX_COMMITTED
											   : CLUSTER_TX_ABORTED;

		locator.tt_wrap = 7;
		before = locator;
		test_provider_outcome = expected;
		test_provider_resolution.outcome = expected;
		test_provider_resolution.commit_scn = leg == 1 ? (SCN)101 : InvalidScn;
		UT_ASSERT_EQ(
			cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_ROW_WAIT, &resolution, &reason),
			expected);
		UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT_EQ(test_admitted_provider_calls, 1);
		UT_ASSERT_EQ(test_legacy_provider_calls, 0);
		UT_ASSERT_EQ(test_provider_mode, CLUSTER_TX_RESOLVE_VISIBILITY);
		UT_ASSERT_EQ(test_provider_locator.tt_wrap, TT_WRAP_INVALID);
		UT_ASSERT_EQ(resolution.locator_echo.tt_wrap, 7);
		UT_ASSERT_EQ(memcmp(&locator, &before, sizeof(locator)), 0);
		UT_ASSERT_EQ(test_enter_calls, 1);
		UT_ASSERT_EQ(test_leave_calls, 1);
		UT_ASSERT_EQ(test_terminal_census_enter_calls, 0);
	}
}

UT_TEST(test_epoch_zero_row_wait_rejects_identity_and_admission_drift)
{
	int leg;

	for (leg = 0; leg < 11; leg++) {
		ClusterTxLocator locator = prepare_epoch_zero_partial_visibility_fixture();
		ClusterTxResolution resolution;
		ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

		locator.tt_wrap = 7;
		if (leg == 0)
			test_provider_resolution.locator_echo.tt_wrap++;
		if (leg == 1)
			test_provider_resolution.locator_echo.xid++;
		if (leg == 2)
			test_provider_resolution.locator_echo.uba.raw[0]++;
		if (leg == 3)
			test_provider_resolution.locator_echo.itl_kind = ITL_FLAG_LOCK_ONLY_ACTIVE;
		if (leg == 4)
			test_provider_resolution.locator_echo.itl_slot_index++;
		if (leg == 5)
			locator.tt_wrap = TT_WRAP_INVALID;
		if (leg == 6)
			test_node_count = 2;
		if (leg == 7)
			cluster_recmerge_window_active = true;
		if (leg == 8)
			test_recheck_result = false;
		if (leg == 9)
			test_provider_mutates_epoch = true;
		if (leg == 10)
			test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
		memset(&resolution, 0xA5, sizeof(resolution));
		UT_ASSERT_EQ(
			cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_ROW_WAIT, &resolution, &reason),
			CLUSTER_TX_UNKNOWN);
		UT_ASSERT(reason != CLUSTER_TX_RESOLVE_NONE);
		UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
		UT_ASSERT_EQ(test_admitted_provider_calls, leg < 5 || leg == 9 ? 1 : 0);
		UT_ASSERT_EQ(test_legacy_provider_calls, 0);
		UT_ASSERT_EQ(test_leave_calls, leg == 10 ? 0 : 1);
	}
}

UT_TEST(test_epoch_zero_partial_visibility_generation_drift_refuses_before_provider)
{
	ClusterTxLocator locator = prepare_epoch_zero_partial_visibility_fixture();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	test_recheck_result = false;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution, &reason),
		CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_provider_calls, 0);
	UT_ASSERT_EQ(test_recheck_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_epoch_zero_terminal_census_peer_topology_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	test_node_count = 2;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_unknown_topology_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	test_node_count = 0;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_foreign_locator_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	locator.uba.raw[0] = (locator.uba.raw[0] & UINT64_C(0xffffffff00000000))
						 | UINT64_C(257);
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_nonterminal_mode_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_VISIBILITY,
		&admission, CLUSTER_TX_RESOLVE_PROTOCOL);
}

UT_TEST(test_epoch_zero_terminal_census_recovery_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	cluster_recmerge_window_active = true;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_single_node_nonzero_generation_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	admission.record_generation = 1;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_current_generation_drift_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	test_node_count = 4;
	test_terminal_census_recheck_result = false;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_storage_disabled_refuses_before_provider)
{
	ClusterTxLocator locator = exact_locator();
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	cluster_enabled = false;
	assert_epoch_zero_refused(&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, CLUSTER_TX_RESOLVE_RF_DEFERRED);
}

UT_TEST(test_epoch_zero_terminal_census_zero_to_nonzero_drift_discards_provider_result)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;
	ClusterSemanticAdmissionToken admission;

	prepare_epoch_zero_terminal_fixture(&admission);
	test_provider_mutates_epoch = true;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_exact_admitted(
		&locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		&admission, &resolution, &reason), CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_terminal_census_recheck_calls, 1);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_exact_resolver_error_releases_target_admission_once)
{
	ClusterTxLocator locator = exact_locator();
	ClusterTxResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;
	volatile bool caught = false;

	reset_exact_resolver_fixture();
	test_provider_raise = true;
	PG_TRY();
	{
		(void)cluster_tx_resolve_exact(&locator, CLUSTER_TX_RESOLVE_VISIBILITY, &resolution,
									   &reason);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_enter_calls, 1);
	UT_ASSERT_EQ(test_provider_calls, 1);
	UT_ASSERT_EQ(test_recheck_calls, 0);
	UT_ASSERT_EQ(test_leave_calls, 1);
	UT_ASSERT_EQ(observed_total(), 0);
}

UT_TEST(test_multixact_resolver_is_dormant_and_zeroes_output)
{
	ClusterMultiResolution resolution;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_exact_resolver_fixture();
	test_admission_result = CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	memset(&resolution, 0xA5, sizeof(resolution));
	UT_ASSERT_EQ(cluster_tx_resolve_multixact((MultiXactId)0, &resolution, &reason),
				 CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT(bytes_are_zero(&resolution, sizeof(resolution)));
}

UT_TEST(test_bad_locator_scaffold_fails_closed_and_zeroes_output)
{
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	memset(&locator, 0xA5, sizeof(locator));
	UT_ASSERT(!cluster_tx_locator_from_itl(NULL, 0, &locator, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
	UT_ASSERT(bytes_are_zero(&locator, sizeof(locator)));
}

UT_TEST(test_valid_caller_selected_data_slot_forms_exact_locator)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 3);
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_BAD_LOCATOR;

	slot->xid = (TransactionId)798;
	slot->wrap = 7;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->undo_segment_head.raw[0] = ((uint64)408 << 32) | 11;
	slot->undo_segment_head.raw[1] = 5;

	UT_ASSERT(cluster_tx_locator_from_itl(page, 3, &locator, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(locator.uba.raw[0], slot->undo_segment_head.raw[0]);
	UT_ASSERT_EQ(locator.uba.raw[1], slot->undo_segment_head.raw[1]);
	UT_ASSERT_EQ(locator.xid, slot->xid);
	UT_ASSERT_EQ(locator.tt_wrap, slot->wrap);
	UT_ASSERT_EQ(locator.itl_kind, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(locator.itl_slot_index, 3);
}

UT_TEST(test_lock_only_active_forms_exact_locator)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_BAD_LOCATOR;

	set_slot(page, 4, ITL_FLAG_LOCK_ONLY_ACTIVE, (TransactionId)900, 2, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 4, &locator, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(locator.uba.raw[0], slot_at(page, 4)->undo_segment_head.raw[0]);
	UT_ASSERT_EQ(locator.uba.raw[1], slot_at(page, 4)->undo_segment_head.raw[1]);
	UT_ASSERT_EQ(locator.xid, (TransactionId)900);
	UT_ASSERT_EQ(locator.tt_wrap, 2);
	UT_ASSERT_EQ(locator.itl_kind, ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT_EQ(locator.itl_slot_index, 4);
}

UT_TEST(test_data_terminal_states_form_locators)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_COMMITTED, (TransactionId)901, 2, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 0, &locator, &reason));
	set_slot(page, 1, ITL_FLAG_ABORTED, (TransactionId)902, 3, 10, 18, 7, 2);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 1, &locator, &reason));
	set_slot(page, 2, ITL_FLAG_NEEDS_CLEANOUT, (TransactionId)903, 4, 11, 19, 8, 3);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 2, &locator, &reason));
}

UT_TEST(test_lock_only_terminal_states_form_locators)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_LOCK_ONLY_COMMITTED, (TransactionId)904, 2, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 0, &locator, &reason));
	set_slot(page, 1, ITL_FLAG_LOCK_ONLY_ABORTED, (TransactionId)905, 3, 10, 18, 7, 2);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 1, &locator, &reason));
}

UT_TEST(test_wrap_zero_is_valid_fresh_generation)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)906, TT_WRAP_INITIAL, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 0, &locator, &reason));
	UT_ASSERT_EQ(locator.tt_wrap, TT_WRAP_INITIAL);
}

UT_TEST(test_terminal_census_locator_keeps_page_wrap_out_of_durable_request)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)919, 6, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl_terminal_census(
		page, 0, &locator, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(locator.tt_wrap, TT_WRAP_INVALID);
}

UT_TEST(test_null_output_fails_bad_locator)
{
	Page page = build_itl_page();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_NONE;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)907, 1, 9, 17, 6, 1);
	UT_ASSERT(!cluster_tx_locator_from_itl(page, 0, NULL, &reason));
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_page_without_itl_fails_bad_locator)
{
	Page page = build_itl_page();

	((PageHeader)page)->pd_flags = 0;
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_short_itl_special_area_fails_bad_locator)
{
	Page page = build_itl_page();

	((PageHeader)page)->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_ARRAY_SIZE + 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_slot_eight_fails_slot_mismatch)
{
	Page page = build_itl_page();

	assert_locator_failure(page, CLUSTER_ITL_INITRANS_DEFAULT, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_slot_255_fails_slot_mismatch)
{
	Page page = build_itl_page();

	assert_locator_failure(page, UINT8_MAX, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
}

UT_TEST(test_free_slot_fails_bad_locator)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_FREE, (TransactionId)908, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_multixact_marker_fails_bad_locator)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI, (TransactionId)908, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_unknown_slot_kind_fails_bad_locator)
{
	Page page = build_itl_page();

	set_slot(page, 0, UINT8_MAX, (TransactionId)908, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
}

UT_TEST(test_invalid_xid_fails_xid_mismatch)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, InvalidTransactionId, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_special_xids_fail_xid_mismatch)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, BootstrapTransactionId, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_XID_MISMATCH);
	set_slot(page, 0, ITL_FLAG_ACTIVE, FrozenTransactionId, 1, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_XID_MISMATCH);
}

UT_TEST(test_invalid_wrap_fails_wrap_mismatch)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)909, TT_WRAP_INVALID, 9, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);
}

UT_TEST(test_zero_uba_fails_bad_uba)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot;

	slot = set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)910, 1, 9, 17, 6, 1);
	slot->undo_segment_head = (UBA)InvalidUba_init;
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_zero_segment_fails_bad_uba)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)911, 1, 0, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_oversize_segment_fails_bad_uba)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)912, 1, (uint32)UINT16_MAX + 1, 17, 6, 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_out_of_range_tt_offset_fails_bad_uba)
{
	Page page = build_itl_page();

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)913, 1, 9, 17, (uint16)TT_SLOTS_PER_SEGMENT,
			 1);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_reserved_uba_bits_fail_bad_uba)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot;

	slot = set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)914, 1, 9, 17, 6, 1);
	slot->undo_segment_head.raw[1] |= ((uint64)1 << 32);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_caller_selected_slot_never_scans_raw_xid_alternate)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)797, 1, 7, 12, 3, 1);
	set_slot(page, 4, ITL_FLAG_ACTIVE, (TransactionId)915, 9, 12, 21, 8, 2);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 4, &locator, &reason));
	UT_ASSERT_EQ(locator.xid, 915);
	UT_ASSERT_EQ(locator.itl_slot_index, 4);
	UT_ASSERT_NE(locator.xid, 797);
}

UT_TEST(test_t408_current_xid798_is_not_rewritten_to_xmin797)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)797, 2, 7, 408, 3, 1);
	set_slot(page, 1, ITL_FLAG_ACTIVE, (TransactionId)798, 3, 8, 408, 4, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 1, &locator, &reason));
	UT_ASSERT_EQ(locator.xid, 798);
	UT_ASSERT_NE(locator.xid, 797);
	UT_ASSERT_EQ(locator.uba.raw[0], (((uint64)408 << 32) | 8));
}

UT_TEST(test_duplicate_xid_has_no_uba_winner_selection)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;
	ClusterTxResolveReason reason;

	set_slot(page, 2, ITL_FLAG_ACTIVE, (TransactionId)916, 4, 13, 22, 5, 1);
	set_slot(page, 6, ITL_FLAG_ACTIVE, (TransactionId)916, 5, 14, 23, 6, 2);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 6, &locator, &reason));
	UT_ASSERT_EQ(locator.itl_slot_index, 6);
	UT_ASSERT_EQ(locator.uba.raw[0], (((uint64)23 << 32) | 14));
	UT_ASSERT_NE(locator.uba.raw[0], (((uint64)22 << 32) | 13));
}

UT_TEST(test_reason_output_is_optional)
{
	Page page = build_itl_page();
	ClusterTxLocator locator;

	set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)917, 1, 9, 17, 6, 1);
	UT_ASSERT(cluster_tx_locator_from_itl(page, 0, &locator, NULL));
	UT_ASSERT(!cluster_tx_locator_from_itl(NULL, 0, &locator, NULL));
}

UT_TEST(test_failure_zeroes_previous_locator)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot;

	slot = set_slot(page, 0, ITL_FLAG_ACTIVE, (TransactionId)918, 1, 9, 17, 6, 1);
	slot->undo_segment_head.raw[1] |= ((uint64)1 << 32);
	assert_locator_failure(page, 0, CLUSTER_TX_RESOLVE_BAD_UBA);
}

UT_TEST(test_epoch_is_absent_from_locator_value)
{
	UT_ASSERT_EQ(sizeof(ClusterTxLocator), sizeof(ClusterUndoByteAddress) + sizeof(TransactionId)
											   + sizeof(uint16) + sizeof(uint8) + sizeof(uint8));
	UT_ASSERT_EQ(offsetof(ClusterTxLocator, itl_slot_index) + sizeof(uint8),
				 sizeof(ClusterTxLocator));
}

UT_TEST(test_terminal_census_batch_preflight_delegates_exit_hook_ensure)
{
	test_runtime_exit_hooks_calls = 0;
	cluster_tx_resolve_terminal_census_batch_preflight();
	UT_ASSERT_EQ(test_runtime_exit_hooks_calls, 1);
}

int
main(void)
{
	UT_PLAN(63);
	UT_RUN(test_epoch_zero_canonical_row_wait_reuses_partial_provider_without_rebinding);
	UT_RUN(test_epoch_zero_row_wait_rejects_identity_and_admission_drift);
	UT_RUN(test_frozen_identity_layout);
	UT_RUN(test_frozen_closed_domains);
	UT_RUN(test_reason_names_are_stable);
	UT_RUN(test_exact_resolver_is_dormant_and_zeroes_output);
	UT_RUN(test_exact_resolver_binds_exact_origin_provider_and_publishes_only_after_recheck);
	UT_RUN(test_exact_resolver_observes_each_positive_outcome_once);
	UT_RUN(test_exact_resolver_rejects_mismatched_locator_echo);
	UT_RUN(test_exact_resolver_discards_provider_result_when_activation_generation_moves);
	UT_RUN(test_exact_cleanout_consumer_rejects_live_provider_outcome);
	UT_RUN(test_exact_terminal_census_uses_only_scoped_inactive_target_admission);
	UT_RUN(test_exact_terminal_census_rejects_prepared_provider_outcome);
	UT_RUN(test_exact_terminal_census_admitted_uses_caller_token_without_reentry);
	UT_RUN(test_partial_terminal_census_accepts_one_canonical_provider_echo);
	UT_RUN(test_partial_visibility_uses_exact_admitted_provider_and_canonical_echo);
	UT_RUN(test_partial_visibility_publishes_exact_in_progress_but_terminal_census_does_not);
	UT_RUN(test_epoch_zero_terminal_census_local_single_node_publishes_exact_terminal);
	UT_RUN(test_epoch_zero_terminal_census_homogeneous_four_node_foreign_publishes_exact_terminal);
	UT_RUN(test_epoch_zero_partial_visibility_homogeneous_four_node_uses_exact_status22_path);
	UT_RUN(test_epoch_zero_complete_visibility_remains_fail_closed);
	UT_RUN(test_epoch_zero_partial_visibility_generation_drift_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_peer_topology_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_unknown_topology_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_foreign_locator_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_nonterminal_mode_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_recovery_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_single_node_nonzero_generation_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_current_generation_drift_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_storage_disabled_refuses_before_provider);
	UT_RUN(test_epoch_zero_terminal_census_zero_to_nonzero_drift_discards_provider_result);
	UT_RUN(test_exact_resolver_error_releases_target_admission_once);
	UT_RUN(test_multixact_resolver_is_dormant_and_zeroes_output);
	UT_RUN(test_bad_locator_scaffold_fails_closed_and_zeroes_output);
	UT_RUN(test_valid_caller_selected_data_slot_forms_exact_locator);
	UT_RUN(test_lock_only_active_forms_exact_locator);
	UT_RUN(test_data_terminal_states_form_locators);
	UT_RUN(test_lock_only_terminal_states_form_locators);
	UT_RUN(test_wrap_zero_is_valid_fresh_generation);
	UT_RUN(test_terminal_census_locator_keeps_page_wrap_out_of_durable_request);
	UT_RUN(test_null_output_fails_bad_locator);
	UT_RUN(test_page_without_itl_fails_bad_locator);
	UT_RUN(test_short_itl_special_area_fails_bad_locator);
	UT_RUN(test_slot_eight_fails_slot_mismatch);
	UT_RUN(test_slot_255_fails_slot_mismatch);
	UT_RUN(test_free_slot_fails_bad_locator);
	UT_RUN(test_multixact_marker_fails_bad_locator);
	UT_RUN(test_unknown_slot_kind_fails_bad_locator);
	UT_RUN(test_invalid_xid_fails_xid_mismatch);
	UT_RUN(test_special_xids_fail_xid_mismatch);
	UT_RUN(test_invalid_wrap_fails_wrap_mismatch);
	UT_RUN(test_zero_uba_fails_bad_uba);
	UT_RUN(test_zero_segment_fails_bad_uba);
	UT_RUN(test_oversize_segment_fails_bad_uba);
	UT_RUN(test_out_of_range_tt_offset_fails_bad_uba);
	UT_RUN(test_reserved_uba_bits_fail_bad_uba);
	UT_RUN(test_caller_selected_slot_never_scans_raw_xid_alternate);
	UT_RUN(test_t408_current_xid798_is_not_rewritten_to_xmin797);
	UT_RUN(test_duplicate_xid_has_no_uba_winner_selection);
	UT_RUN(test_reason_output_is_optional);
	UT_RUN(test_failure_zeroes_previous_locator);
	UT_RUN(test_epoch_is_absent_from_locator_value);
	UT_RUN(test_terminal_census_batch_preflight_delegates_exit_hook_ensure);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
