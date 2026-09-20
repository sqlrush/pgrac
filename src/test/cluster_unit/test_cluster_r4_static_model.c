/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_static_model.c
 *	Static/model RED for the dormant R4 synchronous-CR contract.
 *
 * This step-1 fixture intentionally compiles before any prospective R4
 * declaration exists.  It inventories the current product sources, emits a
 * canonical row for every frozen contract/matrix cell, and then compares the
 * observed model with the required model.  ABSENT is an observation, not a
 * build failure or a product stub.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/cryptohash.h"
#include "unit_test.h"

#ifndef CR_SERVER_HEADER_PATH
#error "CR_SERVER_HEADER_PATH must identify cluster_cr_server.h"
#endif
#ifndef CR_SERVER_SOURCE_PATH
#error "CR_SERVER_SOURCE_PATH must identify cluster_cr_server.c"
#endif
#ifndef CR_SOURCE_PATH
#error "CR_SOURCE_PATH must identify cluster_cr.c"
#endif
#ifndef GCS_HEADER_PATH
#error "GCS_HEADER_PATH must identify cluster_gcs_block.h"
#endif
#ifndef GCS_SOURCE_PATH
#error "GCS_SOURCE_PATH must identify cluster_gcs_block.c"
#endif
#ifndef PCM_HEADER_PATH
#error "PCM_HEADER_PATH must identify cluster_pcm_lock.h"
#endif
#ifndef PCM_SOURCE_PATH
#error "PCM_SOURCE_PATH must identify cluster_pcm_lock.c"
#endif
#ifndef BUFMGR_SOURCE_PATH
#error "BUFMGR_SOURCE_PATH must identify bufmgr.c"
#endif
#ifndef IC_HEADER_PATH
#error "IC_HEADER_PATH must identify cluster_ic.h"
#endif
#ifndef ITL_HEADER_PATH
#error "ITL_HEADER_PATH must identify cluster_itl.h"
#endif
#ifndef HEAP_VIS_SOURCE_PATH
#error "HEAP_VIS_SOURCE_PATH must identify heapam_visibility.c"
#endif
#ifndef HEAPAM_SOURCE_PATH
#error "HEAPAM_SOURCE_PATH must identify heapam.c"
#endif
#ifndef VIS_RESOLVE_SOURCE_PATH
#error "VIS_RESOLVE_SOURCE_PATH must identify cluster_visibility_resolve.c"
#endif
#ifndef TT_STATUS_HEADER_PATH
#error "TT_STATUS_HEADER_PATH must identify cluster_tt_status.h"
#endif
#ifndef TT_STATUS_SOURCE_PATH
#error "TT_STATUS_SOURCE_PATH must identify cluster_tt_status.c"
#endif
#ifndef TT_HINT_HEADER_PATH
#error "TT_HINT_HEADER_PATH must identify cluster_tt_status_hint.h"
#endif
#ifndef TT_HINT_SOURCE_PATH
#error "TT_HINT_SOURCE_PATH must identify cluster_tt_status_hint.c"
#endif
#ifndef MULTIXACT_HEADER_PATH
#error "MULTIXACT_HEADER_PATH must identify cluster_multixact.h"
#endif
#ifndef MULTIXACT_SOURCE_PATH
#error "MULTIXACT_SOURCE_PATH must identify cluster_multixact.c"
#endif
#ifndef TX_RESOLVE_HEADER_PATH
#error "TX_RESOLVE_HEADER_PATH must identify the prospective cluster_tx_resolve.h"
#endif
#ifndef TX_RESOLVE_SOURCE_PATH
#error "TX_RESOLVE_SOURCE_PATH must identify the prospective cluster_tx_resolve.c"
#endif
#ifndef SEMANTIC_HEADER_PATH
#error "SEMANTIC_HEADER_PATH must identify the prospective semantic-activation header"
#endif
#ifndef SEMANTIC_SOURCE_PATH
#error "SEMANTIC_SOURCE_PATH must identify the prospective semantic-activation source"
#endif
#ifndef QVOTEC_SOURCE_PATH
#error "QVOTEC_SOURCE_PATH must identify cluster_qvotec.c"
#endif

#define R4_CONTRACT_COUNT 21
#define R4_MATRIX_STATE_COUNT 10
#define R4_MATRIX_EVENT_COUNT 9
#define R4_MATRIX_COUNT (R4_MATRIX_STATE_COUNT * R4_MATRIX_EVENT_COUNT)
#define R4_MODEL_ROW_COUNT (R4_CONTRACT_COUNT + R4_MATRIX_COUNT)
#define R4_MODEL_ID_MAX 64
#define R4_JSON_LINE_MAX 1024
#define SHA256_DIGEST_LEN 32
#define SHA256_HEX_LEN 64

typedef struct ModelRow {
	const char *id;
	const char *kind;
	const char *required;
	const char *actual;
} ModelRow;

typedef struct SourceBundle {
	char *cr_server_header;
	char *cr_server_source;
	char *cr_source;
	char *gcs_header;
	char *gcs_source;
	char *pcm_header;
	char *pcm_source;
	char *bufmgr_source;
	char *ic_header;
	char *itl_header;
	char *heap_vis_source;
	char *heapam_source;
	char *vis_resolve_source;
	char *tt_status_header;
	char *tt_status_source;
	char *tt_hint_header;
	char *tt_hint_source;
	char *multixact_header;
	char *multixact_source;
	char *tx_resolve_header;
	char *tx_resolve_source;
	char *semantic_header;
	char *semantic_source;
	char *qvotec_source;
} SourceBundle;

static SourceBundle sources;
static ModelRow model_rows[R4_MODEL_ROW_COUNT];
static char model_ids[R4_MODEL_ROW_COUNT][R4_MODEL_ID_MAX];
static int model_row_count;
static bool model_init_ok = true;
static bool model_hash_ok;
static char model_hash[SHA256_HEX_LEN + 1];

static const char *const contract_required[R4_CONTRACT_COUNT]
	= { "CLOSED_90_NO_DEFAULT",
		"REAL_MASTER_THEN_CURRENT_HOLDER",
		"UNDO_ORIGIN_NOT_CONSTRUCTOR",
		"FULL_ONLY_PARTIAL_REJECTED",
		"LOCATOR_24B_NO_EPOCH_EPOCH0_CODEC",
		"ACTIVE_ZERO_OVERLAY_AND_V1_V4",
		"LOCK_GRAPH_SEPARATED",
		"CLOSED_OUTCOME_PROOF_TABLE",
		"WIRE_ABI_EXACT",
		"TYPED_REASONS_POLARITY_STABLE",
		"ONE_OWNER_QVOTEC_WRITER",
		"SAME_DURABLE_OPEN_GENERATION",
		"EXACT_FOUR_GATED_SOURCES",
		"NO_CATVERSION_GUC_WORKER_RECORD_FLIP",
		"RAW_PRIVATE_DESCRIPTOR_ONLY",
		"ACTIVE_INJECTION_REFUSED_PREMUTATION",
		"XMIN_MISMATCH_FULL_CR_ONLY",
		"SCRATCH_ONLY_POST_FULL",
		"FORWARD96_MASTER_TRANSITION_COUNT_ONLY",
		"ONE_PCM_ROUTE_SNAPSHOT",
		"NOFETCH_STABLE_COPY_LOCAL_GENERATION_PRIVATE" };

static const char *const matrix_states[R4_MATRIX_STATE_COUNT]
	= { "E", "R", "F", "B", "U", "P", "C", "T", "X", "K" };

static const char *const matrix_events[R4_MATRIX_EVENT_COUNT]
	= { "valid", "duplicate", "stale",	  "timeout",  "cancel",
		"death", "reconfig",  "capacity", "malformed" };

static const char *const matrix_actions[R4_MATRIX_STATE_COUNT][R4_MATRIX_EVENT_COUNT] = {
	{ "R/arm-before-send", "E/drop", "E/drop", "E/drop", "K/no-send", "E/drop", "E/drop", "T/retry",
	  "X/FC(protocol)" },
	{ "F/master selects+forwards", "R/replay same route", "T/retry", "T/close attempt",
	  "K/cancel route", "T/retry", "T/stale route", "T/retry", "X/FC(protocol)" },
	{ "B/holder stable copy", "F/drop or replay", "T/retry", "T/close attempt", "K/cancel slot",
	  "T/retry", "T/stale slot", "T/retry", "X/FC(protocol)" },
	{ "U/request foreign undo or P/publish result", "B/drop", "T/retry", "T/abort scratch",
	  "K/abort scratch", "T/retry", "T/abort scratch", "T/retry", "X/FC(data/protocol)" },
	{ "B/exact undo reply", "U/drop", "T/retry", "T/abort scratch", "K/abort scratch", "T/retry",
	  "T/abort scratch", "T/retry", "X/FC(data/protocol)" },
	{ "C/exact consumer CAS", "P/drop", "P/drop", "P/drop", "K/drop result", "P/drop", "P/drop",
	  "P/drop", "P/drop" },
	{ "C/drop", "C/drop", "C/drop", "C/drop", "C/drop", "C/drop", "C/drop", "C/drop", "C/drop" },
	{ "R/new request id after typed close", "T/drop", "T/drop", "X/overall deadline", "K/cleanup",
	  "T/wait topology", "T/wait admission", "T/backoff", "X/FC(protocol)" },
	{ "X/drop", "X/drop", "X/drop", "X/drop", "X/cleanup", "X/drop", "X/drop", "X/drop", "X/drop" },
	{ "K/drop", "K/drop", "K/drop", "K/drop", "K/drop", "K/drop", "K/drop", "K/drop", "K/drop" }
};

static char *
read_optional_source(const char *path)
{
	FILE *fp;
	char *contents;
	long length;
	size_t nread;

	fp = fopen(path, "rb");
	if (fp == NULL)
		return NULL;
	if (fseek(fp, 0, SEEK_END) != 0) {
		fclose(fp);
		return NULL;
	}
	length = ftell(fp);
	if (length <= 0 || fseek(fp, 0, SEEK_SET) != 0) {
		fclose(fp);
		return NULL;
	}
	contents = malloc((size_t)length + 1);
	if (contents == NULL) {
		fclose(fp);
		return NULL;
	}
	nread = fread(contents, 1, (size_t)length, fp);
	fclose(fp);
	if (nread != (size_t)length) {
		free(contents);
		return NULL;
	}
	contents[length] = '\0';
	return contents;
}

static void
load_sources(void)
{
	sources.cr_server_header = read_optional_source(CR_SERVER_HEADER_PATH);
	sources.cr_server_source = read_optional_source(CR_SERVER_SOURCE_PATH);
	sources.cr_source = read_optional_source(CR_SOURCE_PATH);
	sources.gcs_header = read_optional_source(GCS_HEADER_PATH);
	sources.gcs_source = read_optional_source(GCS_SOURCE_PATH);
	sources.pcm_header = read_optional_source(PCM_HEADER_PATH);
	sources.pcm_source = read_optional_source(PCM_SOURCE_PATH);
	sources.bufmgr_source = read_optional_source(BUFMGR_SOURCE_PATH);
	sources.ic_header = read_optional_source(IC_HEADER_PATH);
	sources.itl_header = read_optional_source(ITL_HEADER_PATH);
	sources.heap_vis_source = read_optional_source(HEAP_VIS_SOURCE_PATH);
	sources.heapam_source = read_optional_source(HEAPAM_SOURCE_PATH);
	sources.vis_resolve_source = read_optional_source(VIS_RESOLVE_SOURCE_PATH);
	sources.tt_status_header = read_optional_source(TT_STATUS_HEADER_PATH);
	sources.tt_status_source = read_optional_source(TT_STATUS_SOURCE_PATH);
	sources.tt_hint_header = read_optional_source(TT_HINT_HEADER_PATH);
	sources.tt_hint_source = read_optional_source(TT_HINT_SOURCE_PATH);
	sources.multixact_header = read_optional_source(MULTIXACT_HEADER_PATH);
	sources.multixact_source = read_optional_source(MULTIXACT_SOURCE_PATH);
	sources.tx_resolve_header = read_optional_source(TX_RESOLVE_HEADER_PATH);
	sources.tx_resolve_source = read_optional_source(TX_RESOLVE_SOURCE_PATH);
	sources.semantic_header = read_optional_source(SEMANTIC_HEADER_PATH);
	sources.semantic_source = read_optional_source(SEMANTIC_SOURCE_PATH);
	sources.qvotec_source = read_optional_source(QVOTEC_SOURCE_PATH);
}

static void
free_sources(void)
{
	free(sources.cr_server_header);
	free(sources.cr_server_source);
	free(sources.cr_source);
	free(sources.gcs_header);
	free(sources.gcs_source);
	free(sources.pcm_header);
	free(sources.pcm_source);
	free(sources.bufmgr_source);
	free(sources.ic_header);
	free(sources.itl_header);
	free(sources.heap_vis_source);
	free(sources.heapam_source);
	free(sources.vis_resolve_source);
	free(sources.tt_status_header);
	free(sources.tt_status_source);
	free(sources.tt_hint_header);
	free(sources.tt_hint_source);
	free(sources.multixact_header);
	free(sources.multixact_source);
	free(sources.tx_resolve_header);
	free(sources.tx_resolve_source);
	free(sources.semantic_header);
	free(sources.semantic_source);
	free(sources.qvotec_source);
}

static bool
source_has(const char *source, const char *needle)
{
	return source != NULL && strstr(source, needle) != NULL;
}

static bool
source_has_definition(const char *source, const char *symbol)
{
	char marker[128];
	int n;

	n = snprintf(marker, sizeof(marker), "\n%s(", symbol);
	return n > 0 && n < (int)sizeof(marker) && source_has(source, marker);
}

static bool
source_has_ordered(const char *source, const char *const *needles, size_t count)
{
	const char *cursor = source;

	if (cursor == NULL)
		return false;
	for (size_t i = 0; i < count; i++) {
		cursor = strstr(cursor, needles[i]);
		if (cursor == NULL)
			return false;
		cursor += strlen(needles[i]);
	}
	return true;
}

static bool
function_region_has_ordered(const char *source, const char *symbol, const char *next_symbol,
							const char *const *needles, size_t count)
{
	char start_marker[128];
	char end_marker[128];
	const char *start;
	const char *end;
	const char *cursor;
	int n;

	n = snprintf(start_marker, sizeof(start_marker), "\n%s(", symbol);
	if (n <= 0 || n >= (int)sizeof(start_marker) || source == NULL)
		return false;
	n = snprintf(end_marker, sizeof(end_marker), "\n%s(", next_symbol);
	if (n <= 0 || n >= (int)sizeof(end_marker))
		return false;
	start = strstr(source, start_marker);
	end = start != NULL ? strstr(start + strlen(start_marker), end_marker) : NULL;
	if (start == NULL || end == NULL)
		return false;

	cursor = start;
	for (size_t i = 0; i < count; i++) {
		cursor = strstr(cursor, needles[i]);
		if (cursor == NULL || cursor >= end)
			return false;
		cursor += strlen(needles[i]);
	}
	return true;
}

static bool
function_region_has_single_finally_leave(const char *source, const char *symbol,
										 const char *next_symbol)
{
	const char *const ordered[] = { "PG_FINALLY();", "cluster_semantic_activation_leave" };
	char start_marker[128];
	char end_marker[128];
	const char *start;
	const char *end;
	const char *cursor;
	int finally_count = 0;
	int leave_count = 0;
	int n;

	if (!function_region_has_ordered(source, symbol, next_symbol, ordered, lengthof(ordered)))
		return false;
	n = snprintf(start_marker, sizeof(start_marker), "\n%s(", symbol);
	if (n <= 0 || n >= (int)sizeof(start_marker))
		return false;
	n = snprintf(end_marker, sizeof(end_marker), "\n%s(", next_symbol);
	if (n <= 0 || n >= (int)sizeof(end_marker))
		return false;
	start = strstr(source, start_marker);
	end = strstr(start + strlen(start_marker), end_marker);
	for (cursor = start; (cursor = strstr(cursor, "PG_FINALLY();")) != NULL && cursor < end;
		 cursor += strlen("PG_FINALLY();"))
		finally_count++;
	for (cursor = start;
		 (cursor = strstr(cursor, "cluster_semantic_activation_leave")) != NULL && cursor < end;
		 cursor += strlen("cluster_semantic_activation_leave"))
		leave_count++;
	return finally_count == 1 && leave_count == 1;
}

static bool
r4_sources_have(const char *needle)
{
	const char *const candidates[]
		= { sources.cr_server_header,	sources.cr_server_source,  sources.cr_source,
			sources.gcs_header,			sources.gcs_source,		   sources.pcm_header,
			sources.pcm_source,			sources.bufmgr_source,	   sources.ic_header,
			sources.itl_header,			sources.heap_vis_source,   sources.heapam_source,
			sources.vis_resolve_source, sources.tx_resolve_header, sources.tx_resolve_source,
			sources.semantic_header,	sources.semantic_source };

	for (size_t i = 0; i < lengthof(candidates); i++) {
		if (source_has(candidates[i], needle))
			return true;
	}
	return false;
}

static bool
d10_requester_source_edge_present(void)
{
	const char *const ordered[]
		= { "NodeId head_origin = uba_origin_node_id(chains[0].undo_segment_head);",
			"cluster_cr_coordinator_classify_origin(head_origin)",
			"cluster_r4_source_cr_dispatch(CLUSTER_R4_SOURCE_CR_FETCH",
			"/* PARTIAL: continue on the shipped page" };

	return source_has_ordered(sources.cr_source, ordered, lengthof(ordered))
		   && source_has_definition(sources.gcs_source, "cluster_gcs_block_cr_fetch_and_wait_raw")
		   && source_has_definition(sources.gcs_source, "cluster_r4_source_cr_dispatch")
		   && source_has(sources.gcs_source, "int32 origin_node");
}

static bool
d10_tt_source_edge_present(void)
{
	return source_has_definition(sources.tt_status_source, "cluster_tt_status_lookup_exact")
		   && source_has_definition(sources.tt_status_source, "cluster_tt_status_source_dispatch")
		   && source_has(sources.vis_resolve_source,
						 "cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP");
}

static bool
d10_hint_source_edge_present(void)
{
	return source_has_definition(sources.tt_hint_source, "cluster_tt_status_hint_emit_raw")
		   && source_has_definition(sources.tt_hint_source,
									"cluster_tt_status_hint_drain_outbound_raw")
		   && source_has_definition(sources.tt_hint_source,
									"cluster_tt_status_hint_source_dispatch");
}

static bool
d10_multi_source_edge_present(void)
{
	return source_has_definition(sources.multixact_source,
								 "cluster_multixact_member_overlay_install_raw")
		   && source_has_definition(sources.multixact_source,
									"cluster_multixact_member_overlay_lookup_raw")
		   && source_has(sources.multixact_source, "cluster_multixact_source_dispatch_body(")
		   && source_has_definition(sources.multixact_source, "cluster_multixact_source_dispatch");
}

static bool
all_four_d10_source_edges_present(void)
{
	return d10_requester_source_edge_present() && d10_tt_source_edge_present()
		   && d10_hint_source_edge_present() && d10_multi_source_edge_present();
}

static bool
dispatch_orders_gate_before_body(const char *source, const char *dispatch, const char *body)
{
	char marker[128];
	const char *start;
	const char *const ordered[] = { "cluster_semantic_activation_enter", body };
	int n;

	n = snprintf(marker, sizeof(marker), "\n%s(", dispatch);
	if (n <= 0 || n >= (int)sizeof(marker) || source == NULL)
		return false;
	start = strstr(source, marker);
	return start != NULL && source_has_ordered(start, ordered, lengthof(ordered));
}

static bool
all_four_d10_dispatches_gate_before_body(void)
{
	return dispatch_orders_gate_before_body(sources.gcs_source, "cluster_r4_source_cr_dispatch",
											"cluster_gcs_block_cr_fetch_and_wait_raw")
		   && dispatch_orders_gate_before_body(sources.tt_status_source,
											   "cluster_tt_status_source_dispatch",
											   "cluster_tt_status_lookup_exact")
		   && dispatch_orders_gate_before_body(sources.tt_hint_source,
											   "cluster_tt_status_hint_source_dispatch",
											   "cluster_tt_status_hint_emit_raw")
		   && dispatch_orders_gate_before_body(sources.multixact_source,
											   "cluster_multixact_source_dispatch",
											   "cluster_multixact_source_dispatch_body");
}

static bool
d10_admitted_wrappers_have_single_finally_leave(void)
{
	const char *const trace_order[]
		= { "cluster_xp_begin(&trace, CLXP_R_TT_VISIBILITY_RESOLVE)",
			"PG_TRY();",
			"result = cluster_tx_resolve_exact_trace_impl(locator, mode, out, reason_out)",
			"PG_FINALLY();",
			"cluster_xp_end(&trace)",
			"PG_END_TRY();",
			"return result;" };

	/*
	 * The region for cluster_gcs_block_r4_route_cr ends at the immediately
	 * following function gcs_block_try_r4_request80: the two try_* test
	 * hooks (request80/forward96) were inserted between route_cr and
	 * cluster_gcs_block_redo_lsn_covered, and each of them legitimately
	 * owns its own PG_FINALLY/leave pair.
	 */
	return function_region_has_single_finally_leave(sources.gcs_source,
													"cluster_r4_source_cr_dispatch",
													"cluster_gcs_block_undo_tt_fetch_and_wait")
		   && function_region_has_single_finally_leave(
			   sources.gcs_source, "cluster_gcs_block_r4_route_cr", "gcs_block_try_r4_request80")
		   && function_region_has_single_finally_leave(sources.cr_server_source,
													   "cluster_cr_build_on_holder",
													   "cluster_cr_server_shmem_size")
		   /* The observer wrapper owns its timer, not semantic admission. */
		   && function_region_has_ordered(sources.tx_resolve_source, "cluster_tx_resolve_exact",
										  "cluster_tx_resolve_exact_trace_impl", trace_order,
										  lengthof(trace_order))
		   && function_region_has_single_finally_leave(sources.tx_resolve_source,
													   "cluster_tx_resolve_exact_trace_impl",
													   "cluster_tx_resolve_multixact");
}

static bool
d10_epoch_sampling_order_is_exact(void)
{
	/*
	 * The admitted path was refactored into
	 * semantic_activation_enter_internal (shared by the plain and the
	 * r4-terminal-census variants); the public wrapper now only delegates,
	 * so the barrier -> after-snapshot -> after-epoch sequence lives in the
	 * internal function.
	 */
	const char *const enter_order[]
		= { "pg_write_barrier();", "semantic_activation_snapshot(&after)",
			"epoch_after = cluster_epoch_get_current()" };
	const char *const recheck_order[] = { "semantic_activation_snapshot(&snapshot)",
										  "current_epoch = cluster_epoch_get_current()" };

	return function_region_has_ordered(
			   sources.semantic_source, "semantic_activation_enter_internal",
			   "cluster_semantic_activation_recheck", enter_order, lengthof(enter_order))
		   && function_region_has_ordered(
			   sources.semantic_source, "cluster_semantic_activation_recheck",
			   "cluster_semantic_activation_leave", recheck_order, lengthof(recheck_order));
}

static bool
legacy_d6_xmin_route_present(void)
{
	const char *const ordered[] = {
		"TransactionId raw_xmin = HeapTupleHeaderGetRawXmin(tuple);",
		"cluster_visibility_resolve_tuple(buffer, tuple, raw_xmin, CLUSTER_VIS_XMIN, &r);",
		"cluster_vis_evidence_route(r.evidence, TransactionIdIsCurrentTransactionId(raw_xmin))"
	};

	return source_has_ordered(sources.heap_vis_source, ordered, lengthof(ordered));
}

static bool
legacy_d6_live_page_semantics_present(void)
{
	const char *const ordered[]
		= { "cluster_visibility_resolve_tuple_scn(", "page = BufferGetPage(buffer);",
			"cluster_itl_get_tt_ref(page, htup->t_itl_slot_idx, &ref)",
			"classify_page_ref(page, htup->t_itl_slot_idx, which, raw_xid, &ref, anchor_lsn,",
			"read_scn, out);" };

	return source_has_ordered(sources.vis_resolve_source, ordered, lengthof(ordered));
}

static bool
locator_shape_is_exact(void)
{
	const char *begin;
	const char *end;

	if (sources.tx_resolve_header == NULL)
		return false;
	begin = strstr(sources.tx_resolve_header, "typedef struct ClusterTxLocator");
	if (begin == NULL)
		return false;
	end = strstr(begin, "} ClusterTxLocator;");
	if (end == NULL)
		return false;
	if (strstr(begin, "cluster_epoch") != NULL && strstr(begin, "cluster_epoch") < end)
		return false;
	if (strstr(begin, "formation_epoch") != NULL && strstr(begin, "formation_epoch") < end)
		return false;
	return source_has(sources.tx_resolve_header, "sizeof(ClusterTxLocator) == 24")
		   && source_has(sources.tx_resolve_header, "cluster_tx_locator_from_itl")
		   && source_has(sources.tx_resolve_header, "itl_slot_index");
}

static bool
matrix_manifest_present(void)
{
	return r4_sources_have("cluster_r4_transition_manifest")
		   && r4_sources_have("CLUSTER_R4_STATE_EMPTY")
		   && r4_sources_have("CLUSTER_R4_STATE_CANCELLED");
}

static const char *
matrix_actual(const char *required)
{
	if (!matrix_manifest_present())
		return "ABSENT";
	if (r4_sources_have(required))
		return required;
	return "UNMAPPED";
}

static bool
semantic_source_gate_present(void)
{
	return source_has(sources.semantic_header, "cluster_semantic_activation_enter")
		   && source_has(sources.semantic_header, "CLUSTER_SEMANTIC_SOURCE_SIDE")
		   && source_has(sources.semantic_source, "CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT");
}

static const char *
contract_actual(int contract_number)
{
	const char *const holder_order[]
		= { "cluster_gcs_block_r4_route_cr(", "master", "current_holder" };
	bool holder_route
		= source_has_definition(sources.gcs_source, "cluster_gcs_block_r4_route_cr")
		  && (source_has_definition(sources.cr_source, "cluster_cr_build_on_holder")
			  || source_has_definition(sources.cr_server_source, "cluster_cr_build_on_holder"))
		  && source_has_ordered(sources.gcs_source, holder_order, lengthof(holder_order));
	bool legacy_origin_route = d10_requester_source_edge_present();
	bool full_builder
		= (source_has_definition(sources.cr_source, "cluster_cr_build_on_holder")
		   || source_has_definition(sources.cr_server_source, "cluster_cr_build_on_holder"))
		  && r4_sources_have("CLUSTER_CR_BUILD_FULL")
		  && r4_sources_have("CLUSTER_CR_BUILD_RETRYABLE")
		  && r4_sources_have("CLUSTER_CR_BUILD_FAIL_CLOSED");
	bool legacy_partial = legacy_origin_route
						  && source_has(sources.cr_source, "CLUSTER_CR_SPLIT_PARTIAL")
						  && source_has(sources.gcs_source, "GCS_BLOCK_REPLY_CR_RESULT_PARTIAL");
	bool semantic_gate = semantic_source_gate_present();

	switch (contract_number) {
	case 1:
		return matrix_manifest_present() && !r4_sources_have("default: /* R4 transition */")
				   ? contract_required[0]
				   : "ABSENT";
	case 2:
		if (holder_route)
			return contract_required[1];
		return legacy_origin_route ? "LEGACY_UNDO_ORIGIN_DIRECT" : "ABSENT";
	case 3:
		if (holder_route && semantic_gate)
			return contract_required[2];
		return legacy_origin_route ? "NEWEST_UNDO_ORIGIN_CONSTRUCTOR" : "ABSENT";
	case 4:
		if (full_builder && semantic_gate)
			return contract_required[3];
		return legacy_partial ? "PARTIAL_REQUESTER_CONTINUATION" : "ABSENT";
	case 5:
		return locator_shape_is_exact() ? contract_required[4] : "ABSENT";
	case 6:
		if (semantic_gate && all_four_d10_source_edges_present()
			&& source_has(sources.semantic_source, "active_bits")
			&& source_has(sources.tt_hint_source, "CLUSTER_TT_STATUS_HINT_V1")
			&& source_has(sources.tt_hint_source, "CLUSTER_TT_STATUS_HINT_V4"))
			return contract_required[5];
		return all_four_d10_source_edges_present() ? "FOUR_SOURCE_EDGES_WITHOUT_ACTIVE_ZERO_PROOF"
												   : "ABSENT";
	case 7:
		return semantic_gate && holder_route && sources.tx_resolve_source != NULL
				   ? contract_required[6]
				   : "ABSENT";
	case 8:
		if (source_has(sources.tx_resolve_header, "CLUSTER_TX_UNKNOWN")
			&& source_has(sources.tx_resolve_header, "CLUSTER_TX_IN_PROGRESS")
			&& source_has(sources.tx_resolve_header, "CLUSTER_TX_PREPARED")
			&& source_has(sources.tx_resolve_header, "CLUSTER_TX_COMMITTED")
			&& source_has(sources.tx_resolve_header, "CLUSTER_TX_ABORTED")
			&& source_has(sources.tx_resolve_source, "proof_kind"))
			return contract_required[7];
		return "ABSENT";
	case 9:
		if (holder_route && source_has(sources.ic_header, "PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1")
			&& source_has(sources.gcs_header, "StaticAssertDecl"))
			return contract_required[8];
		return "ABSENT";
	case 10:
		/* TYPED_REASONS_POLARITY_STABLE: both reason domains are typed with
		 * a *_reason_name() mapper.  The CR-build domain lives with the
		 * GCS-block route machinery (spec-8.4 §2.2 places
		 * cluster_cr_build_reason_name next to the R4 route record), not in
		 * the tx-resolve header. */
		if (source_has(sources.tx_resolve_header, "ClusterTxResolveReason")
			&& source_has(sources.gcs_header, "ClusterCrBuildReason")
			&& source_has(sources.tx_resolve_source, "reason_name"))
			return contract_required[9];
		return "ABSENT";
	case 11:
		/* ONE_OWNER_QVOTEC_WRITER: the QVOTEC module owns the record-CAS
		 * disk write (cluster_semantic_activation_record_cas_write lives in
		 * cluster_qvotec.c); the semantic module owns the LMON tick and the
		 * QVOTEC-facing mailbox poll. */
		if (source_has(sources.semantic_source, "cluster_semantic_activation_lmon_tick")
			&& source_has(sources.semantic_source,
						  "cluster_semantic_activation_qvotec_poll_record_cas")
			&& source_has(sources.qvotec_source, "cluster_semantic_activation_record_cas_write")
			&& source_has(sources.semantic_source, "QVOTEC"))
			return contract_required[10];
		return "ABSENT";
	case 12:
		return semantic_gate && holder_route
					   && source_has(sources.semantic_header, "record_generation")
				   ? contract_required[11]
				   : "ABSENT";
	case 13:
		if (semantic_gate && all_four_d10_source_edges_present())
			return contract_required[12];
		return all_four_d10_source_edges_present() ? "FOUR_SOURCE_EDGES_WITHOUT_COMMON_GATE"
												   : "ABSENT";
	case 14:
		if (source_has(sources.semantic_source, "DefineCustomBoolVariable")
			|| source_has(sources.semantic_source, "StartChildProcess")
			|| source_has(sources.semantic_source, "CATALOG_VERSION_NO")
			|| source_has(sources.semantic_source, "feature_local_gate"))
			return "FORBIDDEN_SURFACE_PRESENT";
		return contract_required[13];
	case 15:
		if (semantic_gate
			&& !source_has(sources.cr_server_header,
						   "extern bool cluster_gcs_block_cr_fetch_and_wait")
			&& !source_has(sources.tt_status_header, "extern bool cluster_tt_status_lookup_exact")
			&& !source_has(sources.tt_hint_header, "extern void cluster_tt_status_hint_emit")
			&& !source_has(sources.multixact_header,
						   "extern bool cluster_multixact_member_overlay_lookup"))
			return contract_required[14];
		return "RAW_OLD_SOURCE_LINK_VISIBLE";
	case 16:
		return semantic_gate && all_four_d10_dispatches_gate_before_body() ? contract_required[15]
																		   : "ABSENT";
	case 17:
		/* XMIN_MISMATCH_FULL_CR_ONLY (spec-8.4 §2.6g / D6 row): an
		 * updated-row XMIN/current-last-writer mismatch enters the FULL-CR
		 * block-reconstruction route and reevaluates the native logical
		 * row/HOT chain only in the scratch image.  The route landed in
		 * heapam.c (heap_hot_r4_updated_xmin_needs_full ->
		 * cluster_gcs_block_cr_fetch_and_wait ->
		 * heap_hot_r4_search_scratch) with the scratch-only visibility
		 * evaluator HeapTupleSatisfiesMVCCScratch in heapam_visibility.c;
		 * the requester side never touches the live page after the fetch. */
		if (source_has(sources.heapam_source, "heap_hot_r4_updated_xmin_needs_full")
			&& source_has(sources.heapam_source, "cluster_gcs_block_cr_fetch_and_wait")
			&& source_has(sources.heapam_source, "heap_hot_r4_search_scratch")
			&& source_has(sources.heap_vis_source, "HeapTupleSatisfiesMVCCScratch")
			&& source_has(sources.heap_vis_source, "scratch_page"))
			return contract_required[16];
		return legacy_d6_xmin_route_present() ? "RAW_XID_LAST_WRITER_ROUTE" : "ABSENT";
	case 18:
		if (source_has(sources.heap_vis_source, "scratch")
			&& source_has(sources.heapam_source, "scratch") && full_builder)
			return contract_required[17];
		return legacy_d6_live_page_semantics_present() ? "LIVE_PAGE_FIELDS_NO_SCRATCH_REEVALUATION"
													   : "ABSENT";
	case 19:
		if (source_has(sources.gcs_header, "master_resource_transition_count")
			&& source_has(sources.gcs_source, "master_resource_transition_count")
			&& !r4_sources_have("selected_holder_generation"))
			return contract_required[18];
		return "ABSENT";
	case 20:
		if (source_has(sources.pcm_header, "cluster_pcm_lock_r4_route_snapshot")
			&& source_has_definition(sources.pcm_source, "cluster_pcm_lock_r4_route_snapshot")
			&& source_has(sources.gcs_source, "cluster_pcm_lock_r4_route_snapshot("))
			return contract_required[19];
		return "ABSENT";
	case 21:
		if (source_has(sources.gcs_header, "cluster_bufmgr_copy_block_for_r4_cr")
			&& source_has_definition(sources.bufmgr_source, "cluster_bufmgr_copy_block_for_r4_cr")
			&& (source_has(sources.gcs_source, "cluster_bufmgr_copy_block_for_r4_cr(")
				|| source_has(sources.cr_server_source, "cluster_bufmgr_copy_block_for_r4_cr(")))
			return contract_required[20];
		return "ABSENT";
	default:
		return "INVALID_CONTRACT";
	}
}

static void
add_model_row(const char *id, const char *kind, const char *required, const char *actual)
{
	int n;

	if (model_row_count >= R4_MODEL_ROW_COUNT) {
		model_init_ok = false;
		return;
	}
	n = snprintf(model_ids[model_row_count], sizeof(model_ids[model_row_count]), "%s", id);
	if (n < 0 || n >= (int)sizeof(model_ids[model_row_count])) {
		model_init_ok = false;
		return;
	}
	model_rows[model_row_count].id = model_ids[model_row_count];
	model_rows[model_row_count].kind = kind;
	model_rows[model_row_count].required = required;
	model_rows[model_row_count].actual = actual;
	model_row_count++;
}

static int
compare_model_rows(const void *left, const void *right)
{
	const ModelRow *a = left;
	const ModelRow *b = right;

	return strcmp(a->id, b->id);
}

static void
build_model(void)
{
	char id[R4_MODEL_ID_MAX];

	for (int i = 0; i < R4_CONTRACT_COUNT; i++) {
		snprintf(id, sizeof(id), "contract.%02d", i + 1);
		add_model_row(id, "contract", contract_required[i], contract_actual(i + 1));
	}
	for (int state = 0; state < R4_MATRIX_STATE_COUNT; state++) {
		for (int event = 0; event < R4_MATRIX_EVENT_COUNT; event++) {
			snprintf(id, sizeof(id), "matrix.%s.%s", matrix_states[state], matrix_events[event]);
			add_model_row(id, "matrix", matrix_actions[state][event],
						  matrix_actual(matrix_actions[state][event]));
		}
	}
	if (model_row_count != R4_MODEL_ROW_COUNT)
		model_init_ok = false;
	qsort(model_rows, model_row_count, sizeof(model_rows[0]), compare_model_rows);
}

static bool
json_escape(const char *input, char *output, size_t output_size)
{
	size_t used = 0;

	for (const unsigned char *p = (const unsigned char *)input; *p != '\0'; p++) {
		const char *escape = NULL;
		char unicode_escape[7];
		size_t escape_len;

		switch (*p) {
		case '"':
			escape = "\\\"";
			break;
		case '\\':
			escape = "\\\\";
			break;
		case '\b':
			escape = "\\b";
			break;
		case '\f':
			escape = "\\f";
			break;
		case '\n':
			escape = "\\n";
			break;
		case '\r':
			escape = "\\r";
			break;
		case '\t':
			escape = "\\t";
			break;
		default:
			if (*p < 0x20) {
				snprintf(unicode_escape, sizeof(unicode_escape), "\\u%04x", *p);
				escape = unicode_escape;
			}
			break;
		}
		if (escape == NULL) {
			if (used + 1 >= output_size)
				return false;
			output[used++] = (char)*p;
			continue;
		}
		escape_len = strlen(escape);
		if (used + escape_len >= output_size)
			return false;
		memcpy(output + used, escape, escape_len);
		used += escape_len;
	}
	if (used >= output_size)
		return false;
	output[used] = '\0';
	return true;
}

static bool
format_model_row(const ModelRow *row, char *line, size_t line_size)
{
	char actual[R4_JSON_LINE_MAX / 4];
	char id[R4_JSON_LINE_MAX / 4];
	char kind[R4_JSON_LINE_MAX / 4];
	char required[R4_JSON_LINE_MAX / 4];
	int n;

	if (!json_escape(row->actual, actual, sizeof(actual)) || !json_escape(row->id, id, sizeof(id))
		|| !json_escape(row->kind, kind, sizeof(kind))
		|| !json_escape(row->required, required, sizeof(required)))
		return false;
	n = snprintf(line, line_size,
				 "{\"actual\":\"%s\",\"id\":\"%s\",\"kind\":\"%s\",\"required\":\"%s\"}", actual,
				 id, kind, required);
	return n >= 0 && n < (int)line_size;
}

static bool
digest_model_rows(const ModelRow *input, int count, char output[SHA256_HEX_LEN + 1])
{
	ModelRow sorted[R4_MODEL_ROW_COUNT];
	pg_cryptohash_ctx *ctx;
	uint8 digest[SHA256_DIGEST_LEN];
	char line[R4_JSON_LINE_MAX];

	if (count != R4_MODEL_ROW_COUNT)
		return false;
	memcpy(sorted, input, sizeof(sorted));
	qsort(sorted, count, sizeof(sorted[0]), compare_model_rows);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL || pg_cryptohash_init(ctx) < 0) {
		if (ctx != NULL)
			pg_cryptohash_free(ctx);
		return false;
	}
	for (int i = 0; i < count; i++) {
		if (!format_model_row(&sorted[i], line, sizeof(line))
			|| pg_cryptohash_update(ctx, (const uint8 *)line, strlen(line)) < 0
			|| pg_cryptohash_update(ctx, (const uint8 *)"\n", 1) < 0) {
			pg_cryptohash_free(ctx);
			return false;
		}
	}
	if (pg_cryptohash_final(ctx, digest, sizeof(digest)) < 0) {
		pg_cryptohash_free(ctx);
		return false;
	}
	pg_cryptohash_free(ctx);
	for (int i = 0; i < (int)sizeof(digest); i++)
		snprintf(output + (i * 2), 3, "%02x", digest[i]);
	output[SHA256_HEX_LEN] = '\0';
	return true;
}

static bool
emit_model(void)
{
	char line[R4_JSON_LINE_MAX];

	for (int i = 0; i < model_row_count; i++) {
		if (!format_model_row(&model_rows[i], line, sizeof(line)))
			return false;
		printf("%s\n", line);
	}
	if (!model_hash_ok)
		return false;
	printf("model_sha256=%s\n", model_hash);
	return true;
}

static const ModelRow *
find_model_row(const char *id)
{
	for (int i = 0; i < model_row_count; i++) {
		if (strcmp(model_rows[i].id, id) == 0)
			return &model_rows[i];
	}
	return NULL;
}

static void
assert_contract_matches(int contract_number)
{
	char id[R4_MODEL_ID_MAX];
	const ModelRow *row;

	snprintf(id, sizeof(id), "contract.%02d", contract_number);
	row = find_model_row(id);
	UT_ASSERT_NOT_NULL(row);
	if (row != NULL)
		UT_ASSERT_STR_EQ(row->actual, row->required);
}

UT_TEST(test_model_shape_and_key_order_controls)
{
	int contracts = 0;
	int matrix = 0;

	UT_ASSERT(model_init_ok);
	UT_ASSERT_EQ(model_row_count, R4_MODEL_ROW_COUNT);
	for (int i = 0; i < model_row_count; i++) {
		if (strcmp(model_rows[i].kind, "contract") == 0)
			contracts++;
		else if (strcmp(model_rows[i].kind, "matrix") == 0)
			matrix++;
		else
			UT_ASSERT(false);
		if (i > 0)
			UT_ASSERT(strcmp(model_rows[i - 1].id, model_rows[i].id) < 0);
	}
	UT_ASSERT_EQ(contracts, R4_CONTRACT_COUNT);
	UT_ASSERT_EQ(matrix, R4_MATRIX_COUNT);
}

UT_TEST(test_matrix_closed_keyspace_control)
{
	char id[R4_MODEL_ID_MAX];

	for (int state = 0; state < R4_MATRIX_STATE_COUNT; state++) {
		for (int event = 0; event < R4_MATRIX_EVENT_COUNT; event++) {
			const ModelRow *row;

			snprintf(id, sizeof(id), "matrix.%s.%s", matrix_states[state], matrix_events[event]);
			row = find_model_row(id);
			UT_ASSERT_NOT_NULL(row);
			if (row != NULL) {
				UT_ASSERT_STR_EQ(row->required, matrix_actions[state][event]);
				UT_ASSERT(strstr(row->required, "default") == NULL);
			}
		}
	}
}

UT_TEST(test_canonical_hash_and_mutation_controls)
{
	ModelRow mutated[R4_MODEL_ROW_COUNT];
	char repeated[SHA256_HEX_LEN + 1];
	char changed[SHA256_HEX_LEN + 1];

	UT_ASSERT(model_hash_ok);
	UT_ASSERT_EQ(strlen(model_hash), SHA256_HEX_LEN);
	UT_ASSERT(digest_model_rows(model_rows, model_row_count, repeated));
	UT_ASSERT_STR_EQ(repeated, model_hash);

	memcpy(mutated, model_rows, sizeof(mutated));
	mutated[0].actual = "MUTATED_ACTUAL";
	UT_ASSERT(digest_model_rows(mutated, model_row_count, changed));
	UT_ASSERT(strcmp(changed, model_hash) != 0);
	memcpy(mutated, model_rows, sizeof(mutated));
	mutated[0].id = "contract.00-mutated";
	UT_ASSERT(digest_model_rows(mutated, model_row_count, changed));
	UT_ASSERT(strcmp(changed, model_hash) != 0);
	memcpy(mutated, model_rows, sizeof(mutated));
	mutated[0].kind = "mutated-kind";
	UT_ASSERT(digest_model_rows(mutated, model_row_count, changed));
	UT_ASSERT(strcmp(changed, model_hash) != 0);
	memcpy(mutated, model_rows, sizeof(mutated));
	mutated[0].required = "MUTATED_REQUIRED";
	UT_ASSERT(digest_model_rows(mutated, model_row_count, changed));
	UT_ASSERT(strcmp(changed, model_hash) != 0);
}

UT_TEST(test_real_source_observation_controls)
{
	UT_ASSERT_NOT_NULL(sources.cr_server_header);
	UT_ASSERT_NOT_NULL(sources.cr_server_source);
	UT_ASSERT_NOT_NULL(sources.cr_source);
	UT_ASSERT_NOT_NULL(sources.gcs_header);
	UT_ASSERT_NOT_NULL(sources.gcs_source);
	UT_ASSERT_NOT_NULL(sources.pcm_header);
	UT_ASSERT_NOT_NULL(sources.pcm_source);
	UT_ASSERT_NOT_NULL(sources.bufmgr_source);
	UT_ASSERT_NOT_NULL(sources.itl_header);
	UT_ASSERT_NOT_NULL(sources.heap_vis_source);
	UT_ASSERT_NOT_NULL(sources.heapam_source);
	UT_ASSERT_NOT_NULL(sources.vis_resolve_source);
	UT_ASSERT_NOT_NULL(sources.tt_status_source);
	UT_ASSERT_NOT_NULL(sources.tt_hint_source);
	UT_ASSERT_NOT_NULL(sources.multixact_source);
	UT_ASSERT(source_has(sources.cr_source, "NodeId head_origin"));
	UT_ASSERT(source_has(sources.cr_source, "CLUSTER_CR_SPLIT_PARTIAL"));
	UT_ASSERT(d10_requester_source_edge_present());
	UT_ASSERT(d10_tt_source_edge_present());
	UT_ASSERT(d10_hint_source_edge_present());
	UT_ASSERT(d10_multi_source_edge_present());
	UT_ASSERT(legacy_d6_xmin_route_present());
	UT_ASSERT(legacy_d6_live_page_semantics_present());
}

UT_TEST(test_holder_route_matches_required_model)
{
	assert_contract_matches(2);
	assert_contract_matches(3);
}

UT_TEST(test_full_only_matches_required_model)
{
	assert_contract_matches(4);
}

UT_TEST(test_locator_matches_required_model)
{
	assert_contract_matches(5);
}

UT_TEST(test_source_edges_match_required_model)
{
	assert_contract_matches(6);
	assert_contract_matches(13);
	assert_contract_matches(15);
	assert_contract_matches(16);
}

UT_TEST(test_d10_admitted_wrappers_have_single_finally_leave)
{
	UT_ASSERT(d10_admitted_wrappers_have_single_finally_leave());
}

UT_TEST(test_d10_epoch_sampling_order_is_exact)
{
	UT_ASSERT(d10_epoch_sampling_order_is_exact());
}

UT_TEST(test_d6_live_scratch_edges_match_required_model)
{
	assert_contract_matches(17);
	assert_contract_matches(18);
}

UT_TEST(test_matrix_actions_match_required_model)
{
	int mismatches = 0;

	for (int i = 0; i < model_row_count; i++) {
		if (strcmp(model_rows[i].kind, "matrix") == 0
			&& strcmp(model_rows[i].actual, model_rows[i].required) != 0)
			mismatches++;
	}
	/* The all-90-mismatch RED marker expired when the transition manifest
	 * landed; the matrix is now fully implemented and must stay exact. */
	UT_ASSERT_EQ(mismatches, 0);
}

UT_TEST(test_remaining_contracts_match_required_model)
{
	const int remaining[] = { 7, 8, 9, 10, 11, 12, 14, 19, 20, 21 };

	for (size_t i = 0; i < lengthof(remaining); i++)
		assert_contract_matches(remaining[i]);
}

UT_DEFINE_GLOBALS();

int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	bool emitted;

	load_sources();
	build_model();
	model_hash_ok = digest_model_rows(model_rows, model_row_count, model_hash);
	emitted = emit_model();

	UT_PLAN(13);
	UT_RUN(test_model_shape_and_key_order_controls);
	UT_RUN(test_matrix_closed_keyspace_control);
	UT_RUN(test_canonical_hash_and_mutation_controls);
	UT_RUN(test_real_source_observation_controls);
	UT_RUN(test_holder_route_matches_required_model);
	UT_RUN(test_full_only_matches_required_model);
	UT_RUN(test_locator_matches_required_model);
	UT_RUN(test_source_edges_match_required_model);
	UT_RUN(test_d10_admitted_wrappers_have_single_finally_leave);
	UT_RUN(test_d10_epoch_sampling_order_is_exact);
	UT_RUN(test_d6_live_scratch_edges_match_required_model);
	UT_RUN(test_matrix_actions_match_required_model);
	UT_RUN(test_remaining_contracts_match_required_model);
	UT_DONE();

	free_sources();
	return emitted && ut_failed_count == 0 ? 0 : 1;
}
