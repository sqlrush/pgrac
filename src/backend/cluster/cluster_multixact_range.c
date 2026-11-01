/* Bounded original-origin composition service and provisional full caller. */
#include "postgres.h"
#include "access/xlog.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_multixact_range.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_write_fence.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

typedef struct RangeOwned {
	struct RangeOwned *next;
	ResourceOwner owner;
	ClusterMxCompositionReceive *op;
	ClusterKoDirectoryIoV3 io;
	ClusterCurrentMxKey key;
	uint64 system_identifier;
	uint32 loaded_catalog, peer_generation;
} RangeOwned;

typedef struct RangeServiceCheck {
	ClusterMxRangeSource source;
	ClusterKoPeerAdmissionV3 admission;
	int32 peer;
} RangeServiceCheck;

static RangeOwned *range_owned;
static bool range_registered;

static void
range_owned_end(RangeOwned *owned)
{
	RangeOwned **link = &range_owned;

	while (*link != NULL && *link != owned)
		link = &(*link)->next;
	if (*link == NULL)
		return;
	*link = owned->next;
	cluster_mx_composition_finish(&owned->op);
	(void)cluster_ko_directory_io_end_v3(&owned->io);
	pfree(owned);
}

static void
range_resource_release(ResourceReleasePhase phase, bool commit, bool top, void *arg)
{
	RangeOwned *owned = range_owned;

	(void)commit; (void)top; (void)arg;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;
	while (owned != NULL) {
		RangeOwned *next = owned->next;
		if (owned->owner == CurrentResourceOwner)
			range_owned_end(owned);
		owned = next;
	}
}

static RangeOwned *
range_owned_begin(void)
{
	RangeOwned *owned;
	ResourceOwner owner = CurrentResourceOwner != NULL ? CurrentResourceOwner : AuxProcessResourceOwner;

	if (owner == NULL)
		return NULL;
	if (!range_registered) {
		RegisterResourceReleaseCallback(range_resource_release, NULL);
		range_registered = true;
	}
	owned = MemoryContextAllocZero(TopMemoryContext, sizeof(*owned));
	owned->owner = owner;
	owned->next = range_owned;
	range_owned = owned;
	return owned;
}

static bool
range_owned_current(const RangeOwned *owned)
{
	return owned->key.cluster_epoch == cluster_epoch_get_current()
		&& owned->system_identifier == GetSystemIdentifier()
		&& owned->loaded_catalog == GetControlFileCatalogVersion()
		&& cluster_sf_peer_foreign_mx_capability_matches_v1(owned->key.origin_node_id,
			CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
			owned->loaded_catalog, owned->peer_generation);
}

static bool
range_target_valid(const BufferTag *tag)
{
	return tag->spcOid != 0 && tag->relNumber != 0 && tag->forkNum == MAIN_FORKNUM
		&& tag->blockNum != InvalidBlockNumber;
}

static bool
range_snapshot_valid(const ClusterMxRangeSnapshot *snapshot, uint64 epoch)
{
	return snapshot->snapshot_type <= SNAPSHOT_NON_VACUUMABLE
		&& (snapshot->read_epoch == 0 || snapshot->read_epoch == epoch);
}

/* Invoked under the existing native Truncation/Gen locks. These predicates
 * are original lock-free observations; no KO/CONTROL lock is acquired. */
static bool
range_service_current(void *arg)
{
	RangeServiceCheck *check = arg;
	const ClusterMxRangeSource *source = &check->source;

	return source->system_identifier != 0 && source->boot_incarnation != 0
		&& source->system_identifier == GetSystemIdentifier()
		&& source->formation_epoch == cluster_epoch_get_current()
		&& source->boot_incarnation == cluster_qvotec_get_self_incarnation()
		&& cluster_ko_peer_admission_current_v3(&check->admission,
			(RelFileLocator){source->target.spcOid, source->target.dbOid, source->target.relNumber})
		&& (!cluster_write_fence_enforcing() || cluster_write_fence_allowed());
}

static bool
range_service_peer_current(const RangeServiceCheck *check)
{
	return cluster_sf_peer_foreign_mx_capability_matches_v1(check->peer,
		CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
		GetControlFileCatalogVersion(), check->source.capability_generation);
}

bool
cluster_mx_range_serve(const ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response, uint32 capacity, uint32 *bytes)
{
	ClusterMxRangeRoute route;
	BufferTag tag;
	RangeServiceCheck check = {0};
	ClusterMxCompositionNativeChunk chunk;
	ClusterMxCompositionReadStatus status;
	ClusterMxCompositionReply reply = {0};
	ClusterMxRangeWireContext context = {0};
	RangeOwned *owned;
	uint32 body_bytes = 0;
	bool encoded = false;
	RelFileLocator locator;

	if (bytes != NULL) *bytes = 0;
	if (response == NULL || bytes == NULL || capacity < sizeof(*response)) return false;
	memset(response, 0, sizeof(*response));
	if (request == NULL
		|| !cluster_mx_range_transport_route(request, sizeof(*request), PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &route, &tag)
		|| request->key.origin_node_id != cluster_node_id
		|| cluster_mxid_origin_slot(request->key.multixact_id) != cluster_node_id
		|| request->route.epoch != cluster_epoch_get_current()) return false;
	locator = (RelFileLocator){request->target.spcOid, request->target.dbOid, request->target.relNumber};
	check.peer = request->route.requester_node;
	check.source.system_identifier = GetSystemIdentifier();
	check.source.formation_epoch = cluster_epoch_get_current();
	check.source.boot_incarnation = cluster_qvotec_get_self_incarnation();
	check.source.snapshot = request->snapshot;
	check.source.target = request->target;
	if (!cluster_sf_peer_foreign_mx_capability_sample_v1(check.peer,
			CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
			GetControlFileCatalogVersion(), &check.source.capability_generation)) return false;
	if (!cluster_normal_stop_service_new_work(false)
		|| !cluster_ko_peer_admission_begin_v3(locator, &check.admission))
		return cluster_mx_range_refuse(request, GetControlFileCatalogVersion(), response, capacity, bytes);
	check.source.admission_revision = check.admission.revision;
	owned = range_owned_begin();
	if (owned == NULL)
		return cluster_mx_range_refuse(request, GetControlFileCatalogVersion(), response, capacity, bytes);
	PG_TRY();
	{
		if (cluster_ko_directory_io_begin_v3(locator, &owned->io)
			&& range_service_peer_current(&check) && range_service_current(&check)) {
			status = cluster_mx_composition_read_native_checked(request->key.multixact_id,
				request->has_source ? &request->source.native : NULL, request->first, request->cap,
				&chunk, range_service_current, &check);
			check.source.native = chunk.source;
			if (status == MXCOMP_READ_OK && request->has_source
				&& memcmp(&request->source, &check.source, sizeof(check.source)) != 0)
				status = MXCOMP_READ_UNKNOWN;
			context.request_id = request->route.request_id; context.epoch = request->route.epoch;
			context.key = request->key; context.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
			context.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
			context.loaded_catalog = GetControlFileCatalogVersion();
			context.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
			/* Echo for the requester's own exact record check, not holder authority. */
			context.peer_generation = request->peer_generation; context.requested_cap = request->cap;
			context.source = &check.source; context.source_bytes = sizeof(check.source);
			reply.source = &check.source; reply.source_bytes = sizeof(check.source);
			reply.total = chunk.source.span; reply.first = chunk.first; reply.count = chunk.count;
			reply.next = chunk.next; reply.cap = request->cap; reply.last = chunk.last;
			reply.skipped_zero = chunk.skipped_zero; reply.members = chunk.members;
			reply.member_bytes = chunk.count * sizeof(*chunk.members);
			encoded = cluster_mx_range_wire_encode_reply(&context, status,
				status == MXCOMP_READ_OK ? &reply : NULL, response->data.data, BLCKSZ, &body_bytes);
			if (!range_service_current(&check) || !range_service_peer_current(&check)) encoded = false;
		}
	}
	PG_FINALLY();
	{
		range_owned_end(owned);
	}
	PG_END_TRY();
	if (!encoded)
		return cluster_mx_range_refuse(request, GetControlFileCatalogVersion(), response, capacity, bytes);
	response->route = request->route;
	cluster_mx_range_response_seal(response, body_bytes);
	*bytes = response->route.wire_bytes;
	return true;
}

void
cluster_multixact_current_describe_finish(ClusterMxCompositionReceive **op)
{
	RangeOwned *owned;
	if (op == NULL) return;
	for (owned = range_owned; owned != NULL; owned = owned->next)
		if (owned->op == *op && *op != NULL) { range_owned_end(owned); break; }
	*op = NULL;
}

ClusterMxCompositionReceiveResult
cluster_multixact_current_proofs_accept(ClusterMxCompositionReceive **op,
										ClusterMxCompositionReadStatus status,
										const ClusterMxProofReply *reply)
{
	RangeOwned *owned;
	ClusterMxCompositionReceiveResult result = MXCOMP_UNKNOWN;
	if (op == NULL || *op == NULL)
		return MXCOMP_UNKNOWN;
	for (owned = range_owned; owned != NULL; owned = owned->next)
		if (owned->op == *op)
			break;
	if (owned == NULL) {
		*op = NULL;
		return MXCOMP_UNKNOWN;
	}
	PG_TRY();
	{
		/* Preserve the original acquisition through every provisional proof
		 * window, including allocation and the last append. These local
		 * checks do not replace member grants or the caller's tuple fence. */
		if (!range_owned_current(owned))
			result = cluster_mx_composition_accept_proofs(&owned->op, MXCOMP_READ_RETRY, NULL);
		else {
			result = cluster_mx_composition_accept_proofs(&owned->op, status, reply);
			if (result != MXCOMP_UNKNOWN && !range_owned_current(owned))
				result = cluster_mx_composition_accept_proofs(&owned->op, MXCOMP_READ_RETRY, NULL);
		}
	}
	PG_CATCH();
	{
		*op = NULL;
		range_owned_end(owned);
		PG_RE_THROW();
	}
	PG_END_TRY();
	*op = owned->op;
	if (result == MXCOMP_UNKNOWN)
		range_owned_end(owned);
	return result;
}

bool
cluster_multixact_current_describe_full(const ClusterCurrentMxKey *key,
	const BufferTag *target, const ClusterMxRangeSnapshot *snapshot,
	ClusterMxCompositionReceive **op, ClusterMxCompositionReceiveStats *stats)
{
	ClusterMxRangeRequest request = {0};
	ClusterMxRangeResponse response;
	ClusterMxRangeSource source = {0};
	RangeOwned *owned;
	bool complete = false;
	uint32 generation = 0, bytes = 0;
	uint64 unknown_before, restart_before;

	if (op == NULL || *op != NULL || stats == NULL || key == NULL || target == NULL || snapshot == NULL
		|| !range_target_valid(target) || !range_snapshot_valid(snapshot, key->cluster_epoch)
		|| key->cluster_epoch != cluster_epoch_get_current()
		|| key->origin_node_id >= CLUSTER_MAX_NODES || key->reserved16 != 0 || key->reserved32 != 0
		|| cluster_mxid_origin_slot(key->multixact_id) != key->origin_node_id
		|| !cluster_sf_peer_foreign_mx_capability_sample_v1(key->origin_node_id,
			CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
			GetControlFileCatalogVersion(), &generation)) return false;
	owned = range_owned_begin();
	if (owned == NULL) return false;
	unknown_before = stats->unknown_results;
	restart_before = stats->restart_requests;
	request.key = *key; request.target = *target; request.snapshot = *snapshot;
	request.cap = CLUSTER_MX_COMPOSITION_PAGE_MEMBERS; request.peer_generation = generation;
	request.route.epoch = key->cluster_epoch; request.route.requester_node = cluster_node_id;
	request.route.requester_backend = MyBackendId;
	PG_TRY();
	{
		owned->key = *key;
		owned->system_identifier = GetSystemIdentifier();
		owned->loaded_catalog = GetControlFileCatalogVersion();
		owned->peer_generation = generation;
		for (;;) {
			ClusterMxRangeRoute route;
			BufferTag tag;
			ClusterMxCompositionReply reply;
			ClusterMxRangeWireContext context = {0};
			ClusterMxCompositionReceiveResult accepted;

			CHECK_FOR_INTERRUPTS();
			if (!cluster_gcs_current_mx_range_fetch_and_wait(&request, &response, &bytes)
				|| !cluster_mx_range_transport_route(&response, bytes, PGRAC_IC_MSG_GCS_BLOCK_REPLY, &route, &tag)
				|| route.request_id != request.route.request_id || route.epoch != request.route.epoch
				|| route.requester_node != request.route.requester_node
				|| route.requester_backend != request.route.requester_backend
				|| bytes < sizeof(route) + sizeof(ClusterMxRangeWireHeader) + sizeof(source)) break;
			if (!request.has_source) {
				memcpy(&source, response.data.data + sizeof(ClusterMxRangeWireHeader), sizeof(source));
				if (source.system_identifier == 0 || source.system_identifier != GetSystemIdentifier()
					|| source.formation_epoch != request.route.epoch || source.boot_incarnation == 0
					|| source.admission_revision == 0 || source.admission_revision == UINT64_MAX
					|| source.capability_generation == 0 || source.capability_generation == UINT32_MAX
					|| source.reserved != 0 || source.native.multi != key->multixact_id
					|| source.native.origin_node != key->origin_node_id || source.native.epoch != request.route.epoch
					|| memcmp(&source.target, target, sizeof(*target)) != 0
					|| memcmp(&source.snapshot, snapshot, sizeof(*snapshot)) != 0) break;
			}
			context.request_id = request.route.request_id; context.epoch = request.route.epoch;
			context.key = *key; context.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
			context.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
			context.loaded_catalog = GetControlFileCatalogVersion();
			context.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
			context.peer_generation = generation; context.requested_cap = request.cap;
			context.source = &source; context.source_bytes = sizeof(source);
			if (cluster_mx_range_wire_decode_reply(response.data.data, bytes - sizeof(route), &context, &reply)
				!= MXCOMP_READ_OK) break;
			if (owned->op == NULL
				&& !cluster_mx_composition_begin(&owned->op, &source, sizeof(source), source.native.span,
					source.native.start, stats)) break;
			accepted = cluster_mx_composition_accept(&owned->op, MXCOMP_READ_OK, &reply);
			if (accepted == MXCOMP_UNKNOWN) break;
			if (key->cluster_epoch != cluster_epoch_get_current()
				|| !cluster_sf_peer_foreign_mx_capability_matches_v1(key->origin_node_id,
					CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
					GetControlFileCatalogVersion(), generation)) break;
			if (accepted == MXCOMP_COMPLETE) { complete = true; break; }
			request.first = cluster_mx_composition_next(owned->op);
			request.has_source = 1; request.source = source;
		}
	}
	PG_CATCH();
	{
		range_owned_end(owned);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (complete) { *op = owned->op; return true; }
	range_owned_end(owned);
	/* G2 may already have counted the failed complete operation. Incomplete
	 * cleanup also records UNKNOWN; do not count the same outcome twice. */
	if (stats->unknown_results == unknown_before && unknown_before != UINT64_MAX)
		stats->unknown_results++;
	if (stats->restart_requests == restart_before && restart_before != UINT64_MAX)
		stats->restart_requests++;
	return false;
}

/* Preserve the bounded legacy consumer until its proof/receipt side is also
 * full-width. The new service still obtains and validates the entire list;
 * it never labels a prefix as a complete descriptor. */
ClusterMxDescribeResult
cluster_multixact_current_describe_scoped(const ClusterCurrentMxKey *key,
	const BufferTag *target, Snapshot snapshot, ClusterCurrentMxMemberDesc *members,
	uint16 cap, uint16 *count, uint32 *total)
{
	ClusterMxRangeSnapshot value = {0};
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterMxCompositionReceive *op = NULL;
	const ClusterMxCompositionPage *page;
	uint32 generation = 0, copied = 0, n = 0;
	ClusterMxDescribeResult result = CMX_DESC_UNKNOWN;

	if (key == NULL || !cluster_sf_peer_foreign_mx_capability_sample_v1(key->origin_node_id,
		CLUSTER_FOREIGN_MX_FORMAT_ID_V1, CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1,
		GetControlFileCatalogVersion(), &generation))
		return cluster_multixact_current_describe(key, members, cap, count, total);
	if (members != NULL && cap > 0) memset(members, 0, (Size)cap * sizeof(*members));
	if (count != NULL) *count = 0;
	if (total != NULL) *total = 0;
	if (snapshot == NULL || target == NULL || members == NULL || cap == 0 || count == NULL || total == NULL)
		return CMX_DESC_UNKNOWN;
	value.read_scn = snapshot->read_scn; value.read_epoch = snapshot->read_epoch;
	value.snapshot_type = snapshot->snapshot_type; value.xmin = snapshot->xmin;
	value.xmax = snapshot->xmax; value.curcid = snapshot->curcid;
	/* Count the logical describe once, irrespective of its range frames.
	 * The legacy fallback above retains its original statistics owner. */
	cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_ASK);
	PG_TRY();
	{
		if (cluster_multixact_current_describe_full(key, target, &value, &op, &stats)) {
			page = cluster_mx_composition_pages(op, &n);
			if (n > cap || n > CLUSTER_CURRENT_MX_MAX_MEMBERS) {
				*total = n;
				result = CMX_DESC_SUPPORTED_LIMIT;
			} else {
				for (; page != NULL; page = page->next) {
					memcpy(members + copied, page->members, (Size)page->count * sizeof(*members));
					copied += page->count;
				}
				if (copied == n && n > 0) { *total = n; *count = (uint16)n; result = CMX_DESC_OK; }
			}
		}
	}
	PG_FINALLY();
	{
		cluster_multixact_current_describe_finish(&op);
	}
	PG_END_TRY();
	/* ERROR/cancel rethrows through FINALLY and has no returned outcome. */
	if (result == CMX_DESC_OK)
		cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_HIT);
	else if (result == CMX_DESC_SUPPORTED_LIMIT)
		cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT);
	else
		cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
	return result;
}
