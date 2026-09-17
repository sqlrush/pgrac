/*-------------------------------------------------------------------------
 *
 * cluster_rf_route.c
 *    Generated-table lookup for the exhaustive STOP-06 WAL route manifest.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_rf_route.h"

typedef struct RfOpcodeRouteManifestEntryV1 {
	RfOpcodeRouteV1 route;
	bool active;
	const char *diagnostic_name;
} RfOpcodeRouteManifestEntryV1;

#define RF_ROUTE(rmid_, info_, flags_, owner_, blocks_, codec_, active_, name_)                    \
	{ { (uint8)(rmid_), (uint8)(info_), (uint8)(flags_), (uint8)(owner_), (uint8)(blocks_),        \
		(uint8)(codec_), 0 },                                                                      \
	  (active_),                                                                                   \
	  (name_) },
static const RfOpcodeRouteManifestEntryV1 rf_opcode_route_manifest_v1[] = {
#include "cluster/cluster_rf_route_manifest.def"
};
#undef RF_ROUTE

StaticAssertDecl(RM_XLOG_ID == 0, "STOP-06 manifest assumes RM_XLOG_ID 0");
StaticAssertDecl(RM_XACT_ID == 1, "STOP-06 manifest assumes RM_XACT_ID 1");
StaticAssertDecl(RM_SMGR_ID == 2, "STOP-06 manifest assumes RM_SMGR_ID 2");
StaticAssertDecl(RM_CLOG_ID == 3, "STOP-06 manifest assumes RM_CLOG_ID 3");
StaticAssertDecl(RM_DBASE_ID == 4, "STOP-06 manifest assumes RM_DBASE_ID 4");
StaticAssertDecl(RM_TBLSPC_ID == 5, "STOP-06 manifest assumes RM_TBLSPC_ID 5");
StaticAssertDecl(RM_MULTIXACT_ID == 6, "STOP-06 manifest assumes RM_MULTIXACT_ID 6");
StaticAssertDecl(RM_RELMAP_ID == 7, "STOP-06 manifest assumes RM_RELMAP_ID 7");
StaticAssertDecl(RM_STANDBY_ID == 8, "STOP-06 manifest assumes RM_STANDBY_ID 8");
StaticAssertDecl(RM_HEAP2_ID == 9, "STOP-06 manifest assumes RM_HEAP2_ID 9");
StaticAssertDecl(RM_HEAP_ID == 10, "STOP-06 manifest assumes RM_HEAP_ID 10");
StaticAssertDecl(RM_BTREE_ID == 11, "STOP-06 manifest assumes RM_BTREE_ID 11");
StaticAssertDecl(RM_HASH_ID == 12, "STOP-06 manifest assumes RM_HASH_ID 12");
StaticAssertDecl(RM_GIN_ID == 13, "STOP-06 manifest assumes RM_GIN_ID 13");
StaticAssertDecl(RM_GIST_ID == 14, "STOP-06 manifest assumes RM_GIST_ID 14");
StaticAssertDecl(RM_SEQ_ID == 15, "STOP-06 manifest assumes RM_SEQ_ID 15");
StaticAssertDecl(RM_SPGIST_ID == 16, "STOP-06 manifest assumes RM_SPGIST_ID 16");
StaticAssertDecl(RM_BRIN_ID == 17, "STOP-06 manifest assumes RM_BRIN_ID 17");
StaticAssertDecl(RM_COMMIT_TS_ID == 18, "STOP-06 manifest assumes RM_COMMIT_TS_ID 18");
StaticAssertDecl(RM_REPLORIGIN_ID == 19, "STOP-06 manifest assumes RM_REPLORIGIN_ID 19");
StaticAssertDecl(RM_GENERIC_ID == 20, "STOP-06 manifest assumes RM_GENERIC_ID 20");
StaticAssertDecl(RM_LOGICALMSG_ID == 21, "STOP-06 manifest assumes RM_LOGICALMSG_ID 21");
StaticAssertDecl(RM_CLUSTER_UNDO_ID == 22, "STOP-06 manifest assumes RM_CLUSTER_UNDO_ID 22");
StaticAssertDecl(RM_CLUSTER_RAW_LAYOUT_ID == 23,
				 "STOP-06 manifest assumes RM_CLUSTER_RAW_LAYOUT_ID 23");
StaticAssertDecl(RM_CLUSTER_ADG_ID == 24, "STOP-06 manifest assumes RM_CLUSTER_ADG_ID 24");
StaticAssertDecl(RM_CLUSTER_XID_STRIPE_ID == 25,
				 "STOP-06 manifest assumes RM_CLUSTER_XID_STRIPE_ID 25");
StaticAssertDecl(RM_N_BUILTIN_IDS == 26, "STOP-06 manifest must cover exactly 26 built-in rmgrs");

enum {
	RF_OPCODE_ROUTE_MANIFEST_COUNT_V1 = 0
#define RF_ROUTE(rmid_, info_, flags_, owner_, blocks_, codec_, active_, name_) +1
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_ROUTE
};

enum {
	RF_OPCODE_ROUTE_MANIFEST_LIVE_COUNT_V1 = 0
#define RF_ROUTE(rmid_, info_, flags_, owner_, blocks_, codec_, active_, name_) +((active_) ? 1 : 0)
#include "cluster/cluster_rf_route_manifest.def"
#undef RF_ROUTE
};

StaticAssertDecl(RF_OPCODE_ROUTE_MANIFEST_COUNT_V1 == 138,
				 "STOP-06 manifest must contain exactly 138 rows");
StaticAssertDecl(RF_OPCODE_ROUTE_MANIFEST_LIVE_COUNT_V1 == 138,
				 "STOP-06 manifest must contain exactly 138 live rows");
StaticAssertDecl(lengthof(rf_opcode_route_manifest_v1) == RF_OPCODE_ROUTE_MANIFEST_COUNT_V1,
				 "STOP-06 generated table count must match manifest count");

size_t
rf_opcode_route_manifest_count_v1(void)
{
	return RF_OPCODE_ROUTE_MANIFEST_COUNT_V1;
}

size_t
rf_opcode_route_manifest_live_count_v1(void)
{
	return RF_OPCODE_ROUTE_MANIFEST_LIVE_COUNT_V1;
}

bool
rf_opcode_route_manifest_entry_v1(size_t index, RfOpcodeRouteV1 *route, bool *active)
{
	if (index >= lengthof(rf_opcode_route_manifest_v1) || route == NULL || active == NULL)
		return false;

	*route = rf_opcode_route_manifest_v1[index].route;
	*active = rf_opcode_route_manifest_v1[index].active;
	return true;
}

RfOpcodeRouteLookupResultV1
rf_opcode_route_lookup_v1(uint8 rmid, uint8 raw_info, bool has_blocks, bool space_active,
						  RfOpcodeRouteV1 *route)
{
	const RfOpcodeRouteManifestEntryV1 *entry = NULL;
	uint8 normalized_info;
	uint8 info_flags = 0;
	size_t i;

	if (rmid >= RM_MIN_CUSTOM_ID || rmid >= RM_N_BUILTIN_IDS)
		return RF_OPCODE_ROUTE_RMID_UNSUPPORTED;

	if (rmid == RM_XACT_ID) {
		normalized_info = raw_info & XLOG_XACT_OPMASK;
		info_flags = raw_info & XLOG_XACT_HAS_INFO;
	} else
		normalized_info = raw_info & ~XLR_INFO_MASK;

	for (i = 0; i < lengthof(rf_opcode_route_manifest_v1); i++) {
		const RfOpcodeRouteManifestEntryV1 *candidate = &rf_opcode_route_manifest_v1[i];

		if (candidate->route.rmid == rmid && candidate->route.normalized_info == normalized_info) {
			entry = candidate;
			break;
		}
	}

	if (entry == NULL)
		return RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED;
	if ((info_flags & ~entry->route.legal_info_flags) != 0)
		return RF_OPCODE_ROUTE_FLAG_ILLEGAL;
	if (!entry->active && !space_active)
		return RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED;
	if ((entry->route.block_policy == RF_ROUTE_BLOCKS_REQUIRED && !has_blocks)
		|| (entry->route.block_policy == RF_ROUTE_BLOCKS_FORBIDDEN && has_blocks))
		return RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID;
	if (route == NULL)
		return RF_OPCODE_ROUTE_NESTED_VALUE_INVALID;

	*route = entry->route;
	return RF_OPCODE_ROUTE_OK;
}
