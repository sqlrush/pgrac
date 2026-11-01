/* Shared native CLOG WAL framing, without replay or control authority. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "access/clog_wal.h"

static uint64
clog_wal_get(const uint8 *bytes, unsigned width)
{
	uint64 value = 0;
	for (unsigned i = 0; i < width; i++)
		value = (value << 8) | bytes[i];
	return value;
}

bool
cluster_clog_wal_decode_v2(uint8 info, const void *bytes, Size length,
						   ClusterClogWalObservationV2 *out)
{
	const uint8 *p = bytes;
	const uint8 *body;
	ClusterClogWalObservationV2 observed = {0};
	Size expected;
	uintptr_t a = (uintptr_t)bytes, b = (uintptr_t)out;

	switch (info)
	{
		case CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2:
			expected = CLUSTER_CLOG_SEGMENT_WAL_BYTES_V2;
			break;
		case CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2:
			expected = CLUSTER_CLOG_PAGE_WAL_BYTES_V2;
			break;
		case CLUSTER_RECOVERY_CLOG_TRUNCATE_WAL_V2:
			expected = CLUSTER_CLOG_TRUNCATE_WAL_BYTES_V2;
			break;
		default:
			return false;
	}
	if (p == NULL || out == NULL || length != expected ||
		(a <= b ? b - a < length : a - b < sizeof(*out)))
		return false;
	if (!cluster_recovery_file_identity_decode_v2(p, 128, &observed.binding.identity) ||
		observed.binding.identity.kind != CLUSTER_RECOVERY_FILE_CLOG_V2 ||
		observed.binding.identity.object_id >= 4096)
		return false;
	observed.info = info;
	observed.binding.segment = (uint32)observed.binding.identity.object_id;
	body = p + 128;
	if (info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2)
	{
		if (!cluster_clog_file_header_matches_v2(body, CLUSTER_CLOG_FILE_HEADER_BYTES,
											   &observed.binding))
			return false;
		observed.image = body;
		observed.image_bytes = CLUSTER_CLOG_FILE_HEADER_BYTES;
	}
	else if (info == CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2)
	{
		uint8 payload[CLUSTER_CLOG_PAGE_BYTES];
		uint64 logical_page = clog_wal_get(body + 8, 8);

		if (!cluster_clog_file_page_decode_v2(&observed.binding, logical_page,
					body, CLUSTER_CLOG_FRAME_BYTES, &observed.page, payload, sizeof(payload)))
			return false;
		observed.image = body;
		observed.image_bytes = CLUSTER_CLOG_FRAME_BYTES;
	}
	else
	{
		observed.cutoff_page = (uint32)clog_wal_get(body, 4);
		observed.oldest_xid = (TransactionId)clog_wal_get(body + 4, 4);
		observed.oldest_database = (Oid)clog_wal_get(body + 8, 4);
		observed.oldest_full_xid = FullTransactionIdFromU64(clog_wal_get(body + 16, 8));
		if (clog_wal_get(body + 12, 4) != 0 ||
			!TransactionIdIsNormal(observed.oldest_xid) ||
			U64FromFullTransactionId(observed.oldest_full_xid) == UINT64_MAX ||
			XidFromFullTransactionId(observed.oldest_full_xid) != observed.oldest_xid ||
			observed.cutoff_page != observed.oldest_xid / 32768)
			return false;
	}
	*out = observed;
	return true;
}
