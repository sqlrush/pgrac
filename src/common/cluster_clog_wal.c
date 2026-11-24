/* Shared native CLOG WAL framing, without replay or control authority. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif

#include "access/clog.h"
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

static bool
clog_restore_overlap(const void *left, Size left_bytes,
					 const void *right, Size right_bytes)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;

	return a <= b ? b - a < left_bytes : a - b < right_bytes;
}

bool
cluster_clog_wal_page_restore_v2(const ClusterClogFileBindingV2 *expected,
								 const void *record, Size record_length,
								 XLogRecPtr record_end,
								 const void *current_frame, Size current_length,
								 void *out, Size length)
{
	ClusterClogWalObservationV2 observed;
	ClusterClogPageIdentityV2 page, current;
	uint8 reference[CLUSTER_CLOG_FILE_REFERENCE_BYTES];
	uint8 payload[CLUSTER_CLOG_PAGE_BYTES];
	uint8 current_payload[CLUSTER_CLOG_PAGE_BYTES];

	if (expected == NULL || record == NULL || out == NULL ||
		record_length != CLUSTER_CLOG_PAGE_WAL_BYTES_V2 ||
		length != CLUSTER_CLOG_FRAME_BYTES ||
		(current_frame == NULL ? current_length != 0 :
		 current_length != CLUSTER_CLOG_FRAME_BYTES) ||
		clog_restore_overlap(out, length, expected, sizeof(*expected)) ||
		clog_restore_overlap(out, length, record, record_length) ||
		(current_frame != NULL &&
		 clog_restore_overlap(out, length, current_frame, current_length)) ||
		record_end == InvalidXLogRecPtr || record_end == UINT64_MAX)
		return false;

	/* Compare the complete canonical identity, never C padding or only raw
	 * page numbers. The selected source/registration remains owner supplied. */
	if (!cluster_clog_wal_decode_v2(CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2,
								   record, record_length, &observed) ||
		!cluster_recovery_file_identity_encode_v2(&expected->identity,
												 reference, sizeof(reference)) ||
		expected->segment != observed.binding.segment ||
		memcmp(reference, record, sizeof(reference)) != 0 ||
		record_end <= observed.page.identity.page_lsn ||
		record_end <= observed.page.image_lsn ||
		!cluster_clog_file_page_decode_v2(expected, observed.page.identity.logical_page,
				observed.image, observed.image_bytes, &page, payload, sizeof(payload)))
		return false;

	/* The record contains the captured prior image position, not its own
	 * actual end. Keep this distinct from the source-local status watermark. */
	page.image_lsn = record_end;
	if (current_frame != NULL)
	{
		if (!cluster_clog_file_page_decode_v2(expected, page.identity.logical_page,
				current_frame, current_length, &current, current_payload, sizeof(current_payload)) ||
			page.identity.page_generation != current.identity.page_generation ||
			!FullTransactionIdEquals(page.identity.birth_full_xid,
								 current.identity.birth_full_xid))
			return false;

		/* A captured image may reach WAL after native terminal redo for this
		 * same birth. Replacing those bytes would lose a committed transaction.
		 * Join the original CLOG states: in-progress < subcommitted < terminal.
		 * Different terminal outcomes have no legal join. Validate the entire
		 * private page before publishing any byte to the caller. */
		for (Size i = 0; i < sizeof(payload); i++)
		{
			for (unsigned shift = 0; shift < 8; shift += 2)
			{
				unsigned a = (payload[i] >> shift) & 3;
				unsigned b = (current_payload[i] >> shift) & 3;
				unsigned joined;

				if (a == b || b == TRANSACTION_STATUS_IN_PROGRESS)
					joined = a;
				else if (a == TRANSACTION_STATUS_IN_PROGRESS ||
						 a == TRANSACTION_STATUS_SUB_COMMITTED)
					joined = b;
				else if (b == TRANSACTION_STATUS_SUB_COMMITTED)
					joined = a;
				else
					return false;
				payload[i] = (payload[i] & ~(3U << shift)) | (joined << shift);
			}
		}
		page.identity.page_lsn = Max(page.identity.page_lsn, current.identity.page_lsn);
		page.image_lsn = Max(page.image_lsn, current.image_lsn);
	}
	return cluster_clog_file_page_encode_v2(expected, &page, payload, sizeof(payload),
										   out, length);
}
