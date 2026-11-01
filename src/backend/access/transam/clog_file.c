/* Versioned CLOG file framing. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/clog_file.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "port/pg_crc32c.h"

/* Integers in CLOG framing are big endian. TT40 retains its original
 * native page layout and whole-page checksum owner. */
static const uint8 file_magic[8] = {'P', 'C', 'L', 'G', '0', '0', '2', 0};
static const uint8 page_magic[4] = {'P', 'C', 'P', '2'};

static bool
bytes_zero(const uint8 *p, Size n)
{
	for (Size i = 0; i < n; i++)
		if (p[i] != 0)
			return false;
	return true;
}

static void
put16(uint8 *p, uint16 n)
{
	p[0] = (uint8)(n >> 8);
	p[1] = (uint8)n;
}

static void
put32(uint8 *p, uint32 n)
{
	for (int i = 0; i < 4; i++)
		p[i] = (uint8)(n >> (24 - i * 8));
}

static void
put64(uint8 *p, uint64 n)
{
	for (int i = 0; i < 8; i++)
		p[i] = (uint8)(n >> (56 - i * 8));
}

static uint16
get16(const uint8 *p)
{
	return ((uint16)p[0] << 8) | p[1];
}

static uint32
get32(const uint8 *p)
{
	uint32 n = 0;
	for (int i = 0; i < 4; i++)
		n = (n << 8) | p[i];
	return n;
}

static uint64
get64(const uint8 *p)
{
	uint64 n = 0;
	for (int i = 0; i < 8; i++)
		n = (n << 8) | p[i];
	return n;
}

static bool
counter_valid(uint64 value)
{
	return value != 0 && value != UINT64_MAX;
}

/* Common framing checks do not replace the original family/owner bounds. */
static bool
file_reference(const ClusterRecoveryFileIdentityV2 *id, uint8 out[128])
{
	return id != NULL &&
		(id->kind == CLUSTER_RECOVERY_FILE_CLOG_V2 || id->kind == CLUSTER_RECOVERY_FILE_UNDO_V2) &&
		id->origin < UNDO_OWNER_INSTANCE_MAX &&
		cluster_recovery_file_identity_encode_v2(id, out, CLUSTER_CLOG_FILE_REFERENCE_BYTES);
}

static bool
binding_valid(const ClusterClogFileBindingV2 *binding)
{
	uint8 reference[128];

	/* 2^32 raw xids / 32768 per page / 32 pages per segment. */
	return binding != NULL && binding->segment < 4096 &&
		binding->identity.kind == CLUSTER_RECOVERY_FILE_CLOG_V2 &&
		binding->identity.object_id == binding->segment &&
		file_reference(&binding->identity, reference);
}

static bool
page_valid(const ClusterClogFileBindingV2 *binding, const ClusterClogPageIdentityV2 *page)
{
	return binding_valid(binding) && page != NULL &&
		page->identity.logical_page < 131072 && page->identity.logical_page / 32 == binding->segment &&
		counter_valid(page->identity.page_generation) && page->identity.birth_observed &&
		U64FromFullTransactionId(page->identity.birth_full_xid) != UINT64_MAX &&
		TransactionIdIsNormal(XidFromFullTransactionId(page->identity.birth_full_xid)) &&
		XidFromFullTransactionId(page->identity.birth_full_xid) / 32768 == page->identity.logical_page &&
		page->identity.page_lsn != UINT64_MAX && page->image_lsn != UINT64_MAX;
}

static uint32
header_crc(const uint8 *p)
{
	static const uint8 zero[4] = {0};
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, p, 20);
	COMP_CRC32C(crc, zero, 4);
	COMP_CRC32C(crc, p + 24, CLUSTER_CLOG_FILE_HEADER_BYTES - 24);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static uint32
frame_crc(const ClusterClogFileBindingV2 *binding, const uint8 *p, bool payload)
{
	static const uint8 zero[4] = {0};
	uint8 segment[4];
	uint8 reference[128];
	pg_crc32c crc;
	bool valid = file_reference(&binding->identity, reference);

	Assert(valid);
	if (!valid)
		return 0;
	put32(segment, binding->segment);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, reference, sizeof(reference));
	COMP_CRC32C(crc, segment, sizeof(segment));
	COMP_CRC32C(crc, p, 48);
	if (payload)
		COMP_CRC32C(crc, p + 64, CLUSTER_CLOG_PAGE_BYTES);
	else {
		COMP_CRC32C(crc, zero, 4);
		COMP_CRC32C(crc, p + 52, 12);
	}
	FIN_CRC32C(crc);
	return (uint32)crc;
}

bool
cluster_clog_file_offset_v2(uint32 local_page, uint64 *offset)
{
	if (local_page >= CLUSTER_CLOG_PAGES_PER_FILE || offset == NULL)
		return false;
	*offset = CLUSTER_CLOG_FILE_HEADER_BYTES + (uint64)local_page * CLUSTER_CLOG_FRAME_BYTES;
	return true;
}

bool
cluster_clog_file_header_encode_v2(const ClusterClogFileBindingV2 *binding, void *out, Size length)
{
	uint8 bytes[CLUSTER_CLOG_FILE_HEADER_BYTES] = {0};
	if (!binding_valid(binding) || out == NULL || length != sizeof(bytes))
		return false;
	memcpy(bytes, file_magic, sizeof(file_magic));
	put16(bytes + 8, 2);
	put16(bytes + 10, CLUSTER_CLOG_FILE_HEADER_BYTES);
	put32(bytes + 12, CLUSTER_CLOG_PAGE_BYTES);
	put16(bytes + 16, CLUSTER_CLOG_PAGE_HEADER_BYTES);
	put16(bytes + 18, CLUSTER_CLOG_PAGES_PER_FILE);
	put32(bytes + 24, binding->segment);
	if (!file_reference(&binding->identity, bytes + 32))
		return false;
	put32(bytes + 20, header_crc(bytes));
	memcpy(out, bytes, sizeof(bytes));
	return true;
}

bool
cluster_clog_file_header_matches_v2(const void *bytes, Size length,
								   const ClusterClogFileBindingV2 *expected)
{
	const uint8 *p = bytes;
	uint8 reference[128];
	if (p == NULL || length != CLUSTER_CLOG_FILE_HEADER_BYTES || !binding_valid(expected))
		return false;
	if (!file_reference(&expected->identity, reference))
		return false;
	return memcmp(p, file_magic, sizeof(file_magic)) == 0 && get16(p + 8) == 2 &&
		get16(p + 10) == CLUSTER_CLOG_FILE_HEADER_BYTES &&
		get32(p + 12) == CLUSTER_CLOG_PAGE_BYTES &&
		get16(p + 16) == CLUSTER_CLOG_PAGE_HEADER_BYTES &&
		get16(p + 18) == CLUSTER_CLOG_PAGES_PER_FILE &&
		get32(p + 24) == expected->segment && bytes_zero(p + 28, 4) &&
		memcmp(p + 32, reference, sizeof(reference)) == 0 &&
		bytes_zero(p + 160, CLUSTER_CLOG_FILE_HEADER_BYTES - 160) &&
		get32(p + 20) == header_crc(p);
}

bool
cluster_clog_file_page_encode_v2(const ClusterClogFileBindingV2 *binding,
								const ClusterClogPageIdentityV2 *page,
								const void *payload, Size payload_length, void *out, Size length)
{
	uint8 bytes[CLUSTER_CLOG_FRAME_BYTES] = {0};
	if (!page_valid(binding, page) || payload == NULL || out == NULL ||
		payload_length != CLUSTER_CLOG_PAGE_BYTES || length != sizeof(bytes))
		return false;
	memcpy(bytes, page_magic, sizeof(page_magic));
	put16(bytes + 4, 2);
	put16(bytes + 6, CLUSTER_CLOG_PAGE_HEADER_BYTES);
	put64(bytes + 8, page->identity.logical_page);
	put64(bytes + 16, page->identity.page_generation);
	put64(bytes + 24, U64FromFullTransactionId(page->identity.birth_full_xid));
	put64(bytes + 32, page->identity.page_lsn);
	put64(bytes + 40, page->image_lsn);
	memcpy(bytes + CLUSTER_CLOG_PAGE_HEADER_BYTES, payload, payload_length);
	put32(bytes + 52, frame_crc(binding, bytes, true));
	put32(bytes + 48, frame_crc(binding, bytes, false));
	memcpy(out, bytes, sizeof(bytes));
	return true;
}

bool
cluster_clog_file_page_decode_v2(const ClusterClogFileBindingV2 *binding,
								uint64 expected_page, const void *bytes, Size length,
								ClusterClogPageIdentityV2 *page_out, void *payload_out,
								Size payload_length)
{
	const uint8 *p = bytes;
	ClusterClogPageIdentityV2 page;
	uintptr_t a = (uintptr_t)page_out, b = (uintptr_t)payload_out;

	if (!binding_valid(binding) || p == NULL || length != CLUSTER_CLOG_FRAME_BYTES ||
		page_out == NULL || payload_out == NULL || payload_length != CLUSTER_CLOG_PAGE_BYTES ||
		(a <= b ? b - a < sizeof(*page_out) : a - b < payload_length))
		return false;
	if (memcmp(p, page_magic, sizeof(page_magic)) != 0 || get16(p + 4) != 2 ||
		get16(p + 6) != CLUSTER_CLOG_PAGE_HEADER_BYTES || !bytes_zero(p + 56, 8) ||
		get32(p + 48) != frame_crc(binding, p, false) ||
		get32(p + 52) != frame_crc(binding, p, true))
		return false;
	memset(&page, 0, sizeof(page));
	page.identity.logical_page = get64(p + 8);
	page.identity.page_generation = get64(p + 16);
	page.identity.birth_observed = true;
	page.identity.birth_full_xid = FullTransactionIdFromU64(get64(p + 24));
	page.identity.page_lsn = get64(p + 32);
	page.identity.metadata_crc = get32(p + 48);
	page.identity.payload_crc = get32(p + 52);
	page.image_lsn = get64(p + 40);
	if (page.identity.logical_page != expected_page || !page_valid(binding, &page))
		return false;
	memmove(payload_out, p + CLUSTER_CLOG_PAGE_HEADER_BYTES, payload_length);
	*page_out = page;
	return true;
}

/* Copy before reading typed fields, so unaligned byte buffers are safe. This
 * is only framing, not the checksum, control binding, or live writer guard. */
static bool
tt40_private_image(const void *bytes, Size length, const ClusterRecoveryFileIdentityV2 *identity)
{
#ifdef USE_PGRAC_CLUSTER
	UndoSegmentHeaderDataV2 h;
	uint32 first;
	if (bytes == NULL || length != sizeof(h))
		return false;
	memcpy(&h, bytes, sizeof(h));
	if (identity->kind != CLUSTER_RECOVERY_FILE_UNDO_V2 ||
		identity->origin != h.owner_instance - 1 || identity->object_id != h.segment_id ||
		h.tt_layout_version != 2 || h.tt_slot_bytes != 40 || h.tt_layout_reserved != 0 ||
		(h.pd_flags & PD_UNDO_SEG_HEADER) == 0 ||
		h.pd_pagesize_version != (BLCKSZ | PG_PAGE_LAYOUT_VERSION) ||
		h.segment_size_bytes != UNDO_SEGMENT_SIZE_BYTES || h.owner_instance == 0 ||
		h.owner_instance > UNDO_OWNER_INSTANCE_MAX || h.tt_slots_count != TT_SLOTS_PER_SEGMENT ||
		h.tt_next_free_slot > TT_SLOTS_PER_SEGMENT || h.tt_active_count > TT_SLOTS_PER_SEGMENT ||
		h.segment_state > SEGMENT_RECYCLABLE || !bytes_zero((const uint8 *)bytes + 3068, 4) ||
		!bytes_zero((const uint8 *)bytes + 3200, 8192 - 3200))
		return false;
	first = ((uint32)h.owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	return h.segment_id >= first && h.segment_id - first < CLUSTER_UNDO_SEGS_PER_INSTANCE;
#else
	/* A non-cluster build cannot interpret or prepare cluster TT pages. */
	return false;
#endif
}

bool
cluster_tt_file_reference_prepare_v2(void *private_block0, Size length,
									 const ClusterRecoveryFileIdentityV2 *identity)
{
	uint8 *p = private_block0;
	uint8 reference[128];
	if (!file_reference(identity, reference) || !tt40_private_image(p, length, identity))
		return false;
	if (!bytes_zero(p + CLUSTER_TT_FILE_REFERENCE_OFFSET, sizeof(reference)) &&
		memcmp(p + CLUSTER_TT_FILE_REFERENCE_OFFSET, reference, sizeof(reference)) != 0)
		return false;
	memcpy(p + CLUSTER_TT_FILE_REFERENCE_OFFSET, reference, sizeof(reference));
	return true;
}

bool
cluster_tt_file_reference_matches_v2(const void *block0, Size length,
									 const ClusterRecoveryFileIdentityV2 *identity)
{
	uint8 reference[128];
	return file_reference(identity, reference) && tt40_private_image(block0, length, identity) &&
		memcmp((const uint8 *)block0 + CLUSTER_TT_FILE_REFERENCE_OFFSET, reference, 128) == 0;
}

StaticAssertDecl(offsetof(UndoSegmentHeaderDataV2, _reserved) == 3068,
	"TT40 reference cannot move the original management fields");
