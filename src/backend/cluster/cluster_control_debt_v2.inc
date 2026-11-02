/*-------------------------------------------------------------------------
 * cluster_control_debt_v2.inc
 *     Private canonical PREPARED debt bytes for the original ROOT owner.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_recovery_debt.h"
#include "cluster/cluster_recovery_debt_format.h"
#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_tt_slot.h"
#include "common/cluster_recovery_reservations.h"
#include "../../common/sha2_int.h"
#include "port/pg_crc32c.h"

/* Codec inputs are private, borrowed original-owner observations. These bytes
 * have NO authority, custody, publication, transfer or retirement semantics.
 * No production caller exists until the original episode/CF seam is connected.
 * Inline permits inclusion in that owner and in the exact-code unit fixture
 * without introducing public entry-vector or admission interfaces. */
typedef struct ControlDebtWireEntryV1 {
	ClusterRecoveryDebtEntryV2 entry;
	ClusterRelmapRetentionPin pin;
} ControlDebtWireEntryV1;
typedef struct ControlDebtWireBatchV1 {
	uint64 episode, prepare, entry_count;
	const uint8 *basis;
	Size basis_bytes;
	uint8 basis_ref[128];
	const ControlDebtWireEntryV1 *entries;
} ControlDebtWireBatchV1;
typedef struct ControlDebtWireImageV1 {
	uint8 *registry, *pages;
	Size registry_bytes, page_bytes;
	uint64 batch_count;
	uint8 reference[128];
} ControlDebtWireImageV1;
typedef struct ControlDebtWireKeyV1 { uint8 bytes[336]; } ControlDebtWireKeyV1;
typedef struct ControlDebtBatchKeyV1 { uint64 episode, basis, prepare; } ControlDebtBatchKeyV1;

static inline uint64 debt_get(const uint8 *p, Size at, unsigned width)
{
	uint64 n = 0;
	for (unsigned i = 0; i < width; i++) n = (n << 8) | p[at + i];
	return n;
}
static inline void debt_put(uint8 *p, Size at, uint64 n, unsigned width)
{
	for (unsigned i = 0; i < width; i++) p[at + i] = n >> (8 * (width - i - 1));
}
static inline bool debt_zero(const uint8 *p, Size n)
{
	for (Size i = 0; i < n; i++) if (p[i]) return false;
	return true;
}
static inline bool debt_counter(uint64 n) { return n && n != UINT64_MAX; }
static inline bool debt_overlap(const void *a, Size na, const void *b, Size nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && na && nb && (x <= y ? y - x < na : x - y < nb);
}
static inline bool debt_add(Size *n, uint64 count, Size width, Size limit)
{
	if (*n > limit || count > (limit - *n) / width) return false;
	*n += (Size)count * width;
	return true;
}
static inline uint32 debt_crc(const uint8 *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c); COMP_CRC32C(c, p, n); FIN_CRC32C(c);
	return c;
}
static inline uint32 debt_header_crc(const uint8 *p, Size n, Size at)
{
	static const uint8 zeros[4] = {0};
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, at); COMP_CRC32C(c, zeros, 4);
	COMP_CRC32C(c, p + at + 4, n - at - 4); FIN_CRC32C(c);
	return c;
}
static inline void debt_sha(const uint8 *p, Size n, uint8 out[32])
{
	pg_sha256_ctx ctx;
	pg_sha256_init(&ctx); pg_sha256_update(&ctx, p, n); pg_sha256_final(&ctx, out);
}
static inline bool debt_sha_matches(const uint8 *p, Size n, const uint8 expected[32])
{
	uint8 actual[32]; debt_sha(p, n, actual);
	return !memcmp(actual, expected, 32);
}
static inline void control_debt_wire_destroy_v1(ControlDebtWireImageV1 **owner)
{
	ControlDebtWireImageV1 *w;
	if (!owner || !(w = *owner)) return;
	*owner = NULL;
	free(w->pages); free(w->registry); free(w);
}
/* Reconstruct only the old codec's reference, then run its COMPLETE original
 * validator. This authenticates no source: the containing selected digest and
 * independently retained basis reference are still required by the owner.
 * Initial RDB1 supports the complete original RSV1/V3 wrapper only. */
static inline bool
debt_old_open(const uint8 *p, Size n, const uint8 *expected, Size budget,
	ClusterRecoveryRegistryDecodedV3 **out)
{
	uint8 ref[128];
	if (!p || n < 512 || n > budget || memcmp(p, "PRCREG3\0", 8)) return false;
	memcpy(ref, p + 128, 128); memcpy(ref, "PRCREF3\0", 8);
	debt_put(ref, 8, debt_get(p, 8, 4), 4);
	debt_put(ref, 12, debt_get(p, 12, 4), 4);
	debt_put(ref, 48, n, 8); debt_sha(p, n, ref + 72);
	debt_put(ref, 104, 0, 4); debt_put(ref, 108, debt_get(p, 64, 4), 4);
	debt_put(ref, 112, debt_get(p, 56, 8), 8);
	debt_put(ref, 120, CATALOG_VERSION_NO, 4); debt_put(ref, 124, 0, 4);
	debt_put(ref, 104, debt_crc(ref, 128), 4);
	return (!expected || !memcmp(ref, expected, 128))
		&& cluster_recovery_registry_decode_v3(p, n, ref, CATALOG_VERSION_NO, budget, out);
}
static inline void debt_source(const ClusterWalSourceRef *s, uint8 p[136])
{
	const ClusterControlRootIdentity *id = &s->claim.identity;
	memset(p, 0, 136);
	debt_put(p, 0, id->system_identifier, 8); memcpy(p + 8, id->storage_uuid, 16);
	memcpy(p + 24, id->authority_uuid, 16); debt_put(p, 40, id->origin_thread_id, 2);
	debt_put(p, 44, id->origin_node_id, 4); debt_put(p, 48, id->thread_claim_created_at, 8);
	debt_put(p, 56, id->thread_claim_crc32c, 4);
	debt_put(p, 64, id->origin_owner_incarnation, 8); debt_put(p, 72, id->root_lineage_seq, 8);
	debt_put(p, 80, s->claim.database_incarnation, 8);
	debt_put(p, 88, s->claim.max_config_generation, 8);
	memcpy(p + 96, s->claim.claim_sha256, 32); debt_put(p, 128, s->timeline, 4);
}
static inline int debt_key_compare(const void *a, const void *b)
{
	return memcmp(a, b, sizeof(ControlDebtWireKeyV1));
}
static inline int debt_batch_compare(const void *a, const void *b)
{
	const ControlDebtBatchKeyV1 *x = a, *y = b;
	if (x->episode != y->episode) return x->episode < y->episode ? -1 : 1;
	if (x->basis != y->basis) return x->basis < y->basis ? -1 : 1;
	return x->prepare == y->prepare ? 0 : x->prepare < y->prepare ? -1 : 1;
}
/* Complete old file/source membership, not a new floor/era/lifetime proof. */
static inline bool
debt_entry_valid(const uint8 p[512], uint64 ordinal, const ClusterRecoveryRegistryV3 *basis)
{
	const ClusterRecoveryRegistryV2 *r = &basis->available;
	const uint8 *v = p + 352, *tt = v + 88;
	ClusterRecoveryFileIdentityV2 file;
	uint64 source_count = 0, first = debt_get(p, 296, 8), count = debt_get(p, 304, 8);
	uint64 start = debt_get(p, 312, 8), end = debt_get(p, 320, 8);
	uint32 flags = debt_get(v, 80, 4);
	bool file_found = false;
	if (debt_get(p, 0, 8) != ordinal || debt_get(p, 328, 4) != 4
		|| !debt_counter(debt_get(p, 336, 8)) || !debt_zero(p + 332, 4)
		|| debt_get(p, 344, 4) != CLUSTER_RECOVERY_DEBT_RELMAP_FORMAT_ID_V1
		|| debt_get(p, 348, 2) != 1 || debt_get(p, 350, 2) != 128
		|| !debt_zero(p + 480, 28) || debt_get(p, 508, 4) != debt_crc(p, 508)
		|| !cluster_recovery_file_identity_decode_v2(p + 8, 128, &file)
		|| file.kind != CLUSTER_RECOVERY_FILE_RELMAP_V2
		|| !count || first > UINT64_MAX - count || !start || end <= start
		|| (uint32)debt_get(p, 272, 8) < FirstNormalTransactionId
		|| debt_get(p, 288, 8) < 2 || v[20] > 1
		|| !debt_zero(v + 21, 3) || !debt_zero(v + 34, 6)
		|| !debt_zero(v + 84, 4) || !debt_zero(v + 120, 8)
		|| !(flags & 1) || (flags & ~UINT32_C(7))
		|| debt_get(v, 0, 8) != file.system_identifier
		|| debt_get(v, 8, 8) != file.database_incarnation
		|| debt_get(v, 16, 4) != file.database_oid
		|| (v[20] != (file.database_oid == InvalidOid))
		|| debt_get(p, 280, 8) != file.database_oid
		|| debt_get(v, 24, 8) != debt_get(p, 288, 8)
		|| debt_get(v, 40, 8) != debt_get(p, 272, 8)
		|| debt_get(v, 32, 2) >= 128
		|| debt_get(v, 32, 2) != debt_get(p + 136, 44, 4)
		/* Original retained pins permit membership epoch zero. Preserve it
		 * exactly; neither zero nor any other value supplies qualification. */
		|| !debt_get(v, 56, 8) || debt_get(v, 56, 8) != debt_get(p + 136, 64, 8)
		|| !debt_get(v, 64, 8) || debt_get(v, 72, 8) <= start
		|| debt_get(v, 72, 8) > end
		|| file.system_identifier != r->system_identifier
		|| file.database_incarnation != r->database_incarnation
		|| memcmp(file.storage_uuid, r->storage_uuid, 16)
		|| memcmp(file.redo_branch_uuid, r->redo_branch_uuid, 16)
		|| file.redo_branch_generation != r->redo_branch_generation)
		return false;
	if (flags & 2) {
		if (!debt_get(tt, 0, 4) || debt_get(tt, 4, 4) == UINT32_MAX
			|| debt_get(tt, 8, 4) != (uint32)debt_get(v, 40, 8)
			|| debt_get(tt, 12, 2) >= TT_SLOTS_PER_SEGMENT
			|| debt_get(tt, 14, 2) == TT_WRAP_INVALID
			|| tt[16] != debt_get(v, 32, 2) + 1
			|| tt[17] != CLUSTER_CANONICAL_TXN_PUBLISHED
			|| !debt_zero(tt + 18, 6) || !debt_get(tt, 24, 8))
			return false;
	} else if (!debt_zero(tt, 32)) return false;
	for (uint64 i = 0; i < r->coverage.source_count; i++) {
		uint8 source[136];
		const ClusterRecoveryCheckpointSourceV2 *s = &r->coverage.sources[i];
		debt_source(&s->source, source);
		if (!memcmp(p + 136, source, 136)) {
			if (start < s->redo_start || end > s->replay_end) return false;
			source_count++;
		}
	}
	for (uint64 i = 0; i < r->coverage.file_count; i++) {
		const ClusterRecoveryCheckpointFileV2 *f = &r->coverage.files[i];
		if (cluster_recovery_file_identity_matches_v2(p + 8, 128, &f->identity)
			&& first >= f->first_page && first - f->first_page <= f->page_count
			&& count <= f->page_count - (first - f->first_page)) file_found = true;
	}
	for (uint64 i = 0; i < basis->reservation_count; i++)
		if (cluster_recovery_file_identity_matches_v2(p + 8, 128, &basis->reservations[i].expected))
			file_found = true;
	return file_found && source_count == 1;
}
static inline bool debt_entry_encode(const ControlDebtWireEntryV1 *in, uint8 p[512])
{
	const ClusterRecoveryDebtEntryV2 *e = &in->entry;
	const ClusterRelmapRetentionPin *pin = &in->pin;
	const ClusterRelmapRetentionKey *k = &pin->key;
	const ClusterCanonicalTxnBinding *t = &pin->binding;
	uint8 *v = p + 352;
	memset(p, 0, 512);
	debt_put(p, 0, e->ordinal, 8);
	if (!cluster_recovery_file_identity_encode_v2(&e->file, p + 8, 128)) return false;
	debt_source(&e->source, p + 136);
	debt_put(p, 272, U64FromFullTransactionId(e->top_full_xid), 8);
	debt_put(p, 280, e->namespace_id, 8); debt_put(p, 288, e->object_generation, 8);
	debt_put(p, 296, e->first_page, 8); debt_put(p, 304, e->page_count, 8);
	debt_put(p, 312, e->retain_start, 8); debt_put(p, 320, e->retain_end, 8);
	debt_put(p, 328, e->owner, 4); debt_put(p, 336, e->owner_generation, 8);
	debt_put(p, 344, CLUSTER_RECOVERY_DEBT_RELMAP_FORMAT_ID_V1, 4);
	debt_put(p, 348, 1, 2); debt_put(p, 350, 128, 2);
	debt_put(v, 0, k->identity.system_identifier, 8);
	debt_put(v, 8, k->identity.database_incarnation, 8);
	debt_put(v, 16, k->identity.dbid, 4); v[20] = k->identity.shared ? 1 : 0;
	debt_put(v, 24, k->generation, 8); debt_put(v, 32, k->origin, 2);
	debt_put(v, 40, k->top_full_xid, 8); debt_put(v, 48, k->membership_epoch, 8);
	debt_put(v, 56, k->admitted_incarnation, 8); debt_put(v, 64, k->boot_incarnation, 8);
	debt_put(v, 72, pin->wal_end, 8);
	debt_put(v, 80, (pin->occupied ? 1 : 0) | (pin->has_tt_binding ? 2 : 0)
		| (pin->may_be_staged ? 4 : 0), 4);
	/* Even absent bindings are encoded field by field, then required all zero.
	 * Refusal cannot silently erase a caller's contradictory observation. */
	debt_put(v, 88, t->segment_id, 4); debt_put(v, 92, t->segment_generation, 4);
	debt_put(v, 96, t->xid, 4); debt_put(v, 100, t->slot_offset, 2);
	debt_put(v, 102, t->slot_wrap, 2); v[104] = t->origin_instance; v[105] = t->publish_state;
	debt_put(v, 106, t->reserved16, 2); debt_put(v, 112, t->active_lsn, 8);
	debt_put(p, 508, debt_crc(p, 508), 4);
	return true;
}
static inline void debt_root_reference(const uint8 *p, Size n, uint8 ref[128])
{
	memset(ref, 0, 128);
	debt_put(ref, 0, CLUSTER_RECOVERY_DEBT_ROOT_REF_FORMAT_ID_V1, 4);
	debt_put(ref, 4, 1, 4); debt_put(ref, 8, 128, 4);
	debt_put(ref, 12, CATALOG_VERSION_NO, 4); debt_put(ref, 16, debt_get(p, 72, 8), 8);
	debt_put(ref, 24, n, 8); debt_put(ref, 32, debt_get(p, 104, 8), 8);
	debt_put(ref, 40, debt_get(p, 120, 8), 8); debt_put(ref, 48, debt_get(p, 144, 8), 8);
	debt_put(ref, 56, debt_get(p, 152, 8), 8); debt_sha(p, n, ref + 64);
	debt_put(ref, 124, debt_crc(ref, 124), 4);
}
/* Bounds are checked before vector traversal. Include complete wire storage,
 * sorting scratch and conservative space for the original decoded registries.
 * The codec grants no use of a reserved publication/progress budget. */
static inline bool
debt_dimensions(Size embedded, const ControlDebtWireBatchV1 *batches, uint64 nb,
	Size budget, Size *registry, Size *pages, uint64 *entries)
{
	Size r = 256, p = 0, work = sizeof(ControlDebtWireImageV1), max_basis = embedded;
	uint64 ne = 0, np = 0;
	if (!batches || !nb || !debt_add(&r, embedded, 1, budget)
		|| !debt_add(&r, nb, 384, budget)
		|| !debt_add(&work, nb, sizeof(ControlDebtBatchKeyV1), budget)
		|| nb > SIZE_MAX / sizeof(*batches)) return false;
	for (uint64 i = 0; i < nb; i++) {
		uint64 n = batches[i].entry_count, pages_in_batch;
		if (!n || n > SIZE_MAX / sizeof(ControlDebtWireEntryV1) || n > UINT64_MAX - ne
			|| !batches[i].basis || !batches[i].basis_bytes)
			return false;
		pages_in_batch = n / 64 + (n % 64 != 0);
		if (pages_in_batch > UINT64_MAX - np) return false;
		ne += n; np += pages_in_batch;
		max_basis = Max(max_basis, batches[i].basis_bytes);
	}
	if (!debt_add(&r, np, 128, budget) || !debt_add(&p, np, 33024, budget)
		|| !debt_add(&work, r, 1, budget) || !debt_add(&work, p, 1, budget)
		|| !debt_add(&work, ne, sizeof(ControlDebtWireKeyV1), budget)
		|| !debt_add(&work, max_basis, 8, budget)) return false;
	*registry = r; *pages = p; *entries = ne;
	return true;
}
static inline bool
control_debt_wire_validate_v1(const uint8 *p, Size n, const uint8 ref[128],
	const uint8 *pages, Size pn, const ControlDebtWireBatchV1 *basis, uint64 nb, Size budget)
{
	ClusterRecoveryRegistryDecodedV3 *old = NULL, *bd = NULL;
	const ClusterRecoveryRegistryV3 *embedded;
	ControlDebtWireKeyV1 *keys = NULL;
	ControlDebtBatchKeyV1 *bk = NULL;
	Size expected_n, expected_pn, embedded_n, batch_at, ref_at;
	uint64 ne, seen = 0, page_index = 0;
	uint8 reference[128];
	bool ok = false;
	if (!p || !ref || !pages || n < 256
		|| !cluster_recovery_debt_format_registered_v1(debt_get(p, 0, 4), debt_get(p, 4, 4),
			debt_get(p, 12, 4)) || debt_get(p, 8, 4) != 256
		|| !debt_counter(debt_get(p, 72, 8)) || !debt_counter(debt_get(p, 80, 8))
		|| !debt_zero(p + 240, 16)
		|| debt_get(p, 236, 4) != debt_header_crc(p, 256, 236)
		|| debt_get(p, 104, 8) > SIZE_MAX) return false;
	embedded_n = debt_get(p, 104, 8);
	if (!debt_dimensions(embedded_n, basis, nb, budget, &expected_n, &expected_pn, &ne)
		|| n != expected_n || pn != expected_pn || debt_get(p, 96, 8) != 256
		|| debt_get(p, 120, 8) != nb || debt_get(p, 128, 4) != 384
		|| debt_get(p, 132, 4) != 128 || debt_get(p, 144, 8) != pn / 33024
		|| debt_get(p, 152, 8) != ne || debt_get(p, 160, 8) != n)
		return false;
	batch_at = 256 + embedded_n; ref_at = batch_at + (Size)nb * 384;
	debt_root_reference(p, n, reference);
	if (memcmp(ref, reference, 128) || debt_get(p, 112, 8) != batch_at
		|| debt_get(p, 136, 8) != ref_at || debt_get(p, 232, 4) != debt_crc(p + 256, n - 256)
		|| !debt_sha_matches(p + 256, embedded_n, p + 168)
		|| !debt_sha_matches(p + batch_at, n - batch_at, p + 200)
		|| !debt_old_open(p + 256, embedded_n, NULL, budget, &old))
		goto done;
	embedded = cluster_recovery_registry_view_v3(old);
	if (debt_get(p, 88, 4) != debt_get(p + 256, 8, 4)
		|| debt_get(p, 92, 4) != debt_get(p + 256, 12, 4)
		|| debt_get(p, 16, 8) != embedded->available.system_identifier
		|| debt_get(p, 24, 8) != embedded->available.database_incarnation
		|| memcmp(p + 32, embedded->available.storage_uuid, 16)
		|| memcmp(p + 48, embedded->available.redo_branch_uuid, 16)
		|| debt_get(p, 64, 8) != embedded->available.redo_branch_generation
		|| debt_get(p, 72, 8) <= embedded->available.coverage.control_generation
		|| debt_get(p, 80, 8) < embedded->available.coverage.registration_generation)
		goto done;
	keys = malloc((Size)ne * sizeof(*keys)); bk = malloc((Size)nb * sizeof(*bk));
	if (!keys || !bk) goto done;
	for (uint64 i = 0; i < nb; i++) {
		const uint8 *b = p + batch_at + (Size)i * 384;
		uint64 count = debt_get(b, 56, 8), bp = debt_get(b, 64, 8);
		uint8 hash[32], basis_hash[32];
		pg_sha256_ctx entries_sha;
		if (!debt_old_open(basis[i].basis, basis[i].basis_bytes, basis[i].basis_ref,
			budget, &bd)) goto done;
		bk[i].episode = debt_get(b, 16, 8); bk[i].basis = debt_get(b, 24, 8);
		bk[i].prepare = debt_get(b, 32, 8);
		{
			const ClusterRecoveryRegistryV2 *original = &cluster_recovery_registry_view_v3(bd)->available;
			if (original->system_identifier != embedded->available.system_identifier
				|| original->database_incarnation != embedded->available.database_incarnation
				|| memcmp(original->storage_uuid, embedded->available.storage_uuid, 16)
				|| memcmp(original->redo_branch_uuid, embedded->available.redo_branch_uuid, 16)
				|| original->redo_branch_generation != embedded->available.redo_branch_generation)
				goto done;
		}
		if (debt_get(b, 0, 4) != CLUSTER_RECOVERY_DEBT_BATCH_FORMAT_ID_V1
			|| debt_get(b, 4, 4) != 1 || debt_get(b, 8, 4) != 384
			|| debt_get(b, 12, 4) != CLUSTER_RECOVERY_DEBT_PREPARED_WIRE_V1
			|| !debt_counter(bk[i].episode) || !debt_counter(bk[i].prepare)
			|| bk[i].episode != basis[i].episode || bk[i].prepare != basis[i].prepare
			|| bk[i].basis != cluster_recovery_registry_view_v3(bd)->available.coverage.control_generation
			|| bk[i].basis >= debt_get(p, 72, 8) || bk[i].prepare > debt_get(p, 72, 8)
			|| debt_get(b, 40, 8) || debt_get(b, 48, 8)
			|| count != basis[i].entry_count || bp != count / 64 + (count % 64 != 0)
			|| debt_get(b, 72, 8) != page_index || count > UINT64_MAX / 512
			|| debt_get(b, 80, 8) != count * 512
			|| memcmp(b + 184, basis[i].basis_ref, 128)
			|| !debt_sha_matches(b + 184, 128, b + 152) || !debt_zero(b + 312, 68)
			|| debt_get(b, 380, 4) != debt_crc(b, 380)
			|| !debt_sha_matches(p + ref_at + (Size)page_index * 128, (Size)bp * 128, b + 120))
			goto done;
		memcpy(basis_hash, b + 152, 32); pg_sha256_init(&entries_sha);
		for (uint64 j = 0; j < bp; j++, page_index++) {
			const uint8 *q = pages + (Size)page_index * 33024;
			const uint8 *r = p + ref_at + (Size)page_index * 128;
			uint64 used = Min(UINT64_C(64), count - j * 64);
			if (debt_get(r, 0, 4) != CLUSTER_RECOVERY_DEBT_PAGE_REF_FORMAT_ID_V1
				|| debt_get(r, 4, 4) != 1 || debt_get(r, 8, 4) != 128 || !debt_zero(r + 12, 4)
				|| debt_get(r, 16, 8) != bk[i].episode || debt_get(r, 24, 8) != bk[i].basis
				|| debt_get(r, 32, 8) != bk[i].prepare || debt_get(r, 40, 8) != j
				|| debt_get(r, 48, 8) != j * 64 || debt_get(r, 56, 4) != used
				|| debt_get(r, 60, 4) != 33024 || !debt_zero(r + 96, 28)
				|| debt_get(r, 124, 4) != debt_crc(r, 124) || !debt_sha_matches(q, 33024, r + 64)
				|| debt_get(q, 0, 4) != CLUSTER_RECOVERY_DEBT_PAGE_FORMAT_ID_V1
				|| debt_get(q, 4, 4) != 1 || debt_get(q, 8, 4) != 256
				|| debt_get(q, 12, 4) != 33024 || memcmp(q + 16, r + 16, 40)
				|| debt_get(q, 56, 8) != count || debt_get(q, 64, 8) != bp
				|| debt_get(q, 72, 4) != used || debt_get(q, 76, 4) != 512
				|| debt_get(q, 80, 4) != 32768 || !debt_zero(q + 84, 12)
				|| memcmp(q + 96, b + 88, 32) || memcmp(q + 128, basis_hash, 32)
				|| debt_get(q, 160, 4) != debt_crc(q + 256, 32768)
				|| debt_get(q, 164, 4) != debt_header_crc(q, 256, 164)
				|| !debt_zero(q + 168, 88) || !debt_zero(q + 256 + used * 512, (64 - used) * 512))
				goto done;
			for (uint64 k = 0; k < used; k++) {
				const uint8 *entry = q + 256 + k * 512;
				if (!debt_entry_valid(entry, j * 64 + k, cluster_recovery_registry_view_v3(bd)))
					goto done;
				memcpy(keys[seen].bytes, entry + 8, 264);
				memcpy(keys[seen++].bytes + 264, entry + 352, 72);
			}
			pg_sha256_update(&entries_sha, q + 256, (Size)used * 512);
		}
		pg_sha256_final(&entries_sha, hash);
		if (memcmp(hash, b + 88, 32)) goto done;
		cluster_recovery_registry_destroy_v3(&bd);
	}
	if (seen != ne || page_index != pn / 33024) goto done;
	qsort(keys, (Size)ne, sizeof(*keys), debt_key_compare);
	qsort(bk, (Size)nb, sizeof(*bk), debt_batch_compare);
	for (uint64 i = 1; i < ne; i++) if (!debt_key_compare(keys + i - 1, keys + i)) goto done;
	for (uint64 i = 1; i < nb; i++) if (!debt_batch_compare(bk + i - 1, bk + i)) goto done;
	ok = true;
done:
	free(keys); free(bk);
	cluster_recovery_registry_destroy_v3(&bd); cluster_recovery_registry_destroy_v3(&old);
	return ok;
}
static inline bool
control_debt_wire_encode_v1(const uint8 *embedded, Size embedded_n, const uint8 oldref[128],
	uint64 control, uint64 registration, const ControlDebtWireBatchV1 *batches,
	uint64 nb, Size budget, ControlDebtWireImageV1 **out)
{
	ControlDebtWireImageV1 *w = NULL;
	ClusterRecoveryRegistryDecodedV3 *decoded = NULL;
	const ClusterRecoveryRegistryV2 *r;
	Size n, pn, batch_at, ref_at;
	uint64 ne, page_index = 0;
	bool ok = false;
	if (!out || !embedded || !oldref || !debt_counter(control) || !debt_counter(registration)
		|| debt_overlap(embedded, embedded_n, out, sizeof(*out))
		|| debt_overlap(oldref, 128, out, sizeof(*out))
		|| !debt_dimensions(embedded_n, batches, nb, budget, &n, &pn, &ne)
		|| debt_overlap(batches, (Size)nb * sizeof(*batches), out, sizeof(*out)))
		return false;
	for (uint64 i = 0; i < nb; i++)
		if (!batches[i].entries
			|| debt_overlap(batches[i].basis, batches[i].basis_bytes, out, sizeof(*out))
			|| debt_overlap(batches[i].entries, (Size)batches[i].entry_count * sizeof(*batches[i].entries),
				out, sizeof(*out))) return false;
	if (*out || !debt_old_open(embedded, embedded_n, oldref, budget, &decoded)) return false;
	r = &cluster_recovery_registry_view_v3(decoded)->available;
	w = calloc(1, sizeof(*w));
	if (!w) goto done;
	w->registry_bytes = n; w->page_bytes = pn; w->batch_count = nb;
	w->registry = calloc(1, n); w->pages = calloc(1, pn);
	if (!w->registry || !w->pages) goto done;
	batch_at = 256 + embedded_n; ref_at = batch_at + (Size)nb * 384;
	debt_put(w->registry, 0, CLUSTER_RECOVERY_DEBT_REGISTRY_FORMAT_ID_V1, 4);
	debt_put(w->registry, 4, 1, 4); debt_put(w->registry, 8, 256, 4);
	debt_put(w->registry, 12, CATALOG_VERSION_NO, 4);
	debt_put(w->registry, 16, r->system_identifier, 8);
	debt_put(w->registry, 24, r->database_incarnation, 8);
	memcpy(w->registry + 32, r->storage_uuid, 16); memcpy(w->registry + 48, r->redo_branch_uuid, 16);
	debt_put(w->registry, 64, r->redo_branch_generation, 8);
	debt_put(w->registry, 72, control, 8); debt_put(w->registry, 80, registration, 8);
	debt_put(w->registry, 88, debt_get(embedded, 8, 4), 4);
	debt_put(w->registry, 92, debt_get(embedded, 12, 4), 4);
	debt_put(w->registry, 96, 256, 8); debt_put(w->registry, 104, embedded_n, 8);
	debt_put(w->registry, 112, batch_at, 8); debt_put(w->registry, 120, nb, 8);
	debt_put(w->registry, 128, 384, 4); debt_put(w->registry, 132, 128, 4);
	debt_put(w->registry, 136, ref_at, 8); debt_put(w->registry, 144, pn / 33024, 8);
	debt_put(w->registry, 152, ne, 8); debt_put(w->registry, 160, n, 8);
	memcpy(w->registry + 256, embedded, embedded_n);
	for (uint64 i = 0; i < nb; i++) {
		const ControlDebtWireBatchV1 *input = &batches[i];
		uint8 *b = w->registry + batch_at + (Size)i * 384;
		uint64 count = input->entry_count, bp = count / 64 + (count % 64 != 0);
		pg_sha256_ctx ctx;
		uint8 digest[32], basis_sha[32];
		debt_sha(input->basis_ref, 128, basis_sha); pg_sha256_init(&ctx);
		debt_put(b, 0, CLUSTER_RECOVERY_DEBT_BATCH_FORMAT_ID_V1, 4);
		debt_put(b, 4, 1, 4); debt_put(b, 8, 384, 4);
		debt_put(b, 12, CLUSTER_RECOVERY_DEBT_PREPARED_WIRE_V1, 4);
		debt_put(b, 16, input->episode, 8); debt_put(b, 24, debt_get(input->basis_ref, 16, 8), 8);
		debt_put(b, 32, input->prepare, 8); debt_put(b, 56, count, 8);
		debt_put(b, 64, bp, 8); debt_put(b, 72, page_index, 8);
		debt_put(b, 80, count * 512, 8); memcpy(b + 152, basis_sha, 32);
		memcpy(b + 184, input->basis_ref, 128);
		for (uint64 j = 0; j < bp; j++) {
			uint8 *q = w->pages + (Size)(page_index + j) * 33024;
			uint64 used = Min(UINT64_C(64), count - j * 64);
			for (uint64 k = 0; k < used; k++)
				if (!debt_entry_encode(input->entries + j * 64 + k, q + 256 + k * 512)) goto done;
			pg_sha256_update(&ctx, q + 256, (Size)used * 512);
		}
		pg_sha256_final(&ctx, digest); memcpy(b + 88, digest, 32);
		for (uint64 j = 0; j < bp; j++, page_index++) {
			uint8 *q = w->pages + (Size)page_index * 33024;
			uint8 *pr = w->registry + ref_at + (Size)page_index * 128;
			uint64 used = Min(UINT64_C(64), count - j * 64);
			debt_put(q, 0, CLUSTER_RECOVERY_DEBT_PAGE_FORMAT_ID_V1, 4);
			debt_put(q, 4, 1, 4); debt_put(q, 8, 256, 4); debt_put(q, 12, 33024, 4);
			memcpy(q + 16, b + 16, 24); debt_put(q, 40, j, 8); debt_put(q, 48, j * 64, 8);
			debt_put(q, 56, count, 8); debt_put(q, 64, bp, 8);
			debt_put(q, 72, used, 4); debt_put(q, 76, 512, 4); debt_put(q, 80, 32768, 4);
			memcpy(q + 96, digest, 32); memcpy(q + 128, basis_sha, 32);
			debt_put(q, 160, debt_crc(q + 256, 32768), 4);
			debt_put(q, 164, debt_header_crc(q, 256, 164), 4);
			debt_put(pr, 0, CLUSTER_RECOVERY_DEBT_PAGE_REF_FORMAT_ID_V1, 4);
			debt_put(pr, 4, 1, 4); debt_put(pr, 8, 128, 4);
			memcpy(pr + 16, q + 16, 40); debt_put(pr, 56, used, 4);
			debt_put(pr, 60, 33024, 4); debt_sha(q, 33024, pr + 64);
			debt_put(pr, 124, debt_crc(pr, 124), 4);
		}
		debt_sha(w->registry + ref_at + (Size)(page_index - bp) * 128, (Size)bp * 128, b + 120);
		debt_put(b, 380, debt_crc(b, 380), 4);
	}
	debt_sha(embedded, embedded_n, w->registry + 168);
	debt_sha(w->registry + batch_at, n - batch_at, w->registry + 200);
	debt_put(w->registry, 232, debt_crc(w->registry + 256, n - 256), 4);
	debt_put(w->registry, 236, debt_header_crc(w->registry, 256, 236), 4);
	debt_root_reference(w->registry, n, w->reference);
	ok = control_debt_wire_validate_v1(w->registry, n, w->reference, w->pages, pn, batches, nb, budget);
	if (ok) { *out = w; w = NULL; }
done:
	control_debt_wire_destroy_v1(&w); cluster_recovery_registry_destroy_v3(&decoded);
	return ok;
}
