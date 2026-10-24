/* Native issuance-window codec. The original ROOT-selected anchor supplies
 * identity, retention and persistence; these bytes never grant authority. */
#include "postgres.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "common/cryptohash.h"
#include "port/pg_crc32c.h"
StaticAssertDecl(sizeof(CheckPoint) == 88, "native window checkpoint body");
static bool
overlaps(const void *a, size_t na, const void *b, size_t nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && (x <= y ? y - x < na : x - y < nb);
}
static uint64
get(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	for (unsigned i = 0; i < n; i++)
		v |= (uint64)p[i] << (8 * i);
	return v;
}
static void
put(uint8 *p, uint64 v, unsigned n)
{
	for (unsigned i = 0; i < n; i++)
		p[i] = (uint8)(v >> (8 * i));
}
static bool
zero(const uint8 *p, size_t n)
{
	for (size_t i = 0; i < n; i++)
		if (p[i])
			return false;
	return true;
}
static bool
digest(const CheckPoint *cp, uint8 out[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (!ctx)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0
		 && pg_cryptohash_update(ctx, (const uint8 *)cp, sizeof(*cp)) >= 0
		 && pg_cryptohash_final(ctx, out, 32) >= 0;
	pg_cryptohash_free(ctx);
	return ok;
}
static bool
valid(const ClusterRelmapCheckpointWindow *w, const CheckPoint *cp, XLogRecPtr start,
	  XLogRecPtr end)
{
	return w && cp && w->system_identifier && w->database_incarnation && w->epoch
		   && w->admitted_incarnation && w->boot_incarnation && w->origin < 64
		   && TransactionIdIsNormal((TransactionId)w->native_floor)
		   && TransactionIdIsNormal((TransactionId)w->last_issued)
		   && TransactionIdIsNormal((TransactionId)w->allocation_next)
		   && w->native_floor <= w->last_issued
		   && w->last_issued - w->native_floor < UINT64CONST(0x80000000)
		   && w->last_issued < w->allocation_next
		   && w->allocation_next <= U64FromFullTransactionId(cp->nextXid) && cp->redo
		   && cp->redo <= start && start < end && cp->ThisTimeLineID && cp->PrevTimeLineID
		   && cp->PrevTimeLineID <= cp->ThisTimeLineID;
}
static bool
encode_window(const ClusterRelmapCheckpointWindow *w, const CheckPoint *cp, XLogRecPtr start,
			  XLogRecPtr end, uint64 post_issued, unsigned version, uint8 out[160])
{
	uint8 b[160] = { 0 };
	pg_crc32c crc;
	if (!out || overlaps(out, 160, w, sizeof(*w)) || overlaps(out, 160, cp, sizeof(*cp)))
		return false;
	memset(out, 0, 160);
	if (!valid(w, cp, start, end)
		|| (version == 2
			&& (!TransactionIdIsNormal((TransactionId)post_issued) || post_issued < w->last_issued
				|| post_issued - w->native_floor >= UINT64CONST(0x80000000))))
		return false;
	memcpy(b, version == 2 ? "RMWIN002" : "RMWIN001", 8);
	put(b + 8, version, 2);
	if (version == 2)
		put(b + 140, post_issued, 8);
	put(b + 10, 160, 2);
	put(b + 12, w->origin, 2);
	put(b + 16, w->system_identifier, 8);
	put(b + 24, w->database_incarnation, 8);
	put(b + 32, w->epoch, 8);
	put(b + 40, w->admitted_incarnation, 8);
	put(b + 48, w->boot_incarnation, 8);
	put(b + 56, w->native_floor, 8);
	put(b + 64, w->last_issued, 8);
	put(b + 72, w->allocation_next, 8);
	put(b + 80, start, 8);
	put(b + 88, end, 8);
	put(b + 96, cp->ThisTimeLineID, 4);
	if (!digest(cp, b + 104))
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, b, sizeof(b));
	FIN_CRC32C(crc);
	put(b + 136, crc, 4);
	memcpy(out, b, 160);
	return true;
}
bool
cluster_relmap_checkpoint_window_encode_v1(const ClusterRelmapCheckpointWindow *w,
										   const CheckPoint *cp, XLogRecPtr start, XLogRecPtr end,
										   uint8 out[160])
{
	return encode_window(w, cp, start, end, 0, 1, out);
}
bool
cluster_relmap_checkpoint_window_encode_v2(const ClusterRelmapCheckpointWindow *w,
										   const CheckPoint *cp, XLogRecPtr start, XLogRecPtr end,
										   uint64 post_issued, uint8 out[160])
{
	return encode_window(w, cp, start, end, post_issued, 2, out);
}
static bool
decode_window(const uint8 b[160], const CheckPoint *cp, XLogRecPtr start, XLogRecPtr end,
			  const ClusterControlRootIdentity *id, uint64 dbinc,
			  ClusterRelmapCheckpointWindow *out, uint64 *post_out, bool require_v2)
{
	uint8 copy[160], hash[32];
	ClusterRelmapCheckpointWindow w = { 0 };
	pg_crc32c crc;
	unsigned version;
	uint64 post_issued = 0;
	if (overlaps(out, sizeof(*out), b, 160) || overlaps(out, sizeof(*out), cp, sizeof(*cp))
		|| overlaps(out, sizeof(*out), id, sizeof(*id))
		|| overlaps(post_out, sizeof(*post_out), b, 160)
		|| overlaps(post_out, sizeof(*post_out), cp, sizeof(*cp))
		|| overlaps(post_out, sizeof(*post_out), id, sizeof(*id))
		|| overlaps(out, sizeof(*out), post_out, sizeof(*post_out)))
		return false;
	if (out)
		memset(out, 0, sizeof(*out));
	if (post_out)
		*post_out = 0;
	if (!out || (require_v2 && !post_out) || !b || !cp || !id)
		return false;
	version = get(b + 8, 2);
	if ((version != 1 && version != 2) || (require_v2 && version != 2)
		|| memcmp(b, version == 2 ? "RMWIN002" : "RMWIN001", 8) || get(b + 10, 2) != 160
		|| !zero(b + 14, 2) || !zero(b + 100, 4)
		|| (version == 1 ? !zero(b + 140, 20) : !zero(b + 148, 12)))
		return false;
	if (version == 2)
		post_issued = get(b + 140, 8);
	memcpy(copy, b, 160);
	memset(copy + 136, 0, 4);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, copy, sizeof(copy));
	FIN_CRC32C(crc);
	if ((uint32)crc != get(b + 136, 4))
		return false;
	w.origin = get(b + 12, 2);
	w.system_identifier = get(b + 16, 8);
	w.database_incarnation = get(b + 24, 8);
	w.epoch = get(b + 32, 8);
	w.admitted_incarnation = get(b + 40, 8);
	w.boot_incarnation = get(b + 48, 8);
	w.native_floor = get(b + 56, 8);
	w.last_issued = get(b + 64, 8);
	w.allocation_next = get(b + 72, 8);
	if (id->origin_node_id < 0 || w.origin != id->origin_node_id
		|| w.system_identifier != id->system_identifier || w.database_incarnation != dbinc
		|| w.admitted_incarnation != id->origin_owner_incarnation || start != get(b + 80, 8)
		|| end != get(b + 88, 8) || cp->ThisTimeLineID != get(b + 96, 4)
		|| !valid(&w, cp, start, get(b + 88, 8))
		|| (version == 2
			&& (!TransactionIdIsNormal((TransactionId)post_issued) || post_issued < w.last_issued
				|| post_issued - w.native_floor >= UINT64CONST(0x80000000)))
		|| !digest(cp, hash) || memcmp(hash, b + 104, 32))
		return false;
	*out = w;
	if (post_out)
		*post_out = post_issued;
	return true;
}
bool
cluster_relmap_checkpoint_window_decode_v1(const uint8 b[160], const CheckPoint *cp,
										   XLogRecPtr start, XLogRecPtr end,
										   const ClusterControlRootIdentity *id, uint64 dbinc,
										   ClusterRelmapCheckpointWindow *out)
{
	return decode_window(b, cp, start, end, id, dbinc, out, NULL, false);
}
bool
cluster_relmap_checkpoint_window_decode_v2(const uint8 b[160], const CheckPoint *cp,
										   XLogRecPtr start, XLogRecPtr end,
										   const ClusterControlRootIdentity *id, uint64 dbinc,
										   ClusterRelmapCheckpointWindow *out, uint64 *post_out)
{
	return decode_window(b, cp, start, end, id, dbinc, out, post_out, true);
}
