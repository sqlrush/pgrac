/* Explicit complete registry bytes; never native struct overlays or authority. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif
#include "common/cluster_recovery_registry.h"
#include "sha2_int.h"
#include "port/pg_crc32c.h"

static bool overlap(const void *a, Size na, const void *b, Size nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a && b && na && nb && (x <= y ? y - x < na : x - y < nb);
}
static bool nonzero(const uint8 *p, Size n)
{
	for (Size i = 0; i < n; i++)
		if (p[i])
			return true;
	return false;
}
static bool counter(uint64 n)
{
	return n && n != UINT64_MAX;
}
static void put(uint8 *p, Size o, uint64 n, unsigned w)
{
	for (unsigned i = 0; i < w; i++)
		p[o + i] = (uint8)(n >> (8 * (w - i - 1)));
}
static uint64 get(const uint8 *p, Size o, unsigned w)
{
	uint64 n = 0;
	for (unsigned i = 0; i < w; i++)
		n = (n << 8) | p[o + i];
	return n;
}
static uint32 crc(const uint8 *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, n);
	FIN_CRC32C(c);
	return c;
}
static bool sha(const uint8 *p, Size n, uint8 out[32])
{
	pg_sha256_ctx ctx;
	pg_sha256_init(&ctx);
	pg_sha256_update(&ctx, p, n);
	pg_sha256_final(&ctx, out);
	return true;
}
static bool extend(Size *n, uint64 count, Size width, Size budget)
{
	if (*n > budget || count > (budget - *n) / width)
		return false;
	*n += (Size)count * width;
	return true;
}
static bool dimensions(const ClusterRecoveryRegistryV2 *r, Size budget, Size *n)
{
	Size size = 256;
	const ClusterRecoveryCheckpointCoverageV2 *c;
	if (!r || !(c = &r->coverage)->sources || !c->files || !c->source_count || !c->file_count ||
		c->source_count > SIZE_MAX / sizeof(*c->sources) ||
		c->file_count > SIZE_MAX / sizeof(*c->files) ||
		!extend(&size, c->source_count, 288, budget) || !extend(&size, c->file_count, 208, budget))
		return false;
	for (uint64 i = 0; i < c->file_count; i++)
		if (!c->files[i].sources || !c->files[i].source_count ||
			c->files[i].source_count > c->source_count ||
			c->files[i].source_count > SIZE_MAX / sizeof(*c->files[i].sources) ||
			!extend(&size, c->files[i].source_count, 24, budget))
			return false;
	*n = size;
	return true;
}
static void source_bytes(const ClusterRecoveryCheckpointSourceV2 *s, uint8 p[288])
{
	const ClusterControlRootIdentity *id = &s->source.claim.identity;
	const CheckPoint *cp = &s->checkpoint;
	memset(p, 0, 288);
	put(p, 0, id->system_identifier, 8);
	memcpy(p + 8, id->storage_uuid, 16);
	memcpy(p + 24, id->authority_uuid, 16);
	put(p, 40, id->origin_thread_id, 2);
	put(p, 44, id->origin_node_id, 4);
	put(p, 48, id->thread_claim_created_at, 8);
	put(p, 56, id->thread_claim_crc32c, 4);
	put(p, 64, id->origin_owner_incarnation, 8);
	put(p, 72, id->root_lineage_seq, 8);
	put(p, 80, s->source.claim.database_incarnation, 8);
	put(p, 88, s->source.claim.max_config_generation, 8);
	memcpy(p + 96, s->source.claim.claim_sha256, 32);
	put(p, 128, s->source.timeline, 4);
	put(p, 136, cp->redo, 8);
	put(p, 144, cp->ThisTimeLineID, 4);
	put(p, 148, cp->PrevTimeLineID, 4);
	p[152] = cp->fullPageWrites ? 1 : 0;
	put(p, 160, U64FromFullTransactionId(cp->nextXid), 8);
	put(p, 168, cp->nextOid, 4);
	put(p, 172, cp->nextMulti, 4);
	put(p, 176, cp->nextMultiOffset, 4);
	put(p, 180, cp->oldestXid, 4);
	put(p, 184, cp->oldestXidDB, 4);
	put(p, 188, cp->oldestMulti, 4);
	put(p, 192, cp->oldestMultiDB, 4);
	put(p, 200, (uint64)cp->time, 8);
	put(p, 208, cp->oldestCommitTsXid, 4);
	put(p, 212, cp->newestCommitTsXid, 4);
	put(p, 216, cp->oldestActiveXid, 4);
	put(p, 224, s->checkpoint_start, 8);
	put(p, 232, s->checkpoint_end, 8);
	put(p, 240, s->redo_start, 8);
	put(p, 248, s->replay_end, 8);
	put(p, 256, U64FromFullTransactionId(s->native_floor), 8);
	put(p, 264, U64FromFullTransactionId(s->post_flush_issued), 8);
	put(p, 272, U64FromFullTransactionId(s->next_full_xid), 8);
	put(p, 280, s->oldest_clog_xid, 4);
	p[284] = s->local_bounds_observed ? 1 : 0;
}
/* Bounds and complete-key checks precede allocation/output or any I/O. */
/* File birth registration stays immutable across later complete registries. */
static bool valid(const ClusterRecoveryRegistryV2 *r, Size n)
{
	const ClusterRecoveryCheckpointCoverageV2 *c = &r->coverage;
	uint8 a[288], b[288], id[128], prior[128];
	Size actual;
	if (!dimensions(r, n, &actual) || actual != n || !counter(r->system_identifier) ||
		!counter(r->database_incarnation) || !counter(r->redo_branch_generation) ||
		!nonzero(r->storage_uuid, 16) || !nonzero(r->redo_branch_uuid, 16) ||
		!counter(c->control_generation) || !counter(c->registration_generation) ||
		!counter(c->checkpoint_generation) || !counter(c->commit_generation))
		return false;
	for (uint64 i = 0; i < c->source_count; i++)
	{
		const ClusterRecoveryCheckpointSourceV2 *s = &c->sources[i];
		const ClusterControlRootIdentity *key = &s->source.claim.identity;
		if (key->system_identifier != r->system_identifier ||
			memcmp(key->storage_uuid, r->storage_uuid, 16) ||
			s->source.claim.database_incarnation != r->database_incarnation ||
			key->origin_node_id < 0 || key->origin_node_id >= 128 ||
			key->origin_thread_id != key->origin_node_id + 1 || key->reserved42 ||
			key->reserved60 || !counter(key->origin_owner_incarnation) ||
			!counter(key->root_lineage_seq) || key->thread_claim_created_at == 0 ||
			!nonzero(key->authority_uuid, 16) || !counter(s->source.claim.max_config_generation) ||
			!nonzero(s->source.claim.claim_sha256, 32) || !s->source.timeline ||
			s->checkpoint_start >= s->checkpoint_end || s->redo_start > s->replay_end ||
			s->checkpoint_end > s->replay_end ||
			(!s->local_bounds_observed &&
			 (U64FromFullTransactionId(s->next_full_xid) || s->oldest_clog_xid)))
			return false;
		source_bytes(s, a);
		for (uint64 j = 0; j < i; j++)
		{
			source_bytes(&c->sources[j], b);
			if (!memcmp(a, b, 136))
				return false;
		}
	}
	for (uint64 i = 0; i < c->file_count; i++)
	{
		const ClusterRecoveryCheckpointFileV2 *f = &c->files[i];
		uint64 copies = 0;
		if (!cluster_recovery_file_identity_encode_v2(&f->identity, id, 128) ||
			f->identity.system_identifier != r->system_identifier ||
			f->identity.database_incarnation != r->database_incarnation ||
			memcmp(f->identity.storage_uuid, r->storage_uuid, 16) ||
			memcmp(f->identity.redo_branch_uuid, r->redo_branch_uuid, 16) ||
			f->identity.redo_branch_generation != r->redo_branch_generation ||
			f->identity.registration_generation > c->registration_generation || !f->synced_bytes ||
			!f->page_count || f->first_page > UINT64_MAX - f->page_count || !f->copy_count ||
			f->copy_index >= f->copy_count || !nonzero(f->image_digest, 32) ||
			f->source_count > c->source_count)
			return false;
		for (uint64 j = 0; j < c->file_count; j++)
		{
			if (!cluster_recovery_file_identity_encode_v2(&c->files[j].identity, prior, 128))
				return false;
			if (!memcmp(id, prior, 128))
			{
				if (c->files[j].copy_count != f->copy_count ||
					(i != j && c->files[j].copy_index == f->copy_index))
					return false;
				copies++;
			}
		}
		if (copies != f->copy_count)
			return false;
		for (uint64 j = 0; j < f->source_count; j++)
		{
			const ClusterRecoveryCheckpointFileSourceV2 *v = &f->sources[j];
			const ClusterRecoveryCheckpointSourceV2 *s;
			if (v->source_index >= c->source_count)
				return false;
			s = &c->sources[v->source_index];
			if (v->redo_start > v->replay_end || v->redo_start < s->redo_start ||
				v->replay_end > s->replay_end)
				return false;
			for (uint64 k = 0; k < j; k++)
				if (f->sources[k].source_index == v->source_index)
					return false;
		}
	}
	return true;
}
static bool aliases(const ClusterRecoveryRegistryV2 *r, const void *out, Size n)
{
	const ClusterRecoveryCheckpointCoverageV2 *c = &r->coverage;
	if (overlap(r, sizeof(*r), out, n) ||
		overlap(c->sources, c->source_count * sizeof(*c->sources), out, n) ||
		overlap(c->files, c->file_count * sizeof(*c->files), out, n))
		return true;
	for (uint64 i = 0; i < c->file_count; i++)
		if (overlap(c->files[i].sources, c->files[i].source_count * sizeof(*c->files[i].sources),
					out, n))
			return true;
	return false;
}
bool cluster_recovery_registry_size_v2(const ClusterRecoveryRegistryV2 *r, Size budget, Size *n)
{
	Size size;
	if (!n || !dimensions(r, budget, &size) || aliases(r, n, sizeof(*n)) || !valid(r, size))
		return false;
	*n = size;
	return true;
}
static bool build(const ClusterRecoveryRegistryV2 *r, uint8 *p, Size n, uint8 ref[128])
{
	const ClusterRecoveryCheckpointCoverageV2 *c = &r->coverage;
	Size file = 256 + c->source_count * 288, interval = file + c->file_count * 208, at = interval;
	uint64 first = 0;
	memset(p, 0, n);
	memset(ref, 0, 128);
	memcpy(p, "PRCREG2\0", 8);
	put(p, 8, CLUSTER_RECOVERY_FORMAT_ID_V2, 4);
	put(p, 12, 2, 4);
	put(p, 16, 256, 4);
	put(p, 24, r->system_identifier, 8);
	put(p, 32, r->database_incarnation, 8);
	memcpy(p + 40, r->storage_uuid, 16);
	memcpy(p + 56, r->redo_branch_uuid, 16);
	put(p, 72, r->redo_branch_generation, 8);
	put(p, 80, c->control_generation, 8);
	put(p, 88, c->registration_generation, 8);
	put(p, 96, c->checkpoint_generation, 8);
	put(p, 104, c->commit_generation, 8);
	put(p, 112, c->source_count, 8);
	put(p, 120, c->file_count, 8);
	put(p, 128, (n - interval) / 24, 8);
	put(p, 136, 256, 8);
	put(p, 144, file, 8);
	put(p, 152, interval, 8);
	put(p, 160, n, 8);
	for (uint64 i = 0; i < c->source_count; i++)
		source_bytes(&c->sources[i], p + 256 + i * 288);
	for (uint64 i = 0; i < c->file_count; i++)
	{
		const ClusterRecoveryCheckpointFileV2 *f = &c->files[i];
		uint8 *q = p + file + i * 208;
		if (!cluster_recovery_file_identity_encode_v2(&f->identity, q, 128))
			return false;
		put(q, 128, f->synced_bytes, 8);
		put(q, 136, f->first_page, 8);
		put(q, 144, f->page_count, 8);
		put(q, 152, f->copy_index, 4);
		put(q, 156, f->copy_count, 4);
		memcpy(q + 160, f->image_digest, 32);
		put(q, 192, f->source_count, 8);
		put(q, 200, first, 8);
		first += f->source_count;
		for (uint64 j = 0; j < f->source_count; j++, at += 24)
		{
			put(p, at, f->sources[j].source_index, 8);
			put(p, at + 8, f->sources[j].redo_start, 8);
			put(p, at + 16, f->sources[j].replay_end, 8);
		}
	}
	if (!sha(p + 256, file - 256, p + 168) || !sha(p + file, n - file, p + 200))
		return false;
	put(p, 232, crc(p + 256, n - 256), 4);
	put(p, 236, crc(p, 256), 4);
	memcpy(ref, "PRCREF2\0", 8);
	put(ref, 8, CLUSTER_RECOVERY_FORMAT_ID_V2, 4);
	put(ref, 12, 2, 4);
	memcpy(ref + 16, p + 80, 32);
	put(ref, 48, n, 8);
	put(ref, 56, c->source_count, 8);
	put(ref, 64, c->file_count, 8);
	if (!sha(p, n, ref + 72))
		return false;
	put(ref, 104, crc(ref, 128), 4);
	return true;
}
bool cluster_recovery_registry_encode_v2(const ClusterRecoveryRegistryV2 *r, uint8 *out, Size n,
										 uint8 ref[128])
{
	uint8 *tmp, reference[128];
	bool ok;
	if (!out || !ref || !r || !valid(r, n) || aliases(r, out, n) || aliases(r, ref, 128) ||
		overlap(out, n, ref, 128))
		return false;
	tmp = malloc(n);
	if (!tmp)
		return false;
	ok = build(r, tmp, n, reference);
	if (ok)
	{
		memcpy(out, tmp, n);
		memcpy(ref, reference, 128);
	}
	free(tmp);
	return ok;
}
bool cluster_recovery_registry_reference_valid_v2(const uint8 ref[128])
{
	uint8 tmp[128];
	Size minimum = 256;
	uint64 size;
	if (!ref || memcmp(ref, "PRCREF2\0", 8) || get(ref, 8, 4) != CLUSTER_RECOVERY_FORMAT_ID_V2 ||
		get(ref, 12, 4) != 2 || nonzero(ref + 108, 20) || !nonzero(ref + 72, 32))
		return false;
	for (int i = 16; i < 48; i += 8)
		if (!counter(get(ref, i, 8)))
			return false;
	size = get(ref, 48, 8);
	if (size > SIZE_MAX || !get(ref, 56, 8) || !get(ref, 64, 8) ||
		!extend(&minimum, get(ref, 56, 8), 288, (Size)size) ||
		!extend(&minimum, get(ref, 64, 8), 232, (Size)size))
		return false;
	memcpy(tmp, ref, 128);
	memset(tmp + 104, 0, 4);
	return get(ref, 104, 4) == crc(tmp, 128);
}
bool cluster_recovery_registry_matches_v2(const uint8 *p, Size n,
										  const ClusterRecoveryRegistryV2 *r, const uint8 ref[128])
{
	uint8 *expected, reference[128];
	bool ok;
	if (!p || !r || !cluster_recovery_registry_reference_valid_v2(ref) || !valid(r, n) ||
		aliases(r, p, n) || aliases(r, ref, 128) || overlap(p, n, ref, 128))
		return false;
	expected = malloc(n);
	if (!expected)
		return false;
	ok =
		build(r, expected, n, reference) && !memcmp(p, expected, n) && !memcmp(ref, reference, 128);
	free(expected);
	return ok;
}

#ifdef WIN32
bool cluster_recovery_registry_create_v2(int d, const ClusterRecoveryRegistryV2 *r, Size b,
										 uint8 ref[128])
{
	return false;
}
bool cluster_recovery_registry_read_v2(int d, const ClusterRecoveryRegistryV2 *r,
									   const uint8 ref[128], Size b)
{
	return false;
}
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#ifndef FRONTEND
#include "miscadmin.h"
#include "storage/fd.h"
#endif
static int registry_sync(int fd)
{
#ifdef FRONTEND
	return fsync(fd);
#else
	return enableFsync ? pg_fsync(fd) : -1;
#endif
}
static bool directory_safe(int fd)
{
	struct stat st;
	return fstat(fd, &st) == 0 && S_ISDIR(st.st_mode) && st.st_uid == geteuid() &&
		   !(st.st_mode & 0022);
}
static bool same_stat(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino && a->st_mode == b->st_mode &&
		   a->st_nlink == b->st_nlink && a->st_uid == b->st_uid && a->st_size == b->st_size &&
		   a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}
static bool current_file(int dir, const char *name, int fd, Size n)
{
	struct stat a, b;
	return fstat(fd, &a) == 0 && fstatat(dir, name, &b, AT_SYMLINK_NOFOLLOW) == 0 &&
		   S_ISREG(a.st_mode) && a.st_nlink == 1 && a.st_uid == geteuid() && !(a.st_mode & 0022) &&
		   a.st_size >= 0 && (uint64)a.st_size == n && same_stat(&a, &b);
}
static bool current_directory(int global, int dir)
{
	struct stat a, b;
	return directory_safe(global) && directory_safe(dir) && fstat(dir, &a) == 0 &&
		   fstatat(global, "pgrac_recovery_registry", &b, AT_SYMLINK_NOFOLLOW) == 0 &&
		   same_stat(&a, &b);
}
static void registry_name(const uint8 ref[128], char name[56])
{
	snprintf(name, 56, "%016llx-%016llx-%016llx.v2", (unsigned long long)get(ref, 24, 8),
			 (unsigned long long)get(ref, 32, 8), (unsigned long long)get(ref, 40, 8));
}
static bool exact_io(int fd, uint8 *bytes, Size n, bool writing)
{
	Size done = 0;
	while (done < n)
	{
		Size amount = Min(n - done, (Size)1024 * 1024);
		ssize_t moved = writing ? pwrite(fd, bytes + done, amount, (off_t)done)
								: pread(fd, bytes + done, amount, (off_t)done);
		if (moved < 0 && errno == EINTR)
			continue;
		if (moved <= 0)
			return false;
		done += (Size)moved;
	}
	return true;
}
static bool registry_io(int global, const ClusterRecoveryRegistryV2 *r, Size budget,
						const uint8 *selected, uint8 *output, bool writing)
{
	int dir = -1, fd = -1;
	Size n;
	uint8 *bytes = NULL, *readback = NULL, ref[128];
	char name[56];
	bool ok = false;
	struct stat before, after;
	if (!r || !cluster_recovery_registry_size_v2(r, budget, &n) || n > PG_INT64_MAX ||
		(writing && (!output || aliases(r, output, 128))) ||
		(!writing && (!selected || aliases(r, selected, 128))) || !directory_safe(global))
		return false;
	bytes = malloc(n);
	readback = malloc(n);
	if (!bytes || !readback || !build(r, bytes, n, ref) || (!writing && memcmp(ref, selected, 128)))
		goto done;
	if (writing && mkdirat(global, "pgrac_recovery_registry", 0700) != 0 && errno != EEXIST)
		goto done;
	dir =
		openat(global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (dir < 0 || !current_directory(global, dir))
		goto done;
	registry_name(ref, name);
	fd = openat(dir, name,
				(writing ? O_RDWR | O_CREAT | O_EXCL : O_RDONLY) | O_NOFOLLOW | O_NONBLOCK |
					O_CLOEXEC | PG_BINARY,
				0600);
	if (fd < 0)
		goto done;
	if (writing && (!exact_io(fd, bytes, n, true) || registry_sync(fd) != 0 ||
					registry_sync(dir) != 0 || registry_sync(global) != 0))
		goto done;
	if (!current_file(dir, name, fd, n) || fstat(fd, &before) != 0)
		goto done;
	/* Independent complete readback twice, retaining the same exact descriptor. */
	for (int pass = 0; pass < 2; pass++)
		if (!exact_io(fd, readback, n, false) || memcmp(readback, bytes, n) ||
			!current_file(dir, name, fd, n) || fstat(fd, &after) != 0 ||
			!same_stat(&before, &after))
			goto done;
	ok = current_directory(global, dir) &&
		 cluster_recovery_registry_matches_v2(readback, n, r, ref) &&
		 (!selected || !memcmp(selected, ref, 128));
done:
	if (fd >= 0 && close(fd) != 0)
		ok = false;
	if (ok)
	{
		struct stat named;
		ok = fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) == 0 && same_stat(&before, &named) &&
			 current_directory(global, dir) &&
			 cluster_recovery_registry_matches_v2(readback, n, r, ref);
	}
	if (dir >= 0 && close(dir) != 0)
		ok = false;
	if (ok && output)
		memcpy(output, ref, 128);
	free(readback);
	free(bytes);
	return ok;
}
bool cluster_recovery_registry_create_v2(int global, const ClusterRecoveryRegistryV2 *r,
										 Size budget, uint8 ref[128])
{
	return registry_io(global, r, budget, NULL, ref, true);
}
bool cluster_recovery_registry_read_v2(int global, const ClusterRecoveryRegistryV2 *r,
									   const uint8 ref[128], Size budget)
{
	return registry_io(global, r, budget, ref, NULL, false);
}
#endif
