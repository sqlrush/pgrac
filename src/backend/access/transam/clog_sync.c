/* Complete borrowed-file CLOG sync observations.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/clog_storage.h"
#include "common/cryptohash.h"
#include "miscadmin.h"
#include "storage/fd.h"

static bool
clog_sync_readback(int fd, const ClusterClogFileBindingV2 *binding,
				   uint32 pages, pg_cryptohash_ctx *hash,
				   ClusterClogFileSyncV2 *observation)
{
	uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES];
	uint8 frame[CLUSTER_CLOG_FRAME_BYTES];
	uint8 payload[CLUSTER_CLOG_PAGE_BYTES];
	ClusterClogPageIdentityV2 page;

	if (pg_cryptohash_init(hash) < 0 ||
		pg_pread(fd, header, sizeof(header), 0) != sizeof(header) ||
		!cluster_clog_file_header_matches_v2(header, sizeof(header), binding) ||
		pg_cryptohash_update(hash, header, sizeof(header)) < 0)
		return false;
	memset(observation, 0, sizeof(*observation));
	observation->binding = *binding;
	observation->synced_bytes = sizeof(header) + (uint64) pages * sizeof(frame);
	observation->extent_pages = pages;
	for (uint32 i = 0; i < pages; i++)
	{
		bool zero = true;
		uint64 offset = sizeof(header) + (uint64) i * sizeof(frame);

		if (pg_pread(fd, frame, sizeof(frame), (off_t) offset) != sizeof(frame) ||
			pg_cryptohash_update(hash, frame, sizeof(frame)) < 0)
			return false;
		for (Size j = 0; j < sizeof(frame); j++)
		{
			if (frame[j] != 0)
			{
				zero = false;
				break;
			}
		}
		/* Sparse slots are included in the byte digest, but do not become
		 * page births or provide a source-local WAL watermark. */
		if (zero)
			continue;
		if (!cluster_clog_file_page_decode_v2(binding,
				(uint64) binding->segment * CLUSTER_CLOG_PAGES_PER_FILE + i,
				frame, sizeof(frame), &page, payload, sizeof(payload)))
			return false;
		observation->materialized_pages |= UINT32_C(1) << i;
		observation->max_page_lsn = Max(observation->max_page_lsn, page.identity.page_lsn);
		observation->max_image_lsn = Max(observation->max_image_lsn, page.image_lsn);
	}
	return observation->materialized_pages != 0 &&
		pg_cryptohash_final(hash, observation->image_digest,
						   sizeof(observation->image_digest)) == 0;
}

bool
cluster_clog_storage_sync_file_v2(int fd, int directory_fd,
								 const ClusterClogFileBindingV2 *expected,
								 ClusterClogFileSyncV2 *out)
{
	ClusterClogFileBindingV2 binding;
	ClusterClogFileSyncV2 before, after;
	struct stat initial_file, final_file, initial_dir, final_dir;
	uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES];
	pg_cryptohash_ctx *hash;
	uintptr_t a = (uintptr_t) expected, b = (uintptr_t) out;
	uint32 pages;
	bool success = false;

	if (!enableFsync || fd < 0 || directory_fd < 0 || expected == NULL || out == NULL ||
		(a <= b ? b - a < sizeof(*expected) : a - b < sizeof(*out)))
	{
		errno = EINVAL;
		return false;
	}
	binding = *expected;
	if (!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)))
	{
		errno = EINVAL;
		return false;
	}
	if (fstat(fd, &initial_file) != 0 || fstat(directory_fd, &initial_dir) != 0)
		return false;
	if (!S_ISREG(initial_file.st_mode) || !S_ISDIR(initial_dir.st_mode) ||
		initial_file.st_size < CLUSTER_CLOG_FILE_HEADER_BYTES + CLUSTER_CLOG_FRAME_BYTES ||
		initial_file.st_size > CLUSTER_RECOVERY_CLOG_SEGMENT_BYTES_V2 ||
		(initial_file.st_size - CLUSTER_CLOG_FILE_HEADER_BYTES) % CLUSTER_CLOG_FRAME_BYTES != 0)
	{
		errno = EILSEQ;
		return false;
	}
	pages = (initial_file.st_size - CLUSTER_CLOG_FILE_HEADER_BYTES) / CLUSTER_CLOG_FRAME_BYTES;
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL)
		return false;
	/* No error-reporting owner operation occurs here. Allocation precedes all
	 * reads/syncs; the hash provider retains its normal backend cleanup. */
	if (!clog_sync_readback(fd, &binding, pages, hash, &before) || !enableFsync ||
		pg_fsync(fd) != 0 || !enableFsync || pg_fsync(directory_fd) != 0 || !enableFsync ||
		!clog_sync_readback(fd, &binding, pages, hash, &after) || !enableFsync ||
		fstat(fd, &final_file) != 0 || fstat(directory_fd, &final_dir) != 0)
		goto done;
	if (!S_ISREG(final_file.st_mode) || !S_ISDIR(final_dir.st_mode) ||
		initial_file.st_dev != final_file.st_dev || initial_file.st_ino != final_file.st_ino ||
		initial_file.st_size != final_file.st_size ||
		initial_dir.st_dev != final_dir.st_dev || initial_dir.st_ino != final_dir.st_ino ||
		memcmp(&before, &after, sizeof(before)) != 0)
		goto done;
	*out = after;
	success = true;
done:
	pg_cryptohash_free(hash);
	if (!success)
		errno = EIO;
	return success;
}
