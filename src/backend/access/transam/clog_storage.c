/* Borrowed-FD CLOG observations. Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/clog_storage.h"

static bool
clog_storage_overlap(const void *left, Size left_bytes,
					 const void *right, Size right_bytes)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;

	return a <= b ? b - a < left_bytes : a - b < right_bytes;
}

static bool
clog_storage_read(int fd, void *bytes, Size length, off_t offset)
{
	ssize_t count = pg_pread(fd, bytes, length, offset);

	if (count == (ssize_t)length)
		return true;
	if (count >= 0)
		errno = EIO;
	return false;
}

static bool
clog_storage_extent(const struct stat *st, uint64 end)
{
	return S_ISREG(st->st_mode) && st->st_size >= (off_t)end &&
		st->st_size <= CLUSTER_CLOG_FILE_HEADER_BYTES +
			CLUSTER_CLOG_PAGES_PER_FILE * CLUSTER_CLOG_FRAME_BYTES;
}

bool
cluster_clog_storage_read_page_v2(int fd, const ClusterClogFileBindingV2 *expected,
								 uint64 logical_page, ClusterClogPageIdentityV2 *page_out,
								 void *payload_out, Size payload_length)
{
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 page;
	uint8 header[CLUSTER_CLOG_FILE_HEADER_BYTES];
	uint8 frame[CLUSTER_CLOG_FRAME_BYTES];
	uint8 payload[CLUSTER_CLOG_PAGE_BYTES];
	struct stat before, after;
	uint64 offset;

	if (fd < 0 || expected == NULL || page_out == NULL || payload_out == NULL ||
		payload_length != sizeof(payload) || logical_page >= 131072 ||
		logical_page / CLUSTER_CLOG_PAGES_PER_FILE != expected->segment ||
		clog_storage_overlap(page_out, sizeof(*page_out), payload_out, payload_length) ||
		clog_storage_overlap(expected, sizeof(*expected), page_out, sizeof(*page_out)) ||
		clog_storage_overlap(expected, sizeof(*expected), payload_out, payload_length))
	{
		errno = EINVAL;
		return false;
	}
	binding = *expected;
	/* Validate the original expected identity before doing any file I/O. */
	if (!cluster_clog_file_header_encode_v2(&binding, header, sizeof(header)) ||
		!cluster_clog_file_offset_v2(logical_page % CLUSTER_CLOG_PAGES_PER_FILE, &offset))
	{
		errno = EINVAL;
		return false;
	}
	if (fstat(fd, &before) != 0)
		return false;
	/* Require this complete frame. Another page may be extending the same
	 * segment under its own I/O lock. Metadata never supplies a birth identity. */
	if (!clog_storage_extent(&before, offset + sizeof(frame)))
	{
		errno = EILSEQ;
		return false;
	}
	if (!clog_storage_read(fd, header, sizeof(header), 0))
		return false;
	if (!cluster_clog_file_header_matches_v2(header, sizeof(header), &binding))
	{
		errno = EILSEQ;
		return false;
	}
	if (!clog_storage_read(fd, frame, sizeof(frame), (off_t)offset))
		return false;
	if (!cluster_clog_file_page_decode_v2(&binding, logical_page, frame, sizeof(frame),
										&page, payload, sizeof(payload)))
	{
		errno = EILSEQ;
		return false;
	}
	/* The returned page remains private until the immutable header and held
	 * file are rechecked. No partial observation escapes on a late failure. */
	if (!clog_storage_read(fd, header, sizeof(header), 0) || fstat(fd, &after) != 0)
		return false;
	if (!cluster_clog_file_header_matches_v2(header, sizeof(header), &binding) ||
		before.st_dev != after.st_dev || before.st_ino != after.st_ino ||
		!clog_storage_extent(&after, offset + sizeof(frame)))
	{
		errno = EIO;
		return false;
	}
	memcpy(payload_out, payload, sizeof(payload));
	*page_out = page;
	return true;
}
