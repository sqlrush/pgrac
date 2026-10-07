/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_runtime.c
 *    Secure production bootstrap helpers for pgrac-fenced.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#ifdef __linux__
#include <linux/magic.h>
#include <sys/vfs.h>
#else
#include <sys/mount.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

#include "pgrac_fenced_runtime.h"

bool
pgrac_fenced_runtime_dir_stat_secure(const struct stat *st,
								 uint64 allowed_gid)
{
	return st != NULL && S_ISDIR(st->st_mode) && st->st_uid == 0 &&
		(uint64) st->st_gid == allowed_gid && (st->st_mode & 07777) == 0750;
}

bool
pgrac_fenced_journal_dir_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISDIR(st->st_mode) && st->st_uid == 0 &&
		st->st_gid == 0 && (st->st_mode & 07777) == 0700;
}

bool
pgrac_fenced_journal_file_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISREG(st->st_mode) && st->st_uid == 0 &&
		st->st_gid == 0 && (st->st_mode & 07777) == 0600 &&
		st->st_size >= 0 &&
		(uint64) st->st_size <= PGRAC_FENCED_JOURNAL_SEGMENT_BYTES;
}

bool
pgrac_fenced_journal_filesystem_local(int fd)
{
	struct statfs filesystem;

	if (fd < 0 || fstatfs(fd, &filesystem) != 0)
		return false;
#ifdef __linux__
	if (filesystem.f_type == EXT4_SUPER_MAGIC ||
		filesystem.f_type == TMPFS_MAGIC ||
		filesystem.f_type == BTRFS_SUPER_MAGIC ||
		filesystem.f_type == XFS_SUPER_MAGIC ||
		filesystem.f_type == OVERLAYFS_SUPER_MAGIC)
		return true;
#ifdef F2FS_SUPER_MAGIC
	if (filesystem.f_type == F2FS_SUPER_MAGIC)
		return true;
#endif
#ifdef ZFS_SUPER_MAGIC
	if (filesystem.f_type == ZFS_SUPER_MAGIC)
		return true;
#endif
	return false;
#elif defined(MNT_LOCAL)
	return (filesystem.f_flags & MNT_LOCAL) != 0;
#else
	return false;
#endif
}

static bool
parse_sequence(const char **cursor, char delimiter, uint64 *value)
{
	const char *text = *cursor;
	uint64 parsed = 0;

	if (*text < '1' || *text > '9')
		return false;
	while (*text >= '0' && *text <= '9')
	{
		uint32 digit = (uint32) (*text - '0');

		if (parsed > (UINT64_MAX - digit) / 10)
			return false;
		parsed = parsed * 10 + digit;
		text++;
	}
	if (*text != delimiter)
		return false;
	*cursor = text + 1;
	*value = parsed;
	return true;
}

static int
hex_nibble(char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

bool
pgrac_fenced_journal_sealed_name_parse(
	const char *name,
	uint64 *first_seq,
	uint64 *last_seq,
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES])
{
	static const char prefix[] = "journal.";
	static const char suffix[] = ".sealed";
	uint8 parsed_digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
	const char *cursor;
	uint64 first;
	uint64 last;
	size_t i;

	if (name == NULL || first_seq == NULL || last_seq == NULL ||
		digest == NULL || strncmp(name, prefix, sizeof(prefix) - 1) != 0)
		return false;
	cursor = name + sizeof(prefix) - 1;
	if (!parse_sequence(&cursor, '-', &first) ||
		!parse_sequence(&cursor, '.', &last) || last < first ||
		last - first < PGRAC_FENCED_JOURNAL_MIN_SEALED_RECORDS - 1 ||
		last - first > PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS - 1 ||
		strlen(cursor) != sizeof(parsed_digest) * 2 + sizeof(suffix) - 1)
		return false;
	for (i = 0; i < sizeof(parsed_digest); i++)
	{
		int high = hex_nibble(cursor[i * 2]);
		int low = hex_nibble(cursor[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		parsed_digest[i] = (uint8) ((high << 4) | low);
	}
	cursor += sizeof(parsed_digest) * 2;
	if (strcmp(cursor, suffix) != 0)
		return false;
	*first_seq = first;
	*last_seq = last;
	memcpy(digest, parsed_digest, sizeof(parsed_digest));
	return true;
}

static bool
load_journal_segment_fd(
	int fd,
	bool allow_partial,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record,
	PgracFencedJournalReconcileState *reconcile)
{
	PgracFencedJournalScanResult scan_result;
	struct stat before;
	struct stat after;
	uint8 *bytes = NULL;
	size_t size;
	size_t used = 0;
	size_t offset;
	int flags;
	bool ok = false;

	if (state == NULL || last_record == NULL || have_last_record == NULL)
		return false;
	flags = fd >= 0 ? fcntl(fd, F_GETFL, 0) : -1;
	if (flags < 0 || (flags & O_APPEND) == 0 || fstat(fd, &before) != 0 ||
		!S_ISREG(before.st_mode) || before.st_size < 0 ||
		(uint64) before.st_size > PGRAC_FENCED_JOURNAL_SEGMENT_BYTES)
		goto done;
	size = (size_t) before.st_size;
	if (size > 0)
	{
		bytes = (uint8 *) malloc(size);
		if (bytes == NULL)
			goto done;
		while (used < size)
		{
			ssize_t got = pread(fd, bytes + used, size - used, (off_t) used);

			if (got < 0 && errno == EINTR)
				continue;
			if (got <= 0)
				goto done;
			used += (size_t) got;
		}
	}
	if (fstat(fd, &after) != 0 || before.st_dev != after.st_dev ||
		before.st_ino != after.st_ino || before.st_size != after.st_size)
		goto done;
	scan_result = pgrac_fenced_journal_scan_bytes(bytes, size, allow_partial,
		state);
	if (scan_result == PGRAC_FENCED_JOURNAL_SCAN_CORRUPT ||
		scan_result == PGRAC_FENCED_JOURNAL_SCAN_SEQUENCE_EXHAUSTED)
		goto done;
	if (scan_result == PGRAC_FENCED_JOURNAL_SCAN_PARTIAL_TAIL &&
		(!allow_partial ||
		!pgrac_fenced_journal_repair_partial_tail_fd(fd, state))
		)
		goto done;
	if (reconcile != NULL)
	{
		for (offset = 0; offset < state->valid_bytes;)
		{
			PgracFencedJournalRecordV1 replayed;
			size_t frame_len = pgrac_fenced_journal_frame_size(bytes + offset,
				state->valid_bytes - offset);

			if (frame_len == 0 || frame_len > state->valid_bytes - offset ||
				!pgrac_fenced_journal_record_decode(bytes + offset, frame_len, &replayed) ||
				!pgrac_fenced_journal_reconcile_observe(reconcile, &replayed))
				goto done;
			offset += frame_len;
		}
	}
	if (state->valid_bytes > 0)
	{
		if (state->last_record_bytes == 0 || state->last_record_bytes > state->valid_bytes ||
			!pgrac_fenced_journal_record_decode(
				bytes + state->valid_bytes - state->last_record_bytes,
				state->last_record_bytes, last_record))
			goto done;
		*have_last_record = true;
	}
	ok = true;

done:
	if (bytes != NULL)
		free(bytes);
	if (!ok)
	{
		state->available = false;
		if (reconcile != NULL)
			reconcile->available = false;
	}
	return ok;
}

bool
pgrac_fenced_journal_load_active_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record)
{
	return load_journal_segment_fd(fd, true, state, last_record,
		have_last_record, NULL);
}

bool
pgrac_fenced_journal_load_active_reconcile_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record,
	PgracFencedJournalReconcileState *reconcile)
{
	if (reconcile == NULL || !reconcile->available)
		return false;
	return load_journal_segment_fd(fd, true, state, last_record,
		have_last_record, reconcile);
}

bool
pgrac_fenced_journal_load_sealed_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record)
{
	return load_journal_segment_fd(fd, false, state, last_record,
		have_last_record, NULL);
}

bool
pgrac_fenced_journal_load_sealed_reconcile_fd(
	int fd,
	PgracFencedJournalScanState *state,
	PgracFencedJournalRecordV1 *last_record,
	bool *have_last_record,
	PgracFencedJournalReconcileState *reconcile)
{
	if (reconcile == NULL || !reconcile->available)
		return false;
	return load_journal_segment_fd(fd, false, state, last_record,
		have_last_record, reconcile);
}
