/*-------------------------------------------------------------------------
 * voting_io.c — standalone Linux voting-device attestation and fresh I/O.
 *
 * Not linked into PostgreSQL. No truncate operation exists. format-fresh is
 * an administrative primitive: its controller must first bind an exact new-LUN
 * authorization and prove all four databases stopped and formation not begun.
 * It is not a standalone deployment permission or runtime repair command.
 * The image command emits the existing initial member layout to stdout;
 * inspect checks an actual direct-I/O fd and reads the entire frozen extent.
 * A successful observation is not permission to format or start a database.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __linux__
#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#endif

#define MEMBER_COUNT 128U
#define SLOT_BYTES 512U
#define CRC_OFFSET 508U
#define IMAGE_BYTES ((8U * MEMBER_COUNT + 3U) * SLOT_BYTES)

typedef enum FormatIoResult {
	FORMAT_IO_OK,
	FORMAT_IO_ALLOCATION,
	FORMAT_IO_READ,
	FORMAT_IO_NOT_BLANK,
	FORMAT_IO_WRITE,
	FORMAT_IO_SYNC
} FormatIoResult;

/* The caller already owns an identity-checked, exclusive, direct block fd.
 * Keep this bounded I/O core portable so actual fd and fault tests exercise
 * exactly the implementation used on Linux. A write attempt is irreversible
 * evidence even when pwrite returns an error; never retry a partial format.
 */
FormatIoResult
write_fresh_extent(int fd, const unsigned char *initial, int *touched, int *saved)
{
	void *allocation = NULL;
	unsigned char *bytes;
	ssize_t count;
	FormatIoResult result = FORMAT_IO_OK;

	*touched = 0;
	*saved = posix_memalign(&allocation, 4096, IMAGE_BYTES);
	if (*saved)
		return FORMAT_IO_ALLOCATION;
	bytes = allocation;
	count = pread(fd, bytes, IMAGE_BYTES, 0);
	if (count != IMAGE_BYTES) {
		*saved = count < 0 ? errno : 0;
		result = FORMAT_IO_READ;
		goto done;
	}
	for (size_t n = 0; n < IMAGE_BYTES; n++) {
		if (bytes[n]) {
			result = FORMAT_IO_NOT_BLANK;
			goto done;
		}
	}
	memcpy(bytes, initial, IMAGE_BYTES);
	*touched = 1;
	count = pwrite(fd, bytes, IMAGE_BYTES, 0);
	if (count != IMAGE_BYTES) {
		*saved = count < 0 ? errno : 0;
		result = FORMAT_IO_WRITE;
		goto done;
	}
	if (fdatasync(fd) != 0) {
		*saved = errno;
		result = FORMAT_IO_SYNC;
	}
done:
	free(allocation);
	return result;
}

static uint32_t
crc32c(const unsigned char *data, size_t length)
{
	uint32_t crc = UINT32_MAX;

	for (size_t n = 0; n < length; n++) {
		crc ^= data[n];
		for (unsigned int bit = 0; bit < 8; bit++)
			crc = (crc >> 1) ^ ((crc & 1) ? UINT32_C(0x82f63b78) : 0);
	}
	return crc ^ UINT32_MAX;
}

typedef struct MemberObservation {
	uint64_t incarnation;
	uint64_t heartbeat_us;
	uint64_t epoch;
	uint64_t flags;
	uint64_t generation;
} MemberObservation;

static uint64_t
get_le(const unsigned char *p, unsigned int width)
{
	uint64_t value = 0;

	for (unsigned int n = 0; n < width; n++)
		value |= (uint64_t)p[n] << (8 * n);
	return value;
}

/* Mirror only the native ClusterVotingSlot layout and CRC contract. This
 * observation never grants membership, repairs a slot or certifies shutdown.
 * Runtime incarnation must survive clear; an all-zero fresh slot is not proof
 * that the particular writer being stopped has completed its own clear.
 */
int
observe_member(const unsigned char *slot, unsigned int node, unsigned int index,
			   MemberObservation *observed)
{
	memset(observed, 0, sizeof(*observed));
	if (node >= MEMBER_COUNT || index > 2 || get_le(slot, 4) != UINT32_C(0x51564f54)
		|| get_le(slot + 4, 4) != 1 || get_le(slot + 8, 4) != node
		|| get_le(slot + 48, 4) != index
		|| get_le(slot + CRC_OFFSET, 4) != crc32c(slot, CRC_OFFSET))
		return 0;
	observed->incarnation = get_le(slot + 16, 8);
	observed->heartbeat_us = get_le(slot + 24, 8);
	observed->epoch = get_le(slot + 32, 8);
	observed->flags = get_le(slot + 40, 8);
	observed->generation = get_le(slot + 56, 8);
	return 1;
}

static void
put32(unsigned char *p, uint32_t value)
{
	for (unsigned int n = 0; n < 4; n++)
		p[n] = (unsigned char)(value >> (8 * n));
}

static void
initial_image(unsigned char *image, unsigned int index)
{
	memset(image, 0, IMAGE_BYTES);
	for (unsigned int node = 0; node < MEMBER_COUNT; node++) {
		unsigned char *slot = image + node * SLOT_BYTES;

		put32(slot, UINT32_C(0x51564f54));
		put32(slot + 4, 1);
		put32(slot + 8, node);
		put32(slot + 48, index);
		put32(slot + CRC_OFFSET, crc32c(slot, CRC_OFFSET));
	}
}

static int
reject(const char *reason, int error, int rc)
{
	printf("{\"status\":\"%s\",\"reason\":\"%s\",\"errno\":%d,"
		   "\"strict_authority\":false,\"deployment_qualified\":false,\"read_only\":true}\n",
		   rc == 3 ? "ERROR" : "BLOCKED", reason, error);
	return rc;
}

static int
number(const char *text, uint64_t maximum, uint64_t *value)
{
	char *end;
	unsigned long long parsed;

	if (!text[0] || (text[0] == '0' && text[1]))
		return 0;
	for (const char *p = text; *p; p++)
		if (*p < '0' || *p > '9')
			return 0;
	errno = 0;
	parsed = strtoull(text, &end, 10);
	if (errno || *end || parsed > maximum)
		return 0;
	*value = parsed;
	return 1;
}

static int
scsi_naa_wwid(const char *value)
{
	size_t length = strlen(value);

	/* Explicit initial profile: SCSI NAA, not arbitrary aliases or NVMe IDs. */
	if ((length != 17 && length != 33) || value[0] != '3')
		return 0;
	for (size_t n = 1; n < length; n++)
		if (!((value[n] >= '0' && value[n] <= '9') || (value[n] >= 'a' && value[n] <= 'f')))
			return 0;
	return 1;
}

#ifdef __linux__
static int
device_identity(unsigned int maj, unsigned int min, const char *wwid)
{
	char path[160];
	char observed[80];
	char expected[80];
	struct stat st;
	FILE *stream;
	DIR *holders;
	struct dirent *entry;
	int occupied = 0;

	snprintf(path, sizeof(path), "/sys/dev/block/%u:%u/partition", maj, min);
	if (lstat(path, &st) == 0 || errno != ENOENT)
		return 0;
	snprintf(path, sizeof(path), "/sys/dev/block/%u:%u/holders", maj, min);
	holders = opendir(path);
	if (!holders)
		return 0;
	errno = 0;
	while ((entry = readdir(holders)) != NULL)
		if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, ".."))
			occupied = 1;
	if (errno)
		occupied = 1;
	if (closedir(holders) != 0 || occupied)
		return 0;
	/* A whole disk can have mounted partitions without holders on the parent.
	 * Reject every partition child, even if it is currently unmounted.
	 */
	snprintf(path, sizeof(path), "/sys/dev/block/%u:%u", maj, min);
	holders = opendir(path);
	if (!holders)
		return 0;
	errno = 0;
	while ((entry = readdir(holders)) != NULL) {
		char child[512];

		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		snprintf(child, sizeof(child), "%s/%s/partition", path, entry->d_name);
		if (lstat(child, &st) == 0 || (errno != ENOENT && errno != ENOTDIR))
			occupied = 1;
		errno = 0;
	}
	if (errno)
		occupied = 1;
	if (closedir(holders) != 0 || occupied)
		return 0;
	snprintf(path, sizeof(path), "/sys/dev/block/%u:%u/device/wwid", maj, min);
	stream = fopen(path, "r");
	if (!stream)
		return 0;
	if (!fgets(observed, sizeof(observed), stream)) {
		fclose(stream);
		return 0;
	}
	if (fgetc(stream) != EOF || ferror(stream)) {
		fclose(stream);
		return 0;
	}
	if (fclose(stream) != 0)
		return 0;
	snprintf(expected, sizeof(expected), "naa.%s\n", wwid + 1);
	return strcmp(expected, observed) == 0;
}

static int
format_failure(const char *reason, int error, int touched)
{
	printf("{\"status\":\"%s\",\"reason\":\"%s\",\"errno\":%d,"
		   "\"strict_authority\":false,\"deployment_qualified\":false,"
		   "\"write_attempted\":%s,\"read_only\":%s}\n",
		   touched ? "PARTIAL_FORMAT"
		   : error ? "ERROR"
				   : "BLOCKED",
		   reason, error, touched ? "true" : "false", touched ? "false" : "true");
	return touched || error ? 3 : 2;
}

static int
format_open(const char *path, const char *wwid, uint64_t expected_size, unsigned int maj,
			unsigned int min, int writable, int touched)
{
	struct stat st;
	int fd;
	int flags;
	int sector;
	uint64_t capacity;
	int mode = writable ? O_RDWR : O_RDONLY;
	int saved;

	if (lstat(path, &st) != 0)
		return -format_failure("DEVICE_STAT", errno, touched);
	if (!S_ISBLK(st.st_mode) || major(st.st_rdev) != maj || minor(st.st_rdev) != min)
		return -format_failure("DEVICE_TYPE_OR_NUMBER", 0, touched);
	fd = open(path, mode | O_EXCL | O_DIRECT | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		return -format_failure("EXCLUSIVE_DIRECT_OPEN", errno, touched);
	if (fstat(fd, &st) != 0 || !S_ISBLK(st.st_mode) || major(st.st_rdev) != maj
		|| minor(st.st_rdev) != min) {
		close(fd);
		return -format_failure("OPEN_DEVICE_IDENTITY", 0, touched);
	}
	flags = fcntl(fd, F_GETFL);
	if (flags < 0 || !(flags & O_DIRECT) || (flags & O_ACCMODE) != mode
		|| ioctl(fd, BLKGETSIZE64, &capacity) != 0 || ioctl(fd, BLKSSZGET, &sector) != 0) {
		saved = errno;
		close(fd);
		return -format_failure("DIRECT_FD_ATTESTATION", saved, touched);
	}
	if (capacity != expected_size || sector != SLOT_BYTES || !device_identity(maj, min, wwid)) {
		close(fd);
		return -format_failure("DEVICE_GEOMETRY_OR_IDENTITY", 0, touched);
	}
	return fd;
}

static int
format_fresh(const char *path, const char *wwid, unsigned int index, uint64_t expected_size,
			 unsigned int maj, unsigned int min, const unsigned char *initial)
{
	static const char *reasons[] = { "OK",
									 "ALLOCATION",
									 "DIRECT_READ_SHORT_OR_FAILED",
									 "VOTING_NOT_BLANK",
									 "DIRECT_WRITE_SHORT_OR_FAILED",
									 "DEVICE_FLUSH" };
	int fd = format_open(path, wwid, expected_size, maj, min, 1, 0);
	int touched = 0;
	int saved = 0;
	FormatIoResult result;
	void *allocation = NULL;
	ssize_t count;

	if (fd < 0)
		return -fd;
	result = write_fresh_extent(fd, initial, &touched, &saved);
	if (result != FORMAT_IO_OK) {
		close(fd);
		return format_failure(reasons[result], saved, touched);
	}
	if (close(fd) != 0)
		return format_failure("WRITE_DEVICE_CLOSE", errno, touched);
	/* A new actual fd, not the write buffer or a buffered-file cache read. */
	fd = format_open(path, wwid, expected_size, maj, min, 0, touched);
	if (fd < 0)
		return -fd;
	saved = posix_memalign(&allocation, 4096, IMAGE_BYTES);
	if (saved) {
		close(fd);
		return format_failure("READBACK_ALLOCATION", saved, touched);
	}
	count = pread(fd, allocation, IMAGE_BYTES, 0);
	if (count != IMAGE_BYTES || memcmp(allocation, initial, IMAGE_BYTES) != 0) {
		saved = count < 0 ? errno : 0;
		free(allocation);
		close(fd);
		return format_failure("DIRECT_REOPEN_READBACK", saved, touched);
	}
	free(allocation);
	if (close(fd) != 0)
		return format_failure("READBACK_DEVICE_CLOSE", errno, touched);
	printf("{\"status\":\"FORMATTED_LOCAL\",\"index\":%u,\"wwid\":\"%s\","
		   "\"major\":%u,\"minor\":%u,\"capacity\":%" PRIu64 ","
		   "\"logical_sector\":512,\"write_bytes\":%u,\"read_bytes\":%u,"
		   "\"direct\":true,\"flushed\":true,\"reopened\":true,"
		   "\"strict_authority\":true,\"read_only\":false,"
		   "\"deployment_qualified\":false}\n",
		   index, wwid, maj, min, expected_size, IMAGE_BYTES, IMAGE_BYTES);
	return 0;
}

static int
inspect(const char *path, const char *wwid, unsigned int index, uint64_t expected_size,
		unsigned int maj, unsigned int min, const unsigned char *initial)
{
	struct stat st;
	int fd = -1;
	int flags;
	int sector;
	uint64_t capacity;
	void *allocation = NULL;
	unsigned char *bytes;
	ssize_t got;
	int is_blank = 1;
	int is_fresh;
	int saved;

	if (lstat(path, &st) != 0)
		return reject("DEVICE_STAT", errno, 3);
	if (!S_ISBLK(st.st_mode) || major(st.st_rdev) != maj || minor(st.st_rdev) != min)
		return reject("DEVICE_TYPE_OR_NUMBER", 0, 2);
	fd = open(path, O_RDONLY | O_DIRECT | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		return reject("DIRECT_OPEN", errno, 3);
	if (fstat(fd, &st) != 0 || !S_ISBLK(st.st_mode) || major(st.st_rdev) != maj
		|| minor(st.st_rdev) != min) {
		close(fd);
		return reject("OPEN_DEVICE_IDENTITY", 0, 2);
	}
	flags = fcntl(fd, F_GETFL);
	if (flags < 0 || !(flags & O_DIRECT) || (flags & O_ACCMODE) != O_RDONLY
		|| ioctl(fd, BLKGETSIZE64, &capacity) != 0 || ioctl(fd, BLKSSZGET, &sector) != 0) {
		saved = errno;
		close(fd);
		return reject("DIRECT_FD_ATTESTATION", saved, 3);
	}
	if (capacity != expected_size || sector != SLOT_BYTES) {
		close(fd);
		return reject("DEVICE_GEOMETRY", 0, 2);
	}
	if (!device_identity(maj, min, wwid)) {
		close(fd);
		return reject("DEVICE_WWID_PARTITION_OR_HOLDER", 0, 2);
	}
	saved = posix_memalign(&allocation, 4096, IMAGE_BYTES);
	if (saved) {
		close(fd);
		return reject("ALLOCATION", saved, 3);
	}
	bytes = allocation;
	got = pread(fd, bytes, IMAGE_BYTES, 0);
	saved = errno;
	if (got != IMAGE_BYTES) {
		free(allocation);
		close(fd);
		return reject("DIRECT_READ_SHORT_OR_FAILED", got < 0 ? saved : 0, 3);
	}
	if (close(fd) != 0) {
		saved = errno;
		free(allocation);
		return reject("DEVICE_CLOSE", saved, 3);
	}
	for (size_t n = 0; n < IMAGE_BYTES; n++)
		if (bytes[n]) {
			is_blank = 0;
			break;
		}
	is_fresh = memcmp(bytes, initial, IMAGE_BYTES) == 0;
	printf("{\"status\":\"OBSERVED\",\"state\":\"%s\",\"index\":%u,"
		   "\"major\":%u,\"minor\":%u,\"wwid\":\"%s\",\"capacity\":%" PRIu64 ","
		   "\"logical_sector\":%d,\"open_flags\":%d,\"direct\":true,"
		   "\"read_bytes\":%u,\"crc32c\":%" PRIu32 ",\"strict_authority\":true,"
		   "\"deployment_qualified\":false,\"read_only\":true,\"members\":[",
		   is_blank	  ? "BLANK"
		   : is_fresh ? "FRESH_INITIAL_IMAGE"
					  : "NONFRESH_OR_INVALID",
		   index, maj, min, wwid, capacity, sector, flags, IMAGE_BYTES, crc32c(bytes, IMAGE_BYTES));
	for (unsigned int node = 0; node < MEMBER_COUNT; node++) {
		MemberObservation observed;
		int valid = observe_member(bytes + node * SLOT_BYTES, node, index, &observed);

		printf("%s{\"node_id\":%u,\"valid\":%s", node ? "," : "", node,
			   valid ? "true" : "false");
		if (valid)
			printf(",\"incarnation\":%" PRIu64 ",\"heartbeat_us\":%" PRIu64
				   ",\"epoch\":%" PRIu64 ",\"flags\":%" PRIu64 ",\"generation\":%" PRIu64,
				   observed.incarnation, observed.heartbeat_us, observed.epoch,
				   observed.flags, observed.generation);
		printf("}");
	}
	printf("]}\n");
	free(allocation);
	return 0;
}
#endif

int
main(int argc, char **argv)
{
	static unsigned char initial[IMAGE_BYTES];
	uint64_t index;
	uint64_t size;
	uint64_t maj;
	uint64_t min;

	if (argc == 3 && strcmp(argv[1], "image") == 0 && number(argv[2], 2, &index)) {
		initial_image(initial, (unsigned int)index);
		if (fwrite(initial, 1, IMAGE_BYTES, stdout) != IMAGE_BYTES || fflush(stdout) != 0)
			return 3;
		return 0;
	}
	/* Fixed device API; no arbitrary offsets, lengths, images or command passthrough. */
	if (argc != 8 || (strcmp(argv[1], "inspect") != 0 && strcmp(argv[1], "format-fresh") != 0)
		|| argv[2][0] != '/' || !scsi_naa_wwid(argv[3]) || !number(argv[4], 2, &index)
		|| !number(argv[5], UINT64_C(9007199254740991), &size) || size < IMAGE_BYTES
		|| size % SLOT_BYTES || !number(argv[6], UINT32_MAX, &maj)
		|| !number(argv[7], UINT32_MAX, &min))
		return reject("ARGUMENTS", 0, 2);
	initial_image(initial, (unsigned int)index);
#ifdef __linux__
	if (strcmp(argv[1], "format-fresh") == 0)
		return format_fresh(argv[2], argv[3], (unsigned int)index, size, (unsigned int)maj,
							(unsigned int)min, initial);
	return inspect(argv[2], argv[3], (unsigned int)index, size, (unsigned int)maj,
				   (unsigned int)min, initial);
#else
	return reject("LINUX_BLOCK_DEVICE_REQUIRED", 0, 3);
#endif
}
