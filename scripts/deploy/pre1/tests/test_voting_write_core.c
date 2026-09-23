/* Actual bounded voting-write core; real temporary fd plus narrow I/O faults.
 * Not a block-device qualification test or a production format bypass.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fault;
static int writes;
static ssize_t
test_pwrite(int fd, const void *bytes, size_t length, off_t offset)
{
	writes++;
	if (fault == 1) {
		errno = EIO;
		return -1;
	}
	return pwrite(fd, bytes, fault == 2 ? length - 512 : length, offset);
}

static int
test_fdatasync(int fd)
{
	if (fault == 3) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

#define pwrite test_pwrite
#define fdatasync test_fdatasync
#define main helper_main
#include "../voting_io.c"
#undef main
#undef pwrite
#undef fdatasync

#define REQUIRE(value)                                                                             \
	do {                                                                                           \
		if (!(value)) {                                                                            \
			fprintf(stderr, "line %d: %s\n", __LINE__, #value);                                    \
			return 1;                                                                              \
		}                                                                                          \
	} while (0)

int
main(void)
{
	unsigned char *image = malloc(IMAGE_BYTES);
	unsigned char *readback = malloc(IMAGE_BYTES);
	char path[] = "/tmp/pre1-vote-core-XXXXXX";
	int fd = mkstemp(path);
	int touched;
	int saved;
	unsigned char tail = 0x7e;

	REQUIRE(fd >= 0 && image && readback);
	REQUIRE(unlink(path) == 0);
	initial_image(image, 1);
	for (int scenario = 0; scenario < 6; scenario++) {
		REQUIRE(ftruncate(fd, 0) == 0);
		REQUIRE(ftruncate(fd, IMAGE_BYTES + 1) == 0);
		REQUIRE(pwrite(fd, &tail, 1, IMAGE_BYTES) == 1);
		fault = scenario <= 3 ? scenario : 0;
		writes = 0;
		if (scenario == 4)
			REQUIRE(pwrite(fd, &tail, 1, IMAGE_BYTES - 1) == 1);
		if (scenario == 5)
			REQUIRE(ftruncate(fd, IMAGE_BYTES - 512) == 0);
		FormatIoResult result = write_fresh_extent(fd, image, &touched, &saved);
		if (scenario == 0) {
			REQUIRE(result == FORMAT_IO_OK && touched && writes == 1);
			REQUIRE(pread(fd, readback, IMAGE_BYTES, 0) == IMAGE_BYTES);
			REQUIRE(memcmp(readback, image, IMAGE_BYTES) == 0);
			REQUIRE(pread(fd, readback, 1, IMAGE_BYTES) == 1 && readback[0] == tail);
			writes = 0;
			REQUIRE(write_fresh_extent(fd, image, &touched, &saved) == FORMAT_IO_NOT_BLANK);
			REQUIRE(!touched && writes == 0);
		} else if (scenario <= 3) {
			REQUIRE(result == (scenario == 3 ? FORMAT_IO_SYNC : FORMAT_IO_WRITE));
			REQUIRE(touched && writes == 1); /* No retry after any write attempt. */
			REQUIRE(saved == (scenario == 2 ? 0 : EIO));
		} else {
			REQUIRE(result == (scenario == 4 ? FORMAT_IO_NOT_BLANK : FORMAT_IO_READ));
			REQUIRE(!touched && writes == 0);
		}
	}
	REQUIRE(close(fd) == 0);
	free(image);
	free(readback);
	puts("7 bounded write/short-I/O/flush/reentry cases PASS");
	return 0;
}
