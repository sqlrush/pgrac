/* PGRAC: same-host, exact-process exclusion for cold recovery. */
#include "postgres.h"
#include <signal.h>
#include "cluster/cluster_same_host_fence.h"
#include "common/cryptohash.h"
#include <ctype.h>
#include <limits.h>
#ifndef WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

static bool
same_host_nonzero(const uint8 *bytes, size_t length)
{
	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0) return true;
	return false;
}

static bool
same_host_process_valid(const ClusterSameHostProcessV1 *p)
{
	return p != NULL && same_host_nonzero(p->machine_id, 16) && same_host_nonzero(p->boot_id, 16)
		&& p->postmaster_pid > 0 && p->postmaster_pid <= INT_MAX && p->start_ticks > 0
		&& p->start_ticks != UINT64_MAX && p->pid_namespace > 0;
}

static bool
same_host_key_valid(const ClusterSameHostKeyV1 *k)
{
	return k != NULL && k->system_identifier != 0 && k->origin_node_id < 128
		&& k->incarnation != 0 && k->incarnation != UINT64_MAX
		&& same_host_nonzero(k->protected_set_digest, 32);
}

bool
cluster_same_host_record_matches_v1(const ClusterSameHostRecordV1 *r,
								  const ClusterSameHostKeyV1 *k)
{
	return r != NULL && same_host_process_valid(&r->process) && same_host_key_valid(k)
		&& r->key.system_identifier == k->system_identifier && r->key.origin_node_id == k->origin_node_id
		&& r->key.incarnation == k->incarnation
		&& memcmp(r->key.protected_set_digest, k->protected_set_digest, 32) == 0;
}

ClusterSameHostResultV1
cluster_same_host_process_excluded_v1(const ClusterSameHostProcessV1 *old,
	const ClusterSameHostProcessV1 *current, ClusterSameHostProbeV1 probe, uint64 ticks)
{
	if (!same_host_process_valid(old) || current == NULL
		|| !same_host_nonzero(current->machine_id, 16) || !same_host_nonzero(current->boot_id, 16)
		|| current->pid_namespace == 0 || memcmp(old->machine_id, current->machine_id, 16) != 0)
		return CLUSTER_SAME_HOST_UNAVAILABLE;
	if (memcmp(old->boot_id, current->boot_id, 16) != 0)
		return CLUSTER_SAME_HOST_WRITE_EXCLUDED;
	if (old->pid_namespace != current->pid_namespace)
		return CLUSTER_SAME_HOST_UNAVAILABLE;
	if (probe == CLUSTER_SAME_HOST_PROCESS_MISSING)
		return CLUSTER_SAME_HOST_WRITE_EXCLUDED;
	if (probe != CLUSTER_SAME_HOST_PROCESS_PRESENT || ticks == 0 || ticks == UINT64_MAX)
		return CLUSTER_SAME_HOST_UNAVAILABLE;
	return ticks == old->start_ticks ? CLUSTER_SAME_HOST_STILL_ALIVE : CLUSTER_SAME_HOST_WRITE_EXCLUDED;
}

bool
cluster_same_host_process_start_v1(const char *line, uint64 pid, uint64 *out)
{
	const char *p, *end_comm;
	char *end;
	unsigned long long value;

	if (out == NULL) return false;
	*out = 0;
	if (line == NULL || !isdigit((unsigned char)line[0]) || pid == 0 || pid > INT_MAX) return false;
	errno = 0;
	value = strtoull(line, &end, 10);
	if (errno != 0 || value != pid || end[0] != ' ' || end[1] != '(') return false;
	end_comm = strrchr(end + 1, ')');
	if (end_comm == NULL || end_comm[1] != ' ') return false;
	p = end_comm + 2;
	for (int field = 3; field <= 22; field++) {
		while (isspace((unsigned char)*p)) p++;
		if (*p == '\0') return false;
		if (field == 22) {
			if (!isdigit((unsigned char)*p)) return false;
			errno = 0;
			value = strtoull(p, &end, 10);
			if (errno != 0 || value == 0 || value == UINT64_MAX
				|| (*end != '\0' && !isspace((unsigned char)*end))) return false;
			*out = (uint64)value;
			return true;
		}
		while (*p != '\0' && !isspace((unsigned char)*p)) p++;
	}
	return false;
}

static uint64
same_host_get(const uint8 *b, size_t offset, size_t width)
{
	uint64 value = 0;
	for (size_t i = 0; i < width; i++) value |= (uint64)b[offset + i] << (8 * i);
	return value;
}

static void
same_host_put(uint8 *b, size_t offset, size_t width, uint64 value)
{
	for (size_t i = 0; i < width; i++) b[offset + i] = (uint8)(value >> (8 * i));
}

static bool
same_host_hash(const uint8 *bytes, uint8 digest[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok = ctx != NULL && pg_cryptohash_init(ctx) == 0
		&& pg_cryptohash_update(ctx, bytes, 128) == 0 && pg_cryptohash_final(ctx, digest, 32) == 0;
	if (ctx != NULL) pg_cryptohash_free(ctx);
	return ok;
}

bool
cluster_same_host_record_encode_v1(const ClusterSameHostRecordV1 *r, uint8 bytes[160])
{
	if (bytes == NULL) return false;
	memset(bytes, 0, 160);
	if (r == NULL || !cluster_same_host_record_matches_v1(r, &r->key)) return false;
	memcpy(bytes, "PGPX001", 8);
	same_host_put(bytes, 8, 4, 1); same_host_put(bytes, 12, 4, 160);
	same_host_put(bytes, 16, 8, r->key.system_identifier);
	same_host_put(bytes, 24, 4, r->key.origin_node_id);
	same_host_put(bytes, 28, 4, r->key.origin_node_id + 1);
	same_host_put(bytes, 32, 8, r->key.incarnation);
	memcpy(bytes + 40, r->process.machine_id, 16); memcpy(bytes + 56, r->process.boot_id, 16);
	same_host_put(bytes, 72, 8, r->process.postmaster_pid);
	same_host_put(bytes, 80, 8, r->process.start_ticks);
	memcpy(bytes + 88, r->key.protected_set_digest, 32);
	same_host_put(bytes, 120, 8, r->process.pid_namespace);
	return same_host_hash(bytes, bytes + 128);
}

bool
cluster_same_host_record_decode_v1(const uint8 *bytes, size_t length,
	const ClusterSameHostKeyV1 *expected, ClusterSameHostRecordV1 *out)
{
	ClusterSameHostRecordV1 r = { 0 };
	uint8 digest[32];
	if (out == NULL) return false;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || length != 160 || !same_host_key_valid(expected)
		|| memcmp(bytes, "PGPX001", 8) != 0 || same_host_get(bytes, 8, 4) != 1
		|| same_host_get(bytes, 12, 4) != 160 || !same_host_hash(bytes, digest)
		|| memcmp(bytes + 128, digest, 32) != 0) return false;
	r.key.system_identifier = same_host_get(bytes, 16, 8);
	r.key.origin_node_id = same_host_get(bytes, 24, 4);
	r.key.incarnation = same_host_get(bytes, 32, 8);
	if (same_host_get(bytes, 28, 4) != r.key.origin_node_id + 1) return false;
	memcpy(r.process.machine_id, bytes + 40, 16); memcpy(r.process.boot_id, bytes + 56, 16);
	r.process.postmaster_pid = same_host_get(bytes, 72, 8);
	r.process.start_ticks = same_host_get(bytes, 80, 8);
	memcpy(r.key.protected_set_digest, bytes + 88, 32);
	r.process.pid_namespace = same_host_get(bytes, 120, 8);
	if (!cluster_same_host_record_matches_v1(&r, expected)) return false;
	*out = r;
	return true;
}

#ifndef WIN32
#ifdef CLUSTER_SAME_HOST_OS_TEST
extern bool cluster_same_host_test_current(ClusterSameHostProcessV1 *out);
extern ClusterSameHostProbeV1 cluster_same_host_test_probe(uint64 pid, uint64 *ticks);
#define same_host_current cluster_same_host_test_current
#define same_host_probe cluster_same_host_test_probe
#else
static bool
same_host_text(const char *path, char *out, size_t capacity, int *error)
{
	int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
	size_t done = 0;
	bool ok = false;
	*error = fd < 0 ? errno : 0;
	if (fd < 0) return false;
	while (done < capacity - 1) {
		ssize_t n = read(fd, out + done, capacity - 1 - done);
		if (n < 0 && errno == EINTR) continue;
		if (n < 0) { *error = errno; break; }
		if (n == 0) { ok = done > 0; break; }
		done += n;
	}
	out[done] = '\0';
	if (close(fd) != 0) ok = false;
	return ok;
}

static bool
same_host_uuid(const char *path, uint8 out[16])
{
	char text[64];
	int error, digits = 0, nibble = 0;
	if (!same_host_text(path, text, sizeof(text), &error)) return false;
	memset(out, 0, 16);
	for (size_t i = 0; text[i] != '\0'; i++) {
		int c = (unsigned char)text[i], value;
		if (c == '\n' && text[i + 1] == '\0') break;
		if (c == '-' && (i == 8 || i == 13 || i == 18 || i == 23)) continue;
		if (c >= '0' && c <= '9') value = c - '0';
		else if (c >= 'a' && c <= 'f') value = c - 'a' + 10;
		else if (c >= 'A' && c <= 'F') value = c - 'A' + 10;
		else return false;
		if (digits >= 32) return false;
		if ((digits & 1) == 0) nibble = value << 4;
		else out[digits / 2] = (uint8)(nibble | value);
		digits++;
	}
	return digits == 32 && same_host_nonzero(out, 16);
}

static bool
same_host_current(ClusterSameHostProcessV1 *out)
{
	char text[4096];
	struct stat ns;
	int error;
	memset(out, 0, sizeof(*out));
	out->postmaster_pid = (uint64)getpid();
	if (!same_host_uuid("/etc/machine-id", out->machine_id)
		|| !same_host_uuid("/proc/sys/kernel/random/boot_id", out->boot_id)
		|| stat("/proc/self/ns/pid", &ns) != 0 || ns.st_ino == 0
		|| !same_host_text("/proc/self/stat", text, sizeof(text), &error)
		|| !cluster_same_host_process_start_v1(text, out->postmaster_pid, &out->start_ticks)) return false;
	out->pid_namespace = (uint64)ns.st_ino;
	return true;
}

static ClusterSameHostProbeV1
same_host_probe(uint64 pid, uint64 *ticks)
{
	char path[64], text[4096];
	int error;
	*ticks = 0;
	snprintf(path, sizeof(path), "/proc/%u/stat", (unsigned)pid);
	if (!same_host_text(path, text, sizeof(text), &error))
	{
		/* Restricted procfs can hide a live PID.  Only the independent
		 * same-namespace kernel lookup can establish that it is absent. */
		if ((error == ENOENT || error == ESRCH)
			&& kill((pid_t)pid, 0) == -1 && errno == ESRCH)
			return CLUSTER_SAME_HOST_PROCESS_MISSING;
		return CLUSTER_SAME_HOST_PROCESS_UNREADABLE;
	}
	return cluster_same_host_process_start_v1(text, pid, ticks) ? CLUSTER_SAME_HOST_PROCESS_PRESENT
		: CLUSTER_SAME_HOST_PROCESS_UNREADABLE;
}
#endif

static int
same_host_directory(const char *root, const ClusterSameHostKeyV1 *key)
{
	char thread[32];
	struct stat st;
	int fd, dir;
	if (root == NULL || root[0] == '\0' || !same_host_key_valid(key)) return -1;
	fd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (fd < 0) return -1;
	if (fstat(fd, &st) != 0 || !S_ISDIR(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0022)) {
		close(fd); return -1;
	}
	snprintf(thread, sizeof(thread), "thread_%u", key->origin_node_id + 1);
	dir = openat(fd, thread, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
	if (close(fd) != 0) { if (dir >= 0) close(dir); return -1; }
	if (dir >= 0 && (fstat(dir, &st) != 0 || !S_ISDIR(st.st_mode)
		|| st.st_uid != geteuid() || (st.st_mode & 0022))) { close(dir); return -1; }
	return dir;
}

static void
same_host_name(const ClusterSameHostKeyV1 *key, char out[80])
{
	snprintf(out, 80, "pgrac_process_" UINT64_FORMAT ".identity", key->incarnation);
}

static bool
same_host_read(int dir, const char *name, uint8 bytes[160], bool sync)
{
	struct stat st;
	int fd = openat(dir, name, (sync ? O_RDWR : O_RDONLY) | O_CLOEXEC | O_NOFOLLOW);
	bool ok;
	if (fd < 0) return false;
	ok = fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_uid == geteuid()
		&& st.st_nlink == 1 && !(st.st_mode & 0022) && st.st_size == 160;
	/* A retry after an uncertain first write must sync the existing inode. */
	if (ok && sync) ok = fsync(fd) == 0;
	if (ok) ok = read(fd, bytes, 160) == 160;
	if (close(fd) != 0) ok = false;
	return ok;
}
#endif

bool
cluster_same_host_fence_available(void)
{
#ifndef WIN32
	ClusterSameHostProcessV1 current;
	return same_host_current(&current);
#else
	return false;
#endif
}

bool
cluster_same_host_fence_record_writer_v1(const char *root, const ClusterSameHostKeyV1 *key,
									   uint64 postmaster_pid)
{
#ifndef WIN32
	ClusterSameHostRecordV1 r = { 0 };
	ClusterSameHostProcessV1 after;
	uint8 bytes[160], readback[160];
	char name[80];
	int dir, fd;
	bool ok;
	if (!same_host_key_valid(key) || postmaster_pid == 0 || postmaster_pid > INT_MAX
		|| (postmaster_pid != (uint64)getpid() && postmaster_pid != (uint64)getppid())
		|| !same_host_current(&r.process)) return false;
	r.key = *key;
	r.process.postmaster_pid = postmaster_pid;
	if (same_host_probe(postmaster_pid, &r.process.start_ticks) != CLUSTER_SAME_HOST_PROCESS_PRESENT
		|| !same_host_current(&after) || memcmp(r.process.machine_id, after.machine_id, 16) != 0
		|| memcmp(r.process.boot_id, after.boot_id, 16) != 0 || r.process.pid_namespace != after.pid_namespace
		|| !cluster_same_host_record_encode_v1(&r, bytes)) return false;
	dir = same_host_directory(root, key);
	if (dir < 0) return false;
	same_host_name(key, name);
	fd = openat(dir, name, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
	if (fd < 0) {
		ok = errno == EEXIST && same_host_read(dir, name, readback, true) && memcmp(bytes, readback, 160) == 0;
	} else {
		ok = write(fd, bytes, 160) == 160 && fsync(fd) == 0 && lseek(fd, 0, SEEK_SET) == 0
			&& read(fd, readback, 160) == 160 && memcmp(bytes, readback, 160) == 0;
		if (close(fd) != 0) ok = false;
	}
	if (ok && fsync(dir) != 0) ok = false;
	if (close(dir) != 0) ok = false;
	return ok;
#else
	return false;
#endif
}

ClusterSameHostResultV1
cluster_same_host_fence_prove_v1(const char *root, const ClusterSameHostKeyV1 *key,
							   ClusterSameHostRecordV1 *out)
{
	if (out == NULL) return CLUSTER_SAME_HOST_UNAVAILABLE;
	memset(out, 0, sizeof(*out));
#ifndef WIN32
	{
		ClusterSameHostRecordV1 r;
		ClusterSameHostProcessV1 current, after;
		ClusterSameHostProbeV1 probe;
		ClusterSameHostResultV1 result;
		uint8 bytes[160];
		uint64 ticks;
		char name[80];
		int dir;
		bool ok;
		if (!same_host_key_valid(key) || !same_host_current(&current)) return CLUSTER_SAME_HOST_UNAVAILABLE;
		dir = same_host_directory(root, key);
		if (dir < 0) return CLUSTER_SAME_HOST_UNAVAILABLE;
		same_host_name(key, name);
		ok = same_host_read(dir, name, bytes, false);
		if (close(dir) != 0) ok = false;
		if (!ok || !cluster_same_host_record_decode_v1(bytes, 160, key, &r)) return CLUSTER_SAME_HOST_UNAVAILABLE;
		probe = same_host_probe(r.process.postmaster_pid, &ticks);
		if (!same_host_current(&after) || memcmp(current.machine_id, after.machine_id, 16) != 0
			|| memcmp(current.boot_id, after.boot_id, 16) != 0 || current.pid_namespace != after.pid_namespace)
			return CLUSTER_SAME_HOST_UNAVAILABLE;
		result = cluster_same_host_process_excluded_v1(&r.process, &current, probe, ticks);
		if (result == CLUSTER_SAME_HOST_WRITE_EXCLUDED) *out = r;
		return result;
	}
#else
	return CLUSTER_SAME_HOST_UNAVAILABLE;
#endif
}
