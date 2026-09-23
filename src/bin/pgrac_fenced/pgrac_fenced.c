/*-------------------------------------------------------------------------
 *
 * pgrac_fenced.c
 *    Privileged provider-neutral external-fence daemon.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_config.h"
#include "pgrac_fenced_coordinator.h"
#include "pgrac_fenced_core.h"
#include "pgrac_fenced_provider.h"
#include "pgrac_fenced_rejoin_coordinator.h"
#include "pgrac_fenced_runtime.h"

#define PGRAC_FENCED_EXIT_UNAVAILABLE 77
#define PGRAC_FENCED_POLL_MS 100
#define PGRAC_FENCED_TRANSPORT_MAX_NS UINT64_C(600000000000)

typedef struct PgracFencedFrontdoorClient
{
	int fd;
	uint64 deadline_mono_ns;
} PgracFencedFrontdoorClient;

typedef struct PgracFencedFrontdoor
{
	uint32 client_count;
	PgracFencedFrontdoorClient clients[PGRAC_FENCED_MAX_CLIENTS];
} PgracFencedFrontdoor;

static volatile sig_atomic_t stop_requested;
static volatile sig_atomic_t reload_requested;

typedef enum PgracFencedDispatchResult
{
	PGRAC_FENCED_DISPATCH_ERROR = 0,
	PGRAC_FENCED_DISPATCH_STOP = 1,
	PGRAC_FENCED_DISPATCH_RELOAD = 2
} PgracFencedDispatchResult;

static void
request_stop(int signal_number)
{
	(void) signal_number;
	stop_requested = 1;
}

static void
request_reload(int signal_number)
{
	(void) signal_number;
	reload_requested = 1;
}

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
monotonic_now_ns(uint64 *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64) now.tv_sec * UINT64_C(1000000000) +
		(uint64) now.tv_nsec;
	return *out != 0;
}

static bool
same_config_identity(const struct stat *before, const struct stat *after)
{
	return before->st_dev == after->st_dev && before->st_ino == after->st_ino &&
		before->st_size == after->st_size && before->st_uid == after->st_uid &&
		before->st_gid == after->st_gid && before->st_mode == after->st_mode;
}

static bool
read_root_config(PgracFencedConfigV1 *config,
				 uint8 digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES])
{
	uint8 *bytes = NULL;
	struct stat before;
	struct stat after;
	ssize_t got;
	size_t used = 0;
	int fd;
	bool ok = false;

	fd = pgrac_fenced_config_open_secure();
	if (fd < 0 || fstat(fd, &before) != 0 ||
		!pgrac_fenced_config_stat_secure(&before))
		goto done;
	bytes = (uint8 *) malloc((size_t) before.st_size);
	if (bytes == NULL)
		goto done;
	while (used < (size_t) before.st_size)
	{
		got = read(fd, bytes + used, (size_t) before.st_size - used);
		if (got < 0 && errno == EINTR)
			continue;
		if (got <= 0)
			goto done;
		used += (size_t) got;
	}
	if (fstat(fd, &after) != 0 || !same_config_identity(&before, &after) ||
		pgrac_fenced_config_parse(bytes, used, config) !=
			PGRAC_FENCED_CONFIG_OK ||
		!pgrac_fenced_config_digest_v1(bytes, used, digest))
		goto done;
	ok = true;

done:
	if (bytes != NULL)
	{
		memset(bytes, 0, used);
		free(bytes);
	}
	if (fd >= 0)
		(void) close(fd);
	return ok;
}

typedef struct SealedJournalEntry
{
	char name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	uint64 first_seq;
	uint64 last_seq;
	uint8 digest[PGRAC_FENCED_JOURNAL_DIGEST_BYTES];
} SealedJournalEntry;

static bool
journal_directory_inventory(int directory_fd,
						SealedJournalEntry entries[PGRAC_FENCED_JOURNAL_MAX_SEALED],
						uint32 *sealed_count, bool *active_exists)
{
	DIR *directory;
	struct dirent *entry;
	SealedJournalEntry candidate;
	uint32 count = 0;
	uint32 i;
	uint32 j;
	int duplicate_fd;
	bool ok = true;

	*sealed_count = 0;
	*active_exists = false;
	duplicate_fd = dup(directory_fd);
	if (duplicate_fd < 0)
		return false;
	directory = fdopendir(duplicate_fd);
	if (directory == NULL)
	{
		(void) close(duplicate_fd);
		return false;
	}
	errno = 0;
	while ((entry = readdir(directory)) != NULL)
	{
		if (strcmp(entry->d_name, ".") == 0 ||
			strcmp(entry->d_name, "..") == 0)
			continue;
		if (strcmp(entry->d_name, PGRAC_FENCED_JOURNAL_ACTIVE_NAME) == 0)
		{
			*active_exists = true;
			continue;
		}
		memset(&candidate, 0, sizeof(candidate));
		if (count >= PGRAC_FENCED_JOURNAL_MAX_SEALED ||
			!pgrac_fenced_journal_sealed_name_parse(entry->d_name,
				&candidate.first_seq, &candidate.last_seq,
				candidate.digest) ||
			strlcpy(candidate.name, entry->d_name,
				sizeof(candidate.name)) >= sizeof(candidate.name))
		{
			ok = false;
			break;
		}
		entries[count++] = candidate;
	}
	if (entry == NULL && errno != 0)
		ok = false;
	(void) closedir(directory);
	if (!ok)
		return false;
	for (i = 1; i < count; i++)
	{
		candidate = entries[i];
		j = i;
		while (j > 0 && entries[j - 1].first_seq > candidate.first_seq)
		{
			entries[j] = entries[j - 1];
			j--;
		}
		entries[j] = candidate;
	}
	*sealed_count = count;
	return ok;
}

static bool
open_active_journal(const PgracFencedConfigV1 *config,
					const uint8 config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES],
					int *directory_fd, int *active_fd,
					PgracFencedJournalScanState *state,
					PgracFencedJournalReconcileState *reconcile,
					uint32 *sealed_count)
{
	SealedJournalEntry sealed[PGRAC_FENCED_JOURNAL_MAX_SEALED];
	PgracFencedJournalRecordV1 last_record;
	struct stat directory_stat;
	struct stat active_stat;
	uint32 count = 0;
	uint32 i;
	bool have_last_record;
	bool active_existed;
	char sealed_name[PGRAC_FENCED_JOURNAL_SEALED_NAME_MAX];
	int fd = -1;
	int journal_fd = -1;
	int sealed_fd = -1;

	*directory_fd = -1;
	*active_fd = -1;
	*sealed_count = 0;
	memset(&last_record, 0, sizeof(last_record));
	have_last_record = false;
	fd = open(PGRAC_FENCED_JOURNAL_DIR,
		O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	if (fd < 0 || fstat(fd, &directory_stat) != 0 ||
		!pgrac_fenced_journal_dir_stat_secure(&directory_stat) ||
		!pgrac_fenced_journal_filesystem_local(fd) ||
		flock(fd, LOCK_EX | LOCK_NB) != 0 ||
		!journal_directory_inventory(fd, sealed, &count, &active_existed))
		goto fail;
	pgrac_fenced_journal_scan_state_init(state);
	pgrac_fenced_journal_reconcile_state_init(reconcile);
	for (i = 0; i < count; i++)
	{
		if (sealed[i].first_seq != state->next_seq)
			goto fail;
		sealed_fd = openat(fd, sealed[i].name,
			O_RDONLY | O_APPEND | O_NOFOLLOW);
		if (sealed_fd < 0 || fstat(sealed_fd, &active_stat) != 0 ||
			!pgrac_fenced_journal_file_stat_secure(&active_stat) ||
			(uint64) active_stat.st_size != PGRAC_FENCED_JOURNAL_SEGMENT_BYTES ||
			!pgrac_fenced_journal_load_sealed_reconcile_fd(sealed_fd, state,
				&last_record, &have_last_record, reconcile) ||
			!have_last_record ||
			last_record.seq != sealed[i].last_seq ||
			memcmp(state->previous_record_digest, sealed[i].digest,
				sizeof(sealed[i].digest)) != 0)
			goto fail;
		(void) close(sealed_fd);
		sealed_fd = -1;
	}
	journal_fd = openat(fd, PGRAC_FENCED_JOURNAL_ACTIVE_NAME,
		O_RDWR | O_APPEND | O_CREAT | O_NOFOLLOW, 0600);
	if (journal_fd < 0 || fstat(journal_fd, &active_stat) != 0 ||
		!pgrac_fenced_journal_file_stat_secure(&active_stat) ||
		(!active_existed && fsync(fd) != 0) ||
		!pgrac_fenced_journal_load_active_reconcile_fd(journal_fd, state,
			&last_record, &have_last_record, reconcile) ||
		!pgrac_fenced_journal_reconcile_finish(reconcile))
		goto fail;
	if (have_last_record &&
		(last_record.mapping_generation > config->mapping_generation ||
		 (last_record.mapping_generation == config->mapping_generation &&
		  memcmp(last_record.semantic_config_digest, config_digest,
			  PGRAC_FENCED_CONFIG_DIGEST_BYTES) != 0) ||
		 (last_record.mapping_generation < config->mapping_generation &&
		  memcmp(last_record.semantic_config_digest, config_digest,
			  PGRAC_FENCED_CONFIG_DIGEST_BYTES) == 0)))
		goto fail;
	if (state->segment_record_count == PGRAC_FENCED_JOURNAL_SEGMENT_RECORDS)
	{
		if (!pgrac_fenced_journal_rotate_at(fd, &journal_fd, count, state,
				sealed_name, sizeof(sealed_name)))
			goto fail;
		count++;
	}
	*directory_fd = fd;
	*active_fd = journal_fd;
	*sealed_count = count;
	return true;

fail:
	if (sealed_fd >= 0)
		(void) close(sealed_fd);
	if (journal_fd >= 0)
		(void) close(journal_fd);
	if (fd >= 0)
		(void) close(fd);
	return false;
}

static bool
set_nonblocking_cloexec(int fd)
{
	int descriptor_flags = fcntl(fd, F_GETFD, 0);
	int status_flags = fcntl(fd, F_GETFL, 0);

	return descriptor_flags >= 0 && status_flags >= 0 &&
		fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0 &&
		fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) == 0;
}

static bool
create_db_socket(const PgracFencedConfigV1 *config, int *runtime_directory_fd,
				 int *listen_fd, dev_t *socket_device, ino_t *socket_inode)
{
	static const char socket_name[] = "pgrac-fenced.sock";
	struct sockaddr_un address;
	struct stat directory_stat;
	struct stat socket_stat;
	bool bound = false;
	bool existing;
	int directory_fd = -1;
	int fd = -1;

	*runtime_directory_fd = -1;
	*listen_fd = -1;
	directory_fd = open("/var/run/pgrac", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
	if (directory_fd < 0 || fstat(directory_fd, &directory_stat) != 0 ||
		!pgrac_fenced_runtime_dir_stat_secure(&directory_stat,
			config->allowed_db_gid))
		goto fail;
	existing = fstatat(directory_fd, socket_name, &socket_stat,
		AT_SYMLINK_NOFOLLOW) == 0;
	if (!existing && errno != ENOENT)
		goto fail;
	if (existing)
	{
		if (!S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != 0 ||
			(uint64) socket_stat.st_gid != config->allowed_db_gid ||
			(socket_stat.st_mode & 07777) != 0660 ||
			unlinkat(directory_fd, socket_name, 0) != 0)
			goto fail;
	}
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0 || !set_nonblocking_cloexec(fd))
		goto fail;
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	if (strlcpy(address.sun_path, PGRAC_FENCED_DB_SOCKET_PATH,
			sizeof(address.sun_path)) >= sizeof(address.sun_path) ||
		bind(fd, (struct sockaddr *) &address, sizeof(address)) != 0)
		goto fail;
	bound = true;
	if (
		chown(PGRAC_FENCED_DB_SOCKET_PATH, 0, (gid_t) config->allowed_db_gid) != 0 ||
		chmod(PGRAC_FENCED_DB_SOCKET_PATH, 0660) != 0 ||
		lstat(PGRAC_FENCED_DB_SOCKET_PATH, &socket_stat) != 0 ||
		!S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != 0 ||
		(uint64) socket_stat.st_gid != config->allowed_db_gid ||
		(socket_stat.st_mode & 07777) != 0660 ||
		listen(fd, PGRAC_FENCED_MAX_CLIENTS) != 0)
		goto fail;
	*socket_device = socket_stat.st_dev;
	*socket_inode = socket_stat.st_ino;
	*runtime_directory_fd = directory_fd;
	*listen_fd = fd;
	return true;

fail:
	if (fd >= 0)
		(void) close(fd);
	if (bound && directory_fd >= 0)
		(void) unlinkat(directory_fd, socket_name, 0);
	if (directory_fd >= 0)
		(void) close(directory_fd);
	return false;
}

static bool
create_admin_socket(int runtime_directory_fd, int *listen_fd,
			dev_t *socket_device, ino_t *socket_inode)
{
	static const char socket_name[] = "pgrac-fenced-admin.sock";
	struct sockaddr_un address;
	struct stat socket_stat;
	bool bound = false;
	bool existing;
	int fd = -1;

	*listen_fd = -1;
	if (runtime_directory_fd < 0)
		return false;
	existing = fstatat(runtime_directory_fd, socket_name, &socket_stat,
		AT_SYMLINK_NOFOLLOW) == 0;
	if (!existing && errno != ENOENT)
		goto fail;
	if (existing)
	{
		if (!S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != 0 ||
			socket_stat.st_gid != 0 ||
			(socket_stat.st_mode & 07777) != 0600 ||
			unlinkat(runtime_directory_fd, socket_name, 0) != 0)
			goto fail;
	}
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0 || !set_nonblocking_cloexec(fd))
		goto fail;
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	if (strlcpy(address.sun_path, PGRAC_FENCED_ADMIN_SOCKET_PATH,
			sizeof(address.sun_path)) >= sizeof(address.sun_path) ||
		bind(fd, (struct sockaddr *) &address, sizeof(address)) != 0)
		goto fail;
	bound = true;
	if (chown(PGRAC_FENCED_ADMIN_SOCKET_PATH, 0, 0) != 0 ||
		chmod(PGRAC_FENCED_ADMIN_SOCKET_PATH, 0600) != 0 ||
		lstat(PGRAC_FENCED_ADMIN_SOCKET_PATH, &socket_stat) != 0 ||
		!S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != 0 ||
		socket_stat.st_gid != 0 ||
		(socket_stat.st_mode & 07777) != 0600 ||
		listen(fd, PGRAC_FENCED_REJOIN_MAX_CLIENTS) != 0)
		goto fail;
	*socket_device = socket_stat.st_dev;
	*socket_inode = socket_stat.st_ino;
	*listen_fd = fd;
	return true;

fail:
	if (fd >= 0)
		(void) close(fd);
	if (bound)
		(void) unlinkat(runtime_directory_fd, socket_name, 0);
	return false;
}

static void
unlink_own_socket(int runtime_directory_fd, const char *socket_name,
			  dev_t device, ino_t inode)
{
	struct stat st;

	if (runtime_directory_fd >= 0 && socket_name != NULL &&
		fstatat(runtime_directory_fd, socket_name, &st,
			AT_SYMLINK_NOFOLLOW) == 0 && S_ISSOCK(st.st_mode) &&
		st.st_dev == device && st.st_ino == inode)
		(void) unlinkat(runtime_directory_fd, socket_name, 0);
}

static void
frontdoor_init(PgracFencedFrontdoor *frontdoor)
{
	uint32 i;

	memset(frontdoor, 0, sizeof(*frontdoor));
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
		frontdoor->clients[i].fd = -1;
}

static void
frontdoor_clear(PgracFencedFrontdoor *frontdoor, uint32 slot, bool close_fd)
{
	if (close_fd && frontdoor->clients[slot].fd >= 0)
		(void) close(frontdoor->clients[slot].fd);
	if (frontdoor->clients[slot].fd >= 0)
		frontdoor->client_count--;
	memset(&frontdoor->clients[slot], 0,
		   sizeof(frontdoor->clients[slot]));
	frontdoor->clients[slot].fd = -1;
}

static bool
frontdoor_add(PgracFencedFrontdoor *frontdoor, int fd, uint64 deadline)
{
	uint32 i;

	if (frontdoor->client_count >= PGRAC_FENCED_MAX_CLIENTS)
	{
		(void) close(fd);
		return true;
	}
	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (frontdoor->clients[i].fd >= 0)
			continue;
		frontdoor->clients[i].fd = fd;
		frontdoor->clients[i].deadline_mono_ns = deadline;
		frontdoor->client_count++;
		return true;
	}
	(void) close(fd);
	return true;
}

static bool
frontdoor_service(PgracFencedFrontdoor *frontdoor,
		PgracFencedCoordinatorV1 *scalar,
		PgracFencedRejoinCoordinatorV1 *rejoin, uint64 now)
{
	uint8 magic[4];
	ssize_t got;
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		PgracFencedFrontdoorClient *client = &frontdoor->clients[i];

		if (client->fd < 0)
			continue;
		if (now >= client->deadline_mono_ns)
		{
			frontdoor_clear(frontdoor, i, true);
			continue;
		}
		do
		{
			got = recv(client->fd, magic, sizeof(magic),
				MSG_DONTWAIT | MSG_PEEK);
		} while (got < 0 && errno == EINTR);
		if (got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
		{
			frontdoor_clear(frontdoor, i, true);
			continue;
		}
		if (got < (ssize_t) sizeof(magic))
			continue;
		if (memcmp(magic, "PFRQ", sizeof(magic)) == 0)
			(void) pgrac_fenced_coordinator_accept_fd(scalar, client->fd,
				client->deadline_mono_ns);
		else if (memcmp(magic, "PFRJ", sizeof(magic)) == 0)
			(void) pgrac_fenced_rejoin_coordinator_accept_fd(rejoin,
				client->fd, false, client->deadline_mono_ns);
		else
		{
			frontdoor_clear(frontdoor, i, true);
			continue;
		}
		frontdoor_clear(frontdoor, i, false);
		if (!scalar->context->available)
			return false;
	}
	return true;
}

static void
frontdoor_shutdown(PgracFencedFrontdoor *frontdoor)
{
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_MAX_CLIENTS; i++)
	{
		if (frontdoor->clients[i].fd >= 0)
			frontdoor_clear(frontdoor, i, true);
	}
}

static bool
accept_listener_clients(int listen_fd, bool is_admin,
			PgracFencedOperationContextV1 *context,
			PgracFencedFrontdoor *frontdoor,
			PgracFencedRejoinCoordinatorV1 *rejoin)
{
	PgracFencedPeerCredential peer;
	uint64 deadline;
	uint64 now;
	int client_fd;

	for (;;)
	{
		client_fd = accept(listen_fd, NULL, NULL);
		if (client_fd < 0 && errno == EINTR)
			continue;
		if (client_fd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			return true;
		if (client_fd < 0 || !set_nonblocking_cloexec(client_fd) ||
			!pgrac_fenced_peer_credential_get(client_fd, &peer) ||
			(is_admin ? !pgrac_fenced_peer_is_root(&peer) :
			 !pgrac_fenced_peer_is_db(&peer, context->config->allowed_db_uid,
				 context->config->allowed_db_gid)))
		{
			if (client_fd >= 0)
				(void) close(client_fd);
			if (client_fd < 0)
				return false;
			continue;
		}
		if (!monotonic_now_ns(&now) ||
			UINT64_MAX - now < PGRAC_FENCED_TRANSPORT_MAX_NS)
		{
			(void) close(client_fd);
			return false;
		}
		deadline = now + PGRAC_FENCED_TRANSPORT_MAX_NS;
		if (is_admin)
			(void) pgrac_fenced_rejoin_coordinator_accept_fd(rejoin,
				client_fd, true, deadline);
		else if (!frontdoor_add(frontdoor, client_fd, deadline))
			return false;
	}
}

static PgracFencedDispatchResult
run_dispatch_loop(int db_listen_fd, int admin_listen_fd,
			  PgracFencedCoordinatorV1 *coordinator,
			  PgracFencedRejoinCoordinatorV1 *rejoin)
{
	PgracFencedFrontdoor frontdoor;
	struct pollfd descriptors[2];
	uint64 now;
	int rc;
	bool failed = false;
	bool reload_during_operation = false;

	frontdoor_init(&frontdoor);
	while (!stop_requested && !reload_requested &&
		coordinator->context->available)
	{
		descriptors[0].fd = db_listen_fd;
		descriptors[0].events = POLLIN;
		descriptors[0].revents = 0;
		descriptors[1].fd = admin_listen_fd;
		descriptors[1].events = POLLIN;
		descriptors[1].revents = 0;
		rc = poll(descriptors, lengthof(descriptors), PGRAC_FENCED_POLL_MS);
		if (rc < 0 && errno == EINTR)
			continue;
		if (rc < 0 ||
			(descriptors[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
			(descriptors[1].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 ||
			!monotonic_now_ns(&now) ||
			!pgrac_fenced_coordinator_service(coordinator, now) ||
			!pgrac_fenced_rejoin_coordinator_service(rejoin, now) ||
			!frontdoor_service(&frontdoor, coordinator, rejoin, now))
		{
			failed = true;
			break;
		}
		if ((descriptors[0].revents & POLLIN) != 0 &&
			!accept_listener_clients(db_listen_fd, false,
				coordinator->context, &frontdoor, rejoin))
		{
			failed = true;
			break;
		}
		if ((descriptors[1].revents & POLLIN) != 0 &&
			!accept_listener_clients(admin_listen_fd, true,
				coordinator->context, &frontdoor, rejoin))
		{
			failed = true;
			break;
		}
	}
	frontdoor_shutdown(&frontdoor);
	if (!coordinator->context->available)
		return PGRAC_FENCED_DISPATCH_ERROR;
	reload_during_operation = reload_requested &&
		(pgrac_fenced_coordinator_active_worker_count(coordinator) > 0 ||
		 pgrac_fenced_rejoin_coordinator_operation_count(rejoin) > 0);
	if (failed || stop_requested || reload_during_operation)
	{
		if (!pgrac_fenced_coordinator_quiesce(coordinator,
				reload_during_operation ? 17 : 16) ||
			!pgrac_fenced_rejoin_coordinator_quiesce(rejoin))
			return PGRAC_FENCED_DISPATCH_ERROR;
	}
	while (coordinator->context->available &&
		(pgrac_fenced_coordinator_active_worker_count(coordinator) > 0 ||
		 pgrac_fenced_rejoin_coordinator_active_worker_count(rejoin) > 0))
	{
		if (!monotonic_now_ns(&now) ||
			!pgrac_fenced_coordinator_service(coordinator, now) ||
			!pgrac_fenced_rejoin_coordinator_service(rejoin, now))
			return PGRAC_FENCED_DISPATCH_ERROR;
		(void) poll(NULL, 0, 10);
	}
	if (!coordinator->context->available)
		return PGRAC_FENCED_DISPATCH_ERROR;
	if (reload_during_operation)
	{
		(void) pgrac_fenced_coordinator_shutdown(coordinator, 17);
		(void) pgrac_fenced_rejoin_coordinator_shutdown(rejoin);
		return PGRAC_FENCED_DISPATCH_ERROR;
	}
	if (failed)
		return PGRAC_FENCED_DISPATCH_ERROR;
	return reload_requested ? PGRAC_FENCED_DISPATCH_RELOAD :
		PGRAC_FENCED_DISPATCH_STOP;
}

int
main(int argc, char **argv)
{
	PgracFencedOperationContextV1 operation_context;
	PgracFencedJournalScanState journal_state;
	PgracFencedJournalReconcileState reconcile;
	PgracFencedCoordinatorV1 coordinator;
	PgracFencedRejoinCoordinatorV1 rejoin_coordinator;
	PgracFencedConfigV1 *config = NULL;
	PgracFencedConfigV1 *candidate_config = NULL;
	const PgracFencedProviderOpsV1 *provider = NULL;
	const PgracFencedProviderOpsV1 *candidate_provider = NULL;
	uint8 config_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
	uint8 candidate_digest[PGRAC_FENCED_CONFIG_DIGEST_BYTES];
	uint8 daemon_boot_id[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	struct sigaction action;
	dev_t socket_device = 0;
	dev_t admin_socket_device = 0;
	ino_t socket_inode = 0;
	ino_t admin_socket_inode = 0;
	int runtime_directory_fd = -1;
	int journal_directory_fd = -1;
	int journal_fd = -1;
	int listen_fd = -1;
	int admin_listen_fd = -1;
	int rc = PGRAC_FENCED_EXIT_UNAVAILABLE;
	uint32 sealed_count = 0;
	PgracFencedConfigReloadDecision reload_decision;
	PgracFencedDispatchResult dispatch_result;
	bool coordinator_initialized = false;
	bool rejoin_coordinator_initialized = false;
	bool operation_context_initialized = false;
	bool provider_valid = false;

	(void) argc;
	(void) argv;
	memset(config_digest, 0, sizeof(config_digest));
	memset(candidate_digest, 0, sizeof(candidate_digest));
	memset(daemon_boot_id, 0, sizeof(daemon_boot_id));
	if (geteuid() != 0)
	{
		fprintf(stderr, "pgrac-fenced must run as root\n");
		return PGRAC_FENCED_EXIT_UNAVAILABLE;
	}
	config = (PgracFencedConfigV1 *) calloc(1, sizeof(*config));
	if (config == NULL || !read_root_config(config, config_digest))
	{
		fprintf(stderr, "pgrac-fenced configuration is unavailable\n");
		goto done;
	}
	provider = pgrac_fenced_provider_lookup(config->provider_id);
	if (provider == NULL || !pgrac_fenced_provider_ops_valid(provider, false) ||
		provider->provider_id != config->provider_id ||
		provider->abi_version != config->provider_abi)
	{
		fprintf(stderr, "pgrac-fenced production provider is unavailable\n");
		goto done;
	}
	provider_valid = true;
	if (!open_active_journal(config, config_digest, &journal_directory_fd,
			&journal_fd, &journal_state, &reconcile, &sealed_count) ||
		!pg_strong_random(daemon_boot_id, sizeof(daemon_boot_id)) ||
		!bytes_nonzero(daemon_boot_id, sizeof(daemon_boot_id)) ||
		!pgrac_fenced_operation_context_init(&operation_context, config,
			provider, false, config_digest, daemon_boot_id, journal_fd,
			&journal_state))
	{
		fprintf(stderr, "pgrac-fenced secure runtime bootstrap failed\n");
		goto done;
	}
	operation_context_initialized = true;
	if (!pgrac_fenced_operation_enable_rotation(&operation_context,
			journal_directory_fd, sealed_count) ||
		!pgrac_fenced_operation_reconcile_startup(&operation_context,
			&reconcile) ||
		!pgrac_fenced_coordinator_init(&coordinator, &operation_context) ||
		!pgrac_fenced_rejoin_coordinator_init(&rejoin_coordinator,
			&operation_context, &coordinator))
	{
		fprintf(stderr, "pgrac-fenced secure runtime bootstrap failed\n");
		goto done;
	}
	coordinator_initialized = true;
	rejoin_coordinator_initialized = true;
	if (!create_db_socket(config, &runtime_directory_fd, &listen_fd,
			&socket_device, &socket_inode) ||
		!create_admin_socket(runtime_directory_fd, &admin_listen_fd,
			&admin_socket_device, &admin_socket_inode))
	{
		fprintf(stderr, "pgrac-fenced secure runtime bootstrap failed\n");
		goto done;
	}
	memset(&action, 0, sizeof(action));
	action.sa_handler = request_stop;
	sigemptyset(&action.sa_mask);
	if (sigaction(SIGTERM, &action, NULL) != 0 ||
		sigaction(SIGINT, &action, NULL) != 0)
	{
		fprintf(stderr, "pgrac-fenced signal setup failed\n");
		goto done;
	}
	action.sa_handler = request_reload;
	if (sigaction(SIGHUP, &action, NULL) != 0 ||
		signal(SIGPIPE, SIG_IGN) == SIG_ERR)
	{
		fprintf(stderr, "pgrac-fenced signal setup failed\n");
		goto done;
	}
	for (;;)
	{
		dispatch_result = run_dispatch_loop(listen_fd, admin_listen_fd,
			&coordinator, &rejoin_coordinator);
		if (dispatch_result == PGRAC_FENCED_DISPATCH_STOP)
		{
			rc = 0;
			break;
		}
		if (dispatch_result != PGRAC_FENCED_DISPATCH_RELOAD)
			break;
		reload_requested = 0;
		candidate_config = (PgracFencedConfigV1 *) calloc(1,
			sizeof(*candidate_config));
		if (candidate_config == NULL ||
			!read_root_config(candidate_config, candidate_digest))
			break;
		reload_decision = pgrac_fenced_config_reload_decide_v1(config,
			config_digest, candidate_config, candidate_digest);
		if (reload_decision == PGRAC_FENCED_CONFIG_RELOAD_UNCHANGED)
		{
			memset(candidate_config, 0, sizeof(*candidate_config));
			free(candidate_config);
			candidate_config = NULL;
			memset(candidate_digest, 0, sizeof(candidate_digest));
			continue;
		}
		if (reload_decision != PGRAC_FENCED_CONFIG_RELOAD_ADVANCE)
			break;
		candidate_provider = pgrac_fenced_provider_lookup(
			candidate_config->provider_id);
		if (candidate_provider == NULL || candidate_provider != provider ||
			!pgrac_fenced_provider_ops_valid(candidate_provider, false) ||
			candidate_provider->abi_version != candidate_config->provider_abi ||
			!pgrac_fenced_operation_prepare_mapping_reload(&operation_context,
				candidate_config, candidate_provider, candidate_digest) ||
			!pgrac_fenced_rejoin_coordinator_shutdown(&rejoin_coordinator) ||
			!pgrac_fenced_coordinator_shutdown(&coordinator, 17))
			break;
		(void) close(listen_fd);
		listen_fd = -1;
		(void) close(admin_listen_fd);
		admin_listen_fd = -1;
		unlink_own_socket(runtime_directory_fd, "pgrac-fenced.sock",
			socket_device, socket_inode);
		unlink_own_socket(runtime_directory_fd, "pgrac-fenced-admin.sock",
			admin_socket_device, admin_socket_inode);
		(void) close(runtime_directory_fd);
		runtime_directory_fd = -1;
		if (!pgrac_fenced_operation_activate_mapping_reload(
				&operation_context))
			break;
		memset(config, 0, sizeof(*config));
		free(config);
		config = candidate_config;
		candidate_config = NULL;
		provider = candidate_provider;
		candidate_provider = NULL;
		memcpy(config_digest, candidate_digest, sizeof(config_digest));
		memset(candidate_digest, 0, sizeof(candidate_digest));
		if (!pgrac_fenced_coordinator_init(&coordinator, &operation_context) ||
			!pgrac_fenced_rejoin_coordinator_init(&rejoin_coordinator,
				&operation_context, &coordinator) ||
			!create_db_socket(config, &runtime_directory_fd, &listen_fd,
				&socket_device, &socket_inode) ||
			!create_admin_socket(runtime_directory_fd, &admin_listen_fd,
				&admin_socket_device, &admin_socket_inode))
			break;
	}

done:
	if (listen_fd >= 0)
		(void) close(listen_fd);
	if (admin_listen_fd >= 0)
		(void) close(admin_listen_fd);
	if (rejoin_coordinator_initialized &&
		!pgrac_fenced_rejoin_coordinator_shutdown(&rejoin_coordinator))
		rc = PGRAC_FENCED_EXIT_UNAVAILABLE;
	if (coordinator_initialized &&
		!pgrac_fenced_coordinator_shutdown(&coordinator, 16))
		rc = PGRAC_FENCED_EXIT_UNAVAILABLE;
	if (runtime_directory_fd >= 0)
	{
		unlink_own_socket(runtime_directory_fd, "pgrac-fenced.sock",
			socket_device, socket_inode);
		unlink_own_socket(runtime_directory_fd, "pgrac-fenced-admin.sock",
			admin_socket_device, admin_socket_inode);
		(void) close(runtime_directory_fd);
	}
	if (provider_valid)
		provider->shutdown();
	if (operation_context_initialized)
	{
		if (operation_context.journal_fd >= 0)
			(void) close(operation_context.journal_fd);
		journal_fd = -1;
	}
	else if (journal_fd >= 0)
		(void) close(journal_fd);
	if (journal_directory_fd >= 0)
		(void) close(journal_directory_fd);
	if (config != NULL)
	{
		memset(config, 0, sizeof(*config));
		free(config);
	}
	if (candidate_config != NULL)
	{
		memset(candidate_config, 0, sizeof(*candidate_config));
		free(candidate_config);
	}
	memset(config_digest, 0, sizeof(config_digest));
	memset(candidate_digest, 0, sizeof(candidate_digest));
	memset(daemon_boot_id, 0, sizeof(daemon_boot_id));
	return rc;
}
