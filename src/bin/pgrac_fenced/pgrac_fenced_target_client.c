/*-------------------------------------------------------------------------
 * pgrac_fenced_target_client.c
 *    Fixed protected client, no shell or independent isolation authority.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_target_client.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "pgrac_fenced_target_client.h"

#ifdef __linux__
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int
remaining_ms(uint64 deadline)
{
	struct timespec now;
	uint64 current, remaining;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0
		|| (uint64)now.tv_sec > (UINT64_MAX - UINT64_C(999999999)) / UINT64_C(1000000000))
		return 0;
	current = (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
	if (current >= deadline)
		return 0;
	remaining = (deadline - current) / UINT64_C(1000000);
	if ((deadline - current) % UINT64_C(1000000) != 0)
		remaining++;
	return remaining > INT_MAX ? INT_MAX : (int)remaining;
}

static bool
owner_process(void)
{
	struct sigaction action;
	struct dirent *entry;
	DIR *tasks;
	unsigned count = 0;
	if (geteuid() != 0 || getpgrp() != getpid() || sigaction(SIGCHLD, NULL, &action) != 0
		|| action.sa_handler != SIG_DFL || (action.sa_flags & SA_NOCLDWAIT) != 0)
		return false;
	tasks = opendir("/proc/self/task");
	if (tasks == NULL)
		return false;
	errno = 0;
	while ((entry = readdir(tasks)) != NULL)
		if (entry->d_name[0] != '.')
			count++;
	if (errno != 0)
		count = 0;
	closedir(tasks);
	return count == 1;
}

static bool
directory_secure(int fd, bool ancestor)
{
	struct stat info;
	return fstat(fd, &info) == 0 && S_ISDIR(info.st_mode) && info.st_uid == 0
		   && ((info.st_mode & 022) == 0 || (ancestor && (info.st_mode & S_ISVTX) != 0));
}

static int
protected_path(const char *path, bool directory)
{
	char copy[MAXPGPATH], *part, *next;
	int fd, child;
	struct stat info;
	if (path == NULL || path[0] != '/' || strnlen(path, sizeof(copy)) >= sizeof(copy)
		|| path[1] == '\0' || path[strlen(path) - 1] == '/')
		return -1;
	memcpy(copy, path, strlen(path) + 1);
	fd = open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		return -1;
	for (part = copy + 1;; part = next + 1) {
		next = strchr(part, '/');
		if (next != NULL)
			*next = '\0';
		if (*part == '\0' || strcmp(part, ".") == 0 || strcmp(part, "..") == 0
			|| !directory_secure(fd, true))
			break;
		child = openat(fd, part,
					   O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK
						   | (next != NULL || directory ? O_DIRECTORY : 0));
		close(fd);
		fd = child;
		if (fd < 0)
			return -1;
		if (next == NULL) {
			if (directory ? directory_secure(fd, false)
						  : fstat(fd, &info) == 0 && S_ISREG(info.st_mode) && info.st_uid == 0
								&& (info.st_mode & 07777) == 0600 && info.st_nlink == 1)
				return fd;
			break;
		}
	}
	close(fd);
	return -1;
}

static bool
paths_secure(const PgracFencedTargetClientPaths *paths, char script[MAXPGPATH])
{
	struct stat info;
	int directory = -1, config = -1, entry = -1;
	int size;
	bool result = false;
	if (paths == NULL)
		return false;
	directory = protected_path(paths->bundle_directory, true);
	config = protected_path(paths->config_file, false);
	if (directory < 0 || config < 0)
		goto done;
	entry
		= openat(directory, "pgrac-target-service", O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
	if (entry < 0 || fstat(entry, &info) != 0 || !S_ISREG(info.st_mode) || info.st_uid != 0
		|| (info.st_mode & 022) != 0 || info.st_nlink != 1 || info.st_size < 1
		|| info.st_size > 65536)
		goto done;
	size = snprintf(script, MAXPGPATH, "%s/pgrac-target-service", paths->bundle_directory);
	result = size > 0 && size < MAXPGPATH;
done:
	if (directory >= 0)
		close(directory);
	if (config >= 0)
		close(config);
	if (entry >= 0)
		close(entry);
	return result;
}

static int
above_stdio(int fd)
{
	int result;
	if (fd < 0 || fd >= 3)
		return fd;
	result = fcntl(fd, F_DUPFD_CLOEXEC, 3);
	close(fd);
	return result;
}

static void
exec_client(int stream, int nullfd, pid_t parent, char *script, const char *config, char *deadline)
{
	struct sigaction action;
	sigset_t empty;
	const int signals[] = { SIGTERM, SIGINT, SIGHUP, SIGPIPE, SIGCHLD };
	char *const arguments[]
		= { "/usr/bin/python3", "-I",	  "-B", script, "--request", "--config", (char *)config,
			"--deadline-ns",	deadline, NULL };
	char *const environment[] = { "LC_ALL=C", "PATH=/usr/bin:/bin", NULL };
	if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) != 0 || getppid() != parent
		|| dup2(stream, STDIN_FILENO) < 0 || dup2(stream, STDOUT_FILENO) < 0
		|| dup2(nullfd, STDERR_FILENO) < 0)
		_exit(125);
	memset(&action, 0, sizeof(action));
	action.sa_handler = SIG_DFL;
	sigemptyset(&action.sa_mask);
	for (size_t n = 0; n < lengthof(signals); n++)
		if (sigaction(signals[n], &action, NULL) != 0)
			_exit(125);
	sigemptyset(&empty);
	if (sigprocmask(SIG_SETMASK, &empty, NULL) != 0
		|| syscall(SYS_close_range, 3U, UINT_MAX, 0) != 0)
		_exit(125);
	execve(arguments[0], arguments, environment);
	_exit(125);
}

typedef struct ClientChild {
	int stream;
	int pidfd;
	pid_t pid;
	bool reaped;
	int status;
} ClientChild;

static bool
exchange_stream(ClientChild *child, const char *command, size_t size, uint64 deadline, char *reply,
				size_t *length)
{
	size_t sent = 0, received = 0;
	bool eof = false, write_closed = false;
	for (;;) {
		struct pollfd descriptors[2];
		int timeout = remaining_ms(deadline);
		int ready;
		ssize_t count;
		if (timeout == 0)
			return false;
		if (eof && child->reaped) {
			if (sent != size || received == 0 || !WIFEXITED(child->status)
				|| WEXITSTATUS(child->status) != 0)
				return false;
			*length = received;
			return true;
		}
		if (sent == size && !write_closed) {
			if (shutdown(child->stream, SHUT_WR) != 0)
				return false;
			write_closed = true;
		}
		descriptors[0]
			= (struct pollfd){ eof ? -1 : child->stream, POLLIN | (sent < size ? POLLOUT : 0), 0 };
		descriptors[1] = (struct pollfd){ child->reaped ? -1 : child->pidfd, POLLIN, 0 };
		ready = poll(descriptors, 2, timeout);
		if (ready < 0 && errno == EINTR)
			continue;
		if (ready <= 0 || (descriptors[0].revents & (POLLERR | POLLNVAL))
			|| (descriptors[1].revents & POLLNVAL))
			return false;
		if (descriptors[0].revents & POLLOUT) {
			count = send(child->stream, command + sent, size - sent, MSG_NOSIGNAL);
			if (count > 0)
				sent += count;
			else if (count == 0 || (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK))
				return false;
		}
		if (descriptors[0].revents & (POLLIN | POLLHUP)) {
			count = recv(child->stream, reply + received,
						 PGRAC_TARGET_REPLY_MAX_BYTES + 1 - received, 0);
			if (count > 0) {
				received += count;
				if (received > PGRAC_TARGET_REPLY_MAX_BYTES)
					return false;
			} else if (count == 0)
				eof = true;
			else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
				return false;
		}
		if (descriptors[1].revents != 0) {
			pid_t waited = waitpid(child->pid, &child->status, WNOHANG);
			if (waited == child->pid)
				child->reaped = true;
			else if (waited < 0 && errno != EINTR)
				return false;
		}
	}
}

static void
stop_child(ClientChild *child)
{
	if (!child->reaped) {
		if (child->pidfd >= 0)
			(void)syscall(SYS_pidfd_send_signal, child->pidfd, SIGKILL, NULL, 0);
		else
			/*
			 * No reaper has run; default SIGCHLD preserves this child's PID.
			 * This is refusal cleanup, never a success path.
			 */
			(void)kill(child->pid, SIGKILL);
		(void)waitpid(child->pid, &child->status, WNOHANG);
	}
	if (child->pidfd >= 0)
		close(child->pidfd);
}

static bool
invoke_client(const PgracFencedTargetClientPaths *paths, char *script, const char *command,
			  size_t command_length, uint64 deadline, char *reply, size_t *length)
{
	int sockets[2] = { -1, -1 }, nullfd = -1;
	ClientChild child = { .stream = -1, .pidfd = -1, .pid = -1 };
	char deadline_text[32];
	pid_t parent = getpid();
	bool result = false;
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets) != 0)
		return false;
	sockets[0] = above_stdio(sockets[0]);
	sockets[1] = above_stdio(sockets[1]);
	nullfd = above_stdio(open("/dev/null", O_RDWR | O_CLOEXEC));
	if (sockets[0] < 0 || sockets[1] < 0 || nullfd < 0 || remaining_ms(deadline) == 0)
		goto done;
	snprintf(deadline_text, sizeof(deadline_text), UINT64_FORMAT, deadline);
	child.pid = fork();
	if (child.pid == 0)
		exec_client(sockets[1], nullfd, parent, script, paths->config_file, deadline_text);
	if (child.pid < 0)
		goto done;
	child.stream = sockets[0];
	close(sockets[1]);
	sockets[1] = -1;
	child.pidfd = (int)syscall(SYS_pidfd_open, child.pid, 0);
	if (child.pidfd >= 0)
		result = exchange_stream(&child, command, command_length, deadline, reply, length);
	stop_child(&child);
done:
	for (size_t n = 0; n < lengthof(sockets); n++)
		if (sockets[n] >= 0)
			close(sockets[n]);
	if (nullfd >= 0)
		close(nullfd);
	return result;
}
#endif

bool
pgrac_fenced_target_client_exchange(const PgracFencedTargetClientPaths *paths, const char *command,
									size_t command_length, uint64 deadline_mono_ns, char *output,
									size_t capacity, size_t *length)
{
#ifdef __linux__
	char script[MAXPGPATH];
	char reply[PGRAC_TARGET_REPLY_MAX_BYTES + 1];
	size_t received = 0;
#endif
	if (output != NULL && capacity != 0)
		output[0] = '\0';
	if (length != NULL)
		*length = 0;
#ifdef __linux__
	if (length == NULL || output == NULL || capacity < sizeof(reply) || command == NULL
		|| command_length == 0 || command_length > PGRAC_TARGET_COMMAND_MAX_BYTES
		|| remaining_ms(deadline_mono_ns) == 0 || !owner_process() || !paths_secure(paths, script)
		|| !invoke_client(paths, script, command, command_length, deadline_mono_ns, reply,
						  &received)
		|| remaining_ms(deadline_mono_ns) == 0)
		return false;
	memcpy(output, reply, received);
	output[received] = '\0';
	*length = received;
	return true;
#else
	(void)paths;
	(void)command;
	(void)command_length;
	(void)deadline_mono_ns;
	return false;
#endif
}
