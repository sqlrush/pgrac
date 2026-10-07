/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_ipmi.c
 *    Fixed IPMI LAN+ production provider.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "common/cryptohash.h"
#include "pgrac_fenced_ipmi.h"

static uint16
ipmi_get_u16_le(const uint8 *bytes)
{
	return (uint16) bytes[0] | ((uint16) bytes[1] << 8);
}

static bool
ipmi_path_is_canonical(const uint8 *path, size_t len)
{
	size_t component_start;
	size_t i;

	if (len < 2 || len > PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX ||
		path[0] != '/' || path[len - 1] == '/')
		return false;
	component_start = 1;
	for (i = 1; i <= len; i++)
	{
		size_t component_len;

		if (i < len && path[i] == '\0')
			return false;
		if (i < len && path[i] != '/')
			continue;
		component_len = i - component_start;
		if (component_len == 0 ||
			(component_len == 1 && path[component_start] == '.') ||
			(component_len == 2 && path[component_start] == '.' &&
			 path[component_start + 1] == '.'))
			return false;
		component_start = i + 1;
	}
	return true;
}

bool
pgrac_fenced_ipmi_adapter_parse(const uint8 *bytes, size_t len,
							PgracFencedIpmiAdapterV1 *out)
{
	uint16 address_family;
	uint16 path_len;
	size_t i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL ||
		len < PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + 1 ||
		len > PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES ||
		memcmp(bytes, PGRAC_FENCED_IPMI_ADAPTER_MAGIC, 4) != 0 ||
		ipmi_get_u16_le(bytes + 4) != PGRAC_FENCED_IPMI_ADAPTER_VERSION ||
		ipmi_get_u16_le(bytes + 6) != len)
		return false;
	address_family = ipmi_get_u16_le(bytes + 8);
	path_len = ipmi_get_u16_le(bytes + 30);
	if ((address_family != 4 && address_family != 6) ||
		ipmi_get_u16_le(bytes + 10) == 0 || bytes[28] == 0 ||
		bytes[29] != 0 || path_len == 0 ||
		path_len > PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX ||
		len != PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES + path_len ||
		!ipmi_path_is_canonical(
			bytes + PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES, path_len))
		return false;
	if (address_family == 4)
	{
		for (i = 4; i < 16; i++)
		{
			if (bytes[12 + i] != 0)
				return false;
		}
	}
	out->address_family = address_family;
	out->port = ipmi_get_u16_le(bytes + 10);
	memcpy(out->address, bytes + 12, sizeof(out->address));
	out->cipher_suite = bytes[28];
	out->executable_path_len = path_len;
	memcpy(out->executable_sha256, bytes + 32,
		   sizeof(out->executable_sha256));
	memcpy(out->executable_path,
		   bytes + PGRAC_FENCED_IPMI_ADAPTER_FIXED_BYTES, path_len);
	out->executable_path[path_len] = '\0';
	return true;
}

bool
pgrac_fenced_ipmi_executable_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISREG(st->st_mode) && st->st_uid == 0 &&
		(st->st_mode & 0022) == 0;
}

bool
pgrac_fenced_ipmi_credential_stat_secure(const struct stat *st)
{
	return st != NULL && S_ISREG(st->st_mode) && st->st_uid == 0 &&
		st->st_gid == 0 && (st->st_mode & 07777) == 0600;
}

bool
pgrac_fenced_ipmi_username_parse(
	const uint8 *bytes, size_t len,
	char out[PGRAC_FENCED_IPMI_USERNAME_MAX + 1])
{
	size_t username_len;
	size_t i;

	if (out == NULL)
		return false;
	out[0] = '\0';
	if (bytes == NULL || len < 2 ||
		len > PGRAC_FENCED_IPMI_USERNAME_MAX + 1 || bytes[len - 1] != '\n')
		return false;
	username_len = len - 1;
	for (i = 0; i < username_len; i++)
	{
		if (bytes[i] < 0x21 || bytes[i] > 0x7e)
			return false;
	}
	memcpy(out, bytes, username_len);
	out[username_len] = '\0';
	return true;
}

bool
pgrac_fenced_ipmi_password_validate(const uint8 *bytes, size_t len)
{
	size_t password_len;
	size_t i;

	if (bytes == NULL || len < 2 ||
		len > PGRAC_FENCED_IPMI_PASSWORD_MAX + 1 || bytes[len - 1] != '\n')
		return false;
	password_len = len - 1;
	for (i = 0; i < password_len; i++)
	{
		if (bytes[i] == '\0' || bytes[i] == '\n' || bytes[i] == '\r')
			return false;
	}
	return true;
}

bool
pgrac_fenced_ipmi_credential_paths(
	const uint8 target_uuid[16], char *user_path, size_t user_path_size,
	char *password_path, size_t password_path_size)
{
	static const char hex[] = "0123456789abcdef";
	static const char prefix[] = "/etc/pgrac/credentials/ipmi-";
	char uuid_hex[33];
	bool nonzero = false;
	int written;
	size_t i;

	if (target_uuid == NULL || user_path == NULL || password_path == NULL)
		return false;
	for (i = 0; i < 16; i++)
	{
		nonzero = nonzero || target_uuid[i] != 0;
		uuid_hex[i * 2] = hex[target_uuid[i] >> 4];
		uuid_hex[i * 2 + 1] = hex[target_uuid[i] & 0x0f];
	}
	uuid_hex[32] = '\0';
	if (!nonzero)
		return false;
	written = snprintf(user_path, user_path_size, "%s%s.user", prefix,
					   uuid_hex);
	if (written < 0 || (size_t) written >= user_path_size)
		return false;
	written = snprintf(password_path, password_path_size, "%s%s.password",
					   prefix, uuid_hex);
	return written >= 0 && (size_t) written < password_path_size;
}

static bool
ipmi_directory_stat_secure(const struct stat *st, bool credentials_directory)
{
	if (st == NULL || !S_ISDIR(st->st_mode) || st->st_uid != 0 ||
		st->st_gid != 0 || (st->st_mode & 0022) != 0)
		return false;
	return !credentials_directory || (st->st_mode & 07777) == 0700;
}

static int
ipmi_directory_openat_secure(int parent_fd, const char *name,
						 bool credentials_directory)
{
	struct stat st;
	int fd;

	fd = openat(parent_fd, name,
		O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		return -1;
	if (fstat(fd, &st) != 0 ||
		!ipmi_directory_stat_secure(&st, credentials_directory))
	{
		(void) close(fd);
		return -1;
	}
	return fd;
}

static int
ipmi_credentials_directory_open(void)
{
	struct stat st;
	int root_fd = -1;
	int etc_fd = -1;
	int pgrac_fd = -1;
	int credentials_fd = -1;

	root_fd = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (root_fd < 0 || fstat(root_fd, &st) != 0 ||
		!ipmi_directory_stat_secure(&st, false))
		goto done;
	etc_fd = ipmi_directory_openat_secure(root_fd, "etc", false);
	if (etc_fd < 0)
		goto done;
	pgrac_fd = ipmi_directory_openat_secure(etc_fd, "pgrac", false);
	if (pgrac_fd < 0)
		goto done;
	credentials_fd = ipmi_directory_openat_secure(pgrac_fd, "credentials",
		true);

done:
	if (pgrac_fd >= 0)
		(void) close(pgrac_fd);
	if (etc_fd >= 0)
		(void) close(etc_fd);
	if (root_fd >= 0)
		(void) close(root_fd);
	return credentials_fd;
}

static bool
ipmi_credential_identity_same(const struct stat *before,
						  const struct stat *after)
{
	return before->st_dev == after->st_dev &&
		before->st_ino == after->st_ino &&
		before->st_mode == after->st_mode &&
		before->st_uid == after->st_uid &&
		before->st_gid == after->st_gid &&
		before->st_size == after->st_size;
}

static bool
ipmi_credential_read(int directory_fd, const char *name, bool username_file,
					 char username[PGRAC_FENCED_IPMI_USERNAME_MAX + 1])
{
	uint8 bytes[PGRAC_FENCED_IPMI_PASSWORD_MAX + 2];
	struct stat before;
	struct stat after;
	ssize_t got;
	size_t max_payload = username_file ? PGRAC_FENCED_IPMI_USERNAME_MAX :
		PGRAC_FENCED_IPMI_PASSWORD_MAX;
	size_t used = 0;
	int fd = -1;
	bool ok = false;

	memset(bytes, 0, sizeof(bytes));
	fd = openat(directory_fd, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &before) != 0 ||
		!pgrac_fenced_ipmi_credential_stat_secure(&before) ||
		before.st_size < 2 || before.st_size > (off_t) (max_payload + 1))
		goto done;
	while (used < (size_t) before.st_size)
	{
		do
		{
			got = read(fd, bytes + used, (size_t) before.st_size - used);
		} while (got < 0 && errno == EINTR);
		if (got <= 0)
			goto done;
		used += (size_t) got;
	}
	do
	{
		got = read(fd, bytes + used, 1);
	} while (got < 0 && errno == EINTR);
	if (got != 0 || fstat(fd, &after) != 0 ||
		!ipmi_credential_identity_same(&before, &after))
		goto done;
	if (username_file)
		ok = pgrac_fenced_ipmi_username_parse(bytes, used, username);
	else
		ok = pgrac_fenced_ipmi_password_validate(bytes, used);

done:
	memset(bytes, 0, sizeof(bytes));
	if (fd >= 0)
		(void) close(fd);
	return ok;
}

bool
pgrac_fenced_ipmi_credentials_load(
	const uint8 target_uuid[16], PgracFencedIpmiCredentialsV1 *out)
{
	const char *user_name;
	const char *password_name;
	int directory_fd = -1;
	bool ok = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!pgrac_fenced_ipmi_credential_paths(target_uuid, out->user_path,
			sizeof(out->user_path), out->password_path,
			sizeof(out->password_path)))
		goto done;
	user_name = strrchr(out->user_path, '/');
	password_name = strrchr(out->password_path, '/');
	if (user_name == NULL || password_name == NULL)
		goto done;
	directory_fd = ipmi_credentials_directory_open();
	if (directory_fd < 0 ||
		!ipmi_credential_read(directory_fd, user_name + 1, true,
			out->username) ||
		!ipmi_credential_read(directory_fd, password_name + 1, false, NULL))
		goto done;
	ok = true;

done:
	if (directory_fd >= 0)
		(void) close(directory_fd);
	if (!ok)
		memset(out, 0, sizeof(*out));
	return ok;
}

bool
pgrac_fenced_ipmi_sha256_fd(int fd, uint8 out[32])
{
	pg_cryptohash_ctx *ctx;
	uint8 buffer[8192];
	ssize_t got;
	bool ok = false;

	if (fd < 0 || out == NULL || lseek(fd, 0, SEEK_SET) < 0)
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL || pg_cryptohash_init(ctx) < 0)
		goto done;
	for (;;)
	{
		do
		{
			got = read(fd, buffer, sizeof(buffer));
		} while (got < 0 && errno == EINTR);
		if (got < 0)
			goto done;
		if (got == 0)
			break;
		if (pg_cryptohash_update(ctx, buffer, (size_t) got) < 0)
			goto done;
	}
	ok = pg_cryptohash_final(ctx, out, 32) >= 0;

done:
	if (ctx != NULL)
		pg_cryptohash_free(ctx);
	memset(buffer, 0, sizeof(buffer));
	if (lseek(fd, 0, SEEK_SET) < 0)
		ok = false;
	if (!ok)
		memset(out, 0, 32);
	return ok;
}

int
pgrac_fenced_ipmi_executable_open(const PgracFencedIpmiAdapterV1 *adapter)
{
	char component[PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX + 1];
	uint8 digest[32];
	struct stat st;
	size_t component_start;
	size_t i;
	int current_fd;
	int next_fd = -1;

	if (adapter == NULL || adapter->executable_path_len == 0 ||
		adapter->executable_path_len > PGRAC_FENCED_IPMI_ADAPTER_PATH_MAX ||
		adapter->executable_path[adapter->executable_path_len] != '\0' ||
		!ipmi_path_is_canonical((const uint8 *) adapter->executable_path,
			adapter->executable_path_len))
		return -1;
	current_fd = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (current_fd < 0)
		return -1;
	component_start = 1;
	for (i = 1; i <= adapter->executable_path_len; i++)
	{
		size_t component_len;
		bool final_component;
		int flags;

		if (i < adapter->executable_path_len &&
			adapter->executable_path[i] != '/')
			continue;
		component_len = i - component_start;
		memcpy(component, adapter->executable_path + component_start,
			   component_len);
		component[component_len] = '\0';
		final_component = i == adapter->executable_path_len;
		flags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC;
		if (!final_component)
			flags |= O_DIRECTORY;
		next_fd = openat(current_fd, component, flags);
		(void) close(current_fd);
		if (next_fd < 0)
			return -1;
		current_fd = next_fd;
		component_start = i + 1;
	}
	if (fstat(current_fd, &st) != 0 ||
		!pgrac_fenced_ipmi_executable_stat_secure(&st) ||
		!pgrac_fenced_ipmi_sha256_fd(current_fd, digest) ||
		memcmp(digest, adapter->executable_sha256, sizeof(digest)) != 0)
	{
		memset(digest, 0, sizeof(digest));
		(void) close(current_fd);
		return -1;
	}
	memset(digest, 0, sizeof(digest));
	return current_fd;
}

static bool
ipmi_username_is_valid(const char *username)
{
	size_t len;
	size_t i;

	if (username == NULL)
		return false;
	len = strlen(username);
	if (len == 0 || len > PGRAC_FENCED_IPMI_USERNAME_MAX)
		return false;
	for (i = 0; i < len; i++)
	{
		if ((unsigned char) username[i] < 0x21 ||
			(unsigned char) username[i] > 0x7e)
			return false;
	}
	return true;
}

static bool
ipmi_password_path_is_exact(const char *path)
{
	static const char prefix[] = "/etc/pgrac/credentials/ipmi-";
	static const char suffix[] = ".password";
	size_t prefix_len = sizeof(prefix) - 1;
	size_t suffix_len = sizeof(suffix) - 1;
	size_t len;
	size_t i;

	if (path == NULL)
		return false;
	len = strlen(path);
	if (len != prefix_len + 32 + suffix_len ||
		memcmp(path, prefix, prefix_len) != 0 ||
		memcmp(path + prefix_len + 32, suffix, suffix_len) != 0)
		return false;
	for (i = 0; i < 32; i++)
	{
		unsigned char ch = (unsigned char) path[prefix_len + i];

		if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
			return false;
	}
	return true;
}

bool
pgrac_fenced_ipmi_invocation_build(
	const PgracFencedIpmiAdapterV1 *adapter, const char *username,
	const char *password_path, PgracFencedIpmiCommand command,
	PgracFencedIpmiInvocationV1 *out)
{
	const char *command0;
	const char *command1;
	const char *command2;
	int af;
	size_t username_len;
	size_t password_path_len;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (adapter == NULL ||
		(adapter->address_family != 4 && adapter->address_family != 6) ||
		adapter->port == 0 || adapter->cipher_suite == 0 ||
		!ipmi_username_is_valid(username) ||
		!ipmi_password_path_is_exact(password_path))
		return false;
	switch (command)
	{
		case PGRAC_FENCED_IPMI_COMMAND_GUID:
			command0 = "raw";
			command1 = "0x06";
			command2 = "0x37";
			break;
		case PGRAC_FENCED_IPMI_COMMAND_OFF:
			command0 = "chassis";
			command1 = "power";
			command2 = "off";
			break;
		case PGRAC_FENCED_IPMI_COMMAND_STATUS:
			command0 = "chassis";
			command1 = "power";
			command2 = "status";
			break;
		case PGRAC_FENCED_IPMI_COMMAND_ON:
			command0 = "chassis";
			command1 = "power";
			command2 = "on";
			break;
		default:
			return false;
	}
	af = adapter->address_family == 4 ? AF_INET : AF_INET6;
	if (inet_ntop(af, adapter->address, out->address,
				  sizeof(out->address)) == NULL ||
		snprintf(out->port, sizeof(out->port), "%u",
				 (unsigned int) adapter->port) < 0 ||
		snprintf(out->cipher_suite, sizeof(out->cipher_suite), "%u",
				 (unsigned int) adapter->cipher_suite) < 0)
		return false;
	username_len = strlen(username);
	password_path_len = strlen(password_path);
	memcpy(out->username, username, username_len + 1);
	memcpy(out->password_path, password_path, password_path_len + 1);
	out->argv[0] = "ipmitool";
	out->argv[1] = "-I";
	out->argv[2] = "lanplus";
	out->argv[3] = "-H";
	out->argv[4] = out->address;
	out->argv[5] = "-p";
	out->argv[6] = out->port;
	out->argv[7] = "-C";
	out->argv[8] = out->cipher_suite;
	out->argv[9] = "-U";
	out->argv[10] = out->username;
	out->argv[11] = "-f";
	out->argv[12] = out->password_path;
	out->argv[13] = (char *) command0;
	out->argv[14] = (char *) command1;
	out->argv[15] = (char *) command2;
	out->argv[16] = NULL;
	out->argc = 16;
	return true;
}

static int
ipmi_lower_hex_nibble(uint8 ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

bool
pgrac_fenced_ipmi_guid_result_parse(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len, uint8 out_uuid[16])
{
	bool nonzero = false;
	size_t i;

	if (out_uuid == NULL)
		return false;
	memset(out_uuid, 0, 16);
	if (exit_code != 0 || stdout_bytes == NULL || stdout_len != 49 ||
		stderr_len != 0 || (stderr_len > 0 && stderr_bytes == NULL) ||
		stdout_bytes[48] != '\n')
		return false;
	for (i = 0; i < 16; i++)
	{
		int high;
		int low;
		size_t offset = i * 3;

		if (stdout_bytes[offset] != ' ')
			return false;
		high = ipmi_lower_hex_nibble(stdout_bytes[offset + 1]);
		low = ipmi_lower_hex_nibble(stdout_bytes[offset + 2]);
		if (high < 0 || low < 0)
			return false;
		out_uuid[i] = (uint8) ((high << 4) | low);
		nonzero = nonzero || out_uuid[i] != 0;
	}
	if (!nonzero)
		memset(out_uuid, 0, 16);
	return nonzero;
}

bool
pgrac_fenced_ipmi_power_result_parse(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len,
	PgracFencedTargetState *out_state)
{
	static const uint8 on[] = "Chassis Power is on\n";
	static const uint8 off[] = "Chassis Power is off\n";

	if (out_state == NULL)
		return false;
	*out_state = PGRAC_FENCED_TARGET_UNKNOWN;
	if (exit_code != 0 || stdout_bytes == NULL || stderr_len != 0 ||
		(stderr_len > 0 && stderr_bytes == NULL))
		return false;
	if (stdout_len == sizeof(on) - 1 &&
		memcmp(stdout_bytes, on, sizeof(on) - 1) == 0)
	{
		*out_state = PGRAC_FENCED_TARGET_ON;
		return true;
	}
	if (stdout_len == sizeof(off) - 1 &&
		memcmp(stdout_bytes, off, sizeof(off) - 1) == 0)
	{
		*out_state = PGRAC_FENCED_TARGET_OFF;
		return true;
	}
	return false;
}

bool
pgrac_fenced_ipmi_action_result_validate(
	int exit_code, const uint8 *stdout_bytes, size_t stdout_len,
	const uint8 *stderr_bytes, size_t stderr_len)
{
	return exit_code == 0 &&
		stdout_len <= PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX &&
		(stdout_len == 0 || stdout_bytes != NULL) && stderr_len == 0 &&
		(stderr_len == 0 || stderr_bytes != NULL);
}

bool
pgrac_fenced_ipmi_uncertified_readback(
	const uint8 observed_uuid[16], PgracFencedTargetState state,
	PgracFencedReadbackV1 *out)
{
	bool nonzero = false;
	size_t i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (observed_uuid == NULL ||
		(state != PGRAC_FENCED_TARGET_OFF &&
		 state != PGRAC_FENCED_TARGET_ON))
		return false;
	for (i = 0; i < 16; i++)
		nonzero = nonzero || observed_uuid[i] != 0;
	if (!nonzero)
		return false;
	out->state = state;
	out->io_drain_state = PGRAC_FENCED_IO_DRAIN_UNKNOWN;
	memcpy(out->observed_target_uuid, observed_uuid, 16);
	return true;
}

static bool
ipmi_target_identity_valid(const PgracFencedTargetV1 *target)
{
	bool nonzero = false;
	size_t i;

	if (target == NULL || target->reserved0 != 0 ||
		target->victim_node_id < 0 || target->victim_node_id >= 128 ||
		target->mapping_generation == 0)
		return false;
	for (i = 0; i < sizeof(target->target_uuid); i++)
		nonzero = nonzero || target->target_uuid[i] != 0;
	return nonzero;
}

PgracFencedProviderResult
pgrac_fenced_ipmi_resolve_result(
	const PgracFencedTargetV1 *configured,
	const PgracFencedIpmiCommandOutputV1 *guid_output,
	PgracFencedTargetV1 *resolved, int32 *native_status)
{
	uint8 observed_uuid[16];

	if (resolved != NULL)
		memset(resolved, 0, sizeof(*resolved));
	if (native_status != NULL)
		*native_status = 0;
	if (!ipmi_target_identity_valid(configured) || guid_output == NULL ||
		resolved == NULL || native_status == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	*native_status = guid_output->exit_code;
	if (!pgrac_fenced_ipmi_guid_result_parse(guid_output->exit_code,
		guid_output->stdout_bytes, guid_output->stdout_len,
		guid_output->stderr_bytes, guid_output->stderr_len, observed_uuid) ||
		memcmp(observed_uuid, configured->target_uuid, 16) != 0)
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	*resolved = *configured;
	return PGRAC_FENCED_PROVIDER_OK;
}

PgracFencedProviderResult
pgrac_fenced_ipmi_readback_results(
	const PgracFencedTargetV1 *configured,
	const PgracFencedIpmiCommandOutputV1 *guid_output,
	const PgracFencedIpmiCommandOutputV1 *status_output,
	PgracFencedReadbackV1 *out)
{
	PgracFencedTargetState state;
	uint8 observed_uuid[16];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!ipmi_target_identity_valid(configured) || guid_output == NULL ||
		status_output == NULL || out == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	if (!pgrac_fenced_ipmi_guid_result_parse(guid_output->exit_code,
		guid_output->stdout_bytes, guid_output->stdout_len,
		guid_output->stderr_bytes, guid_output->stderr_len, observed_uuid) ||
		memcmp(observed_uuid, configured->target_uuid, 16) != 0 ||
		!pgrac_fenced_ipmi_power_result_parse(status_output->exit_code,
		status_output->stdout_bytes, status_output->stdout_len,
		status_output->stderr_bytes, status_output->stderr_len, &state) ||
		!pgrac_fenced_ipmi_uncertified_readback(observed_uuid, state, out))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	out->native_status = status_output->exit_code;
	return PGRAC_FENCED_PROVIDER_OK;
}

PgracFencedProviderResult
pgrac_fenced_ipmi_action_result(
	const PgracFencedIpmiCommandOutputV1 *action_output,
	int32 *native_status)
{
	if (native_status != NULL)
		*native_status = 0;
	if (action_output == NULL || native_status == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	*native_status = action_output->exit_code;
	if (!pgrac_fenced_ipmi_action_result_validate(action_output->exit_code,
		action_output->stdout_bytes, action_output->stdout_len,
		action_output->stderr_bytes, action_output->stderr_len))
		return PGRAC_FENCED_PROVIDER_UNKNOWN;
	return PGRAC_FENCED_PROVIDER_OK;
}

#ifdef HAVE_FEXECVE

extern int fexecve(int fd, char *const argv[], char *const envp[]);

static bool
ipmi_monotonic_now_ns(uint64 *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64) now.tv_sec * UINT64_C(1000000000) +
		(uint64) now.tv_nsec;
	return true;
}

static int
ipmi_deadline_timeout_ms(uint64 deadline_mono_ns, int maximum_ms)
{
	uint64 now;
	uint64 milliseconds;
	uint64 remaining;

	if (!ipmi_monotonic_now_ns(&now) || now >= deadline_mono_ns)
		return 0;
	remaining = deadline_mono_ns - now;
	milliseconds = (remaining + UINT64_C(999999)) / UINT64_C(1000000);
	if (milliseconds > (uint64) maximum_ms)
		milliseconds = (uint64) maximum_ms;
	return (int) milliseconds;
}

static bool
ipmi_process_group_gone(pid_t pgid)
{
	int rc;

	do
	{
		rc = kill(-pgid, 0);
	} while (rc != 0 && errno == EINTR);
	return rc != 0 && errno == ESRCH;
}

static bool
ipmi_process_group_owner_prepare(void)
{
#ifdef __linux__
	return prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0;
#else
	return true;
#endif
}

static void
ipmi_reap_children(pid_t pid, bool owns_process_group,
				   bool *leader_reaped)
{
	int status;
	pid_t waited;

	for (;;)
	{
		waited = waitpid(owns_process_group ? -pid : pid, &status, WNOHANG);
		if (waited > 0)
		{
			if (waited == pid)
				*leader_reaped = true;
			if (!owns_process_group)
				return;
			continue;
		}
		if (waited < 0 && errno == EINTR)
			continue;
		return;
	}
}

static void
ipmi_terminate_child(pid_t pid, bool owns_process_group)
{
	bool leader_reaped = false;
	uint64 stop_deadline;
	uint64 now;

	if (kill(owns_process_group ? -pid : pid, SIGTERM) != 0 &&
		errno == ESRCH && owns_process_group)
		(void) kill(pid, SIGTERM);
	if (!ipmi_monotonic_now_ns(&now) ||
		now > UINT64_MAX - UINT64_C(1000000000))
		stop_deadline = UINT64_MAX;
	else
		stop_deadline = now + UINT64_C(1000000000);
	for (;;)
	{
		ipmi_reap_children(pid, owns_process_group, &leader_reaped);
		if (leader_reaped &&
			(!owns_process_group || ipmi_process_group_gone(pid)))
			return;
		if (!ipmi_monotonic_now_ns(&now) || now >= stop_deadline)
			break;
		(void) poll(NULL, 0, 10);
	}
	if (!owns_process_group || !ipmi_process_group_gone(pid))
	{
		if (owns_process_group)
			(void) kill(-pid, SIGKILL);
		else
			(void) kill(pid, SIGKILL);
	}
	if (owns_process_group && !leader_reaped &&
		ipmi_process_group_gone(pid))
		(void) kill(pid, SIGKILL);
	while (!leader_reaped ||
		(owns_process_group && !ipmi_process_group_gone(pid)))
	{
		(void) kill(owns_process_group ? -pid : pid, SIGKILL);
		ipmi_reap_children(pid, owns_process_group, &leader_reaped);
		(void) poll(NULL, 0, 10);
	}
}

static bool
ipmi_fd_prepare(int fd, bool nonblocking)
{
	int flags;

	if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0)
		return false;
	if (!nonblocking)
		return true;
	flags = fcntl(fd, F_GETFL, 0);
	return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

/* Match the original fork session, or report only an observed disappearance.
 * The latter is not success: the parent must still collect the exact child's
 * exit status, as after the original ESRCH branch.  No other error qualifies.
 */
static bool
ipmi_group_already_exact(pid_t pid, pid_t session, bool *vanished)
{
	pid_t observed;

	if (vanished != NULL)
		*vanished = false;
	observed = getpgid(pid);
	if (observed == pid)
	{
		observed = getsid(pid);
		if (observed == session)
			return true;
	}
	if (vanished != NULL && observed < 0 && errno == ESRCH)
		*vanished = true;
	return false;
}

static bool
ipmi_pipe_drain(int *fd, uint8 *destination, size_t *used, bool *overflow)
{
	uint8 buffer[1024];
	ssize_t got;

	for (;;)
	{
		do
		{
			got = read(*fd, buffer, sizeof(buffer));
		} while (got < 0 && errno == EINTR);
		if (got > 0)
		{
			size_t available = PGRAC_FENCED_IPMI_ACTION_OUTPUT_MAX - *used;
			size_t copy_len = (size_t) got;

			if (copy_len > available)
			{
				copy_len = available;
				*overflow = true;
			}
			if (copy_len > 0)
			{
				memcpy(destination + *used, buffer, copy_len);
				*used += copy_len;
			}
			continue;
		}
		if (got == 0)
		{
			(void) close(*fd);
			*fd = -1;
			return true;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return true;
		return false;
	}
}

bool
pgrac_fenced_ipmi_command_run_fd(
	int executable_fd, const PgracFencedIpmiInvocationV1 *invocation,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out)
{
	static char *const clean_environment[] = {"LC_ALL=C", "LANG=C", NULL};
	struct pollfd poll_fds[2];
	int stdout_pipe[2] = {-1, -1};
	int stderr_pipe[2] = {-1, -1};
	int status = 0;
	bool child_done = false;
	bool child_reaped_early = false;
	bool owns_process_group;
	bool overflow = false;
	bool ok = false;
	uint64 now;
	pid_t pid = -1;
	pid_t session;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	out->exit_code = -1;
	session = getsid(0);
	owns_process_group =
		pgrac_fenced_provider_callback_deadline_mono_ns() == 0;
	if (session <= 0 || executable_fd < 0 || invocation == NULL || invocation->argc != 16 ||
		invocation->argv[0] == NULL || invocation->argv[16] != NULL ||
		(!owns_process_group &&
		 pgrac_fenced_provider_callback_deadline_mono_ns() !=
		 deadline_mono_ns) ||
		(owns_process_group && !ipmi_process_group_owner_prepare()) ||
		!ipmi_monotonic_now_ns(&now) || now >= deadline_mono_ns ||
		pipe(stdout_pipe) != 0 || pipe(stderr_pipe) != 0)
		goto done;
	if (!ipmi_fd_prepare(stdout_pipe[0], true) ||
		!ipmi_fd_prepare(stdout_pipe[1], false) ||
		!ipmi_fd_prepare(stderr_pipe[0], true) ||
		!ipmi_fd_prepare(stderr_pipe[1], false))
		goto done;
	pid = fork();
	if (pid < 0)
		goto done;
	if (pid == 0)
	{
		(void) close(stdout_pipe[0]);
		(void) close(stderr_pipe[0]);
		if ((owns_process_group ?
			 (setpgid(0, 0) != 0 &&
			  !(errno == EPERM && ipmi_group_already_exact(getpid(), session, NULL))) :
			 getpgrp() != getppid()) ||
			dup2(stdout_pipe[1], STDOUT_FILENO) < 0 ||
			dup2(stderr_pipe[1], STDERR_FILENO) < 0)
			_exit(125);
		(void) close(stdout_pipe[1]);
		(void) close(stderr_pipe[1]);
		(void) fexecve(executable_fd, invocation->argv, clean_environment);
		_exit(126);
	}
	if (owns_process_group && setpgid(pid, pid) != 0 && errno != EACCES &&
		errno != ESRCH)
	{
		bool vanished;
		pid_t waited;

		if (errno != EPERM)
			goto done;
		if (!ipmi_group_already_exact(pid, session, &vanished))
		{
			/* A fast command can exit before the parent's redundant setpgid.
			 * The unreaped child still pins its PID.  Do not infer completion
			 * from ESRCH: collect that exact child without waiting. */
			if (!vanished)
				goto done;
			do
			{
				waited = waitpid(pid, &status, WNOHANG);
			} while (waited < 0 && errno == EINTR);
			if (waited != pid)
				goto done;
			child_reaped_early = true;
		}
	}
	(void) close(stdout_pipe[1]);
	stdout_pipe[1] = -1;
	(void) close(stderr_pipe[1]);
	stderr_pipe[1] = -1;
	while (!child_done || stdout_pipe[0] >= 0 || stderr_pipe[0] >= 0)
	{
		pid_t waited;
		int nfds = 0;
		int poll_rc;
		int timeout_ms;

		if (child_reaped_early)
		{
			waited = pid;
			child_reaped_early = false;
		}
		else
		{
			do
			{
				waited = waitpid(pid, &status, WNOHANG);
			} while (waited < 0 && errno == EINTR);
		}
		if (waited == pid)
			child_done = true;
		else if (waited < 0)
			goto done;
		if (stdout_pipe[0] >= 0)
		{
			poll_fds[nfds].fd = stdout_pipe[0];
			poll_fds[nfds].events = POLLIN | POLLHUP | POLLERR;
			poll_fds[nfds].revents = 0;
			nfds++;
		}
		if (stderr_pipe[0] >= 0)
		{
			poll_fds[nfds].fd = stderr_pipe[0];
			poll_fds[nfds].events = POLLIN | POLLHUP | POLLERR;
			poll_fds[nfds].revents = 0;
			nfds++;
		}
		if (child_done && nfds == 0)
			break;
		timeout_ms = ipmi_deadline_timeout_ms(deadline_mono_ns,
			nfds == 0 ? 10 : INT_MAX);
		if (timeout_ms == 0)
			goto done;
		do
		{
			poll_rc = poll(poll_fds, (nfds_t) nfds, timeout_ms);
		} while (poll_rc < 0 && errno == EINTR);
		if (poll_rc < 0)
			goto done;
		if (poll_rc == 0)
		{
			if (nfds == 0)
				continue;
			goto done;
		}
		if (stdout_pipe[0] >= 0 &&
			!ipmi_pipe_drain(&stdout_pipe[0], out->stdout_bytes,
				&out->stdout_len, &overflow))
			goto done;
		if (stderr_pipe[0] >= 0 &&
			!ipmi_pipe_drain(&stderr_pipe[0], out->stderr_bytes,
				&out->stderr_len, &overflow))
			goto done;
	}
	if (!overflow && child_done && WIFEXITED(status))
	{
		out->exit_code = WEXITSTATUS(status);
		ok = true;
	}

done:
	if (stdout_pipe[0] >= 0)
		(void) close(stdout_pipe[0]);
	if (stdout_pipe[1] >= 0)
		(void) close(stdout_pipe[1]);
	if (stderr_pipe[0] >= 0)
		(void) close(stderr_pipe[0]);
	if (stderr_pipe[1] >= 0)
		(void) close(stderr_pipe[1]);
	if (pid > 0 && !child_done)
		ipmi_terminate_child(pid, owns_process_group);
	if (!ok)
	{
		memset(out, 0, sizeof(*out));
		out->exit_code = -1;
	}
	return ok;
}

PgracFencedProviderResult
pgrac_fenced_ipmi_execute_prevalidated(
	const PgracFencedTargetV1 *target, const char *username,
	const char *password_path, PgracFencedIpmiCommand command,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out)
{
	PgracFencedIpmiInvocationV1 invocation;
	PgracFencedIpmiAdapterV1 adapter;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	int executable_fd = -1;

	memset(&adapter, 0, sizeof(adapter));
	memset(&invocation, 0, sizeof(invocation));
	if (out != NULL)
	{
		memset(out, 0, sizeof(*out));
		out->exit_code = -1;
	}
	if (!ipmi_target_identity_valid(target) || out == NULL ||
		target->adapter_config == NULL ||
		target->adapter_config_len > PGRAC_FENCED_IPMI_ADAPTER_MAX_BYTES ||
		!pgrac_fenced_ipmi_adapter_parse(
			(const uint8 *) target->adapter_config, target->adapter_config_len,
			&adapter) ||
		!pgrac_fenced_ipmi_invocation_build(&adapter, username,
			password_path, command, &invocation))
		goto done;
	executable_fd = pgrac_fenced_ipmi_executable_open(&adapter);
	if (executable_fd < 0)
		goto done;
	if (!pgrac_fenced_ipmi_command_run_fd(executable_fd, &invocation,
			deadline_mono_ns, out))
	{
		result = PGRAC_FENCED_PROVIDER_UNKNOWN;
		goto done;
	}
	result = PGRAC_FENCED_PROVIDER_OK;

done:
	if (executable_fd >= 0)
		(void) close(executable_fd);
	memset(&adapter, 0, sizeof(adapter));
	memset(&invocation, 0, sizeof(invocation));
	return result;
}

static PgracFencedProviderResult
ipmi_execute_with_credentials(
	const PgracFencedTargetV1 *target, PgracFencedIpmiCommand command,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out)
{
	PgracFencedIpmiCredentialsV1 credentials;
	PgracFencedProviderResult result;

	memset(&credentials, 0, sizeof(credentials));
	if (!ipmi_target_identity_valid(target) ||
		!pgrac_fenced_ipmi_credentials_load(target->target_uuid, &credentials))
	{
		if (out != NULL)
		{
			memset(out, 0, sizeof(*out));
			out->exit_code = -1;
		}
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	}
	result = pgrac_fenced_ipmi_execute_prevalidated(target,
		credentials.username, credentials.password_path, command,
		deadline_mono_ns, out);
	memset(&credentials, 0, sizeof(credentials));
	return result;
}

static PgracFencedProviderResult
ipmi_resolve(const PgracFencedTargetV1 *configured,
			 PgracFencedTargetV1 *resolved, int32 *native_status)
{
	PgracFencedIpmiCommandOutputV1 guid_output;
	PgracFencedProviderResult result;
	uint64 deadline_mono_ns;

	if (resolved != NULL)
		memset(resolved, 0, sizeof(*resolved));
	if (native_status != NULL)
		*native_status = 0;
	deadline_mono_ns = pgrac_fenced_provider_callback_deadline_mono_ns();
	if (!ipmi_target_identity_valid(configured) || resolved == NULL ||
		native_status == NULL || deadline_mono_ns == 0)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	result = ipmi_execute_with_credentials(configured,
		PGRAC_FENCED_IPMI_COMMAND_GUID, deadline_mono_ns, &guid_output);
	if (result == PGRAC_FENCED_PROVIDER_OK)
		result = pgrac_fenced_ipmi_resolve_result(configured, &guid_output,
			resolved, native_status);
	memset(&guid_output, 0, sizeof(guid_output));
	return result;
}

static PgracFencedProviderResult
ipmi_actuate_command(const PgracFencedTargetV1 *target,
				 PgracFencedIpmiCommand command, uint64_t deadline_mono_ns,
				 int32 *native_status)
{
	PgracFencedIpmiCommandOutputV1 output;
	PgracFencedProviderResult result;

	if (native_status != NULL)
		*native_status = 0;
	if (native_status == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	result = ipmi_execute_with_credentials(target, command, deadline_mono_ns,
		&output);
	if (result == PGRAC_FENCED_PROVIDER_OK)
		result = pgrac_fenced_ipmi_action_result(&output, native_status);
	memset(&output, 0, sizeof(output));
	return result;
}

static PgracFencedProviderResult
ipmi_actuate_off(const PgracFencedTargetV1 *target,
			 uint64_t deadline_mono_ns, int32 *native_status)
{
	return ipmi_actuate_command(target, PGRAC_FENCED_IPMI_COMMAND_OFF,
		deadline_mono_ns, native_status);
}

static PgracFencedProviderResult
ipmi_actuate_on(const PgracFencedTargetV1 *target,
			uint64_t deadline_mono_ns, int32 *native_status)
{
	return ipmi_actuate_command(target, PGRAC_FENCED_IPMI_COMMAND_ON,
		deadline_mono_ns, native_status);
}

static PgracFencedProviderResult
ipmi_readback(const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
			  PgracFencedReadbackV1 *out)
{
	PgracFencedIpmiCommandOutputV1 guid_output;
	PgracFencedIpmiCommandOutputV1 status_output;
	PgracFencedProviderResult result;
	uint8 observed_uuid[16];

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!ipmi_target_identity_valid(target) || out == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	result = ipmi_execute_with_credentials(target,
		PGRAC_FENCED_IPMI_COMMAND_GUID, deadline_mono_ns, &guid_output);
	if (result != PGRAC_FENCED_PROVIDER_OK)
		goto done;
	if (!pgrac_fenced_ipmi_guid_result_parse(guid_output.exit_code,
		guid_output.stdout_bytes, guid_output.stdout_len,
		guid_output.stderr_bytes, guid_output.stderr_len, observed_uuid) ||
		memcmp(observed_uuid, target->target_uuid, 16) != 0)
	{
		result = PGRAC_FENCED_PROVIDER_UNKNOWN;
		goto done;
	}
	result = ipmi_execute_with_credentials(target,
		PGRAC_FENCED_IPMI_COMMAND_STATUS, deadline_mono_ns, &status_output);
	if (result == PGRAC_FENCED_PROVIDER_OK)
		result = pgrac_fenced_ipmi_readback_results(target, &guid_output,
			&status_output, out);

done:
	memset(&guid_output, 0, sizeof(guid_output));
	memset(&status_output, 0, sizeof(status_output));
	memset(observed_uuid, 0, sizeof(observed_uuid));
	return result;
}

static void
ipmi_shutdown(void)
{
}

static const PgracFencedProviderOpsV1 ipmi_ops = {
	.abi_version = PGRAC_FENCED_PROVIDER_ABI_V1,
	.struct_size = sizeof(PgracFencedProviderOpsV1),
	.provider_id = PGRAC_FENCED_PROVIDER_ID_IPMI_LANPLUS_V1,
	.reserved0 = 0,
	.provider_name = PGRAC_FENCED_PROVIDER_NAME_IPMI_LANPLUS_V1,
	.resolve = ipmi_resolve,
	.actuate_off = ipmi_actuate_off,
	.readback = ipmi_readback,
	.actuate_on = ipmi_actuate_on,
	.shutdown = ipmi_shutdown
};

#endif /* HAVE_FEXECVE */

#ifndef HAVE_FEXECVE

bool
pgrac_fenced_ipmi_command_run_fd(
	int executable_fd, const PgracFencedIpmiInvocationV1 *invocation,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out)
{
	(void) executable_fd;
	(void) invocation;
	(void) deadline_mono_ns;
	if (out != NULL)
	{
		memset(out, 0, sizeof(*out));
		out->exit_code = -1;
	}
	return false;
}

PgracFencedProviderResult
pgrac_fenced_ipmi_execute_prevalidated(
	const PgracFencedTargetV1 *target, const char *username,
	const char *password_path, PgracFencedIpmiCommand command,
	uint64 deadline_mono_ns, PgracFencedIpmiCommandOutputV1 *out)
{
	(void) target;
	(void) username;
	(void) password_path;
	(void) command;
	(void) deadline_mono_ns;
	if (out != NULL)
	{
		memset(out, 0, sizeof(*out));
		out->exit_code = -1;
	}
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
}

#endif /* !HAVE_FEXECVE */

const PgracFencedProviderOpsV1 *
pgrac_fenced_ipmi_provider_ops(void)
{
#ifdef HAVE_FEXECVE
	return &ipmi_ops;
#else
	return NULL;
#endif
}
