/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_pacemaker.c
 *    Fixed OFF/ON requests through the native Pacemaker fencing owner.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_pacemaker.c
 *
 * NOTES
 *    PGRAC-original adapter. Native completion is not OFF/drain evidence;
 *    callers still need fresh, independently bound readback. No CLI fallback.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <limits.h>
#include <time.h>

#include "pgrac_fenced_pacemaker.h"

#ifdef USE_PACEMAKER
#include <crm/stonith-ng.h>

static bool
ascii_alnum(unsigned char value)
{
	return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z')
		   || (value >= '0' && value <= '9');
}

static bool
action_valid(const PgracFencedPacemakerActionV1 *action)
{
	size_t i;
	bool nonzero = false;

	if (action == NULL || action->attempt == 0 || !ascii_alnum(action->node[0]))
		return false;
	for (i = 0; i < sizeof(action->node); i++) {
		unsigned char ch = action->node[i];

		if (ch == '\0')
			break;
		if (!ascii_alnum(ch) && ch != '-' && ch != '_' && ch != '.')
			return false;
	}
	if (i == sizeof(action->node))
		return false;
	for (i = 0; i < sizeof(action->operation_id); i++)
		nonzero = nonzero || action->operation_id[i] != 0;
	return nonzero;
}

static bool
remaining_seconds(uint64 deadline, int *seconds)
{
	struct timespec now;
	uint64 current;
	uint64 remaining;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0
		|| (uint64)now.tv_sec > (UINT64_MAX - UINT64_C(999999999)) / UINT64_C(1000000000))
		return false;
	current = (uint64)now.tv_sec * UINT64_C(1000000000) + now.tv_nsec;
	if (current >= deadline)
		return false;
	remaining = (deadline - current) / UINT64_C(1000000000);
	if ((deadline - current) % UINT64_C(1000000000) != 0)
		remaining++;
	*seconds = remaining > INT_MAX ? INT_MAX : (int)remaining;
	return true;
}

static void
operation_client_name(const uint8 operation[16], char name[46])
{
	static const char hex[] = "0123456789abcdef";
	static const char prefix[] = "pgrac-fenced-";
	size_t i;

	memcpy(name, prefix, sizeof(prefix) - 1);
	for (i = 0; i < 16; i++) {
		name[sizeof(prefix) - 1 + i * 2] = hex[operation[i] >> 4];
		name[sizeof(prefix) + i * 2] = hex[operation[i] & 15];
	}
	name[sizeof(prefix) - 1 + 32] = '\0';
}

static PgracFencedProviderResult
perform_action(const PgracFencedPacemakerActionV1 *action, PgracFencedPacemakerActionResultV1 *out)
{
	stonith_t *api;
	char client[46];
	int descriptor = -1;
	int seconds;
	int status;
	PgracFencedProviderResult result = PGRAC_FENCED_PROVIDER_UNKNOWN;

	if (!remaining_seconds(action->deadline_mono_ns, &seconds)) {
		out->native_status = -ETIMEDOUT;
		return result;
	}
	api = stonith_api_new();
	if (api == NULL) {
		out->native_status = -ENOMEM;
		return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	}
	if (api->cmds == NULL || api->cmds->connect == NULL || api->cmds->fence == NULL
		|| api->cmds->disconnect == NULL) {
		out->native_status = -ENOSYS;
		stonith_api_delete(api);
		return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	}
	operation_client_name(action->operation_id, client);
	status = api->cmds->connect(api, client, &descriptor);
	if (status == 0) {
		/* Connection setup may have consumed the worker's remaining envelope. */
		if (!remaining_seconds(action->deadline_mono_ns, &seconds))
			status = -ETIMEDOUT;
		else {
			status = api->cmds->fence(api, st_opt_sync_call, action->node,
									  action->turn_on ? "on" : "off", seconds, 0);
			out->call_id = api->call_id;
			if (status == 0)
				result = PGRAC_FENCED_PROVIDER_OK;
		}
		(void)api->cmds->disconnect(api);
	}
	out->native_status = status;
	stonith_api_delete(api);
	return result;
}
#endif

PgracFencedProviderResult
pgrac_fenced_pacemaker_action(const PgracFencedPacemakerActionV1 *action,
							  PgracFencedPacemakerActionResultV1 *out)
{
	if (out == NULL)
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	memset(out, 0, sizeof(*out));
#ifdef USE_PACEMAKER
	if (!action_valid(action)) {
		out->native_status = -EINVAL;
		return PGRAC_FENCED_PROVIDER_CONFIG_ERROR;
	}
	return perform_action(action, out);
#else
	(void)action;
	out->native_status = -ENOSYS;
	return PGRAC_FENCED_PROVIDER_UNAVAILABLE;
#endif
}
