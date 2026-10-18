/* Cached numeric OID-list selection for passive visibility diagnostics. */
#ifndef CLUSTER_VISIBILITY_TRACE_FILTER_H
#define CLUSTER_VISIBILITY_TRACE_FILTER_H
#include "c.h"
#define CLUSTER_VISIBILITY_TRACE_MAX_RELIDS 64
typedef struct ClusterVisibilityTraceFilter {
	int count;
	Oid relids[CLUSTER_VISIBILITY_TRACE_MAX_RELIDS];
} ClusterVisibilityTraceFilter;
static inline bool
cluster_visibility_trace_contains(const ClusterVisibilityTraceFilter *filter, Oid relid)
{
	int i;
	for (i = 0; i < filter->count; i++)
		if (filter->relids[i] == relid)
			return true;
	return false;
}
static inline bool
cluster_visibility_trace_space(char c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}
static inline bool
cluster_visibility_trace_parse(const char *input, ClusterVisibilityTraceFilter *output)
{
	ClusterVisibilityTraceFilter parsed = { 0 };
	const char *p = input ? input : "";
	if (output == NULL)
		return false;
	while (cluster_visibility_trace_space(*p))
		p++;
	while (*p) {
		uint64 value = 0;
		if (*p < '0' || *p > '9')
			return false;
		while (*p >= '0' && *p <= '9') {
			value = value * 10 + (*p++ - '0');
			if (value > PG_UINT32_MAX)
				return false;
		}
		if (value == InvalidOid)
			return false;
		if (!cluster_visibility_trace_contains(&parsed, (Oid)value)) {
			if (parsed.count == CLUSTER_VISIBILITY_TRACE_MAX_RELIDS)
				return false;
			parsed.relids[parsed.count++] = (Oid)value;
		}
		while (cluster_visibility_trace_space(*p))
			p++;
		if (!*p)
			break;
		if (*p++ != ',')
			return false;
		while (cluster_visibility_trace_space(*p))
			p++;
		if (!*p)
			return false;
	}
	*output = parsed;
	return true;
}
#endif
