/*-------------------------------------------------------------------------
 *
 * cluster_conf.c
 *	  pgrac cluster topology configuration loader (Stage 0.19).
 *
 *	  Stage 0.19 establishes the cluster topology framework.  This
 *	  file owns:
 *
 *	    - The ClusterConfShmem region allocation (cluster_conf_shmem_*).
 *	    - The pgrac.conf parser (INI + sections; ~150 lines of pure C).
 *	    - The startup-time loader (cluster_conf_load), called from
 *	      cluster_shmem.c::cluster_init_shmem.
 *	    - Lookup / introspection helpers used by Stage 2+ subsystems.
 *	    - The cluster_get_nodes SRF backing pg_cluster_nodes.
 *
 *	  Stage 0 single-node tests do not need to write a pgrac.conf:
 *	  the loader falls back to a 1-row degraded topology when the
 *	  file is absent (Q7=A in spec-0.19).  Stage 2+ multi-node
 *	  deployments must provide a pgrac.conf (typically on shared
 *	  storage).
 *
 *	  See docs/cluster-conf-design.md for the full design rationale,
 *	  file format, and Stage evolution path.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_conf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_conf_ / ClusterConf_ /
 *	  cluster_get_nodes prefix.  Excluded from disable-cluster builds
 *	  via cluster/Makefile (spec-0.3 symbol contract), except for the
 *	  cluster_get_nodes SRF which is unconditional (pg_proc.dat
 *	  reference) and returns zero rows in disable mode.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

#include "cluster/cluster_conf.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_elog.h"	/* CLUSTER_LOG */
#include "cluster/cluster_guc.h"	/* cluster_node_id, cluster_config_file */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (stage 0.27) */
#endif


/*
 * cluster_get_nodes is registered unconditionally so pg_proc.dat
 * references resolve in both build modes.
 */
PG_FUNCTION_INFO_V1(cluster_get_nodes);


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Compile-time anchors for the shmem ABI.
 * ============================================================ */

StaticAssertDecl(sizeof(ClusterNodeRole) == sizeof(int32),
				 "ClusterNodeRole must be int32-sized for shmem ABI stability");

StaticAssertDecl(sizeof(ClusterConf) <= 65536, "ClusterConf must fit in 64 KiB");


/* ============================================================
 * Process-local shmem pointer.
 * ============================================================ */

ClusterConf *ClusterConfShmem = NULL;


/* ============================================================
 * Shmem region request / init.  Called from cluster_shmem.c.
 * ============================================================ */

Size
cluster_conf_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterConf));
}

void
cluster_conf_shmem_init(void)
{
	bool found;

	CLUSTER_INJECTION_POINT("cluster-conf-shmem-init");

	ClusterConfShmem
		= (ClusterConf *)ShmemInitStruct("pgrac cluster conf", cluster_conf_shmem_size(), &found);

	if (!found) {
		/*
		 * First-time init in the postmaster.  Zero the region and stamp
		 * the magic so cluster_conf_load can populate the rest, and so
		 * later sanity checks can detect corruption.
		 */
		memset(ClusterConfShmem, 0, sizeof(*ClusterConfShmem));
		ClusterConfShmem->magic = PGRAC_CLUSTER_CONF_MAGIC;
		ClusterConfShmem->node_count = 0;
	}
}


/* ============================================================
 * Role helpers.
 * ============================================================ */

const char *
cluster_conf_role_to_string(ClusterNodeRole role)
{
	switch (role) {
	case CLUSTER_ROLE_PRIMARY:
		return "primary";
	case CLUSTER_ROLE_STANDBY:
		return "standby";
	case CLUSTER_ROLE_ARBITER:
		return "arbiter";
	}
	return "unknown";
}

bool
cluster_conf_role_from_string(const char *str, ClusterNodeRole *out)
{
	if (str == NULL || out == NULL)
		return false;
	if (strcmp(str, "primary") == 0) {
		*out = CLUSTER_ROLE_PRIMARY;
		return true;
	}
	if (strcmp(str, "standby") == 0) {
		*out = CLUSTER_ROLE_STANDBY;
		return true;
	}
	if (strcmp(str, "arbiter") == 0) {
		*out = CLUSTER_ROLE_ARBITER;
		return true;
	}
	return false;
}


/* ============================================================
 * Lookup / introspection helpers.
 * ============================================================ */

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	int i;

	if (ClusterConfShmem == NULL)
		return NULL;

	for (i = 0; i < ClusterConfShmem->node_count; i++) {
		if (ClusterConfShmem->nodes[i].node_id == node_id)
			return &ClusterConfShmem->nodes[i];
	}
	return NULL;
}

int
cluster_conf_node_count(void)
{
	if (ClusterConfShmem == NULL)
		return 0;
	return ClusterConfShmem->node_count;
}


/* ============================================================
 * INI parser.
 *
 *	Single pass over the file, line by line.  Errors are reported
 *	with errcontext("at line %d of \"%s\"") so DBAs can pinpoint
 *	the offending line.
 * ============================================================ */

/*
 * Strip leading whitespace, trailing whitespace, and any inline
 * comment beginning with '#' or ';'.  Operates in place on the buffer.
 * Returns a pointer to the (possibly trimmed) start of the string.
 */
static char *
trim_and_strip_comment(char *line)
{
	char *start;
	char *p;

	start = line;
	while (*start == ' ' || *start == '\t')
		start++;

	for (p = start; *p != '\0'; p++) {
		if (*p == '#' || *p == ';') {
			*p = '\0';
			break;
		}
	}

	if (p > start) {
		p--;
		while (p >= start && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
			*p = '\0';
			p--;
		}
	}

	return start;
}

/*
 * Parse "[section]" or "[section.suffix]".  On success fills out_name
 * (must hold at least 64 bytes) and out_suffix (-1 if none, else the
 * decimal integer after '.').  Returns true.  Returns false on
 * malformed input.
 */
static bool
parse_section_header(const char *line, char *out_name, size_t out_name_size, int *out_suffix)
{
	const char *l = line;
	const char *r;
	const char *dot;
	size_t name_len;

	if (*l != '[')
		return false;
	l++;

	r = strchr(l, ']');
	if (r == NULL || *(r + 1) != '\0')
		return false;

	dot = memchr(l, '.', r - l);
	if (dot != NULL) {
		char *endptr;
		long val;

		name_len = dot - l;
		if (name_len + 1 > out_name_size)
			return false;
		memcpy(out_name, l, name_len);
		out_name[name_len] = '\0';

		val = strtol(dot + 1, &endptr, 10);
		if (endptr != r || val < 0 || val > INT_MAX)
			return false;
		*out_suffix = (int)val;
	} else {
		name_len = r - l;
		if (name_len + 1 > out_name_size)
			return false;
		memcpy(out_name, l, name_len);
		out_name[name_len] = '\0';
		*out_suffix = -1;
	}
	return true;
}

/*
 * Split "key = value" into its two halves.  Both pointers point into
 * the original (mutated) line buffer.  Returns true on success.
 */
static bool
parse_key_value(char *line, char **out_key, char **out_value)
{
	char *eq;
	char *p;

	eq = strchr(line, '=');
	if (eq == NULL)
		return false;

	*eq = '\0';

	p = eq - 1;
	while (p >= line && (*p == ' ' || *p == '\t')) {
		*p = '\0';
		p--;
	}

	*out_key = line;

	p = eq + 1;
	while (*p == ' ' || *p == '\t')
		p++;
	*out_value = p;

	p = *out_value + strlen(*out_value);
	while (p > *out_value && (*(p - 1) == ' ' || *(p - 1) == '\t')) {
		p--;
		*p = '\0';
	}

	if (**out_key == '\0')
		return false;
	return true;
}

/*
 * Validate "host:port" form.  host must be non-empty (no further DNS
 * checking at parse time), port must be a decimal in [1, 65535].
 */
static bool
validate_addr_format(const char *addr)
{
	const char *colon;
	char *endptr;
	long port;

	if (addr == NULL || *addr == '\0')
		return false;
	colon = strrchr(addr, ':');
	if (colon == NULL || colon == addr)
		return false;
	port = strtol(colon + 1, &endptr, 10);
	if (*endptr != '\0' || port < 1 || port > 65535)
		return false;
	return true;
}


/* ============================================================
 * Loader: parse pgrac.conf and populate ClusterConfShmem.
 * ============================================================ */

/*
 * Initialise a node slot to defaults before any field is set.
 */
static void
init_node_slot(ClusterNodeInfo *n, int32 node_id)
{
	memset(n, 0, sizeof(*n));
	n->node_id = node_id;
	n->role = CLUSTER_ROLE_PRIMARY;
}

/*
 * Single-node fallback: caller passed an absent / empty pgrac.conf.
 * Construct a one-row topology using cluster_node_id.  Even when
 * cluster_node_id is the unconfigured -1, this leaves ClusterConfShmem
 * usable; SRF readers see a single self row.
 */
static void
load_single_node_fallback(const char *path)
{
	ClusterNodeInfo *n;

	ClusterConfShmem->node_count = 1;
	strlcpy(ClusterConfShmem->cluster_name, "(single-node)",
			sizeof(ClusterConfShmem->cluster_name));

	n = &ClusterConfShmem->nodes[0];
	init_node_slot(n, cluster_node_id);

	ereport(LOG,
			(errmsg("cluster_conf: \"%s\" not found, falling back to single-node mode (node_id=%d)",
					path, cluster_node_id)));
}

/*
 * Find or allocate the slot for a given node id.  Returns NULL on
 * duplicate or capacity-overflow (caller ereports FATAL).
 */
static ClusterNodeInfo *
get_or_create_node_slot(int32 node_id, bool *out_duplicate)
{
	int i;

	*out_duplicate = false;

	for (i = 0; i < ClusterConfShmem->node_count; i++) {
		if (ClusterConfShmem->nodes[i].node_id == node_id) {
			*out_duplicate = true;
			return &ClusterConfShmem->nodes[i];
		}
	}

	if (ClusterConfShmem->node_count >= CLUSTER_MAX_NODES)
		return NULL;

	{
		ClusterNodeInfo *n = &ClusterConfShmem->nodes[ClusterConfShmem->node_count];

		init_node_slot(n, node_id);
		ClusterConfShmem->node_count++;
		return n;
	}
}

/*
 * Apply key=value to the node slot, returning false on unknown key
 * or invalid value.  out_err is filled with a static message describing
 * the rejection.
 */
static bool
apply_node_field(ClusterNodeInfo *n, const char *key, const char *value, const char **out_err)
{
	if (strcmp(key, "interconnect_addr") == 0) {
		if (!validate_addr_format(value)) {
			*out_err = "interconnect_addr must be in host:port form";
			return false;
		}
		strlcpy(n->interconnect_addr, value, sizeof(n->interconnect_addr));
		return true;
	}
	if (strcmp(key, "hostname") == 0) {
		strlcpy(n->hostname, value, sizeof(n->hostname));
		return true;
	}
	if (strcmp(key, "public_addr") == 0) {
		if (*value != '\0' && !validate_addr_format(value)) {
			*out_err = "public_addr must be in host:port form";
			return false;
		}
		strlcpy(n->public_addr, value, sizeof(n->public_addr));
		return true;
	}
	if (strcmp(key, "role") == 0) {
		ClusterNodeRole role;

		if (!cluster_conf_role_from_string(value, &role)) {
			*out_err = "role must be one of: primary, standby, arbiter";
			return false;
		}
		n->role = role;
		return true;
	}
	if (strcmp(key, "region") == 0) {
		strlcpy(n->region, value, sizeof(n->region));
		return true;
	}
	*out_err = "unknown key in [node.N] section";
	return false;
}

/*
 * apply_cluster_field handles keys inside the [cluster] section.
 * Stage 0.19 only honours "name"; unknown keys produce a WARNING
 * and are silently ignored to keep forward compatibility.
 */
static void
apply_cluster_field(const char *key, const char *value, int line_number, const char *path)
{
	if (strcmp(key, "name") == 0) {
		strlcpy(ClusterConfShmem->cluster_name, value, sizeof(ClusterConfShmem->cluster_name));
		return;
	}
	ereport(WARNING, (errmsg("cluster_conf: ignoring unknown key \"%s\" in [cluster] section", key),
					  errcontext("at line %d of \"%s\"", line_number, path)));
}

/*
 * Final validation after the parse pass: every parsed node has the
 * required interconnect_addr, and cluster_node_id is declared (unless
 * it is -1 and the topology has exactly one node).
 */
static void
post_validate(const char *path)
{
	int i;
	bool self_seen = false;

	if (ClusterConfShmem->node_count == 0) {
		/* No [node.N] declared: behave as single-node fallback. */
		load_single_node_fallback(path);
		return;
	}

	for (i = 0; i < ClusterConfShmem->node_count; i++) {
		const ClusterNodeInfo *n = &ClusterConfShmem->nodes[i];

		if (n->interconnect_addr[0] == '\0') {
			ereport(
				FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg(
					 "cluster_conf: node %d in \"%s\" is missing required field interconnect_addr",
					 n->node_id, path)));
		}
		if (n->node_id == cluster_node_id)
			self_seen = true;
	}

	if (cluster_node_id == -1) {
		if (ClusterConfShmem->node_count > 1)
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("cluster_conf: cluster.node_id is unconfigured (-1) but \"%s\" "
								   "declares %d nodes",
								   path, ClusterConfShmem->node_count),
							errhint("Set cluster.node_id in postgresql.conf to one of the ids "
									"declared in pgrac.conf.")));
		return;
	}

	if (!self_seen) {
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_conf: cluster.node_id=%d is not declared in \"%s\"",
							   cluster_node_id, path),
						errhint("Add a [node.%d] section to pgrac.conf or change cluster.node_id.",
								cluster_node_id)));
	}
}

void
cluster_conf_load(void)
{
	const char *path;
	struct stat st;
	FILE *f;
	char line[1024];
	int line_number = 0;
	enum { SEC_NONE, SEC_CLUSTER, SEC_NODE } cur_section = SEC_NONE;
	int32 cur_node_id = -1;
	ClusterNodeInfo *cur_node = NULL;

	CLUSTER_INJECTION_POINT("cluster-conf-parse-fail");

	Assert(ClusterConfShmem != NULL);
	Assert(ClusterConfShmem->magic == PGRAC_CLUSTER_CONF_MAGIC);

	path = cluster_config_file != NULL ? cluster_config_file : "pgrac.conf";

	if (stat(path, &st) != 0) {
		if (errno == ENOENT) {
			/*
			 * Spec-2.1 D3 (2026-05-06): pgrac.conf-absent path gated on
			 * cluster.allow_single_node.  When on (Stage 2.1 default,
			 * backward-compat), fall back to single-node mode.  When off
			 * (Stage 2 strict), FATAL per spec-2.0 §3 Invariant 3
			 * "uncertainty fail-closed".
			 *
			 * BOUNDARY INVARIANT (spec-2.1 §3.5): allow_single_node is
			 * checked ONLY in this ENOENT path.  All other error branches
			 * (parser failures / collision / out-of-range / invalid addr /
			 * assert_self_in_topology) ereport(FATAL) unconditionally --
			 * see post_validate() etc below.  Do NOT extend allow_single_node
			 * to swallow malformed conf errors; that is an explicit anti-
			 * pattern (see spec-2.1 §3.5 reverse code example).
			 */
			if (cluster_allow_single_node) {
				load_single_node_fallback(path);
				return;
			}
			ereport(FATAL,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("pgrac.conf is required when cluster.allow_single_node is off"),
					 errhint("Create pgrac.conf with node entries, or set "
							 "cluster.allow_single_node = on for single-node "
							 "compatibility mode.")));
		}
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("cluster_conf: could not stat \"%s\": %m", path)));
	}

	f = AllocateFile(path, "r");
	if (f == NULL) {
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("cluster_conf: could not open \"%s\": %m", path)));
	}

	while (fgets(line, sizeof(line), f) != NULL) {
		char *trimmed;

		line_number++;
		trimmed = trim_and_strip_comment(line);

		if (*trimmed == '\0')
			continue;

		if (*trimmed == '[') {
			char section_name[64];
			int suffix;

			if (!parse_section_header(trimmed, section_name, sizeof(section_name), &suffix))
				ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
								errmsg("cluster_conf: malformed section header"),
								errcontext("at line %d of \"%s\"", line_number, path)));

			if (strcmp(section_name, "cluster") == 0) {
				cur_section = SEC_CLUSTER;
				cur_node = NULL;
			} else if (strcmp(section_name, "node") == 0) {
				bool duplicate;

				if (suffix < 0 || suffix > 127)
					ereport(FATAL,
							(errcode(ERRCODE_CONFIG_FILE_ERROR),
							 errmsg("cluster_conf: node id %d out of range [0, 127]", suffix),
							 errcontext("at line %d of \"%s\"", line_number, path)));

				cur_node_id = (int32)suffix;
				cur_node = get_or_create_node_slot(cur_node_id, &duplicate);
				if (cur_node == NULL)
					ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
									errmsg("cluster_conf: too many nodes in \"%s\" (max %d)", path,
										   CLUSTER_MAX_NODES)));
				if (duplicate)
					ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
									errmsg("cluster_conf: duplicate [node.%d] section in \"%s\"",
										   cur_node_id, path),
									errcontext("at line %d of \"%s\"", line_number, path)));
				cur_section = SEC_NODE;
			} else {
				ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
								errmsg("cluster_conf: unknown section \"%s\"", section_name),
								errcontext("at line %d of \"%s\"", line_number, path)));
			}
			continue;
		}

		{
			char *key;
			char *value;

			if (!parse_key_value(trimmed, &key, &value))
				ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
								errmsg("cluster_conf: malformed key=value"),
								errcontext("at line %d of \"%s\"", line_number, path)));

			if (cur_section == SEC_NONE)
				ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
								errmsg("cluster_conf: key \"%s\" outside any section", key),
								errcontext("at line %d of \"%s\"", line_number, path)));

			if (cur_section == SEC_CLUSTER) {
				apply_cluster_field(key, value, line_number, path);
			} else {
				const char *err_msg = NULL;

				Assert(cur_section == SEC_NODE);
				Assert(cur_node != NULL);

				if (!apply_node_field(cur_node, key, value, &err_msg))
					ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
									errmsg("cluster_conf: %s", err_msg),
									errcontext("at line %d of \"%s\"", line_number, path)));
			}
		}
	}

	FreeFile(f);

	post_validate(path);

	CLUSTER_INJECTION_POINT("cluster-conf-load-success");

	ereport(LOG, (errmsg("cluster_conf: loaded %d node(s) from \"%s\"",
						 ClusterConfShmem->node_count, path)));
}

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SRF cluster_get_nodes -- backs pg_cluster_nodes view.
 *
 *	The function symbol is provided unconditionally so pg_proc.dat
 *	references resolve in --disable-cluster builds; the body is
 *	#ifdef'd and disable mode emits zero rows (same convention as
 *	cluster_get_wait_events).
 * ============================================================ */

Datum
cluster_get_nodes(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
		int i;

		if (ClusterConfShmem == NULL)
			return (Datum)0;

		for (i = 0; i < ClusterConfShmem->node_count; i++) {
			const ClusterNodeInfo *n = &ClusterConfShmem->nodes[i];
			Datum values[7];
			bool nulls[7] = { false, false, false, false, false, false, false };

			values[0] = Int32GetDatum(n->node_id);

			if (n->hostname[0] == '\0')
				nulls[1] = true;
			else
				values[1] = CStringGetTextDatum(n->hostname);

			values[2] = CStringGetTextDatum(n->interconnect_addr);

			if (n->public_addr[0] == '\0')
				nulls[3] = true;
			else
				values[3] = CStringGetTextDatum(n->public_addr);

			values[4] = CStringGetTextDatum(cluster_conf_role_to_string(n->role));

			if (n->region[0] == '\0')
				nulls[5] = true;
			else
				values[5] = CStringGetTextDatum(n->region);

			values[6] = BoolGetDatum(n->node_id == cluster_node_id);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#endif

	return (Datum)0;
}
