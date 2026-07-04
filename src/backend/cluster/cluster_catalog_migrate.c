/*-------------------------------------------------------------------------
 *
 * cluster_catalog_migrate.c
 *	  Seed/adopt the shared catalog relation tree (spec-6.14 D2).
 *
 *	  See cluster_catalog_migrate.h for the seed-vs-join model.  This file
 *	  implements the file-level mechanics: a torn-safe per-file copy, a
 *	  digit-name filter that selects only relation files (never pg_control,
 *	  pg_filenode.map, PG_VERSION, pg_internal.init, ... which are not
 *	  addressed through the sharedfs relation path), and the durable authority
 *	  marker that gates seed vs adopt.
 *
 *	  Runs from cluster_catalog_startup_prepare (postmaster-once, after the
 *	  shared pg_control authority is established) so it precedes the startup
 *	  process and any backend catalog access.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_catalog_migrate.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/catversion.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_guc.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/fd.h"

/*
 * is_relation_filename -- does `name` look like a relation file?
 *
 *	Relation files are named by relfilenumber, optionally with a segment
 *	suffix (".<n>") or a fork suffix ("_fsm"/"_vm"/"_init") -- always leading
 *	with a digit.  Every non-relation file in global/ and base/<db>/ leads
 *	with a letter (PG_VERSION, pg_filenode.map, pg_internal.init, pg_control,
 *	pgrac_*).  So "first char is a digit" cleanly selects exactly the files
 *	the sharedfs backend later resolves through relpathperm().  A stray
 *	"<relfile>.tmp" left by a crashed copy is excluded so it is never adopted
 *	as data.
 */
static bool
is_relation_filename(const char *name)
{
	size_t		len;

	if (name == NULL || !isdigit((unsigned char) name[0]))
		return false;

	len = strlen(name);
	if (len >= 4 && strcmp(name + len - 4, ".tmp") == 0)
		return false;

	return true;
}

/*
 * copy_file_durable -- copy `src` to `dst` torn-safely (temp + fsync +
 *	durable_rename).  Streams in BLCKSZ chunks so a large catalog relation
 *	does not need to be buffered whole.  Any failure is FATAL: at postmaster
 *	startup an incomplete catalog authority means the node cannot come up.
 */
static void
copy_file_durable(const char *src, const char *dst)
{
	char		dsttmp[MAXPGPATH];
	int			srcfd;
	int			dstfd;
	char		buf[BLCKSZ];
	int			nread;

	if (snprintf(dsttmp, sizeof(dsttmp), "%s.tmp", dst) >= (int) sizeof(dsttmp))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("shared catalog authority path too long: \"%s\"", dst)));

	srcfd = OpenTransientFile(src, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open catalog file \"%s\" for shared migration: %m",
						src)));

	dstfd = OpenTransientFile(dsttmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (dstfd < 0)
	{
		CloseTransientFile(srcfd);
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create shared catalog file \"%s\": %m", dsttmp)));
	}

	while ((nread = read(srcfd, buf, sizeof(buf))) > 0)
	{
		if (write(dstfd, buf, nread) != nread)
		{
			CloseTransientFile(srcfd);
			CloseTransientFile(dstfd);
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write shared catalog file \"%s\": %m", dsttmp)));
		}
	}
	if (nread < 0)
	{
		CloseTransientFile(srcfd);
		CloseTransientFile(dstfd);
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read catalog file \"%s\": %m", src)));
	}

	if (pg_fsync(dstfd) != 0)
	{
		CloseTransientFile(srcfd);
		CloseTransientFile(dstfd);
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync shared catalog file \"%s\": %m", dsttmp)));
	}

	if (CloseTransientFile(dstfd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close shared catalog file \"%s\": %m", dsttmp)));
	CloseTransientFile(srcfd);

	/* Atomically publish the fully-written file (fsyncs the directory too). */
	if (durable_rename(dsttmp, dst, LOG) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not rename shared catalog file \"%s\" to \"%s\"",
						dsttmp, dst)));
}

/*
 * seed_dir -- copy every relation file from <local>/<rel> into <shared>/<rel>,
 *	creating the shared subdirectory tree as needed.  `rel` is a PGDATA-
 *	relative directory such as "global" or "base/1".
 */
static void
seed_dir(const char *local_pgdata, const char *rel)
{
	char		localdir[MAXPGPATH];
	char		shareddir[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(localdir, sizeof(localdir), "%s/%s", local_pgdata, rel);
	snprintf(shareddir, sizeof(shareddir), "%s/%s", cluster_shared_data_dir, rel);

	/* The shared root has no pre-created base/<db>; materialise it. */
	if (pg_mkdir_p(shareddir, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create shared catalog directory \"%s\": %m",
						shareddir)));

	dir = AllocateDir(localdir);
	while ((de = ReadDir(dir, localdir)) != NULL)
	{
		char		srcpath[MAXPGPATH];
		char		dstpath[MAXPGPATH];

		if (!is_relation_filename(de->d_name))
			continue;

		snprintf(srcpath, sizeof(srcpath), "%s/%s", localdir, de->d_name);
		snprintf(dstpath, sizeof(dstpath), "%s/%s", shareddir, de->d_name);
		copy_file_durable(srcpath, dstpath);
	}
	FreeDir(dir);
}

/*
 * marker_crc -- CRC over the marker's bytes preceding the crc field.
 */
static pg_crc32c
marker_crc(const ClusterCatalogAuthorityMarker *m)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, m, offsetof(ClusterCatalogAuthorityMarker, crc));
	FIN_CRC32C(crc);
	return crc;
}

/*
 * read_marker -- read + validate the shared authority marker.  Returns true
 *	on a structurally valid record (magic/version/crc), false if the marker is
 *	absent (seed case).  A present-but-corrupt marker is FATAL: the tree it
 *	guards cannot be trusted.
 */
static bool
read_marker(ClusterCatalogAuthorityMarker *out)
{
	char		path[MAXPGPATH];
	int			fd;
	int			nread;

	snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir,
			 CLUSTER_CATALOG_AUTHORITY_REL_PATH);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return false;		/* absent: this node is the seed */
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open shared catalog authority marker \"%s\": %m",
						path)));
	}

	nread = read(fd, out, sizeof(*out));
	CloseTransientFile(fd);

	if (nread != (int) sizeof(*out) ||
		out->magic != CLUSTER_CATALOG_AUTHORITY_MAGIC ||
		out->version != CLUSTER_CATALOG_AUTHORITY_VERSION ||
		out->crc != marker_crc(out))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("shared catalog authority marker \"%s\" is corrupt or truncated",
						path),
				 errhint("Remove the shared catalog tree under cluster.shared_data_dir "
						 "and re-seed from a single node, or restore it from backup.")));

	return true;
}

/*
 * write_marker -- write the authority marker atomically (temp + fsync +
 *	durable_rename).  Called LAST, after every relation file is durably in the
 *	shared tree, so the marker's presence certifies a complete seed.
 */
static void
write_marker(uint64 system_identifier)
{
	char		tmp[MAXPGPATH];
	char		final[MAXPGPATH];
	ClusterCatalogAuthorityMarker m;
	int			fd;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_CATALOG_AUTHORITY_MAGIC;
	m.version = CLUSTER_CATALOG_AUTHORITY_VERSION;
	m.system_identifier = system_identifier;
	m.catalog_version_no = CATALOG_VERSION_NO;
	m.flags = 0;
	m.crc = marker_crc(&m);

	snprintf(tmp, sizeof(tmp), "%s/%s", cluster_shared_data_dir,
			 CLUSTER_CATALOG_AUTHORITY_TMP_REL_PATH);
	snprintf(final, sizeof(final), "%s/%s", cluster_shared_data_dir,
			 CLUSTER_CATALOG_AUTHORITY_REL_PATH);

	fd = OpenTransientFile(tmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not create shared catalog authority marker \"%s\": %m",
						tmp)));
	if (write(fd, &m, sizeof(m)) != sizeof(m))
	{
		CloseTransientFile(fd);
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write shared catalog authority marker \"%s\": %m",
						tmp)));
	}
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync shared catalog authority marker \"%s\": %m",
						tmp)));
	}
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not close shared catalog authority marker \"%s\": %m",
						tmp)));
	if (durable_rename(tmp, final, LOG) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not publish shared catalog authority marker \"%s\"",
						final)));
}

/*
 * seed_base_databases -- seed every base/<db> directory found locally.
 *	At seed time (fresh initdb) these are template0/template1/postgres; each
 *	holds that database's per-db catalog relation files, all of which become
 *	single-authority in the shared tree.  (CREATE DATABASE is fail-closed
 *	under shared_catalog=on, forwarded to spec-6.14b, so no new base/<db>
 *	appears after seed.)
 */
static void
seed_base_databases(const char *local_pgdata)
{
	char		localbase[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;

	snprintf(localbase, sizeof(localbase), "%s/base", local_pgdata);

	dir = AllocateDir(localbase);
	while ((de = ReadDir(dir, localbase)) != NULL)
	{
		char		rel[MAXPGPATH];

		/* database directories are named by numeric db OID */
		if (!isdigit((unsigned char) de->d_name[0]))
			continue;

		snprintf(rel, sizeof(rel), "base/%s", de->d_name);
		seed_dir(local_pgdata, rel);
	}
	FreeDir(dir);
}

void
cluster_catalog_migrate_tree(const char *local_pgdata, uint64 system_identifier)
{
	ClusterCatalogAuthorityMarker existing;

	Assert(cluster_shared_catalog);
	Assert(cluster_shared_data_dir != NULL && cluster_shared_data_dir[0] != '\0');

	/*
	 * JOIN: an authority already exists.  Adopt it after proving it is this
	 * node's own cluster (system identifier) and a compatible catalog layout
	 * (catalog version).  Fail-closed on any mismatch -- never write a
	 * divergent catalog copy over a foreign authority.
	 */
	if (read_marker(&existing))
	{
		if (existing.system_identifier != system_identifier)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					 errmsg("shared catalog authority belongs to a different cluster"),
					 errdetail("Shared authority system identifier " UINT64_FORMAT
							   " does not match this node's " UINT64_FORMAT ".",
							   existing.system_identifier, system_identifier),
					 errhint("Point cluster.shared_data_dir at this cluster's shared tree.")));
		if (existing.catalog_version_no != CATALOG_VERSION_NO)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					 errmsg("shared catalog authority has an incompatible catalog version"),
					 errdetail("Shared authority catalog version %u does not match this "
							   "build's %u.",
							   existing.catalog_version_no, (uint32) CATALOG_VERSION_NO)));

		elog(LOG, "cluster shared_catalog: adopted shared catalog authority (sysid "
			 UINT64_FORMAT ")", system_identifier);
		return;
	}

	/*
	 * SEED: no authority yet.  Copy this node's catalog relation files into
	 * the shared tree, then publish the marker last so a torn seed re-runs
	 * instead of being adopted.
	 */
	seed_dir(local_pgdata, "global");
	seed_base_databases(local_pgdata);
	write_marker(system_identifier);

	elog(LOG, "cluster shared_catalog: seeded shared catalog authority under \"%s\" "
		 "(sysid " UINT64_FORMAT ")", cluster_shared_data_dir, system_identifier);
}
