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
 *	  The seed branch also seeds the relation-map authority (spec-6.14 D5):
 *	  the local pg_filenode.map images (global + per database) become the
 *	  committed generation-1 images of the shared relmap authority, which the
 *	  relmapper reads instead of local files under shared_catalog=on.  Seeding
 *	  requires a cleanly shut down local node: tail WAL could hold
 *	  XLOG_RELMAP_UPDATE records that replay only into the (unused) local
 *	  files, leaving a freshly seeded authority permanently stale.
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

#include "access/transam.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_migrate.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_relmap_authority.h"
#include "common/controldata_utils.h"
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
	size_t len;

	if (name == NULL || !isdigit((unsigned char)name[0]))
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
	char dsttmp[MAXPGPATH];
	int srcfd;
	int dstfd;
	char buf[BLCKSZ];
	int nread;

	if (snprintf(dsttmp, sizeof(dsttmp), "%s.tmp", dst) >= (int)sizeof(dsttmp))
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
						errmsg("shared catalog authority path too long: \"%s\"", dst)));

	srcfd = OpenTransientFile(src, O_RDONLY | PG_BINARY);
	if (srcfd < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open catalog file \"%s\" for shared migration: %m", src)));

	dstfd = OpenTransientFile(dsttmp, O_RDWR | O_CREAT | O_TRUNC | PG_BINARY);
	if (dstfd < 0) {
		CloseTransientFile(srcfd);
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create shared catalog file \"%s\": %m", dsttmp)));
	}

	while ((nread = read(srcfd, buf, sizeof(buf))) > 0) {
		if (write(dstfd, buf, nread) != nread) {
			CloseTransientFile(srcfd);
			CloseTransientFile(dstfd);
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not write shared catalog file \"%s\": %m", dsttmp)));
		}
	}
	if (nread < 0) {
		CloseTransientFile(srcfd);
		CloseTransientFile(dstfd);
		ereport(FATAL,
				(errcode_for_file_access(), errmsg("could not read catalog file \"%s\": %m", src)));
	}

	if (pg_fsync(dstfd) != 0) {
		CloseTransientFile(srcfd);
		CloseTransientFile(dstfd);
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fsync shared catalog file \"%s\": %m", dsttmp)));
	}

	if (CloseTransientFile(dstfd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close shared catalog file \"%s\": %m", dsttmp)));
	CloseTransientFile(srcfd);

	/* Atomically publish the fully-written file (fsyncs the directory too). */
	if (durable_rename(dsttmp, dst, LOG) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not rename shared catalog file \"%s\" to \"%s\"", dsttmp, dst)));
}

/*
 * seed_dir -- copy every relation file from <local>/<rel> into <shared>/<rel>,
 *	creating the shared subdirectory tree as needed.  `rel` is a PGDATA-
 *	relative directory such as "global" or "base/1".
 */
static void
seed_dir(const char *local_pgdata, const char *rel)
{
	char localdir[MAXPGPATH];
	char shareddir[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	snprintf(localdir, sizeof(localdir), "%s/%s", local_pgdata, rel);
	snprintf(shareddir, sizeof(shareddir), "%s/%s", cluster_shared_data_dir, rel);

	/* The shared root has no pre-created base/<db>; materialise it. */
	if (pg_mkdir_p(shareddir, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not create shared catalog directory \"%s\": %m", shareddir)));

	dir = AllocateDir(localdir);
	while ((de = ReadDir(dir, localdir)) != NULL) {
		char srcpath[MAXPGPATH];
		char dstpath[MAXPGPATH];

		if (!is_relation_filename(de->d_name))
			continue;

		snprintf(srcpath, sizeof(srcpath), "%s/%s", localdir, de->d_name);
		snprintf(dstpath, sizeof(dstpath), "%s/%s", shareddir, de->d_name);
		copy_file_durable(srcpath, dstpath);
	}
	FreeDir(dir);
}

/*
 * is_init_fork_filename -- is `name` an unlogged relation's init fork?
 *
 *	Init forks ("<relfilenumber>_init") exist for unlogged relations (and
 *	their indexes) only, so their presence is an exact storage-level witness
 *	that an unlogged relation exists -- no catalog access needed this early
 *	in startup.
 */
static bool
is_init_fork_filename(const char *name)
{
	size_t len;

	if (!is_relation_filename(name))
		return false;
	len = strlen(name);
	return len > 5 && strcmp(name + len - 5, "_init") == 0;
}

/*
 * vet_dir_no_init -- FATAL if <root>/<rel> holds any init fork.
 *
 *	A missing directory is fine (the shared tree has no base/<db> before the
 *	seed; a local join node may have a sparse tree).
 */
static void
vet_dir_no_init(const char *root, const char *rel)
{
	char dirpath[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	snprintf(dirpath, sizeof(dirpath), "%s/%s", root, rel);

	dir = AllocateDir(dirpath);
	if (dir == NULL) {
		if (errno == ENOENT)
			return;
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open directory \"%s\" for the unlogged-relation vet: %m",
							   dirpath)));
	}

	while ((de = ReadDir(dir, dirpath)) != NULL) {
		if (is_init_fork_filename(de->d_name))
			ereport(FATAL, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster.shared_catalog cannot be enabled: unlogged relation "
								   "storage exists"),
							errdetail("Init fork \"%s/%s\" indicates an unlogged relation.",
									  dirpath, de->d_name),
							errhint("Drop existing unlogged relations (or ALTER TABLE ... SET "
									"LOGGED) before enabling cluster.shared_catalog; cluster-wide "
									"unlogged support is spec-6.14c.")));
	}
	FreeDir(dir);
}

/*
 * vet_tree_no_init -- vet one PGDATA-shaped tree (global/ + every base/<db>).
 */
static void
vet_tree_no_init(const char *root)
{
	char basepath[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	vet_dir_no_init(root, "global");

	snprintf(basepath, sizeof(basepath), "%s/base", root);
	dir = AllocateDir(basepath);
	if (dir == NULL) {
		if (errno == ENOENT)
			return;
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open directory \"%s\" for the unlogged-relation vet: %m",
							   basepath)));
	}
	while ((de = ReadDir(dir, basepath)) != NULL) {
		char rel[MAXPGPATH];

		if (!isdigit((unsigned char)de->d_name[0]))
			continue;
		snprintf(rel, sizeof(rel), "base/%s", de->d_name);
		vet_dir_no_init(root, rel);
	}
	FreeDir(dir);
}

/*
 * cluster_catalog_vet_no_unlogged -- spec-6.14 Q12 enable-time vet.
 *
 *	Unlogged relations need a cluster-wide crash-reset protocol (any node's
 *	restart resets the SHARED init-fork copy under every other node) that is
 *	not implemented (spec-6.14c); CREATE UNLOGGED / SET UNLOGGED are rejected
 *	at runtime, and this vet fail-closes the remaining hole: pre-existing
 *	unlogged storage when the GUC is turned on.  Both trees are checked every
 *	shared-catalog boot: the local tree (the seed source) and the shared tree
 *	(an off-mode window with cluster.smgr_user_relations=on routes user
 *	relations -- unlogged included -- into the shared tree).
 */
void
cluster_catalog_vet_no_unlogged(const char *local_pgdata)
{
	Assert(cluster_shared_catalog);

	vet_tree_no_init(local_pgdata);
	if (cluster_shared_data_dir != NULL && cluster_shared_data_dir[0] != '\0')
		vet_tree_no_init(cluster_shared_data_dir);
}

/*
 * marker_crc -- CRC over the marker's bytes preceding the crc field.
 */
static pg_crc32c
marker_crc(const ClusterCatalogAuthorityMarker *m)
{
	pg_crc32c crc;

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
	char path[MAXPGPATH];
	int fd;
	int nread;

	snprintf(path, sizeof(path), "%s/%s", cluster_shared_data_dir,
			 CLUSTER_CATALOG_AUTHORITY_REL_PATH);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0) {
		if (errno == ENOENT)
			return false; /* absent: this node is the seed */
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open shared catalog authority marker \"%s\": %m", path)));
	}

	nread = read(fd, out, sizeof(*out));
	CloseTransientFile(fd);

	if (nread != (int)sizeof(*out) || out->magic != CLUSTER_CATALOG_AUTHORITY_MAGIC
		|| out->version != CLUSTER_CATALOG_AUTHORITY_VERSION || out->crc != marker_crc(out))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("shared catalog authority marker \"%s\" is corrupt or truncated", path),
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
	char tmp[MAXPGPATH];
	char final[MAXPGPATH];
	ClusterCatalogAuthorityMarker m;
	int fd;

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
				 errmsg("could not create shared catalog authority marker \"%s\": %m", tmp)));
	if (write(fd, &m, sizeof(m)) != sizeof(m)) {
		CloseTransientFile(fd);
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write shared catalog authority marker \"%s\": %m", tmp)));
	}
	if (pg_fsync(fd) != 0) {
		CloseTransientFile(fd);
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not fsync shared catalog authority marker \"%s\": %m", tmp)));
	}
	if (CloseTransientFile(fd) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not close shared catalog authority marker \"%s\": %m", tmp)));
	if (durable_rename(tmp, final, LOG) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not publish shared catalog authority marker \"%s\"", final)));
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
	char localbase[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	snprintf(localbase, sizeof(localbase), "%s/base", local_pgdata);

	dir = AllocateDir(localbase);
	while ((de = ReadDir(dir, localbase)) != NULL) {
		char rel[MAXPGPATH];

		/* database directories are named by numeric db OID */
		if (!isdigit((unsigned char)de->d_name[0]))
			continue;

		snprintf(rel, sizeof(rel), "base/%s", de->d_name);
		seed_dir(local_pgdata, rel);
	}
	FreeDir(dir);
}

/*
 * seed_relmap_one -- seed one relmap authority (global or per-db) from the
 *	local pg_filenode.map under <local_pgdata>/<rel>.  Idempotent: an already
 *	valid authority (torn re-seed after a crash between relmap seed and the
 *	marker write) is left untouched.
 *
 *	The image bytes are opaque here (they carry RelMapFile's own CRC, which
 *	the relmapper validates on every load); we only bound their size.  The
 *	write_pending(1) + publish(1) pair leaves committed_generation=1 and no
 *	pending, so the foreign-pending write guard never sees a seed residue.
 */
static void
seed_relmap_one(const char *local_pgdata, const char *rel, bool shared_map, Oid dbid)
{
	char path[MAXPGPATH];
	char image[CLUSTER_RELMAP_IMAGE_MAX];
	ClusterRelmapAuthorityHeader hdr;
	ClusterRelmapOwner owner;
	struct stat st;
	int fd;
	int nread;

	if (cluster_relmap_authority_read_header(shared_map, dbid, &hdr))
		return; /* already seeded (idempotent re-run) */

	/* RELMAPPER_FILENAME is private to relmapper.c; the name is fixed. */
	snprintf(path, sizeof(path), "%s/%s/pg_filenode.map", local_pgdata, rel);

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open relation map file \"%s\" for the shared "
							   "relmap authority seed: %m",
							   path)));
	if (fstat(fd, &st) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not stat relation map file \"%s\": %m", path)));
	if (st.st_size <= 0 || st.st_size > CLUSTER_RELMAP_IMAGE_MAX)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
						errmsg("relation map file \"%s\" has unexpected size %lld", path,
							   (long long)st.st_size),
						errhint("The local relation map is not trustworthy; restore the "
								"node from backup before seeding cluster.shared_catalog.")));
	nread = read(fd, image, (size_t)st.st_size);
	if (nread != (int)st.st_size)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read relation map file \"%s\": %m", path)));
	CloseTransientFile(fd);

	owner.owner_node = (int16)cluster_node_id;
	owner.owner_xid = InvalidTransactionId;
	owner.owner_epoch = 0;
	owner.relmap_lsn = 0;

	cluster_relmap_authority_write_pending(shared_map, dbid, image, (uint32)st.st_size, 1, &owner);
	cluster_relmap_authority_publish(shared_map, dbid, 1);
}

/*
 * seed_relmap_authority -- seed the shared relmap authorities (global map +
 *	every base/<db> map) from this node's local pg_filenode.map files.
 */
static void
seed_relmap_authority(const char *local_pgdata)
{
	char localbase[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	seed_relmap_one(local_pgdata, "global", true, InvalidOid);

	snprintf(localbase, sizeof(localbase), "%s/base", local_pgdata);
	dir = AllocateDir(localbase);
	while ((de = ReadDir(dir, localbase)) != NULL) {
		char rel[MAXPGPATH];

		if (!isdigit((unsigned char)de->d_name[0]))
			continue;

		snprintf(rel, sizeof(rel), "base/%s", de->d_name);
		seed_relmap_one(local_pgdata, rel, false, atooid(de->d_name));
	}
	FreeDir(dir);
}

/*
 * vet_seed_clean_shutdown -- FATAL unless the local node was cleanly shut
 *	down.  Seeding from a crashed node would copy a pg_filenode.map that is
 *	missing tail-WAL XLOG_RELMAP_UPDATE records; replay heals only the local
 *	files (relmap_redo never touches the authority, by design -- INV-14-7
 *	arbitration discipline), so the authority would be permanently stale.
 *	Catalog relation files do not need this vet: their tail WAL replays into
 *	the shared tree through the shared smgr route and heals the seeded copy.
 */
static void
vet_seed_clean_shutdown(const char *local_pgdata)
{
	ControlFileData *lcf;
	bool crc_ok;

	lcf = get_controlfile(local_pgdata, &crc_ok);
	if (!crc_ok)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
						errmsg("local pg_control has an invalid checksum; cannot seed "
							   "the shared catalog authority")));
	if (lcf->state != DB_SHUTDOWNED)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
						errmsg("seeding the shared catalog authority requires a cleanly "
							   "shut down node"),
						errdetail("Local pg_control state is \"%s\"; pending WAL could "
								  "hold relation-map updates the seeded authority would "
								  "never see.",
								  lcf->state == DB_SHUTDOWNED_IN_RECOVERY ? "shut down in recovery"
																		  : "not shut down"),
						errhint("Start and cleanly stop this node with "
								"cluster.shared_catalog=off, then enable it.")));
	pfree(lcf);
}

/*
 * vet_seed_no_tablespaces -- FATAL if any user tablespace exists at seed
 *	time.  A database whose default tablespace is not pg_default keeps its
 *	per-db catalogs (and pg_filenode.map) under pg_tblspc/, which the seed
 *	does not walk; its relmap authority would silently never exist and every
 *	later connection would fail 53RB0.  CREATE TABLESPACE is already rejected
 *	under shared_catalog=on (Q12), so vetting the pre-existing stock here
 *	closes the whole class.
 */
static void
vet_seed_no_tablespaces(const char *local_pgdata)
{
	char tblspcdir[MAXPGPATH];
	DIR *dir;
	struct dirent *de;

	snprintf(tblspcdir, sizeof(tblspcdir), "%s/pg_tblspc", local_pgdata);

	dir = AllocateDir(tblspcdir);
	if (dir == NULL) {
		if (errno == ENOENT)
			return;
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\" for the tablespace vet: %m", tblspcdir)));
	}
	while ((de = ReadDir(dir, tblspcdir)) != NULL) {
		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;
		ereport(FATAL, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.shared_catalog cannot be enabled: user tablespace "
							   "storage exists"),
						errdetail("\"%s/%s\" exists; per-database catalogs under a user "
								  "tablespace are outside the shared catalog seed.",
								  tblspcdir, de->d_name),
						errhint("Drop all user tablespaces before enabling "
								"cluster.shared_catalog; tablespace support is spec-6.14b.")));
	}
	FreeDir(dir);
}

void
cluster_catalog_migrate_tree(const char *local_pgdata, uint64 system_identifier)
{
	ClusterCatalogAuthorityMarker existing;

	Assert(cluster_shared_catalog);
	Assert(cluster_shared_data_dir != NULL && cluster_shared_data_dir[0] != '\0');

	/* spec-6.14 Q12: fail-closed BEFORE any adopt or seed side effect. */
	cluster_catalog_vet_no_unlogged(local_pgdata);

	/*
	 * JOIN: an authority already exists.  Adopt it after proving it is this
	 * node's own cluster (system identifier) and a compatible catalog layout
	 * (catalog version).  Fail-closed on any mismatch -- never write a
	 * divergent catalog copy over a foreign authority.
	 */
	if (read_marker(&existing)) {
		if (existing.system_identifier != system_identifier)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					 errmsg("shared catalog authority belongs to a different cluster"),
					 errdetail("Shared authority system identifier " UINT64_FORMAT
							   " does not match this node's " UINT64_FORMAT ".",
							   existing.system_identifier, system_identifier),
					 errhint("Point cluster.shared_data_dir at this cluster's shared tree.")));
		if (existing.catalog_version_no != CATALOG_VERSION_NO)
			ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
							errmsg("shared catalog authority has an incompatible catalog version"),
							errdetail("Shared authority catalog version %u does not match this "
									  "build's %u.",
									  existing.catalog_version_no, (uint32)CATALOG_VERSION_NO)));

		elog(LOG,
			 "cluster shared_catalog: adopted shared catalog authority (sysid " UINT64_FORMAT ")",
			 system_identifier);
		return;
	}

	/*
	 * SEED: no authority yet.  Copy this node's catalog relation files into
	 * the shared tree, then publish the marker last so a torn seed re-runs
	 * instead of being adopted.  Seed-only vets first (a join node may well
	 * be crash-restarting; a seed node must be pristine).
	 */
	vet_seed_clean_shutdown(local_pgdata);
	vet_seed_no_tablespaces(local_pgdata);

	seed_dir(local_pgdata, "global");
	seed_base_databases(local_pgdata);
	seed_relmap_authority(local_pgdata);
	write_marker(system_identifier);

	elog(LOG,
		 "cluster shared_catalog: seeded shared catalog authority under \"%s\" "
		 "(sysid " UINT64_FORMAT ")",
		 cluster_shared_data_dir, system_identifier);
}

/*
 * cluster_catalog_vet_off_mode -- spec-6.14 D5 off-flip vet.
 *
 *	Booting with cluster.shared_catalog=off against a shared tree that holds
 *	a catalog authority marker is refused outright.  Once a node has run with
 *	the shared catalog, every on-era DDL lives only in the shared tree: the
 *	local catalog files (and pg_filenode.map) are frozen at their seed-time
 *	state, so silently flipping back would serve a stale catalog -- lost
 *	tables at best, wrong-relfilenumber reads at worst.
 */
void
cluster_catalog_vet_off_mode(void)
{
	ClusterCatalogAuthorityMarker m;

	Assert(!cluster_shared_catalog);

	if (cluster_shared_data_dir == NULL || cluster_shared_data_dir[0] == '\0')
		return;

	if (!read_marker(&m))
		return;

	ereport(FATAL, (errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					errmsg("cluster.shared_catalog is off but the shared tree holds a "
						   "shared catalog authority"),
					errdetail("\"%s/%s\" certifies that this cluster's catalog lives in "
							  "the shared tree; the node-local catalog files are stale "
							  "once any DDL ran with cluster.shared_catalog=on.",
							  cluster_shared_data_dir, CLUSTER_CATALOG_AUTHORITY_REL_PATH),
					errhint("Re-enable cluster.shared_catalog, or restore this node from "
							"a backup taken before the shared catalog was seeded.")));
}
