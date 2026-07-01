/*-------------------------------------------------------------------------
 *
 * pg_cluster_basebackup
 *	  Frontend wrapper for pgrac cluster-aware physical backup.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * src/bin/scripts/pg_cluster_basebackup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common.h"
#include "common/logging.h"
#include "fe_utils/option_utils.h"


static void help(const char *progname);
static void exec_command_ok(PGconn *conn, const char *query);
static PGresult *exec_tuples(PGconn *conn, const char *query, int nparams,
							 const char *const *params, int expected_cols);
static void print_result_field(PGresult *res, int col, const char *name);


int
main(int argc, char *argv[])
{
	static struct option long_options[] = {
		{"dbname", required_argument, NULL, 'd'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"username", required_argument, NULL, 'U'},
		{"no-password", no_argument, NULL, 'w'},
		{"password", no_argument, NULL, 'W'},
		{"echo", no_argument, NULL, 'e'},
		{"label", required_argument, NULL, 'l'},
		{"fast", no_argument, NULL, 1},
		{"no-wait", no_argument, NULL, 2},
		{NULL, 0, NULL, 0}
	};

	const char *progname;
	char	   *dbname = NULL;
	char	   *host = NULL;
	char	   *port = NULL;
	char	   *username = NULL;
	char	   *label = NULL;
	enum trivalue prompt_password = TRI_DEFAULT;
	ConnParams	cparams;
	PGconn	   *conn;
	PGresult   *startres;
	PGresult   *stopres;
	PGresult   *historyres;
	const char *start_params[2];
	const char *stop_params[1];
	const char *history_params[1];
	bool		echo = false;
	bool		fast = false;
	bool		waitforarchive = true;
	int			c;
	int			optindex;

	pg_logging_init(argv[0]);
	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pgscripts"));

	handle_help_version_opts(argc, argv, progname, help);

	while ((c = getopt_long(argc, argv, "d:eh:l:p:U:wW", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'd':
				dbname = pg_strdup(optarg);
				break;
			case 'e':
				echo = true;
				break;
			case 'h':
				host = pg_strdup(optarg);
				break;
			case 'l':
				label = pg_strdup(optarg);
				break;
			case 'p':
				port = pg_strdup(optarg);
				break;
			case 'U':
				username = pg_strdup(optarg);
				break;
			case 'w':
				prompt_password = TRI_NO;
				break;
			case 'W':
				prompt_password = TRI_YES;
				break;
			case 1:
				fast = true;
				break;
			case 2:
				waitforarchive = false;
				break;
			default:
				pg_log_error_hint("Try \"%s --help\" for more information.", progname);
				exit(1);
		}
	}

	if (argc - optind > 1)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")", argv[optind + 1]);
		pg_log_error_hint("Try \"%s --help\" for more information.", progname);
		exit(1);
	}
	if (argc - optind == 1)
	{
		if (label != NULL)
		{
			pg_log_error("backup label specified more than once");
			pg_log_error_hint("Try \"%s --help\" for more information.", progname);
			exit(1);
		}
		label = pg_strdup(argv[optind]);
	}
	if (label == NULL)
		label = pg_strdup("pg_cluster_basebackup");
	if (label[0] == '\0')
		pg_fatal("backup label must not be empty");

	if (dbname == NULL)
	{
		const char *envdb = getenv("PGDATABASE");

		dbname = pg_strdup(envdb != NULL && envdb[0] != '\0' ? envdb : "postgres");
	}

	cparams.dbname = dbname;
	cparams.pghost = host;
	cparams.pgport = port;
	cparams.pguser = username;
	cparams.prompt_password = prompt_password;
	cparams.override_dbname = NULL;

	conn = connectDatabase(&cparams, progname, echo, false, false);
	exec_command_ok(conn, "SET client_min_messages = warning");

	start_params[0] = label;
	start_params[1] = fast ? "true" : "false";
	startres = exec_tuples(conn,
						   "SELECT backup_id, start_redo_lsn::text, "
						   "checkpoint_lsn::text, start_tli::text "
						   "FROM pg_catalog.pg_cluster_backup_start($1, $2)",
						   2, start_params, 4);

	stop_params[0] = waitforarchive ? "true" : "false";
	stopres = exec_tuples(conn,
						  "SELECT consistent_scn::text, stop_cut_lsn::text, "
						  "manifest_crc::text "
						  "FROM pg_catalog.pg_cluster_backup_stop($1)",
						  1, stop_params, 3);

	history_params[0] = label;
	historyres = exec_tuples(conn,
							 "SELECT backup_set_path, manifest_path "
							 "FROM pg_catalog.pg_cluster_backup_history "
							 "WHERE backup_id = $1",
							 1, history_params, 2);

	print_result_field(startres, 0, "backup_id");
	print_result_field(startres, 1, "start_redo_lsn");
	print_result_field(startres, 2, "checkpoint_lsn");
	print_result_field(startres, 3, "start_tli");
	print_result_field(stopres, 0, "consistent_scn");
	print_result_field(stopres, 1, "stop_cut_lsn");
	print_result_field(stopres, 2, "manifest_crc");
	print_result_field(historyres, 0, "backup_set_path");
	print_result_field(historyres, 1, "manifest_path");

	PQclear(historyres);
	PQclear(stopres);
	PQclear(startres);
	disconnectDatabase(conn);

	return 0;
}

static void
exec_command_ok(PGconn *conn, const char *query)
{
	PGresult   *res;

	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		pg_log_error("%s", PQerrorMessage(conn));
		PQclear(res);
		disconnectDatabase(conn);
		exit(1);
	}
	PQclear(res);
}

static PGresult *
exec_tuples(PGconn *conn, const char *query, int nparams,
			const char *const *params, int expected_cols)
{
	PGresult   *res;

	res = PQexecParams(conn, query, nparams, NULL, params, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		pg_log_error("%s", PQerrorMessage(conn));
		PQclear(res);
		disconnectDatabase(conn);
		exit(1);
	}
	if (PQntuples(res) != 1 || PQnfields(res) != expected_cols)
	{
		int			ntuples = PQntuples(res);
		int			nfields = PQnfields(res);

		PQclear(res);
		disconnectDatabase(conn);
		pg_fatal("server returned %d row(s) and %d column(s), expected 1 row and %d column(s)",
				 ntuples, nfields, expected_cols);
	}
	return res;
}

static void
print_result_field(PGresult *res, int col, const char *name)
{
	printf("%s=", name);
	if (!PQgetisnull(res, 0, col))
		printf("%s", PQgetvalue(res, 0, col));
	printf("\n");
}

static void
help(const char *progname)
{
	printf(_("%s starts and stops a cluster-aware physical backup.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]... [LABEL]\n"), progname);
	printf(_("\nOptions:\n"));
	printf(_("  -d, --dbname=DBNAME       database to connect to\n"));
	printf(_("  -e, --echo                show commands sent to server\n"));
	printf(_("  -h, --host=HOSTNAME       database server host or socket directory\n"));
	printf(_("  -l, --label=LABEL         backup label/id (default: pg_cluster_basebackup)\n"));
	printf(_("  -p, --port=PORT           database server port\n"));
	printf(_("  -U, --username=USERNAME   user name to connect as\n"));
	printf(_("  -w, --no-password         never prompt for password\n"));
	printf(_("  -W, --password            force password prompt\n"));
	printf(_("      --fast                request a fast checkpoint at backup start\n"));
	printf(_("      --no-wait             do not wait for WAL archive proof at backup stop\n"));
	printf(_("  -?, --help                show this help, then exit\n"));
	printf(_("      --version             output version information, then exit\n"));
	printf(_("\nThe command calls pg_cluster_backup_start() and pg_cluster_backup_stop()\n"));
	printf(_("in one server session and prints key=value metadata for the manifest.\n"));
	printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
	printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
