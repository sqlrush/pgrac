/* Verify fault isolation before using the shim in a PostgreSQL process.
 * Author: SqlRush <sqlrush@gmail.com> */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int
main(int argc, char **argv)
{
	int target;
	int other;
	FILE *control;

	if (argc != 4 || (target = open(argv[1], O_RDWR | O_CREAT, 0600)) < 0 ||
		(other = open(argv[2], O_RDWR | O_CREAT, 0600)) < 0)
		return 1;
	if (fsync(target) != 0 || fsync(other) != 0)
		return 2;
	control = fopen(argv[3], "w");
	if (control == NULL || fprintf(control, "%s\n", argv[1]) < 0 || fclose(control) != 0)
		return 3;
	if (fsync(other) != 0 || fsync(target) != -1 || errno != EIO)
		return 4;
	if (unlink(argv[3]) != 0 || fsync(target) != 0 || fsync(other) != 0)
		return 5;
	return close(target) != 0 || close(other) != 0;
}
