/* Original native relmap bytes, including unused slots and CRC.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_RECOVERY_RELMAP_FIXTURE_H
#define TEST_RECOVERY_RELMAP_FIXTURE_H
#include "common/relmap.h"
#include "utils/relmapper.h"
#include "catalog/pg_tablespace_d.h"

#define TEST_RELMAP_LENGTH (MinSizeOfRelmapUpdate + sizeof(RelMapFile))

static void
test_relmap_payload(char *bytes, bool shared, int count)
{
	xl_relmap_update update
		= { shared ? InvalidOid : 5, shared ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID,
			sizeof(RelMapFile) };
	RelMapFile map;

	memset(&map, 0xA5, sizeof(map)); /* inactive slots remain part of the image */
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = count;
	for (int i = 0; i < count && i < MAX_MAPPINGS; ++i)
		map.mappings[i] = (RelMapping){ 1247 + i, 16385 + i };
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	memcpy(bytes, &update, MinSizeOfRelmapUpdate);
	memcpy(bytes + MinSizeOfRelmapUpdate, &map, sizeof(map));
}

static void
test_relmap_crc(char *bytes)
{
	RelMapFile map;
	memcpy(&map, bytes + MinSizeOfRelmapUpdate, sizeof(map));
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	memcpy(bytes + MinSizeOfRelmapUpdate, &map, sizeof(map));
}
#endif
