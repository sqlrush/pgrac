/*-------------------------------------------------------------------------
 * test_cluster_relmap_file_create_wal.inc
 *    Complete creation-record bytes and independently bound replay inputs.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_file_create_wal.inc
 *-------------------------------------------------------------------------
 */
#include "common/cluster_relmap_create.h"

static void
create_wal_crc(unsigned char *b)
{
	pg_crc32c crc;
	memset(b+40,0,4);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,b,CLUSTER_RELMAP_CREATE_WAL_SIZE_V1);
	FIN_CRC32C(crc);
	for(unsigned i=0;i<4;i++) b[40+i]=(uint8)(crc>>(24-8*i));
}

static void
create_wal_image_crc(unsigned char *b)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc,b+64,CLUSTER_RELMAP_FILE_SIZE_V2);
	FIN_CRC32C(crc);
	for(unsigned i=0;i<4;i++) b[36+i]=(uint8)(crc>>(24-8*i));
	create_wal_crc(b);
}

UT_TEST(creation_wal_roundtrip_preserves_full_identity_batch_and_generation_one)
{
	uint8 batch[16]={17,0,0,0,0,0,0,0,0,0,0,0,0,0,0,99};
	unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRelmapCreateWalV1 decoded;
	ClusterRelmapCreateReplayV1 replay;
	ClusterRelmapV2Image inner;
	prepare(); identity.database_oid=2000; identity.tablespace_oid=1663;
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity,batch,&image.committed,bytes,sizeof(bytes)));
	UT_ASSERT(!memcmp(bytes,"RMC1",4) && bytes[7]==1 && bytes[15]==2);
	UT_ASSERT(!memcmp(bytes+16,batch,16) && !memcmp(bytes+64,"RFM2",4));
	UT_ASSERT(cluster_relmap_create_decode_v1(bytes,sizeof(bytes),&decoded));
	UT_ASSERT(!memcmp(&decoded.identity,&identity,sizeof(identity)));
	UT_ASSERT(!memcmp(decoded.batch_uuid,batch,16));
	UT_ASSERT(!memcmp(&decoded.committed,&image.committed,sizeof(RelMapFile)));
	UT_ASSERT(cluster_relmap_file_decode_v2(bytes+64,CLUSTER_RELMAP_FILE_SIZE_V2,&identity,&inner));
	UT_ASSERT(inner.committed_generation==1 && !inner.pending_generation && !inner.pending_owner.wal_end);
	UT_ASSERT(cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&identity,batch,UINT64_C(0x2000088),&replay));
	UT_ASSERT(replay.actual_end==UINT64_C(0x2000088) && !memcmp(&replay.record.committed,&image.committed,sizeof(RelMapFile)));
	cleanup();
}

UT_TEST(creation_wal_every_byte_and_exact_length_are_checked)
{
	uint8 batch[16]={1};
	unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRelmapCreateWalV1 decoded,saved;
	prepare(); identity.database_oid=2000; identity.tablespace_oid=1663;
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity,batch,&image.committed,bytes,sizeof(bytes)));
	memset(&decoded,0xa5,sizeof(decoded)); saved=decoded;
	for(Size i=0;i<sizeof(bytes);i++) {
		bytes[i]^=1;
		UT_ASSERT(!cluster_relmap_create_decode_v1(bytes,sizeof(bytes),&decoded));
		UT_ASSERT(!memcmp(&decoded,&saved,sizeof(saved))); bytes[i]^=1;
	}
	UT_ASSERT(!cluster_relmap_create_decode_v1(bytes,sizeof(bytes)-1,&decoded));
	UT_ASSERT(!cluster_relmap_create_decode_v1(bytes,sizeof(bytes)+1,&decoded));
	UT_ASSERT(!cluster_relmap_create_decode_v1(bytes+64,CLUSTER_RELMAP_FILE_SIZE_V2,&decoded));
	UT_ASSERT(!memcmp(&decoded,&saved,sizeof(saved)));
	cleanup();
}

UT_TEST(creation_wal_rejects_valid_crc_but_wrong_state_birth_and_pending)
{
	uint8 batch[16]={1};
	unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1],good[sizeof(bytes)];
	ClusterRelmapCreateWalV1 decoded;
	prepare(); identity.database_oid=2000; identity.tablespace_oid=1663;
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity,batch,&image.committed,bytes,sizeof(bytes)));
	memcpy(good,bytes,sizeof(bytes));
	for(unsigned i=0;i<4;i++) {
		memcpy(bytes,good,sizeof(bytes));
		if(i==0) bytes[15]=1; /* An UPDATE WAL_INTENT is not CREATE. */
		if(i==1) memset(bytes+16,0,16);
		if(i==2) bytes[44]=1;
		if(i==3) bytes[7]=2;
		create_wal_crc(bytes);
		UT_ASSERT(!cluster_relmap_create_decode_v1(bytes,sizeof(bytes),&decoded));
	}
	for(unsigned pending=0;pending<2;pending++) {
		memcpy(bytes,good,sizeof(bytes));
		image.identity=(ClusterRelmapV2Namespace){91,27,2000,false};
		image.committed_generation=2;
		if(pending) {
			image.pending_generation=3; image.pending=image.committed;
			image.pending_owner=(ClusterRelmapV2Owner){0,UINT64_C(0x100000003),4,5,6,7};
		}
		UT_ASSERT(cluster_relmap_file_encode_v2(&identity,&image,bytes+64,CLUSTER_RELMAP_FILE_SIZE_V2));
		create_wal_image_crc(bytes);
		UT_ASSERT(!cluster_relmap_create_decode_v1(bytes,sizeof(bytes),&decoded));
	}
	cleanup();
}

UT_TEST(creation_rebuild_requires_independent_identity_batch_and_actual_end)
{
	uint8 batch[16]={1},wrong_batch[16]={2};
	unsigned char bytes[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRecoveryFileIdentityV2 wrong;
	ClusterRelmapCreateReplayV1 replay,saved;
	prepare(); identity.database_oid=2000; identity.tablespace_oid=1663;
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity,batch,&image.committed,bytes,sizeof(bytes)));
	memset(&replay,0xa5,sizeof(replay)); saved=replay;
	wrong=identity; ++wrong.creation_generation;
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&wrong,batch,0x2000088,&replay));
	wrong=identity; wrong.origin=1;
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&wrong,batch,0x2000088,&replay));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&identity,wrong_batch,0x2000088,&replay));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&identity,batch,0,&replay));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(bytes,sizeof(bytes),&identity,batch,UINT64_MAX,&replay));
	UT_ASSERT(!memcmp(&replay,&saved,sizeof(saved)));
	cleanup();
}

UT_TEST(creation_wal_alias_and_invalid_input_do_not_change_any_output)
{
	union { ClusterRelmapCreateReplayV1 replay; unsigned char bytes[4096]; } area,saved;
	uint8 batch[16]={1},zero[16]={0};
	unsigned char record[CLUSTER_RELMAP_CREATE_WAL_SIZE_V1];
	ClusterRelmapCreateWalV1 decoded;
	prepare(); identity.database_oid=2000; identity.tablespace_oid=1663;
	memset(&area,0xa5,sizeof(area)); saved=area;
	UT_ASSERT(!cluster_relmap_create_encode_v1(&identity,zero,&image.committed,area.bytes,sizeof(record)));
	UT_ASSERT(!memcmp(&area,&saved,sizeof(area)));
	UT_ASSERT(cluster_relmap_create_encode_v1(&identity,batch,&image.committed,record,sizeof(record)));
	memcpy(area.bytes,record,sizeof(record)); saved=area;
	UT_ASSERT(!cluster_relmap_create_decode_v1(area.bytes,sizeof(record),(ClusterRelmapCreateWalV1 *)(area.bytes+1)));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(area.bytes,sizeof(record),&identity,batch,123,&area.replay));
	UT_ASSERT(!memcmp(&area,&saved,sizeof(area)));
	memset(&area,0xa5,sizeof(area)); area.replay.record.identity=identity; saved=area;
	UT_ASSERT(!cluster_relmap_create_encode_v1(&area.replay.record.identity,batch,&image.committed,area.bytes,sizeof(record)));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(record,sizeof(record),&area.replay.record.identity,batch,123,&area.replay));
	UT_ASSERT(!memcmp(&area,&saved,sizeof(area)));
	memset(&area,0xa5,sizeof(area)); memcpy(area.bytes,batch,16); saved=area;
	UT_ASSERT(!cluster_relmap_create_encode_v1(&identity,area.bytes,&image.committed,area.bytes,sizeof(record)));
	UT_ASSERT(!cluster_relmap_create_rebuild_v1(record,sizeof(record),&identity,area.bytes,123,&area.replay));
	UT_ASSERT(!memcmp(&area,&saved,sizeof(area)));
	identity.database_oid=0;
	UT_ASSERT(!cluster_relmap_create_encode_v1(&identity,batch,&image.committed,area.bytes,sizeof(record)));
	UT_ASSERT(!cluster_relmap_create_decode_v1(NULL,sizeof(record),&decoded));
	UT_ASSERT(!memcmp(&area,&saved,sizeof(area)));
	cleanup();
}
