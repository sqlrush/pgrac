/* Literal first-input cases.  Codec success grants no creation or writer
 * authority.  Author: SqlRush <sqlrush@gmail.com> */
static void
initialized_digest_fixture(uint8 bytes[1536], const ControlRootImage *root)
{
	static const char domain[] = "PGRAC-INITIALIZED-V1";
	uint8 material[sizeof(domain) + 748], *p = material;

	memcpy(p, domain, sizeof(domain));
	p += sizeof(domain);
	memcpy(p, root->header.storage_uuid, 16);
	memcpy(p + 16, root->header.authority_uuid, 16);
	put_u64_le(p + 32, root->header.system_identifier);
	put_u64_le(p + 40, root->header.v2.database_incarnation);
	put_u64_le(p + 48, root->header.v2.configured[0]);
	put_u64_le(p + 56, root->header.v2.configured[1]);
	put_u64_le(p + 64, root->header.v2.config_generation);
	memcpy(p + 72, root->header.v2.config_sha256, 32);
	put_u64_le(p + 104, root->header.v2.control_image_generation);
	memcpy(p + 112, root->header.v2.control_image_sha256, 32);
	put_u64_le(p + 144, root->header.v2.catalog_manifest_generation);
	memcpy(p + 152, root->header.v2.catalog_manifest_sha256, 32);
	p += 184;
	memcpy(p, bytes + 16, 16);
	memcpy(p + 16, bytes + 256, 512);
	memcpy(p + 528, bytes + 152, 32);
	memcpy(p + 560, bytes + 116, 4);
	sha256_bytes(material, sizeof(material), bytes + 120);
}

static void
initialized_fixture(uint8 bytes[1536], ControlRootImage *root, bool seed, uint32 phase)
{
	uint8 *record = bytes + 256, *claim = bytes + 1280;
	startup_fixture(bytes, root, phase);
	root->header.v2.config_generation = 1;
	root->header.v2.control_image_generation = 1;
	root->header.v2.catalog_manifest_generation = 1;
	root->header.v2.formation_seq = 1;
	root->header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	root->header.v2.configured[0] = 1;
	root->header.v2.configured[1] = 0;
	root->header.v2.serving[0] = root->header.v2.serving[1] = 0;
	for (unsigned i = 1; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
		root->present[i] = false;
		memset(&root->records[i], 0, sizeof(root->records[i]));
		memset(&root->refs[i], 0, sizeof(root->refs[i]));
		memset(&root->startup[i], 0, sizeof(root->startup[i]));
		root->publisher_incarnation[i] = root->publisher_node[i] = root->record_crc32c[i] = 0;
	}
	root->records[0].root_publish_seq = 1;
	root->records[0].identity.root_lineage_seq = 1;
	root->records[0].tail_last_record_crc32c = root->records[0].checkpoint_record_crc32c;
	root->refs[0].history_generation = 0;
	memset(root->refs[0].history_sha256, 0, 32);
	root->refs[0].anchor_generation = 1;
	put_u64_le(record + 16, 1);
	put_u64_le(record + 24, 1);
	put_u32_le(record + 184, 1234);
	memset(record + 216, 0, 40);
	put_u64_le(record + 256, 1);
	put_u32_le(record + 504, image_crc(record, 504));
	put_u32_le(bytes + 12, 4);
	put_u64_le(bytes + 40, 1);
	if (seed) {
		root->header.file_txn_seq = root->startup[0].generation = 1;
		memset(bytes + 48, 0, 48);
		put_u64_le(bytes + 96, 1);
		put_u64_le(bytes + 104, 0);
		memset(bytes + 768, 0, 624);
	} else {
		put_u64_le(bytes + 56, 1);
		put_u64_le(claim + 72, 2);
		put_u64_le(claim + 88, 1);
		put_u32_le(claim + 104, image_crc(claim, 104));
		if (phase == 3) {
			uint8 hash[32];
			sha256_bytes(claim, 112, hash);
			put_u64_le(bytes + 768 + 24, 2);
			put_u32_le(bytes + 768 + 168, image_crc(claim, 104));
			memcpy(bytes + 768 + 296, hash, 32);
			put_u32_le(bytes + 768 + 504, image_crc(bytes + 768, 504));
		}
	}
	initialized_digest_fixture(bytes, root);
	startup_checksum(bytes, root);
}

UT_TEST(test_initialized_seed_literal_roundtrip_has_no_writer)
{
	uint8 bytes[1536], actual[1536];
	ControlRootImage root, before;
	ClusterWalStartupImage input;
	ControlRootStartupRefV3 selected;
	initialized_fixture(bytes, &root, true, 1);
	before = root;
	UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &input), 0);
	UT_ASSERT_EQ(input.input_kind, 4);
	UT_ASSERT_EQ(input.phase, CLUSTER_WAL_STARTUP_RESERVED);
	UT_ASSERT_EQ(input.formation_epoch, 0);
	UT_ASSERT_EQ(input.first_segment_lsn, 0);
	UT_ASSERT(v2_zero(&input.claim, sizeof(input.claim)));
	UT_ASSERT(v2_zero(&input.successor, sizeof(input.successor)));
	UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, actual, &selected), 0);
	UT_ASSERT(memcmp(actual, bytes, sizeof(bytes)) == 0);
	UT_ASSERT(memcmp(&selected, &root.startup[0], sizeof(selected)) == 0);
	UT_ASSERT(memcmp(&before, &root, sizeof(root)) == 0);
}

UT_TEST(test_initialized_bound_phases_keep_real_successor_rules)
{
	uint8 bytes[1536], actual[1536];
	ControlRootImage root;
	ClusterWalStartupImage input;
	ControlRootStartupRefV3 selected;
	for (unsigned phase = 1; phase <= 3; phase++) {
		initialized_fixture(bytes, &root, false, phase);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_decode(bytes, sizeof(bytes), &root, 0, &input), 0);
		UT_ASSERT_EQ(input.input_kind, 4);
		UT_ASSERT(input.formation_epoch != 0 && input.claim.identity.root_lineage_seq == 2);
		UT_ASSERT_EQ(cluster_control_root_v3_startup_encode(&root, 0, &input, actual, &selected), 0);
		UT_ASSERT(memcmp(actual, bytes, sizeof(bytes)) == 0);
		memset(bytes + 48, 0, 8);
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}

UT_TEST(test_initialized_seed_refuses_unearned_target_and_prior_root)
{
	uint8 bytes[1536];
	ControlRootImage root;
	const unsigned offsets[] = { 48, 56, 64, 104, 768, 1280, 1392, 184, 1424 };
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		initialized_fixture(bytes, &root, true, 1);
		bytes[offsets[i]] = 1;
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
	for (unsigned phase = 2; phase <= 3; phase++) {
		initialized_fixture(bytes, &root, true, 1);
		put_u32_le(bytes + 8, phase);
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}

UT_TEST(test_initialized_seed_requires_complete_initial_root)
{
	uint8 bytes[1536];
	ControlRootImage root;
	for (unsigned fault = 0; fault < 13; fault++) {
		initialized_fixture(bytes, &root, true, 1);
		switch (fault) {
		case 0: root.header.file_txn_seq = 2; break;
		case 1: root.header.v2.formation_seq = 2; break;
		case 2: root.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_OPEN; break;
		case 3: root.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_CLOSED; break;
		case 4: root.header.v2.serving[0] = 1; break;
		case 5: root.header.v2.config_generation = 2; break;
		case 6: root.header.v2.control_image_generation = 2; break;
		case 7: root.header.v2.catalog_manifest_generation = 2; break;
		case 8: root.header.v2.configured[0] = 3; break;
		case 9: root.present[1] = true; break;
		case 10: root.startup[0].generation = 2; put_u64_le(bytes + 96, 2); break;
		case 11: root.refs[0].history_generation = 1; memset(root.refs[0].history_sha256, 1, 32); break;
		case 12: root.records[0].identity.root_lineage_seq = 2; break;
		}
		initialized_digest_fixture(bytes, &root);
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}

UT_TEST(test_initialized_bound_source_cannot_be_a_served_generation)
{
	uint8 bytes[1536];
	ControlRootImage root;
	for (unsigned fault = 0; fault < 6; fault++) {
		initialized_fixture(bytes, &root, false, 1);
		switch (fault) {
		case 0:
			root.records[0].identity.root_lineage_seq = 2;
			put_u64_le(bytes + 256 + 24, 2); break;
		case 1:
			root.records[0].root_publish_seq = 2;
			put_u64_le(bytes + 256 + 16, 2); break;
		case 2:
			root.refs[0].history_generation = 1;
			memset(root.refs[0].history_sha256, 1, 32);
			put_u64_le(bytes + 256 + 216, 1);
			memset(bytes + 256 + 224, 1, 32); break;
		case 3:
			root.refs[0].anchor_generation = 2;
			put_u64_le(bytes + 256 + 256, 2); break;
		case 4:
			root.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
			bytes[256 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN; break;
		case 5:
			put_u64_le(bytes + 160, 0x1000080); break;
		}
		put_u32_le(bytes + 256 + 504, image_crc(bytes + 256, 504));
		initialized_digest_fixture(bytes, &root);
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}

UT_TEST(test_initialized_digest_binds_original_common_objects_and_cut)
{
	uint8 bytes[1536];
	ControlRootImage root;
	for (unsigned fault = 0; fault < 6; fault++) {
		initialized_fixture(bytes, &root, false, 1);
		switch (fault) {
		case 0: root.header.v2.config_sha256[0] ^= 1; break;
		case 1: root.header.v2.control_image_sha256[0] ^= 1; break;
		case 2: root.header.v2.catalog_manifest_sha256[0] ^= 1; break;
		case 3: bytes[16] ^= 1; break;
		case 4: bytes[120] ^= 1; break;
		case 5: put_u32_le(bytes + 116, 0x2000000); break;
		}
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}

UT_TEST(test_initialized_does_not_make_clean_or_import_legal)
{
	uint8 bytes[1536];
	ControlRootImage root;
	for (unsigned kind = 0; kind <= 5; kind++) {
		if (kind == 4)
			continue;
		initialized_fixture(bytes, &root, true, 1);
		put_u32_le(bytes + 12, kind);
		startup_checksum(bytes, &root);
		UT_ASSERT(startup_refused(bytes, sizeof(bytes), &root, 0) != 0);
	}
}
