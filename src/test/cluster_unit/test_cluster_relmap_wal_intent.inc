/* PGRAC: WAL intent is not an installable file or an authority grant.
 * Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md */
static void
prepare_intent_image(void)
{
	prepare();
	image.pending_generation = 8;
	image.pending_owner = (ClusterRelmapV2Owner) {3, UINT64CONST(0x200000123), 7, 101, 0, 202};
	image.pending = image.committed;
	image.pending.mappings[0].mapfilenumber = 9002;
	INIT_CRC32C(image.pending.crc);
	COMP_CRC32C(image.pending.crc, &image.pending, offsetof(RelMapFile, crc));
	FIN_CRC32C(image.pending.crc);
}

UT_TEST(wal_intent_roundtrip_remains_noninstallable)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image actual;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT(cluster_relmap_wal_observe_intent_v2(bytes, sizeof(bytes), &observed, &actual));
	UT_ASSERT_EQ(bytes[181], 1);
	for (unsigned n = 208; n < 216; ++n) UT_ASSERT_EQ(bytes[n], 0);
	UT_ASSERT_EQ(actual.pending_owner.wal_end, 0);
	UT_ASSERT(memcmp(&identity, &observed, sizeof(identity)) == 0);
	UT_ASSERT(memcmp(&image, &actual, sizeof(image)) == 0);
	UT_ASSERT(!cluster_relmap_file_decode_v2(bytes, sizeof(bytes), &identity, &actual));
	UT_ASSERT(!cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	cleanup();
}

UT_TEST(wal_intent_rebuild_is_the_exact_pending_file)
{
	unsigned char intent[CLUSTER_RELMAP_FILE_SIZE_V2];
	unsigned char expected[CLUSTER_RELMAP_FILE_SIZE_V2], actual[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2Image installed;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, intent, sizeof(intent)));
	for (unsigned n = 0; n < 2; ++n)
	{
		uint64 end = n ? UINT64CONST(0x200010280) : UINT64CONST(0x100010280);
		image.pending_owner.wal_end = end;
		UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, expected, sizeof(expected)));
		UT_ASSERT(cluster_relmap_wal_rebuild_pending_v2(intent, sizeof(intent), &identity,
			end, actual, sizeof(actual)));
		UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
		UT_ASSERT_EQ(actual[181], 0);
		for (unsigned i = 0; i < sizeof(actual); ++i)
			if (i != 181 && !(i >= 208 && i < 216) && !(i >= 252 && i < 256)
				&& !(i >= 116 && i < 128))
				UT_ASSERT_EQ(actual[i], intent[i]);
		UT_ASSERT(cluster_relmap_file_decode_v2(actual, sizeof(actual), &identity, &installed));
		UT_ASSERT_EQ(installed.pending_owner.wal_end, end);
		UT_ASSERT_EQ(installed.committed_generation, 7);
		UT_ASSERT_EQ(installed.pending_generation, 8);
	}
	cleanup();
}

UT_TEST(wal_intent_and_file_modes_refuse_each_other)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], rebuilt[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image actual;
	prepare();
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(bytes), &observed, &actual));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(bytes), &identity,
		UINT64CONST(0x100010280), rebuilt, sizeof(rebuilt)));
	cleanup();
	prepare_intent_image();
	image.pending_owner.wal_end = UINT64CONST(0x100010280);
	UT_ASSERT(cluster_relmap_file_encode_v2(&identity, &image, bytes, sizeof(bytes)));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(bytes), &observed, &actual));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(bytes), &identity,
		UINT64CONST(0x100010280), rebuilt, sizeof(rebuilt)));
	cleanup();
}

UT_TEST(wal_intent_validates_complete_owner_and_namespace)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], unchanged[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2Image valid;
	prepare_intent_image(); valid = image;
	memset(unchanged, 0x5a, sizeof(unchanged));
	for (unsigned n = 0; n < 9; ++n)
	{
		image = valid; memcpy(bytes, unchanged, sizeof(bytes));
		switch (n)
		{
			case 0: image.pending_owner.wal_end = 1; break;
			case 1: image.pending_generation = 0; break;
			case 2: image.pending_generation = 9; break;
			case 3: image.pending_owner.node = 128; break;
			case 4: image.pending_owner.top_full_xid = UINT64CONST(0x200000002); break;
			case 5: image.pending_owner.admitted_incarnation = 0; break;
			case 6: image.pending_owner.boot_incarnation = 0; break;
			case 7: image.identity.system_identifier++; break;
			case 8: image.pending.crc ^= 1; break;
		}
		UT_ASSERT(!cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)));
		UT_ASSERT(memcmp(bytes, unchanged, sizeof(bytes)) == 0);
	}
	cleanup();
}

UT_TEST(wal_intent_damage_never_publishes_partial_outputs)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], rebuilt[CLUSTER_RELMAP_FILE_SIZE_V2];
	unsigned char unchanged[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 observed, old_observed;
	ClusterRelmapV2Image actual, old_actual;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)));
	memset(&old_observed, 0x5a, sizeof(old_observed)); memset(&old_actual, 0x6b, sizeof(old_actual));
	memset(unchanged, 0x7c, sizeof(unchanged));
	for (unsigned n = 0; n < sizeof(bytes); ++n)
	{
		bytes[n] ^= 1; observed = old_observed; actual = old_actual;
		memcpy(rebuilt, unchanged, sizeof(rebuilt));
		UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(bytes), &observed, &actual));
		UT_ASSERT(memcmp(&observed, &old_observed, sizeof(observed)) == 0);
		UT_ASSERT(memcmp(&actual, &old_actual, sizeof(actual)) == 0);
		UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(bytes), &identity,
			UINT64CONST(0x100010280), rebuilt, sizeof(rebuilt)));
		UT_ASSERT(memcmp(rebuilt, unchanged, sizeof(rebuilt)) == 0);
		bytes[n] ^= 1;
	}
	cleanup();
}

UT_TEST(wal_intent_rebuild_requires_expected_and_actual_end)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], actual[CLUSTER_RELMAP_FILE_SIZE_V2];
	unsigned char unchanged[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 wrong;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)));
	memset(unchanged, 0x5a, sizeof(unchanged)); memcpy(actual, unchanged, sizeof(actual));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(bytes), &identity,
		0, actual, sizeof(actual)));
	UT_ASSERT(memcmp(actual, unchanged, sizeof(actual)) == 0);
	for (unsigned n = 0; n < 4; ++n)
	{
		wrong = identity;
		if (n == 0) ++wrong.creation_generation;
		if (n == 1) ++wrong.registration_generation;
		if (n == 2) ++wrong.redo_branch_generation;
		if (n == 3) wrong.storage_uuid[0] ^= 1;
		UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(bytes), &wrong,
			UINT64CONST(0x100010280), actual, sizeof(actual)));
		UT_ASSERT(memcmp(actual, unchanged, sizeof(actual)) == 0);
	}
	cleanup();
}

UT_TEST(wal_intent_output_aliases_refuse_before_mutation)
{
	union { unsigned char bytes[8192]; ClusterRecoveryFileIdentityV2 id; ClusterRelmapV2Image image; } u;
	unsigned char saved[sizeof(u)], wire[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRelmapV2Image actual;
	ClusterRecoveryFileIdentityV2 observed;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, wire, sizeof(wire)));
	memset(&u, 0x5a, sizeof(u)); memcpy(u.bytes, wire, sizeof(wire)); memcpy(saved, &u, sizeof(u));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(u.bytes, sizeof(wire), &u.id, &actual));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(u.bytes, sizeof(wire), &observed, &u.image));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(wire, sizeof(wire), &u.id, &u.image));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(u.bytes, sizeof(wire), &identity,
		UINT64CONST(0x100010280), u.bytes + 1, sizeof(wire)));
	UT_ASSERT(memcmp(&u, saved, sizeof(u)) == 0);
	u.id = identity; memcpy(saved, &u, sizeof(u));
	UT_ASSERT(!cluster_relmap_wal_encode_intent_v2(&u.id, &image, u.bytes, sizeof(wire)));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(wire, sizeof(wire), &u.id,
		UINT64CONST(0x100010280), u.bytes, sizeof(wire)));
	UT_ASSERT(memcmp(&u, saved, sizeof(u)) == 0);
	u.image = image; memcpy(saved, &u, sizeof(u));
	UT_ASSERT(!cluster_relmap_wal_encode_intent_v2(&identity, &u.image, u.bytes, sizeof(wire)));
	UT_ASSERT(memcmp(&u, saved, sizeof(u)) == 0);
	cleanup();
}

UT_TEST(wal_intent_null_and_length_errors_preserve_output)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2 + 1], out[CLUSTER_RELMAP_FILE_SIZE_V2];
	unsigned char unchanged[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image actual;
	prepare_intent_image(); memset(unchanged, 0x5a, sizeof(unchanged)); memcpy(out, unchanged, sizeof(out));
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, bytes, sizeof(bytes)-1));
	UT_ASSERT(!cluster_relmap_wal_encode_intent_v2(NULL, &image, out, sizeof(out)));
	UT_ASSERT(!cluster_relmap_wal_encode_intent_v2(&identity, NULL, out, sizeof(out)));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(NULL, sizeof(out), &observed, &actual));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(out), NULL, &actual));
	UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(out), &observed, NULL));
	for (unsigned n = 0; n < 3; ++n)
	{
		size_t length = n == 0 ? 2176 : n == 1 ? 2303 : 2305;
		UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, length, &observed, &actual));
		UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, length, &identity,
			UINT64CONST(0x100010280), out, sizeof(out)));
	}
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(out), NULL,
		UINT64CONST(0x100010280), out, sizeof(out)));
	UT_ASSERT(!cluster_relmap_wal_rebuild_pending_v2(bytes, sizeof(out), &identity,
		UINT64CONST(0x100010280), out, sizeof(out)-1));
	UT_ASSERT(memcmp(out, unchanged, sizeof(out)) == 0);
	cleanup();
}

UT_TEST(wal_intent_state_and_end_are_checked_after_resigned_crcs)
{
	unsigned char bytes[CLUSTER_RELMAP_FILE_SIZE_V2], valid[CLUSTER_RELMAP_FILE_SIZE_V2];
	ClusterRecoveryFileIdentityV2 observed;
	ClusterRelmapV2Image actual;
	pg_crc32c crc;
	prepare_intent_image();
	UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&identity, &image, valid, sizeof(valid)));
	for (unsigned n = 0; n < 4; ++n)
	{
		memcpy(bytes, valid, sizeof(bytes));
		if (n == 0) bytes[181] = 2;
		if (n == 1) bytes[181] = 0;
		if (n == 2) bytes[208] = 1;
		if (n == 3) memset(bytes + 168, 0, 8);
		INIT_CRC32C(crc); COMP_CRC32C(crc, bytes + 128, 124); FIN_CRC32C(crc);
		for (unsigned i = 0; i < 4; ++i) bytes[252+i] = (crc >> (8*i)) & 255;
		refresh_envelope(bytes);
		UT_ASSERT(!cluster_relmap_wal_observe_intent_v2(bytes, sizeof(bytes), &observed, &actual));
		UT_ASSERT(!cluster_relmap_file_observe_v2(bytes, sizeof(bytes), &observed, &actual));
	}
	cleanup();
}
