/* Native member-slot observer tests; no device writes or startup authority.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define main voting_helper_main
#include "../voting_io.c"
#undef main
#include <assert.h>

static void
put64_test(unsigned char *p, uint64_t value)
{
	for (unsigned int n = 0; n < 8; n++)
		p[n] = (unsigned char)(value >> (8 * n));
}

static void
seal(unsigned char *slot)
{
	put32(slot + CRC_OFFSET, crc32c(slot, CRC_OFFSET));
}

int
main(void)
{
	unsigned char image[IMAGE_BYTES];
	MemberObservation observed;
	unsigned char *slot = image + 3 * SLOT_BYTES;

	initial_image(image, 2);
	assert(observe_member(slot, 3, 2, &observed));
	assert(observed.incarnation == 0 && observed.flags == 0);
	put64_test(slot + 16, UINT64_C(843294186184659));
	put64_test(slot + 24, UINT64_C(987654321));
	put64_test(slot + 32, 17);
	put64_test(slot + 40, 1);
	put64_test(slot + 56, 22);
	seal(slot);
	assert(observe_member(slot, 3, 2, &observed));
	assert(observed.incarnation == UINT64_C(843294186184659));
	assert(observed.heartbeat_us == UINT64_C(987654321));
	assert(observed.epoch == 17 && observed.flags == 1 && observed.generation == 22);
	/* Normal shutdown clears flags, not the old incarnation or generation. */
	put64_test(slot + 40, 0);
	put64_test(slot + 56, 23);
	seal(slot);
	assert(observe_member(slot, 3, 2, &observed));
	assert(observed.incarnation != 0 && observed.flags == 0 && observed.generation == 23);
	slot[24] ^= 1;
	assert(!observe_member(slot, 3, 2, &observed));
	slot[24] ^= 1;
	assert(!observe_member(slot, 2, 2, &observed));
	assert(!observe_member(slot, 3, 1, &observed));
	put32(slot, 0);
	seal(slot);
	assert(!observe_member(slot, 3, 2, &observed));
	put32(slot, UINT32_C(0x51564f54));
	put32(slot + 4, 2);
	seal(slot);
	assert(!observe_member(slot, 3, 2, &observed));
	puts("native member observer cases PASS");
	return 0;
}
