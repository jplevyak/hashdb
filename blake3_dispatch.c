#include "blake3_impl.h"
#include <string.h>

void blake3_hash_many(const uint8_t *const *inputs, size_t num_inputs,
                      size_t blocks, const uint32_t key[8],
                      uint64_t counter, bool increment_counter,
                      uint8_t flags, uint8_t flags_start,
                      uint8_t flags_end, uint8_t *out) {
  blake3_hash_many_portable(inputs, num_inputs, blocks, key, counter,
                            increment_counter, flags, flags_start,
                            flags_end, out);
}

void blake3_compress_in_place(uint32_t cv[8],
                              const uint8_t block[BLAKE3_BLOCK_LEN],
                              uint8_t block_len, uint64_t counter,
                              uint8_t flags) {
  blake3_compress_in_place_portable(cv, block, block_len, counter, flags);
}

void blake3_compress_xof(const uint32_t cv[8],
                         const uint8_t block[BLAKE3_BLOCK_LEN],
                         uint8_t block_len, uint64_t counter, uint8_t flags,
                         uint8_t out[64]) {
  blake3_compress_xof_portable(cv, block, block_len, counter, flags, out);
}

void blake3_xof_many(const uint32_t cv[8],
                     const uint8_t block[BLAKE3_BLOCK_LEN],
                     uint8_t block_len, uint64_t counter, uint8_t flags,
                     uint8_t out[64], size_t outblocks) {
  // Simple implementation using single blake3_compress_xof calls
  // Does portable provide xof_many? 
  // blake3_portable.c does NOT expose xof_many.
  // We can just loop.
  // Wait, blake3_xof_many is expected to produce 'outblocks' of 64 bytes?
  // No, `blake3_xof_many` signature in header:
  // void blake3_xof_many(..., uint8_t out[64], size_t outblocks);
  // Wait, that's not right. The signature in impl.h (Step 552) is:
  // void blake3_xof_many(const uint32_t cv[8], const uint8_t block[...], ... uint8_t out[64], size_t outblocks);
  // Actually, wait. `blake3_xof_many` seems to produce multiple blocks of output for XOF?
  // Or is it multiple inputs?
  // `blake3_compress_xof` produces 64 bytes.
  
  // Checking `blake3.c` usage (output_root_bytes):
  // blake3_xof_many(self->input_cv, self->block, self->block_len, output_block_counter, self->flags | ROOT, out, out_len / 64);
  // So it produces `out_len / 64` blocks of 64 bytes.
  // We need to implement this loop.
  
  for (size_t i = 0; i < outblocks; i++) {
      blake3_compress_xof_portable(cv, block, block_len, counter + i, flags, &out[i * 64]);
  }
}

size_t blake3_simd_degree(void) {
  return 1;
}
