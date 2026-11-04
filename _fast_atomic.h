/*
 * UltraDict
 *
 * Portable atomic operations for the `_fast` extension module.
 *
 * Licensed under the Apache License, Version 2.0
 */

#ifndef ULTRADICT_FAST_ATOMIC_H
#define ULTRADICT_FAST_ATOMIC_H

#include <stdint.h>

#if defined(_MSC_VER) && !defined(__clang__)

#include <intrin.h>

#if defined(_M_ARM64) || defined(_M_ARM)
#define UD_HW_BARRIER() __dmb(_ARM64_BARRIER_ISH)
#else
/* x86 and x64 keep the order of loads and of stores, a compiler barrier is enough */
#define UD_HW_BARRIER() _ReadWriteBarrier()
#endif

static __inline unsigned char ud_exchange_u8(unsigned char *p, unsigned char v) {
    return (unsigned char)_InterlockedExchange8((volatile char *)p, (char)v);
}

static __inline uint32_t ud_load_acquire_u32(uint32_t *p) {
    uint32_t v = *(volatile uint32_t *)p;
    UD_HW_BARRIER();
    return v;
}

static __inline void ud_store_release_u32(uint32_t *p, uint32_t v) {
    UD_HW_BARRIER();
    *(volatile uint32_t *)p = v;
}

static __inline void ud_fence_acquire(void) { UD_HW_BARRIER(); }
static __inline void ud_fence_release(void) { UD_HW_BARRIER(); }

static __inline void ud_fence_full(void) {
#if defined(_M_ARM64) || defined(_M_ARM)
    __dmb(_ARM64_BARRIER_ISH);
#else
    /* A locked operation is a full barrier on x86 and x64 */
    volatile long dummy = 0;
    _InterlockedExchange(&dummy, 0);
#endif
}

/* Compare the 4 bytes at p with expected. If they are equal, write desired and return 1.
   Otherwise return 0. The values are raw memory, so byte order does not matter. */
static __inline int ud_cmpxchg_u32(uint32_t *p, uint32_t expected, uint32_t desired) {
    return _InterlockedCompareExchange((volatile long *)p, (long)desired, (long)expected) == (long)expected;
}

#else

static inline unsigned char ud_exchange_u8(unsigned char *p, unsigned char v) {
    return __atomic_exchange_n(p, v, __ATOMIC_SEQ_CST);
}

static inline uint32_t ud_load_acquire_u32(uint32_t *p) {
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static inline void ud_store_release_u32(uint32_t *p, uint32_t v) {
    __atomic_store_n(p, v, __ATOMIC_RELEASE);
}

static inline void ud_fence_acquire(void) { __atomic_thread_fence(__ATOMIC_ACQUIRE); }
static inline void ud_fence_release(void) { __atomic_thread_fence(__ATOMIC_RELEASE); }
static inline void ud_fence_full(void) { __atomic_thread_fence(__ATOMIC_SEQ_CST); }

/* Compare the 4 bytes at p with expected. If they are equal, write desired and return 1.
   Otherwise return 0. The values are raw memory, so byte order does not matter. */
static inline int ud_cmpxchg_u32(uint32_t *p, uint32_t expected, uint32_t desired) {
    return __atomic_compare_exchange_n(p, &expected, desired, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

#endif

/*
 * CRC32 of stream records, the same as zlib.crc32(data, seed) in Python.
 * Call ud_crc32_init() once before the first ud_crc32().
 *
 * Slicing by 8: table k gives the CRC of a byte followed by k zero bytes, so each step of
 * the main loop handles 8 bytes. A table for one byte at a time is several times slower,
 * because each step must wait for the result of the step before.
 */
static uint32_t ud_crc32_table[8][256];

static void ud_crc32_init(void) {
    uint32_t i, k, c;
    for (i = 0; i < 256; i++) {
        c = i;
        for (k = 0; k < 8; k++) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        ud_crc32_table[0][i] = c;
    }
    for (i = 0; i < 256; i++) {
        c = ud_crc32_table[0][i];
        for (k = 1; k < 8; k++) {
            c = ud_crc32_table[0][c & 0xFF] ^ (c >> 8);
            ud_crc32_table[k][i] = c;
        }
    }
}

static uint32_t ud_crc32(uint32_t seed, const unsigned char *p, size_t n) {
    uint32_t crc = ~seed;
    uint32_t low, high;
    while (n >= 8) {
        /* Read the bytes one by one, so the result does not depend on alignment or byte order */
        low = crc ^ ((uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24));
        high = (uint32_t)p[4] | ((uint32_t)p[5] << 8) | ((uint32_t)p[6] << 16) | ((uint32_t)p[7] << 24);
        crc = ud_crc32_table[7][low & 0xFF] ^ ud_crc32_table[6][(low >> 8) & 0xFF]
            ^ ud_crc32_table[5][(low >> 16) & 0xFF] ^ ud_crc32_table[4][low >> 24]
            ^ ud_crc32_table[3][high & 0xFF] ^ ud_crc32_table[2][(high >> 8) & 0xFF]
            ^ ud_crc32_table[1][(high >> 16) & 0xFF] ^ ud_crc32_table[0][high >> 24];
        p += 8;
        n -= 8;
    }
    while (n--) {
        crc = ud_crc32_table[0][(crc ^ *p++) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

#endif /* ULTRADICT_FAST_ATOMIC_H */
