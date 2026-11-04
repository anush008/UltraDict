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

#endif

#endif /* ULTRADICT_FAST_ATOMIC_H */
