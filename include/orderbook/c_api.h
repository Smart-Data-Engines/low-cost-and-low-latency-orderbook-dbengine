#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ── Status codes ──────────────────────────────────────────────────────────────
// These match the ob_status_t values defined in types.hpp.
// Defined as enum to avoid macro/constexpr conflicts when included from C++.
#ifdef __cplusplus
enum : int {
    OB_C_OK              =  0,
    OB_C_ERR_INVALID_ARG = -1,
    OB_C_ERR_NOT_FOUND   = -2,
    OB_C_ERR_PARSE       = -3,
    OB_C_ERR_IO          = -4,
    OB_C_ERR_MMAP_FAILED = -5,
    OB_C_ERR_INTERNAL    = -99
};
#else
#define OB_C_OK              0
#define OB_C_ERR_INVALID_ARG (-1)
#define OB_C_ERR_NOT_FOUND   (-2)
#define OB_C_ERR_PARSE       (-3)
#define OB_C_ERR_IO          (-4)
#define OB_C_ERR_MMAP_FAILED (-5)
#define OB_C_ERR_INTERNAL    (-99)
#endif

typedef int ob_status_t;

// ── Opaque types ──────────────────────────────────────────────────────────────
typedef struct ob_engine ob_engine_t;
typedef struct ob_result ob_result_t;

// ── Engine lifecycle ──────────────────────────────────────────────────────────
ob_engine_t* ob_engine_create(const char* base_dir);
void         ob_engine_destroy(ob_engine_t* engine);

// ── Delta ingestion ───────────────────────────────────────────────────────────
ob_status_t ob_apply_delta(ob_engine_t*    engine,
                            const char*     symbol,
                            const char*     exchange,
                            uint64_t        seq,
                            uint64_t        ts_ns,
                            const int64_t*  prices,
                            const uint64_t* qtys,
                            const uint32_t* cnts,
                            uint32_t        n_levels,
                            int             side);  /* 0=bid, 1=ask */

// ── Query ─────────────────────────────────────────────────────────────────────
ob_result_t* ob_query(ob_engine_t* engine, const char* sql);
void         ob_result_free(ob_result_t* result);

/* Advance to next row; returns OB_OK while rows remain, OB_ERR_NOT_FOUND when
   exhausted.  Out parameters are filled on OB_OK; any may be NULL. */
ob_status_t ob_result_next(ob_result_t* result,
                            uint64_t*    out_timestamp_ns,
                            int64_t*     out_price,
                            uint64_t*    out_quantity,
                            uint32_t*    out_order_count,
                            uint8_t*     out_side,
                            uint16_t*    out_level);

/* Same cursor, one more field: the per-origin sequence number of the update that produced the row.
   0 means unknown — a row stored before sequence numbers existed has none.

   This is a separate function rather than an extra parameter on ob_result_next() because that one
   is a C entry point somebody may already have compiled against. Both advance the same cursor; call
   one or the other, not both. */
ob_status_t ob_result_next_seq(ob_result_t* result,
                            uint64_t*    out_timestamp_ns,
                            int64_t*     out_price,
                            uint64_t*    out_quantity,
                            uint32_t*    out_order_count,
                            uint8_t*     out_side,
                            uint16_t*    out_level,
                            uint64_t*    out_sequence_number);

// ── Subscriptions ─────────────────────────────────────────────────────────────
uint64_t ob_subscribe(ob_engine_t* engine,
                      const char*  sql,
                      void (*callback)(const char* json_row, void* userdata),
                      void*        userdata);
void ob_unsubscribe(ob_engine_t* engine, uint64_t sub_id);

#ifdef __cplusplus
}
#endif
