#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include "orderbook/types.hpp"
#include "orderbook/data_model.hpp"

// ── ob_status_t constants ─────────────────────────────────────────────────────

TEST(StatusCodes, SuccessIsZero) {
    EXPECT_EQ(ob::OB_OK, 0);
}

TEST(StatusCodes, ErrorsAreNegative) {
    EXPECT_LT(ob::OB_ERR_INVALID_ARG, 0);
    EXPECT_LT(ob::OB_ERR_NOT_FOUND,   0);
    EXPECT_LT(ob::OB_ERR_PARSE,       0);
    EXPECT_LT(ob::OB_ERR_IO,          0);
    EXPECT_LT(ob::OB_ERR_MMAP_FAILED, 0);
    EXPECT_LT(ob::OB_ERR_OVERFLOW,    0);
    EXPECT_LT(ob::OB_ERR_CHECKSUM,    0);
    EXPECT_LT(ob::OB_ERR_FULL,        0);
    EXPECT_LT(ob::OB_ERR_INTERNAL,    0);
}

// ── ob::Exception ─────────────────────────────────────────────────────────────

TEST(Exception, DefaultCodeIsInternal) {
    ob::Exception ex("boom");
    EXPECT_EQ(ex.code(), ob::OB_ERR_INTERNAL);
    EXPECT_STREQ(ex.what(), "boom");
}

TEST(Exception, CustomCode) {
    ob::Exception ex("not found", ob::OB_ERR_NOT_FOUND);
    EXPECT_EQ(ex.code(), ob::OB_ERR_NOT_FOUND);
}

TEST(Exception, IsStdException) {
    EXPECT_NO_THROW({
        try {
            throw ob::Exception("test");
        } catch (const std::exception& e) {
            EXPECT_STREQ(e.what(), "test");
        }
    });
}

// ── ob::Result<T> ─────────────────────────────────────────────────────────────

TEST(Result, OkHoldsValue) {
    auto r = ob::Result<int>::ok(42);
    EXPECT_TRUE(r.has_value());
    EXPECT_TRUE(static_cast<bool>(r));
    EXPECT_EQ(r.value(), 42);
    EXPECT_EQ(r.error_code(), ob::OB_OK);
}

TEST(Result, ErrHoldsCode) {
    auto r = ob::Result<int>::err(ob::OB_ERR_NOT_FOUND, "missing");
    EXPECT_FALSE(r.has_value());
    EXPECT_FALSE(static_cast<bool>(r));
    EXPECT_EQ(r.error_code(), ob::OB_ERR_NOT_FOUND);
    EXPECT_EQ(r.error_message(), "missing");
}

TEST(Result, ValueThrowsOnError) {
    auto r = ob::Result<int>::err(ob::OB_ERR_IO);
    EXPECT_THROW(r.value(), ob::Exception);
}

TEST(ResultVoid, OkHasValue) {
    auto r = ob::Result<void>::ok();
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r.error_code(), ob::OB_OK);
}

TEST(ResultVoid, ErrHasCode) {
    auto r = ob::Result<void>::err(ob::OB_ERR_PARSE, "bad sql");
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error_code(), ob::OB_ERR_PARSE);
    EXPECT_EQ(r.error_message(), "bad sql");
}

// ── DeltaUpdate layout ────────────────────────────────────────────────────────

TEST(DeltaUpdate, FieldSizes) {
    ob::DeltaUpdate du{};
    EXPECT_EQ(sizeof(du.symbol),          32u);
    EXPECT_EQ(sizeof(du.exchange),        32u);
    EXPECT_EQ(sizeof(du.sequence_number), 8u);
    EXPECT_EQ(sizeof(du.timestamp_ns),    8u);
    EXPECT_EQ(sizeof(du.side),            1u);
    EXPECT_EQ(sizeof(du.n_levels),        2u);
}

TEST(DeltaUpdate, SideConstants) {
    EXPECT_EQ(ob::SIDE_BID, 0u);
    EXPECT_EQ(ob::SIDE_ASK, 1u);
}

TEST(DeltaUpdate, MaxLevels) {
    EXPECT_EQ(ob::MAX_LEVELS, 1000u);
}

// ── SnapshotRow layout ────────────────────────────────────────────────────────

TEST(SnapshotRow, Size) {
    EXPECT_EQ(sizeof(ob::SnapshotRow), 48u);
}

TEST(SnapshotRow, FieldAssignment) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = 1'700'000'000'000'000'000ULL;
    row.sequence_number = 12345ULL;
    row.side            = ob::SIDE_BID;
    row.level_index     = 0;
    row.price           = 15000LL;
    row.quantity        = 500ULL;
    row.order_count     = 3;

    EXPECT_EQ(row.timestamp_ns,    1'700'000'000'000'000'000ULL);
    EXPECT_EQ(row.sequence_number, 12345ULL);
    EXPECT_EQ(row.side,            ob::SIDE_BID);
    EXPECT_EQ(row.level_index,     0u);
    EXPECT_EQ(row.price,           15000LL);
    EXPECT_EQ(row.quantity,        500ULL);
    EXPECT_EQ(row.order_count,     3u);
}

// ── Level struct ──────────────────────────────────────────────────────────────

TEST(Level, FieldTypes) {
    ob::Level lv{};
    lv.price = -1LL;          // signed int64
    lv.qty   = 1ULL;          // unsigned uint64
    lv.cnt   = 1U;            // unsigned uint32
    EXPECT_EQ(lv.price, -1LL);
    EXPECT_EQ(lv.qty,    1ULL);
    EXPECT_EQ(lv.cnt,    1U);
}

// ── Property-based: Result round-trip ─────────────────────────────────────────

RC_GTEST_PROP(ResultProperty, OkValueRoundTrip, (int32_t v)) {
    auto r = ob::Result<int32_t>::ok(v);
    RC_ASSERT(r.has_value());
    RC_ASSERT(r.value() == v);
    RC_ASSERT(r.error_code() == ob::OB_OK);
}

RC_GTEST_PROP(ResultProperty, ErrCodeRoundTrip, (int32_t code)) {
    // Only test negative codes (error range)
    ob::ob_status_t c = (code <= 0) ? code : -code;
    if (c == 0) { c = -1; }
    auto r = ob::Result<int32_t>::err(c, "msg");
    RC_ASSERT(!r.has_value());
    RC_ASSERT(r.error_code() == c);
}
