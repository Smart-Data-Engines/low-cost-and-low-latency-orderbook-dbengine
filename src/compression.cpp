// LZ4 frame compression/decompression implementation.
// Uses the LZ4F (frame) API for streaming with built-in content checksums.

#include "orderbook/compression.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>

#include <lz4frame.h>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

static void check_lz4f(size_t code, const char* context) {
    if (LZ4F_isError(code)) {
        throw std::runtime_error(
            std::string(context) + ": " + LZ4F_getErrorName(code));
    }
}

// ── Stateless compress / decompress ──────────────────────────────────────────

std::vector<uint8_t> lz4_compress(const void* data, size_t len) {
    LZ4F_preferences_t prefs{};
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    prefs.frameInfo.contentSize = len;

    const size_t bound = LZ4F_compressFrameBound(len, &prefs);
    std::vector<uint8_t> out(bound);

    const size_t written = LZ4F_compressFrame(
        out.data(), out.size(), data, len, &prefs);
    check_lz4f(written, "lz4_compress");

    out.resize(written);
    return out;
}

std::vector<uint8_t> lz4_decompress(const void* data, size_t len) {
    LZ4F_dctx* dctx = nullptr;
    size_t status = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    check_lz4f(status, "lz4_decompress: create context");

    std::vector<uint8_t> out;
    // Start with a reasonable buffer; grow as needed.
    size_t buf_cap = len * 4;
    if (buf_cap < 256) buf_cap = 256;
    out.resize(buf_cap);

    const auto* src = static_cast<const uint8_t*>(data);
    size_t src_remaining = len;
    size_t dst_offset = 0;

    while (src_remaining > 0) {
        size_t src_size = src_remaining;
        size_t dst_size = out.size() - dst_offset;

        if (dst_size == 0) {
            out.resize(out.size() * 2);
            dst_size = out.size() - dst_offset;
        }

        status = LZ4F_decompress(
            dctx, out.data() + dst_offset, &dst_size,
            src, &src_size, nullptr);

        if (LZ4F_isError(status)) {
            LZ4F_freeDecompressionContext(dctx);
            throw std::runtime_error(
                std::string("lz4_decompress: ") + LZ4F_getErrorName(status));
        }

        src += src_size;
        src_remaining -= src_size;
        dst_offset += dst_size;

        // status == 0 means frame fully decoded
        if (status == 0) break;
    }

    LZ4F_freeDecompressionContext(dctx);
    out.resize(dst_offset);
    return out;
}

// ── LZ4CompressStream ────────────────────────────────────────────────────────

LZ4CompressStream::LZ4CompressStream() {
    LZ4F_cctx* cctx = nullptr;
    size_t status = LZ4F_createCompressionContext(&cctx, LZ4F_VERSION);
    check_lz4f(status, "LZ4CompressStream: create context");
    ctx_ = static_cast<void*>(cctx);
}

LZ4CompressStream::~LZ4CompressStream() {
    if (ctx_) {
        LZ4F_freeCompressionContext(static_cast<LZ4F_cctx*>(ctx_));
    }
}

std::vector<uint8_t> LZ4CompressStream::begin() {
    auto* cctx = static_cast<LZ4F_cctx*>(ctx_);
    LZ4F_preferences_t prefs{};
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;

    // LZ4F_HEADER_SIZE_MAX is the max header size
    std::vector<uint8_t> out(LZ4F_HEADER_SIZE_MAX);
    size_t written = LZ4F_compressBegin(cctx, out.data(), out.size(), &prefs);
    check_lz4f(written, "LZ4CompressStream::begin");
    out.resize(written);
    return out;
}

std::vector<uint8_t> LZ4CompressStream::update(const void* data, size_t len) {
    auto* cctx = static_cast<LZ4F_cctx*>(ctx_);
    const size_t bound = LZ4F_compressBound(len, nullptr);
    std::vector<uint8_t> out(bound);
    size_t written = LZ4F_compressUpdate(
        cctx, out.data(), out.size(), data, len, nullptr);
    check_lz4f(written, "LZ4CompressStream::update");
    out.resize(written);
    return out;
}

std::vector<uint8_t> LZ4CompressStream::end() {
    auto* cctx = static_cast<LZ4F_cctx*>(ctx_);
    // End marker + content checksum: small fixed size
    std::vector<uint8_t> out(LZ4F_compressBound(0, nullptr) + 16);
    size_t written = LZ4F_compressEnd(cctx, out.data(), out.size(), nullptr);
    check_lz4f(written, "LZ4CompressStream::end");
    out.resize(written);
    return out;
}

// ── LZ4DecompressStream ──────────────────────────────────────────────────────

LZ4DecompressStream::LZ4DecompressStream() {
    LZ4F_dctx* dctx = nullptr;
    size_t status = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    check_lz4f(status, "LZ4DecompressStream: create context");
    ctx_ = static_cast<void*>(dctx);
}

LZ4DecompressStream::~LZ4DecompressStream() {
    if (ctx_) {
        LZ4F_freeDecompressionContext(static_cast<LZ4F_dctx*>(ctx_));
    }
}

std::vector<uint8_t> LZ4DecompressStream::update(const void* data, size_t len) {
    auto* dctx = static_cast<LZ4F_dctx*>(ctx_);
    std::vector<uint8_t> out;
    size_t buf_cap = len * 4;
    if (buf_cap < 256) buf_cap = 256;
    out.resize(buf_cap);

    const auto* src = static_cast<const uint8_t*>(data);
    size_t src_remaining = len;
    size_t dst_offset = 0;

    while (src_remaining > 0) {
        size_t src_size = src_remaining;
        size_t dst_size = out.size() - dst_offset;

        if (dst_size == 0) {
            out.resize(out.size() * 2);
            dst_size = out.size() - dst_offset;
        }

        size_t status = LZ4F_decompress(
            dctx, out.data() + dst_offset, &dst_size,
            src, &src_size, nullptr);

        if (LZ4F_isError(status)) {
            throw std::runtime_error(
                std::string("LZ4DecompressStream::update: ") +
                LZ4F_getErrorName(status));
        }

        src += src_size;
        src_remaining -= src_size;
        dst_offset += dst_size;

        // status == 0 means frame fully decoded; stop consuming
        if (status == 0) break;
    }

    out.resize(dst_offset);
    return out;
}

void LZ4DecompressStream::reset() {
    auto* dctx = static_cast<LZ4F_dctx*>(ctx_);
    LZ4F_resetDecompressionContext(dctx);
}

} // namespace ob
