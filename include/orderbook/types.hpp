#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <variant>

namespace ob {

// ── Status codes ──────────────────────────────────────────────────────────────
// ob_status_t: int32 error code. 0 = success, negative = error.
using ob_status_t = int32_t;

inline constexpr ob_status_t OB_OK              =  0;
inline constexpr ob_status_t OB_ERR_INVALID_ARG = -1;
inline constexpr ob_status_t OB_ERR_NOT_FOUND   = -2;
inline constexpr ob_status_t OB_ERR_PARSE       = -3;
inline constexpr ob_status_t OB_ERR_IO          = -4;
inline constexpr ob_status_t OB_ERR_MMAP_FAILED = -5;
inline constexpr ob_status_t OB_ERR_OVERFLOW    = -6;
inline constexpr ob_status_t OB_ERR_CHECKSUM    = -7;
inline constexpr ob_status_t OB_ERR_FULL        = -8;
inline constexpr ob_status_t OB_ERR_MIGRATED    = -9;
inline constexpr ob_status_t OB_ERR_INTERNAL    = -99;

// ── Exception ─────────────────────────────────────────────────────────────────
// ob::Exception is thrown on fatal, unrecoverable errors.
class Exception : public std::runtime_error {
public:
    explicit Exception(const std::string& msg, ob_status_t code = OB_ERR_INTERNAL)
        : std::runtime_error(msg), code_(code) {}

    explicit Exception(const char* msg, ob_status_t code = OB_ERR_INTERNAL)
        : std::runtime_error(msg), code_(code) {}

    ob_status_t code() const noexcept { return code_; }

private:
    ob_status_t code_;
};

// ── Result<T> ─────────────────────────────────────────────────────────────────
// ob::Result<T> is a std::expected-like type for recoverable errors.
// Holds either a value of type T or an ob_status_t error code + message.
template <typename T>
class Result {
public:
    // Construct a successful result
    static Result ok(T value) {
        Result r;
        r.storage_ = std::move(value);
        return r;
    }

    // Construct an error result
    static Result err(ob_status_t code, std::string msg = {}) {
        Result r;
        r.storage_ = Error{code, std::move(msg)};
        return r;
    }

    bool has_value() const noexcept {
        return std::holds_alternative<T>(storage_);
    }

    explicit operator bool() const noexcept { return has_value(); }

    T& value() & {
        if (!has_value()) throw Exception("Result has no value", error_code());
        return std::get<T>(storage_);
    }

    const T& value() const& {
        if (!has_value()) throw Exception("Result has no value", error_code());
        return std::get<T>(storage_);
    }

    T&& value() && {
        if (!has_value()) throw Exception("Result has no value", error_code());
        return std::get<T>(std::move(storage_));
    }

    ob_status_t error_code() const noexcept {
        if (has_value()) return OB_OK;
        return std::get<Error>(storage_).code;
    }

    const std::string& error_message() const noexcept {
        static const std::string empty;
        if (has_value()) return empty;
        return std::get<Error>(storage_).message;
    }

private:
    struct Error {
        ob_status_t code;
        std::string message;
    };

    std::variant<T, Error> storage_;

    Result() = default;
};

// Specialisation for void (success/failure only)
template <>
class Result<void> {
public:
    static Result ok() {
        Result r;
        r.code_ = OB_OK;
        return r;
    }

    static Result err(ob_status_t code, std::string msg = {}) {
        Result r;
        r.code_ = code;
        r.message_ = std::move(msg);
        return r;
    }

    bool has_value() const noexcept { return code_ == OB_OK; }
    explicit operator bool() const noexcept { return has_value(); }

    ob_status_t error_code() const noexcept { return code_; }
    const std::string& error_message() const noexcept { return message_; }

private:
    ob_status_t code_ = OB_OK;
    std::string message_;

    Result() = default;
};

} // namespace ob
