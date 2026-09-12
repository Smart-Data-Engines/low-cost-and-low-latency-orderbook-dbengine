// Storage fault injection for the integration battery — roadmap #54.
//
// An LD_PRELOAD shim that makes a chosen write, fsync, fdatasync or ftruncate fail, on a chosen
// file, after a chosen number of successful calls. It exists because the failure modes that lose
// data in production are the ones a healthy machine never produces: ENOSPC mid-record, EIO from
// fsync, a short write that leaves a torn record behind.
//
// Three decisions in here are worth more than the code:
//
// **The path comes from /proc/self/fd, not from an fd table.** The obvious design intercepts
// open/open64/openat/creat and keeps fd -> path under a mutex. That means variadic mode arguments,
// a table sized for the process's fd limit, and a bootstrap window in which libc's own
// initialisation reaches an interposer whose real symbols are not resolved yet. Reading the link
// at decision time removes all three, and costs one readlink per *matching* call in a test.
//
// **Pass-through goes straight to the syscall, not through dlsym.** There is therefore no
// recursion risk and no resolution order to get right: the log itself is written with
// syscall(SYS_write), so it cannot re-enter the write interposer. The cost is that these wrappers
// are not cancellation points the way libc's are, which matters to a thread being cancelled and
// not to this engine, which cancels none.
//
// **An empty OB_FAULT_PATH disarms the shim completely.** A misconfigured injector that quietly
// matched everything would fail writes this test never meant to touch; one that matches nothing
// looks exactly like code that survives faults. So the path is required, and the log is what a
// test asserts against to know the injection actually happened.

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

// ── Configuration, read once at load ──────────────────────────────────────────

enum fault_op {
    OP_NONE = 0,
    OP_WRITE,
    OP_FSYNC,
    OP_FDATASYNC,
    OP_FTRUNCATE,
};

static struct {
    const char    *path;        // substring the file's path must contain; NULL disarms everything
    enum fault_op  op;
    int            err;         // errno to report
    unsigned long  skip;        // let this many matching calls succeed first
    unsigned long  count;       // fail this many, then let the rest through
    long           short_bytes; // write: return this many bytes instead of failing
    long           size;        // only calls whose byte count equals this; -1 means any
    int            log_fd;
} cfg = { NULL, OP_NONE, EIO, 0, ~0UL, -1, -1, -1 };

static atomic_ulong seen   = 0;   // matching calls for the configured op
static atomic_ulong failed = 0;   // how many of those were made to fail

static enum fault_op parse_op(const char *s) {
    if (!s) return OP_NONE;
    if (strcmp(s, "write") == 0)      return OP_WRITE;
    if (strcmp(s, "fsync") == 0)      return OP_FSYNC;
    if (strcmp(s, "fdatasync") == 0)  return OP_FDATASYNC;
    if (strcmp(s, "ftruncate") == 0)  return OP_FTRUNCATE;
    return OP_NONE;
}

static int parse_errno(const char *s) {
    if (!s) return EIO;
    if (strcmp(s, "ENOSPC") == 0) return ENOSPC;
    if (strcmp(s, "EIO") == 0)    return EIO;
    if (strcmp(s, "EDQUOT") == 0) return EDQUOT;
    return EIO;
}

__attribute__((constructor)) static void obfault_init(void) {
    const char *path = getenv("OB_FAULT_PATH");
    if (!path || !*path) return;  // disarmed on purpose: see the header comment

    cfg.path = path;
    cfg.op   = parse_op(getenv("OB_FAULT_OP"));
    cfg.err  = parse_errno(getenv("OB_FAULT_ERRNO"));

    const char *skip  = getenv("OB_FAULT_SKIP");
    const char *count = getenv("OB_FAULT_COUNT");
    const char *size = getenv("OB_FAULT_SIZE");
    if (size) cfg.size = strtol(size, NULL, 10);

    const char *shortw = getenv("OB_FAULT_SHORT");
    if (skip)   cfg.skip  = strtoul(skip, NULL, 10);
    if (count)  cfg.count = strtoul(count, NULL, 10);
    if (shortw) cfg.short_bytes = strtol(shortw, NULL, 10);

    const char *log = getenv("OB_FAULT_LOG");
    if (log && *log) {
        // Our own open() is not interposed, so this is the ordinary libc call.
        cfg.log_fd = open(log, O_WRONLY | O_CREAT | O_APPEND, 0644);
    }
}

// ── Decision ──────────────────────────────────────────────────────────────────

/// Written with syscall(SYS_write) so the log cannot re-enter the write interposer.
static void note(const char *op, int fd, const char *path, long arg,
                 unsigned long n, const char *action) {
    if (cfg.log_fd < 0) return;
    char line[1024];
    const int len = snprintf(line, sizeof line,
                             "obfault op=%s fd=%d path=%s arg=%ld seen=%lu action=%s errno=%d\n",
                             op, fd, path, arg, n, action, cfg.err);
    if (len > 0) {
        syscall(SYS_write, cfg.log_fd, line, (size_t)len);
    }
}

/// Does this descriptor point at the file the test named?
static int fd_matches(int fd, char *out, size_t out_len) {
    char link[64];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    const ssize_t n = readlink(link, out, out_len - 1);
    if (n < 0) return 0;
    out[n] = '\0';
    return strstr(out, cfg.path) != NULL;
}

/// Returns 1 when this call has been chosen to fail, and records the decision either way.
static int should_fail(enum fault_op op, const char *name, int fd, long arg) {
    if (cfg.op != op || !cfg.path) return 0;

    // A size filter makes the injection point *nameable*: this engine's WAL writes a 136-byte
    // delta record from the session thread and a 24-byte checkpoint plus a 68-byte version vector
    // from the flush loop, so "the fourth write" is a different call on every run while "the
    // 68-byte write" is the same one every time. Requirement 2.2 asks for a named point rather
    // than an ordinal, and this is what makes one available.
    if (cfg.size >= 0 && arg != cfg.size) return 0;

    char path[4096];
    if (!fd_matches(fd, path, sizeof path)) return 0;

    const unsigned long n = atomic_fetch_add(&seen, 1);
    if (n < cfg.skip) {
        note(name, fd, path, arg, n, "pass-skip");
        return 0;
    }
    if (atomic_load(&failed) >= cfg.count) {
        note(name, fd, path, arg, n, "pass-spent");
        return 0;
    }
    atomic_fetch_add(&failed, 1);
    note(name, fd, path, arg, n, cfg.short_bytes >= 0 && op == OP_WRITE ? "short" : "fail");
    return 1;
}

// ── Interposed calls ──────────────────────────────────────────────────────────

ssize_t write(int fd, const void *buf, size_t count) {
    if (should_fail(OP_WRITE, "write", fd, (long)count)) {
        if (cfg.short_bytes >= 0) {
            // A genuine short write: the bytes it claims really do reach the file, which is what
            // leaves a torn record behind. Returning a count without writing would model nothing.
            const size_t n = (size_t)cfg.short_bytes < count ? (size_t)cfg.short_bytes : count;
            return (ssize_t)syscall(SYS_write, fd, buf, n);
        }
        errno = cfg.err;
        return -1;
    }
    return (ssize_t)syscall(SYS_write, fd, buf, count);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    if (should_fail(OP_WRITE, "pwrite", fd, (long)count)) {
        if (cfg.short_bytes >= 0) {
            const size_t n = (size_t)cfg.short_bytes < count ? (size_t)cfg.short_bytes : count;
            return (ssize_t)syscall(SYS_pwrite64, fd, buf, n, offset);
        }
        errno = cfg.err;
        return -1;
    }
    return (ssize_t)syscall(SYS_pwrite64, fd, buf, count, offset);
}

int fsync(int fd) {
    if (should_fail(OP_FSYNC, "fsync", fd, 0)) {
        errno = cfg.err;
        return -1;
    }
    return (int)syscall(SYS_fsync, fd);
}

int fdatasync(int fd) {
    if (should_fail(OP_FDATASYNC, "fdatasync", fd, 0)) {
        errno = cfg.err;
        return -1;
    }
    return (int)syscall(SYS_fdatasync, fd);
}

int ftruncate(int fd, off_t length) {
    if (should_fail(OP_FTRUNCATE, "ftruncate", fd, (long)length)) {
        errno = cfg.err;
        return -1;
    }
    return (int)syscall(SYS_ftruncate, fd, length);
}
