#!/usr/bin/env bash
# Verify a built package without installing it.
#
# `dpkg -i` is deliberately not used: installing into the build host is a change to the system, not
# a test, and it would need root. Extraction proves the layout, and running the extracted binary
# against the extracted configuration proves the thing that actually matters — that the default
# configuration a fresh install gets is one the server accepts. A package whose shipped
# configuration does not start is worse than no package.
set -euo pipefail

BUILD_DIR="${1:-build-pkg}"
REPO=$(cd "$(dirname "$0")/.." && pwd)
# The names carry the architecture since #42: orderbook-dbengine_X.Y.Z_<arch>.deb and
# orderbook-dbengine-X.Y.Z-Linux-<arch>.tar.gz.
DEB=$(ls "$BUILD_DIR"/orderbook-dbengine_*.deb 2>/dev/null | head -1)
TGZ=$(ls "$BUILD_DIR"/orderbook-dbengine-*-Linux-*.tar.gz 2>/dev/null | head -1)

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "  ok: $*"; }

[ -n "$DEB" ] || fail "no .deb in $BUILD_DIR"
[ -n "$TGZ" ] || fail "no tarball in $BUILD_DIR"
echo "package: $DEB"

# ── Layout ────────────────────────────────────────────────────────────────────
CONTENTS=$(dpkg-deb -c "$DEB" | awk '{print $6}')
for path in ./usr/bin/ob_tcp_server \
            ./usr/bin/ob_restore \
            ./usr/bin/ob_backup \
            ./etc/orderbook/ob.conf \
            ./usr/lib/systemd/system/ob_tcp_server.service \
            ./usr/share/man/man1/ob_tcp_server.1 \
            ./usr/include/orderbook/engine.hpp \
            ./usr/lib/orderbook-dbengine/liborderbook_shared.so; do
    echo "$CONTENTS" | grep -qx -- "$path" || fail "missing from the package: $path"
done
ok "server, backup tools, config, unit, man page, headers and the C API library are all in the package"

# The config must be at /etc, not /usr/etc. `packaging/debian/conffiles` names /etc/orderbook/ob.conf,
# and a conffile declaration pointing at a path the package does not contain marks nothing — the
# first upgrade then silently reverts every local edit.
echo "$CONTENTS" | grep -q "^./usr/etc/" && fail "configuration installed under /usr/etc; the conffile mark would name a path the package does not contain"
ok "no /usr/etc — the conffile declaration names a path that exists"

# The fault injector must not be here (#54). It is an LD_PRELOAD shim built with the tests, whose
# whole purpose is to make writes and fsyncs fail on demand, so shipping it in a release artefact
# would put a loaded gun on an operator's disk. CPack takes every install rule by default, so the
# absence is asserted rather than assumed - `obfault` has no install rule today, and this line is
# what notices the day somebody adds one.
echo "$CONTENTS" | grep -q "obfault" && fail "the fault injector leaked into the package"
ok "no fault injector"

# No Python package here: the client is on PyPI, and a copy in the system package would shadow the
# version an operator installs with pip.
echo "$CONTENTS" | grep -q "orderbook_engine/" && fail "the Python client's package leaked into the system package"
ok "no Python package"

# ── Metadata ──────────────────────────────────────────────────────────────────
META=$(dpkg-deb -I "$DEB")
echo "$META" | grep -q "^ Section: database" || fail "Section is not database"
echo "$META" | grep -q "Depends:.*libc6" || fail "no shared-library dependencies; shlibdeps did not run"
ok "metadata: section, and dependencies resolved by shlibdeps rather than hand-listed"

dpkg-deb -I "$DEB" conffiles | grep -qx "/etc/orderbook/ob.conf" \
    || fail "ob.conf is not marked as a conffile; a package upgrade would overwrite operator edits"
ok "ob.conf is marked as a conffile"

# ── The layouts must agree ────────────────────────────────────────────────────
DEB_PATHS=$(dpkg-deb -c "$DEB" | awk '{print $6}' | sed 's|^\./||' | grep -v '/$' | sort)
TGZ_PATHS=$(tar tzf "$TGZ" | sed 's|^[^/]*/||' | grep -v '/$' | sort)
if [ "$DEB_PATHS" != "$TGZ_PATHS" ]; then
    echo "FAIL: the .deb and the tarball disagree about their layout:" >&2
    diff <(echo "$DEB_PATHS") <(echo "$TGZ_PATHS") >&2 || true
    exit 1
fi
ok "the .deb and the tarball contain the same paths"

# ── Owners and modes ──────────────────────────────────────────────────────────
# An archive records each entry's owner and mode, and tar run as root restores both - onto the
# directories it lands in as well, since GNU tar overwrites an existing directory's metadata by
# default. The tarball carried its builder's uid and umask, so extracted over / as
# docs/operations.md said, it handed /etc, /usr and /usr/bin to that uid, group-writable (#210). Every
# entry of both packages is root's, and nothing but a symlink is writable by its group or by others.
not_roots() {   # <listing> <owner field as the listing prints root's>
    echo "$1" | awk -v root="$2" '$2 != root || ($1 !~ /^l/ && (substr($1, 6, 1) == "w" || substr($1, 9, 1) == "w"))'
}
BAD=$(not_roots "$(tar --numeric-owner -tvzf "$TGZ")" "0/0")
[ -z "$BAD" ] || fail "tarball entries that are not root's, or are writable beyond their owner:
$(echo "$BAD" | head -5)"
BAD=$(not_roots "$(dpkg-deb -c "$DEB")" "root/root")
[ -z "$BAD" ] || fail ".deb entries that are not root's, or are writable beyond their owner:
$(echo "$BAD" | head -5)"
ok "every entry of the .deb and the tarball is root's, and none is writable beyond its owner"

# ── The shipped configuration has to start ────────────────────────────────────
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
dpkg-deb -x "$DEB" "$WORK"

"$WORK/usr/bin/ob_tcp_server" --config "$WORK/etc/orderbook/ob.conf" --print-config > "$WORK/resolved.txt" \
    || fail "the packaged binary refused the packaged configuration"
grep -q "data-dir .*/var/lib/orderbook .*(file)" "$WORK/resolved.txt" \
    || fail "the shipped configuration did not take effect: data-dir is not from the file"
ok "the packaged binary accepts the packaged configuration, and the file's values reach it"

# The backup tools (#34) run from the package: a binary missing a shared library it links would be
# found by the operator restoring a node, which is the worst moment to find it.
"$WORK/usr/bin/ob_restore" --help > /dev/null || fail "the packaged ob_restore does not run"
"$WORK/usr/bin/ob_backup" --help > /dev/null || fail "the packaged ob_backup does not run"
ok "the packaged ob_restore and ob_backup run"

# The C API library from the package, under the Python client's local mode: a write, a flush and a
# query against a data directory of its own (#42). A library missing a symbol or a shared library it
# links is found here rather than by a user of local mode.
OB_LIB_PATH="$WORK/usr/lib/orderbook-dbengine/liborderbook_shared.so" PYTHONPATH="$REPO/python" \
    python3 - "$WORK/local-data" <<'PY' || fail "the packaged C API library does not serve the client's local mode"
import sys
from orderbook_engine import OrderbookEngine
engine = OrderbookEngine(data_dir=sys.argv[1])
engine.insert("PKG", "TEST", "bid", prices=[100, 99], qtys=[5, 6], counts=[1, 1])
engine.flush()
rows = engine.query_all("PKG", "TEST")
engine.close()
assert len(rows) == 2, rows
PY
ok "the packaged C API library serves the client's local mode: a write, a flush, a query"

# ── The unit ──────────────────────────────────────────────────────────────────
# systemd-analyze reports the ExecStart binary as missing unless the package is installed, which it
# is not. It also loads the units ours sits beside and reports what it finds in their files: systemd
# 259 on Ubuntu 26.04 says CPUAccounting= was removed - of xfs_scrub_all.service, a distribution's
# unit - and this check failed on that (#209). What it says of this unit is a complaint about it;
# what it says of the distribution's is not.
UNIT="$WORK/usr/lib/systemd/system/ob_tcp_server.service"
VERIFY=$(systemd-analyze verify "$UNIT" 2>&1 | grep -F "ob_tcp_server.service" \
             | grep -v "is not executable: No such file or directory" || true)
[ -z "$VERIFY" ] || fail "systemd-analyze objected to the unit:
$VERIFY"
ok "systemd-analyze verify is clean apart from the not-yet-installed binary"

grep -q "^ExecStart=/usr/bin/ob_tcp_server --config /etc/orderbook/ob.conf$" "$UNIT" \
    || fail "ExecStart is not the binary plus --config; that simplicity is the whole point of #32"
# An assignment at the start of a line, not the word. The first version matched the comment in the
# unit that explains why the setting is absent — the third time in this repository that a guard
# fired on the presence of a word rather than on the thing it guards (pitfall 78, and the flag list
# that disagreed with the parser).
grep -qE "^LimitMEMLOCK=" "$UNIT" \
    && fail "LimitMEMLOCK is set; the engine locks no memory, so it raises a limit for nothing and reads as knowledge about the engine"
grep -qE "^CPUAffinity=" "$UNIT" \
    && fail "CPUAffinity is set; pinning to particular cores on an unknown machine is a mistake rather than a tuning"
ok "ExecStart is two arguments, and no limit is raised for a thing the engine does not do"

echo "package verified."
