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
DEB=$(ls "$BUILD_DIR"/orderbook-dbengine-*-Linux.deb 2>/dev/null | head -1)
TGZ=$(ls "$BUILD_DIR"/orderbook-dbengine-*-Linux.tar.gz 2>/dev/null | head -1)

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "  ok: $*"; }

[ -n "$DEB" ] || fail "no .deb in $BUILD_DIR"
[ -n "$TGZ" ] || fail "no tarball in $BUILD_DIR"
echo "package: $DEB"

# ── Layout ────────────────────────────────────────────────────────────────────
CONTENTS=$(dpkg-deb -c "$DEB" | awk '{print $6}')
for path in ./usr/bin/ob_tcp_server \
            ./etc/orderbook/ob.conf \
            ./usr/lib/systemd/system/ob_tcp_server.service \
            ./usr/share/man/man1/ob_tcp_server.1 \
            ./usr/include/orderbook/engine.hpp; do
    echo "$CONTENTS" | grep -qx -- "$path" || fail "missing from the package: $path"
done
ok "binary, config, unit, man page and headers are all in the package"

# The config must be at /etc, not /usr/etc. `packaging/debian/conffiles` names /etc/orderbook/ob.conf,
# and a conffile declaration pointing at a path the package does not contain marks nothing — the
# first upgrade then silently reverts every local edit.
echo "$CONTENTS" | grep -q "^./usr/etc/" && fail "configuration installed under /usr/etc; the conffile mark would name a path the package does not contain"
ok "no /usr/etc — the conffile declaration names a path that exists"

# The Python wheel's shared library must not be here. It is installed for scikit-build-core into a
# directory that means nothing on a system, and CPack with component install off takes every rule.
echo "$CONTENTS" | grep -q "orderbook_engine/" && fail "the wheel's shared library leaked into the package"
ok "no wheel artefacts"

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

# ── The shipped configuration has to start ────────────────────────────────────
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
dpkg-deb -x "$DEB" "$WORK"

"$WORK/usr/bin/ob_tcp_server" --config "$WORK/etc/orderbook/ob.conf" --print-config > "$WORK/resolved.txt" \
    || fail "the packaged binary refused the packaged configuration"
grep -q "data-dir .*/var/lib/orderbook .*(file)" "$WORK/resolved.txt" \
    || fail "the shipped configuration did not take effect: data-dir is not from the file"
ok "the packaged binary accepts the packaged configuration, and the file's values reach it"

# ── The unit ──────────────────────────────────────────────────────────────────
# systemd-analyze reports the ExecStart binary as missing unless the package is installed, which it
# is not. Any *other* message is a real complaint.
UNIT="$WORK/usr/lib/systemd/system/ob_tcp_server.service"
VERIFY=$(systemd-analyze verify "$UNIT" 2>&1 | grep -v "is not executable: No such file or directory" || true)
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
