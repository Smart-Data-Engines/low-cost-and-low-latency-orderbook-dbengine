#!/usr/bin/env bash
# What CI's package jobs and the release's packages job do, so that the three do the same (#42):
#
#   - build the server, the backup tools and the C API library natively for this architecture, and
#     check that the server reports the version CMakeLists.txt declares;
#   - package them (.deb, .tar.gz, and .rpm where rpmbuild exists) and check the packages without
#     installing them (scripts/verify_package.sh), the RPM's layout, configuration and licence too;
#   - build the Python client's wheel and sdist: one pure wheel of this version, an sdist that builds
#     the same wheel, `twine check`;
#   - install the .deb with apt, as a user would, and accept the release from the artefacts alone
#     (scripts/release_acceptance.py).
#
#   scripts/package_ci.sh [build dir] [dist dir]
#
# The install needs sudo and changes the system, so this is for a disposable CI runner. On a
# workstation run verify_package.sh, and release_acceptance.py with --root.
set -euo pipefail

BUILD=${1:-build-pkg}
DIST=${2:-dist}
VERSION=$(python3 scripts/release.py version | cut -d= -f2)
step() { echo; echo "── $*"; }

step "build, natively ($(uname -m)), version $VERSION"
cmake -S . -B "$BUILD" -DCMAKE_BUILD_TYPE=Release -DOB_BUILD_TESTS=OFF
cmake --build "$BUILD" -j"$(nproc)" --target ob_tcp_server ob_restore ob_backup orderbook_shared
reported=$("$BUILD/ob_tcp_server" --version)
[ "$reported" = "ob_tcp_server $VERSION" ] || { echo "FAIL: the server says '$reported'"; exit 1; }
echo "  ok: $reported"

step "packages"
(cd "$BUILD" && cpack)
./scripts/verify_package.sh "$BUILD"
if command -v rpm > /dev/null; then
    RPM=$(ls "$BUILD"/orderbook-dbengine-*.rpm | head -1)
    rpm -qip "$RPM"
    # The two properties the .deb is held to - the config at /etc, marked so an upgrade keeps an
    # operator's edits - and the library and the licence (#42: it said MIT).
    rpm -qlp "$RPM" | grep -qx /etc/orderbook/ob.conf
    rpm -qcp "$RPM" | grep -qx /etc/orderbook/ob.conf
    rpm -qlp "$RPM" | grep -qx /usr/lib/orderbook-dbengine/liborderbook_shared.so
    rpm -qip "$RPM" | grep -q "^License *: Apache-2.0"
    echo "  ok: the RPM: config at /etc and marked, the C API library, Apache-2.0"
fi

step "the Python client's wheel and sdist"
python3 -m venv "$BUILD/venv-dist"
"$BUILD/venv-dist/bin/pip" install --quiet --upgrade pip build twine
rm -rf "$DIST"
"$BUILD/venv-dist/bin/python" -m build --outdir "$DIST" .
WHEEL="$DIST/orderbook_dbengine-$VERSION-py3-none-any.whl"
SDIST="$DIST/orderbook_dbengine-$VERSION.tar.gz"
[ -f "$WHEEL" ] && [ -f "$SDIST" ] && [ "$(ls "$DIST" | wc -l)" -eq 2 ] \
    || { echo "FAIL: $DIST holds $(ls "$DIST"), not one pure wheel and one sdist of $VERSION"; exit 1; }
"$BUILD/venv-dist/bin/python" -m twine check --strict "$DIST"/*
"$BUILD/venv-dist/bin/pip" wheel --quiet --no-deps -w "$BUILD/from-sdist" "$SDIST"
[ -f "$BUILD/from-sdist/$(basename "$WHEEL")" ] || { echo "FAIL: the sdist does not build $(basename "$WHEEL")"; exit 1; }
echo "  ok: $(basename "$WHEEL"), and an sdist that builds it"

step "installed as a user would, and accepted from the artefacts"
sudo apt-get install -y -qq "$PWD/$BUILD/orderbook-dbengine_${VERSION}_$(dpkg --print-architecture).deb"
python3 scripts/release_acceptance.py "$DIST" "$VERSION"
