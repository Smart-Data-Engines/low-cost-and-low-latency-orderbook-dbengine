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
#   scripts/package_ci.sh [build dir] [dist dir] [--no-install]
#
# The install needs sudo and changes the system, so it is for a disposable CI runner. On a
# workstation, `--no-install` runs everything else and accepts the release from the extracted
# tarball instead (release_acceptance.py --root), which tests all of it but where the client looks
# for the library by itself.
set -euo pipefail

BUILD=${1:-build-pkg}
DIST=${2:-dist}
case "${3:-}" in
    "")           INSTALL=yes ;;
    --no-install) INSTALL=no ;;
    *)            echo "usage: $0 [build dir] [dist dir] [--no-install]" >&2; exit 2 ;;
esac
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
    # The rule verify_package.sh holds the .deb and the tarball to (#210), and the user the unit runs
    # as - which only the .deb's postinst created, so after an RPM install the service had no user to
    # start as (#211).
    BAD=$(rpm -qlvp "$RPM" | awk '$3 != "root" || $4 != "root" || ($1 !~ /^l/ && (substr($1, 6, 1) == "w" || substr($1, 9, 1) == "w"))')
    [ -z "$BAD" ] || { echo "FAIL: RPM entries that are not root's, or are writable beyond their owner:"; echo "$BAD" | head -5; exit 1; }
    SCRIPTS=$(rpm -qp --scripts "$RPM")
    echo "$SCRIPTS" | grep -q "useradd .*orderbook" \
        || { echo "FAIL: the RPM does not create the orderbook user its unit runs as"; exit 1; }
    echo "  ok: the RPM: every entry root's and none writable beyond its owner, and it creates the orderbook user"
fi

step "the Python client's wheel and sdist"
python3 -m venv "$BUILD/venv-dist"
"$BUILD/venv-dist/bin/pip" install --quiet --upgrade pip build twine
# All emptied first: on a workstation a file left by an earlier run would answer the checks below.
rm -rf "${DIST:?}" "${BUILD:?}/from-sdist" "${BUILD:?}/from-sdist-floor"
"$BUILD/venv-dist/bin/python" -m build --outdir "$DIST" .
WHEEL="$DIST/orderbook_dbengine-$VERSION-py3-none-any.whl"
SDIST="$DIST/orderbook_dbengine-$VERSION.tar.gz"
[ -f "$WHEEL" ] && [ -f "$SDIST" ] && [ "$(ls "$DIST" | wc -l)" -eq 2 ] \
    || { echo "FAIL: $DIST holds $(ls "$DIST"), not one pure wheel and one sdist of $VERSION"; exit 1; }
"$BUILD/venv-dist/bin/python" -m twine check --strict "$DIST"/*
"$BUILD/venv-dist/bin/pip" wheel --quiet --no-deps -w "$BUILD/from-sdist" "$SDIST"
[ -f "$BUILD/from-sdist/$(basename "$WHEEL")" ] || { echo "FAIL: the sdist does not build $(basename "$WHEEL")"; exit 1; }
echo "  ok: $(basename "$WHEEL"), and an sdist that builds it"

# The oldest scikit-build-core pyproject.toml allows, building the same wheel from the sdist: a floor
# in build-system.requires is a promise to whoever builds with their system's version, and nothing
# else holds it - `python -m build` above took the newest.
FLOOR=$(python3 - <<'PY'
import re, tomllib
requires = tomllib.load(open("pyproject.toml", "rb"))["build-system"]["requires"]
print(next(m.group(1) for r in requires if (m := re.fullmatch(r"scikit-build-core>=([0-9.]+)", r))))
PY
)
python3 -m venv "$BUILD/venv-floor"
"$BUILD/venv-floor/bin/pip" install --quiet "scikit-build-core==$FLOOR"
"$BUILD/venv-floor/bin/pip" wheel --quiet --no-deps --no-build-isolation -w "$BUILD/from-sdist-floor" "$SDIST"
[ -f "$BUILD/from-sdist-floor/$(basename "$WHEEL")" ] \
    || { echo "FAIL: scikit-build-core $FLOOR does not build $(basename "$WHEEL") from the sdist"; exit 1; }
echo "  ok: scikit-build-core $FLOOR, the oldest pyproject.toml allows, builds it from the sdist too"

if [ "$INSTALL" = yes ]; then
    step "installed as a user would, and accepted from the artefacts"
    sudo apt-get install -y -qq "$PWD/$BUILD/orderbook-dbengine_${VERSION}_$(dpkg --print-architecture).deb"
    python3 scripts/release_acceptance.py "$DIST" "$VERSION"
else
    step "accepted from the artefacts, the tarball extracted rather than the .deb installed"
    rm -rf "${BUILD:?}/extracted"
    mkdir -p "$BUILD/extracted"
    tar xzf "$BUILD/orderbook-dbengine-$VERSION-Linux-$(uname -m).tar.gz" -C "$BUILD/extracted" \
        --strip-components=1
    python3 scripts/release_acceptance.py "$DIST" "$VERSION" --root "$BUILD/extracted"
fi
