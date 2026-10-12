#!/usr/bin/env python3
"""The engine's releases (#42): what a tag must agree with, and what every pull request holds.

    scripts/release.py check                 # in CI on every pull request
    scripts/release.py gate refs/tags/vX.Y.Z # the release workflow's first step
    scripts/release.py notes X.Y.Z           # the release's notes, from CHANGELOG.md
    scripts/release.py version               # version=, package_version=, CMakeLists.txt's
    scripts/release.py self-test             # the rules above on cases of their own

The version is `set(OB_VERSION "...")` in CMakeLists.txt and nowhere else: X.Y.Z in a release, and
between releases the next one as a development version, X.Y.Z.devN (#234). The binaries report it
through OB_VERSION, pyproject.toml reads it from there, the Python client asks its installed
metadata, and the packages carry it as X.Y.Z~devN, which dpkg and rpm sort before X.Y.Z. `check`
fails on a second copy - pyproject.toml's `version =`, a `__version__` assigned a literal - and on a
README banner that shows another version than this one or, between releases, than one CHANGELOG.md
has released; it also holds the release workflow's name, which PyPI's trusted publisher is bound to,
its trigger - tags alone - and its actions pinned by full commit SHA.

`gate` refuses a tag that is not `vX.Y.Z`, that names another version than CMakeLists.txt's - a
development version among them, which the release's pull request drops - or whose version has no
section in CHANGELOG.md; it prints `version=X.Y.Z` for $GITHUB_OUTPUT. Exit status 0 if clean, 1
otherwise.
"""
from __future__ import annotations

import pathlib
import re
import sys
from typing import Callable

REPO = pathlib.Path(__file__).resolve().parent.parent
CMAKE = REPO / "CMakeLists.txt"
PYPROJECT = REPO / "pyproject.toml"
CLIENT = REPO / "python" / "orderbook_engine" / "__init__.py"
README = REPO / "README.md"
CHANGELOG = REPO / "CHANGELOG.md"
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"

SEMVER = r"[0-9]+\.[0-9]+\.[0-9]+"
TAG_RE = re.compile(rf"^refs/tags/v({SEMVER})$")
VERSION_RE = re.compile(rf"^({SEMVER})(?:\.dev([0-9]+))?$")


def cmake_version() -> str:
    """CMakeLists.txt's OB_VERSION: X.Y.Z, or X.Y.Z.devN between releases."""
    m = re.search(r'^set\(OB_VERSION "([^"]*)"\)', CMAKE.read_text(), re.M)
    if not m:
        sys.exit(f'{CMAKE.name}: no `set(OB_VERSION "X.Y.Z")`')
    if not VERSION_RE.match(m.group(1)):
        sys.exit(f"{CMAKE.name}: OB_VERSION {m.group(1)!r} is neither X.Y.Z nor X.Y.Z.devN")
    return m.group(1)


def parts(version: str) -> tuple[str, str | None]:
    """(X.Y.Z, N) of X.Y.Z.devN, and (X.Y.Z, None) of a release."""
    m = VERSION_RE.match(version)
    if not m:
        sys.exit(f"{version!r} is neither X.Y.Z nor X.Y.Z.devN")
    return m.group(1), m.group(2)


def is_development(version: str) -> bool:
    return parts(version)[1] is not None


def numbers(version: str) -> tuple[int, ...]:
    """X.Y.Z of a version, as numbers, for ordering a released version against it."""
    return tuple(int(n) for n in parts(version)[0].split("."))


def package_version(version: str) -> str:
    """The version the .deb, the .rpm and the tarball carry: X.Y.Z~devN sorts before X.Y.Z."""
    release, dev = parts(version)
    return release if dev is None else f"{release}~dev{dev}"


def changelog_section(version: str) -> str | None:
    """The body of `## [version]`, up to the next `## [`, or None when there is no such section."""
    text = CHANGELOG.read_text() if CHANGELOG.exists() else ""
    m = re.search(rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.M | re.S)
    return m.group(1).strip() if m else None


def banner_problems(readme: str, version: str, released: Callable[[str], bool]) -> list[str]:
    """The README's `ob_tcp_server vX.Y.Z` banners against the version.

    A banner is what a reader gets from the release they install: this version or, between
    releases, one CHANGELOG.md has released (`released`) and that comes before the version under
    development (#234). At a release, only the release's own."""
    problems = []
    for shown in re.findall(rf"ob_tcp_server v({SEMVER}(?:\.dev[0-9]+)?)", readme):
        if shown == version:
            continue
        earlier_release = not is_development(shown) and released(shown) and \
            numbers(shown) < numbers(version)
        if not (is_development(version) and earlier_release):
            problems.append(f"README.md shows `ob_tcp_server v{shown}`; the version is {version}"
                            + (", and CHANGELOG.md has released no earlier one by that number"
                               if is_development(version) else ""))
    return problems


def check() -> list[str]:
    problems = []
    version = cmake_version()

    project = re.search(r"^\[project\]\n(.*?)(?=^\[|\Z)", PYPROJECT.read_text(), re.M | re.S)
    body = project.group(1) if project else ""
    if re.search(r"^version\s*=", body, re.M):
        problems.append("pyproject.toml: [project] has `version =`; it must be dynamic, from CMakeLists.txt")
    if not re.search(r'^dynamic\s*=\s*\[[^\]]*"version"', body, re.M):
        problems.append('pyproject.toml: [project] does not have `dynamic = ["version"]`')
    if re.search(r"^__version__\s*=\s*['\"]", CLIENT.read_text(), re.M):
        problems.append(f"{CLIENT.relative_to(REPO)}: `__version__` is a literal; it must come from the installed metadata")
    problems += banner_problems(README.read_text(), version,
                                lambda v: changelog_section(v) is not None)

    if not CHANGELOG.exists():
        problems.append("CHANGELOG.md does not exist")
    elif changelog_section("Unreleased") is None and changelog_section(version) is None:
        problems.append(f"CHANGELOG.md has neither `## [Unreleased]` nor `## [{version}]`")

    if not WORKFLOW.exists():
        problems.append(f"{WORKFLOW.relative_to(REPO)} does not exist; PyPI's trusted publisher names this file")
    else:
        wf = WORKFLOW.read_text()
        # The key, not the word: the file's own comment says there is none (pitfall 78's shape).
        if re.search(r"^\s*workflow_dispatch\s*:", wf, re.M):
            problems.append("release.yml has workflow_dispatch: a release is a tag, and nothing else")
        if not re.search(r"^\s+tags:\s*\n\s+-\s*'v\*'", wf, re.M):
            problems.append("release.yml is not triggered by tags `v*`")
        if re.search(r"^\s*pull_request\s*:", wf, re.M):
            problems.append("release.yml runs on pull requests: its jobs would be checks no ruleset "
                            "requires; ci.yml's package jobs are the dry run")
        for line in re.findall(r"^\s*-?\s*uses:\s*(\S+)", wf, re.M):
            if line.startswith("./"):
                continue
            if not re.search(r"@[0-9a-f]{40}$", line):
                problems.append(f"release.yml: `{line}` is not pinned to a full commit SHA")
    return problems


def gate_refusal(ref: str, version: str, released: Callable[[str], bool]) -> str | None:
    """Why a tag may not release this tree, or None when it may."""
    m = TAG_RE.match(ref)
    if not m:
        return f"{ref}: a release tag is vX.Y.Z"
    tagged = m.group(1)
    if is_development(version):
        return (f"CMakeLists.txt says {version}, a development version: the release's pull request "
                f"sets OB_VERSION to {tagged} first")
    if tagged != version:
        return f"the tag is v{tagged} and CMakeLists.txt says {version}: bump the version in a PR first"
    if not released(tagged):
        return f"CHANGELOG.md has no `## [{tagged}]` section: a release says what it releases"
    return None


def gate(ref: str) -> None:
    refusal = gate_refusal(ref, cmake_version(), lambda v: changelog_section(v) is not None)
    if refusal:
        sys.exit(refusal)
    print(f"version={TAG_RE.match(ref).group(1)}")  # type: ignore[union-attr]


def self_test() -> list[str]:
    """The version rules on cases of their own, so a change that makes them accept everything fails
    in CI before it is trusted with the tree."""
    has = {"0.1.0", "0.0.9"}.__contains__          # what a CHANGELOG has sections for
    cases = [
        ("a release's own banner", banner_problems("ob_tcp_server v0.1.0", "0.1.0", has), 0),
        ("a release showing an earlier one", banner_problems("ob_tcp_server v0.0.9", "0.1.0", has), 1),
        ("between releases, the last release", banner_problems("ob_tcp_server v0.1.0", "0.1.1.dev0", has), 0),
        ("between releases, a version never released",
         banner_problems("ob_tcp_server v0.0.8", "0.1.1.dev0", has), 1),
        ("between releases, a later release", banner_problems("ob_tcp_server v0.2.0", "0.1.1.dev0",
                                                                  {"0.2.0"}.__contains__), 1),
        ("between releases, the development version itself",
         banner_problems("ob_tcp_server v0.1.1.dev0", "0.1.1.dev0", has), 0),
        # Refused by name: the tag could never equal X.Y.Z.devN, but the reason is what to do.
        ("a tag of a development version", [r for r in [gate_refusal("refs/tags/v0.1.1", "0.1.1.dev0", has)]
                                            if r and "a development version" in r], 1),
        ("a tag of its version, released", [gate_refusal("refs/tags/v0.1.0", "0.1.0", has)], 0),
        ("a tag of another version", [gate_refusal("refs/tags/v0.1.1", "0.1.0", has)], 1),
        ("a tag without a changelog section", [gate_refusal("refs/tags/v0.2.0", "0.2.0", has)], 1),
        ("a tag that is not vX.Y.Z", [gate_refusal("refs/tags/v0.1.1.dev0", "0.1.1.dev0", has)], 1),
    ]
    failures = []
    for name, found, want in cases:
        got = len([f for f in found if f])
        if got != want:
            failures.append(f"self-test: {name}: {got} problem(s), wanted {want}: {found}")
    for version, want in (("0.1.1.dev0", "0.1.1~dev0"), ("0.1.1", "0.1.1"), ("2.0.0.dev12", "2.0.0~dev12")):
        if package_version(version) != want:
            failures.append(f"self-test: the packages' {version} is {package_version(version)}, not {want}")
    return failures


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "check":
        problems = check()
        for p in problems:
            print(f"FAIL: {p}", file=sys.stderr)
        if not problems:
            print(f"release: version {cmake_version()}, one source, workflow and changelog in order")
        return 1 if problems else 0
    if len(argv) == 3 and argv[1] == "gate":
        gate(argv[2])
        return 0
    if len(argv) == 2 and argv[1] == "self-test":
        failures = self_test()
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        if not failures:
            print("release: the version rules pass their own cases")
        return 1 if failures else 0
    if len(argv) == 2 and argv[1] == "version":
        version = cmake_version()
        print(f"version={version}")
        print(f"package_version={package_version(version)}")
        return 0
    if len(argv) == 3 and argv[1] == "notes":
        notes = changelog_section(argv[2])
        if notes is None:
            sys.exit(f"CHANGELOG.md has no `## [{argv[2]}]` section")
        print(notes)
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
