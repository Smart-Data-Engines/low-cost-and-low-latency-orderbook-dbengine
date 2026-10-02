#!/usr/bin/env python3
"""The engine's releases (#42): what a tag must agree with, and what every pull request holds.

    scripts/release.py check                 # in CI on every pull request
    scripts/release.py gate refs/tags/vX.Y.Z # the release workflow's first step
    scripts/release.py notes X.Y.Z           # the release's notes, from CHANGELOG.md
    scripts/release.py version               # version=X.Y.Z, CMakeLists.txt's, for $GITHUB_OUTPUT

The version is `project(orderbook-dbengine VERSION X.Y.Z)` in CMakeLists.txt and nowhere else. The
binaries report it through OB_VERSION, pyproject.toml reads it from there, and the Python client asks
its installed metadata. `check` fails on a second copy - pyproject.toml's `version =`, a `__version__`
assigned a literal - and on a README banner that shows another version; it also holds the release
workflow's name, which PyPI's trusted publisher is bound to, its trigger - tags alone - and its
actions pinned by full commit SHA.

`gate` refuses a tag that is not `vX.Y.Z`, that names another version than CMakeLists.txt's, or whose
version has no section in CHANGELOG.md; it prints `version=X.Y.Z` for $GITHUB_OUTPUT. Exit status 0
if clean, 1 otherwise.
"""
from __future__ import annotations

import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
CMAKE = REPO / "CMakeLists.txt"
PYPROJECT = REPO / "pyproject.toml"
CLIENT = REPO / "python" / "orderbook_engine" / "__init__.py"
README = REPO / "README.md"
CHANGELOG = REPO / "CHANGELOG.md"
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"

SEMVER = r"[0-9]+\.[0-9]+\.[0-9]+"
TAG_RE = re.compile(rf"^refs/tags/v({SEMVER})$")


def cmake_version() -> str:
    m = re.search(rf"^project\(orderbook-dbengine VERSION ({SEMVER})\b", CMAKE.read_text(), re.M)
    if not m:
        sys.exit(f"{CMAKE.name}: no `project(orderbook-dbengine VERSION X.Y.Z ...)`")
    return m.group(1)


def changelog_section(version: str) -> str | None:
    """The body of `## [version]`, up to the next `## [`, or None when there is no such section."""
    text = CHANGELOG.read_text() if CHANGELOG.exists() else ""
    m = re.search(rf"^## \[{re.escape(version)}\][^\n]*\n(.*?)(?=^## \[|\Z)", text, re.M | re.S)
    return m.group(1).strip() if m else None


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
    for shown in re.findall(rf"ob_tcp_server v({SEMVER})", README.read_text()):
        if shown != version:
            problems.append(f"README.md shows `ob_tcp_server v{shown}`; the version is {version}")

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


def gate(ref: str) -> None:
    m = TAG_RE.match(ref)
    if not m:
        sys.exit(f"{ref}: a release tag is vX.Y.Z")
    tagged = m.group(1)
    version = cmake_version()
    if tagged != version:
        sys.exit(f"the tag is v{tagged} and CMakeLists.txt says {version}: bump the version in a PR first")
    if changelog_section(tagged) is None:
        sys.exit(f"CHANGELOG.md has no `## [{tagged}]` section: a release says what it releases")
    print(f"version={tagged}")


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
    if len(argv) == 2 and argv[1] == "version":
        print(f"version={cmake_version()}")
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
