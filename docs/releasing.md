# Releasing the engine

A release is a tag `vX.Y.Z` on a commit of `master`, and nothing else: `.github/workflows/release.yml`
builds, checks and publishes everything from it (#42). There is no manual trigger and no token.

## What a release contains

| Artefact | For |
|---|---|
| `orderbook-dbengine_X.Y.Z_amd64.deb`, `..._arm64.deb` | Debian and Ubuntu: the server, `ob_backup`, `ob_restore`, the configuration, the systemd unit, the man page, the headers, and the C API library under `/usr/lib/orderbook-dbengine/` |
| `orderbook-dbengine-X.Y.Z-1.x86_64.rpm`, `...aarch64.rpm` | The same for RPM systems |
| `orderbook-dbengine-X.Y.Z-Linux-x86_64.tar.gz`, `...-aarch64.tar.gz` | The same layout, for anywhere else |
| `orderbook_dbengine-X.Y.Z-py3-none-any.whl` and `orderbook_dbengine-X.Y.Z.tar.gz` | The Python client, also on PyPI as `orderbook-dbengine` |
| `SHA256SUMS` | Every file above |

The Python client is pure Python: over TCP it needs nothing native, and its local mode loads the C
API library the system package installed (or `OB_LIB_PATH`). A platform wheel would have carried its
own OpenSSL and curl, whose fixes we would then have had to release ourselves.

Every artefact has a build-provenance attestation, and the PyPI upload its own:

```bash
gh attestation verify orderbook-dbengine_0.1.0_amd64.deb \
    --repo Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine
sha256sum -c SHA256SUMS
```

## Cutting one

1. A pull request that sets the version in `CMakeLists.txt` -
   `project(orderbook-dbengine VERSION X.Y.Z ...)`, the only place it is written - and renames
   `## [Unreleased]` in `CHANGELOG.md` to `## [X.Y.Z] - YYYY-MM-DD`, with a new empty
   `## [Unreleased]` above it. CI checks it with `scripts/release.py check` and builds and accepts
   the packages and the wheel on both architectures, as on every pull request.
2. Merge it, and wait for `master`'s CI.
3. Tag the merge and push the tag - signed, once the signing key exists:
   `git tag -s vX.Y.Z <merge sha> -m "orderbook-dbengine X.Y.Z" && git push origin vX.Y.Z`.
4. The workflow runs, and the PyPI job waits for its reviewer's approval in the `pypi` environment.
5. Once it is out, a pull request that moves `scripts/previous_version.txt` to the tagged commit
   (`git rev-parse vX.Y.Z^{commit}`) and the matrix in `docs/upgrading.md` with it: from then on
   `tests/integration/test_mixed_versions.py` measures every tree against the build users run.

A first attempt can fail on something only a tag exercises. PyPI never gives a version back once a
file is uploaded, so a failure after the upload means the next release is X.Y.Z+1; a GitHub release
can be deleted and the tag pushed again.

## What runs, and when

Everything but publishing runs on **every pull request**, in `ci.yml`'s required `package` (x86_64)
and `package-arm64` (aarch64), through `scripts/package_ci.sh`: a native build of the server, the
tools and the C API library, `--version` equal to `CMakeLists.txt`'s, `cpack`,
`scripts/verify_package.sh`, the RPM inspected, the wheel and the sdist built and checked - and the
same wheel built from the sdist by the oldest scikit-build-core `pyproject.toml` allows - the `.deb`
installed with `apt`, and `scripts/release_acceptance.py` - the wheel's client in a fresh venv, run
outside the repository, writing to the installed server over TCP and finding the installed library in
local mode by itself. So the first time a package is built for aarch64 is never the day of a release.

On a workstation, `scripts/package_ci.sh build-pkg dist --no-install` runs all of it but the install,
and accepts the release from the extracted tarball instead.

On a tag, `release.yml`:

| Job | What |
|---|---|
| `gate` | `release.py check`; the tag is `vX.Y.Z`, equal to `CMakeLists.txt`'s version, `CHANGELOG.md` has its section, and the commit is an ancestor of `master` |
| `packages` (`ubuntu-24.04`, `ubuntu-24.04-arm`) | `scripts/package_ci.sh`, as on a pull request, and the artefacts uploaded |
| `github-release` | `SHA256SUMS`, attestations, the release with `CHANGELOG.md`'s section as its notes |
| `publish-pypi` | trusted publishing, after the `pypi` environment's reviewer approves |

`release.yml` has no `pull_request` trigger on purpose: a job there would be a check that runs and
gates nothing, which `.github/rulesets/check_contexts.py` refuses.

`scripts/release_acceptance.py dist X.Y.Z --root <extracted .tar.gz>` runs the acceptance on a
machine nobody should install a package on, with the library named by `OB_LIB_PATH`.

## The owner's steps, once

None of these can be done from the API, and the release cannot run before the first two:

1. **PyPI**: a *pending* trusted publisher for the project `orderbook-dbengine` (the name is free; the
   first upload creates the project): owner `Smart-Data-Engines`, repository
   `low-cost-and-low-latency-orderbook-dbengine`, workflow `release.yml`, environment `pypi`.
2. **GitHub**: an environment named `pypi` with a required reviewer.
3. **A key to sign tags**, and the repository's `required_signatures` with it.

Renaming `release.yml`, or the `pypi` environment, revokes publishing until the trusted publisher on
PyPI is changed to match; `scripts/release.py check` holds the file's name.
