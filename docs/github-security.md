# Securing the public GitHub repository

This repository is public, which means an attacker can read everything in it, including our build
configuration and dependency choices. The measures below cover both sides of that: keeping bad code
and bad dependencies out, and keeping credentials from ever landing in history.

Everything below is either a file in this repo (already added, marked ✅) or a setting that has to
be clicked in the GitHub UI / set via `gh` CLI (marked ⚙️).

## 1. Protect the default branch

### What is already in place ✅

A ruleset named `master` is active on `refs/heads/master` with no bypass actors, enforcing:

- `deletion` — the branch cannot be deleted
- `non_fast_forward` — no force pushes
- `pull_request` — direct pushes are blocked, changes go through a PR

That covers accidental history destruction. The important gap is below.

### The rule that was missing, and now is not ✅

**A required status check.** Before 23 August 2026 a PR could be merged while CI was failing, or
before CI had even run. This is the single most valuable rule in the whole document: it is what makes
"the tests pass on master" a fact rather than a habit. It is now `required_status_checks` with four
contexts and `strict: true`, so a stale branch has to be updated before merging.

The check context only becomes selectable after `ci.yml` has run at least once, so push the workflow
first, then add the rule.

Also worth adding to the same ruleset:

- `required_signatures` — see §6
- `required_linear_history` — keeps `git log` on master readable, and pairs well with squash merges
- `pull_request.required_review_thread_resolution: true` — an unresolved comment cannot be merged past
- `pull_request.required_approving_review_count` stays at `0` while you are the only maintainer;
  raise it to `1` the moment a second person has write access

Do **not** add yourself to `bypass_actors`. The point of the ruleset is that it catches your own
mistakes, and `bypass_actors: []` is currently correct.

Updating the existing ruleset (id `20841774`) rather than creating a second one:

```bash
gh api -X PUT repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/rulesets/20841774 \
  --input - <<'JSON'
{
  "name": "master",
  "target": "branch",
  "enforcement": "active",
  "conditions": { "ref_name": { "include": ["refs/heads/master"], "exclude": [] } },
  "bypass_actors": [],
  "rules": [
    { "type": "deletion" },
    { "type": "non_fast_forward" },
    { "type": "required_signatures" },
    { "type": "required_linear_history" },
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 0,
        "dismiss_stale_reviews_on_push": true,
        "require_code_owner_review": false,
        "require_last_push_approval": false,
        "required_review_thread_resolution": true,
        "allowed_merge_methods": ["squash", "rebase"]
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": true,
        "required_status_checks": [
          { "context": "build-and-test" },
          { "context": "release-build" },
          { "context": "docs-integrity" },
          { "context": "analyze (c-cpp)" }
        ]
      }
    }
  ]
}
JSON
```

The list above is **twelve** checks as of 2 September 2026 — eleven, plus `package` from #33 — not
the four the JSON block shows; see
§1.1, which is about how it came to be wrong.

`package` is the twelfth, from roadmap #33. It builds the `.deb`, the tarball and — where
`rpmbuild` exists, which is CI and not the development machine — the `.rpm`, then checks the layout,
the metadata, the conffile mark, and that **the packaged binary accepts the packaged configuration**.
It runs on pull requests as well as tags: a packaging job first exercised on a tag is first
exercised at the moment it matters most.

`coverage` came with roadmap #37. It gates three things and deliberately not a
percentage: that the tree builds with coverage instrumentation, that the suite passes under it, and
that the instrumentation **reaches the libraries** — which it did not for as long as the option
existed (#83), while the number it printed looked like a number. No third-party service is involved;
the figure lands in the job summary and the per-file report as an artifact.

The tenth is `clang-build`, added with roadmap #37. It carries no infrastructure of its own beyond a
compiler, so it is the cheapest of these to keep green — and it earned its place on the first run by
finding two variables that are computed and never read, which GCC does not warn about.

The ninth is `integration-tests`, added with roadmap #55: the whole pytest suite against a plain
build, which is the only check that exercises failover, crash recovery under `SIGKILL` and the
position-lease invariant. It carries the same cost as the two below and one of its own — the failover
module has a wall-clock dependency, and it flaked once locally when a benchmark was saturating a core
in parallel. If it flakes on shared runners the answer is a measured timeout, not removing the check.

The eighth is `sanitizers-integration (tsan)`, added
with roadmap #80 and required for the same reason the other two sanitizer contexts are: it found a
lock-order inversion on its first run, and a check that finds that class and gates nothing is the
more dangerous kind of green. Its known cost is the same as CodeQL's — **an infrastructure failure
blocks merges exactly as effectively as a real finding**, and here the infrastructure is etcd, a
three-node cluster and a TSan build. The escape hatch is the documented one:
`gh pr merge --admin` with a note on the pull request saying why. The modules that kill nodes are
deliberately outside that job, because their fixtures wait on timeouts that instrumentation makes
unreliable and a flaky required check teaches people to ignore checks.

The seventh is `CodeQL`, and it is not a second analysis job: `analyze (c-cpp)` is ours and goes green when the *job* succeeds, while `CodeQL` is posted by the
code scanning integration and fails when a pull request **introduces a new alert**. Pre-existing alerts
do not fail it, which is what makes it adoptable without clearing the backlog first — it is a ratchet
on the count rather than a demand.

That backlog is worth naming, because a security document that lists a scanner and not its output is
half a document. On 27 August 2026 there were **45 open alerts**: 30 in `build/_deps` — nlohmann/json's
headers, pulled in by `FetchContent` and then analysed as though we wrote them — and 15 in `src/`.
Exactly **one** of the 45 carries a security severity. The other 44 are the quality suite's, and none of
them had been acted on.

An earlier revision of this section said 30 open alerts, half of them vendored. Both numbers were wrong
and the reason is worth recording, because it is the same shape as the `PATCH`-returns-200 trap in §2:
**`GET /code-scanning/alerts` paginates at 30 by default.** The query returned exactly 30 rows, which
looked like a total. Use `per_page=100` and `--paginate`, and be suspicious of any count that equals a
page size.

The 30 vendored findings are now **dismissed** as `won't fix`, each with the reason on the alert. That
is the mechanism that works here, and the first attempt was a mechanism that does not: `paths-ignore`
was added to `codeql.yml` to exclude `build/**` and had **no effect**. It only applies to interpreted
languages; for a compiled one CodeQL analyses whatever the build compiles, and nlohmann/json is
header-only, so its headers are extracted through our own translation units. The analysis after that
change reported the same 45 results, which is how this is known rather than assumed. The config was
removed rather than left in place — configuration that looks load-bearing and is not is worse than
none, because the next reader takes it for a filter that works.

Two smaller things learned in the same hour, both about believing a command that says nothing:

- The dismissal comment is capped at **280 characters**. A longer one is rejected with
  `Invalid request`, and the first bulk run dismissed **0 of 30** while reporting nothing, because the
  loop had `2>/dev/null` on it. Suppressing stderr on a bulk write is how thirty failures become
  silence.
- The engine repository's own count above was quoted in a commit message before it was checked twice.
  The commit is in the history; the number in this document is the corrected one.

### Dismissals in code we wrote

**Alerts 90 and 91 (#30, wire authentication)** are the same rule and the same shape as alert 87:
`cpp/path-injection` on `--auth-secret-file` and `--cluster-secret-file` flowing into file opening.
Dismissed `won't fix`, with the reason on each alert and repeated here:

> Same shape as alert 87 (`--config`, #32): the path is this server's own `--auth-secret-file` /
> `--cluster-secret-file` argument. Whoever starts the process already reads any file, so the source
> does not cross a trust boundary. A bad path is refused at startup.

The assumption is the same one, and so is what would expire it: if a config path or a secret path
could ever be handed to `ob_tcp_server` by something other than its own operator, the sink is real.
Worth noting for these two specifically — the *contents* are a secret, so the eventual answer if that
ever changes is not path sanitisation but refusing to take the path from that source at all.

**Alert 92 (#93, the catch-up cursor)** is `cpp/path-injection` again, and it is the first one here
where the source **does** cross a trust boundary: the `<file>` field of a `REPLICATE` line, read off
a socket, reaching `::open()` through `wal_filename()`. Dismissed `false positive`, with the reason
on the alert and repeated here:

> A uint32_t from REPLICATE, rendered by snprintf("wal_%06u.bin") inside the servers own
> --data-dir. No peer byte reaches the path: an integer cannot emit a separator or "..". A file
> index above ours is answered ERR WAL_TRUNCATED, which is the right remedy.

The argument had to move from the source to the **sink**, and that is the distinction worth keeping.
For alerts 87, 90 and 91 the answer was "the caller already has these privileges"; that answer is
unavailable here, because a replica is a peer and not an operator. What holds instead is that the
tainted value is an *integer* and the only thing built from it is `%06u` inside a directory this
process chose — so there is no byte of peer input in the path at all, and sanitising a `uint32_t`
against traversal is not a coherent operation.

What would change the answer: if `wal_filename()` ever interpolated a **string**, or if the WAL
directory could come from the wire. Both are the same condition stated twice — the moment a peer
contributes a character rather than a number, this is a real sink.

**A code change was considered and rejected, and the reason is not the analyser.** Refusing a
`from_file` above our own current file index looks like the tidy answer, but the client matches
`WAL_TRUNCATED` specifically to trigger snapshot bootstrap (`replication.cpp:2249`), and a snapshot
is the correct recovery for *both* causes — retention removed the position, or the replica rotated
files while it was briefly a primary. A new error code would carry a new name for the same remedy
and leave any replica that does not know it retrying forever. The existing answer is right; only its
log line is less specific than the cause.

Alert 74 (`src/wal.cpp:100`) is the same sink through the same helper with a **command-line** source,
so it belongs to the 87/90/91 group rather than this one. It is still open, which is the open
question at the end of this section rather than an oversight.

**Alerts 88 and 89** are `cpp/unused-static-function`, note severity, on `is_identity_char` and
`valid_identity` in `src/auth.cpp`. Dismissed `false positive`, and the evidence is the compiler
rather than a reading of the code: both are used (one at `auth.cpp:59` as a callable argument to
`std::all_of`, the other at `:247`), and this tree builds under **GCC and Clang with
`-Werror=unused-function`**, which fails on a genuinely unused function in an anonymous namespace.
Verified directly with a two-function test case: the one passed to `std::all_of` is not flagged, the
unused one fails the build. The query misses a function used only as a callable.

That distinction is worth keeping for the next note-severity alert: **"is this reachable?" is a
question a compiler already answers on every push, and its answer beats an analyser's.** Where the
two disagree about liveness, the build is right.

### The first dismissal of our own code, and why it is not a fix

**Alert 87 is the first in code we wrote**:
`cpp/path-injection` on the `--config <path>` value flowing into `std::ifstream` (#32). Dismissed
`won't fix`, with the reason on the alert and repeated here because a dismissal that only lives in a
web UI is a decision nobody can review:

> The path is this server's own `--config` argument. Whoever starts the process already reads any
> file; `--data-dir` is the same shape and unflagged. Sanitising it would break legitimate paths and
> protect nobody. A bad path is refused with `cannot open config file`.

The distinction worth keeping: path injection is a real class when the path crosses a trust boundary
— an HTTP parameter, a filename inside an archive, a value from a config file that a less privileged
user can write. A command-line argument to a service binary does not cross one, because the caller
already has the privileges the sanitisation would be protecting. Adding a check there is security
theatre with a cost: it rejects symlinked and relative paths that operators legitimately use.

What would change this answer: if `ob_tcp_server` ever grew a way to be handed a config path by
something other than its own operator — a management API, a supervisor reading a request — then the
sink is real and the dismissal must be revisited. That is worth writing down, because a dismissal
carries an assumption and the assumption is what expires.

And the `PATCH` trap from §2 applies to dismissals too: the state was **read back** after the call
rather than inferred from a 200.

**The query suite is an open question, deliberately not decided here.** Dropping
`security-and-quality` for the default suite would remove 44 of the 45 alerts in one line and keep the
only one with a security severity — and this repository has `-Wall -Wextra -Werror`, ASan, UBSan and
TSan on every push, plus 684 unit and 127 integration tests, so the quality suite's marginal value is
small. Against that: it is the suite that produced `cpp/stack-address-escape`, which is how the latent
lifetime coupling below was noticed. Changing it is a tuning decision that deserves its own pull
request and its own observation window, not a line slipped into a security fix.

Of the 15 in our own code, 13 are `note`-level tidiness from the quality suite — unused locals in
`client.cpp` and `query_engine.cpp`, two empty `if`s, a long `switch`. They are a chore, not a security
backlog, and they are named here so that "45 alerts" does not read as 45 problems. The two worth a
person's attention were both read, and are recorded here rather than left in a web UI:

- **`cpp/path-injection`, high, `src/wal.cpp:88`** — `::open()` on a path built from `dir_`, which
  CodeQL traces back to `argv`. Real dataflow, but the source is the operator: somebody who can set the
  WAL directory can already write where their own permissions allow. Not reachable from the network and
  not reachable from a client connection. Triage as such, with the reason recorded on the alert.
- **`cpp/stack-address-escape`, warning, `src/engine.cpp:1272`** — `set_read_only_flag()` stores a raw
  `std::atomic<bool>*`. False as stated: both callers pass `&read_only_`, a *member* of `TcpServer` /
  `IoUringServer`, so no stack address escapes. What it does surface is real and unguarded, though:
  `Engine` holds a raw pointer into a server object's storage, nothing sets it back to `nullptr` in
  either destructor, and an `Engine` outliving its server would read freed memory. Today the lifetimes
  are nested in `main()`, so it is latent rather than live — which is precisely the kind of thing that
  should be a roadmap item instead of a comment nobody wrote. The command under "Verify afterwards" prints what is
actually enforced, which is the only answer worth trusting.

### 1.1 Two ways this configuration drifted, and the check that now stops it

Both were found by applying this document's checklist to a second repository. That is the useful part
of doing it twice: the second application is what tells you whether the first one happened.

**`sanitizers (asan)` and `sanitizers (tsan)` ran on every pull request and gated nothing.** They
arrived with roadmap item #40 and were never added to the ruleset. For the whole time they existed, a
PR could go red under AddressSanitizer and merge anyway. This is the more dangerous of the two failure
modes precisely because the job *runs*: it appears on the PR page, and a reader reasonably assumes a
visible check is a gate. They are required now.

**`.github/rulesets/master.json` did not match the live ruleset.** `analyze (c-cpp)` was added to the
live one in August 2026 and never written back into the file. That turned "rulesets as code" into a
loaded gun rather than documentation: applying the file with `PUT`, exactly as its own README
instructs, would have silently dropped the CodeQL requirement. The file is now correct and matches.

Neither of these is the sort of thing a person reliably notices, so it is checked mechanically now.
`.github/rulesets/check_contexts.py` derives the contexts the workflows will actually report — job id
or `name:`, with matrix values appended the way GitHub appends them — and compares that set against
`master.json` in **both** directions:

- a context required by the ruleset that no job produces (a permanent block: the PR waits forever)
- a context produced by a job that the ruleset does not require (the drift above, which looks green)

It runs in the `docs-integrity` job, so it is one of the required checks itself. What it cannot see is
the **live** ruleset, which needs a token that job does not have and should not; after editing
`master.json`, re-apply it with the command in `.github/rulesets/README.md` and verify with the `gh api`
call below.

### 1.2 Why `analyze (c-cpp)` is on that list

It was deliberately left off for a while, for a reason that has since gone away: `parse_cli_args()`
consumed flag values with `argv[++i]` inside a `for` loop, which CodeQL reports as
`cpp/loop-variable-changed` 29 times over. Every PR that touched the file opened a review thread, and
with `required_review_thread_resolution` on, every one of those PRs needed a manual resolution. Making
the analysis required on top of that would have meant a merge blocked by a finding nobody intended to
act on.

Roadmap #36 removed that class — an `ArgCursor` owns the index now — so the analysis is clean, and the
last three PRs went through without a CodeQL thread. Requiring it costs about 3.5 minutes per PR and
buys the guarantee that a real finding cannot be merged past by habit.

The risk is worth stating, because it has already happened once: a CodeQL **infrastructure** failure
blocks merges just as firmly as a real finding. In August 2026 Dependabot split a `codeql-action` bump
into two PRs, one for `init` and one for `analyze`, and each failed on its own with
`Loaded a configuration file for version '4.37.7', but running version '3.37.7'` — the two steps have
to move together. With the analysis required, that state blocks the queue until someone bumps both
steps in one commit (#23 did). If that happens again and the queue must move, the escape hatch is
`gh pr merge --admin`, and using it should be noted on the PR.

Verify afterwards:

```bash
# rule types on master
gh api repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/rulesets/20841774 \
  --jq '.rules[].type'
# and, more usefully, the checks a PR actually has to pass
gh api repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/rulesets/20841774 \
  --jq '.rules[] | select(.type=="required_status_checks")
         | .parameters.required_status_checks[].context'
```

### Tags ✅

A second ruleset named `release tags` targets `refs/tags/v*` and enforces `deletion` and
`non_fast_forward`. Releases are what people download; a silently rewritten tag is a supply-chain
problem.

## 2. GitHub's scanning

| Feature | State | Why |
|---------|-------|-----|
| **Secret scanning** | ✅ enabled | Catches a leaked key the moment it is pushed. Free for public repos. |
| **Push protection** | ✅ enabled | Blocks the push instead of reporting it afterwards. |
| **Dependabot alerts** | ✅ enabled | Vulnerability alerts for dependencies. |
| **Dependabot security updates** | ✅ enabled | Opens the bump PR, rather than only saying one is needed. |
| **Private vulnerability reporting** | ✅ enabled | Lets researchers report privately instead of opening a public issue — which `SECURITY.md` invites, so until now that invitation had nowhere to land. |
| **Code scanning (CodeQL)** | ✅ `codeql.yml` | Static analysis, required on `master` since 23 August 2026. |
| **Non-provider secret patterns** | ⚙️ organisation | See below. |
| **Secret scanning validity checks** | ⚙️ organisation | See below. |

The first five sat in this document as ⚙️ for weeks and were **enabled on 27 August 2026**. Worth
recording plainly rather than quietly ticking: a checklist item marked "to do" in a security document
is indistinguishable from one that is done, once enough time passes that nobody re-reads the marker.
The state above was read back from the API, not assumed from a successful call.

The last two are worth a paragraph because of *how* they fail. `PATCH /repos/{owner}/{repo}` accepts
`secret_scanning_non_provider_patterns` and `secret_scanning_validity_checks` with HTTP 200 and leaves
both `disabled`; the response body reports the old value. They are organisation-level Secret Protection
features rather than repository ones. The lesson generalises past these two settings: **a 200 from that
endpoint is not evidence that a setting changed.** Read the value back.

A leaked secret must be **rotated, not deleted**. Removing a commit does not un-leak it: GitHub keeps
unreachable objects reachable via the API for a while, and forks keep copies indefinitely.

## 3. Harden CI ✅ / ⚙️

Included in this repo:

- `.github/workflows/ci.yml` — build plus full `ctest -j1` on every push and PR, with a native etcd
- `.github/workflows/codeql.yml` — CodeQL analysis for C++
- `.github/dependabot.yml` — updates for GitHub Actions and pip

Rules that matter for workflow security:

- **Third-party actions are pinned to full commit SHAs** ✅, not tags. A tag can be moved; a SHA
  cannot. Dependabot understands this form and keeps bumping both the SHA and the version comment.
  Resolve a SHA by hand with `git ls-remote ... 'refs/tags/vX.Y.Z^{}'` — without `^{}` an
  **annotated** tag hands back the tag object rather than the commit, which for a lightweight tag is
  the same value and so goes unnoticed until it is not.
- **`github/codeql-action*` is grouped in `dependabot.yml`** ✅. Its `init` and `analyze` steps have
  to move together — mixing versions fails with "Loaded a configuration file for version X, but
  running version Y" — so a split bump gives two pull requests that each fail on their own, and with
  `analyze (c-cpp)` and `CodeQL` both required that blocks every merge until both land. This happened
  here in August 2026; the lesson was written into the SDK repository's config and **this** file, the
  one where it happened, was left without the grouping. It then happened again the same month, pull
  requests #50 and #51. Fixed where it occurs, not only where it was noticed.
- **Least-privilege `permissions:`** — `permissions: contents: read` at workflow level, widened only
  where needed (CodeQL needs `security-events: write`).
- **Never use `pull_request_target`** with a checkout of the PR head. That combination hands a fork's
  code your write token. `pull_request` is the safe trigger.
- **Do not interpolate untrusted input into `run:`** — a PR title or branch name containing `$(...)`
  becomes shell injection. Pass values through `env:` instead.
- **The repository now requires SHA pinning** ✅ (`sha_pinning_required: true` on
  `/actions/permissions`), so an unpinned action is refused rather than merely discouraged. The cost is
  worth stating: a workflow that unpins one does not fail a test, it fails to start — which reads as
  infrastructure trouble rather than as policy.
- **Default workflow permissions are read-only** ✅ and workflows cannot approve pull requests ✅.
- **Fork pull requests need approval before workflows run** ✅ — set to `all_external_contributors`,
  not GitHub's default of first-time contributors only. A second PR from the same account should not
  be exempt from review because the first one was benign.

## 4. Secrets ⚙️

- No secrets belong in this repo. There are none today. Keep it that way.
- If a workflow ever needs one (publishing to PyPI, signing releases), use **environments** with
  required reviewers rather than plain repository secrets, and prefer **OIDC trusted publishing**
  over a long-lived token.
- Before every commit that touches config, check the diff for a URL with credentials, a `.env`, or a
  key file. Push protection is a safety net, not a substitute.

## 5. Repository access ⚙️

- **Enforce 2FA** on the `Smart-Data-Engines` organisation ⚙️ (Settings → Authentication security).
  Hardware key or TOTP app, not SMS. Not reachable from the API: `PATCH /orgs/{org}` accepts
  `two_factor_requirement_enabled` with a 200 and leaves it `false` — the same trap as the two
  secret-scanning flags in §2.
- **New repositories in the organisation start protected** ✅. Every `*_enabled_for_new_repositories`
  flag was `false`, so a new repository began life with no secret scanning, no push protection and no
  Dependabot, and needed somebody to remember this document. Now enabled at the organisation level:
  secret scanning, push protection, Dependabot alerts, Dependabot security updates, dependency graph.
  Advanced Security is deliberately left off, since enabling it for new repositories is a billing
  decision rather than a hygiene one.
- Grant the minimum role: `write` for contributors, `admin` only where genuinely needed.
- Review third-party OAuth apps and installed GitHub Apps periodically. Every app with write access
  is another path into the repo.
- `.github/CODEOWNERS` ✅ makes changes to sensitive paths require the maintainer's review.

### 5.1 Verify the CODEOWNERS handle, because a wrong one is invisible

This is not a hypothetical, it is what this repository shipped. `CODEOWNERS` named `@kmacewicz` — a
real GitHub user, but not a collaborator here and not a member of the organisation — and GitHub
**silently ignores** an owner without write access. All eight lines were inert for months while the
file, and the checklist at the end of this document, both read as though the paths were covered. The
correct handle is `@krzysztof-smartdataengines`, and it was fixed on 27 August 2026.

The check takes one call, and the fully-qualified ref matters: the bare endpoint and `?ref=master`
both answer 404 on some repositories, which reads as "broken" rather than "no data".

```bash
gh api repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/codeowners/errors \
  --jq '.errors | length'
# or, if that 404s:
gh api 'repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/codeowners/errors?ref=refs/heads/master' \
  --jq '.errors'
```

An empty `errors` array is the only acceptable answer. Re-run it after adding or removing anyone.

Note that `require_code_owner_review` is `false` and `required_approving_review_count` is `0` while
there is one maintainer — a review you grant yourself is theatre. `CODEOWNERS` is therefore
documentation of what is load-bearing today, and becomes enforcement the day a second person has write
access; turn both on together at that point.

## 6. Signed commits ✅ / ⚙️

Signing proves a commit came from you, which matters for a repository strangers are asked to trust.

```bash
# SSH signing is the simplest path if you already push over SSH
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
git config --global tag.gpgsign true
```

Then add the same public key as a **signing key** in GitHub → Settings → SSH and GPG keys. A key
added as an *authentication* key does not count as a signing key. Once your commits show as verified,
enable `required_signatures` in the ruleset (§1) — doing it in the other order locks you out of your
own branch.

## 7. Supply chain of our own dependencies

The build fetches at configure time via `FetchContent`: googletest, google/benchmark, rapidcheck,
nlohmann/json. Two problems:

All four are pinned to commit SHAs ✅, with the corresponding tag in a trailing comment. Tags are
mutable upstream; a SHA is not. `rapidcheck` publishes no releases at all, so it was previously
tracking `master` — an upstream change could have altered our build without any change on our side.

When bumping a dependency, resolve the new SHA rather than writing a tag:

```bash
git ls-remote https://github.com/<owner>/<repo>.git 'refs/tags/<tag>^{}'
```

The `^{}` is not optional, and the reason belongs here because the SDK repository pinned three actions
without it and found out from Dependabot. For an **annotated** tag, `refs/tags/<tag>` answers with the
tag *object*, and the commit is one dereference further. A lightweight tag's ref is already the commit,
so both forms of the command agree — which is what makes the mistake easy to make and hard to notice.
The symptom is diagnostic: **Dependabot opens a PR bumping a version to itself**, `v4.37.9` →
`v4.37.9`, changing only the SHA. Nothing is insecure, since a tag object's SHA is content-addressed
too and names one fixed commit, but the pin is not the object the trailing comment claims.
`github/codeql-action` uses annotated tags; `actions/*` use lightweight ones.

System libraries (`liblz4`, `libcurl`, `liburing`) and `etcd` come from the OS or an official release
tarball and are covered by the normal update path. Document the versions we test against so a
reviewer can reproduce our build.

## 8. What a reader of this repo should also find

The following make the security posture legible to anyone evaluating the project:

- `SECURITY.md` ✅ — how to report a vulnerability and what response to expect
- `LICENSE` ✅ — Apache 2.0
- `CONTRIBUTING.md` ✅
- A green CI badge in the README ✅ — evidence the tests run, not just that they exist

## 9. Threat model in one paragraph

This repo holds no secrets and no customer data, so the realistic threats are: (a) someone pushes
malicious or broken code and we ship it in a release, (b) a dependency is compromised and lands in
our binaries, (c) we accidentally commit a credential from a client engagement, (d) a leaked
maintainer token is used to rewrite history or publish a fake release. Required status checks on a
protected branch handle (a), pinned dependencies handle (b), secret scanning with push protection
handles (c), and 2FA with signed commits and tag protection handle (d).

## Checklist

```
✅ ci.yml with full test run on PR
✅ codeql.yml
✅ dependabot.yml
✅ SECURITY.md
✅ CODEOWNERS
✅ pull_request_template.md
✅ branch ruleset on master: PR required, no force push, no deletion, no bypass actors
✅ ruleset: required_status_checks — seven contexts, strict (incl. CodeQL, the findings gate)
✅ ruleset: required_linear_history, review thread resolution
✅ tag ruleset on refs/tags/v*
✅ secret scanning + push protection
✅ Dependabot alerts + security updates
✅ private vulnerability reporting
✅ Actions: read-only default token, cannot approve PRs
✅ Actions: fork PR approval required for all external contributors
✅ Actions: SHA pinning required at the repository level
✅ CODEOWNERS handle verified against /codeowners/errors — it was wrong, see §5.1
✅ check_contexts.py in docs-integrity — the ruleset and the workflows cannot drift apart silently
✅ ruleset: sanitizers (asan) and sanitizers (tsan) required — they ran and gated nothing, see §1.1
✅ ruleset: sanitizers-integration (tsan) required — added with #80, the first CI job to run the
   pytest suite, and the one that caught the lock-order inversion
✅ ruleset: integration-tests required — added with #55, the whole suite against a plain build, so
   failover and crash recovery are gated rather than only run locally
✅ ruleset: clang-build required — added with #37, because the README claimed Clang support that
   nothing checked
✅ ruleset: coverage required — added with #37; gates the instrumentation reaching the libraries, not
   a percentage, until a measured baseline exists
✅ .github/rulesets/master.json matches the live ruleset again, see §1.1
✅ the 30 vendored CodeQL alerts dismissed with a reason — paths-ignore does not work here, see §1
⚙️ triage the two own-code CodeQL alerts recorded in §1; the other thirteen are note-level tidiness
⚙️ decide the query suite: security-and-quality keeps 44 unactioned alerts, see §1
✅ all FetchContent dependencies pinned to commit SHAs
✅ third-party actions pinned to commit SHAs
✅ organisation defaults for new repositories: scanning, push protection, Dependabot, dep graph
⚙️ org-wide 2FA on Smart-Data-Engines — UI only, the API reports success and changes nothing
⚙️ SSH/GPG signing key registered as a *signing* key, then required_signatures in the ruleset
⚙️ non-provider secret patterns + validity checks (organisation-level Secret Protection)
```
