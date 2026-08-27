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

The list above is **eight** checks as of 27 August 2026, not the four the JSON block shows — see
§1.1, which is about how it came to be wrong. The eighth is `sanitizers-integration (tsan)`, added
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
