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

### What is missing ⚙️

**A required status check.** Today a PR can be merged while CI is failing, or before CI has even
run. This is the single most valuable rule in the whole document: it is what makes "the tests pass on
master" a fact rather than a habit. Add `required_status_checks` with the `build-and-test` context
from `ci.yml`, and `strict: true` so a stale branch has to be updated before merging.

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
          { "context": "release-build" }
        ]
      }
    }
  ]
}
JSON
```

Verify afterwards:

```bash
gh api repos/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/rulesets/20841774 \
  --jq '.rules[].type'
```

### Tags ⚙️

Add a second ruleset with `target: tag` and condition `refs/tags/v*`, enforcing `deletion` and
`non_fast_forward`. Releases are what people download; a silently rewritten tag is a supply-chain
problem.

## 2. Turn on GitHub's scanning ⚙️

Settings → Code security and analysis:

| Feature | Why |
|---------|-----|
| **Secret scanning** | Catches a leaked key the moment it is pushed. Free for public repos. |
| **Push protection** | Blocks the push instead of reporting it afterwards. Turn this on. |
| **Dependabot alerts** + **security updates** | Vulnerability alerts for dependencies. |
| **Private vulnerability reporting** | Lets researchers report privately instead of opening a public issue. |
| **Code scanning (CodeQL)** | Static analysis. Configured in `codeql.yml` ✅ |

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
- ⚙️ Settings → Actions → General: default workflow permissions **read-only**, and **require approval
  for all outside collaborators** before workflows run on their PRs.

## 4. Secrets ⚙️

- No secrets belong in this repo. There are none today. Keep it that way.
- If a workflow ever needs one (publishing to PyPI, signing releases), use **environments** with
  required reviewers rather than plain repository secrets, and prefer **OIDC trusted publishing**
  over a long-lived token.
- Before every commit that touches config, check the diff for a URL with credentials, a `.env`, or a
  key file. Push protection is a safety net, not a substitute.

## 5. Repository access ⚙️

- **Enforce 2FA** on the `Smart-Data-Engines` organisation (Settings → Authentication security).
  Hardware key or TOTP app, not SMS.
- Grant the minimum role: `write` for contributors, `admin` only where genuinely needed.
- Review third-party OAuth apps and installed GitHub Apps periodically. Every app with write access
  is another path into the repo.
- `.github/CODEOWNERS` ✅ makes changes to sensitive paths require the maintainer's review. Note that
  an unknown handle in CODEOWNERS is silently ignored by GitHub, so verify it resolves.

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
⚙️ ruleset: add required_status_checks (build-and-test) — the important gap
⚙️ ruleset: add required_signatures, required_linear_history, review thread resolution
⚙️ tag ruleset on v*
⚙️ secret scanning + push protection
⚙️ Dependabot alerts + security updates
⚙️ private vulnerability reporting
⚙️ Actions: read-only default token, approval required for outside collaborators
⚙️ org-wide 2FA
⚙️ SSH/GPG signing key registered as a signing key (before requiring signatures)
⚙️ verify the CODEOWNERS handle resolves
✅ all FetchContent dependencies pinned to commit SHAs
✅ third-party actions pinned to commit SHAs
```
