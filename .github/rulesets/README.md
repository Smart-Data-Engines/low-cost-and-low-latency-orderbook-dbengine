# Rulesets as code

The GitHub rulesets protecting this repository, kept in version control so the configuration is
reviewable and reproducible rather than living only in the web UI.

These files are **not applied automatically**. GitHub does not read them; they are inputs for the
`gh` commands below.

## Apply

```bash
REPO=Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine

# Update the existing branch ruleset (PUT replaces it wholesale)
gh api -X PUT "repos/$REPO/rulesets/20841774" --input .github/rulesets/master.json

# Create the tag ruleset (only once; afterwards use PUT with its id)
gh api -X POST "repos/$REPO/rulesets" --input .github/rulesets/tags.json
```

## Verify

```bash
gh api "repos/$REPO/rulesets" --jq '.[] | {id, name, target, enforcement}'
gh api "repos/$REPO/rulesets/20841774" --jq '{rules: [.rules[].type], bypass: .bypass_actors}'
```

## Pushing a history rewrite

Rewriting published history needs the ruleset out of the way, and taking rules out one at a time
does not work: `non_fast_forward` blocks the force push, `required_linear_history` rejects any push
containing a merge commit, `pull_request` blocks every direct push to `master`, and
`required_status_checks` rejects SHAs that have never been checked. The only reliable path is to flip
`enforcement` to `disabled` for the duration.

No "protection off" file is kept in this directory on purpose — a ready-made switch invites casual
use. Build it on the spot, and always restore protection in the same command so a failed push cannot
leave the branch exposed:

```bash
R=Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine
jq '.enforcement = "disabled"' .github/rulesets/master.json > /tmp/off.json

gh api -X PUT "repos/$R/rulesets/20841774" --input /tmp/off.json --jq '.enforcement'
git push --force-with-lease=master:<current-sha> origin <branch>:master
gh api -X PUT "repos/$R/rulesets/20841774" --input .github/rulesets/master.json --jq '.enforcement'
```

Use `--force-with-lease` with an explicit SHA, never a bare `--force`: it is what stops the push when
someone else has merged in the meantime, which is exactly what happened the first time this was done
here (two Dependabot PRs had landed).

Afterwards, verify the result rather than assuming it:

```bash
gh api "repos/$R/rulesets/20841774" --jq '{enforcement, rules: [.rules[].type], bypass: .bypass_actors}'
git log origin/master --format='%ae' | sort -u
```

Note that `GET /contributors` is cached and keeps showing the old author for a while after a rewrite.
The commits themselves are the source of truth.

## Notes

- `bypass_actors` is deliberately empty. The rules exist to catch the maintainer's own mistakes, so
  granting yourself a bypass defeats them.
- `required_status_checks` contexts must have reported to GitHub at least once before they can be
  enforced, so let `ci.yml` run before applying.
- `required_signatures` is intentionally absent from `master.json`. Add it only after registering an
  SSH or GPG **signing** key on your GitHub account and confirming a commit shows as Verified.
  Enabling it first locks you out of your own branch.
- `required_linear_history` and `allowed_merge_methods` are consistent: squash and rebase preserve a
  linear history, a merge commit does not. Change both together or neither.
