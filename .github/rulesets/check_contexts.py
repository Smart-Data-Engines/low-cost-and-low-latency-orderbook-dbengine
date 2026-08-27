#!/usr/bin/env python3
"""Refuse a ruleset that has drifted from the workflows it is supposed to gate.

This exists because the drift already happened here, twice, and both times it looked like green:

- **`sanitizers (asan)` and `sanitizers (tsan)` ran on every pull request and gated nothing.** They
  arrived with roadmap item #40 and nobody added them to the ruleset, so for the whole time they
  existed a PR could go red under ASan and merge anyway. A job nothing requires is the more dangerous
  of the two failure modes precisely because it *runs*: the PR page shows it, and a reader assumes a
  visible check is a gate.
- **`master.json` in this directory did not match the live ruleset.** `analyze (c-cpp)` was added to
  the live one in August 2026 and never written back into the file, which turned "rulesets as code"
  into a loaded gun: applying this file with PUT, as its own README instructs, would have silently
  dropped the CodeQL requirement.

The other direction is worse when it happens, though it has not happened here yet: **a required
context that no job produces is a permanent block, not an error.** Nothing fails - the pull request
simply waits for a check that will never report. A renamed job does it, and so does writing `python`
where the context is `python (3.11)`.

So this compares the contexts the workflows actually produce against the contexts `master.json`
requires, in both directions, and fails on any difference. What it cannot see is the live ruleset on
GitHub - that needs a token this job does not have, and should not. It checks the half that drifts when
somebody edits a workflow, which is the half that drifted.

Run from the repository root:

    python3 .github/rulesets/check_contexts.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

WORKFLOWS = Path(".github/workflows")
RULESET = Path(".github/rulesets/master.json")

# Contexts required by the ruleset that no workflow in this repository produces, with the reason. Each
# entry is a deliberate exception and has to stay short: an allowlist that grows is this check being
# switched off one line at a time.
NOT_FROM_A_WORKFLOW: dict[str, str] = {
    # Empty, and that is the correct state today: every context this repository requires is produced
    # by a job in .github/workflows/. An entry here is a deliberate exception for a check posted by a
    # GitHub integration rather than by us, and the list has to stay short - an allowlist that grows is
    # this check being switched off one line at a time.
}

_MATRIX_REF = re.compile(r"\$\{\{\s*matrix\.([A-Za-z_][A-Za-z0-9_-]*)\s*\}\}")


def triggers_on_pull_request(workflow: dict[str, Any]) -> bool:
    """Only a job that runs on pull requests can gate one.

    `on` is the YAML 1.1 boolean `True` after parsing, not the string "on" - which is the kind of
    detail that makes a checker silently pass by finding nothing to check, so both spellings are read
    and a workflow with neither is an error rather than a skip.
    """
    for key in ("on", True):
        if key in workflow:
            on = workflow[key]
            if isinstance(on, str):
                return on == "pull_request"
            if isinstance(on, list):
                return "pull_request" in on
            if isinstance(on, dict):
                return "pull_request" in on
    raise SystemExit(f"a workflow has no `on:` trigger at all: {workflow.get('name')!r}")


def contexts_of(job_id: str, job: dict[str, Any]) -> list[str]:
    """The status check names GitHub will report for one job.

    Three rules, and the second is the one that surprises people: GitHub uses the job's `name:` when
    it has one and the job *id* when it does not, and for a matrix job it appends the matrix values in
    parentheses unless `name:` already interpolates them itself.
    """
    label = job.get("name", job_id)
    matrix = (job.get("strategy") or {}).get("matrix") or {}
    axes = {k: v for k, v in matrix.items() if isinstance(v, list)}

    if not axes:
        if _MATRIX_REF.search(str(label)):
            raise SystemExit(
                f"job {job_id!r} interpolates a matrix value into its name but declares no matrix "
                f"axis. Its context name cannot be predicted, so this checker would silently pass."
            )
        return [str(label)]

    referenced = set(_MATRIX_REF.findall(str(label)))
    if referenced:
        # `name: analyze (${{ matrix.language }})` - GitHub substitutes and adds nothing.
        unknown = referenced - set(axes)
        if unknown:
            raise SystemExit(f"job {job_id!r} refers to matrix axes that do not exist: {unknown}")
        if len(axes) != 1:
            raise SystemExit(
                f"job {job_id!r} has {len(axes)} matrix axes and an interpolated name. This checker "
                f"only knows how to predict the single-axis case; teach it before adding the second."
            )
        (axis,) = axes
        return [_MATRIX_REF.sub(lambda _: str(v), str(label)) for v in axes[axis]]

    if len(axes) != 1:
        raise SystemExit(
            f"job {job_id!r} has {len(axes)} matrix axes. GitHub joins the values with ', ' and this "
            f"checker only implements the single-axis case; teach it before adding the second."
        )
    (axis,) = axes
    return [f"{label} ({v})" for v in axes[axis]]


def produced() -> dict[str, str]:
    """Every context a pull request in this repository will see, mapped to its workflow file."""
    found: dict[str, str] = {}
    files = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))
    if not files:
        raise SystemExit(f"no workflows found under {WORKFLOWS}. Nothing to check, which is a bug.")
    for path in files:
        workflow = yaml.safe_load(path.read_text())
        if not triggers_on_pull_request(workflow):
            continue
        for job_id, job in (workflow.get("jobs") or {}).items():
            for context in contexts_of(job_id, job):
                if context in found:
                    raise SystemExit(
                        f"two jobs report the same context {context!r} ({found[context]} and "
                        f"{path.name}). One of them would satisfy a rule meant for the other."
                    )
                found[context] = path.name
    return found


def required() -> set[str]:
    ruleset = json.loads(RULESET.read_text())
    for rule in ruleset["rules"]:
        if rule["type"] == "required_status_checks":
            return {c["context"] for c in rule["parameters"]["required_status_checks"]}
    raise SystemExit(
        f"{RULESET} has no required_status_checks rule. Either protection was removed or this "
        f"checker is pointed at the wrong file; both are worth failing on."
    )


def main() -> int:
    from_workflows = produced()
    from_ruleset = required()

    problems: list[str] = []

    for context in sorted(from_ruleset - set(from_workflows) - set(NOT_FROM_A_WORKFLOW)):
        problems.append(
            f"  required but produced by nothing: {context!r}\n"
            f"    A pull request waits for this forever. Either a job was renamed, or the context "
            f"string is wrong - remember a matrix job is 'python (3.11)', not 'python'."
        )

    for context in sorted(set(from_workflows) - from_ruleset):
        problems.append(
            f"  produced but not required: {context!r}  (from {from_workflows[context]})\n"
            f"    It runs on every pull request and gates nothing, which looks like coverage. Add it "
            f"to {RULESET} and re-apply the ruleset, or delete the job."
        )

    for context, why in sorted(NOT_FROM_A_WORKFLOW.items()):
        if context not in from_ruleset:
            problems.append(
                f"  allowlisted but no longer required: {context!r} ({why})\n"
                f"    The exception in this script is now dead configuration. Remove it, or put the "
                f"context back in {RULESET}."
            )

    if problems:
        print("the ruleset and the workflows disagree:\n")
        print("\n".join(problems))
        return 1

    print(
        f"{len(from_workflows)} contexts produced, all required; "
        f"{len(NOT_FROM_A_WORKFLOW)} allowlisted as not coming from a workflow"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
