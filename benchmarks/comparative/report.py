"""The run's output as an artefact, and the two keys it refuses to be written without.

Requirement 6.2 makes a results file immutable: a second run writes a second file. Overwriting
removes the only thing history is kept for, which is seeing whether a number moved.

Requirement 5.1 is the uncomfortable one, and it is enforced here rather than trusted: a table where
we win everywhere reads as selected, and usually is. So `losses` is a required key, and an empty one
has to be accompanied by a sentence describing how a loss was looked for. "We could not find one" is
publishable; silence is not.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any


class IncompleteReport(RuntimeError):
    """A report missing the parts that make it honest is not written at all."""


@dataclass
class Report:
    run: dict[str, Any]
    hardware: dict[str, Any]
    dataset: dict[str, Any]
    resolution: dict[str, Any]
    systems: list[dict[str, Any]] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    losses: list[str] = field(default_factory=list)
    losses_search: str = ""

    def validate(self) -> None:
        if not self.limitations:
            raise IncompleteReport(
                "limitations is empty: this engine is specialised and the comparison is uneven in "
                "our favour, so what it cannot do belongs in the table. Without it a number "
                "promises a replacement")
        if not self.losses and not self.losses_search.strip():
            raise IncompleteReport(
                "losses is empty and nothing says how a loss was looked for. 'We looked and found "
                "none, here is how' is publishable; an empty list on its own is a selected table")
        if not self.systems:
            raise IncompleteReport("no systems in the report, so there is nothing to publish")

    def as_dict(self) -> dict[str, Any]:
        return {
            "run": self.run,
            "hardware": self.hardware,
            "dataset": self.dataset,
            "resolution": self.resolution,
            "systems": self.systems,
            "limitations": self.limitations,
            "losses": self.losses,
            "losses_search": self.losses_search,
        }


def _unused_path(directory: Path, stem: str) -> Path:
    """`<stem>.json`, or `<stem>-2.json`, and so on. Never the one that exists.

    Immutability by refusing the name rather than by checking a flag: a caller that forgets the flag
    still cannot overwrite, and the second run of a day is visibly a second run.
    """
    candidate = directory / f"{stem}.json"
    suffix = 2
    while candidate.exists():
        candidate = directory / f"{stem}-{suffix}.json"
        suffix += 1
    return candidate


def write(report: Report, directory: Path, hardware_digest: str) -> Path:
    report.validate()
    directory.mkdir(parents=True, exist_ok=True)
    path = _unused_path(directory, f"{date.today().isoformat()}-{hardware_digest}")
    path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (path.with_suffix(".md")).write_text(to_markdown(report), encoding="utf-8")
    return path


def to_markdown(report: Report) -> str:
    """The table a reader sees, with the floor in the header rather than in a footnote.

    A benchmark that does not say what it cannot separate claims that every difference it shows is
    real. So the floor sits above the numbers, and `NOT MEASURED` rows stay in the body: a system
    left out as a blank cell reads as a system that lost.
    """
    lines: list[str] = []
    hw = report.hardware
    lines.append(f"### Comparative benchmark — {report.run.get('timestamp', 'unknown date')}")
    lines.append("")
    lines.append(f"**Hardware:** {hw.get('cpu_model')}, {hw.get('cores')} cores, "
                 f"{hw.get('ram_mib')} MiB RAM, {hw.get('disk_model')} "
                 f"({'rotational' if hw.get('disk_rotational') else 'solid state'}), "
                 f"{hw.get('filesystem')}, kernel {hw.get('kernel')}, {hw.get('compiler')}")
    # The layers between the filesystem and the platter, because encryption costs I/O and a reader
    # comparing their hardware to ours needs to know it was in the path.
    lines.append(f"**Storage path:** {hw.get('disk_stack')}")
    ds = report.dataset
    lines.append(f"**Dataset:** {ds.get('rows')} rows, {ds.get('symbols')} symbols, "
                 f"{ds.get('levels')} levels, seed {ds.get('seed')}, "
                 f"sha256 `{str(ds.get('sha256'))[:16]}…`")
    res = report.resolution
    lines.append(f"**Resolution:** control floor {res.get('floor', 0):.4f} over "
                 f"{res.get('rounds')} interleaved rounds after {res.get('warmup')} warm-up calls — differences smaller than this are "
                 f"reported as indistinguishable, not as a win. Verdict: {res.get('verdict')}")
    lines.append("")
    lines.append("| System | Version | Ingest (rows/s) | Time-range (s) | VWAP (s) | Notes |")
    lines.append("|--------|---------|-----------------|----------------|----------|-------|")
    for system in report.systems:
        if not system.get("available", True):
            lines.append(f"| {system['name']} | — | NOT MEASURED | NOT MEASURED | NOT MEASURED "
                         f"| {system.get('reason', 'unavailable')} |")
            continue
        workloads = system.get("workloads", {})

        def cell(key: str) -> str:
            entry = workloads.get(key)
            if entry is None:
                return "—"
            if "note" in entry:
                return entry["note"]
            return f"{entry.get('value', 0):.4f}"

        lines.append(f"| {system['name']} | {system.get('version', '?')} | "
                     f"{cell('ingest')} | {cell('time_range')} | {cell('vwap')} | "
                     f"{'; '.join(system.get('tuning_applied', [])) or '—'} |")
    lines.append("")
    lines.append("**What this engine cannot do**, because the comparison is uneven in its favour:")
    for limitation in report.limitations:
        lines.append(f"- {limitation}")
    lines.append("")
    if report.losses:
        lines.append("**Where it loses:**")
        for loss in report.losses:
            lines.append(f"- {loss}")
    else:
        lines.append(f"**Where it loses:** none found. {report.losses_search}")
    return "\n".join(lines) + "\n"
