#!/usr/bin/env python3
"""Verify the shipped dashboard and alert rules against the metrics the engine exports (#35).

A dashboard panel or an alert over a metric the engine does not export shows a flat line and never
fires, and nothing says so: the same failure as the five dead gauges #35 found before it could ship
a dashboard at all - registered under one name, written under another, zero for ever. So:

* every `ob_*` name in `packaging/grafana/*.json` and `packaging/prometheus/*.yml` is registered in
  `src/metrics.cpp` (a histogram's `_bucket`, `_sum` and `_count` count as it), or is `ob_build_info`,
  which `MetricsRegistry::serialize()` writes itself;
* `rate()` and `increase()` take counters and histogram buckets only - of a gauge they are a slope of
  something that is not a count, which reads as a rate and is not one;
* every alert has a `severity` label and a `summary` and a `description`, and every panel a title
  and a query;
* and, with promtool (`PROMTOOL`, or on PATH), every dashboard query parses: they are handed to
  `promtool check rules` as recording rules, the dashboard's variables replaced by a match-all.
  scripts/check_alert_rules.py tests the alerts themselves.

Exit status 0 if clean, 1 otherwise. Needs PyYAML for the rules.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
REGISTRY = REPO / "src" / "metrics.cpp"
KIND = re.compile(r'make_(counter|gauge|histogram)\(\s*"([^"]+)"')
NAME = re.compile(r"\bob_[a-z0-9_]+\b")
TAKES_A_COUNTER = re.compile(r"\b(?:rate|increase|irate)\(\s*(ob_[a-z0-9_]+)")


def registered() -> dict[str, str]:
    kinds = {name: kind for kind, name in KIND.findall(REGISTRY.read_text())}
    kinds["ob_build_info"] = "gauge"
    return kinds


def exported_as(name: str, kinds: dict[str, str]) -> tuple[str | None, str | None]:
    """(the registered name, its kind) for a name a query uses, or (None, None)."""
    if name in kinds:
        return name, kinds[name]
    for suffix in ("_bucket", "_sum", "_count"):
        base = name[: -len(suffix)]
        if name.endswith(suffix) and kinds.get(base) == "histogram":
            return base, "counter" if suffix != "_sum" else "counter"
    return None, None


def check_expr(where: str, expr: str, kinds: dict[str, str], errors: list[str]) -> None:
    for name in NAME.findall(expr):
        base, _ = exported_as(name, kinds)
        if base is None:
            errors.append(f"{where}: {name} is not a metric the engine exports")
    for name in TAKES_A_COUNTER.findall(expr):
        base, kind = exported_as(name, kinds)
        if base is not None and kind == "gauge":
            errors.append(f"{where}: rate()/increase() of the gauge {name}")


def exprs_of(node, path=""):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "expr" and isinstance(v, str):
                yield path, v
            else:
                yield from exprs_of(v, f"{path}/{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from exprs_of(v, f"{path}[{i}]")


def main() -> int:
    kinds = registered()
    errors: list[str] = []
    dashboards = sorted((REPO / "packaging" / "grafana").glob("*.json"))
    rules = sorted((REPO / "packaging" / "prometheus").glob("*.yml"))
    if not dashboards or not rules:
        errors.append("no dashboard or no rules under packaging/ - nothing checked")
    n_exprs = 0
    for path in dashboards:
        doc = json.loads(path.read_text())
        for panel in doc.get("panels", []):
            if panel.get("type") == "row":
                continue
            if not panel.get("title"):
                errors.append(f"{path.name}: a panel with no title")
            if not [t for t in panel.get("targets", []) if t.get("expr")]:
                errors.append(f"{path.name}: panel '{panel.get('title')}' has no query")
        for where, expr in exprs_of(doc):
            n_exprs += 1
            check_expr(f"{path.name}{where}", expr, kinds, errors)
    n_alerts = 0
    for path in rules:
        doc = yaml.safe_load(path.read_text())
        for group in doc.get("groups", []):
            for rule in group.get("rules", []):
                n_alerts += 1
                name = rule.get("alert", "(unnamed)")
                if not rule.get("labels", {}).get("severity"):
                    errors.append(f"{path.name}: {name} has no severity")
                notes = rule.get("annotations", {})
                if not notes.get("summary") or not notes.get("description"):
                    errors.append(f"{path.name}: {name} lacks a summary or a description")
                check_expr(f"{path.name}: {name}", rule.get("expr", ""), kinds, errors)
    parsed = ""
    promtool = os.environ.get("PROMTOOL") or shutil.which("promtool")
    if promtool:
        queries = [e for path in dashboards for _, e in exprs_of(json.loads(path.read_text()))]
        recording = {"groups": [{"name": "dashboard-queries", "rules": [
            {"record": f"dashboard:query{i}", "expr": q.replace("$job", ".*").replace("$instance", ".*")}
            for i, q in enumerate(queries)]}]}
        with tempfile.TemporaryDirectory() as tmp:
            rules_file = Path(tmp) / "dashboard-queries.yml"
            rules_file.write_text(yaml.safe_dump(recording))
            out = subprocess.run([promtool, "check", "rules", str(rules_file)], capture_output=True,
                                 text=True)
        if out.returncode != 0:
            errors.append("a dashboard query does not parse:\n" + out.stdout + out.stderr)
        else:
            parsed = f", and all {len(queries)} queries parse"
    else:
        print("promtool not found: the dashboard's queries are not parsed")
    for e in errors:
        print(e)
    if errors:
        return 1
    print(f"{len(dashboards)} dashboard(s) with {n_exprs} queries and {len(rules)} rule file(s) with "
          f"{n_alerts} alerts: every metric is one the engine exports, no gauge is taken a rate of"
          f"{parsed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
