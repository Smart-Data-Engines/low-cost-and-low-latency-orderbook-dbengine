#!/usr/bin/env python3
"""Compare the compiled form of named functions between two Release builds.

Why this exists rather than a wall-clock number: the benchmark suite measures `apply_delta` and
`WALWriter::append`, and a change to the network write path does not touch either. Series C promised
to prove "the plaintext path pays nothing" with `BM_IngestionThroughput`, which cannot answer it -
and the machine this engine is developed on has produced a 40.6% swing in 8 of 8 rounds for a
function that did not change. Disassembling the path that did change is a decision, not a debate.

What it compares, in this order:

1. **The mnemonic sequence** - the instructions, without their operands. Two builds that produce the
   same sequence do the same work; the addresses differ and always will.
2. **The operand text**, with call targets, `# comment` annotations and `%rip`-relative
   displacements normalised away. Reported separately so that "same instructions, different
   constant" cannot hide inside a match.

Symbols are matched on the **exact** demangled signature. Series C measured this by substring and
summed `flush_output` with `flush_output_tls`, reporting 310 and then 335 instructions for a function
that has 148 - which nearly bought a second rewrite of code that had not regressed. A symbol found
zero times or more than once is an error here, not a sum.

Usage:

    scripts/mnemonic_diff.py --base /tmp/ob_master_wt/build-release --head build-release \\
        --symbol 'ob::Engine::apply_delta(ob::DeltaUpdate const&, ob::Level const*)' \\
        --symbol 'ob::WALWriter::append(ob::DeltaUpdate const&, ob::Level const*)'

Both directories must be Release builds of the same targets. Exit status is 1 if any symbol was not
found exactly once in both, or if any mnemonic sequence differs.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

LABEL_RE = re.compile(r"^[0-9a-f]+ <(.+)>:$")
# Instruction lines are address, raw bytes, text - tab separated. A long instruction wraps onto a
# continuation line carrying only bytes, which is why the text field is required rather than assumed.
# `call 5347 <ob::Foo::bar()>` - the address is section-relative and moves whenever anything ahead
# of the function moves, so the address and the target it annotates are normalised together. Leaving
# the number in made every call site in every function read as a difference.
CALL_TARGET_RE = re.compile(r"[0-9a-f]+ <[^>]*>")
RIP_RE = re.compile(r"-?0x[0-9a-f]+\(%rip\)")


def disassemble(path: pathlib.Path) -> dict[str, list[list[str]]]:
    """Every function in one object or archive: demangled name -> list of instruction lists."""
    out = subprocess.run(
        ["objdump", "-d", "--demangle", str(path)],
        check=True, capture_output=True, text=True).stdout

    functions: dict[str, list[list[str]]] = {}
    current: list[str] | None = None
    for line in out.splitlines():
        label = LABEL_RE.match(line)
        if label:
            current = []
            functions.setdefault(label.group(1), []).append(current)
            continue
        if current is None or not line.startswith(" "):
            continue
        fields = line.split("\t")
        if len(fields) < 3:
            continue
        current.append(fields[2].strip())
    return functions


# Padding, not work. Alignment NOPs move whenever code layout moves, which is the thing this tool
# exists to see through: without this filter `broadcast` reported 173 against 170 instructions and
# the first difference was `nopl` against `cs nopw`. A NOP is the one instruction that provably
# changes nothing, so dropping it cannot hide a change in behaviour.
PADDING = ("nop", "nopl", "nopw", "nopb", "cs", "data16", "xchg")


def mnemonics(instructions: list[str], keep_padding: bool = False) -> list[str]:
    out = []
    for text in instructions:
        if not text:
            continue
        mnemonic = text.split(maxsplit=1)[0]
        if not keep_padding and mnemonic in PADDING:
            # `xchg %ax,%ax` is the two-byte NOP; a real xchg has different operands.
            if mnemonic != "xchg" or "%ax,%ax" in text:
                continue
        out.append(mnemonic)
    return out


def operands(instructions: list[str]) -> list[str]:
    """Instruction text with everything that legitimately moves between builds removed."""
    normalised = []
    for text in instructions:
        if not text:
            continue
        mnemonic = text.split(maxsplit=1)[0]
        if mnemonic in PADDING and (mnemonic != "xchg" or "%ax,%ax" in text):
            continue
        text = text.split("#", 1)[0].strip()
        text = CALL_TARGET_RE.sub("<T>", text)
        text = RIP_RE.sub("RIP", text)
        normalised.append(text)
    return normalised


def collect(build_dir: pathlib.Path, wanted: list[str]) -> dict[str, list[list[str]]]:
    """Exact matches, plus any GCC clone of a wanted symbol as a row of its own.

    A clone is named `<signature> [clone .cold]` or `[clone .isra.0]`, so this is a prefix match on
    the signature followed by a literal ` [clone `, which no other function's name can produce. It
    matters because `.cold` carries the unlikely paths: a change that only adds a cold branch leaves
    the hot count identical, and a tool that silently ignored the clone would report "identical" for
    a function that grew.
    """
    found: dict[str, list[list[str]]] = {name: [] for name in wanted}
    for archive in sorted(build_dir.rglob("*.a")):
        for name, bodies in disassemble(archive).items():
            if name in found:
                found[name].extend(bodies)
                continue
            for want in wanted:
                if name.startswith(want + " [clone "):
                    found.setdefault(name, []).extend(bodies)
                    break
    return found


def first_difference(left: list[str], right: list[str]) -> int:
    for index, (a, b) in enumerate(zip(left, right)):
        if a != b:
            return index
    return min(len(left), len(right))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True, type=pathlib.Path)
    parser.add_argument("--head", required=True, type=pathlib.Path)
    parser.add_argument("--symbol", required=True, action="append", dest="symbols",
                        help="exact demangled signature, or 'base signature => head signature' "
                             "when the function was renamed or split; repeatable")
    parser.add_argument("--show", type=int, default=0, metavar="N",
                        help="print the first N differing instructions per symbol")
    args = parser.parse_args()

    # `A => B` compares two differently named functions - the case this branch needed, where one
    # function became a dispatcher and a `_plain` body. Without it the only available comparison is
    # "89 instructions against 7", which is true and says nothing.
    pairs = []
    for entry in args.symbols:
        if " => " in entry:
            left, right = entry.split(" => ", 1)
            pairs.append((left.strip(), right.strip()))
        else:
            pairs.append((entry, entry))

    base = collect(args.base, [left for left, _ in pairs])
    head = collect(args.head, [right for _, right in pairs])

    # Requested signatures first, then whatever clones either build produced for them.
    rows = list(pairs)
    for name in sorted(set(base) - {left for left, _ in pairs}):
        rows.append((name, name))
    for name in sorted(set(head) - {right for _, right in pairs} - {n for n, _ in rows}):
        rows.append((name, name))
    for left, right in rows:
        base.setdefault(left, [])
        head.setdefault(right, [])

    problems = 0
    print(f"{'symbol':<62} {'base':>6} {'head':>6}  verdict")
    print("-" * 96)
    for left, right in rows:
        # Truncated in the middle, not at the end: the tail is where `[clone .cold]` lives, and
        # two rows that differ only there have to be told apart.
        name = left if left == right else f"{left.split('(')[0]} => {right.split('(')[0]}"
        short = name if len(name) <= 60 else name[:31] + "..." + name[-26:]
        for side, table, key in (("base", base, left), ("head", head, right)):
            if len(table[key]) != 1:
                print(f"{short:<62} {len(table[key])} definitions in {side} - not a measurement")
                problems += 1
        if len(base[left]) != 1 or len(head[right]) != 1:
            continue

        b_mn, h_mn = mnemonics(base[left][0]), mnemonics(head[right][0])
        b_op, h_op = operands(base[left][0]), operands(head[right][0])
        if b_mn != h_mn:
            at = first_difference(b_mn, h_mn)
            verdict = f"DIFFERS at #{at}: {b_mn[at:at+1]} -> {h_mn[at:at+1]}"
            problems += 1
        else:
            differing = sum(1 for a, b in zip(b_op, h_op) if a != b)
            verdict = ("identical" if differing == 0
                       else f"same instructions, {differing} operand(s) differ")
        print(f"{short:<62} {len(b_mn):>6} {len(h_mn):>6}  {verdict}")

        shown = 0
        for index, (a, b) in enumerate(zip(b_op, h_op)):
            if shown >= args.show:
                break
            if a != b:
                print(f"{'':<62} #{index:<5} {a}   ->   {b}")
                shown += 1

    print()
    print("Mnemonic sequences only; operands are compared with call targets, `#` comments and "
          "%rip displacements normalised.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
