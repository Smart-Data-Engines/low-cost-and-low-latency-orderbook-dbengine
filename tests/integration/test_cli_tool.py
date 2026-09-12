"""The interactive CLI refuses what it does not understand (#110).

Nothing in this repository exercised `ob_cli` before this file — `test_cli_args.py` and
`test_cli_config.py` are about the *server's* flags — which is why three silences lived in the one
tool a human types into. Measured before the fix, by running it:

```
ob> insert AAA EX sideways 6500000 1500
OK  seq=1  sideways AAA@EX  price=6500000 qty=1500      <- echoed back, stored as a BID
ob> insert BBB EX bid 6400000 1400 1 1700000000000000000
OK  seq=2  bid BBB@EX  ...                              <- the last token discarded
$ ob_cli --data-dir /tmp/x
Data directory: --data-dir                              <- a store in a directory of that name
```

The first is the sharpest: `side_str == "ask" || side_str == "ASK" ? ASK : BID`, written three times,
makes **every** other word a bid — and the confirmation repeated the typo as though it had been
understood, which is worse than silence because that line is where a human checks.

The CLI embeds the engine rather than speaking the wire protocol, so #107's refusal does not reach
it and this is a separate surface with its own tests.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

CLI = Path(__file__).resolve().parents[2] / "build" / "ob_cli"

WIDEST = "WHERE timestamp BETWEEN 0 AND 9999999999999999999"


def run_cli(script: str, data_dir: str | None = None) -> str:
    """Drive the tool with a script on stdin and hand back everything it said."""
    if not CLI.is_file():
        pytest.skip(f"{CLI} is not built")
    with tempfile.TemporaryDirectory(prefix="ob_cli_test_") as tmp:
        proc = subprocess.run([str(CLI), data_dir or tmp], input=script + "quit\n",
                              capture_output=True, text=True, timeout=120)
        return proc.stdout + proc.stderr


def test_a_side_that_is_neither_bid_nor_ask_is_refused_rather_than_guessed():
    out = run_cli("insert AAA EX sideways 6500000 1500\n"
                  "flush\n"
                  f"query SELECT side, price FROM 'AAA'.'EX' {WIDEST}\n")
    assert "'sideways' is not a side" in out, f"the word was accepted as a side:\n{out}"
    # Stronger than "it was stored as a bid": the symbol does not exist at all.
    assert "not found" in out, (
        f"a mistyped side stored a row, which is the defect rather than the message:\n{out}")


def test_a_confirmation_names_the_side_that_was_stored_not_the_word_that_was_typed():
    # `Ask` is accepted — the parser got *more* permissive about case while refusing nonsense — and
    # the confirmation says `ask`, because a line that repeats the caller's word confirms the typing
    # rather than the storing.
    out = run_cli("insert CCC EX Ask 6400000 1400\n"
                  "flush\n"
                  f"query SELECT side, price FROM 'CCC'.'EX' {WIDEST}\n")
    assert "OK  seq=1  ask CCC@EX" in out, f"the confirmation echoed the caller's spelling:\n{out}"
    assert "| ask  |" in out, f"the row was not stored on the ask side:\n{out}"


def test_a_token_the_grammar_has_no_place_for_is_refused():
    # The shape #105 added to the wire and this tool does not have: an event time. It used to be
    # read past and discarded, which is the same silence one layer in.
    out = run_cli("insert BBB EX bid 6500000 1500 1 1700000000000000000\n"
                  "flush\n"
                  f"query SELECT side, price FROM 'BBB'.'EX' {WIDEST}\n")
    assert "unexpected token '1700000000000000000'" in out, f"the extra token was read past:\n{out}"
    assert "not found" in out, f"the refused line stored a row anyway:\n{out}"


def test_bulk_refuses_the_same_two_things():
    out = run_cli("bulk DDD EX sideways 5 6500000 100\n"
                  "bulk EEE EX bid 5 6500000 100 extra\n"
                  "bulk FFF EX ask 5 6500000 100\n"
                  "flush\n"
                  f"query SELECT side, price FROM 'FFF'.'EX' {WIDEST}\n")
    assert "'sideways' is not a side" in out, f"bulk guessed the side:\n{out}"
    assert "unexpected token 'extra'" in out, f"bulk read past its arguments:\n{out}"
    # The control: the canonical form still works, and five levels land on the ask side.
    assert "5 ask levels for FFF@EX" in out, f"a correct bulk was refused:\n{out}"
    assert "5 row(s)" in out, f"the correct bulk stored the wrong number of rows:\n{out}"


def test_an_unknown_flag_is_not_taken_for_a_data_directory():
    if not CLI.is_file():
        pytest.skip(f"{CLI} is not built")
    # Run inside a directory of its own, so the assertion below is about a path this test controls.
    # The first version looked for `--data-dir` in the repository root and failed the moment a
    # mutation run left one there — a test asserting on shared state, which is the same class of
    # defect as the fixed ports in #109.
    with tempfile.TemporaryDirectory(prefix="ob_cli_flag_") as cwd:
        proc = subprocess.run([str(CLI), "--data-dir", "/tmp/ob_cli_should_not_exist"],
                              capture_output=True, text=True, timeout=60, cwd=cwd)
        assert proc.returncode == 2, (
            f"an unknown flag started the tool anyway (rc={proc.returncode}):\n"
            f"{proc.stdout}{proc.stderr}")
        assert "unknown argument '--data-dir'" in proc.stdout + proc.stderr
        assert not (Path(cwd) / "--data-dir").exists(), (
            "the tool created a data directory named after the flag, which is what it used to do")
        assert not Path("/tmp/ob_cli_should_not_exist").exists(), (
            "the tool created the directory the flag's value named, so it parsed neither")


def test_the_help_flag_is_the_one_dash_argument_that_works():
    # Because refusing every dash argument would refuse the one a reader tries first.
    if not CLI.is_file():
        pytest.skip(f"{CLI} is not built")
    proc = subprocess.run([str(CLI), "--help"], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0
    assert "takes no flags" in proc.stdout, proc.stdout


def test_the_csv_loader_counts_a_mistyped_side_as_an_error_rather_than_a_bid():
    """The third silence in the same file, and the one a bulk import would hide.

    A row whose side is a typo used to load as a bid, so a loader could halve one side of the book
    and report success. It is counted now, and the count is printed — a loader that says "480 rows
    (20 errors)" is one somebody can act on.
    """
    if not CLI.is_file():
        pytest.skip(f"{CLI} is not built")
    with tempfile.TemporaryDirectory(prefix="ob_cli_csv_") as tmp:
        csv = Path(tmp) / "rows.csv"
        csv.write_text(
            "symbol,exchange,side,price,qty,count,timestamp_ns\n"
            "GGG,EX,bid,6500000,100,1,1700000000000000000\n"
            "GGG,EX,sideways,6400000,200,1,1700000000000000001\n"
            "GGG,EX,ask,6600000,300,1,1700000000000000002\n",
            encoding="utf-8")
        out = run_cli(f"load {csv}\n"
                      "flush\n"
                      f"query SELECT side, price FROM 'GGG'.'EX' {WIDEST}\n",
                      data_dir=str(Path(tmp) / "data"))

    assert "Loaded 2 rows (1 errors)" in out, (
        f"the mistyped row was loaded as a bid, or the count does not say so:\n{out}")
    assert "2 row(s)" in out, f"the two good rows did not both land:\n{out}"
    # And the one that landed on the ask side really is an ask: the control for the row above.
    assert "| ask  |" in out, f"no ask row was stored:\n{out}"
