"""How well this machine can tell two numbers apart, measured rather than assumed.

This is the centre of the harness, and it is a refusal rather than a feature.

A control `BM_VwapLatency` on the development machine once reported **-40.6% in 8 of 8 rounds for an
identical function at the same address**. A comparative benchmark across four systems on a machine
like that will produce a number whether or not a difference exists. So before any comparison is
believed, the harness measures the same system against itself and publishes the worst deviation it
saw. Anything smaller than that is reported as indistinguishable, and no file is allowed to call it
faster.

The word "faster" is produced in exactly one place in this package - `classify()` below - and a
static test over the source holds it there. A rule about not overclaiming, broken in some other
file, looks identical to the rule not existing.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Callable

INDISTINGUISHABLE = "INDISTINGUISHABLE ON THIS HARDWARE"
CANNOT_RESOLVE = "machine cannot resolve"
USABLE = "usable"

# Above this, the machine only separates double-digit differences and the report says so out loud.
COARSE_FLOOR = 0.10

# Above this the same work varied by more than 100% between adjacent calls, which is not a coarse
# resolution but an absent one. Measured on the first real runs of this harness: a millisecond-scale
# query through the Python client on a four-thread laptop gave control ratios of
# [1.6166, 1.0299, 2.8881, 0.8583]. Worth a louder sentence than the 10% case, because a reader
# seeing a floor of 1.89 next to the words "only double-digit differences" would reasonably conclude
# the harness was confused rather than the machine.
UNUSABLE_FLOOR = 1.00


@dataclass(frozen=True)
class Resolution:
    rounds: int
    warmup: int
    discarded_outlier: float | None
    control_ratios: list[float]
    floor: float
    verdict: str
    note: str

    def as_dict(self) -> dict:
        return asdict(self)


def measure(sample: Callable[[], float], rounds: int = 6, warmup: int = 2) -> Resolution:
    """Time the same work twice per round, interleaved, and keep the worst ratio.

    Interleaved (A B A B ...) rather than in blocks: a block measures the machine drifting over the
    run - thermal, page cache, a neighbour on a shared host - which is a different quantity from the
    round-to-round noise a comparison is exposed to. Blocks would flatter the floor, and a flattered
    floor is the failure this whole module exists to prevent.
    """
    if rounds < 2:
        raise ValueError("a resolution needs at least two rounds to have a spread")

    # Warm-up, discarded, and this is not a way to flatter the floor - the same warm-up applies to
    # the measurements the floor governs, so it describes the regime the comparison actually runs
    # in. Measured on the first run of this harness: the control ratios were
    # [1.6785, 0.9837, 0.9993, 0.9701], so a single cold call set the floor at 0.68 while every
    # warm pair agreed within 3%. A floor of 0.68 calls every real difference indistinguishable,
    # which is the "job that always says cannot resolve" failure this module is supposed to avoid,
    # arrived at from the opposite direction.
    for _ in range(warmup):
        sample()

    ratios: list[float] = []
    for _ in range(rounds):
        first = sample()
        second = sample()
        if first <= 0 or second <= 0:
            raise ValueError(f"non-positive timing in a control round: {first}, {second}")
        ratios.append(first / second)

    # The floor is the worst deviation, with **one** extreme discarded when there are enough rounds
    # to afford it - and the discarded value is published rather than dropped.
    #
    # Plain `max` was the first version and it is too sharp to be useful on a shared machine: a run
    # of eight rounds gave [1.214, 1.05, 0.478, 1.035, 1.003, 1.019, 0.994, 1.001], where five pairs
    # agree within 3.5% and one scheduler hiccup sets the floor at 0.52. A harness that then refuses
    # every comparison is the "always says cannot resolve" job the spec warns about, reached from
    # the other side.
    #
    # Discarding exactly one, only at six rounds or more, and naming it in the note: that is the
    # same shape this project already uses to report its own benchmark comparisons ("median of
    # per-round ratios 0.9905 over 0.973-1.008"). It must not become a knob - two discards, or a
    # discard at four rounds, would be flattery.
    deviations = sorted((abs(ratio - 1.0) for ratio in ratios), reverse=True)
    discarded: float | None = None
    if rounds >= 6:
        discarded = deviations[0]
        floor = deviations[1]
    else:
        floor = deviations[0]

    # A control run in which **every** ratio is exactly 1.0 did not time work. Real work is never
    # bit-identical twice at `perf_counter` resolution, so this means the sampler is returning a
    # constant - and a floor of 0.0 makes every difference clear it, which is `classify()` calling
    # noise a win. Refused rather than reported, because the number it produces looks the same as a
    # good one.
    #
    # This guard exists because I wrote that exact bug in `run.py`: the first version passed a
    # cached value to `measure()` instead of re-running the workload. The module was right and the
    # glue was wrong, which is where this class of mistake actually lives.
    if all(ratio == 1.0 for ratio in ratios):
        raise ValueError(
            "the control sampler returned identical timings in every round, so it is not timing "
            "work - a floor of 0.0 would let every difference count as real")

    note = (
        f"control floor {floor:.4f} over {rounds} interleaved rounds; this machine does not "
        f"separate differences smaller than that")
    if discarded is not None:
        note += (f". One extreme of {discarded:.4f} was discarded as an outlier and every control "
                 f"ratio is in the results file")
    if floor >= UNUSABLE_FLOOR:
        note += (f". Above 100%: the *same* work varied by {floor * 100:.0f}% between adjacent "
                 f"calls, so nothing measured on this machine in this configuration is comparable. "
                 f"More rounds and a larger dataset are the levers; editing this threshold is not")
    elif floor >= COARSE_FLOOR:
        note += ". Above 10%: only double-digit differences are separable here"
    return Resolution(rounds=rounds, warmup=warmup, discarded_outlier=discarded,
                      control_ratios=ratios, floor=floor, verdict=USABLE, note=note)


def verdict_for(resolution: Resolution, largest_difference: float) -> Resolution:
    """Downgrade the verdict when the floor swallows everything the run measured.

    Exit code stays zero: "this machine does not resolve this comparison" is a correct result and
    reporting it is the harness working. A run that dressed it up as a failure would push the next
    person towards re-running until a number appeared.
    """
    if largest_difference <= resolution.floor:
        return Resolution(
            rounds=resolution.rounds,
            warmup=resolution.warmup,
            discarded_outlier=resolution.discarded_outlier,
            control_ratios=resolution.control_ratios,
            floor=resolution.floor,
            verdict=CANNOT_RESOLVE,
            note=(f"{resolution.note}. The largest difference measured was "
                  f"{largest_difference:.4f}, which is inside that floor"))
    return resolution


def classify(a_seconds: float, b_seconds: float, resolution: Resolution) -> str:
    """The only function in this package that may call one thing faster than another."""
    if a_seconds <= 0 or b_seconds <= 0:
        raise ValueError("cannot classify non-positive timings")
    difference = abs(a_seconds - b_seconds) / max(a_seconds, b_seconds)
    if difference < resolution.floor:
        return INDISTINGUISHABLE
    return "faster" if a_seconds < b_seconds else "slower"
