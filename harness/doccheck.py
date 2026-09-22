#!/usr/bin/env python3
"""The prose and the code state the same numbers, or this fails.

Two defects found by clean-room run 31 were both the shipped text disagreeing
with the shipped behaviour: SKILL.md quoted a 10% spread ceiling the harness
had refused at 20% since run 9, and pipeline.example.json carried a fan-out of
8 directly under a comment deriving 5. Neither cost that run anything -- it
read the code. A first reader follows the prose, so the agreement is checked
here rather than written down and hoped for.

Run it directly, or as part of CI.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)


def read(*parts):
    with open(os.path.join(*parts), encoding="utf-8") as f:
        return f.read()


def check_spread_ceiling(fail):
    """Every place the prose names the spread ceiling names the code's value."""
    import lib
    pct = f"{lib.T['spreadCeil']:.0%}"
    skill = read(ROOT, "SKILL.md")
    for phrase in (f"spread exceeds {pct}", f"spread >{pct}"):
        if phrase not in skill:
            fail(f"SKILL.md does not say {phrase!r}; the harness refuses at {pct}")
    # and no stale figure survives next to the word "spread"
    for m in re.finditer(r"spread (?:exceeds |>)(\d+)%", skill):
        if m.group(1) + "%" != pct:
            fail(f"SKILL.md states a spread ceiling of {m.group(1)}%, code says {pct}")
    return f"spread ceiling {pct} in SKILL.md and lib.py"


def check_tiny_ratio_band(fail):
    """The tiny proof's band, as stated, is the band the code applies.

    The code scales it by the ideal ratio -- `tinyRatioLo * ideal / 2` -- so a
    tiny proof run at one and four cores refuses outside 3x-5x, not 1.5x-2.5x.
    The prose quoted the doubling's numbers as if they were absolute, which
    reads as "superlinear is refused" when 25% above ideal is not.
    """
    import lib
    band = f"{lib.T['tinyRatioLo'] / 2:.2f}\u00d7\u2013{lib.T['tinyRatioHi'] / 2:.2f}\u00d7"
    if band not in read(ROOT, "SKILL.md"):
        fail(f"SKILL.md does not state the tiny-proof band as {band} of the ideal")
    return f"tiny-proof band {band} of ideal"


def check_cases_match(fail):
    """The core counts the skill promises are the ones the example runs.

    Section 5 says not to run the one-unit case when the claim is a step from
    two units up -- it is the structurally weakest case and the noisiest. The
    example config agrees: cases [2, 4]. Section 1a used to promise "1, 2 and
    4 cores" anyway, so clean-room run 31 ran the one-core case and got a
    1->2 step of 2.76x, above the 2.00x that is arithmetically possible,
    because one core carries all the fixed cost with nothing to share it.
    """
    cases = json.loads(read(HERE, "pipeline.example.json")).get("cases") or []
    if not cases:
        return "no cases in the example to check"
    names = [str(c) for c in cases]
    phrase = (names[0] if len(names) == 1
              else " and ".join(names) if len(names) == 2
              else ", ".join(names[:-1]) + " and " + names[-1]) + " cores"
    skill = read(ROOT, "SKILL.md")
    if f"always measures {phrase}" not in skill:
        fail(f"SKILL.md does not say it always measures {phrase}, which is what the example runs")
    if f"near-linear scaling across {phrase}" not in skill:
        fail(f"SKILL.md's objective does not name {phrase}")
    return f"skill and example agree on {phrase}"


def check_plan_discloses(fail):
    """The plan shown for approval names everything decided without asking.

    Section 1a exists so nothing is hidden by not having been asked. It was
    hiding several things anyway: how many passes, how long it takes, how much
    disk it writes, which ports it takes, and what guarantee it configures --
    all decided by the skill, none of them shown. A user who stops a run
    halfway because they expected twenty minutes was not told.
    """
    skill = read(ROOT, "SKILL.md")
    try:
        plan = skill[skill.index("## 1a."):skill.index("## 2.")]
    except ValueError:
        fail("SKILL.md has no section 1a to check")
        return "no plan section"
    needed = {
        "the cores measured": "cores",
        "the passes per case": "passes per case",
        "the guarantee": "exactly-once checkpointing",
        "the time it takes": "in hours",
        "the disk it writes": "backlogs",
        "the ports it takes": "ports in",
        "the mid-run kill": "killing the pipeline mid-run",
        "where to watch it run": "results/PROGRESS.txt",
        "where it runs": "in Docker",
    }
    # section 1a had no unattended path and deadlocked clean-room run 31; section
    # 6 had the same hole and stopped run 32 with a fix in hand. Both are checked
    # so the third one does not happen quietly.
    for where, needle in (("section 1a", "When there is no human to say yes"),
                          ("section 6", "no human to say yes, tune until you run out of levers"),
                          ("section 6", "If the step got worse, revert it"),
                          ("section 6", "Before looking at the pipeline, measure the machine"),
                          ("section 6", "prove.py ceiling"),
                          ("section 6", "prove.py probe --repeats"),
                          ("section 6a", "Tuning what you built")):
        if needle not in skill:
            fail(f"{where} has no path for an unattended run")
    missing = [name for name, needle in needed.items() if needle not in plan]
    for name in missing:
        fail(f"the plan in section 1a does not disclose {name}")
    return f"the plan discloses all {len(needed)} unconditionals"


def check_order_is_asserted(fail):
    """The interview asks for ordering, so the verifier has to check it.

    Section 1 q4's default says positions must be "published in order" and
    section 4 listed four assertions, none of them about order. A skill that
    asks a question and never verifies the answer is collecting an opinion.
    Ordering within a key is the one ordering guarantee a keyed stream makes,
    which is exactly why it is cheap to assert and worth refusing without.
    """
    skill = read(ROOT, "SKILL.md")
    needed = {"per-key order": "published values never go backwards",
              "one key in one partition": "each key appears in exactly one partition",
              "what the killed arm may do": "may step backwards **once**"}
    for name, needle in needed.items():
        if needle not in skill:
            fail(f"section 4 does not require {name} to be asserted")
    return f"the verifier must assert {len(needed)} things about order"


def check_example_backlogs(fail):
    """The example's backlogs are big enough for the rates on record.

    Three shipped defaults have now been caught by running them rather than
    reading them: the broker's page cache, the tiny proof's backlog, and the
    suite's. All three were the example disagreeing with a rule the harness
    already states and already enforces at run time -- which means it costs
    20 minutes of rig to find out, or in the suite's case rather more.

    So both backlogs are checked against the fastest rate the record holds,
    with the harness's own sizing functions. The floor moves with the record.
    """
    import lib
    rec = json.loads(read(HERE, "record", "sizing.json"))
    suites = rec.get("suites") or []
    if not suites:
        return "no recorded rates to size against"
    top = max(suites, key=lambda s: s["rateAtTop"])
    ex = json.loads(read(HERE, "pipeline.example.json"))
    b = ex.get("backlog") or {}
    ckpt = ex.get("checkpointMs", 10000) / 1000.0

    want = lib.size_backlog(top["rateAtTop"], top["cores"], top["ckptS"],
                            warmup_max_s=top.get("warmupS"))
    if (b.get("count") or 0) < want:
        fail(f"pipeline.example.json backlog.count is {b.get('count'):,}, but the fastest rate on "
             f"record ({top['run']}, {top['rateAtTop']:,.0f}/s) needs {want:,}. A short suite "
             f"backlog costs the suite.")

    # the tiny proof: the README's rule, warm-up ceiling + window + two intervals
    tiny_want = int(top["rateAtTop"] * (120 + 40 + 2 * ckpt))
    if (b.get("tinyCount") or 0) < tiny_want:
        fail(f"pipeline.example.json backlog.tinyCount is {b.get('tinyCount'):,}, but at "
             f"{top['rateAtTop']:,.0f}/s the tiny proof needs {tiny_want:,} to warm up and measure "
             f"without running dry.")
    return (f"backlogs cover {top['rateAtTop']:,.0f}/s: suite {b.get('count'):,} >= {want:,}, "
            f"tiny {b.get('tinyCount'):,} >= {tiny_want:,}")


def check_only_the_throttled_thing_is_throttled(fail):
    """Question 1 and question 2 must not disagree about what is throttled.

    Q1 read "emits a position and market value every 10 seconds" while Q2 said
    positions are one per symbol plus one per allocation -- five records per
    input -- and only the market value is throttled. An agent reading Q1 alone
    throttles the positions too, and outputsPerInput stops being a constant,
    which is the one thing the two-vantage check needs it to be.
    """
    skill = read(ROOT, "SKILL.md")
    if "position and market value every" in skill:
        fail("question 1 says a position is emitted on the throttle interval; question 2 says "
             "positions are one per input and only the market value is throttled")
    if "only the market value is throttled" not in skill:
        fail("question 1 does not say that only the market value is throttled")
    return "only the market value is throttled, in both questions"


def check_example_matches_interview(fail):
    """The example config describes the pipeline the interview asks for.

    Question 1's default has a price input and a market value emitted every 10
    seconds. The example described only the positions half and mentioned
    neither, so "take the defaults" gave two different answers depending on
    which file you read, and clean-room run 36 reported the interview's own
    default as a pipeline the harness could not express. It can; the example
    just never said how.
    """
    ex = read(HERE, "pipeline.example.json").lower()
    for what, needle in (("the price input", "price"), ("the market value output", "market value")):
        if needle not in ex:
            fail(f"pipeline.example.json does not mention {what}, which question 1's default asks for")
    return "the example covers the interview's whole pipeline"


def check_example_broker_memory(fail):
    """The shipped example gives Kafka at least as much page cache as the
    configurations on record that actually produced a table.

    What matters is kafkaMemory minus kafkaHeap: the remainder is page cache,
    and a broker that cannot hold the backlog reads it back off disk and
    becomes the constraint instead of the worker. Clean-room run 31 lost a
    whole 44-minute suite to this -- the example shipped 4g with a 3G heap,
    leaving 1.00 GB where every accepted configuration in record/configs.json
    leaves 4.25-5.00 GB. Three cases came back as ceilings and both steps were
    voided. The floor here is measured, not chosen: it is the smallest page
    cache any recorded configuration produced a usable table with.
    """
    def mb(v):
        if not v:
            return None
        v = str(v).strip()
        return float(v[:-1]) * 1024 if v[-1] in "gG" else float(v[:-1])

    import lib
    floor = lib.broker_cache_floor_mib()
    if not floor:
        return "no recorded broker sizes to check against"

    caps = json.loads(read(HERE, "pipeline.example.json")).get("caps") or {}
    total, heap = mb(caps.get("kafkaMemory")), mb(caps.get("kafkaHeap"))
    if not (total and heap):
        fail("pipeline.example.json sets no kafkaMemory/kafkaHeap to check")
        return "broker memory not set"
    cache = total - heap
    if cache < floor:
        fail(f"pipeline.example.json leaves Kafka {cache / 1024:.2f} GB for caching "
             f"({caps['kafkaMemory']} total minus {caps['kafkaHeap']} heap). Every recorded "
             f"configuration that produced a table left at least {floor / 1024:.2f} GB.")
    return f"example leaves Kafka {cache / 1024:.2f} GB of page cache (floor {floor / 1024:.2f} GB)"


def check_example_comments(fail):
    """A `_field` comment that derives a number and the `field` beside it agree.

    The example is the file README.md tells you to copy. A comment there that
    argues against the value under it is worse than no comment: it is a trap
    with an explanation attached.
    """
    path = os.path.join(HERE, "pipeline.example.json")
    cfg = json.loads(read(path))
    checked = 0
    for key, note in list(cfg.items()):
        if not key.startswith("_") or not isinstance(note, str):
            continue
        field = key[1:]
        if field not in cfg or not isinstance(cfg[field], (int, float)):
            continue
        # "... is 1 + 4 = 5, not 8." -- the comment's own conclusion
        m = re.search(r"=\s*(\d+),\s*not\s+(\d+)", note)
        if not m:
            continue
        derived, rejected = int(m.group(1)), int(m.group(2))
        checked += 1
        if cfg[field] != derived:
            fail(f"pipeline.example.json: {field} is {cfg[field]}, but {key} derives {derived}"
                 + (f" and rejects {rejected}" if cfg[field] == rejected else ""))
    return f"{checked} derived value(s) in pipeline.example.json"


def check_key_layout(fail):
    """The 5/3/4/4 the prose quotes is what Flink's assignment actually gives,
    and the example names its key sets so the check can run at all."""
    import prove
    import lib
    layout = getattr(prove, "ACCOUNT_KEY_LAYOUT_128", None)
    if layout is None:
        fail("prove.py no longer publishes the recorded key layout for the demo's account keys")
        return "key layout: not checked"
    spread = lib.key_spread({"positions-by-account": sorted(layout)}, [1, 2, 4], layout, 128)
    counts = spread["stages"]["positions-by-account"]["cases"][4]["keysPerSubtask"]
    shape = "/".join(str(n) for n in counts)
    ceiling = f"{spread['worst']['stageCeiling']:.2f}"
    for where, text in (("SKILL.md", read(ROOT, "SKILL.md")),
                        ("harness/README.md", read(HERE, "README.md")),
                        ("harness/pipeline.example.json", read(HERE, "pipeline.example.json"))):
        if shape not in text:
            fail(f"{where} does not say the demo's account keys land {shape}")
        if where != "harness/pipeline.example.json" and ceiling not in text:
            fail(f"{where} does not state the {ceiling} of linear that layout bounds the stage at")
    example = json.loads(read(HERE, "pipeline.example.json"))
    if not example.get("keySets"):
        fail("pipeline.example.json names no keySets, so its own preflight cannot check the key layout")
    return f"the demo's account keys land {shape}, bounding that stage at {ceiling} of linear"
def check_tinyproof_reruns(fail):
    """§6a says the tiny proof can be re-run after the fill. That is only true
    while the disk projection credits the backlog already on the broker, so the
    claim is checked against the function that has to make it good."""
    import inspect
    import lib
    sig = inspect.signature(lib.disk_verdict)
    if "input_on_disk_bytes" not in sig.parameters:
        fail("disk_verdict does not credit the backlog already on disk, so the tiny proof "
             "cannot be re-run after the fill, which SKILL.md \u00a76a says it can")
    for where, text in (("SKILL.md", read(ROOT, "SKILL.md")),
                        ("harness/README.md", read(HERE, "README.md"))):
        if "re-run after the fill" not in text and "re-runs after the\nfill" not in text:
            fail(f"{where} does not say the tiny proof can be re-run after the fill")
    # and the credit actually changes the verdict, on run 36's own shape
    shape = dict(in_bytes_per_rec=172.3, backlog=220_000_000, sink_bytes_per_in=284.8,
                 partitions=8, n_out_topics=2, ckpt_bytes=151552)
    try:
        lib.disk_verdict(77.3e9, **shape)
        fail("the projection no longer refuses run 36's post-fill re-run without the credit; "
             "the self-test fixture and this check disagree")
    except lib.Refusal:
        pass
    d = lib.disk_verdict(77.3e9, input_on_disk_bytes=37.9e9, **shape)
    if not d["fits"]:
        fail("crediting the backlog on disk still refuses run 36's post-fill re-run")
    return "the tiny proof re-runs after the fill, and disk_verdict credits the backlog"


def check_broker_cpu_hook(fail):
    """Section 7 names a file for the broker half of the CPU panel. It has to
    be there, and it has to refuse to run without a name prefix -- an exporter
    that publishes every container on the machine is not a hook, it is a leak."""
    path = os.path.join(HERE, "dashboard", "docker_cpu_exporter.py")
    if not os.path.exists(path):
        fail("harness/dashboard/docker_cpu_exporter.py is missing, and SKILL.md \u00a77 names it")
        return "broker CPU hook: missing"
    body = read(HERE, "dashboard", "docker_cpu_exporter.py")
    if "PREFIX is not set" not in body:
        fail("the CPU exporter does not refuse to start without PREFIX, so it would export "
             "every container on the machine")
    if "docker_cpu_exporter.py" not in read(ROOT, "SKILL.md"):
        fail("SKILL.md \u00a77 does not name the exporter that feeds the CPU per component panel")
    readme = read(HERE, "README.md")
    for needle in ("docker_cpu_exporter.py", "docker.sock", "PREFIX"):
        if needle not in readme:
            fail(f"harness/README.md gives no extraServices example naming {needle}")
    return "the CPU per component panel has a broker source, and it needs a name prefix"


def check_target_not_needed(fail):
    """Section 6 says "target", not "needed". There are three renderings of the
    same figures -- the scorecard, the suite table and suite.md -- and the rule
    held in one of them until clean-room run 36 read all three. Every line that
    prints the scaling floor is checked, so a fourth rendering cannot drift."""
    lib_src = read(HERE, "lib.py")
    bad = [ln.strip() for ln in lib_src.splitlines()
           if "scalingFloor" in ln and "needed" in ln]
    for ln in bad:
        fail(f"a report line says 'needed' where section 6 says 'target': {ln[:90]}")
    skill = read(ROOT, "SKILL.md")
    if 'Say "target", not "needed"' not in skill:
        fail("SKILL.md no longer states the target-not-needed rule the harness is checked against")
    for ln in skill.splitlines():
        if "doubling gave" in ln and "needed" in ln:
            fail(f"SKILL.md's own example breaks the rule beside it: {ln.strip()[:90]}")
    if "never called \"short of\"" not in skill:
        fail("SKILL.md does not say a ratio above its target is never called short of it")
    return "the scaling target is called a target in every rendering"


def check_probe_advice(fail):
    """The skill must not tell anyone to raise the repeats until the *range*
    narrows. It cannot: min to max only widens with more samples, and run 36
    did as it was told and went from 9% to 15%."""
    import lib
    skill = read(ROOT, "SKILL.md")
    if "More repeats do not\nnarrow that range" not in skill:
        fail("SKILL.md does not say that more repeats widen the range rather than narrowing it")
    if "middle half" not in skill:
        fail("SKILL.md does not point at the middle half, the figure that does settle")
    nine = {"repeats": 9, "ofLinearRange": {"mem": {"2->4": {
        "spread": 0.15, "middleHalf": {"low": 0.80, "high": 0.84, "spread": 0.04}}}}}
    said = " ".join(lib.probe_advice(lib.probe_spread(nine), 0.09))
    if "tighten" in said or "more repeats" in said:
        fail(f"the harness still tells a nine-repeat probe to tighten its range: {said[:80]}")
    return "more repeats widen the range and settle the middle half, in the prose and the harness"


def check_calls_are_grouped(fail):
    """The skill is written for agents, and an agent pays for every turn by
    re-reading the conversation. Both places that tell one how to wait have to
    say so, or the advice drifts back to polling on a timer."""
    skill = read(ROOT, "SKILL.md")
    readme = read(HERE, "README.md")
    if "do not\npoll on a timer" not in skill:
        fail("SKILL.md \u00a72 does not say to wait on results/DONE rather than poll on a timer")
    if "Group the calls that do not depend on each other" not in skill:
        fail("SKILL.md \u00a72 does not say to group independent calls")
    if "not on a timer" not in readme:
        fail("harness/README.md still invites polling PROGRESS.txt on a timer")
    return "independent calls travel together, and a run is waited on rather than polled"


def main():
    problems = []
    lines = []
    for check in (check_spread_ceiling, check_tiny_ratio_band, check_cases_match,
                  check_plan_discloses, check_order_is_asserted,
                  check_only_the_throttled_thing_is_throttled,
                  check_example_matches_interview, check_example_backlogs,
                  check_example_broker_memory, check_example_comments,
                  check_key_layout, check_tinyproof_reruns,
                  check_broker_cpu_hook, check_target_not_needed,
                  check_probe_advice,
                  check_calls_are_grouped):
        lines.append(check(problems.append))
    for p in problems:
        print(f"doccheck: {p}")
    if problems:
        print(f"doccheck: {len(problems)} disagreement(s) between the prose and the code")
        return 1
    print("doccheck: " + "; ".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
