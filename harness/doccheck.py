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
import ast
import io
import tokenize
import os
import re
import shutil
import statistics
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


def check_chain_sweeps_on_start(fail):
    """A restarted chain must not share the machine with the attempt before it.

    Clean-room run 43 restarted three times and left the previous attempt's
    generator writing 400,000,000 records at the broker the next attempt was
    measuring. The reaper could already find those processes; cmd_all simply
    never asked it to, so the sweep only happened at tinyproof and at down.
    """
    prove_src = read(HERE, "prove.py")
    i = prove_src.find("def cmd_all(")
    if i < 0:
        fail("cmd_all is gone from prove.py")
        return "cmd_all not found"
    body = prove_src[i:i + 4000]
    if "reap_host_watchers" not in body:
        fail("cmd_all no longer sweeps leftover processes before the chain starts, so a "
             "restarted chain can share the machine with the attempt before it -- which is "
             "how run 43 measured a warm-up that would not settle")
    return "the chain sweeps leftover processes before it starts"


def check_run42_lessons(fail):
    """The rest of what clean-room run 42 found stays fixed.

    Four separate things, each of which cost that run time and each of which is
    a sentence or a config key that a later edit could quietly undo.
    """
    skill = read(HERE, "..", "SKILL.md")
    readme = read(HERE, "README.md")
    lib_src = read(HERE, "lib.py")
    prove_src = read(HERE, "prove.py")
    windowed = read(HERE, "pipeline.example.windowed.json")

    # the metrics reporter has a route to the classpath at all
    if "Do not set `ENABLE_BUILT_IN_PLUGINS` on these images" not in skill:
        fail("SKILL.md section 7 no longer warns against ENABLE_BUILT_IN_PLUGINS on flink:1.20.x. "
             "The reporters ship as plugins there; setting it kills the job manager at startup, "
             "and clean-room run 43 lost its first prove.py up to this exact advice")
    if "flinkEnv" not in skill:
        fail("the flinkEnv hook is no longer described in SKILL.md section 7")
    if "flink_env" not in lib_src or "flinkEnv" not in readme:
        fail("the flinkEnv hook is gone from the harness or its field table, so section 7's "
             "metrics advice has no route again")

    # the example is not the clean-room interview's answer key
    for word in ("temperature", "Celsius", "sensor"):
        if word.lower() in windowed.lower():
            fail(f"pipeline.example.windowed.json is back to the temperature interview "
                 f"({word!r}); an example that IS the interview hands over the one design "
                 f"decision its second vantage depends on")

    # polling the stack is polling the thing under test
    if "Do not poll the stack while it is measuring" not in skill:
        fail("SKILL.md no longer warns that polling the stack is work inside the measurement; "
             "run 42 opened two passes at load 8.2 and 13.5 on eight cores doing it")

    # the retention row is not vacuous for the shape that needs it
    if "topic_retention_bytes" not in lib_src or "topic_retention_bytes" not in prove_src:
        fail("the retention preflight row no longer reads back on topicsAlsoWritten, so a "
             "pipeline with no topics.out passes it on an empty list")
    return "run 42's other four findings are still fixed"


def check_broker_ceiling_observation(fail):
    """What run 42 measured about the broker-ceiling guard stays written down.

    The guard threw away the two fastest four-core passes and kept the slowest,
    and giving the broker its page cache then moved that case's mean by 0.5%.
    That is one run, so it is not a threshold -- but it is the kind of thing a
    later run rediscovers expensively if nobody wrote it next to the rule.
    """
    skill = read(HERE, "..", "SKILL.md")
    for needle in ("2,771-2,840 times a window", "One run is not a threshold"):
        if needle not in skill:
            fail(f"SKILL.md no longer records what run 42 measured about the broker "
                 f"ceiling guard: {needle!r}")
    return "run 42's broker-ceiling measurement is still written next to the guard"


def check_windowed_example_backlogs(fail):
    """The same floor, for the other shipped example.

    check_example_backlogs has caught three shipped defaults by running the
    rule rather than reading it, but it only ever looked at one of the two
    examples. The windowed one shipped a 200,000,000-record suite backlog and a
    120,000,000-record tiny one; clean-room run 42 drained 2,657,223 readings a
    second, which needs 718,247,376 and 478,300,140. An example that runs dry
    mid-window costs whoever copies it a suite.
    """
    import lib
    rec = json.loads(read(HERE, "record", "sizing.json"))
    # Sized like the other example: a typical recorded rate across every shape,
    # because one windowed run is not a distribution and sizing off its single
    # figure is what inflated this example to 720,000,000 records.
    all_suites = rec.get("suites") or []
    if not all_suites:
        return "no recorded rates to size against"
    rate = statistics.median([s["rateAtTop"] for s in all_suites])
    suites = all_suites
    top = min(suites, key=lambda s: abs(s["rateAtTop"] - rate))
    ex = json.loads(read(HERE, "pipeline.example.windowed.json"))
    b = ex.get("backlog") or {}
    ckpt = ex.get("checkpointMs", 10000) / 1000.0
    want = lib.size_backlog(top["rateAtTop"], top["cores"], top["ckptS"],
                            warmup_max_s=top.get("warmupS"))
    if (b.get("count") or 0) < want:
        fail(f"pipeline.example.windowed.json backlog.count is {b.get('count'):,}, but the "
             f"fastest windowed rate on record ({top['run']}, {top['rateAtTop']:,.0f}/s) needs "
             f"{want:,}")
    tiny_want = int(top["rateAtTop"] * (120 + 40 + 2 * ckpt))
    if (b.get("tinyCount") or 0) < tiny_want:
        fail(f"pipeline.example.windowed.json backlog.tinyCount is {b.get('tinyCount'):,}, but "
             f"at {top['rateAtTop']:,.0f}/s the tiny proof needs {tiny_want:,}")
    return (f"windowed backlogs cover {top['rateAtTop']:,.0f}/s: suite {b.get('count'):,} "
            f">= {want:,}, tiny {b.get('tinyCount'):,} >= {tiny_want:,}")


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
    # Like with like. Records/sec is not comparable across record sizes: the
    # windowed example's reading is about 80 bytes and run 42 drained 2,657,223
    # of them a second, while this example's order is 326 bytes and the fastest
    # run on record managed 964,913. Sizing the 326-byte example for the 80-byte
    # pipeline's rate demands roughly 234 GB of Kafka log -- and it is the same
    # mistake as explaining one system's number with another system's
    # measurement. Untagged records predate the tag and are this example's.
    # A TYPICAL recorded rate, not the fastest ever seen. Sizing an example for
    # the fastest run on record made the windowed example ship 720,000,000
    # records -- about 37 GB with its tiny proof -- for a file whose own comment
    # says "These counts are NOT for you. Size them from your own pipeline's
    # measured rate." An example is a starting point; the tiny proof is what
    # actually protects a run, because it measures the rate and fails the chain
    # when the backlog is short of what it measured.
    suites = [s for s in suites if s.get("pipeline", "trading") == "trading"]
    if not suites:
        return "no recorded rates for this example's shape"
    rate = statistics.median([s["rateAtTop"] for s in suites])
    top = min(suites, key=lambda s: abs(s["rateAtTop"] - rate))
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
    # ...and both of them. Run 37 read "joins those to the positions" as the
    # symbol side only and built half the pipeline the default describes.
    skill = read(ROOT, "SKILL.md")
    if '"The positions" is both of them' not in skill:
        fail("SKILL.md question 1 does not say the price join applies to both position outputs")
    if "at both of those key levels" not in skill:
        fail("SKILL.md question 4 does not require market values at both key levels")
    if "two of them" not in ex.replace("TWO", "two"):
        fail("pipeline.example.json does not say there are two market-value outputs")
    if "broadcast" not in ex:
        fail("pipeline.example.json does not say prices are broadcast, which is what lets the "
             "account side join at all")
    return "the example covers the interview's whole pipeline, both market values included"


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
        fail("SKILL.md does not point at the middle half")
    if "It does not shrink either" not in skill:
        fail("SKILL.md still implies the middle half narrows with repeats; measured, it went from "
             "6% over nine repeats to 11% over twenty-five")
    if "Read the arms separately" not in skill:
        fail("SKILL.md does not say to read the probe's arms separately, so a steady arm stays "
             "hidden behind an unsteady one")
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


def check_no_duplicate_keys(fail):
    """A JSON object with the same key twice keeps the last one, so the other
    is dead text nobody sees. pipeline.example.json carried two
    _flinkProperties for long enough that the first -- the one with the metrics
    reporter example -- was invisible to every reader and every parser."""
    dupes, counted = [], [0]

    def watch(pairs):
        # per object, not across the file: "cmd" appears once under generator
        # and once under verifier, and that is two different keys
        keys = [k for k, _ in pairs]
        counted[0] += len(keys)
        for k in keys:
            if keys.count(k) > 1 and k not in dupes:
                dupes.append(k)
        return dict(pairs)

    json.loads(read(HERE, "pipeline.example.json"), object_pairs_hook=watch)
    for k in sorted(dupes):
        fail(f"pipeline.example.json sets {k!r} twice in one object; only the last one is "
             f"read, and the other is text nobody will ever see")
    return f"no key is set twice in pipeline.example.json ({counted[0]} keys)"


def check_design_is_diffed(fail):
    """Section 4 promises a design-versus-build diff that fails the run and is
    corrected before anything is measured. The promise is only worth something
    while the function behind it refuses, so both are checked."""
    import lib
    skill = read(ROOT, "SKILL.md")
    for needle, what in (
            ("diff the design against the build", "\u00a74 does not say to diff the design against the build"),
            ("No fill, no suite, until the diff is\nclean", "\u00a74 does not say to correct it and re-run before measuring"),
            ("```mermaid", "the interview has no diagram of the default business case")):
        if needle not in skill:
            fail(f"SKILL.md: {what}")
    ex = json.loads(read(HERE, "pipeline.example.json"))
    if not (ex.get("design") or {}).get("operators"):
        fail("pipeline.example.json declares no design.operators, so its own build cannot be diffed")
    if not ex.get("topicsAlsoWritten"):
        fail("pipeline.example.json declares no topicsAlsoWritten, so the outputs outside topics.out "
             "are not held to anything")
    # and the diff still refuses a build missing a declared output
    plan = {"nodes": [{"id": "a", "description": "positions<br/>+- sink-mv: Writer<br/>", "inputs": []}]}
    _, bad = lib.design_diff({"outputs": ["market-values-by-account"]}, plan,
                             {"market-values-by-account": 0}, {})
    if bad is None:
        fail("design_diff no longer refuses a declared output that nothing wrote")
    return "the design is diffed against the build, and a missing output fails the run"


def check_windowed_pipelines(fail):
    """The harness must be able to measure a pipeline with no constant fan-out
    anywhere. Both halves are checked: the prose says how, and Cfg refuses a
    pipeline that declares no second way to be measured at all."""
    import lib
    skill = read(ROOT, "SKILL.md")
    if "secondVantage" not in skill:
        fail("SKILL.md \u00a75 does not say how a pipeline with no constant fan-out is measured")
    if "inputRecordsProcessed" not in read(HERE, "README.md"):
        fail("harness/README.md does not document what the secondVantage command must print")
    if "outputsPerInput" in [k for k in json.loads(read(HERE, "pipeline.example.json"))] and \
            "optional" not in read(HERE, "README.md").lower():
        fail("harness/README.md does not say outputsPerInput is optional")
    return "a pipeline whose outputs are per window can be measured"


def check_both_examples_load(fail):
    """Both shipped examples are complete and valid, and they are different
    shapes. One example is a template people copy; two are a choice people have
    to read. The windowed one is also the only proof in the repository that a
    pipeline with no constant fan-out can be configured at all."""
    import tempfile
    import lib
    shapes = {}
    for name in ("pipeline.example.json", "pipeline.example.windowed.json"):
        path = os.path.join(HERE, name)
        if not os.path.exists(path):
            fail(f"{name} is missing; README.md offers it as one of two worked examples")
            continue
        tmp = tempfile.mkdtemp(prefix="example-doccheck-")
        try:
            with open(os.path.join(tmp, "pipeline.json"), "w") as f:
                f.write(read(HERE, name))
            saved = lib._CFG
            try:
                lib._CFG = None
                c = lib.Cfg(os.path.join(tmp, "pipeline.json"))
                shapes[name] = c.vantage_mode
            finally:
                lib._CFG = saved
        except lib.Refusal as e:
            fail(f"{name} does not load: {e.msg[:120]}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    if len(set(shapes.values())) < 2:
        fail(f"both examples measure themselves the same way ({shapes}); the second one exists "
             f"to show the other shape")
    if "Do not copy an" not in read(HERE, "README.md"):
        fail("harness/README.md no longer says to write pipeline.json rather than copy an example")
    return f"two worked examples, both load, different shapes ({', '.join(sorted(shapes.values()))})"


def check_shape_ignores_the_plan(fail):
    """The plan is kept so the report can draw the graph. It must never be
    compared: it carries a fresh job id per submission and the parallelism
    being varied, so comparing it refuses every case after the first, for every
    pipeline. Clean-room run 41 lost its chain to exactly that."""
    import lib
    same = {"vertexCount": 1, "signature": [["x", []]], "maxParallelism": [128]}
    a = dict(same, plan={"jid": "aaaa", "nodes": [{"id": "n", "parallelism": 1}]})
    b = dict(same, plan={"jid": "bbbb", "nodes": [{"id": "n", "parallelism": 4}]})
    try:
        lib.check_shape(a, b)
    except lib.Refusal:
        fail("check_shape refuses the same graph at a new job id and size; it is comparing "
             "the running plan, which differs on every case by design")
    try:
        lib.check_shape(dict(a, vertexCount=2), b)
        fail("check_shape no longer notices a graph that really is different")
    except lib.Refusal:
        pass
    return "the shape comparison ignores the plan it keeps for drawing"


def check_windowed_in_one_place(fail):
    """A windowed pipeline's whole configuration, in one section.

    Clean-room run 44 assembled it from eight cross-references in five files
    before it could write pipeline.json, and the single most useful sentence --
    the best signal is a count the pipeline already publishes -- lived only in
    a JSON comment inside an example the skill tells you not to copy.
    """
    skill = read(ROOT, "SKILL.md")
    wanted = {
        "If your outputs are per window, set these six things":
            "§5 has no one place that gathers what a windowed pipeline must set",
        "The best signal is a count the pipeline already publishes":
            "§5 no longer says where a windowed pipeline's second reading should come from",
        "one row the harness cannot drive at all":
            "§6a does not say which levers the harness can actually express",
        "fewer subtasks for the same cores":
            "§6a does not name the lever that has no hook",
        "the sign says which cause it is":
            "§5 no longer says which of the two causes a disagreement points at",
        "an 8.8× fall at the only case that ever missed":
            "§5 lost run 45's measurement of the checkpoint interval against the disagreement",
        "The same is true of `flinkProperties`":
            "§5 does not warn that flinkProperties is baked in when the stack comes up",
        "can cost you the worker":
            "§6 no longer says that raising the broker can starve the worker",
        "77.8% of cap against 94.6% before":
            "§6 lost run 45's two-arm measurement of the broker against the worker",
    }
    for needle, why in wanted.items():
        if needle.lower() not in skill.lower():
            fail(why)

    # the example's caps note carries arithmetic, and arithmetic drifts
    ex = json.loads(read(HERE, "pipeline.example.windowed.json"))
    note = ex.get("_caps", "")
    if not note:
        fail("pipeline.example.windowed.json has no _caps note saying what VM it needs")
        return "the windowed example has no caps note"

    def mib(v):
        v = str(v).strip()
        return float(v[:-1]) * 1024 if v[-1] in "gG" else float(v[:-1])

    caps, cores = ex["caps"], max(ex["cases"])
    worker = mib(caps["tmMemoryBase"]) + cores * mib(caps["tmMemoryPerCore"])
    total = worker + mib(caps["kafkaMemory"]) + 1600.0     # the job manager, set by the harness
    shown = f"{total:,.0f} MiB"
    if shown not in note:
        fail(f"the windowed example's _caps note does not say {shown}, which is what its own "
             f"caps add up to at {cores} cores ({worker:,.0f} worker + "
             f"{mib(caps['kafkaMemory']):,.0f} broker + 1,600 job manager)")
    if "_project" not in ex:
        fail("pipeline.example.windowed.json does not say to change `project` before copying it")
    return (f"the windowed example declares the {shown} its caps need, and §5 gathers "
            f"the windowed settings in one place")


def check_plain_english(fail):
    """No message a person reads uses a word we have had to explain.

    The user asked for this three times -- 2026-09-20, 2026-09-22 and
    2026-09-23, the last time about single words in a status table. A prose
    rule would not have held: the first attempt swapped "refuses" for
    "failed", and "FAIL at report" was the exact line that drew the complaint
    the third time. So the rule is a check.

    Only string literals are read. Identifiers, comments and docstrings belong
    to whoever works on the harness, and `class Refusal` stays.
    """
    banned = {"refus": "say stopped the run, or threw the case out",
              "fail at ": "say STOPPED at <step>, and why in the same line",
              "failed at ": "say stopped at <step>, and why in the same line",
              "ran out of space": "say there is not enough disk space"}
    # Keys the harness writes into its JSON for a machine to read back, and the
    # recorded expectations that go with them. Not prose, not read by a person,
    # and renaming them would break every recorded run.
    allowed = {"refusal", "refusalScope", "refusals", "refuse", "accept"}
    triples = ('"' * 3, "'" * 3)
    bad = []
    for name in ("lib.py", "prove.py"):
        src = read(HERE, name)
        # The text inside f-strings too: it reached a person on every stopped
        # pass ("refusal (rig): ...") and this check never read it.
        fmid = getattr(tokenize, "FSTRING_MIDDLE", None)
        for tok in tokenize.generate_tokens(io.StringIO(src).readline):
            if tok.type == fmid:
                text = tok.string
            elif tok.type != tokenize.STRING or tok.string[:3] in triples:
                continue
            else:
                try:
                    text = ast.literal_eval(tok.string)
                except Exception:
                    continue
            if not isinstance(text, str) or text.strip() in allowed:
                continue
            low = text.lower()
            for word, why in banned.items():
                if word in low:
                    bad.append(f"{name}:{tok.start[0]} {why} -- {text[:70]!r}")
            # A bare FAILED is a status a person reads first (run 47's F2).
            # Flink's own job state is exactly "FAILED" and stays allowed.
            if "FAILED" in text and text.strip() != "FAILED":
                bad.append(f"{name}:{tok.start[0]} say STOPPED, and why -- {text[:70]!r}")
    for b in bad:
        fail(b)
    skill = read(ROOT, "SKILL.md")
    if "Every message a person reads is plain English" not in skill:
        fail("SKILL.md no longer states the plain-English rule the harness enforces")
    for needed in ("stopped the run, or threw the case out", "not enough disk space",
                   "Say which way a near miss went"):
        if needed not in skill:
            fail(f"SKILL.md's plain-English table lost: {needed!r}")
    return (f"{len(bad)} message(s) use a word we have had to explain" if bad
            else "every message a person reads is plain English, and the skill says so")


def check_run41_lessons(fail):
    """The findings clean-room run 41 paid for, held in the prose that carries
    them. Each cost that run real time and each is a sentence someone will
    tidy away."""
    skill = read(ROOT, "SKILL.md")
    wanted = {
        "*It moves in steps.*": "\u00a75 does not warn that a windowed progress signal moves "
                                "one window at a time",
        "**at the smallest case**": "\u00a75 does not say to size the step against the smallest case",
        "last checkpoint": "\u00a75 does not warn that the two readings are taken at different moments",
        "property of the job, not of the test data": "\u00a75 does not say a fan-out must be the job's "
                                                     "property rather than the generator's",
        "make the pipeline cost something per record": "\u00a76a has no lever for a pipeline that is too "
                                                       "cheap per record to be bound by its cores",
        "Two of these rows do not apply": "\u00a76a does not say which levers a given pipeline cannot use",
        "assertion with nothing to compare is written down": "\u00a74 does not say what to do when an "
                                                             "assertion has nothing to compare",
        "panel list is for a pipeline with fan-out": "\u00a77 does not say the panels assume a fan-out",
    }
    for needle, why in wanted.items():
        if why and needle not in skill:
            fail(f"SKILL.md: {why}")
    ex = read(HERE, "pipeline.example.windowed.json")
    if "sum of that field" not in ex:
        fail("the windowed example does not point at the exact progress signal (a count the pipeline "
             "already publishes) over the coarse one")
    if "topicsAlsoWritten" not in read(HERE, "README.md"):
        fail("harness/README.md does not document topicsAlsoWritten at all")
    import lib
    import inspect
    if "include_declared" not in inspect.signature(lib.recreate_output_topics).parameters:
        fail("recreate_output_topics cannot clear the run's own outputs, so a verifier asked for "
             "different assertions on the two completeness arms sees both arms' rows at once")
    return "run 41's findings are still written down"


def main():
    problems = []
    lines = []
    for check in (check_spread_ceiling, check_tiny_ratio_band, check_cases_match,
                  check_plan_discloses, check_order_is_asserted,
                  check_only_the_throttled_thing_is_throttled,
                  check_example_matches_interview, check_example_backlogs,
                  check_windowed_example_backlogs,
                  check_broker_ceiling_observation,
                  check_run42_lessons,
                  check_chain_sweeps_on_start,
                  check_example_broker_memory, check_example_comments,
                  check_key_layout, check_tinyproof_reruns,
                  check_broker_cpu_hook, check_target_not_needed,
                  check_probe_advice,
                  check_calls_are_grouped,
                  check_no_duplicate_keys,
                  check_design_is_diffed,
                  check_windowed_pipelines,
                  check_both_examples_load,
                  check_shape_ignores_the_plan,
                  check_run41_lessons,
                  check_plain_english,
                  check_windowed_in_one_place):
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
