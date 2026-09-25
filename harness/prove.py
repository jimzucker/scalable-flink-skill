#!/usr/bin/env python3
"""
scalable-flink-skill harness — the one entry point.

    python3 harness/prove.py <command>     (run from the directory holding pipeline.json,
                                            or set PIPELINE_JSON=/path/to/pipeline.json)

  replay        check every threshold against the runs already recorded  (nothing starts; seconds)
  selftest      break every check on purpose and confirm it catches it   (needs the stack; ~3 min)
  up            write stack/compose.yml, start the stack, compile the offset sampler
  preflight     the section 3 checks, PASS or FAIL per row
  tinyproof     a short run at every core count, each step bounded, plus the self-test
  fill          write the full test data set and results/manifest.json     (run it detached)
  completeness  process a small test data set twice — once cleanly, once killed and restarted
                partway — and check nothing was lost either time
  suite         the measurements: every core count several times over, going up the sizes
                then back down then up again (if the order changes the answer, something is
                warming up between them), then a repeat of the first to check the machine
                did not slow down while it ran
  ceiling       hold the largest case and squeeze Kafka in steps, to find where it gives out
  probe         what this machine's own cores do, with no pipeline involved: the first
                thing to run when a step falls short   (minutes, nothing starts)
  report        results/suite.json -> results/suite.txt + results/suite.md
  down          stop everything, check nothing survived, give the disk space back
  all           up -> preflight -> completeness -> tinyproof -> fill -> suite -> report,
                one stack session, stopping at the first step that fails. results/PROGRESS.txt
                says where it is up to, results/DONE holds the outcome   (run it detached)

Exit code 0 means the command's own assertion held; anything else, read the log.
"""
import inspect
import json
import os
import re
import shutil
import sys
import tempfile
import textwrap
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib as L  # noqa: E402
from lib import (T, Refusal, CaseRefused, cfg, log, sh, rest, save_json, load_json,  # noqa: E402
                 build_hash, build_table, render_table, render_markdown)


# ---------------------------------------------------------------------- replay

def replay_names():
    """Names that only exist in another scope. The pure self-test and the replay
    both pass without ever running preflight, so a defect like that reaches the
    rig; one did on 2026-09-08 and cost a chain attempt."""
    import namecheck
    bad = []
    for f in ("prove.py", "lib.py"):
        bad += [(f,) + b for b in namecheck.undefined(os.path.join(L.HERE, f))]
    for f, line, scope, name in bad:
        print(f"  UNDEFINED: {f}:{line} {scope}() reads {name!r}")
    print(f"checked 2 harness files for undefined names" + ("" if not bad else f" — {len(bad)} FOUND"))
    return 1 if bad else 0


def replay_cases():
    """Case verdicts against classifications whose answer is already known.
    The suite record has no cap fractions or broker counters, so a change to
    what makes a case a scaling claim cannot be replayed against it."""
    path = os.path.join(L.HERE, "record", "cases.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    base = dict(boundaries=6, recordsConsumed=10_000_000, elapsedS=60.0, recordsPerSec=166_666.7,
                rateSource="kafka committed offsets", vantageSinkRecords=10_010_000.0,
                vantageDisagreement=0.001, backlogRemaining=50_000_000, headroomS=300.0,
                sourceIdle=0.0, tmCapFrac=0.99, bpSamples=6, brokerLimitHits=0, brokerRefaults=0)
    bad = 0
    for entry in doc["cases"]:
        rec = dict(base); rec.update(entry["rec"])
        got = "claim"
        try:
            L.check_case(rec, 4, False)
        except L.Ceiling:
            got = "ceiling"
        except Refusal:
            got = "invalid"
        if got != entry["expect"]:
            bad += 1
            print(f"  DISAGREES: {entry['name']}: expected {entry['expect']}, got {got}")
    print(f"replayed {len(doc['cases'])} recorded case verdicts" + ("" if not bad else f" — {bad} DISAGREE"))
    return 1 if bad else 0


def replay_sizing():
    """Backlog sizing against the backlogs that produced valid tables.

    size_backlog decides whether a suite is allowed to run at all, but nothing
    replayed it: a key-name slip (reading "seconds" where warmup_verdict returns
    "warmupS") left it falling back to the tiny proof's own 20 s override for
    however long it stood, and the suite replay cannot see that because it never
    calls size_backlog. Each recorded suite here produced a table we accepted,
    so demanding more records than that suite actually used is a guard that
    disagrees with the record -- and wrong.
    """
    path = os.path.join(L.HERE, "record", "sizing.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    bad = 0
    for s in doc["suites"]:
        # the same window the live call site sizes on -- see whyThatWindow
        want = L.size_backlog(s["rateAtTop"], s["cores"], s["ckptS"],
                              warmup_max_s=s.get("warmupS"),
                              window_s=doc.get("sizingWindowS"))
        if want > s["backlog"]:
            print(f"REPLAY FAIL: {s['run']} would fail -- sizing wants {want:,} "
                  f"records, the suite ran on {s['backlog']:,} and its table was accepted")
            bad += 1
    if not bad:
        print(f"replayed {len(doc['suites'])} recorded backlog sizings")
    return bad


def replay_broker_memory():
    """Broker sizing against the sizes whose outcome we know.

    size_broker_memory gates the tiny proof, so it can stop a run before rig
    time is spent -- and it can also wave through a broker that will lose the
    suite, which is what happened in run 31. Both directions are recorded.
    """
    path = os.path.join(L.HERE, "record", "broker-memory.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    bad = 0
    for s in doc["sizings"]:
        got = L.size_broker_memory(s["limitMb"] * 1048576, s["hits"])
        if got != s["expectMb"]:
            print(f"REPLAY FAIL: {s['run']} -- sizing says {got}, the record says "
                  f"{s['expectMb']}: {s['why']}")
            bad += 1
    if not bad:
        print(f"replayed {len(doc['sizings'])} recorded broker memory sizings")
    return bad


def replay_configs():
    """Config guards against configurations whose verdict we already know.

    The suite record cannot exercise these: it re-derives step verdicts from
    recorded rates and never builds a Cfg. #68 shipped a rule that refused two
    configurations which had already produced accepted runs, and cost a whole
    clean-room run to find out. Same discipline as the suite replay -- a guard
    that disagrees with the record is wrong.
    """
    path = os.path.join(L.HERE, "record", "configs.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    bad = 0
    for entry in doc["configs"]:
        cfg_doc = json.load(open(os.path.join(L.HERE, "pipeline.example.json")))
        cfg_doc["cases"] = entry["cases"]
        cfg_doc["baseline"] = min(entry["cases"])
        cfg_doc["partitions"] = entry["partitions"]
        cfg_doc["caps"] = entry["caps"]
        tmp = os.path.join(tempfile.mkdtemp(prefix="replay-cfg-"), "pipeline.json")
        json.dump(cfg_doc, open(tmp, "w"))
        got, why = "accept", ""
        try:
            c = L.Cfg(tmp)
            if [n for n in c.cases if c.partitions % n]:
                got, why = "refuse", f"{c.partitions} partitions do not divide by {c.cases}"
        except Refusal as e:
            got, why = "refuse", e.msg
        if got != entry["expect"]:
            bad += 1
            print(f"  DISAGREES: {entry['name']}: expected {entry['expect']}, got {got}"
                  + (f" ({why[:90]})" if why else ""))
    print(f"replayed {len(doc['configs'])} recorded configurations"
          + ("" if not bad else f" — {bad} DISAGREE"))
    return 1 if bad else 0


def cmd_replay():
    """Every threshold, checked against every recorded suite before it can refuse
    anything new. record/*.json holds per-pass rates per case and a verdict on
    which step ratios the record considers valid."""
    import glob
    rec_dir = os.path.join(L.HERE, "record")
    # Suites are the files that carry per-pass rates. Naming the others instead
    # meant every new record file broke this the moment it was added -- which it
    # duly did when broker-memory.json arrived.
    files, docs = [], {}
    for f in sorted(glob.glob(os.path.join(rec_dir, "*.json"))):
        d = json.load(open(f))
        if isinstance(d, dict) and isinstance(d.get("rates"), dict):
            files.append(f)
            docs[f] = d
    bad, n = [], 0
    for f in files:
        d = docs[f]
        runs = []
        for cores, rates in d["rates"].items():
            for i, r in enumerate(rates):
                runs.append({"cores": int(cores), "pass": f"p{i+1}", "recordsPerSec": float(r), "status": "OK"})
        t = build_table(runs)
        for step, valid in d.get("validSteps", {}).items():
            n += 1
            got = next((x for x in t["stepRatios"] if x["step"] == step), None)
            reportable = bool(got and got["reportable"])
            if valid and not reportable:
                bad.append(f"{os.path.basename(f)}: {step} is valid in the record but would be voided "
                           f"({got['reason'] if got else 'no such step'})")
            if not valid and reportable:
                bad.append(f"{os.path.basename(f)}: {step} is NOT valid in the record but would be reported "
                           f"({got['ratio']}x)")
    print(f"replayed {len(files)} recorded suites, {n} step verdicts, thresholds "
          f"spreadCeil={T['spreadCeil']:.0%} minPasses={T['minPasses']}")
    for b in bad:
        print("  DISAGREES:", b)
    if bad:
        print("REPLAY FAILED: a threshold disagrees with the record. Fix the threshold, not the record.")
        return 1
    if (replay_names() or replay_cases() or replay_configs() or replay_sizing()
            or replay_broker_memory()):
        print("REPLAY FAILED: a guard disagrees with a recorded configuration. Fix the guard, not the record.")
        return 1
    print("REPLAY OK: no recorded valid table would fail, no recorded invalid one reported, "
          "and every recorded configuration still gets its recorded verdict")
    return 0


# -------------------------------------------------------------------- selftest

# Where Flink 1.20.1 actually puts the demo's sixteen account keys at the
# default maxParallelism of 128, read out of the image with KeyCheck. The
# four symbol keys land 1/1/1/1; these land 5/3/4/4, so one core would do
# a quarter more work than an even split and that stage could not return
# more than 0.80 of linear at four cores. Clean-room run 36 hit this, wrote
# its own tool to find it, and chose key names by hand to get round it.
ACCOUNT_KEY_LAYOUT_128 = {
    "ACC1/SUB1/AAPL": {1: 0, 2: 0, 4: 0}, "ACC1/SUB1/MSFT": {1: 0, 2: 1, 4: 3},
    "ACC1/SUB1/GOOG": {1: 0, 2: 0, 4: 0}, "ACC1/SUB1/AMZN": {1: 0, 2: 0, 4: 0},
    "ACC2/SUB1/AAPL": {1: 0, 2: 0, 4: 0}, "ACC2/SUB1/MSFT": {1: 0, 2: 1, 4: 3},
    "ACC2/SUB1/GOOG": {1: 0, 2: 0, 4: 1}, "ACC2/SUB1/AMZN": {1: 0, 2: 1, 4: 2},
    "ACC3/SUB1/AAPL": {1: 0, 2: 1, 4: 2}, "ACC3/SUB1/MSFT": {1: 0, 2: 0, 4: 1},
    "ACC3/SUB1/GOOG": {1: 0, 2: 1, 4: 2}, "ACC3/SUB1/AMZN": {1: 0, 2: 0, 4: 1},
    "ACC4/SUB1/AAPL": {1: 0, 2: 1, 4: 3}, "ACC4/SUB1/MSFT": {1: 0, 2: 1, 4: 2},
    "ACC4/SUB1/GOOG": {1: 0, 2: 0, 4: 0}, "ACC4/SUB1/AMZN": {1: 0, 2: 1, 4: 3},
}


def cmd_selftest(live=True, topic=None):
    """A guard that has never fired is a guess. Each guard is broken on purpose
    through the same code path the suite uses."""
    c = cfg()
    results = []
    t_self = time.time()

    def expect(name, fn, needle, should_fire=True, ceiling=False):
        try:
            fn()
            res = dict(guard=name, ok=not should_fire, result="DID NOT FIRE")
        except L.Ceiling as e:
            res = dict(guard=name, ok=should_fire and ceiling and needle.lower() in e.msg.lower(),
                       result="CEILING", message=e.msg[:200])
        except (Refusal, CaseRefused) as e:
            msg = e.msg if isinstance(e, Refusal) else e.refusal.msg
            res = dict(guard=name, ok=should_fire and needle.lower() in msg.lower(), result="FAILED", message=msg[:200])
        except Exception as e:
            res = dict(guard=name, ok=False, result=f"WRONG ERROR {type(e).__name__}: {e}"[:200])
        results.append(res)
        print(("  ok  " if res["ok"] else "  BAD ") + f"{name:46s} -> {res['result']}"
              + (" | " + res["message"][:100] if "message" in res else ""), flush=True)

    good = dict(boundaries=6, recordsConsumed=10_000_000, elapsedS=60.0, recordsPerSec=166_666.7,
                rateSource="kafka committed offsets", vantageSinkRecords=10_010_000.0,
                vantageDisagreement=0.001, backlogRemaining=50_000_000, headroomS=300.0,
                sourceIdle=0.0, tmCapFrac=0.99, bpSamples=6)

    def case(**kw):
        r = dict(good); r.update(kw)
        return lambda: L.check_case(r, 4, kw.pop("_baseline", False))

    print("pure guards (synthetic case records through check_case):")
    # Retrying one transient case is a section 6 rule that nothing implemented
    # until run 31 lost a chain attempt to a tiny-proof case that passed on the
    # next try. Each message carries its attempt number, so a needle of
    # "attempt 1" fails if anything retried that should not have.
    def flaky(scope, times):
        st = {"n": 0}

        def run():
            st["n"] += 1
            if st["n"] <= times:
                raise CaseRefused({"cores": 1}, Refusal(scope, f"attempt {st['n']} failed"))
            return "ok", None

        def go():
            got, _ = L.run_case_retrying(run)
            assert got == "ok" and st["n"] == 2, f"expected 2 attempts, took {st['n']}"
        return go

    expect("a transient case is retried and passes (must not fire)", flaky("case", 1), "",
           should_fire=False)
    expect("a case that fails twice is not retried again", flaky("case", 5), "attempt 2")
    expect("a rig failure is never retried", flaky("rig", 5), "attempt 1")
    expect("a ceiling is never retried", flaky("ceiling", 5), "attempt 1")
    # What was holding a case back, named from what was already measured. Each
    # of these is a case whose cause was established independently -- by a guard
    # that fired, or in the sink case by a controlled A/B in clean-room run 32.
    def names(kw, want):
        r = dict(good); r.update(kw)

        def go():
            got = L.bottleneck(r)
            assert want in got, f"bottleneck said {got!r}, expected {want!r}"
        return go

    def labelled(kw, want):
        r = dict(good); r.update(kw)

        def go():
            got = L.bottleneck_short(r)
            assert got == want, f"short label said {got!r}, expected {want!r}"
        return go

    expect("bottleneck label: Pipeline CPU (must not fire)",
           labelled(dict(tmCapFrac=0.99), "Pipeline CPU"), "", should_fire=False)
    expect("bottleneck label: Kafka memory (must not fire)",
           labelled(dict(tmCapFrac=0.93, brokerLimitHits=12780), "Kafka memory"), "", should_fire=False)
    # Runs 46-48: passes at 95-99% of cap with broker limit hits were blamed on
    # Kafka and thrown out, at rates inside the passes kept beside them.
    expect("bottleneck label: broker hits at 96.8% of cap are not blamed on Kafka (must not fire)",
           labelled(dict(tmCapFrac=0.968, brokerLimitHits=10517), "Pipeline CPU"), "", should_fire=False)
    expect("bottleneck label: Pipeline memory (must not fire)",
           labelled(dict(tmCapFrac=0.99, gcFracOfCapacity=0.064), "Pipeline memory"), "", should_fire=False)
    expect("bottleneck label: Kafka writes (must not fire)",
           labelled(dict(tmCapFrac=0.9495, sourceBackpressured=0.6738), "Kafka writes"), "", should_fire=False)
    expect("bottleneck label: Input feed (must not fire)",
           labelled(dict(tmCapFrac=0.80, sourceIdle=0.40), "Input feed"), "", should_fire=False)
    def plural(n, want):
        def go():
            got = L.n_cores(n)
            assert got == want, f"n_cores({n}) said {got!r}, expected {want!r}"
        return go

    expect("one core is singular (must not fire)", plural(1, "1 core"), "", should_fire=False)
    expect("two cores is plural (must not fire)", plural(2, "2 cores"), "", should_fire=False)

    def verdict_not_met_above_ideal():
        """A step above its ideal clears a floor, but it is not a pass to report
        as one: run 31's 1->2 read 2.76x and sat under a note saying the smaller
        case reads low."""
        def go():
            runs = [{"cores": n, "pass": f"p{i}", "status": "OK",
                     "recordsPerSec": 100000.0 * (8 if n == 2 else n),
                     "tmCapFrac": 0.99, "gcFracOfCapacity": 0.02, "kafkaCores": 0.3,
                     "brokerLimitBytes": 4 * 1024**3, "brokerLimitHits": 0,
                     "sourceBackpressured": 0.05, "sourceIdle": 0.01}
                    for n in (1, 2) for i in (1, 2)]
            out = {"runs": runs, "table": L.build_table(runs)}
            line = [x for x in L.scorecard(out).splitlines() if "1->2" in x]
            assert line, "no 1->2 verdict line"
            assert "met" not in line[0].split("->")[-1], \
                f"a step above its ideal is reported as a pass: {line[0].strip()!r}"
            assert "reads low" in line[0], f"and does not say why: {line[0].strip()!r}"
        return go

    expect("a step above 2x is not reported as met (must not fire)",
           verdict_not_met_above_ideal(), "", should_fire=False)

    def compose_is_filled_in():
        """The generated stack file has no unfilled placeholders and parses.

        compose_text() is three string literals joined. Adding flinkProperties
        split it and left the trailing segment a plain string, so the job
        manager's jar mount was written out as the literal "{c.jar_dir}" and
        the file was not valid YAML. Nothing caught it: the harness renders
        that file on every `up`, and no test had ever looked at it. Clean-room
        run 35 lost about 25 minutes to it and could only get past it by
        patching the string in memory.
        """
        def go():
            text = L.compose_text()
            left = re.findall(r"\{[A-Za-z_][A-Za-z0-9_.\[\]']*\}", text)
            assert not left, f"the generated compose file still contains {sorted(set(left))}"
            # and it is a document, not just placeholder-free
            import subprocess as sp
            r = sp.run(["python3", "-c",
                        "import sys,yaml;yaml.safe_load(sys.stdin.read())"],
                       input=text, capture_output=True, text=True)
            if r.returncode and "No module named" not in r.stderr:
                raise AssertionError(f"the generated compose file is not valid YAML: {r.stderr.strip()[:120]}")
            for want in ("services:", "volumes:", "/jobs:ro"):
                assert want in text, f"the generated compose file has no {want!r}"
        return go

    expect("the generated stack file is filled in (must not fire)",
           compose_is_filled_in(), "", should_fire=False)

    def scorecard_width(limit=136):
        """The scorecard stays readable. It reached 175 characters once, a word
        at a time, because nobody measured it after each addition."""
        def go():
            # pinned on CPU with steps that fall short, so the row takes the
            # longest branch there is; wide numbers everywhere else
            runs = [{"cores": n, "pass": f"p{i}", "status": "OK", "recordsPerSec": 1234567.0,
                     "tmCapFrac": 0.9912, "gcFracOfCapacity": 0.031, "kafkaCores": 0.4,
                     "brokerLimitBytes": 4 * 1024**3, "brokerLimitHits": 1234567,
                     "sourceBackpressured": 0.05, "sourceIdle": 0.01}
                    for n in (1, 2, 4) for i in (1, 2)]
            out = {"runs": runs, "table": L.build_table(runs)}
            wide = [ln for ln in L.scorecard(out).splitlines() if len(ln) > limit]
            assert not wide, (f"the scorecard runs to {max(len(x) for x in wide)} characters; "
                              f"the limit is {limit}. Put the words under the table, not in the row.")
        return go

    expect("the scorecard stays under 136 characters (must not fire)",
           scorecard_width(), "", should_fire=False)

    def sizes(byts, want):
        def go():
            got = L.gib_str(byts)
            assert got == want, f"mib_str({byts}) said {got!r}, expected {want!r}"
        return go

    expect("size: 4 GiB reads as 4g (must not fire)", sizes(4 * 1024**3, "4g"), "", should_fire=False)
    expect("size: 6 GiB reads as 6g (must not fire)", sizes(6 * 1024**3, "6g"), "", should_fire=False)
    expect("size: 6400 MiB reads as 6.25g (must not fire)",
           sizes(6400 * 1024**2, "6.25g"), "", should_fire=False)

    def from_record(kw, want):
        """The Kafka memory column reads the run's own limit, not the config."""
        r = dict(good); r.update(kw)

        def go():
            shown = L.gib_str(r.get("brokerLimitBytes"))
            assert shown == want, f"column would show {shown!r}, the run recorded {want!r}"
        return go

    expect("the Kafka memory column comes from the record (must not fire)",
           from_record(dict(brokerLimitBytes=4 * 1024**3), "4g"), "", should_fire=False)

    def acts(kw, want):
        r = dict(good); r.update(kw)

        def go():
            got = L.corrective_action(r)
            assert want in got, f"action said {got!r}, expected {want!r}"
        return go

    def steps(kw, step, baseline, want):
        r = dict(good); r.update(kw)

        def go():
            got = L.corrective_action(r, step, baseline)
            assert want in got, f"action said {got!r}, expected {want!r}"
        return go

    met = dict(reportable=True, meetsClaim=True, ratio=1.95, idealRatio=2.0, ratioLowCI=1.91)
    short = dict(reportable=True, meetsClaim=False, ratio=1.53, idealRatio=2.0, ratioLowCI=1.41)
    over = dict(reportable=True, meetsClaim=True, ratio=2.76, idealRatio=2.0, ratioLowCI=2.41)
    def detail(kw, cores, step, baseline, want):
        r = dict(good); r.update(kw)

        def go():
            got = L.action_detail(r, cores, step, baseline)
            assert got and want in got, f"detail said {got!r}, expected {want!r}"
        return go

    expect("detail: the baseline says why (must not fire)",
           detail(dict(tmCapFrac=0.99), 1, None, True, "Do not tune it"), "", should_fire=False)
    expect("detail: a short step names both numbers (must not fire)",
           detail(dict(tmCapFrac=0.99), 4, short, False, f"1.53x, short of the {2 * L.T['scalingFloor']:.2f}x"),
           "", should_fire=False)
    expect("detail: a step above 2x says the smaller case reads low (must not fire)",
           detail(dict(tmCapFrac=0.99), 2, over, False, "reads too low"), "", should_fire=False)
    expect("detail: Kafka memory names the size to try (must not fire)",
           detail(dict(tmCapFrac=0.93, brokerLimitHits=12780, brokerLimitBytes=4096 * 1048576),
                  2, None, False, "from 4g to about 6.25g"), "", should_fire=False)
    expect("action: the baseline has no step into it (must not fire)",
           steps(dict(tmCapFrac=0.99), None, True, "check it matches"), "", should_fire=False)
    expect("action: a step that doubled needs nothing (must not fire)",
           steps(dict(tmCapFrac=0.99), met, False, "add cores"), "", should_fire=False)
    # "investigate" said nothing: clean-room run 44 was told it twice while
    # every column beside it said the pipeline was the constraint, the broker
    # was idle and the GC was at 0.3%. Each of the three now names what it is.
    expect("action: a step short of the target points at the host (must not fire)",
           steps(dict(tmCapFrac=0.99), short, False, "check the host"), "", should_fire=False)
    expect("action: a step above 2x says the baseline reads low (must not fire)",
           steps(dict(tmCapFrac=0.99), over, False, "baseline reads low"), "", should_fire=False)
    expect("action: says so when there is no usable step (must not fire)",
           acts(dict(tmCapFrac=0.99), "no usable step"), "", should_fire=False)
    expect("action: names the Kafka memory to try, in gigabytes (must not fire)",
           acts(dict(tmCapFrac=0.93, brokerLimitHits=12780, brokerLimitBytes=4096 * 1048576),
                "raise kafkaMemory"), "", should_fire=False)
    expect("action: more memory for the pipeline (must not fire)",
           acts(dict(tmCapFrac=0.99, gcFracOfCapacity=0.064), "more memory"), "", should_fire=False)
    expect("action: compress the writes (must not fire)",
           acts(dict(tmCapFrac=0.9495, sourceBackpressured=0.6738), "compress"), "", should_fire=False)
    expect("bottleneck: CPU is the block (must not fire)",
           names(dict(tmCapFrac=0.99), "blocking higher throughput"), "", should_fire=False)
    expect("bottleneck: Kafka out of memory (must not fire)",
           names(dict(tmCapFrac=0.93, brokerLimitHits=12780), "Kafka's memory"),
           "", should_fire=False)
    expect("bottleneck: out of memory (must not fire)",
           names(dict(tmCapFrac=0.99, gcFracOfCapacity=0.064), "cleaning up memory"),
           "", should_fire=False)
    expect("bottleneck: waiting to write (must not fire)",
           names(dict(tmCapFrac=0.9495, sourceBackpressured=0.6738), "Waiting to write to Kafka"),
           "", should_fire=False)
    expect("bottleneck: unknown says investigating (must not fire)",
           names(dict(tmCapFrac=0.80, sourceBackpressured=0.05, sourceIdle=0.02), "Investigating"),
           "", should_fire=False)
    expect("bottleneck: waiting for input (must not fire)",
           names(dict(tmCapFrac=0.80, sourceIdle=0.40), "Nothing to read"),
           "", should_fire=False)
    expect("window has < 3 commit boundaries", case(boundaries=2), "commit boundaries")
    expect("measured rate is zero", case(recordsConsumed=0), "not positive")
    expect("rate came from the engine", case(rateSource="engine numRecordsIn"), "engine")
    expect("two vantage points disagree", case(vantageDisagreement=0.12), "do not agree")
    expect("the data ran out before the window closed",
           case(backlogRemaining=1000, headroomS=0.006), "the data ran out before the measurement finished")
    expect("external-boundary samples missing", case(sourceIdle=None), "no back-pressure reading")
    expect("too few reporter samples in the window", case(bpSamples=2), "readings landed inside")
    expect("cores are not the constraint (baseline, run 5\'s 94%)", case(tmCapFrac=0.94, _baseline=True), "only used", ceiling=True)
    expect("baseline at 95.9% is the constraint (run 12 p3; must not fire)",
           case(tmCapFrac=0.959, _baseline=True), "", should_fire=False)
    expect("cores are not the constraint (other)", case(tmCapFrac=0.90), "only used", ceiling=True)
    expect("source idle past the ceiling", case(sourceIdle=0.4), "waiting for input", ceiling=True)
    expect("garbage collection is the constraint", case(gcFracOfCapacity=0.13), "garbage collection", ceiling=True)
    expect("GC at the worst level that behaved, 4.8% (must not fire)",
           case(gcFracOfCapacity=0.048), "", should_fire=False)
    # The machine's swap, read at each window's open and close. Real readings:
    # run 47's broker test, arm A, four-core passes (rate, swap MB at open, at
    # close), and run 48 chain 3's one-core passes on a quiet machine.
    SWAP_47 = [(227272, 4840.94, 6187.06), (282954, 4734.88, 4620.38),
               (243496, 4529.06, 4394.69), (428504, 2375.56, 2018.31)]
    SWAP_48 = [(129601, 2467.75, 2459.75), (125597, 2315.75, 2291.75),
               (129686, 2275.75, 2267.75), (130460, 2251.75, 2251.75)]
    def swapcase(rows, cores, reportable, want):
        def go():
            runs = [dict(cores=cores, recordsPerSec=r, hostSwapOpenMB=o, hostSwapCloseMB=c) for r, o, c in rows]
            got = L.swap_note(runs, {str(cores): dict(cores=cores, reportable=reportable)})
            if want is None:
                assert not got, f"said {got!r} where swap did not split the passes"
            else:
                assert got and all(w in got[0] for w in want), f"said {got!r}, expected {want!r}"
        return go
    expect("swap: run 47's slow passes are put down to the machine's memory (must not fire)",
           swapcase(SWAP_47, 4, False, ["227,272/s, 243,496/s, 282,954/s", "4.3-6.0 GB", "428,504/s", "2.0-2.3 GB",
                                        "short of memory, not the pipeline"]), "", should_fire=False)
    expect("swap: a quiet machine says nothing about swap (must not fire)",
           swapcase(SWAP_48, 1, False, None), "", should_fire=False)
    expect("swap: a case that counts gets no swap sentence (must not fire)",
           swapcase(SWAP_47, 4, True, None), "", should_fire=False)
    # Finding F10, run 48: `orders-*` matched orders-tiny-0 and orders-small-3,
    # so the suite's backlog was credited with 12.5 GB it did not have.
    DIRS = ["orders-0", "orders-7", "orders-tiny-0", "orders-tiny-7", "orders-small-3",
            "prices-0", "orders-2024-1", "__consumer_offsets-12", "orders"]
    def dirs(topics, want):
        def go():
            got = L.partition_dirs(DIRS, topics)
            assert got == want, f"picked {got!r}, expected {want!r}"
        return go
    expect("disk: a topic's bytes are its own partitions only (must not fire)",
           dirs(["orders"], ["orders-0", "orders-7"]), "", should_fire=False)
    expect("disk: a longer topic with the same start is its own (must not fire)",
           dirs(["orders-tiny"], ["orders-tiny-0", "orders-tiny-7"]), "", should_fire=False)
    expect("disk: two topics together (must not fire)",
           dirs(["orders", "prices"], ["orders-0", "orders-7", "prices-0"]), "", should_fire=False)
    # Finding F8, run 48: a generator whose producer died 7.5 s in was waited on
    # for 29 minutes in silence. samples are (seconds since start, records).
    def stall(samples, now, want):
        def go():
            got = L.fill_stall_reason([(t, n) for t, n in samples], now, "orders")
            if want is None:
                assert got is None, f"stopped a healthy fill: {got!r}"
            else:
                assert got and want in got, f"said {got!r}, expected {want!r}"
        return go
    steady = [(i * 30, i * 30 * 2_000_000) for i in range(10)]
    expect("fill: a fill that keeps growing runs on (must not fire)",
           stall(steady, 300, None), "", should_fire=False)
    expect("fill: 6.7 minutes before the first record, like run 48's price feed, runs on (must not fire)",
           stall([(i * 30, 0) for i in range(14)], 402, None), "", should_fire=False)
    expect("fill: nothing written in 10 minutes is stopped (must not fire)",
           stall([(i * 30, 0) for i in range(21)], 600, "written nothing to orders in 10 minutes"),
           "", should_fire=False)
    expect("fill: a fill that stopped growing for 5 minutes is stopped (must not fire)",
           stall([(0, 0), (30, 4_000_000)] + [(30 + i * 30, 4_000_000) for i in range(1, 11)], 330,
                 "has held at 4,000,000 records for 5 minutes"), "", should_fire=False)
    expect("fill: a 4-minute pause after writing runs on (must not fire)",
           stall([(0, 0), (30, 4_000_000)] + [(30 + i * 30, 4_000_000) for i in range(1, 9)], 270, None),
           "", should_fire=False)
    # Confluent's broker image keeps its tools in /usr/bin with no .sh; the
    # harness sent every command to Apache's /opt/kafka/bin (cp-kafka:7.7.0).
    def tools(found, want):
        def go():
            got = L.kafka_tool_path(found)
            assert got == want, f"read {found!r} as {got!r}, expected {want!r}"
        return go
    expect("kafka tools: the Apache image (must not fire)",
           tools("/opt/kafka/bin/kafka-topics.sh\n", ("/opt/kafka/bin", ".sh")), "", should_fire=False)
    expect("kafka tools: the Confluent image (must not fire)",
           tools("/usr/bin/kafka-topics\n", ("/usr/bin", "")), "", should_fire=False)
    expect("kafka tools: nothing found is not guessed at (must not fire)",
           tools("", None), "", should_fire=False)
    def pinned(props, want):
        def go():
            got = L.pin_collector(props).get("env.java.opts.taskmanager")
            assert got == want, f"{props!r} gave {got!r}, expected {want!r}"
        return go
    expect("collector: none named, G1 on every case (must not fire)",
           pinned({}, "-XX:+UseG1GC"), "", should_fire=False)
    expect("collector: other options kept, G1 added (must not fire)",
           pinned({"env.java.opts.taskmanager": "-Xss1m"}, "-Xss1m -XX:+UseG1GC"), "", should_fire=False)
    expect("collector: G1 already named is kept as it is (must not fire)",
           pinned({"env.java.opts.taskmanager": "-XX:+UseG1GC -Xss1m"}, "-XX:+UseG1GC -Xss1m"), "", should_fire=False)
    def other_gc():
        L.pin_collector({"env.java.opts.taskmanager": "-XX:+UseParallelGC"})
    expect("collector: any collector but G1 in the config stops the run", other_gc, "G1 only")
    expect("collector: a pass that reported Serial stops the suite",
           case(gcNames=["All", "Copy", "MarkSweepCompact"]), "not G1")
    expect("collector: a pass that reported G1 is fine (must not fire)",
           case(gcNames=["All", "G1 Young Generation", "G1 Old Generation"]), "", should_fire=False)
    def mixed_gc():
        def run(c, p, rate, gc):
            return dict(cores=c, **{"pass": p}, recordsPerSec=rate, status="OK", gcNames=["All"] + gc)
        serial, g1 = ["Copy", "MarkSweepCompact"], ["G1 Old Generation", "G1 Young Generation"]
        runs = [run(1, "p1-asc", 56299, serial), run(1, "p2-desc", 59341, serial),
                run(2, "p1-asc", 117954, g1), run(2, "p2-desc", 118055, g1)]
        t = L.build_table(runs, [1, 2])
        st = t["stepRatios"][0]
        assert st["reportable"] is False and "garbage collector" in st["reason"], st
    expect("collector: a step between a Serial and a G1 case is not counted (must not fire)",
           mixed_gc, "", should_fire=False)
    def libs(image, want):
        def go():
            got = L.default_kafka_libs(image)
            assert got == want, f"{image} got {got!r}, expected {want!r}"
        return go
    expect("kafka jars: left out, Apache's path for the Apache image (must not fire)",
           libs("apache/kafka:3.9.0", "/opt/kafka/libs"), "", should_fire=False)
    expect("kafka jars: left out, Confluent's path for the Confluent image (must not fire)",
           libs("confluentinc/cp-kafka:7.7.0", "/usr/share/java/kafka"), "", should_fire=False)
    def tools_only(names, want):
        def go():
            got = L.vendor_only_classes(names)
            assert got == want, f"found {got!r}, expected {want!r}"
        return go
    expect("either Kafka: a plain Apache-client build has nothing vendor-only (must not fire)",
           tools_only(["org/apache/kafka/clients/producer/KafkaProducer.class", "st48/Job.class"], []),
           "", should_fire=False)
    expect("either Kafka: Confluent serializers are found (must not fire)",
           tools_only(["io/confluent/kafka/serializers/KafkaAvroSerializer.class", "io/confluent/x.txt"],
                      ["io/confluent/kafka/serializers/KafkaAvroSerializer.class"]), "", should_fire=False)
    # Finding F10, run 49: the tiny proof advised more broker memory for a case
    # at 100.1% of its cap. Cases (cores, cap used, broker limit hits):
    def held(cases, want_cores):
        def go():
            got = L.broker_held_back([dict(cores=c, tmCapFrac=f, brokerLimitHits=h) for c, f, h in cases])
            assert (got or {}).get("cores") == want_cores, f"picked {got!r}, expected cores {want_cores}"
        return go
    expect("broker advice: run 49's hits on cases at their cap ask for nothing (must not fire)",
           held([(1, 1.002, 381), (2, 0.992, 0), (4, 1.001, 0)], None), "", should_fire=False)
    expect("broker advice: a case under its cap with hits is the one named (must not fire)",
           held([(1, 0.99, 2000), (4, 0.93, 30927)], 4), "", should_fire=False)
    expect("the broker was starved of page cache (cores off their cap)",
           case(brokerLimitHits=310423, brokerRefaults=6270562, tmCapFrac=0.93), "hit its memory limit", ceiling=True)
    expect("cores off their cap with no broker hits still says something else held it back",
           case(tmCapFrac=0.93), "Something else was holding it back", ceiling=True)
    # The 99% exemption this replaced threw out twelve recorded passes at 95-99%
    # of cap, every one within -4.7% to +5.2% of the passes kept beside it.
    expect("broker limit hits at 97.9% of cap: the pass is kept (must not fire)",
           case(brokerLimitHits=1494, brokerRefaults=90000, tmCapFrac=0.979), "", should_fire=False)
    expect("broker limit hits at exactly the cap floor: the pass is kept (must not fire)",
           case(brokerLimitHits=4496, brokerRefaults=300000, tmCapFrac=0.95), "", should_fire=False)
    expect("broker limit hits while the cores are pinned (must not fire)",
           case(brokerLimitHits=9437, brokerRefaults=572000, tmCapFrac=0.996), "", should_fire=False)
    expect("a broker that never hit its limit (must not fire)",
           case(brokerLimitHits=0, brokerRefaults=1200), "", should_fire=False)
    def shape_same_job_new_id():
        """The same graph at a different size, which is every case after the
        first. The plan carries a fresh job id per submission and the
        parallelism being varied, so comparing it refused every pipeline."""
        a = {"vertexCount": 2, "signature": [["x", []]], "maxParallelism": [128],
             "plan": {"jid": "aaaaaaaa", "nodes": [{"id": "n", "description": "x",
                                                    "parallelism": 1, "inputs": []}]}}
        b = {"vertexCount": 2, "signature": [["x", []]], "maxParallelism": [128],
             "plan": {"jid": "bbbbbbbb", "nodes": [{"id": "n", "description": "x",
                                                    "parallelism": 4, "inputs": []}]}}
        L.check_shape(a, b)
    expect("the same graph at a new job id and size passes (must not fire)",
           shape_same_job_new_id, "", should_fire=False)

    expect("job graph differs across cases",
           lambda: L.check_shape({"vertexCount": 3, "signature": [["a", ["HASH"]]]},
                                 {"vertexCount": 3, "signature": [["a", ["REBALANCE"]]]}), "shape")

    def spread():
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 2, "pass": "p2", "recordsPerSec": 130.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0},
                         {"cores": 4, "pass": "p2", "recordsPerSec": 205.0}])
        c2, step = t["cases"][2], t["stepRatios"][0]
        if c2["reportable"] or step["reportable"] or t["cases"][4]["reportable"] is not True:
            raise Exception(f"spread guard did not void the case and only the case: {t}")
        raise Refusal("case", c2["unreportableReason"])
    expect("a case's readings are too far apart", spread, "readings are 26% apart")

    def one_pass():
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0}])
        if t["cases"][2]["reportable"]:
            raise Exception("single pass was reportable")
        raise Refusal("case", t["cases"][2]["unreportableReason"])
    expect("a case measured only once", one_pass, "only 1 usable reading")

    def split_commit_boundary():
        """REGRESSION, from the rig's own ticks (2026-09-05, 4c, 10 s checkpoints):
        a commit arrived as +1,902,797 then +5,672,429 half a second later, the
        window closed on the first piece, and the vantage points disagreed by 33%."""
        ticks = [{"ts": 39590, "committed": 54613970}, {"ts": 49600, "committed": 56516767},
                 {"ts": 50100, "committed": 62189196}, {"ts": 59610, "committed": 69800000}]
        got = L.settle_boundary(ticks, ticks[1], 1500.0)
        if got["committed"] != 62189196:
            raise Exception(f"the window closed on half a commit: {got}")
        whole = L.settle_boundary(ticks, ticks[3], 1500.0)
        if whole["committed"] != 69800000:
            raise Exception(f"a settled boundary was dragged forward into the next commit: {whole}")
        clean = [{"ts": 1000, "committed": 100}, {"ts": 11000, "committed": 200}]
        if L.settle_boundary(clean, clean[0], 1500.0)["ts"] != 1000:
            raise Exception("a boundary with nothing to settle was moved")
    expect("a commit that lands in two pieces (must not fire)", split_commit_boundary, "", should_fire=False)

    def quick_does_not_leak(): 
        """REGRESSION (2026-09-05): with the flag set process-wide, the replay
        re-derived the record with minPasses bypassed and three recorded-invalid
        suites would have been reported, and this file's own single-pass guard
        stopped firing. Both correctly refused a run. Quickness belongs to one
        table, never to the record or the guards."""
        prev = L.QUICK          # restore, never assume: setting this False at the
        L.QUICK = True          # end turned quick mode off for the rest of a live
        try:                    # chain on 2026-09-05, and the suite voided itself
            if cmd_replay() != 0:
                raise Exception("the replay disagreed with the record while --quick was set")
            t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                             {"cores": 4, "pass": "p1", "recordsPerSec": 200.0}])
            if t["cases"][2]["reportable"] or t.get("quickLook"):
                raise Exception(f"the flag leaked into a table that did not ask for it: {t['cases'][2]}")
        finally:
            L.QUICK = prev
        if L.QUICK != prev:
            raise Exception("the self-test did not restore the quick flag")
    expect("quick look: the flag reaches neither the record nor the guards (must not fire)",
           quick_does_not_leak, "", should_fire=False)

    def quick_two_passes():
        """REGRESSION (2026-09-06): --quick measured each case once, and a ratio
        built from two single measurements wandered 8.3% against the same
        build's three-pass answer. It now measures twice."""
        if L.T["quickPasses"] < 2:
            raise Exception(f"quick mode would report a ratio from {L.T['quickPasses']} pass(es)")
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 2, "pass": "p2", "recordsPerSec": 104.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0},
                         {"cores": 4, "pass": "p2", "recordsPerSec": 208.0}], quick=True)
        if t["cases"][2]["spread"] <= 0 or not t["stepRatios"][0]["reportable"]:
            raise Exception(f"a two-pass quick table must carry a spread and a ratio: {t['cases'][2]}")
    expect("quick look: two passes, so every case has a spread (must not fire)",
           quick_two_passes, "", should_fire=False)

    def quick_marks_unpublishable():
        """QUICK: one pass per case still produces numbers, and every one of them
        is stamped unpublishable. The mode exists to answer 'did it run clean and
        how fast', and the record says a single pass lands anywhere in a band
        wider than the accept line (2.04-2.27x where a suite reported 2.15x)."""
        t = build_table([{"cores": 2, "pass": "p1-asc", "recordsPerSec": 100.0},
                         {"cores": 4, "pass": "p1-asc", "recordsPerSec": 200.0}], quick=True)
        c2, step = t["cases"][2], t["stepRatios"][0]
        if t.get("publishable") is not False or t.get("quickLook") is not True:
            raise Exception(f"quick table was not stamped: {t.get('quickLook')} {t.get('publishable')}")
        if c2.get("publishable") is not False or not c2["reportable"] or not step["reportable"]:
            raise Exception(f"quick mode should compute the ratio and mark it unpublishable: {c2} {step}")
        if abs(step["ratio"] - 2.0) > 1e-9:
            raise Exception(f"quick ratio wrong: {step}")
    expect("quick look: one pass computes a ratio, stamped unpublishable (must not fire)",
           quick_marks_unpublishable, "", should_fire=False)

    def sentinel_drift():
        runs = [{"cores": 2, "pass": "p1-asc", "recordsPerSec": 400.0},
                {"cores": 4, "pass": "p1-asc", "recordsPerSec": 800.0},
                {"cores": 4, "pass": "p2-desc", "recordsPerSec": 790.0},
                {"cores": 2, "pass": "p2-desc", "recordsPerSec": 395.0},
                {"cores": 2, "pass": "sentinel", "recordsPerSec": 300.0}]
        t = build_table(runs)
        sd = t["sentinel"]
        if sd is None or abs(sd["drift"] - (300.0 - 400.0) / 350.0) > 1e-3:
            raise Exception(f"sentinel drift not computed: {sd}")
        if t["cases"][2]["reportable"]:
            raise Exception("a 25% first-to-last drift left the baseline reportable")
        raise Refusal("case", f"sentinel drift {sd['drift']:.1%}: " + t["cases"][2]["unreportableReason"])
    expect("sentinel: rig drifted across the suite", sentinel_drift, "sentinel drift")

    def sentinel_ok():
        runs = [{"cores": 2, "pass": "p1-asc", "recordsPerSec": 400.0},
                {"cores": 4, "pass": "p1-asc", "recordsPerSec": 800.0},
                {"cores": 2, "pass": "sentinel", "recordsPerSec": 370.0}]
        t = build_table(runs)
        if not t["cases"][2]["reportable"] or t["sentinel"]["drift"] > 0:
            raise Exception(f"an 8% drift (inside the 10% noise floor) was refused: {t['sentinel']}")
    expect("sentinel: drift inside the noise floor (must not fire)", sentinel_ok, "", should_fire=False)

    def watcher():
        import subprocess
        os.makedirs(c.results, exist_ok=True)
        probe = os.path.join(c.results, "selftest-watcher.log")
        open(probe, "a").close()
        # three shapes of watcher: names the project on its command line; holds a
        # results file open; names prove.py and runs from inside the project (run 11's shells)
        w = subprocess.Popen(["bash", "-c", f"while true; do ls {c.results} >/dev/null; sleep 5; done"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd="/")
        t = subprocess.Popen(["tail", "-f", probe], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True, cwd="/")
        u = subprocess.Popen(["bash", "-c", "while true; do pgrep -f 'prove.py suite' >/dev/null; sleep 5; done"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd=c.root)
        # and two that must NOT be touched: another project's harness elsewhere, and a
        # bystander shell merely sitting in the project directory
        other = subprocess.Popen(["bash", "-c", "exec -a 'python3 harness/prove.py tinyproof' sleep 300"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd="/")
        bystander = subprocess.Popen(["bash", "-c", "sleep 300"], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True, cwd=c.root)
        time.sleep(0.5)
        # they are our children here; the suite's watchers are not, so look at them as strangers
        found = {pid for pid, _ in L.host_watchers(ignore_children=True)}
        want = {w.pid, t.pid, u.pid}
        if not want <= found or other.pid in found or bystander.pid in found:
            for x in (w, t, u, other, bystander):
                x.kill()
            raise Exception(f"host_watchers wanted {sorted(want)} and not {other.pid}/{bystander.pid}; got {sorted(found)}")
        # only what this test started: anything else naming this directory could
        # be a live chain launched from the same harness
        L.reap_host_watchers(ignore_children=True, only=want)
        time.sleep(0.5)
        left = {pid for pid, _ in L.host_watchers(ignore_children=True)} & want
        other_alive = other.poll() is None and bystander.poll() is None
        other.kill(); bystander.kill()
        try:
            os.remove(probe)
        except OSError:
            pass
        if left:
            raise Exception(f"watchers survived reaping: {left}")
        if not other_alive:
            raise Exception("another project's harness, or a bystander shell in the project directory, was killed")
        raise Refusal("rig", f"host processes watching this run were found and killed: {sorted(want)}")
    expect("a host-side watcher outlives the run", watcher, "watching this run")

    # run 11's build A: 210.6 B/input, 607 B of sink per input, 200M backlog, 8 partitions,
    # two sinks, 103 GB free. Retention caps the sinks at 34.4 GB; it fitted, and the
    # rebuild that re-ran both gates was not needed.
    run11 = dict(in_bytes_per_rec=210.6, backlog=200_000_000, sink_bytes_per_in=607.0, partitions=8,
                 n_out_topics=2, ckpt_bytes=1e9)
    expect("disk: run 11's build A fitted (must not fire)",
           lambda: L.disk_verdict(103e9, **run11), "", should_fire=False)
    expect("disk: the suite would not fit", lambda: L.disk_verdict(60e9, **run11), "of disk and only")

    acct128 = ACCOUNT_KEY_LAYOUT_128
    acct_sets = {"positions-by-account": sorted(acct128)}
    floor = L.T["scalingFloor"]

    def skew_counts():
        sp = L.key_spread(acct_sets, [1, 2, 4], acct128, 128)
        got = sp["stages"]["positions-by-account"]["cases"][4]["keysPerSubtask"]
        if got != [5, 3, 4, 4]:
            raise Exception(f"Flink 1.20.1 put the demo's account keys {got}, the record says [5, 3, 4, 4]")
        if abs(sp["worst"]["stageCeiling"] - 0.8) > 0.001:
            raise Exception(f"ceiling {sp['worst']['stageCeiling']}")
    expect("key layout: the demo's own 5/3/4/4 is measured, not assumed (must not fire)",
           skew_counts, "", should_fire=False)

    def skew_refuses():
        sp = L.key_spread(acct_sets, [1, 2, 4], acct128, 128)
        bad = L.key_skew_verdict(sp, floor, [1115])
        if bad:
            raise bad
    expect("keys do not divide evenly and a maxParallelism would fix it",
           skew_refuses, "cannot return more than 0.80 of linear")

    def skew_reports():
        # the same layout with nothing that would fix it: a property of the key
        # space, reported in the row rather than refused in a message nobody
        # can act on
        sp = L.key_spread(acct_sets, [1, 2, 4], acct128, 128)
        bad = L.key_skew_verdict(sp, floor, [])
        if bad:
            raise bad
    expect("an uneven layout with no fix available is reported, not thrown out (must not fire)",
           skew_reports, "", should_fire=False)

    def skew_idle():
        lay = {}
        for k, where in acct128.items():
            lay[k] = dict(where)
            lay[k][4] = min(where[4], 2)                      # nothing lands on subtask 3
        sp = L.key_spread(acct_sets, [1, 2, 4], lay, 128)
        bad = L.key_skew_verdict(sp, floor, [1115])
        if bad:
            raise bad
    expect("a subtask would get no keys at all", skew_idle, "no keys at all")

    def skew_even():
        # 4/4/4/4: what pipeline.max-parallelism 1115 gives the same keys
        lay = {k: {1: 0, 2: i % 2, 4: i % 4} for i, k in enumerate(sorted(acct128))}
        sp = L.key_spread(acct_sets, [1, 2, 4], lay, 1115)
        if sp["worst"]["stageCeiling"] != 1.0 or sp["idle"]:
            raise Exception(f"even layout judged {sp['worst']}")
        bad = L.key_skew_verdict(sp, floor, [1115])
        if bad:
            raise bad
    expect("an even key layout passes (must not fire)", skew_even, "", should_fire=False)
    # clean-room run 36's own measured shape (its results/tinyproof.json): 172.3 B
    # per input record, a 220M backlog = 37.9 GB, sinks capped by retention at
    # 34.4 GB, so the suite needs 92.3 GB. Re-running the tiny proof after the fill
    # left 77.3 GB free and the projection asked for the backlog a second time,
    # so it refused a suite that fitted and every tuning lever cost a delete and a
    # re-fill -- about 12 minutes of broker I/O each (feedback 5).
    run36 = dict(in_bytes_per_rec=172.3, backlog=220_000_000, sink_bytes_per_in=284.8,
                 partitions=8, n_out_topics=2, ckpt_bytes=151552)
    expect("disk: the backlog is projected twice after a fill",
           lambda: L.disk_verdict(77.3e9, **run36), "of disk and only")
    expect("disk: the tiny proof re-runs once the backlog on disk is credited (must not fire)",
           lambda: L.disk_verdict(77.3e9, input_on_disk_bytes=37.9e9, **run36), "", should_fire=False)
    expect("disk: crediting the backlog does not excuse a suite that still will not fit",
           lambda: L.disk_verdict(40e9, input_on_disk_bytes=37.9e9, **run36), "still to write")

    def wording_above_target():
        """A ratio above its target is not short of it. Clean-room run 36's
        scorecard called 1.93x short of 1.90x, on the same page as a step line
        that got it right. Read from the target, so it cannot drift again."""
        rec = {"tmCapFrac": 0.99, "tmThrottledPeriodsPct": 40, "sourceBackpressured": 0.0,
               "gcFraction": 0.01, "brokerLimitHits": 0}
        # both ways round: with a lower bound to name, and without one. The
        # second is the path that still called 1.93x "short of" 1.90x, because
        # the branch that gets it right was reached only when a lower bound
        # existed to quote.
        target = 2 * L.T["scalingFloor"]
        above = round(target + 0.03, 2)
        for lo in (round(target - 0.004, 3), None):
            step = {"reportable": True, "ratio": above, "idealRatio": 2.0,
                    "ratioLowCI": lo, "meetsClaim": False}
            line = L.action_detail(rec, 2, step=step) or ""
            if "short of" in line:
                raise Exception(f"{above}x called short of its target (lowCI={lo}): {line}")
            if f"{target:.2f}" not in line or f"{above:.2f}" not in line:
                raise Exception(f"the line names neither number (lowCI={lo}): {line}")
    expect("scorecard: a ratio above its target is not called short of it (must not fire)",
           wording_above_target, "", should_fire=False)

    def wording_below_target():
        rec = {"tmCapFrac": 0.99, "tmThrottledPeriodsPct": 40, "sourceBackpressured": 0.0,
               "gcFraction": 0.01, "brokerLimitHits": 0}
        step = {"reportable": True, "ratio": 1.74, "idealRatio": 2.0,
                "ratioLowCI": 1.64, "meetsClaim": False}
        line = L.action_detail(rec, 4, step=step) or ""
        if "short of" not in line:
            raise Exception(f"1.74x against a {2 * L.T['scalingFloor']:.2f}x target does not say short of: {line}")
    expect("scorecard: a ratio below its target still says so (must not fire)",
           wording_below_target, "", should_fire=False)

    def probe_envelope_vs_middle():
        """Run 36 was told to tighten the range by raising the repeats and read
        9% at 3 then 15% at 9. Min to max cannot narrow; the middle half can."""
        three = {"repeats": 3, "ofLinearRange": {"mem": {"2->4": {"spread": 0.09, "middleHalf": None}}}}
        nine = {"repeats": 9, "ofLinearRange": {"mem": {"2->4": {
            "spread": 0.15, "middleHalf": {"low": 0.80, "high": 0.84, "spread": 0.04}}}}}
        a, b = L.probe_spread(three), L.probe_spread(nine)
        if a["middleHalf"] is not None:
            raise Exception("three repeats produced a middle half")
        if b["middleHalf"] != 0.04 or b["envelope"] != 0.15:
            raise Exception(f"nine repeats read {b}")
        if any("tighten" in ln for ln in L.probe_advice(b, 0.09)):
            raise Exception("still telling a 9-repeat probe to tighten its range")
        # per arm: the steady arm must not be hidden behind the unsteady one.
        # Measured on this rig at 25 repeats: register-only middle half 0%,
        # memory-bound 10%, and the worst-of figure made both look unusable.
        both = {"repeats": 25, "ofLinearRange": {
            "alu": {"2->4": {"spread": 0.03, "middleHalf": {"low": .98, "high": .985, "spread": 0.005}}},
            "mem": {"2->4": {"spread": 0.15, "middleHalf": {"low": .73, "high": .83, "spread": 0.10}}}}}
        if L.probe_spread(both, mode="alu")["middleHalf"] != 0.005:
            raise Exception("the steady arm cannot be read on its own")
        if L.probe_spread(both)["middleHalf"] != 0.10:
            raise Exception("the across-arms figure is no longer the worst one")
        if "tighter than the shortfall" not in " ".join(
                L.probe_advice(L.probe_spread(both, mode="alu"), 0.05)):
            raise Exception("a steady arm inside the shortfall is not called usable")
        if "middle half" not in " ".join(L.probe_advice(a, 0.05)):
            raise Exception("a 3-repeat probe is not pointed at the middle half")
        if "tighter than the shortfall" not in " ".join(L.probe_advice(b, 0.20)):
            raise Exception("a middle half inside the shortfall is not called usable")
    expect("probe: the range is an envelope, the middle half is the figure that tightens (must not fire)",
           probe_envelope_vs_middle, "", should_fire=False)

    def held_still_memory():
        """suite.json records what each case was given, never Cfg.tm_mem's
        default. Run 36's heldStill said 4096m while its own scorecard said
        1728m / 2368m / 3648m."""
        got = L.tm_memory_record()
        if got == "4096m":
            raise Exception("heldStill still records Cfg.tm_mem's default")
        if c.tm_mem_per_core and not isinstance(got, dict):
            raise Exception(f"memory is a base plus a per-core share and heldStill records one "
                            f"figure, {got!r}")
        if isinstance(got, dict):
            per = got["perCase"]
            for n in c.cases:
                if per[str(n)] != L.tm_mem_of(n):
                    raise Exception(f"heldStill says {per[str(n)]} at {n} cores, the case gets "
                                    f"{L.tm_mem_of(n)}")
        elif got is not None and got != L.tm_mem_of(c.cases[0]):
            raise Exception(f"heldStill says {got}, the cases get {L.tm_mem_of(c.cases[0])}")
    expect("suite.json records the memory each case was given (must not fire)",
           held_still_memory, "", should_fire=False)

    # A plan shaped like the one clean-room run 36 actually recorded (the vertex
    # descriptions and ship strategies are verbatim from its suite.json; the
    # node ids are made up, because the plan itself was not kept -- which is
    # why graph_shape keeps it now). Its third vertex chains ONE sink-mv where
    # the business case asks for two.
    RUN36_PLAN = {"nodes": [
        {"id": "a1", "description": "Source: kafka-source-trades<br/>+- parse-order<br/>", "inputs": []},
        {"id": "a2", "description": "Source: price-ticks<br/>+- parse-price<br/>", "inputs": []},
        {"id": "a3", "description": "positions<br/>:- sink-positions-by-symbol: Writer<br/>"
                                    ":  +- sink-positions-by-symbol: Committer<br/>"
                                    "+- sink-mv: Writer<br/>   +- sink-mv: Committer<br/>",
         "inputs": [{"id": "a1", "ship_strategy": "HASH"},
                    {"id": "a2", "ship_strategy": "BROADCAST"}]}]}

    def design_catches_the_missing_output():
        design = {"operators": ["parse-order", "sink-mv-by-account"],
                  "inputs": ["orders"],
                  "outputs": ["market-values-by-symbol", "market-values-by-account"]}
        records = {"orders": 20_000_000, "market-values-by-symbol": 812,
                   "market-values-by-account": 0}
        rows, bad = L.design_diff(design, RUN36_PLAN, records, {"outputsPerInput": (5, 5)})
        if not rows:
            raise Exception("the diff produced no rows")
        if bad:
            raise bad
    expect("the build is missing an output the business case asks for",
           design_catches_the_missing_output, "market-values-by-account")

    def design_allows_extra_vertices():
        # everything declared is there; the build has more, which is allowed
        design = {"operators": ["parse-order"], "inputs": ["orders"], "outputs": ["positions"]}
        rows, bad = L.design_diff(design, RUN36_PLAN, {"orders": 20_000_000, "positions": 99},
                                  {"outputsPerInput": (5, 5)})
        if bad:
            raise bad
        if not any(r["area"] == "in the build only" for r in rows):
            raise Exception("the vertices the design did not name are not reported")
    expect("a build with more operators than the design names passes, and says so (must not fire)",
           design_allows_extra_vertices, "", should_fire=False)

    def design_catches_a_constraint():
        rows, bad = L.design_diff({"operators": ["parse-order"]}, RUN36_PLAN, {},
                                  {"outputsPerInput": (5, 8)})
        if bad:
            raise bad
    expect("the measured fan-out is not the declared one",
           design_catches_a_constraint, "outputsPerInput = 5, read back 8")

    def design_tolerates_an_input_the_fill_has_not_created():
        # The diff runs inside completeness, which is BEFORE the fill, so on a
        # cold stack the suite's own input topic does not exist yet. Both
        # shipped examples declare it, so both would refuse on their own first
        # run. Clean-room run 42 lost about 25 minutes pre-creating it by hand.
        rows, bad = L.design_diff({"inputs": ["st-readings"], "operators": ["parse-order"]},
                                  RUN36_PLAN, {}, {}, before_fill=True)
        if bad:
            raise bad
        if not any(r["area"] == "input" and "not created yet" in r["detail"] for r in rows):
            raise Exception("the row does not say the fill has not run")
    expect("a declared input the fill has not created yet (must not fire)",
           design_tolerates_an_input_the_fill_has_not_created, "", should_fire=False)

    def design_still_catches_a_missing_input_after_the_fill():
        # The tolerance above is scoped to the pre-fill phase and nothing else:
        # after the fill, a declared input that is not on the broker is still a
        # design the build did not meet.
        rows, bad = L.design_diff({"inputs": ["st-readings"]}, RUN36_PLAN, {}, {})
        if bad:
            raise bad
    expect("a declared input missing after the fill",
           design_still_catches_a_missing_input_after_the_fill, "input st-readings")

    def report_renders_with_no_constant_fan_out():
        # Clean-room run 42 lost its ENTIRE report here: a pipeline whose
        # outputs are per window leaves outputsPerInput unset, so
        # outputRecsPerSec is never written -- and the per-pass row read it
        # unconditionally while the MEAN row thirteen lines below guarded it
        # correctly. The fixture is that run's own suite.json, trimmed; it
        # carries no outputRecsPerSec key at all, which is the point of it.
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "fixtures", "suite-no-fanout.json")
        with open(path) as f:
            out = json.load(f)
        if any("outputRecsPerSec" in r for r in out["runs"]):
            raise Exception("the fixture carries the key, so it tests nothing")
        out["table"] = L.build_table(out["runs"], quick=out.get("quickLook", False))
        # The ambient pipeline.json may declare a fan-out; this pipeline does
        # not, which is the condition under test.
        saved = L._CFG.out_per_in
        L._CFG.out_per_in = None
        try:
            text = L.render_table(out)
        finally:
            L._CFG.out_per_in = saved
        if "nan" in text.lower():
            raise Exception("the table rendered a nan")
    expect("a pipeline with no constant fan-out still gets a report (must not fire)",
           report_renders_with_no_constant_fan_out, "", should_fire=False)

    def sizing_does_not_trust_a_tiny_proof_warm_up():
        # size_backlog is called from inside the tiny proof, where warmupMinS is
        # 20 s. Clean-room run 42 measured 44.2 s there, was told 401,149,826
        # records would do, and had a suite pass consume 422,499,766 -- its ten
        # suite warm-ups all ran 90 s. The floor is the suite's, not the tiny
        # proof's, whatever T says at the moment of the call.
        saved = dict(L.T)
        try:
            L.T.update(warmupMinS=20.0, minWindowS=30.0)   # as the tiny proof leaves it
            want = L.size_backlog(2_566_538.0, 4, 10.0, warmup_max_s=44.2)
        finally:
            L.T.clear(); L.T.update(saved)
        consumed, headroom = 422_499_766, int(2_566_538.0 * 10)
        if want < consumed + headroom:
            raise Exception(f"sized {want:,}, which run 42 would have overrun: it consumed "
                            f"{consumed:,} and needs {headroom:,} more at window close")
    expect("the backlog is not sized on the tiny proof's own warm-up floor (must not fire)",
           sizing_does_not_trust_a_tiny_proof_warm_up, "", should_fire=False)

    def graph_renders():
        m = L.graph_mermaid(RUN36_PLAN)
        for needle in ("flowchart LR", "HASH", "BROADCAST", "parse-order"):
            if needle not in m:
                raise Exception(f"the rendered graph has no {needle}: {m}")
        if "<br/>:-" in m or "+-" in m:
            raise Exception(f"the ASCII tree survived into the diagram: {m}")
        if L.graph_mermaid(None) or L.graph_mermaid({"nodes": []}):
            raise Exception("an empty plan rendered something")
    expect("the job graph draws from the plan the engine served (must not fire)",
           graph_renders, "", should_fire=False)

    def vantage_two_ways():
        """The second measurement of how much input was swallowed, both ways.

        A pipeline whose every output is per window -- an hourly average per
        location -- has no constant fan-out anywhere, so there is nothing to
        divide. Until the second vantage could be delegated, the harness could
        not measure such a pipeline at all.

        Fixed literals, not the live pipeline.json: read from the config, this
        test exercised whichever mode the config happened to use and threw a
        TypeError on the other. Clean-room run 41's own pipeline could not pass
        the self-test that gates its suite, for that reason.
        """
        saved_mode, saved_outs, saved_fan = c.vantage_mode, c.topics_out, c.out_per_in
        try:
            c.vantage_mode, c.topics_out, c.out_per_in = "constantFanOut", ["a", "b"], 5.0
            ticks = ({"end_a": 0, "end_b": 0}, {"end_a": 300_000, "end_b": 200_000})
            got, how = L.vantage_delta(*ticks, None, None)
            if got != 100_000.0 or "outputs per input" not in how:
                raise Exception(f"constant fan-out read {got} ({how}), expected 100000")
            c.vantage_mode = "command"
            got, how = L.vantage_delta(*ticks, 1_000, 401_000)
            if got != 400_000 or "progress command" not in how:
                raise Exception(f"delegated vantage read {got} ({how})")
        finally:
            c.vantage_mode, c.topics_out, c.out_per_in = saved_mode, saved_outs, saved_fan
    expect("the second vantage reads the same either way (must not fire)",
           vantage_two_ways, "", should_fire=False)

    def no_vantage_at_all():
        """A pipeline that declares neither is refused, rather than measured
        once and called measured."""
        import copy
        raw = copy.deepcopy(c.raw)
        raw.pop("outputsPerInput", None)
        raw.pop("secondVantage", None)
        raw["topics"] = dict(raw["topics"], out=[])
        tmp = tempfile.mkdtemp(prefix="vantage-selftest-")
        try:
            path = os.path.join(tmp, "pipeline.json")
            with open(path, "w") as f:
                json.dump(raw, f)
            L.Cfg(path)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    expect("a pipeline with no second way to measure it",
           no_vantage_at_all, "nothing about how to measure the pipeline a second way")

    def fanout_without_anything_to_count():
        import copy
        raw = copy.deepcopy(c.raw)
        raw.pop("secondVantage", None)
        raw["outputsPerInput"] = 5          # fixed, not the live file's
        raw["topics"] = dict(raw["topics"], out=[])
        tmp = tempfile.mkdtemp(prefix="vantage-selftest-")
        try:
            path = os.path.join(tmp, "pipeline.json")
            with open(path, "w") as f:
                json.dump(raw, f)
            L.Cfg(path)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    expect("outputsPerInput with no output to count it in",
           fanout_without_anything_to_count, "needs at least one topic in topics.out")

    def chain():
        # in its own directory: the first version wrote its fake chain into the
        # live results/ (phases.log, all.json and a DONE saying "STOPPED at c")
        # while a real `all` was running the live self-test around it
        ran = []
        fake = [(n, (lambda n=n: (ran.append(n), 0)[1])) for n in ("a", "b")]
        fake += [("c", lambda: (ran.append("c"), 1)[1]), ("d", lambda: (ran.append("d"), 0)[1])]
        tmp = tempfile.mkdtemp(prefix="prove-all-selftest-")
        try:
            rc = cmd_all(steps=fake, results=tmp)
            done = open(os.path.join(tmp, "DONE")).read().strip()
            allj = json.load(open(os.path.join(tmp, "all.json")))
            stray = [f for f in ("DONE", "all.json") if os.path.exists(os.path.join(c.results, f))
                     and os.path.getmtime(os.path.join(c.results, f)) > t_self]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if rc != 1 or ran != ["a", "b", "c"] or not done.startswith("STOPPED at c") or allj["verdict"] != "STOPPED at c":
            raise Exception(f"rc={rc} ran={ran} DONE={done!r} verdict={allj.get('verdict')}")
        if stray:
            raise Exception(f"the self-test wrote into the live results directory: {stray}")
        raise Refusal("rig", f"chain stopped at c, d never ran, DONE says {done!r}")
    expect("all: the chain stops at the first failing step", chain, "stopped at c")

    def no_result_is_not_a_pass():
        # clean-room run 46: ten cases measured, eight thrown out, the two
        # survivors both at four cores, stepRatios null -- and results/DONE
        # said "PASS 58.8 min". Comparing one core count with another IS the
        # measurement, so a run with only one size left has measured nothing.
        table = {"stepRatios": [{"step": "1->2", "reportable": False},
                                {"step": "2->4", "reportable": False}],
                 "cases": {"4": {"cores": 4, "reportable": True}}}
        runs = [{"cores": 1, "status": "CEILING"}, {"cores": 2, "status": "CEILING"},
                {"cores": 4, "status": "OK"}, {"cores": 4, "status": "OK"}]
        got = L.no_result_reason(table, runs)
        if not got:
            raise Exception("a run whose only surviving cases share a core count was called a result")
        if got["thrownOut"] != 2 or "4 cores" not in got["usableAt"]:
            raise Exception(f"the sentence does not describe what happened: {got}")
        # and a run that DID compare two sizes is left alone
        ok_table = {"stepRatios": [{"step": "2->4", "reportable": True}],
                    "cases": {"2": {"cores": 2, "reportable": True},
                              "4": {"cores": 4, "reportable": True}}}
        if L.no_result_reason(ok_table, runs) is not None:
            raise Exception("a run with a reportable step was called a no-result")
        raise Refusal("rig", f"no scaling result: {got['sentence']}")
    expect("a run that measured nothing does not report a pass",
           no_result_is_not_a_pass, "no scaling result")

    def broker_advice_on_a_small_machine():
        # clean-room run 45: told to raise the broker to 6,400m on a 9,937 MiB
        # VM. It did, the limit hits went to zero, and the four-core worker fell
        # from 94.6% to 77.8% of cap because the three limits no longer fit.
        class FakeRun:
            stdout = "10420092928"      # 9,937 MiB, the VM run 44 and 45 measured on
        real = L.sh
        try:
            L.sh = lambda *a, **k: FakeRun()
            why = L.broker_advice_fits(6400)
        finally:
            L.sh = real
        if not why:
            raise Exception("6,400m of broker was called affordable on a 9,937 MiB machine")
        raise Refusal("rig", why)
    expect("broker advice says when the machine cannot give it",
           broker_advice_on_a_small_machine, "cannot give it that")

    def broker_advice_on_a_big_machine():
        class FakeRun:
            stdout = "34359738368"      # 32 GB
        real = L.sh
        try:
            L.sh = lambda *a, **k: FakeRun()
            why = L.broker_advice_fits(6400)
        finally:
            L.sh = real
        if why:
            raise Exception(f"6,400m was called unaffordable on a 32 GB machine: {why}")
    expect("broker advice is silent when the machine can give it (must not fire)",
           broker_advice_on_a_big_machine, "", should_fire=False)

    def vantage_direction(sink, needle):
        # The message used to offer two causes and let the run pick. Run 45 was
        # thrown out five times out of five with the outputs behind the source,
        # while the cause it acted on predicts them ahead.
        def go():
            cf = L.cfg()
            was = cf.vantage_mode
            cf.vantage_mode = "command"
            try:
                r = dict(good)
                r.update(vantageSinkRecords=sink, vantageDisagreement=0.09)
                L.check_case(r, 4, False)
            finally:
                cf.vantage_mode = was
        return go
    expect("outputs behind the source point at checkpoint phase",
           vantage_direction(9_000_000.0, None), "BEHIND the source, which points at checkpoint phase")
    expect("outputs ahead of the source point at the step size",
           vantage_direction(11_000_000.0, None), "AHEAD of the source, which points at the step size")

    def restarting_job_says_so():
        # clean-room run 45: fifteen restarts on OutOfMemoryError, reported as
        # "the rate never settled down ... Last readings were [], drift None".
        real = L.job_health
        try:
            L.job_health = lambda jid: {"state": "RUNNING", "restored": 15,
                                        "cause": "Caused by: java.lang.OutOfMemoryError: Direct buffer memory"}
            why = L.job_is_failing("jid", 0)
        finally:
            L.job_health = real
        if not why:
            raise Exception("a job that restarted 15 times was called healthy")
        raise Refusal("rig", why)
    expect("a job that is restarting is not called an unsteady rate",
           restarting_job_says_so, "restarted 15 times")

    def dead_job_says_so():
        real = L.job_health
        try:
            L.job_health = lambda jid: {"state": "FAILED", "restored": 0,
                                        "cause": "Caused by: java.lang.OutOfMemoryError: Direct buffer memory"}
            why = L.job_is_failing("jid", 0)
        finally:
            L.job_health = real
        raise Refusal("rig", why or "no message")
    expect("a failed job is named as failed", dead_job_says_so, "the job is failed, not running")

    def healthy_job_is_left_alone():
        real = L.job_health
        try:
            L.job_health = lambda jid: {"state": "RUNNING", "restored": 2, "cause": None}
            why = L.job_is_failing("jid", 2)      # same count it started with
        finally:
            L.job_health = real
        if why:
            raise Exception(f"a healthy job was called failing: {why}")
    expect("a running job that has not restarted is left alone (must not fire)",
           healthy_job_is_left_alone, "", should_fire=False)

    def ceiling_on_top_case():
        # clean-room run 44: the 4-core case hit the broker's memory limit, was
        # kept as a ceiling, and the tiny proof then stopped with KeyError: 4.
        cases = [dict(cores=1, recordsPerSec=608_333.0, status="OK"),
                 dict(cores=2, recordsPerSec=1_141_957.0, status="OK"),
                 dict(cores=4, recordsPerSec=2_100_000.0, status="CEILING")]
        got = L.sizing_case(cases)
        if got["cores"] != 4:
            raise Exception(f"sized from the {got['cores']}-core case, not the 4-core ceiling")
        # and a case that stopped before it had a rate is not sizeable
        partial = [dict(cores=1, recordsPerSec=608_333.0, status="OK"),
                   dict(cores=4, status="DROPPED")]
        if L.sizing_case(partial)["cores"] != 1:
            raise Exception("sized from a case that never produced a rate")
        raise Refusal("rig", "sized from the 4-core ceiling case, as it should")
    expect("tinyproof sizes from a ceiling top case instead of stopping",
           ceiling_on_top_case, "sized from the 4-core ceiling case")

    def no_case_has_a_rate():
        L.sizing_case([dict(cores=1, status="DROPPED"), dict(cores=2, status="DROPPED")])
    expect("tinyproof stops plainly when no case produced a rate",
           no_case_has_a_rate, "nothing to size the suite from")

    def bytes_per_record_reads_every_input_topic():
        # run 44: 25,000,000 readings on <in>-small at 18 B each, and the disk
        # check still used its 120-byte guess because it only looked at <in>.
        src = inspect.getsource(L.measured_bytes_per_record)
        for needed in ("-small", "-tiny"):
            if needed not in src:
                raise Exception(f"measured_bytes_per_record does not look at {needed} topics")
        raise Refusal("rig", "bytes per record is measured across every input topic")
    expect("bytes per record is measured on every input topic the run owns",
           bytes_per_record_reads_every_input_topic, "every input topic")

    def warm():
        ok, d = L.warmup_verdict([325e3, 259e3, 241e3, 340e3], 120)
        if ok:
            raise Exception("a 40% scatter was called flat")
        raise Refusal("case", f"scatter {d['scatter']:.0%}")
    expect("warm-up accepts a noisy ramp", warm, "scatter")

    def warm_ok():
        ok, d = L.warmup_verdict([400e3, 405e3, 398e3, 402e3], 120)
        if not ok:
            raise Refusal("case", f"flat plateau rejected: {d}")
    expect("warm-up accepts a flat plateau (must not fire)", warm_ok, "", should_fire=False)

    def good_case():
        L.check_case(dict(good), 4, False)
    expect("a valid case record passes (must not fire)", good_case, "", should_fire=False)

    if live:
        print("live guards (the rig, broken on purpose):")
        rest("/overview")  # stack must be up

        def wrong_cap():
            L.stop_tm()
            sh(f"docker run -d --name {c.tm} --network {c.net} --cpus 2 --entrypoint sleep alpine 60")
            try:
                L.assert_cap(c.tm, 4)
            finally:
                sh(f"docker rm -f {c.tm}", check=False)
        expect("resource cap was not applied", wrong_cap, "cap did not apply")

        def busy():
            L.start_tm(2)
            try:
                L.assert_cluster_idle()
            finally:
                L.stop_tm()
        expect("cluster still busy from the last case", busy, "still registered")

        def truncated():
            # a real topic against a manifest that is one record off
            t = topic or c.topic_in
            L.verify_backlog({c.count_field: L.log_end(t)[0] - 1}, topic=t)
        expect("backlog does not match its manifest", truncated, "manifest says")

        def disk_reads():
            # the projection's two measurements against the real broker and a real volume;
            # the first live 2.6 test died here on awk quoting the pure self-test never ran
            t = topic or c.topic_in
            n = L.log_end(t)[0]
            b = L.topic_bytes([t])
            if n and b < n:
                raise Exception(f"topic {t}: {n} records but only {b} bytes on the broker")
            vb = L.volume_bytes(c.ckpt_vol)
            if vb < 0:
                raise Exception(f"volume {c.ckpt_vol}: {vb}")
        expect("disk: broker and volume sizes read (must not fire)", disk_reads, "", should_fire=False)

        def dead_sampler():
            sh(f"docker rm -f {c.sampler}", check=False)
            sh(f"docker run -d --name {c.sampler} --network {c.net} --entrypoint sh alpine -c 'echo nope; exit 1'")
            try:
                for _ in range(20):
                    lg = sh(f"docker logs {c.sampler}", check=False)
                    if '"sampler":"up"' in lg.stdout:
                        return
                    if not sh(f"docker ps -q -f name=^{c.sampler}$", check=False).stdout.strip():
                        raise Refusal("rig", f"offset sampler died at startup:\n{lg.stdout}")
                    time.sleep(0.5)
            finally:
                sh(f"docker rm -f {c.sampler}", check=False)
        expect("monitor launched unattended is dead", dead_sampler, "died at startup")

        def no_job():
            L.wait_running("0" * 32, 2, timeout=3)
        expect("no job is actually running", no_job, "never reached RUNNING")

    save_json("selftest.json", {"build": build_hash() if os.path.exists(c.jar) else None,
                                "results": results, "at": time.strftime("%Y-%m-%d %H:%M:%S")})
    bad = [r for r in results if not r["ok"]]
    print(f"{len(results)-len(bad)}/{len(results)} guards fired as expected")
    return 1 if bad else 0


# ------------------------------------------------------------------- preflight

def cmd_preflight():
    c = cfg()
    rows = []
    extra = {}

    def check(name, fn):
        try:
            detail = fn()
            rows.append((name, "PASS", detail)); print(f"PASS  {name:52s} {detail}", flush=True)
        except Exception as e:
            rows.append((name, "FAIL", str(e))); print(f"FAIL  {name:52s} {e}", flush=True)

    def arch():
        host = sh("uname -m").stdout.strip()
        out = []
        for img in (c.kafka_img, c.flink_img):
            a = sh(f"docker image inspect {img} -f '{{{{.Architecture}}}}'").stdout.strip()
            if a != host:
                raise Exception(f"{img} is {a}, host is {host} — emulated CPU numbers")
            out.append(f"{img.split(':')[0]}={a}")
        return f"host={host} " + " ".join(out)

    def jdk():
        v = sh(f"{c.java} -version 2>&1 | head -1").stdout.strip()
        inside = sh(f"docker run --rm --entrypoint java {c.flink_img} -version 2>&1 | head -1").stdout.strip()
        import re
        mh = re.search(r'"(\d+)', v); mi = re.search(r'"(\d+)', inside)
        if not mh or not mi or mh.group(1) != mi.group(1):
            raise Exception(f"host JDK {v!r} vs engine image {inside!r}: major versions differ")
        return f"host {v} | image {inside}"

    def statedir():
        sh(f"docker run --rm --user 9999:9999 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
           f"-c 'mkdir -p /ckpt/.probe/shared && rmdir /ckpt/.probe/shared /ckpt/.probe'")
        return "uid 9999 (flink) can mkdir under /ckpt"

    def reporter():
        lib = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c 'ls /opt/flink/lib'").stdout.split()
        plug = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c 'ls -R /opt/flink/plugins'").stdout
        dup = [l for l in lib if "metrics" in l and l in plug]
        if dup:
            raise Exception(f"reporter jar in both lib/ and plugins/: {dup}")
        return "no reporter jar copied into lib/; slf4j reporter used from plugins/"

    def disk():
        # How big a record is on disk, measured off the broker when anything is
        # already there, and only guessed when nothing is. The guess used to be
        # 120 bytes for every run: clean-room run 43 measured 30 bytes a record
        # (30,000,000 records in 0.90 GB, compressed on the way in), so the guess
        # asked for four times the space the run needed and stopped a
        # configuration that would have fitted three times over. The run then
        # shrank its data to get under a limit that was never real.
        per_rec, how = L.measured_bytes_per_record(), "measured on the broker"
        if per_rec is None:
            per_rec, how = 120.0, "an assumption, since nothing is on the broker yet"
        in_bytes = c.backlog * per_rec
        per_case_out = T["sinkRetentionBytes"] * c.partitions * len(c.topics_out)
        need = in_bytes + per_case_out + 2 * 1024 ** 3 + 10 * 1024 ** 3
        free = L.host_free_bytes()
        shape = (f"{c.backlog:,} records at {per_rec:.0f} bytes each ({how}) "
                 f"= {in_bytes/1e9:.1f} GB, plus {per_case_out/1e9:.1f} GB of outputs, "
                 f"2 GB of checkpoints and 10 GB spare")
        if free < need:
            raise Exception(f"there is not enough disk space. This run needs about "
                            f"{need/1e9:.0f} GB and only {free/1e9:.0f} GB is free: {shape}. "
                            f"Either free up space or lower backlog.count in pipeline.json"
                            + ("" if how.startswith("measured") else
                               ". Note the record size above is a guess; if your records are "
                               "smaller, or compressed on the way in, the real need is lower"))
        return f"{free/1e9:.1f} GB free, about {need/1e9:.1f} GB needed: {shape}"

    def retention():
        L.recreate_output_topics()
        # topicsAlsoWritten too. This row read back on topics.out alone, so a
        # pipeline whose output is per window -- which has no topics.out at all
        # -- passed it on an empty list and was told its retention was fine.
        # That is the one shape where an undrained topic is guaranteed to exist.
        also = [t for t in c.topics_also if L.topic_exists(t)]
        missing = [t for t in also if not L.topic_retention_bytes(t)]
        if missing:
            return f"FAIL: no retention.bytes on {missing}, which nothing ever drains"
        checked = list(c.topics_out) + also
        if not checked:
            # Not the same as "there are none". A windowed pipeline declares
            # every one of its outputs in topicsAlsoWritten and none of them
            # exists until the job has run once, so this row could only ever
            # pass vacuously on a first run -- on exactly the shape it exists
            # to protect. Clean-room run 45 created its topic by hand to make
            # the row mean something.
            declared = list(c.topics_also)
            if declared:
                return ("nothing to check yet: " + ", ".join(declared)
                        + " will be written and never drained, but the job has not run yet so "
                          "they do not exist. Checked again after the completeness drain.")
            return "no topic is written but never drained"
        names = ", ".join(c.topics_out) or "none"
        return (f"retention.bytes={T['sinkRetentionBytes']} set and read back on {names}"
                + (f"; retention declared by the run and read back on {', '.join(also)}" if also else "")
                + " (a periodic sweep, not a bound)")

    def determinism():
        if not c.manifest_cmd:
            raise Exception("pipeline.json has no generator.manifestCmd (manifest-only mode)")
        hs = []
        for name in ("det-a.json", "det-b.json"):
            p = os.path.join(c.results, name)
            sh(c.fmt(c.manifest_cmd, count=200_000, seed=c.seed, manifest=p, topic=c.topic_in), timeout=600)
            hs.append(sh(f"shasum -a 256 {p}").stdout.split()[0])
        if hs[0] != hs[1]:
            raise Exception("two manifests from one seed are NOT identical")
        return f"identical sha256={hs[0][:12]} for 200,000 records at seed {c.seed}"

    def capmech():
        probe = f"{c.project}-capprobe"
        sh(f"docker rm -f {probe}", check=False)
        sh(f"docker run -d --name {probe} --cpus 2 alpine sleep 60")
        try:
            nano = L.assert_cap(probe, 2)
        finally:
            sh(f"docker rm -f {probe}", check=False)
        return f"--cpus throughout; read back NanoCpus={nano} and cgroup cpu.max = 2.0 cores"

    def interview_and_plan():
        """The run wrote down what it decided, before it had any numbers.

        doccheck proves SKILL.md *asks* for these. Nothing proved a run ever
        produced them, so the interview and the plan were honour-system and
        every run produced them differently. Missing files fail; a plan that
        does not name one of the disclosures is listed, because matching prose
        is loose, and a loose check that stops a run is worse than one that
        says what it looked for and could not find.
        """
        root = os.path.dirname(os.path.abspath(cfg().results))
        missing = [f for f in ("ASSUMPTIONS.md", "PLAN.md")
                   if not os.path.exists(os.path.join(root, f))]
        if missing:
            raise Exception(f"{' and '.join(missing)} not written. Section 1 says to answer the six "
                            f"questions and write them down; section 1a says to write the plan even "
                            f"with nobody to approve it. Both come before building.")
        plan = open(os.path.join(root, "PLAN.md")).read().lower()
        want = {"the objective": (f"{2 * L.T['scalingFloor']:.2f}", "near-linear"),
                "how long it takes": ("hour",),
                "what it writes": ("record", "backlog"),
                "the ports it takes": ("port",),
                "the guarantee": ("exactly-once",),
                "the shape of the suite": ("pass",),
                "the mid-run kill": ("kill",),
                "where to watch it": ("progress.txt",)}
        absent = [(k, want[k]) for k in want if not any(n in plan for n in want[k])]
        # Say what was looked for. Clean-room run 44 read "the plan does not
        # mention: the objective" and had no way to know the check wanted the
        # target's digits or the words near-linear -- a plan saying "99% of linear"
        # gets the same line. It passed only because it had read this file.
        return ("the plan names every disclosure" if not absent
                else "the plan does not mention: "
                     + "; ".join(f"{k} (looked for {' or '.join(repr(n) for n in v)})"
                                 for k, v in absent))

    def quiet_machine():
        """Nothing else should be competing for the cores under test.

        Reported, not enforced: a busy laptop is the owner's business and the
        number to compare against is not one this project has measured. But a
        cgroup cap is a share of what the host has left, so a case can read
        100% of its cap on a machine that is doing half as much work per cycle,
        and nothing else in the table would show it.
        """
        try:
            one, five, fifteen = os.getloadavg()
        except Exception:
            return "load average not available on this host"
        n = os.cpu_count() or 1
        # Two numbers, because the load average alone cannot tell a busy host
        # from a working one. Clean-room run 45 was told "OVERSUBSCRIBED: load
        # 11.31 ... busiest now: com.apple.Virtualization.VirtualMachine 298%"
        # on a host measured at 1.09 cores: the virtual machine at 298% was the
        # run's own Kafka and job manager, and the load average was the probe
        # that had just finished. So: exclude the Docker VM and this harness's
        # own processes, and sum what is left.
        ours = ("virtualmachine", "docker", "com.docker", "qemu", "java", "python3", "prove.py")
        rows = L.sh("ps -Ao %cpu,comm -r | tail -n +2", check=False).stdout.strip().splitlines()
        other, top = 0.0, []
        for line in rows:
            parts = line.split(None, 1)
            if len(parts) < 2:
                continue
            try:
                pct = float(parts[0])
            except ValueError:
                continue
            name = parts[1].strip()
            if any(o in name.lower() for o in ours):
                continue
            other += pct
            if pct >= 5.0 and len(top) < 3:
                top.append(f"{name.split('/')[-1]} {pct:.0f}%")
        cores_other = other / 100.0
        verdict = ("quiet" if cores_other < n * 0.25
                   else ("busy" if cores_other < n * 0.5 else "OVERSUBSCRIBED"))
        return (f"{verdict}: {cores_other:.2f} of {n} cores are being used by something that is not "
                f"this run (load average {one:.2f} / {five:.2f} / {fifteen:.2f}, which also counts "
                f"this run's own containers)"
                + (f" — busiest: {'; '.join(top)}" if top else ""))

    def host_memory():
        """The Docker VM against the machine it runs on. Reported, not enforced.

        Preflight checks the containers against the VM and nothing checked the
        VM against the machine. Clean-room run 45 wedged filling a 400,000,000
        record topic because macOS ran out of memory, not the VM: 3,044 MB of
        4,096 MB of swap used, 62 MB of free pages, and the generator's JVM
        swapped down to a 27 MB resident set and stopped answering. The
        generator is a host process the harness launches itself, so the memory
        the VM does not take has to hold it.

        Reported rather than enforced because one run is one data point, and
        what is left over has to cover a browser, an editor and whatever else
        the owner is doing -- which is not a number this project has measured.
        """
        try:
            phys = int(L.sh("sysctl -n hw.memsize", check=False).stdout.strip()) / 1048576.0
        except Exception:
            return "host memory not readable on this platform"
        info = L.sh("docker info --format '{{.MemTotal}}'", check=False).stdout.strip()
        vm = (int(info) / 1048576.0) if info.isdigit() else 0.0
        if not (phys and vm):
            return "could not read both the machine's memory and the VM's"
        left = phys - vm
        return (f"the machine has {phys/1024:.1f} GB and the Docker VM takes {vm/1024:.1f} GB, "
                f"leaving {left/1024:.1f} GB for macOS, this harness and the generator, which runs "
                f"on the host. Run 45 swapped 3 GB and stalled its generator with {left/1024:.1f} GB "
                f"left over, so watch the fill if this figure is near that."
                if left < 7168 else
                f"the machine has {phys/1024:.1f} GB and the Docker VM takes {vm/1024:.1f} GB, "
                f"leaving {left/1024:.1f} GB for the host and the generator")

    def memory_budget():
        """Worker at its largest case, broker and job manager must fit the VM with
        room to spare. Runs 20 and 21 each lost attempts discovering this by
        being stopped mid-run instead: the broker's page cache grows into whatever cap it is
        given, and paying for that cap out of the worker drove 1-core GC to 26%."""
        info = L.sh("docker info --format '{{.MemTotal}}'", check=False).stdout.strip()
        vm = int(info) if info.isdigit() else 0
        top = max(c.cases)
        capped = L.tm_memory_capped()
        worker = (L._mib(L.mem_for(c.tm_mem_per_core, top, c.tm_mem_base)) if c.tm_mem_per_core
                  else L._mib(c.tm_mem)) if capped else 0.0
        broker = L._mib(c.kafka_mem)
        # what the compose file actually gives it: jobmanager.memory.process.size
        # is 1600m, and clean-room run 45 found this line budgeting 1024m while
        # the container it describes gets more than that.
        jm = 1600.0
        need = worker + broker + jm
        # Reported, not enforced. A rule refusing need > VM - 1 GB was added on
        # 2026-09-07 and removed the same day: runs 20 and 21 both passed with a
        # 6,144m broker on a 7,838 MiB VM, which that rule refuses, and run 22
        # spent two chains discovering that it contradicts the broker-memory
        # hint (which asked for 5,632m where the rule allowed 3,613m). The
        # broker's page cache is elastic; over-committing it against the VM is
        # normal and the cases that matter are caught by the cap floor and the
        # broker's own limit-hit guard.
        over = " (over-committed, which is normal — the broker's cache is elastic)" if vm and need > vm / 1048576.0 else ""
        # Say "uncapped" here too. Budgeting Cfg.tm_mem's 4096m default while the
        # row above reports the worker as uncapped states a figure that was never
        # applied, and the over-commit warning it produces is then arithmetic on
        # a phantom. Clean-room run 30 reported the contradiction.
        w = (f"memory {worker:.0f}m at {top} cores" if capped
             else "memory uncapped (engine default)")
        return (f"{w} + broker {broker:.0f}m + job manager {jm:.0f}m "
                f"= {need:.0f}m of {vm / 1048576:.0f}m VM{over}" if vm
                else f"{need:.0f}m requested, VM size unknown")

    def backlog_sizing_hint():
        """Preflight cannot know the rate yet, but it can say what the guess must
        cover so the tiny proof does not have to refuse it."""
        secs = T["warmupMinS"] + T["minWindowS"] + 3 * c.ckpt_ms / 1000.0
        top = max(c.cases)
        return (f"backlog {c.backlog:,} covers {secs:.0f}s x 1.5 at the {top}-core rate, so up to "
                f"{c.backlog / (secs * 1.5):,.0f} rec/s; the tiny proof checks this against the "
                f"measured rate")

    def memory_per_subtask():
        """Each case must give its subtasks the same memory, or the largest case
        measures memory pressure rather than cores (2026-09-07: a flat 2048m read
        2->4 = 1.645 with GC at 9.3%; the same build with memory scaled per core
        read 1.910 with GC at 2.3%, and the 2-core figure did not move)."""
        if not (c.tm_mem_per_core or c.raw["caps"].get("tmMemory") or c.per_case):
            # "uncapped" was never true. Passing no process size does not leave
            # memory to the engine: the image ships one. flink:1.20.1's
            # config.yaml sets taskmanager.memory.process.size: 1728m, so every
            # case gets the same flat figure -- the configuration this very
            # check refuses when somebody writes it down. Clean-room run 36
            # measured the cost: 2->4 read 1.510 on the image default and 1.743
            # with memory scaled per subtask, and GC never flagged it (it was
            # LOWEST, 1.40%, on the case losing the most).
            img = L.image_tm_memory()
            flat = img or "the image's own default"
            raise Exception(
                f"no memory keys are set, so every case runs on {flat} from the image's "
                f"config.yaml -- one flat figure for 1, 2 and 4 cores, which is exactly what "
                f"this check refuses when it is written down. Set caps.tmMemoryBase and "
                f"caps.tmMemoryPerCore so each subtask gets the same memory. The GC ceiling "
                f"does not catch this: run 36 measured GC at its lowest on the starved case.")
        if not c.tm_mem_per_core:
            return f"per case: {', '.join(f'{k}c {v}' for k, v in sorted(c.per_case.items()))}"
        return (f"{c.tm_mem_base} base + {c.tm_mem_per_core} per subtask: "
                + ", ".join(f"{L.mem_for(c.tm_mem_per_core, n, c.tm_mem_base)} at {n}"
                            for n in sorted(c.cases)))

    def partitions_per_subtask():
        """Every case must divide the input evenly across its subtasks. Fewer
        partitions than subtasks leaves some with nothing to read; an uneven
        split makes the busiest subtask set the pace, and either one shows up
        as the largest case sitting below its CPU cap — which is a scaling
        shortfall the table cannot tell apart from a real one."""
        bad = [n for n in c.cases if c.partitions % n]
        if bad:
            raise Exception(f"{c.partitions} partitions do not divide evenly by parallelism {bad}: "
                            f"subtasks would read {c.partitions // max(bad)} or "
                            f"{c.partitions // max(bad) + 1} partitions each")
        return (f"{c.partitions} partitions / parallelism {sorted(c.cases)} = "
                + ", ".join(f"{c.partitions // n} per subtask at {n}" for n in sorted(c.cases)))

    def keys_per_subtask():
        """Partitions dividing evenly is only half of it: the keyed stages have
        to divide too. Flink hashes a key into one of maxParallelism key groups
        and gives each subtask a contiguous range, so four keys do not spread
        over four subtasks by themselves. The engine's own assignment is asked
        where they would land, out of the image under test."""
        if not c.key_sets:
            raise Exception("pipeline.json does not say which manifest fields hold the key sets "
                            "(keySets). A small key set does not spread over subtasks by itself, "
                            "and nothing here can check it without knowing the keys.")
        man = os.path.join(c.results, "det-a.json")
        if not os.path.exists(man):
            raise Exception(f"{man} not written: the determinism check writes the manifest this "
                            f"reads the keys from, so it has to pass first")
        m = json.load(open(man))
        sets = {}
        for stage, field in c.key_sets.items():
            if field not in m:
                raise Exception(f"the manifest has no field {field!r} for the {stage} stage; "
                                f"it has {sorted(k for k, v in m.items() if isinstance(v, dict))}")
            sets[stage] = sorted(m[field])
        max_par, whence = L.max_parallelism_for(c.cases)
        layout = L.key_layout(sorted({k for ks in sets.values() for k in ks}), max_par, c.cases)
        spread = L.key_spread(sets, c.cases, layout, max_par)
        extra["keySpread"] = spread
        floor = L.T["scalingFloor"]
        # only searched for when there is something to fix: the search is cheap
        # but a suggestion nobody needs is noise in a PASS row
        w = spread["worst"]
        suggest = []
        if spread["idle"] or w["stageCeiling"] < floor:
            suggest = L.suggest_max_parallelism(sets, c.cases)
            spread["suggestedMaxParallelism"] = suggest
        bad = L.key_skew_verdict(spread, floor, suggest)
        if bad:
            raise Exception(bad.msg)
        shape = "; ".join(f"{stage} {'/'.join(str(n) for n in st['cases'][max(c.cases)]['keysPerSubtask'])}"
                          for stage, st in spread["stages"].items())
        note = "" if w["stageCeiling"] >= floor else (
            f" — the busiest holds {w['busiestShare']:.1%} against {1.0 / w['cores']:.1%} even, "
            f"which bounds that stage at {w['stageCeiling']:.2f} of linear and no maxParallelism "
            f"between 128 and 32,768 divides these keys; the keys themselves are the fix")
        return (f"maxParallelism {max_par} ({whence}); keys per subtask at {max(c.cases)} cores: "
                f"{shape}{note}")

    def slots():
        m = max(c.cases)
        L.start_tm(m)
        try:
            o = rest("/overview")
            if o["slots-total"] < m:
                raise Exception(f"slots {o['slots-total']} < parallelism {m}")
            return f"slots-total={o['slots-total']} >= parallelism {m} x 1 job"
        finally:
            L.stop_tm()

    def scoping():
        return f"consumer group = {c.project}-<runid>-c<cores>-<pass>; sink is at-least-once (no txn ids)"

    def bp_endpoint():
        v = rest("/config")["flink-version"]
        return f"Flink {v}: busy/idle/backPressured read from the slf4j reporter; REST path deprecated"

    def trim():
        return "docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker"

    def jar():
        if not os.path.exists(c.jar):
            raise Exception(f"{c.jar} does not exist")
        return f"build {build_hash()}"

    def either_kafka():
        """The same build should run on Apache Kafka or Confluent with a config
        change. Reported, not enforced: an interview can ask for a Confluent-only
        feature, and then the build is meant to need it."""
        import zipfile
        if not os.path.exists(c.jar):
            return "nothing to check yet: the job jar is not built"
        with zipfile.ZipFile(c.jar) as z:
            only = L.vendor_only_classes(z.namelist())
        if not only:
            return ("no Confluent-only classes in the job jar, so this build runs on Apache Kafka or "
                    "Confluent with only images.kafka and images.kafkaLibs changed in pipeline.json")
        pkgs = sorted({"/".join(n.split("/")[:3]) for n in only})
        return (f"the job jar carries {len(only):,} Confluent-only classes ({', '.join(pkgs[:3])}), so moving "
                f"it to Apache Kafka needs more than a config change. Fine if the interview asked for them")

    os.makedirs(c.results, exist_ok=True)
    check("every image is native to the host arch", arch)
    check("the JDK the engine needs resolves, pinned", jdk)
    check("the engine can write its state directory", statedir)
    check("the metrics reporter is not duplicated", reporter)
    check("disk budget on the HOST, not the container", disk)
    check("retention on every topic written but never drained", retention)
    check("the generator is deterministic", determinism)
    check("CPU cap mechanism chosen once", capmech)
    check("slots >= parallelism x jobs", slots)
    check("partitions divide evenly by every parallelism", partitions_per_subtask)
    check("memory is per subtask, not per container", memory_per_subtask)
    check("keys divide evenly across subtasks", keys_per_subtask)

    def host_ceiling():
        """No pipeline beats its machine. Measured here so a missed claim can be
        read against what this host does at all (run 24: alu 2->4 = 0.980,
        mem 2->4 = 0.690 -- a memory-touching pipeline could not reach 95%)."""
        h = L.host_scaling(seconds=5.0, cases=sorted(set(c.cases)))
        extra["hostScaling"] = h
        if not h or h.get("error"):
            return f"not measured ({(h or {}).get('error', 'no probe')})"
        parts = []
        for mode in ("alu", "mem"):
            steps = h["ofLinear"].get(mode, {})
            parts.append(mode + " " + ", ".join(f"{k} {v:.0%}" for k, v in steps.items()))
        return "; ".join(parts)
    # Before the probe, not after it. host_ceiling runs Spin at 1, 2 and 4 cores
    # in two arms, and a one-minute load average taken straight afterwards is
    # mostly the probe: clean-room run 45 was told OVERSUBSCRIBED: load 11.31 on
    # a host its brief had measured at 1.09 cores, and watched the average fall
    # from 10.52 to 7.15 in 45 seconds with nothing running.
    check("nothing else is using the cores (reported)", quiet_machine)
    check("what this host's own cores do", host_ceiling)
    check("backlog covers warm-up, window and headroom", backlog_sizing_hint)
    check("the interview and the plan were written down", interview_and_plan)
    check("the Docker VM against the machine's memory (reported)", host_memory)
    check("pipeline, broker and job manager against the VM (reported)", memory_budget)
    check("group / txn-id prefix scoped per run", scoping)
    check("back-pressure counters exist on the endpoint read", bp_endpoint)
    check("the VM trim command is known", trim)
    check("the job jar exists and hashes", jar)
    check("the build runs on either Kafka (reported)", either_kafka)
    save_json("preflight.json", {"checks": [{"check": a, "result": b, "detail": d} for a, b, d in rows],
                                 **extra})
    fails = [r for r in rows if r[1] == "FAIL"]
    print(f"\n{len(rows)-len(fails)}/{len(rows)} PASS")
    return 1 if fails else 0


# ------------------------------------------------------------------ tiny proof

def cmd_tinyproof():
    """Two cases on a small backlog, a short checkpoint interval so a window fits,
    ratio bounded, then every guard broken on purpose."""
    c = cfg()
    topic = f"{c.topic_in}-tiny"
    man = L.fill(topic, c.tiny, c.seed + 1, "manifest-tiny.json")
    L._CFG.topic_in = topic  # the case measures the tiny topic
    out = {"build": build_hash(), "records": c.tiny, "cases": [], "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    lo, hi = min(c.cases), max(c.cases)
    # Every case the suite will run, so every step the suite will report is
    # bounded here rather than after 45 minutes of suite. Clean-room run 31 ran
    # cases 1, 2 and 4; the tiny proof measured only 1 and 4, so the 1->2 step
    # that came back at an arithmetically impossible 2.76x was not seen until
    # the report. With the default two cases this costs nothing -- lo and hi are
    # the only cases there.
    tiny_cases = sorted(set(c.cases))
    T_save = dict(T)
    # the pipeline's own checkpoint interval: at 2 s checkpoints the worker read
    # 83-94% of its cap where 10 s read 100% (harness live test, 2 cores, same
    # 30 s window). Three boundaries, a 2 s reporter so the window holds samples.
    T.update(warmupMinS=20.0, minBoundaries=3, minWindowS=30.0, reporterS=2)
    rc = 0
    try:
        recs = {}
        shape_ref = None
        for cores in tiny_cases:
            log(f"---- tiny case {cores} cores ----")
            try:
                def once(cores=cores):
                    return L.run_case(cores, "tiny", "tiny", shape_ref, cores == lo, man,
                                      warmup_max_s=120.0, reporter_s=2)

                def again(e, attempt):
                    log(f"  {cores}c failed on its own data, retrying once (section 6): {e.refusal.msg}")

                rec, shape_ref = L.run_case_retrying(once, on_retry=again)
                recs[cores] = rec
                out["cases"].append(rec)
                log(f"  {cores}c: {rec['recordsPerSec']:,.0f} rec/s, tm {rec['tmCapFrac']:.1%} of cap, "
                    f"vantage {rec['vantageDisagreement']:.2%}")
            except CaseRefused as e:
                out["cases"].append(e.rec)
                # A ceiling is a result, not a failure -- the suite keeps it,
                # reports it, and leaves it out of the ratios. The tiny proof
                # ended the whole chain on one, twice in clean-room run 35, on
                # a one-core case whose 2- and 4-core neighbours were fine and
                # would have produced a publishable 2->4.
                if e.refusal.scope == "ceiling":
                    e.rec["status"] = "CEILING"
                    out.setdefault("ceilings", []).append(
                        {"case": cores, "message": e.refusal.msg})
                    rate = e.rec.get("recordsPerSec")
                    log(f"  CEILING at {rate:,.0f} rec/s, tm {e.rec.get('tmCapFrac', 0):.1%} of cap: "
                        f"{e.refusal.msg}" if rate else f"  CEILING: {e.refusal.msg}")
                    log(f"  keeping it: a case that is not the constraint is reported and left out "
                        f"of the steps, not a reason to stop.")
                else:
                    log(f"  FAILED ({e.refusal.scope}): {e.refusal.msg}")
                    out["result"] = "FAIL"
                    rc = 1
        # Every case that produced a rate, whether it was accepted or was a
        # ceiling. A ceiling case is measured and kept a few lines above -- its
        # rate is real, and it is still the case the suite has to be sized for
        # -- but it never went into recs, and recs[hi] then asked for a key that
        # was not there. Clean-room run 44's 4-core case hit the broker's memory
        # limit, the harness logged "keeping it: a case that is not the
        # constraint is reported and left out of the steps", and one statement
        # later the tiny proof died with KeyError: 4. Nothing caught it: rc was
        # still 0, tinyproof.json was never written, the 98-guard self-test never
        # ran, and the ceiling case's rate was lost with it -- so the broker
        # memory change that run then made had nothing to be measured against.
        top = None
        if rc == 0:
            try:
                top = L.sizing_case(out["cases"])
            except Refusal as e:
                out["result"] = "FAIL"
                log(f"  STOPPED: {e.msg}")
                rc = 1
        if rc == 0:
            if top["cores"] != hi:
                log(f"  sizing the suite from the {top['cores']}-core case at "
                    f"{top['recordsPerSec']:,.0f} rec/s -- the {hi}-core case did not "
                    f"produce a rate. The suite may need more records than this says.")
            # GUARD: the suite's disk, projected from the measured shape, before the fill
            try:
                out["disk"] = L.disk_projection(topic, c.tiny, top)
            except Refusal as e:
                out["disk"] = getattr(e, "detail", None)
                out["result"] = "FAIL"
                log(f"  FAILED ({e.scope}): {e.msg}")
                rc = 1
        if rc == 0:
            d = out["disk"]
            on_disk = (f" ({d['inputBytesOnDisk']/1e9:.1f} GB of it already on the broker, "
                       f"{d['inputBytesToWrite']/1e9:.1f} GB still to write)" if d.get("inputBytesOnDisk") else "")
            log(f"  disk: input {d['inputBytesPerRecord']:.0f} B/record x {c.backlog:,} = {d['inputBytes']/1e9:.1f} GB{on_disk}; "
                f"sinks {d['sinkBytesPerInput']:.0f} B/input -> {d['sinkBytesUnbounded']/1e9:.1f} GB, "
                f"retention caps them at {d['sinkRetentionCapBytes']/1e9:.1f} GB; checkpoints {d['checkpointBytes']/1e9:.2f} GB; "
                f"need {d['neededBytes']/1e9:.1f} GB incl. the {d['floorBytes']/1e9:.0f} GB floor, "
                f"{d['hostFreeBytesNow']/1e9:.1f} GB free now + {d['reclaimableBytes']/1e9:.1f} GB the tiny proof gives back "
                f"= {d['hostFreeBytes']/1e9:.1f} GB: FITS")
            # warmup_verdict returns "warmupS"; reading "seconds" silently
            # yielded None, so sizing fell back to the tiny proof's own
            # warmupMinS override (20 s) instead of the measured warm-up.
            warm = (top.get("warmup") or {}).get("warmupS")
            want = L.size_backlog(top["recordsPerSec"], top["cores"], c.ckpt_ms / 1000.0,
                                  warmup_max_s=warm)
            out["backlogNeeded"] = want
            out["backlogConfigured"] = c.backlog
            if c.backlog < want:
                out["result"] = "FAIL"
                log(f"  FAILED (rig): backlog {c.backlog:,} is short of the {want:,} records the "
                    f"{top['cores']}-core case needs at its measured {top['recordsPerSec']:,.0f} rec/s "
                    f"(warm-up + window + headroom, x1.5); set backlog.count to at least that")
                rc = 1
            else:
                log(f"  backlog: {c.backlog:,} configured, {want:,} needed at the measured "
                    f"{top['recordsPerSec']:,.0f} rec/s")
            # Kafka's memory, sized the same way and at the same moment as the
            # backlog. Run 31 saw 1,104 limit hits here in a 30 s window, passed,
            # and then lost a 44-minute suite to the same broker at 60 s windows
            # with 90 s of warm-up in front of them -- four times the exposure.
            # The tiny proof is where this gets caught, because it is the first
            # real drain and it already knows the rate.
            # Every case, not only the accepted ones. The case that hits the
            # broker's memory limit is usually the case that was called a
            # ceiling for hitting it, and recs left that one out -- so the
            # broker sizing skipped the only evidence it had.
            worst = max(out["cases"], key=lambda r: r.get("brokerLimitHits") or 0)
            hits = worst.get("brokerLimitHits") or 0
            # Advice only where the broker could have held the worker back: a case
            # under its cap floor. A case at its cap is the constraint whatever the
            # broker did (the rule check_case applies since 2026-09-24). Clean-room
            # run 49 was told to drop its 4-core case for 381 hits on a case at
            # 100.1% of cap, then passed both steps (finding F10).
            held = L.broker_held_back(out["cases"])
            if held:
                worst, hits = held, held.get("brokerLimitHits") or 0
            want_mem = L.size_broker_memory(worst.get("brokerLimitBytes") or 0, hits) if held else None
            out["brokerLimitHits"] = hits
            out["brokerMemoryNeededMb"] = want_mem
            if want_mem:
                # Reported, not failed. The record has a case the other way:
                # run 23's 1-core case hit the limit 9,437 times at 99.6% of cap
                # with no effect on its rate, and its suite was accepted. Hits
                # alone do not separate a broker that will lose the suite from
                # one that will not, so this says what it saw and what it would
                # cost to be safe, and lets the config check upstream do the
                # gating.
                have = (worst.get("brokerLimitBytes") or 0) / 1048576
                doesnt_fit = L.broker_advice_fits(want_mem)
                log(f"  kafka memory: ran out {hits:,} times in a {worst.get('elapsedS', 0):.0f}s "
                    f"window at {have:.0f}m. The suite's windows are longer. If its cases come "
                    f"back as ceilings, {want_mem}m is the size to try"
                    + (f" -- {doesnt_fit}" if doesnt_fit else "."))
            else:
                log(f"  kafka memory: {(worst.get('brokerLimitBytes') or 0) / 1048576:.0f}m, hit its limit "
                    f"{hits:,} times" + (" with every worker at its cap, so nothing to change"
                                         if hits else ""))
            # Every step the suite will report, bounded here. The pairs are the
            # adjacent ones the report leads with, plus the whole span, so a
            # config with three cases has its middle step checked too -- the
            # step run 31 could not see until the report.
            pairs = list(zip(tiny_cases, tiny_cases[1:]))
            if (lo, hi) not in pairs:
                pairs.append((lo, hi))
            out["steps"] = []
            print()
            for a, b in pairs:
                if a not in recs or b not in recs:
                    log(f"  {a}->{b}: not bounded — {a if a not in recs else b} cores was a ceiling")
                    continue
                ratio = recs[b]["recordsPerSec"] / recs[a]["recordsPerSec"]
                ideal = b / a
                bound = (T["tinyRatioLo"] * ideal / 2, T["tinyRatioHi"] * ideal / 2)
                share = recs[a]["recordsPerSec"] / recs[b]["recordsPerSec"]
                step = {"step": f"{a}->{b}", "ratio": round(ratio, 3), "idealRatio": ideal,
                        "boundLo": round(bound[0], 3), "boundHi": round(bound[1], 3),
                        "baselineShare": round(share, 4),
                        "baselineShareExpected": round(1 / ideal, 4),
                        "ok": bound[0] <= ratio <= bound[1]}
                out["steps"].append(step)
                print(f"tiny proof {a} -> {b} ratio: {ratio:.3f}x (bounds {bound[0]:.2f}-{bound[1]:.2f})")
                print(f"  {a} core(s)  {recs[a]['recordsPerSec']:>12,.0f} rec/s")
                print(f"  {b} core(s)  {recs[b]['recordsPerSec']:>12,.0f} rec/s")
                print(f"  {a} of {b} cores did {share:.0%} of the work. It should be about "
                      f"{1 / ideal:.0%}.")
                if step["ok"]:
                    continue
                # Say which case is wrong, not that the ratio looks odd. A big
                # ratio is almost always the small case running slow.
                out["result"] = "FAIL"
                rc = 1
                if ratio > bound[1]:
                    print(f"STOPPING: the {a}-core case is too slow. The {b}-core case is fine.")
                    print(f"  Look at the {a}-core case only. Two things make it slow:")
                    print(f"  its job graph is a different shape, or its threads are sharing one core.")
                    print(f"  Check the graph shape and the CPU cap on that case.")
                else:
                    print(f"STOPPING: {b} cores did only {ratio:.2f}x the work of {a}. "
                          f"It should be about {ideal:.0f}x.")
                    print(f"  The rig is not set up the way you think. Check three things:")
                    print(f"  the CPU cap, the partition count, and the backlog size.")
                print(f"  Fix this before running the suite. The suite reports this step, so a")
                print(f"  suite run now would spend 45 minutes arriving at the same number.")
            # the span, kept for anything that reads one ratio
            span = next((x for x in out["steps"] if x["step"] == f"{lo}->{hi}"), None)
            if span:
                out["ratio"] = span["ratio"]
                out["baselineShare"] = span["baselineShare"]
                out["baselineShareExpected"] = span["baselineShareExpected"]
            if rc == 0:
                out["result"] = "PASS"
    finally:
        T.clear(); T.update(T_save)
        L._CFG.topic_in = c.raw["topics"]["in"]
    save_json("tinyproof.json", out)
    if rc == 0:
        print("\nguard self-test:")
        rc = cmd_selftest(live=True, topic=topic)
        out["selftest"] = "PASS" if rc == 0 else "FAIL"
        save_json("tinyproof.json", out)
    L.delete_topic(topic)  # the disk budget did not include it
    print("TINY PROOF " + ("PASSED" if rc == 0 else "FAILED"))
    return rc


# ------------------------------------------------------------------------ fill

def cmd_fill():
    c = cfg()
    man = L.fill(c.topic_in, c.backlog, c.seed, "manifest.json")
    print(f"backlog {c.backlog:,} records on {c.topic_in}; manifest results/manifest.json")
    return 0


# ---------------------------------------------------------------- completeness

def cmd_completeness():
    """Process a small test data set twice — once cleanly, once killed and restarted partway —
    and compare the sinks to the generator manifest with no tolerances."""
    c = cfg()
    topic = f"{c.topic_in}-small"
    cores = c.baseline
    man = L.fill(topic, c.small, c.seed + 2, "manifest-small.json")
    man_path = os.path.join(c.results, "manifest-small.json")
    L._CFG.topic_in = topic
    out = {"build": build_hash(), "records": c.small, "cores": cores, "arms": [],
           "at": time.strftime("%Y-%m-%d %H:%M:%S")}

    def drain(group, kill_at=None):
        jid, killed, killed_at = None, False, None
        restored0, checked_at = None, time.time()
        try:
            # the run's own declared outputs are cleared too: the two arms are
            # asserted differently and their rows must not be mixed
            L.recreate_output_topics(include_declared=True)
            L.assert_cluster_idle()
            L.delete_group(group)
            L.start_tm(cores)
            L.start_sampler(group)
            jid = L.submit_job(cores, group)
            L.wait_running(jid, cores)
            restored0 = L.job_health(jid)["restored"]
            t0 = checked_at = time.time()
            while True:
                ticks = L.sampler_tail(4)
                cm = max([t.get("committed", -1) for t in ticks] or [-1])
                if kill_at and not killed and cm >= c.small:
                    # run 5: a kill after the drain has finished proves nothing
                    raise Refusal("rig", f"the run finished ({cm:,} records committed) before the pipeline "
                                         f"could be killed at {kill_at:.0%}. Nothing was proved. Make "
                                         f"backlog.smallCount big enough to span several checkpoint "
                                         f"intervals at the baseline rate.")
                if kill_at and not killed and cm >= c.small * kill_at:
                    log(f"KILL: committed={cm}, killing the pipeline mid-run")
                    sh(f"docker kill {c.tm}")
                    if L.tm_running():
                        raise Refusal("rig", "docker kill reported success but the container is alive")
                    killed, killed_at = True, cm
                    time.sleep(3)
                    L.start_tm(cores)
                    L.wait_running(jid, cores, timeout=240)
                    log("KILL: job RUNNING again after the restart")
                if cm >= c.small:
                    time.sleep(c.ckpt_s + 2)
                    log(f"processed the full test data set in {time.time()-t0:.1f}s" + (" (killed and restarted mid-run)" if killed else ""))
                    return {"group": group, "killed": killed, "drainS": round(time.time() - t0, 1),
                            "killedAtCommitted": killed_at,
                            # read while it is still RUNNING: the finally below
                            # cancels the job, and the plan goes with it
                            "shape": L.graph_shape(jid)}
                if time.time() - t0 > 1800:
                    raise Refusal("rig", f"it did not process all of the input: {cm:,} of {c.small:,} records")
                # Ask the engine how the job is. This loop watched the offset
                # and nothing else, so clean-room run 45 sat here for 11 silent
                # minutes against a job that could not run, and would have sat
                # for 30. The kill arm restarts the job on purpose, so the
                # baseline moves with it.
                if time.time() - checked_at > 10:
                    checked_at = time.time()
                    if killed:
                        restored0 = L.job_health(jid)["restored"]
                    why = L.job_is_failing(jid, restored0)
                    if why:
                        raise Refusal("rig", f"the drain stopped after {time.time()-t0:.0f}s: {why}")
                time.sleep(0.5)
        finally:
            try:
                L.cancel_job(jid)
            finally:
                L.stop_sampler(); L.stop_tm()

    def verify(label, arm):
        # Section 4 asks for DIFFERENT assertions on the two arms -- never
        # backwards, against backwards at most once per key -- but the harness
        # never said which arm it was running, so every run had to infer it.
        # Run 41 stamped the consumer group into its published business rows to
        # tell them apart; run 42 inferred it from the presence of duplicates.
        # A verifier that does not use {arm} is unaffected.
        cmd = c.fmt(c.verify_cmd, manifest=man_path, topic=topic, arm=arm)
        r = sh(cmd, check=False, timeout=3600)
        print(r.stdout)
        if r.returncode != 0:
            print(r.stderr[-3000:])
            raise Refusal("rig", f"COMPLETENESS FAILED ({label}): verifier exit {r.returncode}")
        return r.stdout

    try:
        a = drain(f"{c.project}-complete-clean"); a["verify"] = verify("clean drain", "clean"); out["arms"].append(a)
        # GUARD: the job matches the design the run wrote down. The verifier
        # checks the numbers in the topics the harness owns; this checks that
        # the operators, the inputs and the outputs the interview asked for
        # are all there -- including the ones outside topics.out, which is
        # where both market values live and where nothing else looks.
        # tolerant: this runs before the fill, so the suite's own input topic
        # may not exist yet, and kafka-get-offsets.sh exits non-zero on a topic
        # that is not there -- which took the whole step down on a cold stack
        wanted = dict.fromkeys([c.suite_topic_in, topic] + list(c.topics_out) + list(c.topics_also)
                               + list((c.design.get("inputs") or []) + (c.design.get("outputs") or [])))
        records = {}
        for t in wanted:
            n = L.log_end_if_any(t)
            records[t] = n if n else (0 if L.topic_exists(t) else None)
        sink_rows = sum(records.get(t, 0) for t in c.topics_out)
        measured_fanout = round(sink_rows / c.small, 3) if c.small else 0
        constraints = ({"outputsPerInput": (c.out_per_in, measured_fanout)}
                       if c.out_per_in is not None else {})
        shape = (a.get("shape") or {})
        if shape.get("maxParallelism"):
            constraints["maxParallelism"] = (L.max_parallelism_for(c.cases)[0],
                                             shape["maxParallelism"][0])
        rows, bad = L.design_diff(c.design, shape.get("plan"), records, constraints,
                                  before_fill=True)
        out["designDiff"] = rows
        log("  design against build:")
        for line in L.design_table(rows):
            log(line)
        if bad:
            log("  the build does not match the design. Section 4 says to correct it and run "
                "completeness again — no fill, no suite, until it matches.")
            raise bad
        b = drain(f"{c.project}-complete-kill", kill_at=c.kill_frac); b["verify"] = verify("killed mid-run", "killed"); out["arms"].append(b)
        out["result"] = "PASS"
    except Refusal as e:
        out["result"] = "FAIL"; out["error"] = e.msg
        save_json("completeness.json", out)
        print("COMPLETENESS FAILED:", e.msg)
        return 1
    finally:
        L._CFG.topic_in = c.raw["topics"]["in"]
    save_json("completeness.json", out)
    print(f"COMPLETENESS PASSED FOR BUILD {out['build']} (a clean run, and one killed and restarted at {c.kill_frac:.0%})")
    return 0


# ----------------------------------------------------------------------- suite

def passes_plan(cases, n, baseline=None):
    """Alternating order, then the sentinel: the baseline case once more at the
    very end, so the suite's first and last measurements are the same case. A
    rig that drifts across the suite shows up as baseline spread instead of
    hiding inside the alternation (plan 12: 4c read 688k-776k across a suite
    and 801k ten minutes later)."""
    plan = []
    for i in range(n):
        asc = (i % 2 == 0)
        plan.append((f"p{i+1}-{'asc' if asc else 'desc'}", list(cases) if asc else list(reversed(cases))))
    if baseline is not None:
        plan.append(("sentinel", [baseline]))
    return plan


def cmd_suite():
    c = cfg()
    man = load_json("manifest.json")
    comp = load_json("completeness.json")
    bh = build_hash()
    # GUARD: no table for a build that has not passed completeness
    if comp.get("result") != "PASS" or comp.get("build") != bh:
        raise Refusal("rig", f"completeness has not passed for build {bh} "
                             f"(completeness.json: {comp.get('result')} for {comp.get('build')})")
    tp = load_json("tinyproof.json") if os.path.exists(os.path.join(c.results, "tinyproof.json")) else {}
    if tp.get("result") != "PASS" or tp.get("selftest") != "PASS":
        raise Refusal("rig", "the tiny proof (with guard self-test) has not passed; run `prove.py tinyproof`")
    plan = passes_plan(c.cases, c.passes, c.baseline)
    out = {"axis": c.axis, "apiLevel": c.api_level, "guarantee": c.guarantee,
           "checkpointIntervalMs": c.ckpt_ms, "buildHash": bh, "completenessBuild": comp.get("build"),
           "passesPerCase": c.passes, "quickLook": L.QUICK, "publishable": not L.QUICK,
           "study": ("capacity curve: each case configured separately, declared before the run"
                     if c.per_case else "scaling: every case configured identically"),
           "perCase": c.per_case or None,
           "cases": c.cases, "baseline": c.baseline,
           # Every scalar the generator's own manifest declares. Agents name these
           # differently -- distinctSymbols, symbolUniverse, numSymbols, symbolCount
           # -- so the harness keeps them all rather than guessing a schema. Without
           # this, two runs of "the same" workload can differ by 4 keys against
           # 32,768 and nothing in the results says so: comparing their step ratios
           # for days is then comparing two different problems.
           "workload": {k: v for k, v in man.items() if isinstance(v, (int, float, str))},
           "backlogRecords": int(man[c.count_field]), "partitions": c.partitions,
           "outputsPerInput": c.out_per_in,
           "heldStill": {"kafkaCap": c.kafka_cap, "jobManagerCap": c.jm_cap, "partitions": c.partitions,
                         "checkpointMs": c.ckpt_ms, "sinkRetentionBytesPerPartition": T["sinkRetentionBytes"],
                         # null when nothing was capped, one figure when one was set, and
                         # a figure per case when memory is a base plus a per-core share:
                         # Cfg.tm_mem's default is not a setting that was in effect, and the
                         # record says only true things
                         "tmProcessMemory": L.tm_memory_record(),
                         # anything else that was on the machine while this was measured
                         "extraServices": sorted((c.raw.get("extraServices") or {}).keys()) or None},
           "thresholds": dict(T), "harness": harness_version(),
           "startedAt": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "startedAtEpoch": time.time(),
           "runs": [], "refusals": []}

    def save():
        out["savedAt"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
        out["savedAtEpoch"] = time.time()
        out["table"] = build_table(out["runs"], quick=out.get("quickLook", False))
        save_json("suite.json", out)

    shape_ref, stop = None, None
    run_id = time.strftime("%m%d%H%M")
    total_cases = sum(len(order) for _, order in plan)
    done_cases, t_suite = 0, time.time()
    for pass_id, order in plan:
        for cores in order:
            log(f"---- case {cores} cores, pass {pass_id} ----")
            per = (time.time() - t_suite) / done_cases if done_cases else None
            log(L.progress(f"suite: case {done_cases + 1} of {total_cases} "
                           f"({cores} cores, pass {pass_id})",
                           pct=done_cases / total_cases,
                           eta_s=per * (total_cases - done_cases) if per else None))
            try:
                def once(cores=cores, pass_id=pass_id):
                    return L.run_case(cores, pass_id, run_id, shape_ref, cores == c.baseline, man)

                def again(e, attempt):
                    log(f"  {cores}c {pass_id} failed on its own data, retrying once "
                        f"(section 6): {e.refusal.msg}")
                    out.setdefault("retries", []).append(
                        {"case": cores, "pass": pass_id, "message": e.refusal.msg})

                rec, shape_ref = L.run_case_retrying(once, on_retry=again)
                out["runs"].append(rec)
                log(f"  {cores}c {pass_id}: {rec['recordsPerSec']:,.0f} rec/s  tm {rec['tmCores']:.2f}/{cores} "
                    f"({rec['tmCapFrac']:.1%})  kafka {rec['kafkaCores']:.2f}/{c.kafka_cap:g}  "
                    f"srcIdle {rec['sourceIdle']:.1%}  srcBP {rec['sourceBackpressured']:.1%}  "
                    f"headroom {rec['headroomS']:.0f}s  vantage {rec['vantageDisagreement']:.2%}")
            except CaseRefused as e:
                out["runs"].append(e.rec)
                kind = "CEILING" if e.refusal.scope == "ceiling" else "DROPPED"
                label = "CEILING" if kind == "CEILING" else "THROWN OUT"
                out.setdefault("ceilings" if kind == "CEILING" else "refusals", []).append(
                    {"case": cores, "pass": pass_id, "scope": e.refusal.scope, "message": e.refusal.msg})
                log(f"  {label}: {e.refusal.msg}")
                if e.refusal.scope == "rig":
                    stop = ("a check about the rig stopped it", e.refusal.msg)
            done_cases += 1
            save()
            if stop:
                break
        if stop:
            break
    if stop:
        out["stoppedEarly"] = {"reason": stop[0], "message": stop[1],
                               "note": "stopping here: the remaining cases would fail the same way"}
    save()
    cmd_report()
    print(render_table(out))
    return 1 if stop else 0


def harness_version():
    r = sh(f"shasum -a 256 {L.HERE}/lib.py {L.HERE}/prove.py", check=False)
    return {"lib.py": r.stdout.split()[0][:16] if r.stdout else None,
            "prove.py": r.stdout.split()[2][:16] if len(r.stdout.split()) > 2 else None}


# --------------------------------------------------------------------- ceiling

def cmd_ceiling():
    """Hold the component under test at its largest size; cap the broker in
    steps. The handover is where the broker pins and the worker falls off its cap."""
    c = cfg()
    man = load_json("manifest.json")
    top = max(c.cases)
    steps = [float(x) for x in c.raw.get("ceilingBrokerCaps", [c.kafka_cap, 1.0, 0.5])]
    out = {"cores": top, "steps": [], "buildHash": build_hash()}
    run_id = time.strftime("%m%d%H%M")
    try:
        for cap in steps:
            sh(f"docker update --cpus {cap} {c.kafka}")
            L.assert_cap(c.kafka, cap)
            log(f"---- ceiling: broker capped at {cap} cores, pipeline at {top} ----")
            try:
                rec, _ = L.run_case(top, f"k{cap:g}", run_id, None, False, man, kafka_cap=cap)
                rec["brokerCap"] = cap
                rec["brokerCapFrac"] = rec["kafkaCapFrac"]
                out["steps"].append(rec)
                log(f"  broker {rec['kafkaCores']:.2f}/{cap:g} ({rec['brokerCapFrac']:.0%})  cores {rec['tmCapFrac']:.1%}  "
                    f"{rec['recordsPerSec']:,.0f} rec/s  srcIdle {rec['sourceIdle']:.1%}")
            except CaseRefused as e:
                e.rec["brokerCap"] = cap
                if "kafkaCapFrac" in e.rec:
                    e.rec["brokerCapFrac"] = e.rec["kafkaCapFrac"]
                out["steps"].append(e.rec)
                log(f"  at broker cap {cap:g}: {e.refusal.msg}")
            save_json("ceiling.json", out)
    finally:
        sh(f"docker update --cpus {c.kafka_cap} {c.kafka}", check=False)
        L.assert_cap(c.kafka, c.kafka_cap)
    return 0


# ---------------------------------------------------------------------- report

def bottleneck_short_of(out, step):
    """What held back the case a step ends at, for the report's fix list."""
    last = next((r for r in reversed(out.get("runs") or [])
                 if r.get("cores") == step.get("to") and r.get("status") in ("OK", "CEILING")), None)
    return L.bottleneck_short(last) if last else None


def cmd_report():
    out = load_json("suite.json")
    out["table"] = build_table(out["runs"], quick=out.get("quickLook", False))
    c = cfg()
    steps = out["table"]["stepRatios"]
    short = [r for r in steps if r.get("meetsClaim") is False]
    # A step above its ideal is not a fast pipeline. Nothing does more than
    # double the work on double the cores, so the lower case of that step read
    # too low, and every step it appears in means less than it looks like.
    # Judged on the low end of the interval, so a noisy pair is not called
    # impossible on one bad pass.
    impossible = [r for r in steps if r.get("reportable")
                  and (r.get("ratioLowCI") or 0) > r["idealRatio"]]
    # Render both BEFORE opening anything for writing. open(..., "w") empties
    # the file before the renderer runs, so a renderer that raises destroys the
    # previous run's table as well as failing to write this one. That is how
    # clean-room run 42 ended with a 0-byte suite.txt: two separate losses from
    # one bug.
    text, markdown = render_table(out) + "\n", render_markdown(out)
    for name, body in (("suite.txt", text), ("suite.md", markdown)):
        # Written whole, then moved into place. Rendering first already stops a
        # renderer crash from destroying the previous run's table; this also
        # covers dying part-way through the write, which would leave half a
        # table looking like a whole one.
        final = os.path.join(c.results, name)
        tmp = final + ".partial"
        with open(tmp, "w") as f:
            f.write(body)
        os.replace(tmp, final)
    save_json("suite.json", out)
    print("wrote results/suite.txt and results/suite.md")
    # GUARD: a run that measured nothing is not a pass.
    #
    # Clean-room run 46 threw out eight of its ten cases -- four for source
    # idle, four for the broker's memory -- kept two, both at four cores, and
    # so had nothing to compare against anything. stepRatios came out null,
    # `short` was therefore empty, and this function returned 0. results/DONE
    # said "PASS 58.8 min" for a run that produced no scaling result at all.
    # That is the one failure a reader cannot catch by reading further, because
    # DONE is the file they are told to wait on.
    nothing = L.no_result_reason(out["table"], out["runs"])
    if nothing:
        out["reportVerdict"] = "no-result"
        out["noResult"] = nothing
        save_json("suite.json", out)
        print("\nNO SCALING RESULT\n")
        print(f"  {nothing['sentence']}")
        print("  Comparing one core count with another is the whole measurement, so there is")
        print("  nothing here to report. This is not a slow pipeline and not a fast one.\n")
        why = {}
        for r in out["runs"]:
            if r.get("status") != "OK":
                msg = (r.get("ceiling") or r.get("refusal") or "thrown out")
                # group by the KIND of reason, not the exact text: the numbers
                # differ every time, so grouping on the whole sentence puts
                # every case in a group of one. And never cut at a full stop --
                # the numbers are full of them.
                key = re.sub(r"[\d][\d,.%]*", "N", msg)[:110]
                why.setdefault(key, [[], msg])[0].append(f"{r['cores']}c {r.get('pass', '')}".strip())
        if why:
            print("  Why each was thrown out:\n")
            for _, (who, sample) in sorted(why.items(), key=lambda kv: -len(kv[1][0])):
                print(f"   {len(who)} case{'' if len(who) == 1 else 's'} — {', '.join(who)}")
                for line in textwrap.wrap(sample, 86):
                    print(f"     {line}")
                print()
        print("  What to do: a guard that threw out a case whose worker was at its cap is")
        print("  worth doubting before the pipeline is. Run `prove.py ceiling` to find out")
        print("  whether the thing a guard blamed actually moves the rate, and `prove.py")
        print("  probe` to find out whether this machine can do the step at all. Both are")
        print("  minutes, and neither changes the pipeline.\n")
        return 1

    if short:
        t = out["table"]
        need = 2 * T["scalingFloor"]

        def why(r, pad):
            """What changed across one step, for whoever has to chase it."""
            a, b = t["cases"].get(r["from"]), t["cases"].get(r["to"])
            if not (a and b):
                return
            pa = a["meanRecordsPerSec"] / r["from"]
            pb = b["meanRecordsPerSec"] / r["to"]
            print(f"{pad}per core        {pa:>12,.0f} -> {pb:>12,.0f}   ({pb / pa - 1:+.1%})")
            for k, label in (("tmCapFrac", "% of cap"), ("sourceIdle", "source idle"),
                             ("gcFracOfCapacity", "GC"), ("sourceBackpressured", "back-pressure")):
                if a.get(k) is not None and b.get(k) is not None:
                    print(f"{pad}{label:<15} {a[k]:>11.1%} -> {b[k]:>11.1%}")

        print("\nCLAIM NOT MET\n")

        # The whole picture first: every case, with each step beside the case
        # it lands on. A reader should not have to hunt for the good step.
        print(f"  {'cores':>6}{'speed':>15}" + "".join(f"{r['step'] + ' cores':>15}" for r in steps))
        for cs in t["cases"].values():
            cells = [f"{r['ratio']:.2f}x" if r["to"] == cs["cores"] and r.get("reportable") else ""
                     for r in steps]
            print(f"  {cs['cores']:>6}{cs['meanRecordsPerSec']:>13,.0f}/s"
                  + "".join(f"{x:>15}" for x in cells))
        print(f"\n  Doubling the cores should give 2.00x. This run needs {need:.2f}x or better.")

        print("\n  Fix these in order:\n")
        n = 1
        for r in impossible:
            print(f"  {n}. {r['step']} cores reads {r['ratio']:.2f}x. Nothing does more than "
                  f"{r['idealRatio']:.2f}x, so the {r['from']}-core reading is too low.")
            print(f"     Fix this one first. While it is wrong, every step it appears in is")
            print(f"     wrong too. Check the {r['from']}-core case: is its job graph the same")
            print(f"     shape as the others, and did it get the cores it asked for?")
            why(r, "     ")
            print()
            n += 1
        for r in short:
            print(f"  {n}. {r['step']} cores reads {r['ratio']:.2f}x, under the {need:.2f}x it needs."
                  f" This is the real shortfall.")
            why(r, "     ")
            print()
            n += 1

        try:
            pf = load_json("preflight.json")
            h = (pf.get("hostScaling") if isinstance(pf, dict) else None) or {}
            for mode, label in (("alu", "register-only"), ("mem", "memory-bound")):
                steps = (h.get("ofLinear") or {}).get(mode) or {}
                rng = (h.get("ofLinearRange") or {}).get(mode) or {}
                if steps:
                    parts = []
                    for k, v in steps.items():
                        r = rng.get(k) or {}
                        # the probe stores a fraction of linear; say it as the
                        # multiple of that step, which is what everything else
                        # about scaling is said in
                        try:
                            a, b = (float(x) for x in k.split("->"))
                            ideal = b / a
                        except Exception:
                            ideal = 2.0
                        parts.append(f"{k} {v * ideal:.2f}x"
                                     + (f" [{r['low'] * ideal:.2f}-{r['high'] * ideal:.2f}x]" if r else ""))
                    print(f"  this host, {label:<13} " + "  ".join(parts))
            if h:
                sp = L.probe_spread(h)
                # the shortfall this is being asked to explain
                gap = max((1 - (r["ratio"] / (r["idealRatio"] * T["scalingFloor"]))
                           for r in short), default=0)
                mid = (f", its middle half {sp['middleHalf']:.0%}"
                       if sp["middleHalf"] is not None else "")
                print(f"  A pipeline cannot beat its machine — but the probe's own range is "
                      f"{sp['envelope']:.0%} over {sp['repeats']} repeats{mid}, against a "
                      f"shortfall of {gap:.0%}.")
                for line in L.probe_advice(sp, gap):
                    print(f"  {line}")
        except Exception:
            pass
        print("  What this rig has shown: memory that does not scale per subtask costs about 14%;"
              "\n  a broker starved of page cache costs about 13%; four subtasks instead of two costs about"
              "\n  8% on the same cores, of which ~3 points is the source idling. Partition count and network"
              "\n  buffers were tested and changed nothing. See harness/README.md.")
        print("\n  The table stands. The pipeline did not scale on this machine.\n")
        print("  This is a list, not a decision. Take it to whoever asked for the run as a")
        print("  plan -- what you would change, in this order, and what you expect it to")
        print("  move -- and get a yes before changing anything and measuring again. A")
        print("  re-run costs about what the run that just finished cost.")
        # A short step with the pipeline pinned on CPU has nothing obvious to
        # fix, and guessing is what section 8 exists to prevent. Two cheap
        # measurements answer it, in this order, before anything is changed.
        if any(bottleneck_short_of(out, r) == "Pipeline CPU" for r in short):
            print()
            print("  Every case was pinned on CPU, so there is no setting on the table to")
            print("  change. Two measurements answer this, cheapest first:")
            print()
            print("   1. prove.py probe --repeats 9")
            print("      Can this machine do the step at all, with no pipeline in the way?")
            print("      Minutes, starts nothing. If the machine itself cannot, stop here.")
            print()
            print("   2. prove.py ceiling")
            print("      Is the largest case already against a ceiling? Holds it at its size")
            print("      and squeezes Kafka in steps. If the rate barely moves as Kafka is")
            print("      starved, Kafka is not the ceiling and the pipeline is at its own.")
            print("      A few short cases on the stack that is already up.")
            print()
            print("  Only then change something, and only one thing. Section 6a of the skill")
            print("  lists what has been worth what on this rig, biggest first, and the two")
            print("  things that were tested and changed nothing.")
        print()
        print("  With no one to ask: run those two first, then work down section 6a one")
        print("  change at a time. Write what you changed and what you expect it to move")
        print("  into FIXES.md before you measure. Keep a change that helped; revert one")
        print("  that did not, so the next is measured against your best pipeline and not")
        print("  your worst. Stop when the target is met, when no untried row matches the")
        print("  symptom, or after four changes.")
        out["reportVerdict"] = "claim-not-met"
        save_json("suite.json", out)
        return 1
    out["reportVerdict"] = "met"
    save_json("suite.json", out)
    return 0


# ------------------------------------------------------------------------ all

def cmd_all(steps=None, results=None):
    """The whole chain as one command. Run 11 spent 20 minutes of its 1.97 h in
    the gaps between commands an agent typed by hand, and wrote phases.log by
    hand; here the harness writes it, and DONE is the file to wait on.
    `results` is where phases.log, all.json and DONE go — the self-test passes
    its own directory so a fake chain never lands in the live one."""
    c = cfg()
    steps = steps or [("up", COMMANDS["up"]), ("preflight", cmd_preflight), ("completeness", cmd_completeness),
                      ("tinyproof", cmd_tinyproof), ("fill", cmd_fill), ("suite", cmd_suite), ("report", cmd_report)]
    results = results or c.results
    os.makedirs(results, exist_ok=True)
    phases = os.path.join(results, "phases.log")
    done = os.path.join(results, "DONE")
    if os.path.exists(done):
        os.remove(done)
    # GUARD: sweep before the chain starts, not only at tinyproof and down.
    # A chain that fails restarts from the top, and the attempt before it can
    # still be running: clean-room run 43 restarted three times and left a
    # `prove.py tinyproof` (parent already dead) and a generator writing
    # 400,000,000 records at the broker the next attempt was measuring. Load
    # average sat at 9.24 on eight cores with nothing supposed to be running,
    # its warm-up read 957,976 then 216,896 then 12,697,565 records an interval
    # and never settled, and garbage collection took 14.1% of a case. Every one
    # of those was read as the machine being too small for the pipeline. The
    # reaper already knew how to find them -- it was only ever asked at the end.
    if results == c.results:
        swept = L.reap_host_watchers(ignore_children=True)
        if swept:
            log(f"  swept {len(swept)} process(es) left over from an earlier attempt: "
                + ", ".join(f"{pid} {cl[:70]}" for pid, cl in swept))
            log("  a previous chain was still running. Its numbers and this one's would have "
                "shared the machine, so they are gone before anything is measured.")

    out = {"build": build_hash() if os.path.exists(c.jar) else None, "steps": []}
    quick0 = L.QUICK   # GUARD: a phase that leaves the flag different from how it
                       # found it mis-stamps every table after it (2026-09-05: the
                       # tiny proof's self-test cleared it and the suite voided
                       # itself as "1 pass < 2" while still running one pass).

    def mark(line):
        with open(phases, "a") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + line + "\n")
        if results == c.results:  # the self-test's fake chain stays out of harness.log too (run 12)
            log(line)

    def save_all():
        with open(os.path.join(results, "all.json"), "w") as f:
            json.dump(out, f, indent=2, default=str)

    t_all = time.time()
    mark("phase=all start")
    verdict = "PASS"
    say = {"up": "starting the stack", "preflight": "preflight checks",
           "completeness": "proving nothing is lost, including after killing the pipeline mid-run",
           "tinyproof": "the tiny proof: two cases end to end, and every guard broken on purpose",
           "fill": "filling the backlog — the long quiet one",
           "suite": "measuring the cases", "report": "writing the report"}
    for i, (name, fn) in enumerate(steps):
        t0 = time.time()
        mark(f"phase={name} start")
        if results == c.results:
            L.progress(f"step {i + 1} of {len(steps)}: {say.get(name, name)}", pct=i / len(steps))
        try:
            rc = fn()
        except Refusal as e:
            log(f"FAILED ({e.scope}): {e.msg}")
            rc = 1
        finally:
            if name in ("completeness", "tinyproof", "suite"):
                try:
                    L.stop_sampler(); L.stop_tm()
                except Exception:
                    pass
        if L.QUICK != quick0:
            log(f"phase {name} left the quick flag {L.QUICK} (it was {quick0}); restoring")
            L.QUICK = quick0
            rc = rc or 1
        if name == "report" and rc:
            log("the chain measured a valid table whose step ratios do not meet the claim")
        out["steps"].append({"step": name, "rc": rc, "seconds": round(time.time() - t0, 1)})
        mark(f"phase={name} end rc={rc} {time.time() - t0:.0f}s")
        save_all()
        if rc:
            # What a person reads when the chain is over. "FAIL at report"
            # said nothing about what happened; the run it described had a
            # clean table and a pipeline that missed its target. And a run
            # that measured nothing is a third thing again -- clean-room run
            # 46 kept two cases out of ten, both at the same core count, and
            # this file said PASS.
            why = None
            if name == "report":
                try:
                    why = (load_json("suite.json") or {}).get("reportVerdict")
                except Exception:
                    why = None
            verdict = ({"no-result": "STOPPED at report: no scaling result — too many cases were "
                                     "thrown out to compare one core count with another",
                        "claim-not-met": "STOPPED at report: the table is good, the pipeline did "
                                         "not meet the target"}.get(why,
                        "STOPPED at report: the table could not be reported")
                       if name == "report" else f"STOPPED at {name}")
            break
    out["verdict"] = verdict
    out["seconds"] = round(time.time() - t_all, 1)
    save_all()
    mark(f"phase=all end {verdict} {out['seconds']/60:.1f} min")
    with open(done, "w") as f:
        f.write(f"{verdict} {out['seconds']/60:.1f} min\n")
    return 0 if verdict == "PASS" else 1


# ---------------------------------------------------------------------- main

def cmd_probe():
    """What this machine's own cores do, with no pipeline involved.

    Preflight already runs three repeats. That is enough to put a number on the
    page and rarely enough to explain a shortfall: run 31's memory-bound 2->4
    read 76% with a range of 70-88%, which is wider than the 14 points it was
    being asked to account for. This runs it again with as many repeats as it
    takes to be worth quoting, and says plainly whether it is.

    Minutes, and no stack: `prove.py probe --repeats 9`.
    """
    c = cfg()
    reps = 3
    for i, a in enumerate(sys.argv):
        if a == "--repeats" and i + 1 < len(sys.argv):
            reps = int(sys.argv[i + 1])
    print(f"probing this machine at {sorted(set(c.cases))} cores, {reps} repeats per case, "
          f"no pipeline involved")
    h = L.host_scaling(seconds=6.0, cases=tuple(sorted(set(c.cases))), repeats=reps)
    if not h or h.get("error"):
        print(f"could not probe: {(h or {}).get('error', 'no probe source')}")
        return 1
    save_json("hostprobe.json", h)
    print()
    for mode, label in (("alu", "simple arithmetic"), ("mem", "memory-heavy work")):
        pc = (h.get("perCore") or {}).get(mode) or {}
        if pc:
            print(f"  {label:<20} per core: " +
                  "  ".join(f"{k}c {v:,.0f}" for k, v in sorted(pc.items(), key=lambda x: int(x[0]))))
        for step, v in ((h.get("ofLinear") or {}).get(mode) or {}).items():
            try:
                a, b = (float(x) for x in step.split("->"))
                ideal = b / a
            except Exception:
                ideal = 2.0
            r = ((h.get("ofLinearRange") or {}).get(mode) or {}).get(step) or {}
            rng = (f"  [{r['low'] * ideal:.2f}-{r['high'] * ideal:.2f}x]" if r else "")
            print(f"  {label:<20} {step}: {v * ideal:.2f}x{rng}")
    sp = L.probe_spread(h)
    print()
    # per arm: one arm is often steady while another is not, and a single
    # worst-of figure makes the steady one look as useless as the unsteady one
    for mode, label in (("alu", "simple arithmetic"), ("mem", "memory-heavy work")):
        a = L.probe_spread(h, mode=mode)
        if a["envelope"] or a["middleHalf"] is not None:
            print(f"  {label:<20} full range {a['envelope']:.0%}"
                  + (f", middle half {a['middleHalf']:.0%}" if a["middleHalf"] is not None else ""))
    print(f"  the widest of those is {sp['envelope']:.0%} over {reps} repeats"
          + (f", middle half {sp['middleHalf']:.0%}." if sp["middleHalf"] is not None
             else " — too few to take a middle half from."))
    print("  Compare the middle half with the shortfall before leaning on it: a bound that")
    print("  moves more than the thing it is meant to explain, explains nothing. The full")
    print("  range is an envelope and gets wider with repeats, never narrower, so it is not")
    print("  the figure to tighten. If the middle half is still too wide, say it cannot be")
    print("  settled here — that is a complete answer.")
    return 0


COMMANDS = {
    "replay": lambda: cmd_replay(),
    "selftest": lambda: cmd_selftest(live=True),
    "selftest-pure": lambda: cmd_selftest(live=False),
    "up": lambda: (L.stack_up(), 0)[1],
    "preflight": cmd_preflight,
    "tinyproof": cmd_tinyproof,
    "fill": cmd_fill,
    "completeness": cmd_completeness,
    "suite": cmd_suite,
    "ceiling": cmd_ceiling,
    "probe": cmd_probe,
    "report": cmd_report,
    "down": lambda: (L.stack_down(), 0)[1],
    "all": cmd_all,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__); sys.exit(2)
    name = sys.argv[1]
    if "--quick" in sys.argv[2:]:
        L.QUICK = True
        print(f"QUICK LOOK: {L.T['quickPasses']} passes per case; the table it writes is marked unpublishable")
    if name not in ("replay",):
        cfg()  # validate pipeline.json first
    if name not in ("replay", "selftest-pure", "report"):
        rc = cmd_replay()
        if rc:
            print("not running: a threshold disagrees with the record")
            sys.exit(rc)
    try:
        rc = COMMANDS[name]()
    except Refusal as e:
        print(f"FAILED ({e.scope}): {e.msg}")
        rc = 1
    except KeyboardInterrupt:
        rc = 130
    finally:
        if name in ("suite", "tinyproof", "completeness", "ceiling", "selftest", "all"):
            try:
                L.stop_sampler(); L.stop_tm()
            except Exception:
                pass
    sys.exit(rc)
