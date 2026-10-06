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
import calendar
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
        print("REPLAY STOPPED: a threshold disagrees with the record. Fix the threshold, not the record.")
        return 1
    if (replay_names() or replay_cases() or replay_configs() or replay_sizing()
            or replay_broker_memory()):
        print("REPLAY STOPPED: a guard disagrees with a recorded configuration. Fix the guard, not the record.")
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
            res = dict(guard=name, ok=should_fire and needle.lower() in msg.lower(), result="FIRED", message=msg[:200])
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
    # 2026-09-28: three reference runs printed "missed" for a step whose range
    # spanned the target. Replayed over 63 recorded suites: 33 of 36 "missed"
    # steps spanned it, 3 were wholly under, and no "met" step changed.
    def verdict_of(step, want):
        def go():
            got = L.step_verdict(step)
            assert got == want, f"judged {got!r}, expected {want!r}"
        return go
    run4 = dict(reportable=True, meetsClaim=False, ratio=1.779, idealRatio=2.0,
                ratioLowCI=1.645, ratioHighCI=1.823, adjacentPairs=[1.734, 1.734, 1.87],
                step="2->4", **{"from": 2, "to": 4})
    expect("verdict: reference run 4's 2->4, 1.65-1.82x, is undecided (must not fire)",
           verdict_of(run4, "undecided"), "", should_fire=False)
    expect("verdict: run 37's 2->4, 1.75-1.78x, wholly under 1.80x, is missed (must not fire)",
           verdict_of(dict(run4, ratio=1.77, ratioLowCI=1.752, ratioHighCI=1.776), "missed"), "",
           should_fire=False)
    expect("verdict: a range wholly above 1.80x is met (must not fire)",
           verdict_of(dict(run4, meetsClaim=True, ratio=1.90, ratioLowCI=1.88, ratioHighCI=1.94), "met"), "",
           should_fire=False)
    expect("verdict: one pair and no range is judged on the point, and under is missed (must not fire)",
           verdict_of(dict(reportable=True, meetsClaim=False, ratio=1.70, idealRatio=2.0), "missed"), "",
           should_fire=False)
    expect("verdict: a step not reported has no verdict (must not fire)",
           verdict_of(dict(reportable=False), None), "", should_fire=False)
    def settles(runs_path_rates, want_seq):
        """Extra cases chosen one after another settle toward the open step."""
        def go():
            runs = [dict(cores=c, recordsPerSec=r, status="OK", pass_=p) for c, r, p in runs_path_rates]
            got = []
            for _ in range(len(want_seq)):
                nxt = L.settle_next(runs, [run4])
                assert nxt, "stopped before the step settled"
                got.append(nxt[0])
                runs.append(dict(cores=nxt[0], recordsPerSec=1.0, status="OK"))
            assert got == want_seq, f"ran {got}, expected {want_seq}"
            assert L.settle_next(runs, [dict(run4, meetsClaim=True)]) is None, "kept going on a settled step"
        return go
    # Reference run 4's order ended on the 1-core sentinel.
    RUN4 = [(1, 838186, "p1"), (2, 1574122, "p1"), (4, 2728757, "p1"), (4, 2846468, "p2"),
            (2, 1522041, "p2"), (1, 768518, "p2"), (1, 776678, "p3"), (2, 1558714, "p3"),
            (4, 2703505, "p3"), (1, 835127, "sentinel")]
    expect("settle: after the sentinel, 2->4 alternates 2, 4, 2, 4 (must not fire)",
           settles(RUN4, [2, 4, 2, 4]), "", should_fire=False)
    expect("settle: after a 2-core case, 2->4 starts at 4 (must not fire)",
           settles(RUN4[:8], [4, 2, 4]), "", should_fire=False)
    expect("scorecard: a step that is undecided asks for more passes, not tuning (must not fire)",
           steps({}, run4, False, "more passes"), "", should_fire=False)

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
    def one_verdict_per_step():
        # Run 51 printed "2→4 met the target" and, twenty lines lower, "2->4 ...
        # above 2.00x, so the smaller case reads low" about one step. Whatever
        # calls a step met and whatever says it reads low must never both apply.
        high = dict(step="2->4", reportable=True, meetsClaim=True, ratio=2.121,
                    ratioLowCI=2.03, idealRatio=2.0)
        noisy = dict(high, step="1->2", ratioLowCI=1.95)   # high middle, low end under 2
        if L.met_steps([high]) or not L.reads_low(high):
            raise Exception("a step whose low end is above 2.00x is called met")
        if L.met_steps([noisy]) != ["1->2"] or L.reads_low(noisy):
            raise Exception("a step whose low end is under 2.00x is not called met")
        # the scorecard's own "reads low" line is pinned by the test at the top
    expect("a step is met or reads low, never both (must not fire)", one_verdict_per_step, "",
           should_fire=False)
    expect("action: a step above 2x says the baseline reads low (must not fire)",
           steps(dict(tmCapFrac=0.99), over, False, "baseline reads low"), "", should_fire=False)
    expect("action: says so when there is no usable step (must not fire)",
           acts(dict(tmCapFrac=0.99), "no usable step"), "", should_fire=False)
    expect("action: names the Kafka memory to try, in gigabytes (must not fire)",
           acts(dict(tmCapFrac=0.93, brokerLimitHits=12780, brokerLimitBytes=4096 * 1048576),
                "raise kafkaMemory"), "", should_fire=False)
    expect("action: more memory for the pipeline (must not fire)",
           acts(dict(tmCapFrac=0.99, gcFracOfCapacity=0.064), "try more memory"), "", should_fire=False)

    def gc_states_no_cause():
        # A garbage-collection ceiling says what was measured. "Ran short of
        # memory" and "give it more memory" were causes nobody measured: run 51
        # followed that advice (+30% at 1 core) and its GC did not move.
        runs = [dict(cores=1, status="OK", recordsPerSec=50_000, gcFracOfCapacity=0.08),
                dict(cores=2, status="OK", recordsPerSec=120_000, gcFracOfCapacity=0.02)]
        L.gc_judgement(runs)
        text = runs[0].get("ceiling") or ""
        label = L.bottleneck(dict(tmCapFrac=0.99, gcFracOfCapacity=0.064, cores=1))
        for said in (text, label):
            if "ran short of memory" in said or "Give it more memory" in said:
                raise Exception(f"states a cause that was not measured: {said!r}")
            if "not been measured" not in said:
                raise Exception(f"does not say the remedy is untested: {said!r}")
        if "less work per core" not in text:
            raise Exception(f"the ceiling no longer says what was measured: {text!r}")
    expect("GC ceiling: what was measured, not a cause (must not fire)", gc_states_no_cause, "",
           should_fire=False)
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
    expect("garbage collection over the limit is flagged on the pass, not ruled on (must not fire)",
           case(gcFracOfCapacity=0.13), "", should_fire=False)
    # Judged at the table, against the nearest larger case under the limit. Real
    # per-core figures from the record (pre-fix GC halved to its true value).
    def gcj(cases, want):
        def go():
            runs = []
            for cores, rate, gcf in cases:
                for p in ("p1-asc", "p2-desc"):
                    runs.append(dict(cores=cores, **{"pass": p}, recordsPerSec=rate, status="OK",
                                     gcFracOfCapacity=gcf))
            L.gc_judgement(runs)
            got = {r["cores"]: r["status"] for r in runs}
            assert got == want, f"got {got}, expected {want}"
        return go
    expect("gc: run 50's 1-core case (GC 9.1%, as much work per core as 2 cores) is kept (must not fire)",
           gcj([(1, 28357, 0.091), (2, 56231, 0.033), (4, 105317, 0.010)], {1: "OK", 2: "OK", 4: "OK"}),
           "", should_fire=False)
    expect("gc: run 25's 1-core case (GC 7.7%, 0.85x the 2-core case per core) is a ceiling (must not fire)",
           gcj([(1, 220719, 0.077), (2, 517824, 0.030), (4, 913312, 0.014)], {1: "CEILING", 2: "OK", 4: "OK"}),
           "", should_fire=False)
    expect("gc: run 21's 1-core case is judged against the 4-core case, not a 2-core case also over (must not fire)",
           gcj([(1, 198930, 0.132), (2, 389594, 0.068), (4, 726636, 0.043)], {1: "OK", 2: "OK", 4: "OK"}),
           "", should_fire=False)
    def gc_as_saved():
        # the suite saves after every pass: judge after the first 1-core pass alone,
        # then again once the larger cases exist -- run 50's figures, which pass
        runs = [dict(cores=1, **{"pass": "p1-asc"}, recordsPerSec=28357, status="OK",
                     gcFracOfCapacity=0.091, gcAboveLimit=True)]
        L.build_table(runs)
        assert runs[0]["status"] == "CEILING", runs[0]
        for c, rate, gcf in ((2, 56231, 0.033), (4, 105317, 0.010), (2, 55800, 0.033), (4, 104900, 0.010)):
            runs.append(dict(cores=c, **{"pass": "p"}, recordsPerSec=rate, status="OK", gcFracOfCapacity=gcf))
        runs.append(dict(cores=1, **{"pass": "p2-desc"}, recordsPerSec=28100, status="OK",
                         gcFracOfCapacity=0.090, gcAboveLimit=True))
        L.build_table(runs)
        got = {r["status"] for r in runs if r["cores"] == 1}
        assert got == {"OK"}, f"the first 1-core pass stayed ruled out: {[r['status'] for r in runs if r['cores']==1]}"
    expect("gc: a ruling made before the larger cases exist does not stick (must not fire)",
           gc_as_saved, "", should_fire=False)
    expect("gc: the largest case over the limit has nothing to compare with and is a ceiling (must not fire)",
           gcj([(1, 130000, 0.030), (2, 255000, 0.030), (4, 470000, 0.070)], {1: "OK", 2: "OK", 4: "CEILING"}),
           "", should_fire=False)
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
    # The dashboard (2026-09-28): a dashboard asking for data source
    # "prometheus" where the stack provisioned "st44prom" loaded in Grafana with
    # every panel empty, and the check that ran queried Prometheus directly.
    def panel(uid="st44prom", expr="up", title="p"):
        t = {"refId": "A", "expr": expr}
        return {"type": "timeseries", "title": title, "targets": [t],
                "datasource": None if uid is None else ({"type": "prometheus", "uid": uid}
                                                        if not uid.startswith("$") else uid)}
    DS = [{"name": "Prometheus", "uid": "st44prom", "type": "prometheus"}]

    def wiring(boards, want, ds=DS):
        def go():
            problems, _ = L.dashboard_wiring(ds, boards)
            if want is None:
                assert not problems, f"stopped a good dashboard: {problems}"
            else:
                assert problems and want in " ".join(problems), f"said {problems}, expected {want!r}"
        return go
    row = {"type": "row", "title": "r", "panels": []}
    expect("dashboard: the 2026-09-28 mistake, a data source nobody provisioned, is found (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [row, panel("prometheus"), panel("prometheus", title="q")]}},
                  '2 panels ask for data source "prometheus"; the stack provisions Prometheus, st44prom'),
           "", should_fire=False)
    expect("dashboard: panels naming the provisioned source pass (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel(), {"type": "text", "title": "notes"}]}}, None),
           "", should_fire=False)
    expect("dashboard: a panel with no data source uses the default and passes (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel(None)]}}, None), "", should_fire=False)
    expect("dashboard: a data source variable the dashboard defines passes (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel("${DS}")],
                              "templating": {"list": [{"name": "DS", "type": "datasource"}]}}}, None),
           "", should_fire=False)
    expect("dashboard: a data source variable nobody defines is found (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel("${DS}")]}}, "variable ${DS}"),
           "", should_fire=False)
    expect("dashboard: a panel with no query is found (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel(expr="  ", title="empty one")]}},
                  '"empty one" has no query'), "", should_fire=False)
    expect("dashboard: no data source provisioned is found (must not fire)",
           wiring({"d.json": {"title": "d", "panels": [panel(None)]}}, "no data source is provisioned", ds=[]),
           "", should_fire=False)

    def files(split):
        def go():
            with tempfile.TemporaryDirectory() as tdir:
                prov = os.path.join(tdir, "prov")
                os.makedirs(os.path.join(prov, "datasources")); os.makedirs(os.path.join(prov, "dashboards", "json"))
                open(os.path.join(prov, "datasources", "ds.yml"), "w").write(
                    "apiVersion: 1\ndatasources:\n  - name: Prometheus\n    uid: st44prom\n    type: prometheus\n")
                open(os.path.join(prov, "dashboards", "p.yml"), "w").write(
                    "apiVersion: 1\nproviders:\n  - name: x\n    options:\n      path: /etc/grafana/provisioning/dashboards/json\n")
                json.dump({"title": "d", "panels": [panel()]}, open(os.path.join(prov, "dashboards", "json", "d.json"), "w"))
                mounts = ({"/etc/grafana/provisioning/datasources": prov + "/datasources",
                           "/etc/grafana/provisioning/dashboards": prov + "/dashboards"} if split
                          else {"/etc/grafana/provisioning": prov})
                ds, boards, found = L.dashboard_files({"service": "grafana", "mounts": mounts})
                assert not found, found
                assert [d["uid"] for d in ds] == ["st44prom"], ds
                assert len(boards) == 1, boards
        return go
    expect("dashboard: provisioning mounted whole is read (must not fire)", files(False), "", should_fire=False)
    expect("dashboard: provisioning mounted in two parts, like run 36, is read (must not fire)",
           files(True), "", should_fire=False)

    def service():
        svc = L.dashboard_service({"exporter": {"image": "python:3.12-alpine"},
                                   "grafana": {"image": "grafana/grafana:11.2.0", "ports": ['"13000:3000"'],
                                               "volumes": ["/r/dash/prov:/etc/grafana/provisioning:ro"]}})
        assert svc and svc["port"] == 13000 and svc["mounts"] == {"/etc/grafana/provisioning": "/r/dash/prov"}, svc
    expect("dashboard: the Grafana service and its port are found among extraServices (must not fire)",
           service, "", should_fire=False)

    def empties():
        got = L.empty_panels([("d", "full", 40, None), ("d", "empty", 0, None), ("d", "broken", 0, "bad query")])
        assert got == ['"empty"', '"broken" (bad query)'], got
        assert L.empty_panels([("d", "full", 40, None)]) == []
    expect("dashboard: panels with no points or an error are named (must not fire)", empties, "",
           should_fire=False)

    def settling(open_until, budget, stop_at=None):
        """The settling loop with a fake case runner: the step stays open until
        `open_until` runs exist."""
        def go():
            runs = [dict(cores=c_, recordsPerSec=r, status="OK") for c_, r, _ in RUN4]
            ran, labels, said = [], [], []

            def run_one(cores, pass_id, label=None):
                runs.append(dict(cores=cores, recordsPerSec=1.0, status="OK"))
                ran.append(cores); labels.append(label)
                return ("rig", "stopped") if stop_at and len(ran) == stop_at else None
            extra, stop = settle_suite(runs, run_one, budget,
                                       judge=lambda rs: [run4] if len(rs) < open_until else [dict(run4, meetsClaim=True)],
                                       say=said.append)
            return extra, stop, ran, labels, said
        return go

    def settles_in_two():
        extra, stop, ran, labels, said = settling(12, 6)()
        assert (extra, stop, ran) == (2, None, [2, 4]), (extra, stop, ran)
        assert labels[0] == "suite: settling 2->4, extra case 1 of up to 6 (2 cores)", labels[0]
        assert len(said) == 1 and "Running up to 6 more case(s) of 2 and 4 cores" in said[0], said
    expect("settle: the loop runs the open step's two sizes until it settles (must not fire)",
           settles_in_two, "", should_fire=False)

    def never_settles():
        extra, stop, ran, _, _ = settling(10_000, 6)()
        assert extra == 6 and ran == [2, 4, 2, 4, 2, 4] and stop is None, (extra, ran)
    expect("settle: the loop stops at its budget when the step never settles (must not fire)",
           never_settles, "", should_fire=False)

    def stops_on_rig():
        extra, stop, ran, _, _ = settling(10_000, 6, stop_at=1)()
        assert extra == 1 and stop == ("rig", "stopped"), (extra, stop)
    expect("settle: a rig check stopping a settling case stops the loop (must not fire)",
           stops_on_rig, "", should_fire=False)

    def not_settled_words():
        step = dict(run4, adjacentPairs=[1.697, 1.75, 1.8, 1.85, 1.86, 1.9, 1.95, 1.982],
                    ratio=1.858, ratioLowCI=1.771, ratioHighCI=1.9)
        assert L.settle_range(step) == ("The range its 8 pairs of passes support is 1.77x to 1.90x "
                                        "(single pairs ran 1.70x to 1.98x)"), L.settle_range(step)
        done = report_verdict("not-settled", {"unsettledSteps": [
            {"step": "2->4", "ratio": 1.858, "low": 1.771, "high": 1.9, "need": 1.8, "pairs": 8}]})
        assert done == ("STOPPED at report: undecided — 2→4 reads 1.86x, and the range its 8 pairs "
                        "support, 1.77x to 1.90x, spans the 1.80x target. Report it as undecided and "
                        "change nothing in the pipeline"), done
        assert report_verdict("claim-not-met", {}).startswith("STOPPED at report: the table is good")
        assert report_verdict(None, None) == "STOPPED at report: the table could not be reported"
    expect("report: the not-settled range and DONE line read as written (run 52's figures) (must not fire)",
           not_settled_words, "", should_fire=False)

    def memory_row():
        # The row's arithmetic from the example config: the worker's container
        # limit at its largest case, broker, job manager 2g and extra services;
        # an unreadable limit is named, not a crash.
        saved = c.raw.get("extraServices")
        try:
            c.raw["extraServices"] = {"grafana": {"mem_limit": "224m"}, "prometheus": {"mem_limit": "192m"},
                                      "odd": {"mem_limit": "1.5g"}}
            _, limit = L.tm_memory_for(max(c.cases))
            want_worker = L._mib(limit) if L.tm_memory_capped() and limit else 0
            line = L.memory_budget_line(10_000 * 1048576)
            need = want_worker + L._mib(c.kafka_mem) + 2048 + 416
            assert f"= {need:.0f}m of 10000m VM" in line, line
            assert "other services 416m" in line and "could not read the memory limit of odd" in line, line
            assert L.memory_budget_line(0).endswith("requested, VM size unknown"), L.memory_budget_line(0)
        finally:
            if saved is None:
                c.raw.pop("extraServices", None)
            else:
                c.raw["extraServices"] = saved
    expect("memory: the budget row adds container limits and names a limit it cannot read (must not fire)",
           memory_row, "", should_fire=False)

    def worker_memory():
        # Clean-room run 52: the memory row budgeted the worker's process size,
        # while start_tm gives its container 1.25x that. Both now read one
        # function; with a per-core figure the limit is 1.25x the process.
        if not c.tm_mem_per_core or c.tm_mem_limit_per_core:
            return
        for n in c.cases:
            mem, limit = L.tm_memory_for(n)
            if abs(L._mib(limit) - int(L._mib(mem) * 1.25)) > 1:
                raise Exception(f"{n} cores: process {mem}, container limit {limit}")
    expect("memory: the worker's container limit is 1.25x its process size, as started (must not fire)",
           worker_memory, "", should_fire=False)

    def inputs_from_the_spec():
        # The number of inputs comes from the requirements (design.inputs), not
        # from the harness: every declared input gets a placeholder, and the
        # scaled one follows topic_in to the tiny proof's topic.
        saved = (c.topic_in, c.design.get("inputs"))
        try:
            c.design["inputs"] = [c.suite_topic_in, "prices"]
            got = c.fmt("--in {in} --a {in0} --b {in1} --all {ins}")
            want = f"--in {c.suite_topic_in} --a {c.suite_topic_in} --b prices --all {c.suite_topic_in},prices"
            assert got == want, got
            c.topic_in = c.suite_topic_in + "-tiny"
            got = c.fmt("{in0} {in1}")
            assert got == f"{c.suite_topic_in}-tiny prices", got
        finally:
            c.topic_in, c.design["inputs"] = saved[0], saved[1]
            if saved[1] is None:
                c.design.pop("inputs", None)
    expect("args: every declared input has a placeholder, and the scaled one follows the tiny proof "
           "(must not fire)", inputs_from_the_spec, "", should_fire=False)

    # A fake Grafana for the parts of the dashboard checks that talk to it.
    # They ran only live until 2026-10-02 (two reference runs, clean-room run
    # 52), and the path that stops the chain on an empty panel had never run.
    import http.server, threading, urllib.parse as _up

    class FakeGrafana:
        def __init__(self, prov):
            self.prov, self.ds_status, self.hide, self.empty = prov, "OK", set(), set()
            fake = self

            class H(http.server.BaseHTTPRequestHandler):
                def log_message(self, *a):
                    pass

                def reply(self, code, body):
                    data = json.dumps(body).encode()
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def do_GET(self):
                    path = _up.urlparse(self.path).path
                    boards = fake.boards()
                    if path == "/api/health":
                        return self.reply(200, {"database": "ok"})
                    if path == "/api/search":
                        return self.reply(200, [{"uid": u, "title": b["title"]} for u, b in boards.items()
                                                if b["title"] not in fake.hide])
                    if path.startswith("/api/dashboards/uid/"):
                        return self.reply(200, {"dashboard": boards[path.rsplit("/", 1)[1]]})
                    if path.startswith("/api/datasources/uid/") and path.endswith("/health"):
                        return self.reply(200, {"status": fake.ds_status, "message": "fake"})
                    self.reply(404, {"message": "not found"})

                def do_POST(self):
                    body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    res = {}
                    for q in body["queries"]:
                        if (q.get("datasource") or {}).get("uid") != "st44prom":
                            return self.reply(404, {"message": "data source not found"})
                        n = 0 if q["expr"] in fake.empty else 30
                        res[q["refId"]] = {"frames": [{"data": {"values": [list(range(n)), [1.0] * n]}}]}
                    self.reply(200, {"results": res})

            self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
            self.port = self.srv.server_address[1]
            threading.Thread(target=self.srv.serve_forever, daemon=True).start()

        def boards(self):
            d = os.path.join(self.prov, "dashboards", "json")
            out = {}
            for f in os.listdir(d):
                b = json.load(open(os.path.join(d, f)))
                out[b.get("uid") or f] = b
            return out

        def close(self):
            # shutdown() alone leaves the socket accepting, so a request waits
            # out its timeout instead of being refused as by a Grafana that is down
            if not getattr(self, "closed", False):
                self.closed = True
                self.srv.shutdown()
                self.srv.server_close()

    def grafana_rig(fn):
        def go():
            with tempfile.TemporaryDirectory() as tdir:
                prov = os.path.join(tdir, "prov")
                os.makedirs(os.path.join(prov, "datasources")); os.makedirs(os.path.join(prov, "dashboards", "json"))
                open(os.path.join(prov, "datasources", "ds.yml"), "w").write(
                    "apiVersion: 1\ndatasources:\n  - name: Prometheus\n    uid: st44prom\n    type: prometheus\n")
                open(os.path.join(prov, "dashboards", "p.yml"), "w").write(
                    "apiVersion: 1\nproviders:\n  - name: x\n    options:\n      path: /etc/grafana/provisioning/dashboards/json\n")
                json.dump({"uid": "d1", "title": "Pipeline", "time": {"from": "now-90m", "to": "now"},
                           "panels": [panel(expr="rate_a", title="Input rate"),
                                      panel(expr="lag_b", title="Records waiting")]},
                          open(os.path.join(prov, "dashboards", "json", "d.json"), "w"))
                fake = FakeGrafana(prov)
                try:
                    svc = {"service": "grafana", "port": fake.port, "mounts": {"/etc/grafana/provisioning": prov}}
                    fn(fake, svc)
                finally:
                    fake.close()
        return go

    def loaded(fake, svc):
        ds, boards, found = L.dashboard_files(svc)
        assert not found, found
        assert "has loaded 1 dashboard" in L.dashboard_loaded(svc, ds, boards, wait_s=5, tries=1)
        fake.ds_status = "ERROR"
        try:
            L.dashboard_loaded(svc, ds, boards, wait_s=5, tries=1)
            raise AssertionError("a data source failing its health check passed")
        except Exception as e:
            assert "does not answer" in str(e), e
        fake.ds_status, fake.hide = "OK", {"Pipeline"}
        try:
            L.dashboard_loaded(svc, ds, boards, wait_s=5, tries=1)
            raise AssertionError("a dashboard Grafana had not loaded passed")
        except Exception as e:
            assert "has not loaded Pipeline" in str(e), e
    expect("grafana: loaded, data source answering, and both failures caught (must not fire)",
           grafana_rig(loaded), "", should_fire=False)

    def has_data(fake, svc):
        now = time.time()
        assert L.dashboard_stop_reason(svc, now - 600, now) is None
        fake.empty = {"lag_b"}
        why = L.dashboard_stop_reason(svc, now - 600, now)
        assert why and '1 of 2 dashboard panels show no data' in why and '"Records waiting"' in why, why
    expect("grafana: every panel with data passes; one empty panel is named (must not fire)",
           grafana_rig(has_data), "", should_fire=False)

    def grafana_down(fake, svc):
        fake.close()
        now = time.time()
        why = L.dashboard_stop_reason(svc, now - 600, now)
        assert why and "could not be read through Grafana" in why, why
    expect("grafana: a Grafana that does not answer stops it, and says so (must not fire)",
           grafana_rig(grafana_down), "", should_fire=False)

    def opens_on_suite(fake, svc):
        t0, t1 = time.time() - 3600, time.time() - 600
        ok, detail = L.dashboard_open_on_suite(svc, t0, t1, wait_s=5)
        assert ok and detail.startswith("opens on the suite"), detail
        b = fake.boards()["d1"]
        assert L.range_covers(b, t0, t1) and b["refresh"] == "", b.get("time")
    expect("grafana: the suite's range is written to the file and read back (must not fire)",
           grafana_rig(opens_on_suite), "", should_fire=False)

    def section7_names(fake, svc):
        now = time.time()
        assert L.section7_metrics_missing(svc, now - 600, now) == []
        assert L.section7_metrics_line([]).startswith(f"all {len(L.SECTION7_METRICS)} series")
        gone = "flink_jobmanager_job_numRestarts"
        fake.empty = {f'count({{__name__="{gone}"}})'}
        missing = L.section7_metrics_missing(svc, now - 600, now)
        assert missing == [gone], missing
        line = L.section7_metrics_line(missing)
        assert "1 series this image did not export: flink_jobmanager_job_numRestarts" in line, line
        assert L.section7_metrics_missing(None, now - 600, now) is None
    expect("grafana: a section 7 series the image does not export is named (must not fire)",
           grafana_rig(section7_names), "", should_fire=False)

    def chain_stops_on_empty_panel(fake, svc):
        fake.empty = {"rate_a"}
        ran = []
        with tempfile.TemporaryDirectory() as tmp:
            rc = cmd_all(steps=[("completeness", lambda: (ran.append("completeness"), 0)[1]),
                                ("tinyproof", lambda: (ran.append("tinyproof"), 0)[1])],
                         results=tmp,
                         dashboard_check=lambda since: L.dashboard_stop_reason(svc, since - 60, time.time()))
            done = open(os.path.join(tmp, "DONE")).read()
        assert rc == 1 and ran == ["completeness"], (rc, ran)
        assert done.startswith("STOPPED at completeness: 1 of 2 dashboard panels show no data"), done
        assert '"Input rate"' in done, done
    expect("chain: an empty dashboard panel stops it at completeness, before the suite (must not fire)",
           grafana_rig(chain_stops_on_empty_panel), "", should_fire=False)

    def one_query_empty():
        qs = [{"refId": "A", "expr": "a", "legendFormat": "orders read"},
              {"refId": "B", "expr": "b", "legendFormat": "rows written"}]
        frame = lambda n: {"frames": [{"data": {"values": [list(range(n)), [1.0] * n]}}]}
        n, err = L.points_by_query({"results": {"A": {"frames": []}, "B": frame(40)}}, qs)
        assert n == 0 and err and "orders read" in err, (n, err)
        n, err = L.points_by_query({"results": {"A": frame(30), "B": frame(40)}}, qs)
        assert n == 30 and err is None, (n, err)
    expect("dashboard: a panel with one empty query is named, not passed on the other's points "
           "(must not fire)", one_query_empty, "", should_fire=False)
    # 2026-09-28: the reference dashboard opened on "now-90m". It covered the
    # suite for 90 minutes and then showed "No data" on every Flink panel.
    T0, T1 = 1_790_605_140, 1_790_607_300

    def opens(board, want):
        def go():
            got = L.range_covers(board, T0, T1)
            assert got == want, f"range {board.get('time')} judged {got}, expected {want}"
        return go
    iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(t))
    expect("dashboard: opening on the last 90 minutes does not cover the suite later (must not fire)",
           opens({"time": {"from": "now-90m", "to": "now"}}, False), "", should_fire=False)
    expect("dashboard: a fixed range around the suite covers it (must not fire)",
           opens({"time": {"from": iso(T0 - 60), "to": iso(T1 + 30)}}, True), "", should_fire=False)
    expect("dashboard: a fixed range in epoch milliseconds covers it (must not fire)",
           opens({"time": {"from": str((T0 - 5) * 1000), "to": str((T1 + 5) * 1000)}}, True), "",
           should_fire=False)
    expect("dashboard: a range that ends before the suite does does not cover it (must not fire)",
           opens({"time": {"from": iso(T0 - 60), "to": iso(T1 - 600)}}, False), "", should_fire=False)

    def rewrite():
        for wrapped in (False, True):
            b = {"title": "d", "time": {"from": "now-90m", "to": "now"}, "refresh": "10s", "panels": [panel()]}
            b = {"dashboard": b} if wrapped else b
            got = L.with_suite_range(b, T0, T1)
            assert L.range_covers(got, T0, T1), got
            inner = got.get("dashboard", got)
            assert inner["refresh"] == "" and inner["panels"] == [panel()], inner
            assert b.get("dashboard", b)["time"]["from"] == "now-90m", "changed the board it was given"
    expect("dashboard: the suite's range is written in, refresh off, panels untouched (must not fire)",
           rewrite, "", should_fire=False)
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
    # Clean-room run 50: cases 1, 2, 4; every 1-core pass thrown out for GC.
    def gaps(runs, cases, want_steps):
        def go():
            table = L.build_table(runs, cases)
            got = [g["step"] for g in L.missing_steps(cases, table, runs)]
            assert got == want_steps, f"missing {got!r}, expected {want_steps!r}"
        return go
    def pas(c, p, rate, status="OK", ceiling=None):
        return dict(cores=c, **{"pass": p}, recordsPerSec=rate, status=status, ceiling=ceiling)
    gc = "garbage collection used 9.2% of this case's time and the limit is 5.5%. The pipeline ran short"
    run50 = [pas(1, "p1-asc", 28012, "CEILING", gc), pas(2, "p1-asc", 54514), pas(4, "p1-asc", 102706),
             pas(4, "p2-desc", 106728), pas(2, "p2-desc", 55378), pas(1, "p2-desc", 27249, "CEILING", gc),
             pas(1, "p3-asc", 28874, "CEILING", gc), pas(2, "p3-asc", 58802), pas(4, "p3-asc", 106517),
             pas(1, "sentinel", 29292, "CEILING", gc)]
    expect("claim: run 50's thrown-out baseline leaves 1->2 missing (must not fire)",
           gaps(run50, [1, 2, 4], ["1->2"]), "", should_fire=False)
    expect("claim: every case counted, nothing missing (must not fire)",
           gaps([dict(r, status="OK") for r in run50], [1, 2, 4], []), "", should_fire=False)
    def why50():
        table = L.build_table(run50, [1, 2, 4])
        g = L.missing_steps([1, 2, 4], table, run50)[0]["why"]
        assert "every 1-core pass was thrown out (4 of 4)" in g and "garbage collection used 9.2%" in g, g
    expect("claim: the missing step says why, in the harness's own words (must not fire)",
           why50, "", should_fire=False)
    def diagram():
        # issue #76, drawn the way a person plans a job: one box per step, input
        # topics with partitions, keyBy with the key set and its size, a dotted
        # broadcast, outputs with key counts and the interval on the arrow in.
        # Shapes from clean-room runs 49 (DataStream) and 51 (Flink SQL).
        plan = {"nodes": [
            {"id": "a", "description": "positions-by-symbol+market-value-by-symbol<br/>:- sink-positions-by-symbol: Writer<br/>:  +- sink-positions-by-symbol: Committer<br/>+- sink-market-values-by-symbol: Writer<br/>   +- sink-market-values-by-symbol: Committer<br/>",
             "inputs": [{"id": "s", "ship_strategy": "HASH"}, {"id": "p", "ship_strategy": "BROADCAST"}]},
            {"id": "s", "description": "Source: orders-source<br/>+- parse<br/>", "inputs": []},
            {"id": "p", "description": "Source: prices-source<br/>+- parse-prices<br/>", "inputs": []}]}
        ctx = {"inputs": [{"topic": "orders", "partitions": 8, "source": "orders-source"}, {"topic": "prices"}],
               "outputs": [{"topic": "positions-by-symbol", "keys": 4096},
                           {"topic": "market-values-by-symbol", "every": "10 s"},
                           {"topic": "nobody-writes-this"}],
               "keyed": {"positions-by-symbol": 4096}}
        g = L.graph_mermaid(plan, ctx)
        for want in ('in0(["orders<br/>8 partitions"])', "in0 --> v1o0", '("parse")',
                     "-- keyBy positions-by-symbol, 4,096 keys -->", "-. broadcast .->",
                     'out0(["positions-by-symbol<br/>4,096 keys"])', "v0o0 --> out0",
                     'out1(["market-values-by-symbol<br/>4,096 keys"])', "v0o0 -- every 10 s --> out1"):
            assert want in g, f"missing {want!r} in:\n{g}"
        assert "Writer" not in g and "Committer" not in g, "sink plumbing was drawn"
        assert "--> out2" not in g, "a topic no step names was connected by guess"
        # SQL names are shortened the same way for every job
        for raw, want in (("[38]:TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[a])", "read orders"),
                          ("[52]:WindowAggregate(groupBy=[symbol], window=[TUMBLE(time_col=[pt], size=[10 s])], select=[x])",
                           "window by symbol, every 10 s"),
                          ("[62]:Join(joinType=[InnerJoin], where=[(symbol = symbol0)], select=[x])", "join on symbol"),
                          ("[59]:Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=1], partitionBy=[symbol], orderBy=[ts DESC], select=[x])", "latest by symbol"),
                          ("[39]:Calc(select=[symbol, qty])", None)):
            got = L._op_name(raw)
            assert got == want, f"{raw[:40]} read as {got!r}, expected {want!r}"
    expect("diagram: steps, keys, interval and partitions from the configuration (must not fire)",
           diagram, "", should_fire=False)
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
    expect("source: no vertex matching sourceVertexMatch names the setting and the vertices",
           case(sourceIdle=None, backpressure={"orders[38] -> Calc[39]": {"idle": 0.1}}),
           "sourceVertexMatch")
    # the vertex name comes from whatever config is loaded: CI's example says
    # 'kafka-source', a local one may say 'Source' (#99 hardcoded the latter)
    expect("source: a matching vertex with no reading keeps the plain message",
           case(sourceIdle=None, backpressure={f"{L.cfg().source_match}[38] -> Calc[39]": {}}),
           "no way to tell whether")
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
            # The replay's own lines go into the message: on 2026-10-02 this
            # failed once in 18 runs saying only "disagreed", and the output that
            # named the record was not kept, so the cause could not be found.
            import io, contextlib
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cmd_replay()
            if rc != 0:
                said = [l.strip() for l in buf.getvalue().splitlines()
                        if "DISAGREE" in l or "STOPPED" in l]
                raise Exception("the replay disagreed with the record while --quick was set: "
                                + " | ".join(said[:6] or buf.getvalue().splitlines()[-6:]))
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
            raise Exception(f"an 8% drift (inside the 10% noise floor) was thrown out: {t['sentinel']}")
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

    def sql_keys_not_judged():
        # the uneven 5/3/4/4 layout that stops a DataStream build above: for SQL
        # the check does not describe the job, so it reports and never stops
        note = L.key_spread_skipped(L.api_kind({"api": "sql"}))
        if not note or "not checked" not in note:
            raise Exception(f"a SQL build got {note!r} instead of a not-checked note")
        if L.key_spread_skipped(L.api_kind({})) is not None:
            raise Exception("a config with no api field was treated as SQL")
        dstream = L.api_kind({"apiLevel": "Flink DataStream API, hand-written operators (no SQL, no Table API)"})
        if L.key_spread_skipped(dstream) is not None:
            raise Exception("a DataStream config that says 'no SQL' was treated as SQL")
    expect("key spread: a SQL build is reported as not checked, never stopped (must not fire)",
           sql_keys_not_judged, "", should_fire=False)
    expect("api names something other than datastream or sql",
           lambda: L.api_kind({"api": "table"}), "must be")

    # ---- platforms: a managed service is asked, Docker never is
    class FakeService(L.P.Platform):
        kind, unit = "fake", "CFU"
        def __init__(self, raw, under=0):
            self.calls, self.size, self.under = [], 0, under
        def up(self): self.calls.append("up")
        def down(self): self.calls.append("down")
        def set_size(self, units): self.calls.append(f"size {units}"); self.size = units - self.under
        def read_size(self): return self.size
        def clear_size(self): self.calls.append("clear"); self.size = 0
        def cpu_stat(self, comp): self.calls.append(f"cpu {comp}"); return dict(usage_usec=1, nr_periods=1, nr_throttled=0)
        def mem_stat(self, comp): self.calls.append(f"mem {comp}"); return dict(limitHits=0, refaults=0, fileCache=0, limitBytes=1)
        def submit(self, par, group, ckpt_ms=None): self.calls.append(f"submit {par}"); return "job-1"
        def surviving(self): self.calls.append("surviving"); return []

    def on_fake_service(body, under=0):
        import copy
        raw = copy.deepcopy(c.raw)
        raw["platform"] = {"kind": "fake"}
        tmp = tempfile.mkdtemp(prefix="platform-selftest-")
        saved_cfg, saved_sh = L._CFG, L.sh
        L.P.register("fake", lambda r: FakeService(r, under))
        def no_docker(cmd, *a, **k):
            raise Exception(f"Docker or the host was called on a managed service: {cmd[:80]!r}")
        try:
            path = os.path.join(tmp, "pipeline.json")
            with open(path, "w") as f:
                json.dump(raw, f)
            L._CFG = L.Cfg(path)
            L.sh = no_docker
            return body(L._CFG)
        finally:
            L._CFG, L.sh = saved_cfg, saved_sh
            L.P.unregister("fake")
            shutil.rmtree(tmp, ignore_errors=True)

    def every_operation_goes_to_the_service():
        def body(fc):
            L.stack_up(); L.start_tm(2); L.assert_cap(fc.tm, 2)
            L.cgroup_cpu(fc.tm); L.cgroup_cpu(fc.kafka); L.cgroup_mem(fc.kafka)
            L.submit_job(2, "g"); L.stop_tm(); L.surviving(); L.stack_down()
            want = ["up", "size 2", "cpu engine", "cpu broker", "mem broker", "submit 2", "clear",
                    "surviving", "down"]
            if fc.plat.calls != want:
                raise Exception(f"the service was asked {fc.plat.calls}, expected {want}")
        on_fake_service(body)
    expect("platform: a managed service is asked for every stack step, Docker never (must not fire)",
           every_operation_goes_to_the_service, "", should_fire=False)
    expect("platform: a size the service did not apply stops the case",
           lambda: on_fake_service(lambda fc: L.start_tm(4), under=1), "did not apply")
    expect("platform: a service that is named but not built stops before anything is paid for",
           lambda: L.P.platform_for(L.P.platform_kind({"platform": "aws"})),
           "cannot run on it yet")
    # Confluent Cloud, against a fake Confluent CLI: the stack's life, the
    # budget guard, the credentials file, read-back, and a teardown that
    # proves nothing survives. No cloud account is touched.
    import platform_confluent as PC

    class FakeConfluent:
        def __init__(self, ignore_update=False, fail=(), cold=0):
            self.cold = cold
            self.clusters, self.pools, self.keys, self.calls = {}, {}, {}, []
            self.envs = {"env-test": "test-env", "env-dflt": "default"}
            self.statements = {}
            self.written = {}          # topic -> rows written by INSERT ... VALUES
            self.spent = 0.0           # what the billing cost list reports, before the promo credit
            self.partitions = {}       # topic -> partition count, for the partition guard
            self.ignore_update, self.fail, self.n = ignore_update, set(fail), 0

        def docker(self, args):
            """The Kafka tools, as kafka-get-offsets.sh answers."""
            t = args[args.index("--topic") + 1]
            parts = self.partitions.get(t, 1)
            rows = [f"{t}:{i}:{self.written.get(t, 0) if i == 0 else 0}" for i in range(parts)]
            return 0, "\n".join(rows) + "\n", ""

        def rest(self, method, url, auth, body=None):
            """The Flink REST API for one statement: GET, the baseline PATCH, and its results."""
            if "/results" in url:
                # As measured 2026-10-06: 409 "not ready" first, then pages, each
                # with a link to the next until the statement has given everything.
                name = url.split("/statements/")[1].split("/")[0]
                if getattr(self, "results_409", 0) > 0:
                    self.results_409 -= 1
                    return 409, {"errors": [{"status": "409", "detail": f"Results for Statement={name} not ready"}]}
                pages = getattr(self, "pages", {}).get(name) or [[{"op": 0, "row": r}
                                                                  for r in getattr(self, "results", {}).get(name, [])]]
                i = int(url.rsplit("page=", 1)[1]) if "page=" in url else 0
                nxt = f"{url.split('?')[0]}?page={i + 1}" if i + 1 < len(pages) else ""
                return 200, {"results": {"data": pages[i]}, "metadata": {"next": nxt}}
            name = url.rsplit("/", 1)[1]
            if name in getattr(self, "schemas", {}):
                # Measured 2026-10-06: the last page (no next link) came while the
                # statement still said RUNNING, for two more reads.
                phase = "COMPLETED"
                if getattr(self, "running_reads", 0) > 0:
                    self.running_reads -= 1
                    phase = "RUNNING"
                return 200, {"name": name, "status": {"phase": phase,
                                                      "traits": {"schema": {"columns": [{"name": c} for c in
                                                                                        self.schemas[name]]}}}}
            st = self.statements.get(name)
            if st is None:
                return 404, {"errors": [{"detail": f"statement {name} not found"}]}
            if method == "PATCH" and "baseline" not in self.fail:
                st["scaling"] = body[0]["value"]
            return 200, {"name": name, "spec": {"scaling": st.get("scaling"), "properties": st.get("properties")}}

        def __call__(self, args):
            a = [x for i, x in enumerate(args) if not (x in ("--environment", "-o") or
                 (i and args[i - 1] in ("--environment", "-o")))]
            self.calls.append(" ".join(a[:3]))
            ok = lambda obj=None: (0, json.dumps(obj) if obj is not None else "", "")
            def flag(name, default=None):
                return a[a.index(name) + 1] if name in a else default
            self.n += 1
            if a[:3] == ["billing", "cost", "list"]:
                if "--environment" in args:
                    return 1, "", "Error: unknown flag: --environment"
                day = {"start_date": self.posted_through} if getattr(self, "posted_through", None) else {}
                return ok([dict(day, product="FLINK", line_type="FLINK_NUM_CFUS", amount=f"${self.spent:.2f}"),
                           dict(day, product=None, line_type="PROMO_CREDIT", amount=f"$-{self.spent:.2f}")])
            if a[:3] == ["billing", "promo", "list"]:
                if "--environment" in args:
                    return 1, "", "Error: unknown flag: --environment"
                return ok(getattr(self, "promos", []))
            if a[:2] == ["organization", "list"]:
                if "--environment" in args:         # as the real CLI answers (2026-10-04)
                    return 1, "", "Error: unknown flag: --environment"
                return ok([{"id": "org-test", "name": "Test", "is_current": True}])
            if a[:2] == ["environment", "list"]:
                return ok([{"id": i, "name": n} for i, n in self.envs.items()])
            if a[:2] == ["environment", "create"]:
                eid = f"env-new{self.n}"; self.envs[eid] = a[2]; return ok({"id": eid, "name": a[2]})
            if a[:2] == ["environment", "delete"]:
                self.envs.pop(a[2], None); return ok()
            if a[:3] == ["kafka", "cluster", "create"]:
                cid = f"lkc-{self.n}"; self.clusters[cid] = {"id": cid, "name": a[3], "status": "PROVISIONING",
                                                              "endpoint": "SASL_SSL://pkc-x.gcp.confluent.cloud:9092"}
                return ok(self.clusters[cid])
            if a[:3] == ["kafka", "cluster", "describe"]:
                self.clusters[a[3]]["status"] = "UP"; return ok(self.clusters[a[3]])
            if a[:3] == ["kafka", "cluster", "list"]:
                return ok(list(self.clusters.values()))
            if a[:3] == ["kafka", "cluster", "delete"]:
                if "cluster" in self.fail: return 1, "", "the cluster could not be deleted"
                self.clusters.pop(a[3], None); return ok()
            if a[:3] == ["flink", "compute-pool", "create"] and int(flag("--max-cfu", 5)) not in PC.POOL_SIZES:
                return 1, "", "Error: Bad Request: Violations [MaxCfu is not one of 5, 10, 20, 30, 40, 50]"
            if a[:3] == ["flink", "compute-pool", "create"]:
                pid = f"lfcp-{self.n}"; self.pools[pid] = {"id": pid, "name": a[3], "status": "PROVISIONING",
                                                            "max_cfu": 5 if self.ignore_update else int(flag("--max-cfu", 5)),
                                                            "current_cfu": 0}
                return ok(self.pools[pid])
            if a[:3] == ["flink", "compute-pool", "describe"]:
                self.pools[a[3]]["status"] = "PROVISIONED"; return ok(self.pools[a[3]])
            if a[:3] == ["flink", "compute-pool", "update"]:
                if int(flag("--max-cfu")) < self.pools[a[3]]["max_cfu"]:     # as answered on 2026-10-03
                    return 1, "", "Error: Reducing the max_cfu of a compute pool is currently unsupported."
                if not self.ignore_update: self.pools[a[3]]["max_cfu"] = int(flag("--max-cfu"))
                return ok()
            if a[:3] == ["flink", "compute-pool", "list"]:
                return ok(list(self.pools.values()))
            if a[:3] == ["flink", "compute-pool", "delete"]:
                if "pool" in self.fail: return 1, "", "the pool could not be deleted"
                self.pools.pop(a[3], None); return ok()
            if a[:2] == ["api-key", "create"]:
                if "key" in self.fail: return 1, f"SECRET-leak-{self.n}", "boom"
                k = f"KEY{self.n}"; self.keys[k] = {"key": k, "description": flag("--description")}
                return ok({"api_key": k, "api_secret": f"SECRET-{self.n}"})
            if a[:2] == ["api-key", "list"]:
                return ok(list(self.keys.values()))
            if a[:2] == ["api-key", "delete"]:
                if "--environment" in args:         # as the real CLI answers (2026-10-03)
                    return 1, "", "Error: unknown flag: --environment"
                self.keys.pop(a[2], None); return ok()
            if a[:3] == ["flink", "statement", "create"]:
                if a[3] in self.statements:
                    return 1, "", f'Error: Statement with name "{a[3]}" already exists.'
                props = dict(a[i + 1].split("=", 1) for i, x in enumerate(a) if x == "--property")
                self.statements[a[3]] = {"name": a[3], "status": "PENDING", "sql": flag("--sql"),
                                         "pool": flag("--compute-pool"),
                                         "properties": {} if "props" in self.fail else props}
                return ok()
            if a[:3] == ["flink", "statement", "describe"]:
                if a[3] not in self.statements:      # as the real CLI answers (2026-10-04)
                    return 1, "", f"Error: Statement resource={a[3]} does not exist"
                st = self.statements[a[3]]
                if st["status"] == "PENDING":
                    self.ran = getattr(self, "ran", 0) + 1
                    if self.ran <= self.cold:          # a new stack, as measured: refuses at first
                        st["status"], st["status_detail"] = "FAILED", (
                            "Unable to process the request due to technical difficulties on our end.")
                        return ok(st)
                    if self.ran <= getattr(self, "schema_cold", 0):   # as answered on 2026-10-06
                        st["status"], st["status_detail"] = "FAILED", (
                            "failed registering schemas: unable to register schema on 'x-value': "
                            "read: connection reset by peer")
                        return ok(st)
                    st["status"] = "FAILED" if ("statement" in self.fail and "ready" not in st["name"]) else (
                        "COMPLETED" if st["sql"].upper().startswith("CREATE") else "RUNNING")
                    if st["status"] == "FAILED":
                        st["status_detail"] = "Table 'why_t' could not be created: the environment is not ready"
                    if ("late" in self.fail and st["sql"].upper().startswith("INSERT")
                            and getattr(self, "late_left", 0) > 0):
                        self.late_left -= 1          # as seen 2026-10-03, right after CREATE TABLE
                        st["status"], st["status_detail"] = "FAILED", "Cannot find table 'orders' in 'db'."
                    m = re.match(r"INSERT INTO (\w+) VALUES", st["sql"] or "")
                    if m and st["status"] == "COMPLETED" or (m and st["status"] == "RUNNING"):
                        st["status"] = "COMPLETED"
                        self.written[m.group(1)] = self.written.get(m.group(1), 0) + 1
                return ok(st)
            if a[:3] == ["flink", "statement", "list"]:
                pool = flag("--compute-pool")
                return ok([v for v in self.statements.values() if pool in (None, v.get("pool"))])
            if a[:3] == ["flink", "statement", "delete"]:
                names = [n for i, n in enumerate(a[3:], 3)
                         if not n.startswith("--") and not a[i - 1].startswith("--")]
                for n in names:
                    if n not in self.statements or n in getattr(self, "ghosts", set()):
                        self.ghosts.discard(n) if hasattr(self, "ghosts") else None
                        self.statements.pop(n, None)
                        return 1, "", f'Error: Flink SQL statement "{n}" not found'
                    self.statements.pop(n, None)
                return ok()
            return 1, "", f"the fake does not know {' '.join(a)}"

    def on_confluent(fn, **fake_kw):
        def go():
            with tempfile.TemporaryDirectory() as tdir:
                fake = FakeConfluent(**fake_kw); said = []
                raw = {"environment": "env-test", "prefix": "fsk-t", "stateDir": tdir,
                       "credentials": os.path.join(tdir, "creds", "flink-skill.env"),
                       "_docker": fake.docker, "_rest": fake.rest}
                mk = lambda extra=None: PC.ConfluentCloud(dict(raw, **(extra or {})), runner=fake, log=said.append)
                PC.ConfluentCloud.poll_s = 0
                PC.ConfluentCloud.ready_wait_s = 0
                try:
                    fn(fake, mk, said, raw)
                finally:
                    PC.ConfluentCloud.poll_s = 10
                    PC.ConfluentCloud.ready_wait_s = 60
        return go

    def cc_life(fake, mk, said, raw):
        p = mk(); p.up()
        creds = raw["credentials"]
        assert oct(os.stat(creds).st_mode & 0o777) == "0o600", oct(os.stat(creds).st_mode)
        body = open(creds).read()
        for k in ("KAFKA_API_KEY", "KAFKA_API_SECRET", "FLINK_API_KEY", "FLINK_API_SECRET", "METRICS_API_KEY",
                  "METRICS_API_SECRET", "KAFKA_BOOTSTRAP", "FLINK_COMPUTE_POOL"):
            assert k + "=" in body, k
        assert "SECRET-" in body
        state = open(os.path.join(raw["stateDir"], "confluent-state.json")).read()
        assert "SECRET" not in state and "SECRET" not in " ".join(said), "a secret reached the state or the log"
        assert L.P.set_and_read_back(p, 10) == 10
        p.down()
        assert p.surviving() == [] and not os.path.exists(creds), (p.surviving(), os.path.exists(creds))
    expect("confluent: up writes owner-only credentials and no secret anywhere else; down leaves nothing "
           "(must not fire)", on_confluent(cc_life), "", should_fire=False)

    expect("confluent: a size the pool did not take stops the case",
           on_confluent(lambda fake, mk, said, raw: (lambda p: (p.up(), L.P.set_and_read_back(p, 10)))(mk()),
                        ignore_update=True), "the case is 10, the platform reports 5")
    expect("confluent: a size Confluent does not allow is named, with the sizes it does",
           on_confluent(lambda fake, mk, said, raw: (lambda p: (p.up(), p.set_size(4)))(mk())),
           "cannot be 4 CFU: its size can only be 5, 10, 20, 30, 40, 50")

    def cc_over_budget(fake, mk, said, raw):
        try:
            mk({"estimateUsd": 400}).up()
        finally:
            assert fake.calls == ["billing cost list"], f"something was created over budget: {fake.calls}"
    expect("confluent: a run estimated over budget stops before anything is created", on_confluent(cc_over_budget),
           "would be over the $350.00 budget")

    def cc_spent_counts(fake, mk, said, raw):
        fake.spent = 137.77          # as billed by 2026-10-05, before the promo credit
        try:
            mk({"estimateUsd": 220}).up()
        finally:
            assert fake.calls == ["billing cost list"], f"something was created over budget: {fake.calls}"
    expect("confluent: what has already been charged counts against the budget, not only this run's estimate",
           on_confluent(cc_spent_counts), "$137.77 has been charged since")

    def cc_within_budget(fake, mk, said, raw):
        fake.spent = 137.77
        p = mk({"estimateUsd": 12}); p.up()
        assert any("charged since" in l and "$137.77" in l for l in said), said
        p.down()
    expect("confluent: a run that fits in what is left of the budget goes ahead, saying what is left "
           "(must not fire)", on_confluent(cc_within_budget), "", should_fire=False)

    # 2026-10-06 replayed: the cost list said $170.89 through 2026-10-05 while the
    # FREETRIAL400 credit had $17.98 left -- $382.02 used. A $60 run must stop.
    def cc_promo_counts(fake, mk, said, raw):
        fake.spent, fake.posted_through = 170.89, "2026-10-05"
        fake.promos = [{"code": "FREETRIAL400", "balance": 17.9763, "expiration": 1793613600}]
        try:
            mk({"estimateUsd": 60, "promoUsd": 400}).up()
        finally:
            assert "kafka cluster" not in " ".join(fake.calls), f"something was created over budget: {fake.calls}"
    expect("confluent: the promo credit's balance counts when the cost list is a day behind",
           on_confluent(cc_promo_counts), "the promo credit says $382.02 has been used")

    def cc_unposted_runs_count(fake, mk, said, raw):
        fake.spent, fake.posted_through = 170.89, "2026-10-05"
        today = time.strftime("%Y-%m-%d", time.gmtime())
        os.makedirs(os.path.dirname(raw["credentials"]), exist_ok=True)
        with open(os.path.join(os.path.dirname(raw["credentials"]), "flink-skill-spend.json"), "w") as f:
            json.dump([{"day": "2026-10-05", "estimateUsd": 60}] + [{"day": today, "estimateUsd": 60}] * 2, f)
        try:
            mk({"estimateUsd": 60}).up()
        finally:
            assert "kafka cluster" not in " ".join(fake.calls), f"something was created over budget: {fake.calls}"
    expect("confluent: runs the cost list does not show yet count, and a posted day is not counted twice",
           on_confluent(cc_unposted_runs_count), "plus $120.00 estimated for 2 runs")

    def cc_spend_logged(fake, mk, said, raw):
        p = mk({"estimateUsd": 12}); p.up(); p.down()
        entries = json.load(open(os.path.join(os.path.dirname(raw["credentials"]), "flink-skill-spend.json")))
        assert [e["estimateUsd"] for e in entries] == [12.0], entries
    expect("confluent: every run that goes ahead is written to the spend log (must not fire)",
           on_confluent(cc_spend_logged), "", should_fire=False)

    def cc_survivor(fake, mk, said, raw):
        p = mk(); p.up(); p.down()
    expect("confluent: a pool that will not delete is named as still costing money",
           on_confluent(cc_survivor, fail={"pool"}), "Flink compute pool lfcp-")

    def cc_crash_then_clean(fake, mk, said, raw):
        try:
            mk().up()
        except Refusal as e:
            assert "SECRET" not in e.msg and "withheld" in e.msg, e.msg
        assert fake.clusters and fake.pools, "the failed up created nothing to clean"
        mk().down()                       # a new process: only the state file knows what exists
        assert not fake.clusters and not fake.pools, (fake.clusters, fake.pools)
    expect("confluent: after a failed up, down from a new process removes what was created (must not fire)",
           on_confluent(cc_crash_then_clean, fail={"key"}), "", should_fire=False)

    def cc_named(fake, mk, said, raw):
        # Assets named after the project, not left in "default" (2026-10-03).
        p = mk({"environment": "flink-training", "prefix": None}); p.up()
        new = [i for i, n in fake.envs.items() if n == "flink-training"]
        assert len(new) == 1, fake.envs
        assert all(c["name"].startswith("flink-training-") for c in fake.clusters.values()), fake.clusters
        assert all(v["name"].startswith("flink-training-") for v in fake.pools.values()), fake.pools
        p.down()
        assert "flink-training" not in fake.envs.values(), "the environment up created survived down"
        q = mk({"environment": "default", "prefix": "fsk-t"}); q.up(); q.down()
        assert "default" in fake.envs.values(), "down deleted an environment it did not create"
    expect("confluent: a named environment is created and later deleted; an existing one is only used "
           "(must not fire)", on_confluent(cc_named), "", should_fire=False)

    def cc_statements(fake, mk, said, raw):
        p = mk(); p.up()
        assert p.run_statement("fsk-t-ddl", "CREATE TABLE t (a INT)", "db") == "COMPLETED"
        assert p.run_statement("fsk-t-copy", "INSERT INTO t SELECT 1", "db") == "RUNNING"
        p.down()
        assert not fake.statements, f"teardown left statements: {list(fake.statements)}"
    expect("confluent: a statement is judged on the status read back, and teardown removes it (must not fire)",
           on_confluent(cc_statements), "", should_fire=False)

    def cc_statement_fails(fake, mk, said, raw):
        p = mk(); p.up()
        try:
            p.run_statement("fsk-t-ddl", "CREATE TABLE t (a INT)", "db")
        finally:
            p.down()
    expect("confluent: a statement Confluent reports as failed stops with Confluent's own reason",
           on_confluent(cc_statement_fails, fail={"statement"}),
           "did not run on Confluent Cloud (failed): Table 'why_t' could not be created")

    def cc_cold_stack(fake, mk, said, raw):
        p = mk(); p.up()
        assert any("ran its first statement" in l for l in said), said
        assert sum(1 for n in fake.statements if "ready" in n and "write" not in n) == 3, list(fake.statements)
        p.down()
    expect("confluent: up waits until a new stack runs a statement, as measured (must not fire)",
           on_confluent(cc_cold_stack, cold=2), "", should_fire=False)

    def cc_schema_registry_late(fake, mk, said, raw):
        fake.schema_cold = 2
        p = mk(); p.up()
        assert any("read back from Kafka" in l for l in said), said
        p.down()
    expect("confluent: a schema registry not reachable yet on a new stack is waited for, as measured "
           "(must not fire)", on_confluent(cc_schema_registry_late), "", should_fire=False)

    def cc_never_ready(fake, mk, said, raw):
        p = mk()
        p.ready_tries = 3
        try:
            p.up()
        finally:
            p.down()
    expect("confluent: a stack that never runs a statement stops, quoting Confluent",
           on_confluent(cc_never_ready, cold=99), "never ran a statement in 3 tries")

    def cc_pool_per_case(fake, mk, said, raw):
        p = mk(); p.up()
        setup = p.state["pool"]
        assert fake.pools[setup]["max_cfu"] == 20, fake.pools[setup]
        sizes = []
        for units in (5, 10, 20, 5):              # ascending, then the sentinel back at the baseline
            sizes.append(L.P.set_and_read_back(p, units))
            p.run_statement("fsk-t-job", "INSERT INTO t SELECT 1", "db")
            assert fake.statements[p.last_statement]["pool"] == p.state["casePool"], "the job ran outside its pool"
        assert sizes == [5, 10, 20, 5], sizes
        assert set(fake.pools) == {setup, p.state["casePool"]}, f"earlier case pools survived: {fake.pools}"
        p.down()
        assert not fake.pools and not fake.statements, (fake.pools, fake.statements)
    expect("confluent: every case gets a new pool of its own size, so a suite can come back down to its "
           "baseline; earlier pools are deleted (must not fire)", on_confluent(cc_pool_per_case), "",
           should_fire=False)

    def cc_late_table(fake, mk, said, raw):
        p = mk(); p.up(); p.retry_wait_s = 0
        fake.late_left = 2
        assert p.run_statement("fsk-t-job", "INSERT INTO orders SELECT 1", "db") == "RUNNING"
        assert p.last_statement == "fsk-t-job-r3", p.last_statement
        assert "fsk-t-job" not in fake.statements and "fsk-t-job-r2" not in fake.statements, list(fake.statements)
        assert any("could not see a table" in l for l in said), said
        p.down()
    expect("confluent: a statement that cannot see a table created just before it is retried under a new "
           "name, as measured (must not fire)", on_confluent(cc_late_table, fail={"late"}), "",
           should_fire=False)

    def cc_late_table_gives_up(fake, mk, said, raw):
        p = mk(); p.up(); p.retry_wait_s = 0
        fake.late_left = 99
        try:
            p.run_statement("fsk-t-job", "INSERT INTO orders SELECT 1", "db")
        finally:
            p.down()
    expect("confluent: a table that never becomes visible stops, quoting Confluent",
           on_confluent(cc_late_table_gives_up, fail={"late"}), "Cannot find table 'orders'")

    def cc_ghost_statement(fake, mk, said, raw):
        # As on 2026-10-04: the list still names a statement already deleted.
        p = mk(); p.up()
        L.P.set_and_read_back(p, 20)
        p.run_statement("fsk-t-job", "INSERT INTO t SELECT 1", "db")
        p.run_statement("fsk-t-ddl", "CREATE TABLE t (a INT)", "db")
        fake.ghosts = {"fsk-t-job"}
        L.P.set_and_read_back(p, 5)               # deletes the last case's pool and its statements
        p.down()
        assert not fake.pools and not fake.statements, (fake.pools, fake.statements)
    expect("confluent: a statement already deleted but still listed does not stop the teardown, and no "
           "pool is left behind (must not fire)", on_confluent(cc_ghost_statement), "", should_fire=False)

    def cc_readings(fake, mk, said, raw):
        seen = {}
        def docker(args):
            if args[args.index("--topic") + 1] != "orders":
                return fake.docker(args)         # the readiness row
            seen["args"] = list(args)
            mount = args[args.index("-v") + 1].split(":")[0]
            client = os.path.join(mount, "client.properties")
            seen["mode"] = oct(os.stat(client).st_mode & 0o777)
            seen["client"] = open(client).read()
            seen["path"] = client
            return 0, "orders:0:100\norders:1:250\nother:0:7\n", ""
        def http(url, auth, body):
            seen.setdefault("bodies", []).append(body)
            return 200, {"data": [{"timestamp": "2026-10-03T00:55:00Z", "value": "14597694"},
                                  {"timestamp": "2026-10-03T00:54:00Z", "value": 11003843.0}]}
        p = mk({"_docker": docker, "_http": http}); p.up()
        assert p.log_end("orders") == (350, {0: 100, 1: 250}), p.log_end("orders")
        assert seen["mode"] == "0o600", seen["mode"]
        assert "SECRET-" in seen["client"] and not any("SECRET" in a for a in seen["args"]), \
            "a secret went on the docker command line"
        assert not os.path.exists(seen["path"]), "the Kafka client file outlived the call"
        m = p.statement_minutes("fsk-t-job", 0, 600)
        assert set(m) == set(PC.STATEMENT_METRICS), sorted(m)
        assert m["recordsIn"] == [("2026-10-03T00:54:00Z", 11003843.0), ("2026-10-03T00:55:00Z", 14597694.0)], m
        flt = seen["bodies"][0]["filter"]["filters"]
        assert {"field": "resource.flink_statement.name", "op": "EQ", "value": "fsk-t-job"} in flt, flt
        assert not any("SECRET" in l for l in said), "a secret reached the log"
        p.down()
    expect("confluent: the log end and the per-minute readings come back parsed, with no secret on a "
           "command line or in the log (must not fire)", on_confluent(cc_readings), "", should_fire=False)

    def cc_metrics_refused(fake, mk, said, raw):
        p = mk({"_http": lambda url, auth, body: (401, {"errors": [{"detail": "invalid API key"}]})}); p.up()
        try:
            p.statement_minutes("fsk-t-job", 0, 600)
        finally:
            p.down()
    expect("confluent: a metrics API that will not answer stops with its own reason",
           on_confluent(cc_metrics_refused), "answered HTTP 401")

    def cc_ready_round_trip(fake, mk, said, raw):
        p = mk(); p.up()
        assert any("read back from Kafka" in l for l in said), said
        assert fake.written.get("fsk_t_ready0") == 1, fake.written
        p.down()
    expect("confluent: up proves a new table can be written and read back before the stack counts as ready "
           "(must not fire)", on_confluent(cc_ready_round_trip), "", should_fire=False)

    def cc_ready_table_invisible(fake, mk, said, raw):
        # As measured 2026-10-04: a stack ready in 5-7 s whose new tables never became visible.
        fake.late_left = 99
        p = mk(); p.retry_wait_s = 0
        try:
            p.up()
        finally:
            assert not fake.clusters and not fake.pools and not fake.keys, \
                f"an unusable stack was left up: {fake.clusters} {fake.pools} {fake.keys}"
    expect("confluent: a stack that can create a table but not use it is torn down before anything is filled",
           on_confluent(cc_ready_table_invisible, fail={"late"}), "could create a table but not use it")

    def cc_start_job(fake, mk, said, raw):
        p = mk(); p.up()
        L.P.set_and_read_back(p, 20)
        nm = p.start_job("fsk-t-job", "INSERT INTO out SELECT * FROM orders", "db", 20)
        st = fake.statements[nm]
        assert st["properties"] == PC.ALIGNMENT_OFF, st["properties"]
        assert st["scaling"] == {"baseline_cfu": 20}, st.get("scaling")
        assert st["pool"] == p.state["casePool"], "the job ran outside its case's pool"
        p.down()
    expect("confluent: a case's job runs with watermark alignment off and its baseline at the pool's size, "
           "both read back (must not fire)", on_confluent(cc_start_job), "", should_fire=False)

    def cc_job_without(fake, mk, said, raw):
        p = mk(); p.up()
        L.P.set_and_read_back(p, 20)
        try:
            p.start_job("fsk-t-job", "INSERT INTO out SELECT * FROM orders", "db", 20)
        finally:
            p.down()
    expect("confluent: a job that does not report alignment off stops before it is measured",
           on_confluent(cc_job_without, fail={"props"}),
           "sql.tables.scan.watermark-alignment.max-allowed-drift should be '1 d'")
    expect("confluent: a baseline the statement does not report stops before the case is measured",
           on_confluent(cc_job_without, fail={"baseline"}), "the baseline of 20 CFU did not apply")

    def cc_wait_at_size(fake, mk, said, raw):
        calls = {"n": 0}
        def http(url, auth, body):
            metric = body["aggregations"][0]["metric"]
            if metric.endswith("current_cfus"):
                calls["n"] += 1
                rows = [{"timestamp": f"2026-10-04T00:0{i}:00Z", "value": v}
                        for i, v in enumerate([1, 1, 9, 20][:min(4, calls["n"] + 1)])]
                return 200, {"data": rows}
            return 200, {"data": []}
        p = mk({"_http": http}); p.up()
        assert p.wait_at_size("fsk-t-job", 20, 0) == "2026-10-04T00:03:00Z"
        assert calls["n"] == 3, calls
        p.down()
    expect("confluent: the window waits until the statement uses its whole pool (must not fire)",
           on_confluent(cc_wait_at_size), "", should_fire=False)

    def cc_new_pool_not_authorized(fake, mk, said, raw):
        calls = {"n": 0}
        def http(url, auth, body):
            calls["n"] += 1
            if calls["n"] <= 3:          # as answered for a pool a minute old (2026-10-05)
                return 403, {"errors": [{"status": "403", "detail": "Query must filter by at least one of your "
                                                                     "authorized resources"}]}
            m = body["aggregations"][0]["metric"]
            return 200, {"data": [{"timestamp": "2026-10-05T04:10:00Z", "value": 20}] if m.endswith("current_cfus") else []}
        p = mk({"_http": http}); p.up()
        assert p.wait_at_size("fsk-t-job", 20, 0) == "2026-10-05T04:10:00Z"
        assert any("does not cover pool" in l for l in said), said
        p.down()
    expect("confluent: a new pool the metrics API does not cover yet is waited for, not a stop (must not fire)",
           on_confluent(cc_new_pool_not_authorized), "", should_fire=False)

    def cc_never_at_size(fake, mk, said, raw):
        def http(url, auth, body):
            m = body["aggregations"][0]["metric"]
            return 200, {"data": [{"timestamp": "2026-10-04T00:09:00Z", "value": 10}] if m.endswith("current_cfus") else []}
        p = mk({"_http": http}); p.up(); p.at_size_wait_s = 0
        try:
            p.wait_at_size("fsk-t-job", 20, 0)
        finally:
            p.down()
    expect("confluent: a statement that never uses its whole pool stops the case, with what it did use",
           on_confluent(cc_never_at_size), "never used its 20 CFU pool")

    def cc_partitions(fake, mk, said, raw):
        p = mk(); p.up()
        fake.partitions["orders40"] = 40
        assert p.check_partitions("orders40", [5, 10, 20]) == 40
        fake.partitions["orders24"] = 24
        try:
            p.check_partitions("orders24", [5, 10, 20])
        finally:
            p.down()
    expect("confluent: an input topic whose partitions do not divide by every case stops, naming sizes that do",
           on_confluent(cc_partitions), "do not divide evenly by 5, 10, 20 subtasks (1 CFU runs one subtask). "
           "Use a multiple of 20, for example 20, 40 or 60")
    expect("confluent: partitions that divide by every case are accepted (must not fire)",
           lambda: (PC.uneven_cases(40, [5, 10, 20]) == [] or (_ for _ in ()).throw(Exception("40 rejected"))),
           "", should_fire=False)

    def cfu_http(units):
        def http(url, auth, body):
            m = body["aggregations"][0]["metric"]
            return 200, {"data": [{"timestamp": "2026-10-05T05:16:00Z", "value": units}] if m.endswith("current_cfus") else []}
        return http

    def cc_submit(fake, mk, said, raw):
        fake.partitions["orders"] = 40
        p = mk({"jobSql": "INSERT INTO out SELECT * FROM orders", "_cases": [5, 10, 20], "_topicIn": "orders",
                "_http": cfu_http(20)}); p.up()
        L.P.set_and_read_back(p, 20)
        nm = p.submit(20, "g1")
        st = fake.statements[nm]
        assert st["properties"] == PC.ALIGNMENT_OFF and st["scaling"] == {"baseline_cfu": 20}, st
        assert p.partitions_checked == "orders", p.partitions_checked     # the topic it checked
        p.down()
    expect("confluent: submit starts every job with alignment off, the baseline at the pool's size and the "
           "partitions checked, and waits for the whole pool (must not fire)", on_confluent(cc_submit), "",
           should_fire=False)

    def cc_submit_bad_partitions(fake, mk, said, raw):
        fake.partitions["orders"] = 24
        p = mk({"jobSql": "INSERT INTO out SELECT * FROM orders", "_cases": [5, 10, 20], "_topicIn": "orders",
                "_http": cfu_http(20)}); p.up()
        L.P.set_and_read_back(p, 20)
        try:
            p.submit(20, "g1")
        finally:
            assert not any(n.startswith("fsk-t-job") for n in fake.statements), "a job started on an uneven input"
            p.down()
    expect("confluent: submit stops before starting a job when the input's partitions do not divide by every case",
           on_confluent(cc_submit_bad_partitions), "do not divide evenly by 5, 10, 20 subtasks")
    expect("confluent: submit with no job SQL stops and says what to set",
           on_confluent(lambda fake, mk, said, raw: (lambda p: (p.up(), p.submit(20, "g1")))(mk())),
           "platform.jobSql is not set")

    class FakeClock:
        """time for platform_confluent, moved only by sleep: a four-minute window in no time."""
        def __init__(self, t0=1791177300.0):
            self.t = t0
        def time(self):
            return self.t
        def sleep(self, s):
            self.t += max(0, s)
        def __getattr__(self, name):          # strptime, strftime, gmtime ... as the real module
            return getattr(time, name)

    def window_fake(fake, clock, out_per_s, read_per_min, cfu, pending=500_000_000, ecku=6):
        t_start = clock.t
        def docker(args):
            t = args[args.index("--topic") + 1]
            if t.startswith("out"):
                return 0, f"{t}:0:{int(out_per_s * (clock.t - t_start))}\n", ""
            return fake.docker(args)
        def http(url, auth, body):
            m = body["aggregations"][0]["metric"]
            iv = body["intervals"][0].split("/")
            a = calendar.timegm(time.strptime(iv[0][:19], "%Y-%m-%dT%H:%M:%S"))
            z = calendar.timegm(time.strptime(iv[1][:19], "%Y-%m-%dT%H:%M:%S"))
            val = {"current_cfus": cfu, "num_records_in": read_per_min, "pending_records": pending,
                   "busy_time_ms_per_second": 1000, "backpressure_time_ms_per_second": 0,
                   "idle_time_ms_per_second": 0, "elastic_cku_count": ecku}
            v = next((x for k, x in val.items() if m.endswith(k)), 0)
            rows = [{"timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t)), "value": v}
                    for t in range(a - a % 60, z, 60)]
            return 200, {"data": rows}
        return docker, http

    def cc_window(fake, mk, said, raw):
        clock = FakeClock()
        real = PC.time
        PC.time = clock
        try:
            docker, http = window_fake(fake, clock, out_per_s=2_000_000, read_per_min=60_000_000, cfu=20)
            p = mk({"_docker": docker, "_http": http}); p.poll_s = 0
            p.up()
            m = p.measure_window("fsk-t-job", 20, ["out_a", "out_b"], 4.0)
        finally:
            PC.time = real
        assert len(m["wholeMinutes"]) >= 3, m["wholeMinutes"]
        assert abs(m["recordsPerSec"] - 1_000_000) < 1, m["recordsPerSec"]       # 2 topics x 2M/s / 4 per input
        assert abs(m["recordsReadPerSec"] - 1_000_000) < 1, m["recordsReadPerSec"]
        assert m["cfuInUse"] == 20 and m["busy"] == 1.0 and m["heldBack"] == 0.0, m
        assert m["backlogRemaining"] == 500_000_000, m["backlogRemaining"]
        assert m["kafkaEcku"] == 6 and m["kafkaEckuLimit"] == 10 and m["kafkaEckuMinutesAtLimit"] == 0, m
    expect("confluent: a case's window reads the rate from the output topics' log end over whole minutes and "
           "lines Confluent's own readings up with it (must not fire)", on_confluent(cc_window), "",
           should_fire=False)

    good_cloud = {"readings": 9, "wholeMinutes": ["m1", "m2", "m3", "m4"], "recordsPerSec": 700_000.0,
                  "recordsReadPerSec": 702_000.0, "vantageDisagreement": 0.0029, "backlogRemaining": 200_000_000,
                  "tmCapFrac": 1.0, "tmCores": 20.0, "sourceIdle": 0.0, "sourceBackpressured": 0.0,
                  "kafkaEcku": 6.0, "kafkaEckuLimit": 10, "kafkaEckuMinutesAtLimit": 0}
    def cloud_case(**kw):
        r = dict(good_cloud, **kw)
        return lambda: L.check_case_cloud(r, 20, False)
    expect("cloud case: a valid record passes (must not fire)", cloud_case(), "", should_fire=False)
    expect("cloud case: the output count and Confluent's records read disagree",
           cloud_case(recordsReadPerSec=760_000.0, vantageDisagreement=0.0857), "the two ways of counting do not agree")
    expect("cloud case: the backlog ran out inside the window",
           cloud_case(backlogRemaining=10_000_000), "the data ran out before the measurement finished")
    expect("cloud case: a job that did not use its pool is a ceiling, kept and left out of the ratios",
           cloud_case(tmCapFrac=0.5, tmCores=10.0), "the job used 10.0 CFU of the 20 CFU pool", ceiling=True)
    expect("cloud case: a job waiting on its input is a ceiling", cloud_case(sourceIdle=0.3),
           "sat idle 30.0% of the window", ceiling=True)
    # Replayed against the record: runs 18 and 19 had the cluster at its 50 eCKU
    # limit in both cases and steps of 1.77x and 2.01x; the live harness case of
    # 2026-10-06 at 20 CFU read 857,092/s against 840,631 records read, 1.9% apart.
    for label, kw in (("run 18 at 10 CFU", dict(recordsPerSec=499_333.0, recordsReadPerSec=499_333.0,
                                                 vantageDisagreement=0.0, tmCapFrac=1.0, tmCores=10.0)),
                      ("run 19 at 20 CFU", dict(recordsPerSec=856_833.0, recordsReadPerSec=856_833.0,
                                                 vantageDisagreement=0.0, tmCapFrac=1.0, tmCores=20.0)),
                      ("the live case at 20 CFU", dict(recordsPerSec=857_091.6, recordsReadPerSec=840_631.5,
                                                       vantageDisagreement=0.0192, tmCapFrac=1.0, tmCores=20.0,
                                                       backlogRemaining=196_802_143))):
        expect(f"cloud case: a cluster at its eCKU limit does not decide a case on its own -- {label} "
               f"(must not fire)", cloud_case(kafkaEcku=50.0, kafkaEckuLimit=50, kafkaEckuMinutesAtLimit=4, **kw),
               "", should_fire=False)
    expect("cloud case: no eCKU reading is recorded as missing and does not decide the case (must not fire)",
           cloud_case(kafkaEcku=None), "", should_fire=False)
    expect("cloud case: too short a window to see whole minutes", cloud_case(wholeMinutes=["m1", "m2"]),
           "only 2 whole minutes inside the window")

    def cloud_bottleneck_says_cfu():
        c = L.cfg()
        was = c.plat
        c.plat = type("P", (), {"unit": "CFU", "kind": "confluent-cloud"})()
        try:
            text = L.bottleneck({"cores": 20, "tmCapFrac": 1.0, "sourceIdle": 0.0, "kafkaCores": None})
        finally:
            c.plat = was
        if "its 20 CFU" not in text:
            raise Exception(f"the bottleneck sentence does not name the unit: {text}")
    expect("cloud case: the bottleneck sentence counts CFU, not cores (must not fire)", cloud_bottleneck_says_cfu,
           "", should_fire=False)
    def cloud_bottleneck_kafka_capacity():
        got = L.bottleneck_short({"cores": 20, "tmCapFrac": 0.6, "sourceIdle": 0.0, "kafkaEcku": 10.0,
                                  "kafkaEckuLimit": 10})
        if got != "Kafka capacity":
            raise Exception(f"a cluster at its eCKU limit is labelled {got!r}")
    expect("cloud case: a cluster at its eCKU limit with the pool under-used is named as the likely "
           "bottleneck (must not fire)",
           cloud_bottleneck_kafka_capacity, "", should_fire=False)

    def lf(files):
        def read(p):
            if p not in files:
                raise OSError("not found")
            return files[p]
        return read
    good_files = {"/w/local/pipeline.json": {"results": "results", "job": {"sql": "INSERT INTO out\n SELECT * FROM orders;"}},
                  "/w/local/results/completeness.json": {"result": "PASS"},
                  "/w/local/results/tinyproof.json": {"result": "PASS"}}
    cloud_sql = "insert into out select * from orders"
    expect("local first: the same app passed on the laptop, so the cloud may start (must not fire)",
           lambda: (L.P.local_first_reason("/w/local/pipeline.json", cloud_sql, lf(good_files)) is None
                    or (_ for _ in ()).throw(Exception(L.P.local_first_reason("/w/local/pipeline.json", cloud_sql, lf(good_files))))),
           "", should_fire=False)
    def lf_reason(files, sql=cloud_sql, path="/w/local/pipeline.json"):
        def go():
            why = L.P.local_first_reason(path, sql, lf(files))
            if why:
                raise Refusal("rig", why)
        return go
    expect("local first: no laptop run named", lf_reason(good_files, path=None), "platform.localPipeline is not set")
    def shared(cloud_results):
        def run():
            why = L.P.shared_results_reason("/w/app/pipeline.json", cloud_results,
                                            read_json=lambda p: {"results": "results"})
            if why:
                raise Refusal("rig", why)
        return run
    expect("local first: the cloud run would write into the laptop's results", shared("/w/app/results"),
           "same folder")
    expect("local first: the cloud run has its own results folder (must not fire)", shared("/w/app/results-cloud"),
           "", should_fire=False)

    def claimed(cases, steps):
        def run():
            why = L.P.unclaimed_cases_reason(cases, steps)
            if why:
                raise Refusal("rig", why)
        return run
    expect("cases: the first full Confluent suite's 5 CFU cases served no claimed step",
           claimed([5, 10, 20], ["10->20"]), "the 5 CFU case is in no claimed step")
    expect("cases: a paid run names the steps its claim is about", claimed([10, 20], None), "name the steps")
    expect("cases: a claimed step must be between neighbouring cases", claimed([5, 10, 20], ["5->20"]),
           "not a step between neighbouring cases")
    expect("cases: every case serves a claimed step (must not fire)", claimed([10, 20], ["10->20"]), "",
           should_fire=False)
    expect("cases: both steps claimed keeps all three cases (must not fire)",
           claimed([5, 10, 20], ["5->10", "10->20"]), "", should_fire=False)

    def docker_config_without_helper():
        home = tempfile.mkdtemp(prefix="fsk-home-")
        try:
            os.makedirs(os.path.join(home, ".docker", "cli-plugins"))
            os.makedirs(os.path.join(home, ".docker", "contexts"))
            with open(os.path.join(home, ".docker", "config.json"), "w") as f:
                json.dump({"auths": {"x": {}}, "credsStore": "desktop", "currentContext": "desktop-linux"}, f)
            d = L.harness_docker_config(home=home, dest=os.path.join(home, "out"))
            got = json.load(open(os.path.join(d, "config.json")))
            links = sorted(x for x in os.listdir(d) if os.path.islink(os.path.join(d, x)))
        finally:
            shutil.rmtree(home, ignore_errors=True)
        if got != {"currentContext": "desktop-linux"} or links != ["cli-plugins", "contexts"]:
            raise Exception(f"config {got}, links {links}")
    expect("docker: the harness's config keeps the context and plugins, never the credential helper (must not fire)",
           docker_config_without_helper, "", should_fire=False)

    def shape_changes_the_build():
        c = cfg()
        jar0, tmpd = c.jar, tempfile.mkdtemp(prefix="fsk-jar-")
        c.jar = os.path.join(tmpd, "job.jar")
        with open(c.jar, "wb") as f:
            f.write(b"a jar")
        gen = c.raw.setdefault("generator", {})
        old = gen.get("cmd")
        try:
            before = L.build_hash()
            gen["cmd"] = (old or "") + " --symbols=32768"
            after = L.build_hash()
        finally:
            gen["cmd"] = old
            c.jar = jar0
            shutil.rmtree(tmpd, ignore_errors=True)
        if before == after:
            raise Exception(f"the generator's settings do not change the build hash ({before} -> {after})")
    expect("build: a changed input shape is a new build, so completeness runs again (must not fire)",
           shape_changes_the_build, "", should_fire=False)
    expect("local first: completeness has not passed on the laptop",
           lf_reason(dict(good_files, **{"/w/local/results/completeness.json": {"result": "FAIL"}})),
           "completeness has not passed on the laptop")
    expect("local first: the tiny proof never ran on the laptop",
           lf_reason({k: v for k, v in good_files.items() if "tinyproof" not in k}), "the tiny proof has not passed")
    expect("local first: the laptop declares no job SQL",
           lf_reason(dict(good_files, **{"/w/local/pipeline.json": {"results": "results", "job": {}}})),
           "declares no job.sql")
    expect("local first: the cloud would run a different INSERT",
           lf_reason(good_files, sql="INSERT INTO out SELECT order_id FROM orders"), "is not the INSERT")

    def ddl_both():
        cols = [("order_id", "STRING"), ("account", "INT")]
        cc = L.P.table_ddl("confluent-cloud", "orders", cols, 40)
        lo = L.P.table_ddl("local", "orders", cols, 40, bootstrap="kafka:9092")
        assert cc == "CREATE TABLE IF NOT EXISTS `orders` (order_id STRING, account INT) DISTRIBUTED INTO 40 BUCKETS", cc
        assert lo.startswith("CREATE TABLE orders (order_id STRING, account INT) WITH ('connector' = 'kafka', "
                             "'topic' = 'orders', 'properties.bootstrap.servers' = 'kafka:9092'"), lo
        keyed = L.P.table_ddl("confluent-cloud", "sums", cols, 20, key=["account"])
        assert keyed.endswith("DISTRIBUTED BY (account) INTO 20 BUCKETS"), keyed
    expect("tables: the same name and columns on both platforms, each connected its own way (must not fire)",
           ddl_both, "", should_fire=False)
    expect("tables: a platform with no table definition yet says so",
           lambda: L.P.table_ddl("aws", "orders", [("a", "INT")], 4), "no table definition for platform 'aws'")

    def rows_same():
        m, e = L.rows_differ([{"account": 1, "n": 10}, {"account": 2, "n": "5"}],
                             [{"n": 5, "account": "2"}, {"account": "1", "n": 10.0}])
        if m or e:
            raise Exception(f"equal rows reported different: {m} {e}")
    expect("cloud completeness: the same rows in any order and number format match (must not fire)", rows_same,
           "", should_fire=False)
    def rows_lost():
        m, e = L.rows_differ([{"account": 1, "n": 10}, {"account": 2, "n": 5}], [{"account": 1, "n": 10}])
        if m:
            raise Refusal("case", f"rows missing from the outputs: {m}")
    expect("cloud completeness: a row the outputs do not have is named", rows_lost, "rows missing from the outputs")

    fill_raw = {"tables": {"orders": {"columns": [["order_id", "STRING"], ["account", "INT"]]}},
                "fill": {"jobs": 3, "perJob": ["CREATE TABLE base_{i} (a INT)",
                                               "INSERT INTO {topic} SELECT * FROM base_{i}"]}}

    def cc_fill(fake, mk, said, raw):
        calls = {"n": 0}
        def docker(args):
            t = args[args.index("--topic") + 1]
            if t == "orders":
                calls["n"] += 1
                return 0, f"orders:0:{400 * calls['n']}\n", ""
            return fake.docker(args)
        p = mk(dict(fill_raw, _docker=docker)); p.up(); p.fill_poll_s = 0
        p.create_table("orders", partitions=40)
        ddl = [v["sql"] for v in fake.statements.values() if v["sql"].startswith("CREATE TABLE IF NOT EXISTS `orders`")]
        assert ddl == ["CREATE TABLE IF NOT EXISTS `orders` (order_id STRING, account INT) DISTRIBUTED INTO 40 BUCKETS"], ddl
        assert p.fill_topic("orders", 1000) == 1200
        inserts = [n for n, v in fake.statements.items() if v["sql"].startswith("INSERT INTO orders")]
        assert inserts == [], f"fill jobs left running: {inserts}"
        assert sum(1 for v in fake.statements.values() if v["sql"].startswith("CREATE TABLE base_")) == 3
        p.down()
    expect("cloud fill: generator jobs side by side until the topic holds the count, then deleted (must not fire)",
           on_confluent(cc_fill), "", should_fire=False)

    def cc_fill_stalls(fake, mk, said, raw):
        def docker(args):
            t = args[args.index("--topic") + 1]
            return (0, "orders:0:400\n", "") if t == "orders" else fake.docker(args)
        p = mk(dict(fill_raw, _docker=docker)); p.up(); p.fill_poll_s = 0; p.fill_stall_s = 0
        try:
            p.fill_topic("orders", 1000)
        finally:
            assert not [n for n, v in fake.statements.items() if v["sql"].startswith("INSERT INTO orders")], \
                "a stalled fill left its jobs running"
            p.down()
    expect("cloud fill: a fill that stops writing stops, says how far it got, and deletes its jobs",
           on_confluent(cc_fill_stalls), "the fill stopped writing: orders held 400 of 1,000 records")

    def cc_rows(fake, mk, said, raw):
        p = mk(); p.up()
        fake.schemas = {"fsk-t-count": ["account", "n"]}
        fake.results = {"fsk-t-count": [[1, 10], [2, 5]]}
        rows = p.statement_rows("fsk-t-count", "SELECT account, COUNT(*) AS n FROM orders GROUP BY account")
        assert rows == [{"account": 1, "n": 10}, {"account": 2, "n": 5}], rows
        p.down()
    expect("cloud rows: a bounded statement's rows come back by column name (must not fire)", on_confluent(cc_rows),
           "", should_fire=False)

    def cc_rows_snapshot(fake, mk, said, raw):
        p = mk(); p.up()
        fake.schemas = {"fsk-t-count": ["account", "n"]}
        fake.results = {"fsk-t-count": [[1, 10]]}
        p.statement_rows("fsk-t-count", "SELECT account, COUNT(*) AS n FROM orders GROUP BY account")
        got = fake.statements["fsk-t-count"]["properties"]
        p.down()
        if got.get("sql.snapshot.mode") != "now":
            raise Refusal("rig", f"the count ran without sql.snapshot.mode=now ({got}): a plain GROUP BY reads two "
                                 f"change-log rows per record")
    expect("cloud rows: every count runs as a snapshot query, which gives only its final rows (must not fire)",
           on_confluent(cc_rows_snapshot), "", should_fire=False)

    def cc_rows_changelog(fake, mk, said, raw):
        p = mk(); p.up(); p.results_wait_s = 0
        fake.schemas = {"fsk-t-count": ["account", "n"]}
        fake.results_409 = 2
        fake.pages = {"fsk-t-count": [[{"op": 0, "row": [1, 10]}, {"op": 0, "row": [2, 3]}],
                                      [{"op": 1, "row": [2, 3]}, {"op": 2, "row": [2, 5]}],
                                      [{"op": 0, "row": [3, 1]}, {"op": 3, "row": [3, 1]}]]}
        rows = p.statement_rows("fsk-t-count", "SELECT account, COUNT(*) AS n FROM orders GROUP BY account")
        assert sorted(rows, key=lambda r: r["account"]) == [{"account": 1, "n": 10}, {"account": 2, "n": 5}], rows
        p.down()
    expect("cloud rows: results read while the statement runs, through 'not ready' and every page, with updates "
           "and deletes applied (must not fire)", on_confluent(cc_rows_changelog), "", should_fire=False)

    def cc_rows_last_page_once(fake, mk, said, raw):
        p = mk(); p.up(); p.results_wait_s = 0
        fake.schemas = {"fsk-t-count": ["account", "n"]}
        fake.pages = {"fsk-t-count": [[{"op": 0, "row": [1, 10]}], [{"op": 0, "row": [2, 5]}]]}
        fake.running_reads = 2
        rows = p.statement_rows("fsk-t-count", "SELECT account, COUNT(*) AS n FROM orders GROUP BY account")
        p.down()
        if sorted(rows, key=lambda r: r["account"]) != [{"account": 1, "n": 10}, {"account": 2, "n": 5}]:
            raise Refusal("rig", f"the last page was read more than once: {rows}")
    expect("cloud rows: the last page is read once, though the statement still says running (must not fire)",
           on_confluent(cc_rows_last_page_once), "", should_fire=False)

    def cc_gone(fake, mk, said, raw):
        p = mk(); p.up()
        assert p.statement_status("fsk-t-never-made")[0] == "GONE"
        p.down()
    expect("confluent: reading the status of a statement that no longer exists says it is gone, and does not "
           "stop the run (must not fire)", on_confluent(cc_gone), "", should_fire=False)

    def cc_estimate():
        est = PC.estimate_usd([5, 10, 20], 3)
        assert 1 < est < 150, est
        assert PC.estimate_usd([5, 10, 20], 3, settle_cases=0) < est
        assert PC.estimate_usd([5, 10, 20], 3, max_ecku=1) < est      # the eCKU cap is in it
    expect("confluent: a full suite's estimate sits well inside the budget (must not fire)", cc_estimate, "",
           should_fire=False)

    expect("platform: a name that is not a platform",
           lambda: L.P.platform_kind({"platform": "azure"}), "must be one of")

    def laptop_rows_are_real_rows():
        import re as _re
        rows_here = set(_re.findall(r'^    check\("([^"]+)"', open(__file__).read(), _re.M))
        stale = sorted(set(L.P.LAPTOP_ONLY) - rows_here)
        if stale:
            raise Exception(f"LAPTOP_ONLY names rows preflight no longer has: {stale}")
        if any(L.P.not_checked("local", r) for r in rows_here):
            raise Exception("a row is skipped on the laptop")
        if not all(L.P.not_checked("aws", r) for r in L.P.LAPTOP_ONLY):
            raise Exception("a laptop-only row is still judged on a managed service")
    expect("platform: every laptop-only row is a real row, judged locally, reported elsewhere (must not fire)",
           laptop_rows_are_real_rows, "", should_fire=False)

    def rows_reaching_the_laptop():
        """Every preflight row whose check touches the laptop's stack (Docker, the
        engine's REST, the cgroup) is reported as not checked on a managed
        service. The first cloud chain stopped on one that was not."""
        import re as _re
        src = open(__file__).read()
        pf = src[src.index("\ndef cmd_preflight("):]       # the definition, not this line
        pf = pf[:pf.index("\ndef ", 10)]
        rows = _re.findall(r'^    check\("([^"]+)",\s*([A-Za-z_]+)\)', pf, _re.M)
        if len(rows) < 20:
            raise Exception(f"found {len(rows)} preflight rows; the scan is not reading preflight")
        missed = []
        for row, fn in rows:
            m = _re.search(r"^    def " + fn + r"\(\):\n(.*?)(?=^    def |\Z)", pf, _re.M | _re.S)
            body = m.group(1) if m else ""
            if _re.search(r"\brest\(|docker|dexec|tm_container|compose|cgroup|\bbroker\(", body) \
                    and not L.P.not_checked("confluent-cloud", row):
                missed.append(row)
        if missed:
            raise Exception(f"rows that reach the laptop's stack but run on a managed service: {missed}")
    expect("platform: every row that reaches the laptop's stack is reported on a managed service (must not fire)",
           rows_reaching_the_laptop, "", should_fire=False)
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

    # Clean-room run 52: 0.26% of input repeated on purpose, as section 4 asks,
    # read 4.987 against a declared 5 and was stopped although nothing was lost.
    def fanout(got, should_pass):
        def go():
            rows, bad = L.design_diff({"operators": ["parse-order"]}, RUN36_PLAN, {},
                                      {"outputsPerInput": (5.0, got)})
            if should_pass and bad:
                raise bad
            if not should_pass and not bad:
                raise Exception(f"a fan-out of {got} against 5 passed")
        return go
    expect("fan-out: 4.987 against 5, the repeats section 4 asks for, passes (must not fire)",
           fanout(4.987, True), "", should_fire=False)
    expect("fan-out: 4.70 against 5, more than 5% under, still stops (must not fire)",
           fanout(4.70, False), "", should_fire=False)
    expect("fan-out: 5.2 against 5, over the declared fan-out, still stops (must not fire)",
           fanout(5.2, False), "", should_fire=False)

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

    def every_renderer_on_real_suites():
        # Clean-room run 42 ended with a 0-byte suite.txt because a renderer
        # raised. Both fixtures are real suites -- run 42's trimmed, and run
        # 52's (settling cases, a not-settled step, the dashboard line) -- and
        # each goes through every renderer the report uses.
        here = os.path.dirname(os.path.abspath(__file__))
        saved = L._CFG.out_per_in
        try:
            for name, must in (("suite-no-fanout.json", ()),
                               ("suite-run52-not-settled.json", ("undecided", "settle-6", "1->2"))):
                out = json.load(open(os.path.join(here, "fixtures", name)))
                out["table"] = L.build_table(out["runs"], quick=out.get("quickLook", False))
                L._CFG.out_per_in = out.get("outputsPerInput")
                text, md, card = L.render_table(out), L.render_markdown(out), L.scorecard(out)
                for kind, body in (("table", text), ("markdown", md), ("scorecard", "\n".join(card) if isinstance(card, list) else str(card))):
                    if not body.strip() or "nan" in body.lower():
                        raise Exception(f"{name}: the {kind} rendered empty or with a nan")
                if not md.lstrip().startswith(("#", "|", "**")):
                    raise Exception(f"{name}: the markdown does not start as markdown: {md[:60]!r}")
                for word in must:
                    if word not in text + md:
                        raise Exception(f"{name}: the report never says {word!r}")
            rows, _ = L.design_diff({"operators": ["parse-order"], "outputs": ["positions"]}, RUN36_PLAN,
                                    {"positions": 5}, {"outputsPerInput": (5, 4.99)})
            table = "\n".join(L.design_table(rows))
            if "parse-order" not in table or "outputsPerInput" not in table:
                raise Exception(f"the design table lost a row: {table[:200]!r}")
        finally:
            L._CFG.out_per_in = saved
    expect("report: every renderer runs on two real suites, run 42's and run 52's (must not fire)",
           every_renderer_on_real_suites, "", should_fire=False)

    # Small guards that had no test of their own (review of 2026-10-02).
    def outputs_written():
        assert L.declared_outputs_verdict({"mv-sym": 812, "mv-acct": 40}) is None
        one = L.declared_outputs_verdict({"mv-sym": 812, "mv-acct": 0})
        assert isinstance(one, Refusal) and "topic mv-acct is empty" in one.msg, one
        two = L.declared_outputs_verdict({"mv-sym": 0, "mv-acct": 0})
        assert "topics mv-acct, mv-sym are empty" in two.msg, two.msg
    expect("outputs: a declared output left empty by a full drain is named (must not fire)",
           outputs_written, "", should_fire=False)

    def backlog_ran_out():
        L.drained({"committed": 900, "endIn": 2000})        # still draining: must not fire (else "2,000")
        L.drained({"committed": 900, "endIn": 0})           # no end known yet: not a verdict
        L.drained({"committed": 1000, "endIn": 1000})
    expect("the backlog ran out under the job", backlog_ran_out, "the backlog ran out (1,000 records)")

    def one_max_parallelism():
        saved_kc, saved_props = L.keycheck, dict(c.flink_props)
        try:
            c.flink_props.pop("pipeline.max-parallelism", None)
            L.keycheck = lambda *a, **k: ["1\t128", "2\t128", "4\t128"]
            got = L.max_parallelism_for([1, 2, 4])
            assert got == (128, "Flink's own default for these parallelisms"), got
            c.flink_props["pipeline.max-parallelism"] = "369"
            assert L.max_parallelism_for([1, 2, 4])[0] == 369
            c.flink_props.pop("pipeline.max-parallelism")
            L.keycheck = lambda *a, **k: ["1\t128", "2\t128", "4\t256"]
            L.max_parallelism_for([1, 2, 4])
        finally:
            L.keycheck = saved_kc
            c.flink_props.clear(); c.flink_props.update(saved_props)
    expect("cases that would not share one maxParallelism", one_max_parallelism,
           "would not share one maxParallelism")

    # The warm-up and window-anchoring loops, on a fake clock fed with the
    # offset sampler's ticks. Their verdicts (warmup_verdict, settle_boundary)
    # were tested; the loops that find each commit, honour the deadline and
    # notice a dry backlog ran only live. The ticks are clean-room run 52's own.
    TICKS = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "fixtures", "ticks-run52-2c.json")))["ticks"]

    class Clock:
        """time.time() and time.sleep() on a virtual clock; the rest is real."""
        def __init__(self, t):
            self.now = t

        def time(self):
            return self.now

        def sleep(self, s):
            self.now += max(s, 0.01)

        def __getattr__(self, name):
            return getattr(time, name)

    def on_ticks(ticks, fn):
        """Run fn with the sampler returning, at each virtual moment, the ticks
        written by then."""
        clock = Clock(ticks[0]["ts"] / 1000.0)
        saved = (L.time, L.sampler_tail)
        L.time = clock
        L.sampler_tail = lambda n=8: [t for t in ticks if t["ts"] <= clock.now * 1000][-n:]
        try:
            return fn()
        finally:
            L.time, L.sampler_tail = saved

    def stretched(ticks, seconds, scale=lambda i: 1.0, end_in=None):
        """A longer stream with run 52's own commit steps, repeated, each step's
        records multiplied by scale(commit number)."""
        steps, prev = [], ticks[0]
        for t in ticks[1:]:
            if t["committed"] != prev["committed"]:
                steps.append((t["ts"] - prev["ts"], t["committed"] - prev["committed"]))
                prev = t
        out, ts, committed, i, last = [], ticks[0]["ts"], ticks[0]["committed"], 0, ticks[0]["ts"]
        while ts - ticks[0]["ts"] < seconds * 1000:
            dt, dc = steps[i % len(steps)]
            while last + 500 < ts + dt:
                last += 500
                out.append({"ts": last, "committed": committed, "endIn": end_in or 10**12})
            ts += dt
            committed += int(dc * scale(i))
            if end_in:
                committed = min(committed, end_in)
            out.append({"ts": ts, "committed": committed, "endIn": end_in or 10**12})
            last, i = ts, i + 1
        return out

    def warm_steady():
        d = on_ticks(stretched(TICKS, 300), lambda: L.wait_flat(600))
        assert d.get("rampFlatAtS") is not None, d
    expect("warm-up: run 52's own commit steps settle (must not fire)", warm_steady, "", should_fire=False)

    def warm_falling():
        on_ticks(stretched(TICKS, 900, scale=lambda i: 0.93 ** i), lambda: L.wait_flat(600))
    expect("warm-up: a rate that keeps falling never settles", warm_falling, "never settled")

    def warm_nothing():
        flat = [{"ts": TICKS[0]["ts"] + 500 * i, "committed": 0, "endIn": 10**9} for i in range(2000)]
        on_ticks(flat, lambda: L.wait_flat(300))
    expect("warm-up: a pipeline that commits nothing is named as such", warm_nothing,
           "did not commit a single offset")

    def warm_runs_dry():
        # about 60 s of run 52's 2-core rate (~250,000/s): dry before the 90 s warm-up floor
        on_ticks(stretched(TICKS, 900, end_in=TICKS[0]["committed"] + 15_000_000), lambda: L.wait_flat(600))
    expect("warm-up: the backlog running dry is named", warm_runs_dry, "the backlog ran out")

    def window_opens_on_a_commit():
        base = TICKS[0]
        got = on_ticks(TICKS, lambda: L.next_boundary(after=base, timeout=60))
        assert got["committed"] > base["committed"] and got["ts"] > base["ts"], got
        assert any(t["committed"] == got["committed"] for t in TICKS), "a boundary no tick recorded"
    expect("window: it opens on run 52's next real commit, not on the clock (must not fire)",
           window_opens_on_a_commit, "", should_fire=False)

    def window_never_opens():
        still = [{"ts": TICKS[0]["ts"] + 500 * i, "committed": TICKS[0]["committed"], "endIn": 10**12}
                 for i in range(400)]
        on_ticks(still, lambda: L.next_boundary(after=still[0], timeout=60))
    expect("window: an offset that never advances stops the case", window_never_opens,
           "did not advance within 60s")

    # graph_shape reads the running plan over REST; until 2026-10-02 only its
    # comparison (check_shape) was tested. A fake REST answer drives the read.
    def plan_of(chained, max_par, jid="j1", par=2):
        if chained:
            nodes = [{"id": "a", "description": "Source: orders -> parse -> aggregate -> sink",
                      "parallelism": par, "inputs": []}]
        else:
            nodes = [{"id": "s", "description": "Source: orders -> parse", "parallelism": par, "inputs": []},
                     {"id": "a", "description": "aggregate -> sink", "parallelism": par,
                      "inputs": [{"id": "s", "ship_strategy": "HASH"}]}]
        return {"plan": {"jid": jid, "nodes": nodes}}, {"vertices": [{"maxParallelism": max_par} for _ in nodes]}

    def shape_with(chained, max_par, jid="j1", par=2):
        plan, job = plan_of(chained, max_par, jid, par)
        saved = L.rest
        L.rest = lambda path, *a, **k: plan if path.endswith("/plan") else job
        try:
            return L.graph_shape(jid)
        finally:
            L.rest = saved

    def shape_read():
        s2 = shape_with(False, 128)
        assert s2["vertexCount"] == 2 and s2["maxParallelism"] == [128], s2
        assert s2["signature"] == [["Source: orders -> parse", []], ["aggregate -> sink", ["HASH"]]], s2["signature"]
        # the same job at another size, under a new job id, is the same shape
        L.check_shape(shape_with(False, 128, jid="j2", par=4), s2)
    expect("graph: the shape is read off the plan, and a new job at another size matches (must not fire)",
           shape_read, "", should_fire=False)

    def chained_baseline():
        # Measured on one build: a chained baseline read 211,533 against 140,308
        # for the same graph as the other cases, and 1->4 2.16x against 3.26x.
        L.check_shape(shape_with(True, 128, par=1), shape_with(False, 128))
    expect("graph: a baseline chained into one vertex is a different shape", chained_baseline,
           "job graph shape differs")

    def other_key_groups():
        L.check_shape(shape_with(False, 256, par=4), shape_with(False, 128))
    expect("graph: a different key-group count is a different shape", other_key_groups,
           "job graph shape differs")

    # backpressure_in_window parsed the worker's reporter log only live. These
    # lines follow the SLF4J reporter's form: a scope ending in the task name and
    # subtask, then the metric. One sample before the window and one after must
    # not count.
    def reporter_log(gc_names=("G1_Young_Generation",)):
        def at(t):
            return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".000000000Z"
        lines = []
        for i, t in enumerate((990, 1000, 1010, 1020, 1030)):
            busy = 999.0 if t in (990, 1030) else 800.0 + 20 * i
            lines += [f"{at(t)} tm.taskmanager.x1.job.Source: orders -> parse.0.busyTimeMsPerSecond: {busy}",
                      f"{at(t)} tm.taskmanager.x1.job.Source: orders -> parse.0.idleTimeMsPerSecond: 100.0",
                      f"{at(t)} tm.taskmanager.x1.job.aggregate -> sink.1.backPressuredTimeMsPerSecond: 50.0"]
            for gname in gc_names:
                lines.append(f"{at(t)} tm.taskmanager.x1.Status.JVM.GarbageCollector.{gname}.Time: {100 * i}")
        return "\n".join(lines)

    def bp_with(log, t0=1000, t1=1020):
        saved = L.sh
        L.sh = lambda cmd, *a, **k: type("Result", (), {"stdout": log, "stderr": "", "returncode": 0})()
        try:
            return L.backpressure_in_window(t0, t1)
        finally:
            L.sh = saved

    def bp_window():
        got = bp_with(reporter_log())
        src = got["Source: orders -> parse"]
        assert src["busy"] == round((820 + 840 + 860) / 3 / 1000, 4) and src["samples"] == 3, src
        assert src["idle"] == 0.1, src
        assert got["aggregate -> sink"]["backPressured"] == 0.05, got
        assert got["_gc"] == {"G1_Young_Generation.Time": 200.0}, got["_gc"]   # 100 -> 300 inside
        assert got["_gcNames"] == ["G1_Young_Generation"], got["_gcNames"]
    expect("back-pressure: only samples inside the window count, averaged per vertex (must not fire)",
           bp_window, "", should_fire=False)

    def bp_serial_seen():
        got = bp_with(reporter_log(gc_names=("Copy", "MarkSweepCompact")))
        assert got["_gcNames"] == ["Copy", "MarkSweepCompact"], got["_gcNames"]
    expect("back-pressure: the collector that actually ran is read from the log (must not fire)",
           bp_serial_seen, "", should_fire=False)

    # disk_projection measures what disk_verdict decides on; only the verdict
    # was tested. Fake broker sizes drive the measurement: 20 GB of tiny input
    # for 100,000,000 records (200 bytes each) and 5 GB of sink for 10,000,000
    # consumed (500 bytes per input), all of which is deleted before the fill.
    def projected(free_gb, on_disk_gb=0.0):
        GB = 1e9
        saved = {k: getattr(L, k) for k in ("topic_bytes", "topic_bytes_if_any", "volume_bytes",
                                            "host_free_bytes")}
        L.topic_bytes = lambda topics: 20 * GB
        L.topic_bytes_if_any = lambda topics: (on_disk_gb * GB if topics == [c.suite_topic_in] else 5 * GB)
        L.volume_bytes = lambda vol: 1 * GB
        L.host_free_bytes = lambda: free_gb * GB
        try:
            return L.disk_projection("orders-tiny", 100_000_000, {"close": {"committed": 10_000_000}})
        finally:
            for k, v in saved.items():
                setattr(L, k, v)

    def disk_measured():
        d = projected(500)
        assert d["inputBytesPerRecord"] == 200.0, d
        assert d["reclaimableBytes"] == int(25e9) and d["hostFreeBytesNow"] == int(500e9), d
        m = d["measuredOn"]
        assert (m["tinyTopicBytes"], m["sinkRecordsConsumed"], m["sinkBytesOnDisk"]) == (int(20e9), 10_000_000, int(5e9)), m
        assert d["fits"] and d["hostFreeBytes"] == int(525e9), d       # free now + what is deleted before the fill
    expect("disk: the projection measures bytes per record and credits what the fill deletes (must not fire)",
           disk_measured, "", should_fire=False)

    def disk_does_not_fit():
        projected(1)
    expect("disk: a suite that cannot fit stops before the fill", disk_does_not_fit, "of disk and only")

    def disk_input_already_there():
        need_cold = projected(10_000)["neededBytes"]
        need_warm = projected(10_000, on_disk_gb=50)["neededBytes"]
        assert need_cold - need_warm == int(50e9), (need_cold, need_warm)   # run 36: not asked for twice
    expect("disk: input already on the broker is not asked for twice (must not fire)",
           disk_input_already_there, "", should_fire=False)

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
        # edges say how records cross, in the words a reader uses (issue #76)
        for needle in ("flowchart LR", "keyBy", "broadcast", "parse-order"):
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

    def read_progress():
        try:
            return open(os.path.join(c.results, "PROGRESS.txt")).read()
        except OSError:
            return None

    def chain():
        # in its own directory: the first version wrote its fake chain into the
        # live results/ (phases.log, all.json and a DONE saying "STOPPED at c")
        # while a real `all` was running the live self-test around it
        ran = []
        fake = [(n, (lambda n=n: (ran.append(n), 0)[1])) for n in ("a", "b")]
        fake += [("c", lambda: (ran.append("c"), 1)[1]), ("d", lambda: (ran.append("d"), 0)[1])]
        tmp = tempfile.mkdtemp(prefix="prove-all-selftest-")
        try:
            with open(os.path.join(tmp, "phases.log"), "w") as f:
                f.write("2026-01-01 00:00:00 phase=b end rc=0 1s\n")   # an earlier chain's line
            progress_before = read_progress()
            rc = cmd_all(steps=fake, results=tmp)
            done = open(os.path.join(tmp, "DONE")).read().strip()
            phase_lines = open(os.path.join(tmp, "phases.log")).read()
            allj = json.load(open(os.path.join(tmp, "all.json")))
            stray = [f for f in ("DONE", "all.json") if os.path.exists(os.path.join(c.results, f))
                     and os.path.getmtime(os.path.join(c.results, f)) > t_self]
            if read_progress() != progress_before:
                stray.append("PROGRESS.txt")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if rc != 1 or ran != ["a", "b", "c"] or not done.startswith("STOPPED at c") or allj["verdict"] != "STOPPED at c":
            raise Exception(f"rc={rc} ran={ran} DONE={done!r} verdict={allj.get('verdict')}")
        if stray:
            raise Exception(f"the self-test wrote into the live results directory: {stray}")
        if "2026-01-01" in phase_lines:
            raise Exception("phases.log still holds the previous chain's lines")
        raise Refusal("rig", f"chain stopped at c, d never ran, DONE says {done!r}")
    expect("all: the chain stops at the first failing step", chain, "stopped at c")

    def paid_chain_tears_down():
        downs = []
        tmp = tempfile.mkdtemp(prefix="prove-all-selftest-")
        try:
            cmd_all(steps=[("up", lambda: 0), ("preflight", lambda: 1)], results=tmp,
                    teardown=lambda: downs.append("after a stop"))
            cmd_all(steps=[("up", lambda: 0), ("report", lambda: 0)], results=tmp,
                    teardown=lambda: downs.append("after a pass"))
            cmd_all(steps=[("local first", lambda: 1)], results=tmp,
                    teardown=lambda: downs.append("with nothing created"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if downs != ["after a stop", "after a pass"]:
            raise Exception(f"teardown ran {downs}; it should run after a stop and after a pass, "
                            f"and not when nothing was created")
    expect("all: a chain on a paid service tears its stack down however it ends (must not fire)",
           paid_chain_tears_down, "", should_fire=False)

    def crash_and_interrupt_tear_down():
        # The first full Confluent chain crashed in its report with a KeyError
        # and left its stack up for nine hours (2026-10-06). A crash and an
        # interrupt must both reach the teardown and write DONE with the reason.
        def boom():
            raise KeyError("tmThrottledPeriodsPct")
        def interrupted():
            raise KeyboardInterrupt("signal 15")
        got = []
        for label, fn in (("crash", boom), ("interrupt", interrupted)):
            downs = []
            tmp = tempfile.mkdtemp(prefix="prove-all-selftest-")
            try:
                cmd_all(steps=[("up", lambda: 0), ("report", fn)], results=tmp,
                        teardown=lambda: downs.append(label))
                done = open(os.path.join(tmp, "DONE")).read().strip()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
            got.append((label, downs, done))
        crash, intr = got
        if crash[1] != ["crash"] or "stopped with an error: KeyError" not in crash[2]:
            raise Exception(f"a crash in a step: teardown {crash[1]}, DONE {crash[2]!r}")
        if intr[1] != ["interrupt"] or "interrupted during report" not in intr[2]:
            raise Exception(f"an interrupt in a step: teardown {intr[1]}, DONE {intr[2]!r}")
    expect("all: a crash or an interrupt in a step still tears the stack down and writes DONE (must not fire)",
           crash_and_interrupt_tear_down, "", should_fire=False)

    def report_renders_cloud_records():
        # The first full Confluent chain's own suite.json (2026-10-06): its cases
        # have no cgroup readings, and the report crashed on the first of them.
        out = json.load(open(os.path.join(L.HERE, "fixtures", "confluent-sqlapp-suite.json")))
        for fn in (L.render_table, L.render_markdown):
            try:
                fn(out)
            except KeyError as e:
                raise Exception(f"{fn.__name__} still needs a laptop-only reading: {e}")
    expect("report: a cloud case with no cgroup readings renders, with dashes (must not fire)",
           report_renders_cloud_records, "", should_fire=False)

    def interval_holds_its_ratio():
        # The first full Confluent suite printed 5->10 at 1.789x and called it met:
        # its one same-time pair read 1.948x, and the interval sat above the point.
        out = json.load(open(os.path.join(L.HERE, "fixtures", "confluent-sqlapp-suite.json")))
        t = L.build_table(out["runs"])
        s = {r["step"]: r for r in t["stepRatios"]}["5->10"]
        if not (s["ratioLowCI"] <= s["ratio"] <= s["ratioHighCI"]):
            raise Exception(f"5->10 prints {s['ratio']}x outside its interval {s['ratioLowCI']}-{s['ratioHighCI']}x")
        if L.step_verdict(s) != "undecided":
            raise Exception(f"5->10 at {s['ratio']}x with {s['ratioLowCI']}-{s['ratioHighCI']}x reads "
                            f"{L.step_verdict(s)!r}, not undecided")
    expect("verdict: a step printed under its target is never called met (must not fire)",
           interval_holds_its_ratio, "", should_fire=False)

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
        # A check that describes the laptop says so on any other platform,
        # rather than passing about a machine the run does not use.
        why = L.P.not_checked(c.platform, name)
        if why:
            name = name if name.endswith("(reported)") else name + " (reported)"
            rows.append((name, "PASS", why)); print(f"PASS  {name:52s} {why}", flush=True)
            return
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
            # Raised, not returned: a returned string is the row's PASS detail,
            # so this printed PASS with "FAIL" inside it (clean-room run 52,
            # read from the code; no recorded run hit it).
            raise Exception(f"no retention.bytes on {missing}, which nothing ever drains")
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
        return L.memory_budget_line(int(info) if info.isdigit() else 0)

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
                f"this check stops the run when it is written down. Set caps.tmMemoryBase and "
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
        skipped = L.key_spread_skipped(c.api)
        if skipped:
            return skipped
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

    def every_declared():
        """Outputs written outside topics.out are the throttled or windowed ones,
        and only the build knows how often they are written. Reported, not
        enforced: a build may have none (issue #76)."""
        every = (c.raw.get("design") or {}).get("every") or {}
        missing = [t for t in c.topics_also if t not in every]
        if not c.topics_also:
            return "no output is written outside topics.out, so there is no interval to show"
        if not missing:
            return "design.every gives an interval for " + ", ".join(f"{t} ({every[t]})" for t in c.topics_also)
        return (f"no interval declared for {', '.join(missing)}: add design.every "
                f"{{\"{missing[0]}\": \"10 s\"}} (with the real interval) so the job graph shows how often it is written")

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
    check("keys divide evenly across subtasks" + (" (reported)" if L.key_spread_skipped(c.api) else ""),
          keys_per_subtask)

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
    check("the diagram can show how often each output is written (reported)", every_declared)

    # The dashboard is built by the agent like the job is, and is tested like
    # it: from its files here, through Grafana after completeness, and over
    # the suite in the report (see lib.dashboard_wiring).
    def dash_wiring():
        svc = L.dashboard_here()
        if not svc:
            return "no Grafana among extraServices; nothing to check"
        ds, boards, found = L.dashboard_files(svc)
        problems, summary = L.dashboard_wiring(ds, boards)
        if found + problems:
            raise Exception("; ".join(found + problems))
        extra["dashboard"] = summary
        return summary

    def dash_loaded():
        svc = L.dashboard_here()
        if not svc:
            return "no Grafana among extraServices; nothing to check"
        ds, boards, _ = L.dashboard_files(svc)
        return L.dashboard_loaded(svc, ds, boards)

    check("the dashboard's panels name a data source that exists", dash_wiring)
    check("the dashboard is loaded and its data source answers", dash_loaded)
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
    if L.on_confluent():
        return cmd_tinyproof_cloud()
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
                    log(f"  STOPPED ({e.refusal.scope}): {e.refusal.msg}")
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
                log(f"  STOPPED ({e.scope}): {e.msg}")
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
                log(f"  STOPPED (rig): backlog {c.backlog:,} is short of the {want:,} records the "
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
            # Measure an out-of-bounds step once more before stopping on it. One
            # pass per case is noisier than the bounds it is judged against:
            # clean-room run 51's unchanged build read 2->4 at 1.49x, 1.91x and
            # 2.14x in three tiny proofs, and stopped the chain twice on noise.
            # The two recorded out-of-bounds tiny proofs were both that noise;
            # the failure the bounds exist for (a 3.73x from a baseline running
            # its tasks on one core) is built into the rig and reads the same
            # twice. So the second reading decides, and both are kept.
            first = {k: v["recordsPerSec"] for k, v in recs.items()}
            def in_bounds(a, b):
                r = recs[b]["recordsPerSec"] / recs[a]["recordsPerSec"]
                return T["tinyRatioLo"] * (b / a) / 2 <= r <= T["tinyRatioHi"] * (b / a) / 2
            odd = sorted({x for a, b in pairs if a in recs and b in recs and not in_bounds(a, b) for x in (a, b)})
            if odd:
                log(f"  out of bounds on the first reading; measuring {', '.join(f'{x}c' for x in odd)} once more")
                for cores in odd:
                    try:
                        rec2, _ = L.run_case_retrying(
                            lambda cores=cores: L.run_case(cores, "tiny", "tiny-again", shape_ref, cores == lo, man,
                                                           warmup_max_s=120.0, reporter_s=2))
                        rec2["firstReading"] = first[cores]
                        recs[cores] = rec2
                        out["cases"].append(rec2)
                        log(f"  {cores}c again: {rec2['recordsPerSec']:,.0f} rec/s (first {first[cores]:,.0f})")
                    except CaseRefused as e:
                        out["cases"].append(e.rec)
                        log(f"  {cores}c again: thrown out -- {e}")
                out["remeasured"] = {str(k): {"first": first[k], "second": recs[k]["recordsPerSec"]} for k in odd}
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
                    again = out.get("remeasured") or {}
                    if str(a) in again or str(b) in again:
                        print(f"  Measured twice: " + "; ".join(
                            f"{k} cores {v['first']:,.0f} then {v['second']:,.0f} rec/s" for k, v in again.items()) + ".")
                    print(f"  The CPU cap, partitions and backlog were already checked, so look at")
                    print(f"  what else ran on the machine while the larger case measured.")
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
    print("TINY PROOF " + ("PASSED" if rc == 0 else "STOPPED"))
    return rc


# ------------------------------------------------------------------------ fill

# ---------------------------------------------------------------- Confluent Cloud
# The same three steps as on the laptop -- fill, completeness, tiny proof --
# with the parts the service owns done its way: tables written for it, a fill
# of generator jobs side by side, a backlog counted as a reader sees it, and
# no worker to kill (the service restarts its own).

def cloud_manifest(topic, name):
    """Count a topic the way a reader sees it: platform.manifestSql, bounded,
    with {topic}; its rows each carry `n`, the records in that row's group.
    The topic's log end counted 7-8% more than any reader got (findings §8)."""
    c = cfg()
    p = c.plat
    if not p.manifest_sql:
        raise Refusal("rig", "platform.manifestSql is not set: a bounded SELECT over {topic} whose rows carry n, "
                             "the records in each group, and the totals completeness compares")
    rows = p.statement_rows(f"{p.prefix}-count-{name}", p.manifest_sql.replace("{topic}", topic))
    total = sum(int(float(r.get("n") or 0)) for r in rows)
    end, _ = p.log_end(topic)
    man = {c.count_field: total, "rows": rows, "topic": topic, "logEnd": end,
           "countedAs": "records a reader gets (a bounded SELECT), not the topic's log end"}
    save_json(f"{name}.json", man)
    log(f"  {topic}: {total:,} records a reader gets; the log end says {end:,}")
    return man


def cmd_fill_cloud():
    c = cfg()
    p = c.plat
    p.create_table(c.topic_in, partitions=c.partitions)
    for t in c.topics_out:
        p.create_table(t, partitions=c.partitions)
    p.fill_topic(c.topic_in, c.backlog)
    man = cloud_manifest(c.topic_in, "manifest")
    print(f"backlog {man[c.count_field]:,} readable records on {c.topic_in}; manifest results/manifest.json")
    return 0


def cmd_tinyproof_cloud():
    """The two smallest cases on a small backlog through run_case_cloud, the
    step held to the laptop's band, then the guard self-test."""
    c = cfg()
    p = c.plat
    topic = f"{c.topic_in}_tiny"
    p.create_table(c.topic_in, as_name=topic, partitions=c.partitions)
    p.fill_topic(topic, c.tiny)
    lo, hi = sorted(c.cases)[:2]
    out = {"build": build_hash(), "topic": topic, "cases": [lo, hi], "runs": [],
           "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    p.input_now = topic
    try:
        for units in (lo, hi):
            try:
                rec, _ = L.run_case_cloud(units, "tiny", "tiny", None, units == lo, {})
            except CaseRefused as e:
                rec = e.rec
            out["runs"].append(rec)
    finally:
        p.input_now = None
    a, b = out["runs"]
    ok = a.get("status") == "OK" and b.get("status") == "OK"
    ratio = (b["recordsPerSec"] / a["recordsPerSec"]) if ok and a.get("recordsPerSec") else None
    ideal = hi / lo
    lo_band, hi_band = T["tinyRatioLo"] * ideal / 2, T["tinyRatioHi"] * ideal / 2
    out["ratio"], out["band"] = ratio, [lo_band, hi_band]
    out["selftest"] = "PASS" if cmd_selftest(live=False) == 0 else "FAIL"
    out["result"] = "PASS" if (ratio and lo_band <= ratio <= hi_band and out["selftest"] == "PASS") else "FAIL"
    save_json("tinyproof.json", out)
    why = ("" if out["result"] == "PASS" else
           f": cases {a.get('status')} and {b.get('status')}, step {ratio and f'{ratio:.2f}x'} against "
           f"{lo_band:.2f}-{hi_band:.2f}x, self-test {out['selftest']}")
    log(f"tiny proof on Confluent Cloud: {out['result']}{why}")
    return 0 if out["result"] == "PASS" else 1


def cmd_completeness_cloud():
    """A small backlog drained to the last record by the job at the baseline
    size, then platform.verifySql over the outputs compared with the manifest
    rows exactly. Killing a worker mid-drain is not possible here: the service
    restarts its own workers, and the record says so."""
    c = cfg()
    p = c.plat
    if not p.verify_sql:
        raise Refusal("rig", "platform.verifySql is not set: a bounded SELECT over the outputs whose rows have the "
                             "same columns as manifestSql's, so the two can be compared row for row")
    topic = f"{c.topic_in}_small"
    p.create_table(c.topic_in, as_name=topic, partitions=c.partitions)
    for t in c.topics_out:
        p.create_table(t, partitions=c.partitions)
    p.fill_topic(topic, c.small)
    man = cloud_manifest(topic, "manifest-small")
    units = c.baseline
    out = {"build": build_hash(), "records": man[c.count_field], "cores": units, "unit": p.unit,
           "killArm": f"not checked on {p.kind}: the service restarts its own workers",
           "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    L.P.set_and_read_back(p, units)
    p.input_now = topic
    name = None
    try:
        t0 = time.time()
        name = p.submit(units, f"{c.project}-complete")
        last, still = None, 0
        while True:
            n = sum(p.log_end(t)[0] for t in c.topics_out)
            still = still + 1 if n == last else 0
            last = n
            waiting = p.statement_minutes(name, t0 - 60, time.time())["pending"]
            if still >= 2 and waiting and waiting[-1][1] == 0:
                break
            if time.time() - t0 > 3600:
                raise Refusal("rig", f"the small backlog had not drained after an hour ({n:,} records out)")
            time.sleep(60)
        got = p.statement_rows(f"{p.prefix}-verify", p.verify_sql)
    finally:
        p.input_now = None
        if name:
            p.cli("flink", "statement", "delete", name, "--cloud", p.cloud, "--region", p.region, "--force")
    expect_rows = [{k: v for k, v in r.items()} for r in man["rows"]]
    missing, extra = L.rows_differ(expect_rows, got)
    out["rowsExpected"], out["rowsGot"] = len(expect_rows), len(got)
    out["missing"], out["extra"] = missing, extra
    out["result"] = "PASS" if not missing and not extra else "FAIL"
    save_json("completeness.json", out)
    log(f"completeness on Confluent Cloud: {out['result']} ({len(got)} rows against {len(expect_rows)} expected"
        + (f"; first missing {missing[:1]}, first extra {extra[:1]}" if out["result"] != "PASS" else "") + ")")
    return 0 if out["result"] == "PASS" else 1


def cmd_fill():
    c = cfg()
    if L.on_confluent():
        return cmd_fill_cloud()
    man = L.fill(c.topic_in, c.backlog, c.seed, "manifest.json")
    print(f"backlog {c.backlog:,} records on {c.topic_in}; manifest results/manifest.json")
    return 0


# ---------------------------------------------------------------- completeness

def cmd_completeness():
    """Process a small test data set twice — once cleanly, once killed and restarted partway —
    and compare the sinks to the generator manifest with no tolerances."""
    c = cfg()
    if L.on_confluent():
        return cmd_completeness_cloud()
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
                    # the job keeps running this long after the last input is committed,
                    # so a throttled output emits its final value before the job is cancelled
                    time.sleep(c.settle_s)
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
            raise Refusal("rig", f"the completeness check did not pass ({label}): the verifier exited with {r.returncode}")
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
        print("COMPLETENESS STOPPED:", e.msg)
        return 1
    finally:
        L._CFG.topic_in = c.raw["topics"]["in"]
    save_json("completeness.json", out)
    # Where the kill actually landed, not where it was asked to (run 47: asked
    # 35%, landed 42.6%, the line said 35%).
    landed = next((a.get("killedAtCommitted") for a in out.get("arms") or [] if a.get("killed")), None)
    at = f"{landed / c.small:.0%}" if landed and c.small else f"{c.kill_frac:.0%}"
    dups = (man or {}).get("duplicateRecords")
    also = (f"; the input repeated {dups:,} records on purpose and each was counted once" if dups else
            "; the input repeated nothing on purpose, so only the kill tested double counting")
    print(f"COMPLETENESS PASSED FOR BUILD {out['build']} (a clean run, and one killed and restarted at {at}{also})")
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


def settle_suite(runs, run_one, budget, judge=None, say=None):
    """Run extra cases until every step is settled or `budget` cases are spent.
    `run_one(cores, pass_id, label=...)` runs one case and appends it to `runs`,
    returning a (reason, message) pair when a rig check says to stop. `judge`
    turns runs into step verdicts (the suite's own table by default). Returns
    (extra cases run, stop). Pulled out of cmd_suite so the self-test can drive
    it with a fake case runner; it had run only live (reference run 5,
    clean-room run 52)."""
    judge = judge or (lambda rs: build_table(rs, quick=False)["stepRatios"])
    say = say or log
    extra, announced, stop = 0, None, None
    while not stop and extra < budget:
        nxt = L.settle_next(runs, judge(runs))
        if not nxt:
            break
        cores, step = nxt
        # Said again whenever the open step changes: reference run 5 settled
        # 1->2 with one case and went on to 2->4 under a line naming 1 and 2.
        if step["step"] != announced:
            say(f"  {step['step']} is undecided: {step['ratioLowCI']:.2f}x to {step['ratioHighCI']:.2f}x "
                f"spans the target. Running up to {budget - extra} more case(s) of "
                f"{step['from']} and {step['to']} cores to decide it.")
            announced = step["step"]
        extra += 1
        # "case 13 of 13", then "case 15 of 15", read as a suite that kept
        # finishing (clean-room run 52); say what these cases are.
        stop = run_one(cores, f"settle-{extra}",
                       label=f"suite: settling {step['step']}, extra case {extra} of up to "
                             f"{budget} ({cores} cores)")
    return extra, stop


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

    def run_one(cores, pass_id, label=None):
        """One case, logged and recorded. Returns a (reason, message) pair
        when a check about the rig says to stop the suite, else None."""
        nonlocal shape_ref, done_cases
        found = None
        log(f"---- case {cores} cores, pass {pass_id} ----")
        per = (time.time() - t_suite) / done_cases if done_cases else None
        log(L.progress(label or (f"suite: case {done_cases + 1} of {total_cases} "
                                 f"({cores} cores, pass {pass_id})"),
                       pct=min(done_cases / total_cases, 0.99),
                       eta_s=per * max(total_cases - done_cases, 1) if per else None))
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
            unit = rec.get("unit") or "cores"
            kafka = (f"kafka {rec['kafkaCores']:.2f}/{c.kafka_cap:g}" if rec.get("kafkaCores") is not None
                     else "kafka cores not measured on this platform")
            log(f"  {cores} {unit} {pass_id}: {rec['recordsPerSec']:,.0f} rec/s  in use {rec['tmCores']:.2f}/{cores} "
                f"({rec['tmCapFrac']:.1%})  {kafka}  "
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
                found = ("a check about the rig stopped it", e.refusal.msg)
        done_cases += 1
        save()
        return found

    for pass_id, order in plan:
        for cores in order:
            stop = run_one(cores, pass_id)
            if stop:
                break
        if stop:
            break
    # A step whose interval spans the target is undecided by the planned
    # passes. Run its two cases alternately -- each case after the first adds a
    # pair of neighbours in time -- until it settles or the budget is spent.
    # Reference run 4 (2026-09-28) read 2->4 at 1.65-1.82x from three pairs.
    extra = 0
    if not stop and not L.QUICK:
        extra, stop = settle_suite(out["runs"], run_one, T["settleExtraCases"])
    if extra:
        out["settleCases"] = extra
        save()
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
    if not os.path.exists(os.path.join(c.results, "manifest.json")):
        # clean-room run 51 (S10): a chain stopped at the tiny proof never fills
        raise Refusal("rig", "ceiling drains the full test data set, and there is none yet: results/manifest.json "
                             "is written by `prove.py fill`. Run that first, then ceiling.")
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
    impossible = [r for r in steps if r.get("reportable") and L.reads_low(r)]
    # Render both BEFORE opening anything for writing. open(..., "w") empties
    # the file before the renderer runs, so a renderer that raises destroys the
    # previous run's table as well as failing to write this one. That is how
    # clean-room run 42 ended with a 0-byte suite.txt: two separate losses from
    # one bug.
    text, markdown = render_table(out) + "\n", render_markdown(out)
    # The dashboard over the whole suite. A panel full while the suite ran and
    # empty afterwards is a time range or a retention too short, and the
    # dashboard is looked at afterwards. Reported, never a reason to stop.
    dash = L.dashboard_report_line(out)
    if dash:
        out["dashboard"] = dash
        text += f"dashboard            : {dash}\n"
    try:
        named = load_json("section7-metrics.json") if os.path.exists(os.path.join(c.results, "section7-metrics.json")) else None
    except Exception:
        named = None
    if named is not None:
        line = L.section7_metrics_line(named.get("missing"))
        out["section7Metrics"] = named.get("missing")
        text += f"metric names         : {line}\n"
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

    # A step the configuration asks for with no counted number is not a pass,
    # however well the other step did (clean-room run 50: every 1-core pass
    # thrown out, 2->4 met, DONE said PASS).
    gaps = L.missing_steps(c.cases, out["table"], out["runs"])
    if gaps:
        out["reportVerdict"] = "step-missing"
        out["missingSteps"] = gaps
        save_json("suite.json", out)
        print("\nA STEP THE CLAIM NEEDS HAS NO NUMBER\n")
        for g in gaps:
            print(f"  {g['step'].replace('->', '→')} cores: not measured — {g['why']}.")
        # Not "met" when its low end is above the ideal: that says the smaller case
        # reads low (run 51 printed both about the same step).
        met = [s.replace("->", "→") for s in L.met_steps(steps)]
        if met:
            print(f"  {', '.join(met)} met the target, but the claim covers every step in cases, so it is not met.")
        print("  Fix what threw that case out, or take the case out of `cases` and claim only the steps")
        print("  you can measure. Either way it is a new claim, and it is said next to the numbers.\n")
        return 1
    # A step whose interval spans the target is not a shortfall: the passes
    # cannot tell met from missed. Said as such, and never sent to tuning.
    unsettled = [r for r in short if L.step_verdict(r) == "undecided"]
    short = [r for r in short if L.step_verdict(r) != "undecided"]
    if unsettled and not short:
        out["reportVerdict"] = "not-settled"
        out["unsettledSteps"] = [{"step": r["step"], "ratio": r["ratio"], "low": r["ratioLowCI"],
                                  "high": r["ratioHighCI"], "need": r["idealRatio"] * T["scalingFloor"],
                                  "pairs": len(r.get("adjacentPairs") or []),
                                  "range": L.settle_range(r)}
                                 for r in unsettled]
        save_json("suite.json", out)
        print("\nUNDECIDED\n")
        for u in out["unsettledSteps"]:
            for line in textwrap.wrap(f"{u['step'].replace('->', '→')} cores reads {u['ratio']:.2f}x. "
                                      f"{u['range']}, which spans the {u['need']:.2f}x target.", 86):
                print(f"  {line}")
        ran = out.get("settleCases") or 0
        for line in textwrap.wrap("That is not a shortfall: the passes cannot tell met from missed."
                                  + (f" The suite ran {ran} extra case(s) to settle it and it is still open."
                                     if ran else ""), 86):
            print(f"  {line}")
        print()
        print("  What to do: change nothing in the pipeline on this step. Report it as undecided,")
        print("  with the range above; that is a result. If the claim needs it decided, run")
        print("  `prove.py suite` again — a new suite, with its own passes and settling cases —")
        print("  or raise `passes` in pipeline.json first. With nobody to ask, stop here and report.\n")
        return 1
    if short:
        t = out["table"]
        need = 2 * T["scalingFloor"]
        for r in unsettled:
            print(f"  ({r['step']} cores is undecided: {r['ratioLowCI']:.2f}x to {r['ratioHighCI']:.2f}x "
                  f"spans the target.)")

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

def report_verdict(why, suite):
    """Pure. The DONE line for a chain that stopped at the report, from the
    report's own verdict and suite.json. What a person reads when the chain is
    over: "FAIL at report" said nothing about what happened."""
    suite = suite or {}
    if why == "no-result":
        return ("STOPPED at report: no scaling result — too many cases were "
                "thrown out to compare one core count with another")
    if why == "not-settled":
        return ("STOPPED at report: undecided — "
                + "; ".join(f"{u['step'].replace('->', '→')} reads {u['ratio']:.2f}x, and "
                            f"the range its {u['pairs']} pairs support, {u['low']:.2f}x "
                            f"to {u['high']:.2f}x, spans the {u['need']:.2f}x target"
                            for u in (suite.get("unsettledSteps") or []))
                + ". Report it as undecided and change nothing in the pipeline")
    if why == "claim-not-met":
        return "STOPPED at report: the table is good, the pipeline did not meet the target"
    if why == "step-missing":
        return ("STOPPED at report: a step the claim needs was not measured"
                + "".join(f" — {g['step'].replace('->', '→')}: {g['why']}"
                          for g in (suite.get("missingSteps") or [])))
    return "STOPPED at report: the table could not be reported"


def cmd_local_first():
    """GUARD: a run on a paid service starts only after the same app passed on
    the laptop, where it costs nothing: completeness, the tiny proof, and the
    same job SQL (platforms.local_first_reason)."""
    c = cfg()
    raw = c.raw.get("platform") if isinstance(c.raw.get("platform"), dict) else {}
    lp = raw.get("localPipeline")
    if lp and not os.path.isabs(lp):
        lp = os.path.join(c.root, lp)
    why = L.P.local_first_reason(lp, getattr(c.plat, "job_sql", None))
    if why:
        raise Refusal("rig", why)
    # GUARD: the cloud run must not write over the laptop evidence this gate
    # reads. Both configs sat in one folder and shared results/ (2026-10-06).
    why = L.P.shared_results_reason(lp, c.results)
    if why:
        raise Refusal("rig", why)
    # GUARD: on a paid service, only the cases a claimed step needs.
    why = L.P.unclaimed_cases_reason(c.cases, c.raw.get("claimSteps"), getattr(c.plat, "unit", "units"))
    if why:
        raise Refusal("rig", why)
    log(f"  the same app passed completeness and the tiny proof on the laptop ({lp})")
    return 0


def cmd_all(steps=None, results=None, dashboard_check=None, teardown=None):
    """The whole chain as one command. Run 11 spent 20 minutes of its 1.97 h in
    the gaps between commands an agent typed by hand, and wrote phases.log by
    hand; here the harness writes it, and DONE is the file to wait on.
    `results` is where phases.log, all.json and DONE go — the self-test passes
    its own directory so a fake chain never lands in the live one."""
    c = cfg()
    if steps is None and c.plat:
        # Before `up`: nothing is created, so nothing is paid for, until the
        # same app has passed locally.
        steps = [("local first", cmd_local_first)]
    steps = (steps or []) + ([] if steps and steps[0][0] != "local first" else
                             [("up", COMMANDS["up"]), ("preflight", cmd_preflight),
                              ("completeness", cmd_completeness), ("tinyproof", cmd_tinyproof),
                              ("fill", cmd_fill), ("suite", cmd_suite), ("report", cmd_report)])
    results = results or c.results
    os.makedirs(results, exist_ok=True)
    phases = os.path.join(results, "phases.log")
    done = os.path.join(results, "DONE")
    if os.path.exists(done):
        os.remove(done)
    # A fresh phases.log per chain, like DONE: appended across chains, a wait for
    # "phase=tinyproof end" matched the previous chain's line and returned at
    # once (clean-room run 51, S13). Every line is in harness.log as well.
    if os.path.exists(phases):
        os.remove(phases)
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
    say = {"local first": "checking the same app passed on the laptop first", "up": "starting the stack",
           "preflight": "preflight checks",
           "completeness": "proving nothing is lost, including after killing the pipeline mid-run",
           "tinyproof": "the tiny proof: every case end to end, and every guard broken on purpose",
           "fill": "filling the backlog — the long quiet one",
           "suite": "measuring the cases", "report": "writing the report"}
    # GUARD: a crash or an interrupt still reaches the teardown and DONE below.
    # The first full Confluent chain crashed in its report (a KeyError) after
    # every case was measured, never reached the teardown, and its stack sat
    # up for nine hours with nothing in DONE to say so (2026-10-06).
    import signal as _signal
    def _term(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    try:
        _old_term = _signal.signal(_signal.SIGTERM, _term)
    except ValueError:                      # not the main thread
        _old_term = None
    current = None
    try:
        for i, (name, fn) in enumerate(steps):
            t0 = time.time()
            mark(f"phase={name} start")
            if results == c.results:
                L.progress(f"step {i + 1} of {len(steps)}: {say.get(name, name)}", pct=i / len(steps))
            current = name
            crash = None
            try:
                rc = fn()
            except Refusal as e:
                log(f"STOPPED ({e.scope}): {e.msg}")
                rc = 1
            except Exception as e:
                import traceback as _tb
                crash = f"the harness itself stopped with an error: {type(e).__name__}: {e}"
                log(f"STOPPED: {crash}\n{_tb.format_exc()}")
                rc = 1
            finally:
                if name in ("completeness", "tinyproof", "suite"):
                    try:
                        L.stop_sampler(); L.stop_tm()
                    except Exception:
                        pass
            # Every dashboard panel shows data through Grafana, checked as soon as
            # a job has run -- minutes in, not after the hour-long suite.
            # `dashboard_check` is the self-test's way in; the live chain asks Grafana.
            stop_why = None
            if name == "completeness" and not rc and (results == c.results or dashboard_check):
                stop_why = (dashboard_check or (lambda since: L.dashboard_stop_reason(
                    L.dashboard_here(), since, time.time())))(t0)
                if stop_why:
                    log(f"STOPPED: {stop_why}")
                    rc = 1
            # Every series section 7 names, asked of the image actually running.
            # Reported, never a reason to stop: a build may not use every panel.
            if name == "completeness" and not rc and results == c.results:
                try:
                    missing = L.section7_metrics_missing(L.dashboard_here(), t0, time.time())
                    if missing is not None:
                        save_json("section7-metrics.json", {"missing": missing,
                                                            "checked": list(L.SECTION7_METRICS)})
                        log(f"  {L.section7_metrics_line(missing)}")
                except Exception as e:
                    log(f"  the series section 7 names were not checked: {e}")
            if L.QUICK != quick0:
                log(f"phase {name} left the quick flag {L.QUICK} (it was {quick0}); restoring")
                L.QUICK = quick0
                rc = rc or 1
            if name == "report" and rc and not crash:
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
                verdict = (f"STOPPED at {name}: {crash}" if crash else
                           report_verdict(why, load_json("suite.json") if why else None)
                           if name == "report" else f"STOPPED at {name}" + (f": {stop_why}" if stop_why else ""))
                break
    except (KeyboardInterrupt, SystemExit) as e:
        verdict = f"STOPPED: interrupted during {current} ({e or type(e).__name__})"
        mark(f"phase={current} end rc=1 interrupted")
    finally:
        if _old_term is not None:
            _signal.signal(_signal.SIGTERM, _old_term)

    # GUARD: a chain on a paid service never leaves its stack running, however
    # it ends. The first full Confluent Cloud chain (2026-10-06) stopped at
    # preflight and left its cluster and compute pool up. On the laptop the
    # stack stays up, as it always has. `teardown` is the self-test's way in.
    teardown = teardown or ((lambda: L.stack_down()) if c.plat else None)
    if teardown and any(s["step"] == "up" for s in out["steps"]):
        try:
            teardown()
            mark("phase=down end rc=0: a stack on a paid service is not left running")
        except Exception as e:
            mark(f"phase=down end rc=1: {e}")
            verdict += f"; the stack may still be running, check the service: {e}"
    out["verdict"] = verdict
    out["seconds"] = round(time.time() - t_all, 1)
    save_all()
    mark(f"phase=all end {verdict} {out['seconds']/60:.1f} min")
    # PROGRESS.txt used to stay at "86% ... writing the report" after DONE
    # appeared (runs 48 and 49).
    # Only for the live chain: the guard self-test's fake chain wrote "finished:
    # STOPPED at c" here in the middle of clean-room run 52's real tiny proof,
    # an hour before the suite, for anyone relaying the run from this file.
    if results == c.results:
        try:
            L.progress(f"finished: {verdict} {out['seconds']/60:.1f} min", pct=1.0)
        except Exception:
            pass
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
    # replay and selftest-pure read no stack, so they should not need a run
    # directory: run 49 found `prove.py replay` crashing anywhere else.
    if name in ("replay", "selftest-pure") and not os.environ.get("PIPELINE_JSON") \
            and not os.path.exists("pipeline.json"):
        os.environ["PIPELINE_JSON"] = os.path.join(L.HERE, "pipeline.example.json")
    if name not in ("replay",):
        cfg()  # validate pipeline.json first
    if name not in ("replay", "selftest-pure", "report"):
        rc = cmd_replay()
        if rc:
            print("not running: a threshold disagrees with the record")
            sys.exit(rc)
    # The user's own DOCKER_CONFIG wins; otherwise no credential helper.
    if "DOCKER_CONFIG" not in os.environ:
        os.environ["DOCKER_CONFIG"] = L.harness_docker_config()
    try:
        rc = COMMANDS[name]()
    except Refusal as e:
        print(f"STOPPED ({e.scope}): {e.msg}")
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
