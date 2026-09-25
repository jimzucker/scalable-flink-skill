#!/usr/bin/env python3
"""
scalable-flink-skill: the measurement harness.

This file is the skill's guards, as code. It is shipped with the skill and used
verbatim: an agent following the skill supplies a pipeline (see README.md) and
does NOT rewrite any of this. Every threshold below names the measurement it
was set from, and `prove.py replay` checks each one against the recorded runs
before it is allowed to refuse anything new.

Rules this file implements (one guard per rule; `prove.py selftest` breaks
every one on purpose, because a guard that has never fired is a guess):

  * the resource cap is read back from the container, never from the env var
  * parallelism == cap == allocated slots, all three read back from the engine
  * the job graph shape is read off the running plan and compared across cases
  * the component under test must be the constraint (cap consumption floors)
  * the job is torn down on every exit path, refused cases included
  * the window is anchored on the committed offset advancing, not wall clock
  * throughput comes from the transport (committed broker offsets), not Flink
  * CPU comes from the cumulative cgroup counter, not `docker stats`
  * the backlog must have a full checkpoint interval of headroom at close
  * two vantage points must agree
  * a case's passes spread past the ceiling: that case and its ratios are
    unreportable; the other cases still report

A refusal about the RIG stops the suite. A refusal about one case's DATA marks
that case and moves on.
"""

import base64
import calendar
import hashlib
import json
import os
import re
import shlex
import statistics
import tempfile
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))

# quick look: set by `prove.py <cmd> --quick`, and read in exactly one place —
# Cfg, to run each case once instead of the configured number. It must never
# reach build_table through this global: the first version did, and the replay
# then re-derived the *record* with minPasses bypassed (three recorded suites
# the record marks invalid would have been reported) while the live self-test's
# "a case measured only once" guard stopped firing. Both refusals were correct
# and both stopped a run (2026-09-05). Quickness is a property of one table, so
# build_table takes it as an argument; the record and the guards keep normal
# semantics whatever the flag says.
QUICK = False
QUICK_BANNER = ("two passes per case, not the configured number: enough for a spread, not enough to publish. A one-pass version of this table read 2->4 = 1.645 where the same build measured 1.910 with the memory it needed, and 1.539 where three passes read 1.678. Quote the spread with the ratio, or do not quote it.")

# ------------------------------------------------------------------ thresholds
#
# Each value names where it came from. `prove.py replay` re-derives the table
# for every recorded suite in record/ with these numbers and refuses to run if
# one of them would void a table the record marks valid.

T = {
    # constraint ownership — run 5 published a 2.82x from a worker at 94% of cap.
    # The baseline floor was 98% until 2026-09-04. Replayed by hand against every
    # baseline pass it had refused (the record does not carry per-pass cap
    # fractions, so `replay` cannot): four refusals at 95.9-97.4% (runs 11, 12,
    # rig Phase 3, incl. run 12's sentinel) carried the same rate as the accepted
    # passes (x0.97-x1.05); the two at 95.3% and 94.2% (Phase 1) were 31% high and
    # 24% low, which the spread guard refuses on its own. One floor for every
    # case; run 5's 94% stays out. The good/bad gap on record is 0.6 points at
    # n=6 — 95% is the existing floor the record does not contradict, not a
    # floor measured from noise.
    "capFloorBaseline": 0.95,
    "capFloorOther": 0.95,
    # the broker starved of page cache — measured 2026-09-05 by a one-variable
    # comparison on one rig, one build, one backlog: at a 2 GiB container limit
    # the broker hit that limit 310,423 times with 6.3M file-page refaults and
    # 649 MB of cache, and all three 4-core passes were refused at 93.1-93.8%
    # of cap; at 4 GiB, with nothing else changed, the same case held 99.6-100.1%
    # of cap at 651,653 rec/s (mean of 3) and the cache grew to 2.14 GB. Across
    # the seven cases measured at 4 GiB the limit was hit zero times inside a
    # window, so any hit at all is outside the measured noise.
    "brokerLimitHits": 0,
    # A worker at its cap is the constraint whatever the broker is doing, and
    # "at its cap" is the same 0.95 every other guard uses. This was 0.99 and
    # marked UNSETTLED; runs 46-48 settled it. Replayed on 2026-09-24 against
    # every recorded suite: the 99% rule threw out twelve passes at 95-99% of
    # cap (runs 32, 46, 47, 48) and every one sat -4.7% to +5.2% of the passes
    # kept beside it -- inside the case's own noise, never slow. Limit hits do
    # not separate the outcomes (run 23: 9,437 hits at 99.6% of cap, no rate
    # effect). The broker that really was too small -- 2 GiB, 13% slow -- is
    # stopped at setup by the page-cache floor (broker_cache_floor_mib) before
    # a single pass is measured, which is where that check belongs.
    "brokerHitsCapExempt": 0.95,
    # external boundary: a starved source idles (run 5: the broker was the ceiling
    # at 43% back-pressure with the TM under cap). Measured 2026-09-04: at-cap
    # 2-core cases idle 8.1-16.8% at the same throughput (14 cases, sd 2.3%), and
    # the one recorded broker-constrained step (ceiling run, TM at 91.7%) idled
    # 16.7% — idle alone does not separate the two; the cap floor does. Ceiling:
    # max at-cap idle + one sd = 19%. 15% was a round number inside the band.
    "sourceIdleCeil": 0.20,
    # two vantage points: 0.5% measured after anchoring on commit boundaries (run 7);
    # 5% is ten times that
    "vantageTol": 0.05,
    # pass-to-pass spread. Measured over 13 suites in runs 9-10: 2- and 4-core
    # cases 0.1%-15.8%, one-core cases up to 42%, one broken suite at 86%.
    # 20% sits above the whole valid band and below every outlier. The 10% it
    # replaces was set below the band and voided six valid tables in run 9.
    "spreadCeil": 0.20,
    "minPasses": 2,
    # the claim, as opposed to the measurement. A table can be beyond reproach
    # and still say the pipeline does not scale: the demo's own job reads 1.99x
    # from 2 to 4 cores on this host, so a step that doubles the resource and
    # returns less than 95% of that is a result about the pipeline, not noise.
    # Set from the demo's measured 1.99x and the +-3% a two-pass ratio carries.
    # The claim: each doubling returns at least this fraction of linear, judged on
    # the low end of the step's range. 0.95 (1.90x) until 2026-09-24, taken from
    # the published demo's 1.99x less a two-pass ratio's noise; lowered to 0.90
    # (1.80x) by the author's decision. A target, not a noise threshold: this
    # Mac's own cores return 1.48-1.82x on memory-heavy work over the same steps,
    # and replayed against the 22 recorded steps it passes 7 where 1.90 passed 3.
    "scalingFloor": 0.90,
    # Memory is not capped by default: this repository's own demo caps none and
    # reads 1.99x, while every memory cap we chose starved something. What the
    # claim needs is that CPU is the constraint, so instead of fixing memory's
    # size the harness checks it was not the constraint. Measured across 14
    # recorded runs: cases that behaved sat at 0.7-9.6% of capacity in GC, and
    # every case above 11% came with a distorted result -- run 21's 1-core case
    # at 26.4%, run 25's at 15.5% with a 14.3% spread and -14.5% sentinel drift,
    # run 18's at 12.9% with a 7.5% spread. Ceiling: 11%, just above the worst
    # well-behaved case on record.
    # halved with the double count: the cases that behaved sat at 0.35-4.8% of
    # capacity and every distorted one was above 6% -- run 21's 1-core case at
    # 13.2%, run 25's at 7.75%, run 18's at 6.45%. Ceiling: 5.5%, just above the
    # worst well-behaved case on record.
    "gcCeil": 0.055,
    # when a step has one pair and no spread of its own, assume the wander this
    # rig showed over an hour on an unchanged build: sd 2.8% across 13 pairs
    "ratioSdFallback": 0.028,
    # --quick measures each case twice, not once. Measured 2026-09-06: a
    # one-pass ratio compounds the error of both cases and wandered 8.3% low on
    # run 18's build (1.539x against 1.678x from three passes) and 3.2% low on
    # run 16's (1.809x against 1.752x). Two passes of the same build reproduce
    # the three-pass ratio to 0.2% (1.675x against 1.678x), give every case a
    # spread, and cost about twelve minutes.
    "quickPasses": 2,
    # warm-up: a flat least-squares slope through four commit intervals, and the
    # scatter bounded too — run 10 suite B admitted a still-accelerating ramp on
    # slope alone. 10% scatter was unsatisfiable at 4 cores (checkpoint jitter is
    # 13-15% and stationary); 20% still rejects the 40% ramp.
    "warmupIntervals": 4,
    "warmupFlatTol": 0.10,
    "warmupScatterTol": 0.20,
    "warmupMinS": 90.0,        # run 10: the ramp ran 46-136 s; outlast it
    # The same figure, under a name the tiny proof does not override. The tiny
    # proof lowers warmupMinS to 20 s so it stays cheap, and sizing runs inside
    # it -- so the backlog was sized on a warm-up that only had to reach 20 s
    # while the suite it sizes for will not start measuring before 90 s.
    "suiteWarmupMinS": 90.0,
    "sizingWindowS": 30.0,
    "warmupMaxS": 240.0,
    # window: >=3 commit boundaries is the rule; six 10 s boundaries average the
    # checkpoint jitter (run 10). Windows of 20 s, 45 s and 60 s were tried in
    # run 9 and did not move the spread.
    "minBoundaries": 6,
    "minWindowS": 60.0,
    "windowMaxS": 300.0,
    # the reporter samples busy/idle/back-pressured on this interval; a window
    # must hold at least three samples or the external-boundary guard is a guess
    # (harness live test: a 6 s window against a 10 s reporter held none)
    "reporterS": 10,
    "minBpSamples": 3,
    # the tiny proof: superlinear has been an artefact every time (run 8: 3.73x
    # from a chained baseline)
    "tinyRatioLo": 1.5,
    "tinyRatioHi": 2.5,
    # a full disk takes the shell down with it (run 5)
    "diskFloorBytes": 20e9,
    # retention on undrained sink topics is a periodic sweep, not a bound (run 5:
    # 27 GB against a 4 GB cap between sweeps); 2 GB/partition let run 10's
    # sinks run without a sweep inside a window
    # A fill that stops writing. Chosen, not measured, and set well clear of any
    # healthy fill: run 48's generator spent 6.7 minutes writing its own price
    # feed before its first order, and healthy fills here write 0.6-3.9 million
    # records a second, so five minutes with no growth is not a pause. Run 48's
    # generator died 7.5 s in and was waited on for 29 minutes (finding F8).
    "fillStartS": 600.0,
    "fillStallS": 300.0,
    "sinkRetentionBytes": 2 * 1024 ** 3,
}


# ---------------------------------------------------------------------- config

class Ceiling(Exception):
    """The component under test stopped being the constraint. Not a broken
    measurement: the case is measured and reported as where scaling stops, and
    is excluded from the ratios, because a ratio built on it is not a statement
    about the component. The demo's own 8-unit row -- 4.98 of 8 cores, 1.18x --
    is exactly this, and the harness used to delete such cases instead of
    saying what they show."""

    def __init__(self, msg, rec=None):
        super().__init__(msg)
        self.msg = msg
        self.rec = rec or {}


class Refusal(Exception):
    def __init__(self, scope, msg):
        super().__init__(msg)
        self.scope = scope   # "rig" or "case"
        self.msg = msg


class CaseRefused(Exception):
    def __init__(self, rec, refusal):
        super().__init__(refusal.msg)
        self.rec = rec
        self.refusal = refusal


class Cfg:
    """pipeline.json, resolved. Everything the harness needs to know about the
    pipeline under test comes from here; everything about *measurement* does not."""

    def __init__(self, path):
        self.path = os.path.abspath(path)
        self.root = os.path.dirname(self.path)
        with open(self.path) as f:
            c = json.load(f)
        self.raw = c
        req = ["project", "topics", "partitions", "checkpointMs",
               "job", "generator", "verifier", "cases", "baseline", "passes", "backlog",
               "caps", "images", "jdk", "axis", "apiLevel", "guarantee"]
        missing = [k for k in req if k not in c]
        if missing:
            raise Refusal("rig", f"pipeline.json is missing {missing}")
        p = c["project"]
        if not re.fullmatch(r"[a-z][a-z0-9]{1,15}", p):
            raise Refusal("rig", f"project must be a short lowercase token, got {p!r}")
        self.project = p
        self.results = os.path.join(self.root, "results")
        self.stack_dir = os.path.join(self.root, "stack")
        self.net = f"{p}_default"
        self.kafka = f"{p}-kafka"
        self.jm = f"{p}-jm"
        self.tm = f"{p}-tm"
        self.sampler = f"{p}-sampler"
        self.ckpt_vol = f"{p}_ckpt"
        self.kafka_vol = f"{p}_kafkadata"
        self.boot_int = f"{self.kafka}:9092"
        port = int(c.get("ports", {}).get("kafka", 19092))
        self.kafka_port = port
        self.rest_port = int(c.get("ports", {}).get("rest", 18081))
        self.boot_ext = f"localhost:{port}"
        self.rest = f"http://localhost:{self.rest_port}"
        self.topic_in = c["topics"]["in"]
        # The same name, kept where nothing reassigns it. The tiny proof points
        # topic_in at its own small topic while it measures, and the disk
        # projection has to know which topic the suite will actually drain.
        self.suite_topic_in = c["topics"]["in"]
        self.topics_out = list(c["topics"]["out"])
        self.partitions = int(c["partitions"])
        # How the harness gets its SECOND measurement of how much input the
        # pipeline has swallowed. The first is always the committed offsets on
        # the input topic; the second has to be independent of it, or a stuck
        # consumer group reads as a fast pipeline.
        #
        # The default derives it from the outputs -- their rows divided by a
        # constant fan-out. That works for a pipeline whose outputs grow with
        # the input, and not at all for one whose outputs are per window: an
        # hourly average per location emits the same number of rows whether it
        # read a thousand readings an hour or a million. Such a pipeline has no
        # constant fan-out anywhere, and until this existed the harness simply
        # could not measure one.
        #
        # So the second vantage is delegated, exactly as correctness already is
        # to verifier.cmd: the harness does not know how to read progress out
        # of an arbitrary output, and the person who wrote the pipeline does.
        self.out_per_in = float(c["outputsPerInput"]) if c.get("outputsPerInput") is not None else None
        sv = dict(c.get("secondVantage") or {})
        self.vantage_mode = sv.get("mode") or ("constantFanOut" if self.out_per_in is not None else None)
        self.vantage_cmd = sv.get("cmd")
        if self.vantage_mode not in ("constantFanOut", "command"):
            raise Refusal("rig", "pipeline.json says nothing about how to measure the pipeline a second "
                                 "way. Either set outputsPerInput, for a pipeline whose outputs grow by "
                                 "a constant multiple of the input, or set secondVantage to "
                                 '{"mode": "command", "cmd": "..."} for one whose outputs do not -- a '
                                 "windowed aggregate, say. One measurement is not a measurement: the "
                                 "committed offsets alone cannot tell a fast pipeline from a stuck "
                                 "consumer group.")
        if self.vantage_mode == "constantFanOut":
            if self.out_per_in is None:
                raise Refusal("rig", "secondVantage mode constantFanOut needs outputsPerInput")
            if not c["topics"]["out"]:
                raise Refusal("rig", "secondVantage mode constantFanOut needs at least one topic in "
                                     "topics.out to count. A pipeline with no output that grows with "
                                     'its input wants {"mode": "command"} instead.')
        elif not self.vantage_cmd:
            raise Refusal("rig", 'secondVantage mode command needs a cmd that prints '
                                 '{"inputRecordsProcessed": N}')
        self.ckpt_ms = int(c["checkpointMs"])
        self.ckpt_s = self.ckpt_ms / 1000.0
        # How long completeness keeps the job running after the last input is
        # committed. A checkpoint interval and two seconds by default: enough for
        # a throttle no slower than the checkpoint (runs 48-50's 10 s market value
        # passed with 2 s to spare). Set it for a slower throttled output.
        self.settle_s = float(c.get("settleS") or (self.ckpt_s + 2))
        self.jar = os.path.join(self.root, c["job"]["jar"])
        self.jar_dir = os.path.dirname(self.jar)
        self.jar_in_ctr = "/jobs/" + os.path.basename(self.jar)
        self.main_class = c["job"]["mainClass"]
        self.job_args = c["job"]["args"]
        self.source_match = c["job"].get("sourceVertexMatch", "Source")
        self.gen_cmd = c["generator"]["cmd"]
        self.manifest_cmd = c["generator"].get("manifestCmd")
        self.count_field = c["generator"].get("manifestCountField", "count")
        self.verify_cmd = c["verifier"]["cmd"]
        self.cases = [int(x) for x in c["cases"]]
        self.baseline = int(c["baseline"])
        self.passes = int(c["passes"])
        self.backlog = int(c["backlog"]["count"])
        self.seed = int(c["backlog"].get("seed", 1))
        self.small = int(c["backlog"].get("smallCount", 2_000_000))
        self.tiny = int(c["backlog"].get("tinyCount", 8_000_000))
        self.kill_frac = float(c["backlog"].get("killAtFraction", 0.35))
        caps = c["caps"]
        self.kafka_cap = float(caps.get("kafka", 2.5))
        self.jm_cap = float(caps.get("jobmanager", 0.5))
        # Worker memory is per subtask, not per container. A flat figure gives
        # the 4-core case a quarter of it each where the 2-core case had a half,
        # and the largest case is then measuring memory pressure rather than
        # cores. Measured 2026-09-07 on run 18's build, cap == parallelism,
        # interleaved: at a flat 2048m, 2c read 558,059 and 4c 917,807 (2->4 =
        # 1.645, GC 9.3% at four cores); with 5g, 2c read 549,380 -- unchanged --
        # and 4c 1,049,130 (2->4 = 1.910, GC 2.3%). The fourth core was starved,
        # not slow.
        # Flink's process size also carries fixed overheads -- metaspace, JVM
        # overhead, the network buffer floor -- that do not shrink with cores.
        # Scaling the whole figure by cores therefore starves the smallest case:
        # measured 2026-09-07 on the rig, 1280m per core read GC 17.4% at one
        # core against 3.4% at two and 1.1% at four. The base term covers the
        # fixed part; only the rest is per subtask.
        # Two different studies, and the table must say which it is.
        # "does this component scale" holds everything else constant and varies
        # only cores and parallelism -- the ratio is then a property of the
        # component. "what is the best configuration at each size" tunes each
        # case, and the ratio is best-at-2 against best-at-4, which is a
        # capacity curve an operator buys against and not a scaling proof.
        # perCase declares the second up front, before any number is seen,
        # because tuning after the result turns a curve into a story. The
        # demo in this repository avoids the question by capping no memory at
        # all, so memory is never its constraint.
        self.per_case = {int(k): v for k, v in (c.get("perCase") or {}).items()}
        for n in self.per_case:
            if n not in self.cases:
                raise Refusal("rig", f"perCase names case {n}, which is not in cases {self.cases}")
        self.tm_mem_base = caps.get("tmMemoryBase", "0m")
        self.tm_mem_per_core = caps.get("tmMemoryPerCore")
        self.tm_mem_limit_per_core = caps.get("tmMemoryLimitPerCore")
        self.tm_mem = caps.get("tmMemory", "4096m")
        self.tm_mem_limit = caps.get("tmMemoryLimit", "6g")
        # Extra Flink settings, for the job manager and the worker alike. The
        # only reason this exists: section 7 asks for panels the harness cannot
        # feed, because the properties were hard-coded and forking is forbidden.
        # Clean-room run 32 spent about 50 minutes writing a metrics exporter
        # from outside the engine to get round it, and noted every run would
        # rewrite the same thing.
        # Which fields of the generator's manifest hold a keyed stage's key set.
        # The manifest already lists every key (section 4 computes the expected
        # answer from the input), so naming the fields is all it takes to find
        # out where Flink would put them -- and a small key set does not spread
        # over subtasks by itself. Clean-room run 36 had to write its own tool
        # and pick key names by hand to get an even layout.
        # Every topic the pipeline writes that the harness does not own -- the
        # throttled outputs, anything per interval rather than per input. The
        # harness cannot infer them (that is why they are outside topics.out)
        # and it cannot read a diagram, so the run declares them and the
        # completeness drain holds the job to the list. Clean-room runs 36 and
        # 37 both built ONE market-value sink where the business case asks for
        # two -- by symbol and by account/sub-account/symbol -- and every guard
        # passed, because nothing knew how many there should be.
        self.topics_also = list(c.get("topicsAlsoWritten") or [])
        # The design, as lists that can be diffed against the running job: the
        # operators the interview's answers imply, the topics read, the topics
        # written. Outputs default to every topic the pipeline writes.
        self.design = dict(c.get("design") or {})
        if self.topics_also and not self.design.get("outputs"):
            self.design["outputs"] = list(self.topics_out) + list(self.topics_also)
        self.key_sets = {str(k): str(v) for k, v in (c.get("keySets") or {}).items()}
        # Environment for the engine's containers, job manager and task
        # manager alike. Section 7 told runs to set a metrics reporter through
        # flinkProperties, but on flink:1.20.1 the Prometheus reporter jar sits
        # in /opt/flink/opt and only reaches the classpath through the image's
        # own ENABLE_BUILT_IN_PLUGINS -- which nothing could set, because the
        # harness passed exactly one variable and forking it is forbidden.
        # Clean-room run 42 spent about forty minutes writing a REST exporter,
        # which is the cost section 7 says run 32 already paid once.
        self.flink_env = c.get("flinkEnv") or {}
        self.flink_props = c.get("flinkProperties") or {}
        if isinstance(self.flink_props, str):
            self.flink_props = dict(
                (k.strip(), v.strip())
                for k, _, v in (ln.partition(":") for ln in self.flink_props.splitlines() if ln.strip()))
        self.flink_props = pin_collector(self.flink_props)
        # What the harness sets for the measurement is not open for discussion:
        # slots, parallelism, worker memory and the checkpoint store decide what
        # is being measured, and the slf4j reporter is where busy, idle and
        # back-pressure are read from.
        reserved = ("taskmanager.numberOfTaskSlots", "parallelism.default",
                    "taskmanager.memory.process.size", "state.checkpoint-storage",
                    "state.checkpoints.dir", "jobmanager.rpc.address")
        for k in self.flink_props:
            if k in reserved or k.startswith("metrics.reporter.slf4j."):
                raise Refusal("rig", f"flinkProperties sets {k}, which the harness sets for the "
                                     f"measurement. Task slots, parallelism, memory and the checkpoint "
                                     f"store decide what is being measured, and the slf4j reporter is "
                                     f"where busy, idle and back-pressure are read from. Add settings, "
                                     f"do not replace these.")
        self.kafka_mem = caps.get("kafkaMemory", "4g")
        self.kafka_heap = caps.get("kafkaHeap", "3G")
        self.flink_img = c["images"]["flink"]
        self.kafka_img = c["images"]["kafka"]
        # Where the broker image keeps its jars. The harness mines the image for a
        # kafka-clients jar to compile the offset sampler against, and runs the
        # sampler inside that image, so the path moves with the vendor:
        # apache/kafka keeps them in /opt/kafka/libs, confluentinc/cp-kafka in
        # /usr/share/java/kafka. Wrong path refuses rather than measuring badly.
        self.kafka_libs = c["images"].get("kafkaLibs", default_kafka_libs(self.kafka_img)).rstrip("/")
        # deterministic 22-char base64url id, stable for this project
        self.cluster_id = base64.urlsafe_b64encode(
            hashlib.sha256(self.project.encode()).digest()[:16]).decode().rstrip("=")
        self.jdk = c["jdk"]
        self.java = os.path.join(self.jdk, "bin", "java")
        self.axis = c["axis"]
        self.api_level = c["apiLevel"]
        self.guarantee = c["guarantee"]
        self.log_path = os.path.join(self.results, "harness.log")
        # Three ways to run: no worker memory settings at all (the default, and
        # what the demo does -- memory can never be the thing that runs out),
        # tmMemoryPerCore so every subtask gets the same, or perCase. A flat
        # tmMemory across more than one case is still refused: it divides across
        # each case's subtasks, which cost 14% at four cores before #62 found it.
        if (caps.get("tmMemory") and self.tm_mem_per_core is None
                and len(set(self.cases)) > 1
                and not all(n in self.per_case for n in self.cases)):
            raise Refusal("rig", "caps.tmMemoryPerCore is not set. One flat memory figure gets split "
                                 f"among each case's subtasks, so cases {self.cases} would each run with a "
                                 "different amount of memory per subtask and could not be compared. "
                                 "Measured: a flat 2048m read 2->4 = 1.645 where per-core memory read 1.910.")
        floor = broker_cache_floor_mib()
        total, heap = mib(self.kafka_mem), mib(self.kafka_heap)
        if floor and total and heap and (total - heap) < floor:
            raise Refusal("rig", f"caps.kafkaMemory {self.kafka_mem} minus caps.kafkaHeap "
                                 f"{self.kafka_heap} leaves Kafka {(total - heap) / 1024:.2f} GB to cache "
                                 f"the backlog with. Every configuration on record that produced a usable "
                                 f"table left it at least {floor / 1024:.2f} GB. Below that the broker reads "
                                 f"the backlog back off disk and becomes the constraint instead of the "
                                 f"cores, and the cases come back as ceilings. Raise kafkaMemory to about "
                                 f"{int((floor + heap + 255) / 256) * 256:.0f}m, or lower kafkaHeap.")
        if self.baseline not in self.cases:
            raise Refusal("rig", f"baseline {self.baseline} is not one of the cases {self.cases}")
        if QUICK:
            # quick look: one pass per case, still followed by the sentinel, so
            # the baseline is measured twice and drift is visible. Every per-case
            # guard stays live; what is lost is the spread, and with it the right
            # to publish. Replayed against the record 2026-09-04: single passes of
            # the recorded suites read 2.039-2.273x where the suite reported
            # 2.154x (run 12, 2->4) and 1.837-1.859x against 1.850x (run 13) —
            # a one-pass number lands anywhere in a band wider than the accept
            # line, which is why this mode marks its table unpublishable.
            self.passes = T["quickPasses"]
        elif self.passes < T["minPasses"]:
            raise Refusal("rig", f"passes must be >= {T['minPasses']} (every case at least twice)")

    def fmt(self, s, **kw):
        """Fill a command template from pipeline.json."""
        d = dict(java=self.java, jar=self.jar, bootstrap=self.boot_int, bootstrapExt=self.boot_ext,
                 partitions=self.partitions, ckptMs=self.ckpt_ms, seed=self.seed,
                 **{"in": self.topic_in})
        for i, t in enumerate(self.topics_out):
            d[f"out{i}"] = t
        d["outs"] = ",".join(self.topics_out)
        # The same courtesy for topicsAlsoWritten. Without it a run hand-types
        # the topic name into the job args, the verifier and the second-vantage
        # command, and a typo in any one of them is found at run time.
        for i, t in enumerate(self.topics_also):
            d[f"also{i}"] = t
        d["alsos"] = ",".join(self.topics_also)
        d.update(kw)
        return s.format(**d)


_CFG = None


def cfg():
    global _CFG
    if _CFG is None:
        path = os.environ.get("PIPELINE_JSON", "pipeline.json")
        _CFG = Cfg(path)
    return _CFG


# ---------------------------------------------------------------------- basics

def log(*a):
    c = cfg()
    line = " ".join(str(x) for x in a)
    print(f"[{time.strftime('%H:%M:%S')}] {line}", flush=True)
    os.makedirs(c.results, exist_ok=True)
    with open(c.log_path, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {line}\n")


def sh(cmd, check=True, timeout=600, input=None):
    """Never silence a command while you are still finding out whether it works."""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout,
                           input=input)
    except subprocess.TimeoutExpired:
        # A timeout used to come out as a raw traceback with no idea what to do
        # about it. Clean-room run 31 lost about 25 minutes to one: Docker's
        # credential helper hung, so every docker command sat there until the
        # timeout, and the traceback said only that a subprocess had expired.
        hint = ""
        if cmd.strip().startswith("docker"):
            # Name the usual cause and stop there. What to do about a machine's
            # docker credentials is that machine's owner's call, not the
            # harness's, and a benchmark has no business telling anyone to take
            # their credential store out of the path.
            hint = ("\nA docker command that hangs rather than failing is usually the credential "
                    "helper configured in docker's config.json.")
        raise Refusal("rig", f"this command was still running after {timeout}s and was given up on:"
                             f"\n  {cmd}\nIt did not fail, it never answered.{hint}")
    if r.returncode != 0 and check:
        raise Refusal("rig", f"command failed ({r.returncode}): {cmd}\n"
                             f"stdout: {r.stdout[-2000:]}\nstderr: {r.stderr[-2000:]}")
    return r


def rest(path, timeout=15):
    with urllib.request.urlopen(cfg().rest + path, timeout=timeout) as r:
        return json.loads(r.read().decode())


def rest_patch(path, timeout=30):
    req = urllib.request.Request(cfg().rest + path, method="PATCH")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


_KAFKA_TOOLS = {}


def pin_collector(props):
    """Every case runs G1. The skill requires it (2026-09-24, the author's call).

    Left to itself Java picks the Serial collector in a one-CPU container and
    G1 at two or more, so the one-core baseline runs a different program from
    every case above it. Measured 2026-09-24 on the published demo's own build
    at 4,096 keys: the 1-core case ran Copy + MarkSweepCompact (Serial) and the
    rest G1, and 1->2 read 2.01x; with G1 on every case, nothing else changed,
    1->2 read 1.91x. The published 2.06x came from the first configuration.
    No collector named: G1 is added. Another one named: the run is stopped."""
    props = dict(props)
    both = " ".join(str(props.get(k, "")) for k in ("env.java.opts.taskmanager", "env.java.opts.all"))
    named = re.findall(r"-XX:\+Use(\w*GC)\b", both)
    other = [n for n in named if n != "G1GC"]
    if other:
        raise Refusal("rig", f"flinkProperties asks for the {', '.join(other)} garbage collector. The skill "
                             f"measures on G1 only: a different collector is a different program, and Java's "
                             f"own pick differs between one core and two, which made one published step read "
                             f"2.01x where G1 on every case reads 1.91x. Remove it; the harness sets G1 itself.")
    if not named:
        cur = str(props.get("env.java.opts.taskmanager", "")).strip()
        props["env.java.opts.taskmanager"] = (cur + " -XX:+UseG1GC").strip()
    return props


def not_g1(gc_names):
    """The collectors a pass reported that are not G1, without the 'All' total."""
    return sorted(n for n in (gc_names or []) if n != "All" and not n.startswith("G1 "))


def case_collectors(runs):
    """The garbage collectors a case's passes reported, without the 'All' total."""
    return frozenset(n for r in runs for n in (r.get("gcNames") or []) if n != "All")


def default_kafka_libs(image):
    """Where a broker image keeps its jars, when pipeline.json does not say.
    Naming the image is then enough: a config that set images.kafka to
    confluentinc/cp-kafka and left kafkaLibs out used to get Apache's path."""
    return "/usr/share/java/kafka" if "confluentinc/" in (image or "") else "/opt/kafka/libs"


def vendor_only_classes(names):
    """Classes in a jar that only one Kafka vendor provides: Confluent's own
    client libraries (Schema Registry serializers and the like) live under
    io/confluent/. A pipeline built without them runs against Apache Kafka or
    Confluent with nothing changed but the broker image in pipeline.json."""
    return sorted(n for n in names if n.startswith("io/confluent/") and n.endswith(".class"))


def kafka_tool_path(found):
    """Where the broker image keeps its tools, from where kafka-topics was found.

    apache/kafka ships /opt/kafka/bin/kafka-topics.sh; confluentinc/cp-kafka ships
    /usr/bin/kafka-topics, with no .sh. Every command used to be sent to the
    first, so a Confluent broker came up healthy and the harness still stopped
    with "kafka never answered" (checked on cp-kafka:7.7.0, 2026-09-24).
    Returns (directory, suffix), or None if nothing was found."""
    found = (found or "").strip().splitlines()
    if not found or not found[0].startswith("/"):
        return None
    path = found[0]
    stem = path.rsplit("/", 1)[-1]
    if stem not in ("kafka-topics.sh", "kafka-topics"):
        return None
    return path.rsplit("/", 1)[0], (".sh" if stem.endswith(".sh") else "")


def kafka(args, check=True, timeout=300):
    """Run one of the broker's own command-line tools inside its container.
    args start with the tool's Apache name (kafka-topics.sh ...); the name is
    mapped to wherever this image keeps it."""
    c = cfg()
    if c.kafka not in _KAFKA_TOOLS:
        r = sh(f"docker exec {c.kafka} sh -c 'command -v /opt/kafka/bin/kafka-topics.sh "
               f"|| command -v kafka-topics.sh || command -v kafka-topics'", check=False)
        where = kafka_tool_path(r.stdout) if r.returncode == 0 else None
        if where:
            _KAFKA_TOOLS[c.kafka] = where
    directory, suffix = _KAFKA_TOOLS.get(c.kafka, ("/opt/kafka/bin", ".sh"))
    tool, _, rest = args.partition(" ")
    tool = tool[:-3] if tool.endswith(".sh") else tool
    return sh(f"docker exec {c.kafka} {directory}/{tool}{suffix} {rest}", check=check, timeout=timeout)


def host_free_bytes():
    r = sh(f"df -k {cfg().root} | tail -1")   # the HOST filesystem the results live on
    return int(r.stdout.split()[3]) * 1024


def partition_dirs(names, topics):
    """The broker's partition directories that belong to these topics, exactly.

    Kafka names each one <topic>-<partition number>. Matching <topic>-* also
    takes every longer topic that starts with the same name: clean-room run 48
    had "12.5 GB already on the broker" for a suite topic `orders` that had not
    been filled yet, because orders-tiny-* and orders-small-* matched it, and
    the disk check then under-counted what was still to write (finding F10)."""
    want = [re.compile(re.escape(t) + r"-\d+") for t in topics]
    return sorted(n for n in names if any(w.fullmatch(n) for w in want))


def topic_bytes(topics):
    """Bytes on the broker's disk for these topics, read from the log dir."""
    c = cfg()
    r = sh(f"docker exec {c.kafka} ls -1 /var/lib/kafka/data", check=False)
    if r.returncode != 0:
        raise Refusal("rig", f"could not list the broker's log directory: {r.stderr.strip()}")
    dirs = partition_dirs((r.stdout or "").split(), topics)
    if not dirs:
        raise Refusal("rig", f"the broker holds nothing for {', '.join(topics)}: no partition of it is "
                             f"on disk, so its size cannot be read.")
    # summed here, not in the container: the first live test died on awk quoting three shells deep
    pat = " ".join(f"/var/lib/kafka/data/{d}" for d in dirs)
    r = sh(f"docker exec {c.kafka} sh -c 'du -sk {pat}'", check=False)
    sizes = [ln.split()[0] for ln in r.stdout.splitlines() if ln.split()]
    if r.returncode != 0 or not sizes or not all(x.isdigit() for x in sizes):
        raise Refusal("rig", f"could not read the broker log dir size for {topics}: {r.stdout} {r.stderr}")
    return sum(int(x) for x in sizes) * 1024


def topic_exists(topic):
    """Whether the broker has this topic at all, without refusing if it does not."""
    r = kafka(f"kafka-topics.sh --bootstrap-server {cfg().boot_int} --list", check=False)
    return topic in (r.stdout or "").split()


def measured_bytes_per_record():
    """How many bytes one record of input actually takes on the broker, or None.

    Every topic the harness owns that already holds records, summed: bytes on
    disk divided by records. Returns None on a cold broker, where there is
    nothing to measure and the caller has to assume.

    This exists because the disk check assumed 120 bytes for every run whatever
    the record was. Clean-room run 43 measured 30 bytes -- 30,000,000 records in
    0.90 GB, compressed on the way in -- so the check asked for four times the
    space the run needed, stopped a configuration that would have fitted three
    times over, and the run shrank its data to get under a limit that did not
    exist.
    """
    c = cfg()
    best = None
    # Every input topic the harness owns, not only the suite's. The completeness
    # step fills <in>-small and the tiny proof fills <in>-tiny, and both are on
    # the broker long before the suite's own topic has a single record in it.
    # Clean-room run 44 had 25,000,000 readings sitting on <in>-small at a
    # measured 18 bytes each and this function still returned None, so the disk
    # check fell back to its 120-byte guess and projected 108 GB for a run that
    # wanted 16 -- 10 GB short of stopping a run that would have fitted six
    # times over.
    base = c.suite_topic_in
    names = [c.suite_topic_in, c.topic_in, f"{base}-tiny", f"{base}-small"]
    seen = set()
    for t in names:
        if t in seen:
            continue
        seen.add(t)
        if not t or not topic_exists(t):
            continue
        try:
            recs, _ = log_end(t)
            if not recs:
                continue
            b = topic_bytes([t])
            if b:
                per = b / recs
                # the largest of the measured topics, not the smallest: every
                # one of them holds the same record shape, and a projection
                # that understates the disk is the one that fills it.
                best = per if best is None else max(best, per)
        except Exception:
            continue
    return best


def topic_retention_bytes(topic):
    """retention.bytes as the broker has it, or None.

    The preflight row that asserts retention existed read back only on the
    topics the harness owns. A run declares the rest in topicsAlsoWritten and
    sets their retention itself, so this is how that promise gets checked
    rather than assumed.
    """
    r = kafka(f"kafka-configs.sh --bootstrap-server {cfg().boot_int} --entity-type topics "
              f"--entity-name {topic} --describe", check=False)
    m = re.search(r"retention\.bytes=(\d+)", r.stdout or "")
    return int(m.group(1)) if m else None


def log_end_if_any(topic):
    """log_end, but a topic that does not exist yet is nought rather than a
    failure. kafka-get-offsets.sh exits non-zero on a missing topic, which took
    the completeness step down on a cold stack before the fill had run."""
    try:
        return log_end(topic)[0]
    except Refusal:
        return 0
    except Exception:
        return 0


def topic_bytes_if_any(topics):
    """topic_bytes, but a topic that has not been created yet is nought bytes
    rather than a refusal. Used for the suite's input topic, which exists on a
    re-run after `fill` and does not exist before one."""
    try:
        return topic_bytes(topics)
    except Refusal:
        return 0


def volume_bytes(vol):
    r = sh(f"docker run --rm -v {vol}:/v alpine du -sk /v", check=False)
    if r.returncode != 0 or not r.stdout.split():
        raise Refusal("rig", f"could not read the size of volume {vol}: {r.stdout} {r.stderr}")
    return int(r.stdout.split()[0]) * 1024


def disk_verdict(free, in_bytes_per_rec, backlog, sink_bytes_per_in, partitions, n_out_topics, ckpt_bytes,
                 input_on_disk_bytes=0.0):
    """Pure. Project the suite's disk from the tiny proof's measured shape, before
    the fill. The sinks are bounded by retention, so the projection is too — run
    11 rebuilt its sink payload against a 75 GB figure that retention would have
    capped at 34 GB, and re-ran both gates for the new build.

    input_on_disk_bytes is what the suite's input topic already holds. Without
    it the projection asks for the whole backlog a second time while it is
    sitting on the very disk being measured, so the tiny proof could not be
    re-run after the fill: clean-room run 36 refused a suite that fitted, and
    every tuning lever then cost a delete and a re-fill, about 12 minutes of
    broker I/O each. Crediting it is right whichever way the topic goes — kept,
    and the fill does not write it again; deleted, and its bytes come back as
    free space."""
    inp = in_bytes_per_rec * backlog
    already = min(inp, max(0.0, input_on_disk_bytes))
    to_write = inp - already
    sink_unbounded = sink_bytes_per_in * backlog
    sink_cap = partitions * n_out_topics * T["sinkRetentionBytes"]
    sink = min(sink_unbounded, sink_cap)
    need = to_write + sink + ckpt_bytes + T["diskFloorBytes"]
    d = {"hostFreeBytes": int(free), "inputBytesPerRecord": round(in_bytes_per_rec, 1),
         "inputBytes": int(inp), "inputBytesOnDisk": int(already),
         "inputBytesToWrite": int(to_write), "sinkBytesPerInput": round(sink_bytes_per_in, 1),
         "sinkBytesUnbounded": int(sink_unbounded), "sinkRetentionCapBytes": int(sink_cap),
         "sinkBytes": int(sink), "sinkBoundedByRetention": sink_unbounded > sink_cap,
         "checkpointBytes": int(ckpt_bytes), "floorBytes": int(T["diskFloorBytes"]),
         "neededBytes": int(need), "fits": need <= free}
    if need > free:
        inp_note = (f"{to_write/1e9:.1f} GB of input still to write — the backlog needs "
                    f"{inp/1e9:.1f} GB and {already/1e9:.1f} GB of it is on the broker already — "
                    if already else f"{inp/1e9:.1f} GB of input, ")
        e = Refusal("rig", f"the suite needs {need/1e9:.1f} GB of disk and only {free/1e9:.1f} GB will be free "
                           f"once the tiny proof's topics are deleted. That is {inp_note}"
                           f"{sink/1e9:.1f} GB of sinks (capped at {sink_cap/1e9:.1f} GB), "
                           f"{ckpt_bytes/1e9:.1f} GB of checkpoints and {T['diskFloorBytes']/1e9:.0f} GB kept spare. "
                           f"Shrink the backlog or the record size now. After the suite is too late.")
        e.detail = d
        raise e
    return d


def size_backlog(rate_at_top, cores_top, ckpt_s, warmup_max_s=None, window_s=None, margin=1.5):
    """How many records the suite needs, from the rate the tiny proof measured
    at the largest case — instead of a guess made before anything ran.

    Every clean-room run from 15 to 20 lost an attempt to this guess: run 18
    sized for 500k rec/s against an actual 930k, run 20 drained a 50M backlog
    mid-window. The largest case must survive warm-up, the window, and close
    with more than one checkpoint interval of headroom left.
    """
    # the measured warm-up, not the ceiling: warmupMaxS is 240 s and real
    # warm-ups run 100-150 s, so the ceiling would demand a backlog three times
    # what any run has needed and fail the disk projection instead.
    # Floored at the SUITE's warm-up minimum, not the tiny proof's. This call
    # happens inside the tiny proof, where warmupMinS is 20 s; clean-room run 42
    # measured a 44.2 s warm-up there, was told 401,149,826 records would do,
    # and then had a suite pass consume 422,499,766 -- every one of its ten
    # suite warm-ups ran 90 s. It survived only because it ignored the number
    # and used its own worst-case arithmetic. Replayed against all 23 recorded
    # suites: every one already warmed up for 90 s or more, so none of them
    # moves. Sizing on the suite's 60 s window as well would have refused three
    # of them (run-11, run-17, phase3), which is why only this term changes.
    warmup = max(warmup_max_s if warmup_max_s is not None else T["warmupMinS"],
                 T.get("suiteWarmupMinS", 0.0))
    # An explicit number, not whatever minWindowS happens to be when this is
    # called. It is called from inside the tiny proof, which has already lowered
    # minWindowS to 30 s for its own use, so the sizing window was the tiny
    # proof's window by accident -- change the tiny proof's window and the suite
    # silently gets sized for a different one. Clean-room run 44 read the code
    # and filed this as sizing being understated by 107,000,000 records.
    #
    # It is 30 s and not the suite's own 60 s because the suite's window was
    # tried and the record refuses it. Replayed against all 24 recorded sizings:
    # at 30 s none of them is stopped; at 60 s four are -- run-11 (wants
    # 216,628,068 against 200,000,000), run-17 (225,451,973 against 192,000,000),
    # phase3 (215,934,416 against 200,000,000) and run-42 (718,247,376 against
    # 600,000,000) -- and all four produced tables we accepted. The x1.5 margin
    # absorbs the difference in practice.
    window = window_s if window_s is not None else T["sizingWindowS"]
    seconds = warmup + window + ckpt_s * 3          # headroom guard wants > 1 interval
    return int(rate_at_top * seconds * margin)


def sizing_case(cases):
    """The case the suite gets sized from: the biggest one that produced a rate.

    A ceiling case is measured and kept -- it is where scaling stopped, not a
    broken measurement -- so its rate is real and it is still the case the suite
    has to hold data for. The tiny proof used to reach for the biggest case by
    core count and index a dictionary that only held the accepted ones. Clean-room
    run 44's 4-core case hit the broker's memory limit, was logged as kept, and
    the next statement stopped the tiny proof with KeyError: 4 -- no
    tinyproof.json, no self-test, and the ceiling case's rate gone with it.
    """
    sized = {r["cores"]: r for r in cases if r.get("recordsPerSec")}
    if not sized:
        raise Refusal("rig", "not one case produced a rate, so there is nothing to size "
                             "the suite from. The messages above say why each case stopped.")
    return sized[max(sized)]


def disk_projection(tiny_topic, tiny_count, last_case_rec):
    """The measured shape: input bytes per record from the tiny topic, sink bytes
    per input from what the last tiny case wrote, checkpoint bytes from the volume."""
    c = cfg()
    tiny_bytes = topic_bytes([tiny_topic])
    in_bpr = tiny_bytes / tiny_count
    consumed = int(last_case_rec["close"]["committed"])
    # everything the pipeline writes, not only the topics with constant fan-out:
    # a windowed pipeline has none of those and was projecting no sink disk at all
    written = list(c.topics_out) + list(c.topics_also)
    sink_bytes = topic_bytes_if_any(written) if written else 0
    sink_bpi = sink_bytes / consumed if consumed else 0.0
    ckpt = volume_bytes(c.ckpt_vol)
    # The tiny topic and the tiny cases' sinks are still on disk when this runs and are
    # deleted before the suite fill; measured on the noise rig, deleting the 29.6 GB tiny
    # topic returned 29 GB to the host within three minutes (92.4 -> 121.3 GB free). The
    # first live projection counted them as used and refused a suite that fitted.
    host_free = host_free_bytes()
    reclaimable = tiny_bytes + sink_bytes
    # What the suite's own input topic already holds. Nought before the fill,
    # the whole backlog on a re-run after it -- which is the case that could not
    # be projected at all before (run 36, feedback 5).
    on_disk = topic_bytes_if_any([c.suite_topic_in])
    d = disk_verdict(host_free + reclaimable, in_bpr, c.backlog, sink_bpi, c.partitions,
                     max(1, len(written)), ckpt, input_on_disk_bytes=on_disk)
    d.update(hostFreeBytesNow=int(host_free), reclaimableBytes=int(reclaimable),
             measuredOn={"tinyTopicRecords": tiny_count, "tinyTopicBytes": int(tiny_bytes),
                         "sinkRecordsConsumed": consumed, "sinkBytesOnDisk": int(sink_bytes),
                         "suiteInputTopic": c.suite_topic_in, "suiteInputBytesOnDisk": int(on_disk)})
    return d


def build_hash():
    h = hashlib.sha256()
    with open(cfg().jar, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()[:16]


def save_json(name, obj):
    c = cfg()
    os.makedirs(c.results, exist_ok=True)
    p = os.path.join(c.results, name)
    with open(p, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    return p


def load_json(name):
    with open(os.path.join(cfg().results, name)) as f:
        return json.load(f)


# ----------------------------------------------------------------------- stack

def compose_text():
    c = cfg()
    return f"""name: {c.project}

services:
  kafka:
    image: {c.kafka_img}
    container_name: {c.kafka}
    hostname: {c.kafka}
    cpus: {c.kafka_cap}
    mem_limit: {c.kafka_mem}
    ports:
      - "{c.kafka_port}:{c.kafka_port}"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:{c.kafka_port}
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://{c.kafka}:9092,EXTERNAL://localhost:{c.kafka_port}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@{c.kafka}:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      # apache/kafka generates one; confluentinc/cp-kafka requires it. Fixed per
      # project so a restart rejoins its own log rather than refusing a new id.
      CLUSTER_ID: {c.cluster_id}
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_NUM_PARTITIONS: {c.partitions}
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_LOG_SEGMENT_BYTES: "268435456"
      KAFKA_NUM_IO_THREADS: 8
      KAFKA_NUM_NETWORK_THREADS: 5
      KAFKA_SOCKET_SEND_BUFFER_BYTES: "1048576"
      KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: "1048576"
      KAFKA_HEAP_OPTS: "-Xmx{c.kafka_heap} -Xms{c.kafka_heap}"
    volumes:
      - kafkadata:/var/lib/kafka/data

  jobmanager:
    image: {c.flink_img}
    container_name: {c.jm}
    hostname: {c.jm}
    command: jobmanager
    user: "0:0"
    cpus: {c.jm_cap}
    mem_limit: 2g
    depends_on:
      - kafka
    ports:
      - "{c.rest_port}:8081"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: {c.jm}
        jobmanager.memory.process.size: 1600m
        rest.address: 0.0.0.0
        rest.bind-address: 0.0.0.0
        state.checkpoint-storage: filesystem
        state.checkpoints.dir: file:///ckpt
        state.backend.type: hashmap
        parallelism.default: {c.baseline}
        heartbeat.timeout: 120000
""" + "".join(f"        {k}: {v}\n" for k, v in c.flink_props.items()) + "".join(
    f"      {k}: \"{v}\"\n" for k, v in c.flink_env.items()) + f"""
    volumes:
      - ckpt:/ckpt
      - {c.jar_dir}:/jobs:ro

""" + extra_services_text() + """
volumes:
  kafkadata:
  ckpt:
"""


def extra_services_text():
    """Services the pipeline adds to the stack -- a dashboard, an exporter.

    Section 7 asks for a dashboard, section 6 requires anything watching the
    stack to live in the compose file, and sections 1 and 10 forbid forking the
    harness. With no hook the three cancelled out: clean-room run 30 read all
    of them, concluded the dashboard could not be built, and skipped it. This
    is the hook.

    `extraServices` in pipeline.json is a map of service name to a compose
    service body, spliced in under the harness's own. Two rules, because the
    measurement depends on them:

      * the container name must start with the project prefix, or `down` will
        leave it behind and then refuse for a survivor it did not create;
      * give it a CPU cap. Anything sharing the cores under test changes the
        number being measured, which is the whole reason section 6 exists.

    They are recorded in the results header, so a reader knows something else
    was on the machine.
    """
    extra = cfg().raw.get("extraServices") or {}
    if not extra:
        return ""
    out = []
    for name, body in extra.items():
        out.append(f"  {name}:")
        for line in yaml_block(body, indent=4):
            out.append(line)
    return "\n" + "\n".join(out) + "\n"


def yaml_block(value, indent=0):
    """The little of YAML a compose service body needs, so the harness stays
    standard library only."""
    pad = " " * indent
    lines = []
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{pad}{k}:")
                lines.extend(yaml_block(v, indent + 2))
            else:
                lines.append(f"{pad}{k}: {v}")
    elif isinstance(value, list):
        for v in value:
            if isinstance(v, (dict, list)):
                lines.append(f"{pad}-")
                lines.extend(yaml_block(v, indent + 2))
            else:
                lines.append(f"{pad}- {v}")
    else:
        lines.append(f"{pad}{value}")
    return lines


def compose_path():
    c = cfg()
    os.makedirs(c.stack_dir, exist_ok=True)
    p = os.path.join(c.stack_dir, "compose.yml")
    with open(p, "w") as f:
        f.write(compose_text())
    return p


def stack_up():
    """Bring the stack up from cold. Idempotent. Asserts every effect."""
    c = cfg()
    save_json("volumes-before.json", dangling_anonymous_volumes())
    p = compose_path()
    sh(f"docker compose -f {p} up -d", timeout=900)
    # The flink entrypoint drops privileges to uid 9999; a named volume is
    # created root-owned, so the checkpoint coordinator cannot mkdir under it.
    sh(f"docker run --rm --user 0:0 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
       f"-c 'chown -R 9999:9999 /ckpt'")
    sh(f"docker run --rm --user 9999:9999 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
       f"-c 'mkdir -p /ckpt/.probe/shared && rmdir /ckpt/.probe/shared /ckpt/.probe'")
    for _ in range(120):
        try:
            rest("/overview")
            break
        except Exception:
            time.sleep(1)
    else:
        raise Refusal("rig", "job manager REST never came up")
    for _ in range(120):
        if kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --list", check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        raise Refusal("rig", "kafka never answered")
    build_sampler()
    log(f"stack up: {c.kafka} (cap {c.kafka_cap}), {c.jm} (cap {c.jm_cap}); "
        f"task manager is started per case")


def dangling_anonymous_volumes():
    """Anonymous volumes the engine image declares, left by any container removed
    without -v. The harness removes with -v; this catches the ones it did not."""
    r = sh("docker volume ls -q -f dangling=true", check=False)
    return [v for v in r.stdout.split() if re.fullmatch(r"[0-9a-f]{64}", v)]


def stack_down(trim=True):
    """Tear down everything this project started, and assert nothing survives."""
    c = cfg()
    for n in (c.sampler, c.tm, f"{c.project}-capprobe"):
        sh(f"docker rm -f -v {n}", check=False)
    p = os.path.join(c.stack_dir, "compose.yml")
    if os.path.exists(p):
        sh(f"docker compose -f {p} down -v --remove-orphans", check=False, timeout=600)
    for v in (c.ckpt_vol, c.kafka_vol):
        sh(f"docker volume rm {v}", check=False)
    left = surviving()
    if left:
        raise Refusal("rig", f"teardown left these behind: {left}")
    # dangling anonymous volumes were recorded at `up`; anything newer is ours
    vb = os.path.join(c.results, "volumes-before.json")
    before = set(load_json("volumes-before.json")) if os.path.exists(vb) else set()
    ours = [v for v in dangling_anonymous_volumes() if v not in before]
    for v in ours:
        sh(f"docker volume rm {v}", check=False)
    if [v for v in dangling_anonymous_volumes() if v not in before]:
        raise Refusal("rig", "anonymous volumes created during the run survive teardown")
    if ours:
        log(f"removed {len(ours)} anonymous volume(s) left by containers removed without -v")
    killed = reap_host_watchers()
    if killed:
        log(f"killed {len(killed)} host process(es) watching this run: " +
            "; ".join(f"{pid} {cmd[:80]}" for pid, cmd in killed))
    left = host_watchers()
    if left:
        raise Refusal("rig", "host processes watching this run survive teardown: " +
                             "; ".join(f"{pid} {cmd[:80]}" for pid, cmd in left))
    if trim:
        r = sh("docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- "
               "fstrim -v /var/lib/docker", check=False, timeout=600)
        log("fstrim:", (r.stdout + r.stderr).strip()[-200:])
    log("stack down; nothing with prefix", c.project, "survives")


def host_watchers(ignore_children=False):
    """Host processes the harness did not start but that watch this run: anything
    holding a file open under results/, naming the project directory on its
    command line, or naming prove.py on its command line *and* running from
    inside the project. Run 11 left two `until ! pgrep -f "prove.py suite"`
    shells alive, each matching its own command line and so waiting on itself;
    the assertion on containers, volumes and networks could not see them. Own
    process, its ancestors and its children are excluded. Scoped this tightly
    on purpose: a first version matched any command line naming prove.py and
    its self-test killed a live tiny proof in another directory; a second
    matched any process whose working directory was the project and killed
    the `tail` its own shell was piping into."""
    c = cfg()
    root = os.path.realpath(c.root)
    me = os.getpid()
    ps = sh("ps -axo pid=,ppid=,command=", check=False).stdout.splitlines()
    parent, cmd = {}, {}
    for line in ps:
        parts = line.split(None, 2)
        if len(parts) < 2:
            continue
        pid, ppid = int(parts[0]), int(parts[1])
        parent[pid] = ppid
        cmd[pid] = parts[2] if len(parts) > 2 else ""
    skip = {me}
    p = me
    while p in parent and parent[p] not in skip and parent[p] > 1:
        p = parent[p]; skip.add(p)
    def descends(pid):
        while pid in parent and pid > 1:
            if pid == me:
                return True
            pid = parent[pid]
        return False
    holders = set()
    r = sh(f"lsof -t +D {c.results}", check=False)
    for tok in r.stdout.split():
        if tok.isdigit():
            holders.add(int(tok))
    # working directories inside the project: `lsof -d cwd` prints one 'n<path>' per process
    inside = set()
    r = sh("lsof -a -d cwd -F pn", check=False)
    pid = None
    for line in r.stdout.splitlines():
        if line.startswith("p"):
            pid = int(line[1:])
        elif line.startswith("n") and pid is not None:
            path = line[1:]
            if path == root or path.startswith(root + os.sep):
                inside.add(pid)
    found = []
    for pid, line in cmd.items():
        if pid in skip or (descends(pid) and not ignore_children):
            continue
        if pid in holders or root in line or c.root in line or ("prove.py" in line and pid in inside):
            found.append((pid, line))
    return sorted(found)


def reap_host_watchers(ignore_children=False, only=None):
    """Stop the processes host_watchers finds. only: a set of pids to limit it
    to. The guard's own self-test passes the pids it started: without that it
    stopped every process naming the project directory, and under the example
    config that directory is the harness's own folder -- so running the gates
    stopped a live chain launched from the same harness (2026-09-24, a
    completeness run on a Confluent broker, killed one second in)."""
    found = [(pid, cl) for pid, cl in host_watchers(ignore_children) if only is None or pid in only]
    for pid, _ in found:
        sh(f"kill -TERM {pid}", check=False)
    if found:
        time.sleep(1.0)
        for pid, _ in host_watchers(ignore_children):
            if only is None or pid in only:
                sh(f"kill -KILL {pid}", check=False)
        time.sleep(0.5)
    return found


def surviving():
    """No child the run started survives: containers, volumes, networks."""
    c = cfg()
    out = []
    for kind, cmd in (("container", "docker ps -a --format '{{.Names}}'"),
                      ("volume", "docker volume ls --format '{{.Name}}'"),
                      ("network", "docker network ls --format '{{.Name}}'")):
        names = sh(cmd, check=False).stdout.split()
        out += [f"{kind}:{n}" for n in names if n.startswith(c.project + "-") or n.startswith(c.project + "_")]
    return out


# --------------------------------------------------------------------- sampler

def build_sampler():
    """Compile the offset sampler on the host JDK against the broker image's own
    kafka-clients jar, so the transport vantage point does not live in the
    pipeline's jar and the pipeline cannot influence it."""
    c = cfg()
    out = os.path.join(c.stack_dir, "sampler")
    src = os.path.join(HERE, "sampler", "OffsetSampler.java")
    cls = os.path.join(out, "OffsetSampler.class")
    if os.path.exists(cls) and os.path.getmtime(cls) >= os.path.getmtime(src):
        return out
    os.makedirs(out, exist_ok=True)
    cid = sh(f"docker create {c.kafka_img}").stdout.strip()
    try:
        libs = sh(f"docker run --rm --entrypoint sh {c.kafka_img} -c 'ls {c.kafka_libs}'").stdout.split()
        cl = [l for l in libs if l.startswith("kafka-clients-") and l.endswith(".jar")]
        if not cl:
            raise Refusal("rig", f"no kafka-clients jar under {c.kafka_libs} in {c.kafka_img} — "
                                 f"set images.kafkaLibs to where this image keeps them")
        sh(f"docker cp {cid}:{c.kafka_libs}/{cl[0]} {out}/{cl[0]}")
    finally:
        sh(f"docker rm -v {cid}", check=False)
    sh(f"{c.jdk}/bin/javac --release 17 -cp {out}/{cl[0]} -d {out} {HERE}/sampler/OffsetSampler.java")
    if not os.path.exists(os.path.join(out, "OffsetSampler.class")):
        raise Refusal("rig", "sampler did not compile")
    return out


def start_sampler(group):
    c = cfg()
    sdir = build_sampler()
    sh(f"docker rm -f -v {c.sampler}", check=False)
    sh(f"docker run -d --name {c.sampler} --network {c.net} -v {sdir}:/sampler:ro "
       f"--entrypoint java {c.kafka_img} -cp '{c.kafka_libs}/*:/sampler' OffsetSampler "
       f"--bootstrap={c.boot_int} --group={group} --inTopic={c.topic_in} "
       f"--outTopics={','.join(c.topics_out)} --intervalMs=500")
    # assert anything launched unattended is alive before waiting on it
    for _ in range(60):
        lg = sh(f"docker logs {c.sampler}", check=False)
        if '"sampler":"up"' in lg.stdout:
            return
        if not sh(f"docker ps -q -f name=^{c.sampler}$", check=False).stdout.strip():
            raise Refusal("rig", f"offset sampler died at startup:\n{lg.stdout}\n{lg.stderr}")
        time.sleep(1)
    raise Refusal("rig", "offset sampler never reported up")


def stop_sampler():
    sh(f"docker rm -f -v {cfg().sampler}", check=False)


def sampler_ticks_since(ts_ms):
    """Every tick the sampler printed at or after ts_ms (its whole log is read)."""
    r = sh(f"docker logs {cfg().sampler}", check=False)
    out = []
    for line in r.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and '"ts"' in line:
            try:
                t = json.loads(line)
            except Exception:
                continue
            if t.get("ts", 0) >= ts_ms:
                out.append(t)
    return out


def sampler_tail(n=8):
    r = sh(f"docker logs --tail {n} {cfg().sampler}", check=False)
    ticks = []
    for line in r.stdout.strip().splitlines():
        line = line.strip()
        if line.startswith("{") and '"ts"' in line:
            try:
                ticks.append(json.loads(line))
            except Exception:
                pass
    return ticks


# ---------------------------------------------------------------------- topics

def topic_exists(t):
    return t in kafka(f"kafka-topics.sh --bootstrap-server {cfg().boot_int} --list").stdout.split()


def create_topic(t, partitions=None, retention_bytes=None):
    c = cfg()
    partitions = partitions or c.partitions
    extra = ""
    if retention_bytes is not None:
        extra = f" --config retention.bytes={retention_bytes} --config segment.bytes=134217728"
    kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --create --topic {t} "
          f"--partitions {partitions} --replication-factor 1{extra}")
    d = kafka(f"kafka-topics.sh --bootstrap-server {c.boot_int} --describe --topic {t}").stdout
    n = d.count("\tPartition:")
    if n != partitions:
        raise Refusal("rig", f"topic {t} created with {n} partitions, wanted {partitions}")
    if retention_bytes is not None and f"retention.bytes={retention_bytes}" not in d:
        raise Refusal("rig", f"retention.bytes did not apply on {t}: {d}")
    return d


def delete_topic(t):
    if not topic_exists(t):
        return
    kafka(f"kafka-topics.sh --bootstrap-server {cfg().boot_int} --delete --topic {t}")
    for _ in range(60):
        if not topic_exists(t):
            return
        time.sleep(1)
    raise Refusal("rig", f"topic {t} still present after delete")


def log_end(topic):
    r = kafka(f"kafka-get-offsets.sh --bootstrap-server {cfg().boot_int} --topic {topic} --time -1")
    tot, per = 0, {}
    for line in r.stdout.strip().splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic:
            per[int(parts[1])] = int(parts[2])
            tot += int(parts[2])
    return tot, per


def delete_group(g):
    kafka(f"kafka-consumer-groups.sh --bootstrap-server {cfg().boot_int} --delete --group {g}", check=False)


def recreate_output_topics(include_declared=False):
    """Empty the outputs before a case.

    include_declared also empties the topics the run named in
    topicsAlsoWritten. Section 4 asks the verifier for different assertions on
    the clean drain and the killed one -- never backwards against backwards at
    most once -- and only topics.out was ever cleared between them. A pipeline
    whose output is per window has all of its output in topicsAlsoWritten by
    construction, so its verifier saw both arms' rows concatenated with nothing
    to tell them apart. Clean-room run 41 worked round it by stamping the
    consumer group into the published business row, which is measurement
    machinery in someone's data.
    """
    c = cfg()
    topics = list(c.topics_out) + (list(c.topics_also) if include_declared else [])
    for t in topics:
        delete_topic(t)
        create_topic(t, retention_bytes=T["sinkRetentionBytes"])
    for t in topics:
        tot, _ = log_end(t)
        if tot != 0:
            raise Refusal("rig", f"output topic {t} is not empty after recreate: {tot}")


def verify_backlog(manifest, topic=None):
    """After any recreating infrastructure command, re-verify the backlog against its manifest."""
    c = cfg()
    topic = topic or c.topic_in
    want = int(manifest[c.count_field])
    tot, per = log_end(topic)
    if tot != want:
        raise Refusal("rig", f"backlog on {topic} is {tot} records, manifest says {want}")
    if len(per) != c.partitions:
        raise Refusal("rig", f"backlog has {len(per)} partitions, pipeline.json says {c.partitions}")
    return tot


# Where each manifest this process loaded came from. The manifest is passed
# around as the parsed object -- putting the path inside it would put a user's
# home directory into every results file, which is redacted on the way out.
MANIFEST_PATHS = {}


def manifest_path_of(manifest):
    return MANIFEST_PATHS.get(id(manifest))


def minutes(seconds):
    n = round(seconds / 60)
    return "1 minute" if n == 1 else f"{n} minutes"


def fill_stall_reason(samples, now, topic, start_s=None, stall_s=None):
    """Why a running fill should be stopped, in one sentence, or None.

    samples: [(time, records on the topic)], the first taken when the
    generator started. Pure, so the self-test can drive it."""
    start_s = T["fillStartS"] if start_s is None else start_s
    stall_s = T["fillStallS"] if stall_s is None else stall_s
    t0 = samples[0][0]
    top = max(n for _, n in samples)
    if top <= samples[0][1]:
        if now - t0 >= start_s:
            return f"the generator has written nothing to {topic} in {minutes(now - t0)}"
        return None
    grew = max(t for (t, n), (_, prev) in zip(samples[1:], samples) if n > prev)
    if now - grew >= stall_s:
        return f"{topic} has held at {top:,} records for {minutes(now - grew)} -- the generator stopped writing"
    return None


def fill(topic, count, seed, manifest_name):
    """Fill a backlog, write its manifest, read the log end back against it."""
    c = cfg()
    delete_topic(topic)
    create_topic(topic)
    man = os.path.join(c.results, manifest_name)
    cmd = c.fmt(c.gen_cmd, count=count, seed=seed, topic=topic, manifest=man)
    log("fill:", cmd)
    # Watched, not waited on: a generator that stops writing used to hold the
    # chain for up to two hours in silence (run 48, finding F8), and its output
    # was thrown away except for the last line (finding F6).
    out_path = os.path.join(c.results, f"fill-{topic}.log")
    with open(out_path, "w") as out:
        proc = subprocess.Popen(cmd, shell=True, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    last_lines = lambda: open(out_path, errors="replace").read().strip().splitlines()[-5:]

    def stop(why):
        try:
            os.killpg(proc.pid, 9)
        except Exception:
            pass
        proc.wait()
        raise Refusal("rig", f"{why}. The fill was stopped. The generator's last lines, from {out_path}:\n  "
                             + "\n  ".join(last_lines() or ["(it printed nothing)"]))

    t0 = time.time()
    samples, said = [(t0, 0)], t0
    while proc.poll() is None:
        for _ in range(30):
            if proc.poll() is not None:
                break
            time.sleep(1)
        if proc.poll() is not None:
            break
        try:
            n, _ = log_end(topic)
        except Exception:
            continue
        now = time.time()
        samples.append((now, n))
        if now - said >= 60:
            log(f"fill: {n:,} of {count:,} records written to {topic} ({n / count:.0%}), "
                f"{(now - t0) / 60:.0f} min in")
            said = now
        why = fill_stall_reason(samples, now, topic)
        if why:
            stop(why)
        if now - t0 > 7200:
            stop(f"the generator was still running after two hours, with {n:,} of {count:,} records written")
    if proc.returncode != 0:
        raise Refusal("rig", f"the generator stopped with exit code {proc.returncode}. Its last lines, "
                             f"from {out_path}:\n  " + "\n  ".join(last_lines() or ["(it printed nothing)"]))
    tail = last_lines()[-1:]
    log("fill done:", tail[0] if tail else "")
    m = json.load(open(man))
    MANIFEST_PATHS[id(m)] = man
    if int(m[c.count_field]) != count:
        raise Refusal("rig", f"manifest says {m[c.count_field]} records, asked for {count}")
    verify_backlog(m, topic)
    log(f"backlog {topic}: {count:,} records read back over {c.partitions} partitions")
    return m


# ---------------------------------------------------------------- task manager

def tm_running():
    return bool(sh(f"docker ps -q -f name=^{cfg().tm}$", check=False).stdout.strip())


def stop_tm():
    c = cfg()
    sh(f"docker rm -f -v {c.tm}", check=False)
    for _ in range(60):
        if not tm_running():
            break
        time.sleep(1)
    else:
        raise Refusal("rig", "task manager container would not die")
    for _ in range(90):
        try:
            if rest("/overview")["taskmanagers"] == 0:
                return
        except Exception:
            pass
        time.sleep(1)
    raise Refusal("rig", "engine still reports a registered task manager after teardown")


def _mib(spec):
    m = re.match(r"^(\d+)\s*([kmgKMG])$", str(spec).strip())
    if not m:
        raise Refusal("rig", f"memory {spec!r} must be a number followed by k, m or g")
    n, unit = int(m.group(1)), m.group(2).lower()
    return n * {"k": 1 / 1024.0, "m": 1.0, "g": 1024.0}[unit]


def mem_for(spec, cores, base="0m"):
    """base + per-subtask x cores, as the container's figure for this case."""
    return f"{int(_mib(base) + _mib(spec) * cores)}m"


def start_tm(cores, slots=None, reporter_s=None):
    c = cfg()
    slots = slots if slots is not None else cores
    over = c.per_case.get(cores, {})
    if not (over.get("tmMemory") or c.tm_mem_per_core or c.raw["caps"].get("tmMemory")):
        # the demo's behaviour: no process size, no container limit, so memory
        # can never be the thing that runs out first
        tm_mem = tm_mem_limit = None
    elif over.get("tmMemory"):
        tm_mem = over["tmMemory"]
    else:
        tm_mem = mem_for(c.tm_mem_per_core, cores, c.tm_mem_base) if c.tm_mem_per_core else c.tm_mem
    if over.get("tmMemoryLimit"):
        tm_mem_limit = over["tmMemoryLimit"]
    elif c.tm_mem_limit_per_core:
        tm_mem_limit = mem_for(c.tm_mem_limit_per_core, cores, c.tm_mem_base)
    elif c.tm_mem_per_core:
        m = re.match(r"^(\d+)([kmg])$", tm_mem)
        tm_mem_limit = f"{int(int(m.group(1)) * 1.25)}{m.group(2)}"   # headroom over the JVM's own figure
    else:
        tm_mem_limit = c.tm_mem_limit
    reporter_s = reporter_s or T["reporterS"]
    stop_tm()
    props = (f"jobmanager.rpc.address: {c.jm}\n"
             f"taskmanager.numberOfTaskSlots: {slots}\n"
             # Uncapped means uncapped: no process size of ours either, so the
             # image's own default applies -- which is what this repository's
             # demo runs with. Setting flink.size here instead collided with
             # that default and killed the task manager at startup.
             + (f"taskmanager.memory.process.size: {tm_mem}\n" if tm_mem else "") +
             f"taskmanager.memory.managed.fraction: 0.1\n"
             f"taskmanager.memory.network.fraction: 0.15\n"
             f"taskmanager.memory.network.max: 512m\n"
             f"state.checkpoint-storage: filesystem\n"
             f"state.checkpoints.dir: file:///ckpt\n"
             # busy / idle / back-pressured come from the SLF4J reporter plugin on
             # the worker: the JobManager REST back-pressure path is deprecated on
             # 1.20 and per-vertex metrics come back empty under load.
             f"metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory\n"
             f"metrics.reporter.slf4j.interval: {reporter_s} SECONDS\n"
             f"metrics.reporter.slf4j.scope.variables.excludes: job_id;task_id;task_attempt_id;tm_id\n"
             + "".join(f"{k}: {v}\n" for k, v in c.flink_props.items()))
    sh(f"docker run -d --name {c.tm} --hostname {c.tm} --network {c.net} --user 0:0 "
       f"--cpus {cores} " + (f"--memory {tm_mem_limit} " if tm_mem_limit else "") +
       f"-v {c.ckpt_vol}:/ckpt -v {c.jar_dir}:/jobs:ro "
       + "".join(f"-e {k}={shlex.quote(str(v))} " for k, v in c.flink_env.items())
       + f"-e FLINK_PROPERTIES=$'{props}' {c.flink_img} taskmanager")
    nano = assert_cap(c.tm, cores)
    for _ in range(120):
        try:
            o = rest("/overview")
            if o["taskmanagers"] == 1 and o["slots-total"] == slots:
                return nano
        except Exception:
            pass
        if not tm_running():
            tail = sh(f"docker logs --tail 40 {c.tm}", check=False).stdout
            raise Refusal("rig", f"task manager died during startup:\n{tail}")
        time.sleep(1)
    raise Refusal("rig", f"task manager never registered {slots} slots")


def assert_cap(container, cores):
    """GUARD: read the cap back from the container, never from the env var."""
    nano = int(sh(f"docker inspect -f '{{{{.HostConfig.NanoCpus}}}}' {container}").stdout.strip())
    if nano != int(round(cores * 1_000_000_000)):
        raise Refusal("rig", f"cpu cap did not apply on {container}: NanoCpus={nano}, wanted {int(cores*1e9)}")
    cpumax = sh(f"docker exec {container} cat /sys/fs/cgroup/cpu.max").stdout.split()
    if len(cpumax) != 2 or cpumax[0] == "max" or abs(int(cpumax[0]) / int(cpumax[1]) - cores) > 1e-6:
        raise Refusal("rig", f"cgroup cpu.max={cpumax} on {container} does not equal {cores} cores")
    return nano


def image_tm_memory():
    """What the engine image itself sets, when the harness passes nothing.

    Passing no process size does not mean memory is unmanaged: flink:1.20.1
    ships config.yaml with taskmanager.memory.process.size: 1728m, so every
    case gets the same flat figure. Read it rather than assume it -- the value
    moves between images and vendors.
    """
    c = cfg()
    r = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c "
           f"\"grep -A3 -E '^taskmanager:' /opt/flink/conf/config.yaml 2>/dev/null | "
           f"grep -oE '[0-9]+[mMgG]' | head -1\"", check=False, timeout=120)
    v = (r.stdout or "").strip().splitlines()
    return v[0] if v else None


def tm_memory_capped():
    """Is the worker's memory actually capped by us, or left to the engine?

    Cfg.tm_mem carries a 4096m default that applies whether or not anything was
    set, so asking it is not the same as asking whether a cap exists. Preflight
    said "uncapped" on one line and budgeted 4096m three lines later, and the
    same phantom went into suite.json as heldStill.tmProcessMemory -- a recorded
    setting that was never in effect. Clean-room run 30 reported both.
    """
    c = cfg()
    return bool(c.tm_mem_per_core or c.raw["caps"].get("tmMemory") or c.per_case)


def host_scaling(seconds=5.0, cases=(1, 2, 4), repeats=3):
    """What this host's own cores do, before any pipeline is judged for missing
    linear. Register-only and memory-bound arms at each case, in the same image
    the worker runs in, capped the same way.

    Repeated, because a single reading is not a bound. Clean-room run 30 ran the
    memory-bound arm three times on an idle machine inside 90 minutes and read
    76.4%, 83% and 91% at 2->4 -- a spread of 15 points, used to judge a 12-point
    shortfall. It now reports the median with the range it came from, so a
    reader can see whether the bound is tight enough to explain anything.

    Four repeats or more also get the middle half, which is the figure that
    narrows as repeats are added. The full range does not: it is an envelope,
    and more samples can only find more of the distribution.
    """
    src = os.path.join(HERE, "probe", "Spin.java")
    if not os.path.exists(src):
        return None
    work = tempfile.mkdtemp(prefix="hostprobe-")
    shutil.copy(src, work)
    # the Flink image has no compiler; use the host JDK the rig already requires
    r = sh(f"{cfg().jdk}/bin/javac --release 17 -d {work} {work}/Spin.java", check=False)
    if r.returncode:
        return {"error": (r.stderr or "")[-200:]}
    out = {"perCore": {}, "ofLinear": {}, "ofLinearRange": {}, "repeats": repeats}
    for mode in ("alu", "mem"):
        # one reading per case per repeat, so each repeat yields its own step ratios
        runs = []
        for _ in range(repeats):
            per = {}
            for n in cases:
                rr = sh(f"docker run --rm --cpus {n} -v {work}:/probe:ro --entrypoint java "
                        f"{cfg().flink_img} -cp /probe Spin {n} {int(seconds * 1000)} {mode}", check=False)
                m = re.search(r"per-core=([\d,]+)", rr.stdout or "")
                if m:
                    per[n] = float(m.group(1).replace(",", ""))
            runs.append(per)
        ordered = sorted(runs[0]) if runs and runs[0] else []
        out["perCore"][mode] = {n: round(statistics.median([r[n] for r in runs if r.get(n)]), 1)
                                for n in ordered if any(r.get(n) for r in runs)}
        steps, ranges = {}, {}
        for lo, hi in zip(ordered, ordered[1:]):
            vals = [round((r[hi] * hi) / (r[lo] * lo) / (hi / lo), 3)
                    for r in runs if r.get(lo) and r.get(hi)]
            if vals:
                steps[f"{lo}->{hi}"] = round(statistics.median(vals), 3)
                # Min to max is an envelope, and an envelope only ever widens as
                # repeats are added -- it cannot do otherwise, since a wider
                # sample can only find more of the distribution. Clean-room run
                # 36 followed the advice to "tighten it" from 3 repeats to 9 and
                # read 9% then 15%, exactly backwards. The middle half is the
                # figure that does tighten, so it is recorded beside the
                # envelope and it is the one compared against a shortfall.
                mid = None
                if len(vals) >= 4:
                    q = statistics.quantiles(vals, n=4, method="inclusive")
                    mid = {"low": round(q[0], 3), "high": round(q[2], 3),
                           "spread": round(q[2] - q[0], 3)}
                ranges[f"{lo}->{hi}"] = {"low": min(vals), "high": max(vals),
                                         "spread": round(max(vals) - min(vals), 3), "n": len(vals),
                                         "middleHalf": mid}
        out["ofLinear"][mode] = steps
        out["ofLinearRange"][mode] = ranges
    shutil.rmtree(work, ignore_errors=True)
    return out


def probe_spread(h, mode=None):
    """Pure. How steady a probe's readings are, for one arm or across all of
    them. The middle half is None until there are four repeats to take
    quartiles of.

    mode picks one arm. Reporting only the worst across arms hides the answer:
    measured on this rig at 25 repeats, the register-only arm's middle half was
    0% and the memory-bound arm's was 10%, and a single "11%" made the steady
    arm look as useless as the unsteady one.
    """
    per = (h or {}).get("ofLinearRange") or {}
    if mode is not None:
        per = {mode: per.get(mode) or {}}
    ranges = [r for m in per.values() for r in m.values()]
    if not ranges:
        return {"envelope": 0.0, "middleHalf": None, "repeats": (h or {}).get("repeats", 0),
                "mode": mode}
    mids = [r["middleHalf"]["spread"] for r in ranges if r.get("middleHalf")]
    return {"envelope": max(r.get("spread", 0) for r in ranges),
            "middleHalf": max(mids) if mids else None,
            "repeats": (h or {}).get("repeats", 0), "mode": mode}


def probe_advice(spread, gap):
    """Pure. What to say about a probe that is being asked to explain a
    shortfall.

    Never "raise the repeats until the range is narrower". The full range is min
    to max and only widens (run 36 went 9% over 3 repeats to 15% over 9, doing
    exactly as it was told). **And the middle half does not shrink either** --
    it converges on how variable the machine actually is, which may be a lot.
    Measured on this rig: the memory-bound arm's middle half read 6% over 9
    repeats and 11% over 25. More repeats buy an honest figure, not a smaller
    one, and once it has settled a wide one is the answer rather than a reason
    to run again.
    """
    usable = spread["middleHalf"] if spread["middleHalf"] is not None else spread["envelope"]
    which = "middle half" if spread["middleHalf"] is not None else "full range"
    if usable < gap:
        return [f"That {which} is tighter than the shortfall, so it is worth comparing against."]
    if spread["middleHalf"] is None:
        return [f"That is too wide to explain anything, and {spread['repeats']} repeats is too few to",
                "take a middle half from. Run `prove.py probe --repeats 9`: the full range will get",
                "wider, it is an envelope — but the middle half it prints is a real figure for how",
                "steady this machine is, and that is the one to compare with the shortfall."]
    return ["That is too wide to explain anything, and more repeats will not fix it: the middle",
            "half converges on how variable this machine is, and this is that figure. Say the",
            "machine cannot be ruled in or out here, and stop — an honest 'not settled' costs one",
            "line. Look at the arms separately first: one of them is often steady enough to use."]


def cgroup_mem(container):
    """Page-cache pressure on a container, as the kernel counts it: how many
    times the cgroup hit its memory limit, and how many file pages it had to
    read back after eviction."""
    r = sh(f"docker exec {container} sh -c 'cat /sys/fs/cgroup/memory.events; "
           f"cat /sys/fs/cgroup/memory.stat'", check=False)
    d = {"limitHits": 0, "refaults": 0, "fileCache": 0, "limitBytes": 0}
    for line in (r.stdout or "").strip().splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        k, v = parts[0], parts[1]
        if k == "max":
            d["limitHits"] = int(v)
        elif k == "workingset_refault_file":
            d["refaults"] = int(v)
        elif k == "file":
            d["fileCache"] = int(v)
    r2 = sh(f"docker exec {container} cat /sys/fs/cgroup/memory.max", check=False)
    v = (r2.stdout or "").strip()
    d["limitBytes"] = int(v) if v.isdigit() else 0
    return d


def cgroup_cpu(container):
    r = sh(f"docker exec {container} cat /sys/fs/cgroup/cpu.stat")
    d = {}
    for line in r.stdout.strip().splitlines():
        k, v = line.split()
        d[k] = int(v)
    return d


# ------------------------------------------------------------------ key layout

def build_keycheck():
    """Compile KeyCheck against the flink image's own dist jar, as the sampler is
    compiled against the broker image's kafka-clients jar. The image has no
    compiler, so the host JDK the rig already requires does the work."""
    c = cfg()
    out = os.path.join(c.stack_dir, "keycheck")
    cls = os.path.join(out, "KeyCheck.class")
    src = os.path.join(HERE, "keycheck", "KeyCheck.java")
    jar = os.path.join(out, "flink-dist.jar")
    if os.path.exists(cls) and os.path.exists(jar) and os.path.getmtime(cls) >= os.path.getmtime(src):
        return out, jar
    os.makedirs(out, exist_ok=True)
    cid = sh(f"docker create {c.flink_img}").stdout.strip()
    try:
        libs = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c 'ls /opt/flink/lib'").stdout.split()
        dist = [l for l in libs if l.startswith("flink-dist") and l.endswith(".jar")]
        if not dist:
            raise Refusal("rig", f"no flink-dist jar under /opt/flink/lib in {c.flink_img}, so the "
                                 f"engine's own key-group assignment cannot be asked where keys land")
        sh(f"docker cp {cid}:/opt/flink/lib/{dist[0]} {jar}")
    finally:
        sh(f"docker rm -v {cid}", check=False)
    sh(f"{c.jdk}/bin/javac --release 17 -cp {jar} -d {out} {src}")
    if not os.path.exists(cls):
        raise Refusal("rig", "KeyCheck did not compile")
    return out, jar


def keycheck(args, stdin_text=""):
    """Run KeyCheck and return its lines. Its answers come from the image under
    test, never from a copy of Flink's hash kept here."""
    c = cfg()
    out, jar = build_keycheck()
    r = sh(f"{c.java} -cp '{jar}:{out}' KeyCheck {args}", input=stdin_text, timeout=300)
    return [ln for ln in r.stdout.splitlines() if ln.strip()]


def max_parallelism_for(cases):
    """The maxParallelism every case will run with, and where it came from.

    Flink picks it per operator from the parallelism when nothing sets it, so
    two cases can be given different key-group counts and therefore a different
    key layout -- the same class of problem as a baseline with a different graph
    shape, one level down. Cases that would not share one are refused here.
    """
    c = cfg()
    explicit = c.flink_props.get("pipeline.max-parallelism")
    if explicit is not None:
        return int(str(explicit).strip()), "pipeline.max-parallelism, set in flinkProperties"
    got = {}
    for ln in keycheck("default " + ",".join(str(n) for n in sorted(set(cases)))):
        par, mp = ln.split("\t")
        got[int(par)] = int(mp)
    values = set(got.values())
    if len(values) != 1:
        raise Refusal("rig", f"the cases would not share one maxParallelism: {got}. Flink chooses it "
                             f"from the parallelism when nothing sets it, so the key layout -- which "
                             f"key lands on which subtask -- would differ between cases and the ratio "
                             f"would not be about cores. Set pipeline.max-parallelism in "
                             f"flinkProperties to one value for the whole suite.")
    return values.pop(), "Flink's own default for these parallelisms"


def key_layout(keys, max_par, cases):
    """{key: {parallelism: subtask}}, from the engine's own assignment."""
    pars = ",".join(str(n) for n in sorted(set(cases)))
    lines = keycheck(f"layout {max_par} {pars}", "\n".join(keys) + "\n")
    if len(lines) != len(keys):
        raise Refusal("rig", f"KeyCheck answered for {len(lines)} of {len(keys)} keys")
    layout = {}
    for ln in lines:
        f = ln.split("\t")
        layout[f[0]] = dict(zip(sorted(set(cases)), (int(x) for x in f[2:])))
    return layout


def suggest_max_parallelism(sets, cases, how_many=3):
    """maxParallelism values that divide every key set evenly at every case."""
    rows = [f"{name}\t{k}" for name, keys in sets.items() for k in keys]
    pars = ",".join(str(n) for n in sorted(set(cases)))
    return [int(x) for x in keycheck(f"suggest {pars} {how_many}", "\n".join(rows) + "\n")]


def key_spread(sets, cases, layout, max_par=None):
    """Pure. What each case's subtasks would get, and what that bounds.

    A subtask with more keys than its neighbours does proportionally more work,
    so the busiest one sets a ceiling on the stage: a stage whose busiest
    subtask holds a share b of the keys at parallelism p cannot return more
    than 1/(b*p) of linear, whatever the pipeline does. Nothing here talks to
    anything, so every threshold can be self-tested without a stack.
    """
    out = {"maxParallelism": max_par, "stages": {}, "idle": [], "worst": None}
    worst = None
    for name, keys in sets.items():
        st = {"keys": len(keys), "cases": {}}
        for par in sorted(set(cases)):
            counts = [0] * par
            for k in keys:
                counts[layout[k][par]] += 1
            busiest = max(counts) / len(keys) if keys else 0.0
            ceiling = 1.0 / (busiest * par) if busiest else 0.0
            idle = [i for i, n in enumerate(counts) if n == 0]
            st["cases"][par] = {"keysPerSubtask": counts, "busiestShare": busiest,
                                "evenShare": 1.0 / par, "idleSubtasks": idle,
                                "stageCeiling": ceiling}
            if idle:
                out["idle"].append({"stage": name, "cores": par, "subtasks": idle})
            if worst is None or ceiling < worst["stageCeiling"]:
                worst = {"stage": name, "cores": par, "keysPerSubtask": counts,
                         "busiestShare": busiest, "stageCeiling": ceiling, "idleSubtasks": idle}
        out["stages"][name] = st
    out["worst"] = worst
    return out


def key_skew_verdict(spread, floor, suggestions=()):
    """Pure. What to do about a layout that is not even.

    Refuse what can be fixed, report what cannot. A subtask with no keys at all
    cannot reach its cap and the case is not about cores, so that always
    refuses. Short of that, an uneven stage refuses only when a maxParallelism
    exists that would even it out -- the fix is one line of flinkProperties and
    costs nothing. When no value in Flink's range divides the keys, the shape
    is a property of the key space, and it belongs in the table and in the next
    interview rather than in a refusal nobody can satisfy.
    """
    w = spread["worst"]
    if w is None:
        return None
    where = (f"the {w['stage']} stage at {w['cores']} cores would get "
             f"{'/'.join(str(n) for n in w['keysPerSubtask'])} keys across its subtasks")
    fix = (f" Set pipeline.max-parallelism to {suggestions[0]} in flinkProperties, which divides "
           f"every key set evenly at every case." if suggestions else "")
    if spread["idle"]:
        i = spread["idle"][0]
        n = len(i["subtasks"])
        which = ", ".join(str(x) for x in i["subtasks"])
        return Refusal("rig", f"{where}, so {'subtask ' + which + ' gets' if n == 1 else 'subtasks ' + which + ' get'}"
                              f" no keys at all. {'That core' if n == 1 else 'Those cores'} cannot reach "
                              f"a cap with no work to do, and the case would be measuring the key layout "
                              f"rather than cores."
                              + (fix or " No maxParallelism between 128 and 32,768 divides these keys "
                                        "evenly, so the keys themselves have to change: the interview's "
                                        "third question is where that is decided."))
    if w["stageCeiling"] < floor and suggestions:
        even_share = 1.0 / w["cores"]
        return Refusal("rig", f"{where}. The busiest one holds {w['busiestShare']:.1%} of that stage's "
                              f"keys where an even split is {even_share:.1%}, so the stage cannot "
                              f"return more than {w['stageCeiling']:.2f} of linear at {w['cores']} "
                              f"cores however fast the pipeline is, and the run is judged at "
                              f"{floor:.2f}." + fix)
    return None


# ------------------------------------------------------------------------- job

def submit_job(par, group, ckpt_ms=None):
    c = cfg()
    args = c.fmt(c.job_args, par=par, group=group, ckptMs=ckpt_ms or c.ckpt_ms)
    r = sh(f"docker exec {c.jm} flink run -d -p {par} -c {c.main_class} {c.jar_in_ctr} {args}", timeout=300)
    m = re.search(r"JobID\s+([0-9a-f]{32})", r.stdout + r.stderr)
    if not m:
        raise Refusal("rig", f"could not read a JobID back from submit:\n{r.stdout}\n{r.stderr}")
    return m.group(1)


def wait_running(jid, par, timeout=180):
    """GUARD: no job is actually running / parallelism != requested."""
    for _ in range(timeout):
        try:
            j = rest(f"/jobs/{jid}")
            if j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]):
                bad = [(v["name"], v["parallelism"]) for v in j["vertices"] if v["parallelism"] != par]
                if bad:
                    raise Refusal("rig", f"vertex parallelism != {par}: {bad}")
                return j
            if j["state"] in ("FAILED", "CANCELED", "FINISHED"):
                raise Refusal("rig", f"job reached {j['state']} instead of RUNNING")
        except Refusal:
            raise
        except Exception:
            pass
        time.sleep(1)
    raise Refusal("rig", "job never reached RUNNING")


def graph_shape(jid):
    """Read the shape off the RUNNING plan: vertex descriptions, edge ship
    strategies, and the key-group count every keyed vertex was given.

    maxParallelism belongs here because Flink picks it from the parallelism
    when nothing sets it, and it decides which key lands on which subtask. Two
    cases given different key-group counts are running different key layouts,
    which is the same class of difference as a baseline with a different graph
    -- one level down, and invisible in every other column."""
    plan = rest(f"/jobs/{jid}/plan")["plan"]
    sig = []
    for n in sorted(plan["nodes"], key=lambda x: x["description"]):
        ships = sorted(i.get("ship_strategy", "?") for i in n.get("inputs", []))
        sig.append([n["description"], ships])
    # JobVertexDetailsInfo.FIELD_NAME_MAX_PARALLELISM, read back from the engine
    max_par = sorted({v["maxParallelism"] for v in rest(f"/jobs/{jid}")["vertices"]})
    # The plan itself, kept. The signature is what cases are compared on, and it
    # deliberately drops the edges -- which left nothing in the record to draw
    # the graph from afterwards. A drawn diagram is a claim; this one is read
    # off the job that ran.
    return {"vertexCount": len(plan["nodes"]), "signature": sig, "maxParallelism": max_par,
            "plan": plan}


MERMAID_SAFE = re.compile(r'[^A-Za-z0-9 ()/,.:+_<>-]')


def vertex_label(description):
    """Pure. A running plan's vertex description, fit to print.

    Flink describes a vertex as its chained operators in an ASCII tree --
    "positions<br/>:- sink-by-symbol: Writer<br/>:  +- ...: Committer<br/>" --
    which is unreadable in a box. The tree drawing goes, the operator names
    stay, one per line."""
    parts = [p for p in re.split(r"<br\s*/?>", description or "") if p.strip()]
    out = []
    for part in parts:
        name = re.sub(r"^[:+\-\s|]+", "", part).strip()
        if name:
            out.append(MERMAID_SAFE.sub("", name))
    return "<br/>".join(out) or "?"


def graph_mermaid(plan):
    """Pure. The job graph as Mermaid, from the plan the engine served.

    Every other picture of the pipeline is drawn by hand and is therefore a
    claim about what was built. Clean-room runs 36 and 37 both built one
    market-value sink where the business case asks for two, and both drew
    diagrams; nothing compared either drawing with the job. This one is the
    job.
    """
    nodes = (plan or {}).get("nodes") or []
    if not nodes:
        return ""
    name = {n["id"]: f"v{i}" for i, n in enumerate(nodes)}
    lines = ["flowchart LR"]
    for n in nodes:
        lines.append(f'  {name[n["id"]]}["{vertex_label(n.get("description"))}"]')
    for n in nodes:
        for i in n.get("inputs") or []:
            src = name.get(i.get("id"))
            if not src:
                continue
            ship = MERMAID_SAFE.sub("", (i.get("ship_strategy") or "").strip())
            arrow = f'-- {ship} -->' if ship and ship.upper() != "FORWARD" else "-->"
            lines.append(f'  {src} {arrow} {name[n["id"]]}')
    return "\n".join(lines)


def design_diff(design, plan, topic_records, built_constraints, before_fill=False):
    """Pure. What the run wrote down, against the job that ran.

    A diagram cannot be checked and a sentence cannot be diffed, so the design
    is declared as lists -- operators, inputs, outputs -- and each line is
    looked for in the running plan and on the broker. Clean-room runs 36 and
    37 both built one market-value sink where the business case asks for two;
    both drew a picture of what they meant; nothing ever held the job to it.

    Returns rows and a refusal, or None. Declared-and-missing refuses. Built
    -and-not-declared is reported, never refused: a build is allowed more
    operators than the design names -- Flink adds its own -- and the row is
    there so a reader can see what else is in the graph.
    """
    design = design or {}
    labels = [vertex_label(n.get("description")) for n in ((plan or {}).get("nodes") or [])]
    flat = " | ".join(labels).lower()
    rows, missing = [], []

    def look(area, declared, found, detail):
        rows.append({"area": area, "declared": declared, "built": found, "detail": detail})
        if not found:
            missing.append(f"{area} {declared}")

    for op in design.get("operators") or []:
        look("operator", op, op.lower() in flat, "in the running plan" if op.lower() in flat
             else "no vertex in the running plan mentions it")
    for topic in design.get("inputs") or []:
        # An input is checked for EXISTING, not for holding records. The diff
        # runs during the completeness step, which is before the fill, so the
        # suite's own input topic is legitimately empty then -- and the shipped
        # example names it, so the example would have failed its own check.
        # Whether the job reads a topic is not visible from the broker at all;
        # the running plan is where that shows, and the operator rows cover it.
        there = topic_records.get(topic) is not None
        n = topic_records.get(topic) or 0
        if not there and before_fill:
            # Not merely empty -- not created. The diff runs inside the
            # completeness step, which is before the fill, so the suite's own
            # input topic does not exist yet on a cold stack. That is a fact
            # about the order the harness does its work in, not a build that
            # missed an input. Both shipped examples declare that topic and so
            # would refuse on their own first run; clean-room run 42 lost about
            # 25 minutes pre-creating it by hand and writing the workaround down
            # as an assumption.
            rows.append({"area": "input", "declared": topic, "built": True,
                         "detail": "not created yet -- the fill has not run"})
            continue
        look("input", topic, there,
             (f"{n:,} records on the broker" if n else "exists, not filled yet") if there
             else "no such topic")
    for topic in design.get("outputs") or []:
        n = topic_records.get(topic)
        look("output", topic, bool(n), f"{n:,} records after the drain" if n
             else ("written by nothing -- the topic is empty" if n == 0 else "no such topic"))
    for what, (want, got) in (built_constraints or {}).items():
        ok = str(want) == str(got)
        rows.append({"area": "constraint", "declared": f"{what} = {want}", "built": ok,
                     "detail": f"read back {got}"})
        if not ok:
            missing.append(f"constraint {what} = {want}, read back {got}")

    extra = [l for l in labels if not any((op or "").lower() in l.lower()
                                          for op in (design.get("operators") or []))]
    for l in extra:
        rows.append({"area": "in the build only", "declared": l, "built": True,
                     "detail": "not named in the design, which is allowed"})

    if not missing:
        return rows, None
    return rows, Refusal("build", "the job does not match the design the run wrote down: "
                                  + "; ".join(missing)
                                  + ". The interview's answers are the spec, and a picture of them "
                                    "is not a check -- this is.")


def design_table(rows):
    """Pure. The diff, as lines to print."""
    if not rows:
        return []
    rows = [dict(r, declared=re.sub(r"<br\s*/?>", " + ", r["declared"])) for r in rows]
    w = max(len(r["declared"]) for r in rows)
    w = min(max(w, 9), 60)
    out = [f"  {'area':<18} {'declared':<{w}} {'built':<5}  what was found",
           "  " + "-" * (18 + w + 7 + 30)]
    for r in rows:
        # "yes" beside "not created yet" is a row contradicting itself, which
        # clean-room run 45 filed. A third word for the one case that is neither.
        mark = "later" if (r["built"] and "not created yet" in (r["detail"] or "")) else \
               ("yes" if r["built"] else "NO")
        out.append(f"  {r['area']:<18} {r['declared'][:w]:<{w}} {mark:<5}  {r['detail']}")
    return out


def progress_from_outputs(manifest_path=None):
    """How much input the pipeline's own outputs account for, asked of the
    pipeline. The second vantage point for a job whose outputs are not a
    constant multiple of its input.

    The harness cannot read progress out of an arbitrary output -- an hourly
    average per location emits one row an hour whatever the input rate -- and
    the person who wrote the pipeline can. So this is supplied, exactly as
    verifier.cmd is. It prints one JSON object with inputRecordsProcessed.
    """
    c = cfg()
    cmd = c.fmt(c.vantage_cmd, manifest=manifest_path or "")
    r = sh(cmd, check=False, timeout=180)
    if r.returncode != 0:
        raise Refusal("rig", f"the secondVantage command failed (exit {r.returncode}): "
                             f"{(r.stderr or r.stdout or '')[-300:]}")
    try:
        n = json.loads(r.stdout.strip().splitlines()[-1])["inputRecordsProcessed"]
    except Exception as e:
        raise Refusal("rig", f"the secondVantage command did not print "
                             f'{{"inputRecordsProcessed": N}}: {r.stdout[-200:]!r} ({e})')
    return int(n)


def vantage_delta(open_tick, close_tick, open_progress, close_progress):
    """Pure. The second vantage's reading of how much input was consumed in the
    window, whichever way it was measured."""
    c = cfg()
    if c.vantage_mode == "command":
        return float(close_progress - open_progress), "the pipeline's own progress command"
    d_out = sum(close_tick[f"end_{t}"] - open_tick[f"end_{t}"] for t in c.topics_out)
    return d_out / c.out_per_in, f"sink rows / {c.out_per_in:g} outputs per input"


def declared_outputs_verdict(counts):
    """Pure. Every topic the run said its pipeline writes got records.

    counts is {topic: records after a drain that ran to the last record}. A
    topic the business case asks for and the build never writes is the defect
    that survived two clean-room runs: the picture in the report showed what
    was meant, the job did something smaller, and nothing compared them.
    """
    empty = sorted(t for t, n in counts.items() if not n)
    if not empty:
        return None
    return Refusal("build", f"the pipeline was declared to write {', '.join(sorted(counts))}, and after "
                            f"a drain that ran to the last record "
                            f"{'topic ' + empty[0] + ' is' if len(empty) == 1 else 'topics ' + ', '.join(empty) + ' are'}"
                            f" empty. Either the build is missing an output the business case asks for, "
                            f"or topicsAlsoWritten names a topic the design does not have. The interview's "
                            f"answers are the spec; the job has to match them.")


def declared_output_counts():
    """Records in each topic the run declared its pipeline writes."""
    return {t: log_end(t)[0] for t in cfg().topics_also}


def cancel_job(jid):
    c = cfg()
    if not jid:
        return
    try:
        rest_patch(f"/jobs/{jid}?mode=cancel")
    except Exception:
        sh(f"docker exec {c.jm} flink cancel {jid}", check=False)
    for _ in range(90):
        try:
            if rest(f"/jobs/{jid}")["state"] in ("CANCELED", "FINISHED", "FAILED"):
                return
        except Exception:
            return
        time.sleep(1)
    raise Refusal("rig", f"job {jid} would not cancel")


def assert_cluster_idle():
    """GUARD: assert idle by asking the engine, not by killing what you think is there."""
    o = rest("/overview")
    if o["jobs-running"] != 0:
        raise Refusal("rig", f"cluster is still busy: {o['jobs-running']} jobs running")
    if o["taskmanagers"] != 0:
        raise Refusal("rig", f"a task manager from the last case is still registered: {o}")


GC_RE = re.compile(r"\.Status\.JVM\.GarbageCollector\.(?P<gc>[^.]+)\.(?P<metric>Time|Count): (?P<val>[\d.]+)\s*$")
BP_RE = re.compile(r"\.(?P<vertex>[^.]+?)\.(?P<sub>\d+)\."
                   r"(?P<metric>busyTimeMsPerSecond|idleTimeMsPerSecond|backPressuredTimeMsPerSecond): (?P<val>[\d.]+)\s*$")


def backpressure_in_window(t_open, t_close):
    """busy / idle / back-pressured per vertex, averaged over the reporter samples
    that fall INSIDE the window, read from the worker's SLF4J reporter log.
    Internal back-pressure inside a capped worker is reported and gated on
    nothing; source IDLE is the external boundary and is gated."""
    c = cfg()
    r = sh(f"docker logs --timestamps {c.tm}", check=False, timeout=120)
    acc, gc = {}, {}
    for line in (r.stdout + r.stderr).splitlines():
        sp = line.split(" ", 1)
        if len(sp) != 2:
            continue
        try:
            ts = calendar.timegm(time.strptime(sp[0][:19], "%Y-%m-%dT%H:%M:%S"))  # docker emits UTC
        except Exception:
            continue
        if not (t_open <= ts <= t_close):
            continue
        g = GC_RE.search(sp[1])
        if g:
            gc.setdefault(f"{g['gc']}.{g['metric']}", []).append(float(g["val"]))
            continue
        m = BP_RE.search(sp[1])
        if m:
            acc.setdefault(m["vertex"], {}).setdefault(m["metric"], []).append(float(m["val"]))
    out = {}
    for vtx, mm in acc.items():
        out[vtx] = {k.replace("TimeMsPerSecond", ""): round(sum(v) / len(v) / 1000.0, 4) for k, v in mm.items()}
        out[vtx]["samples"] = min(len(v) for v in mm.values())
    out["_gc"] = {k: (max(v) - min(v)) for k, v in gc.items()}
    # Which collector actually ran, not which one was asked for. --cpus 1 makes
    # the JVM see one processor and choose the serial collector, so the baseline
    # runs different code from every case above it unless something pins it.
    out["_gcNames"] = sorted({k.split(".")[0] for k in gc})
    return out


# ------------------------------------------------------------------- the case

def settle_boundary(ticks, cand, settle_ms):
    """Pure: a checkpoint's offsets can reach the broker in pieces, so the first
    tick that shows an advance may hold only part of the commit. Take the last
    tick that keeps advancing within settle_ms of the one before it.

    Measured on the rig 2026-09-05, 4 cores, 10 s checkpoints: commits landed
    every ~10 s at 7.5-8.2M records, and one arrived as +1,902,797 followed
    0.50 s later by +5,672,429. The window closed on the first half and the two
    vantage points disagreed by 33% — the third such refusal on record (20.2%,
    33.9%, 33.0%), every one at parallelism > 1, none at 1. Nothing real
    advances the committed offset twice inside a fraction of a checkpoint
    interval, so settling is safe: settle_ms is a fifth of the interval, and
    the widest observed split is a twentieth of it."""
    out = cand
    for t in ticks:
        if t["ts"] <= out["ts"] or t.get("committed", -1) < 0:
            continue
        if t["committed"] > out["committed"] and t["ts"] - out["ts"] <= settle_ms:
            out = t
        elif t["ts"] - out["ts"] > settle_ms:
            break
    return out


def next_boundary(after=None, timeout=90, settle_ms=None):
    """The window is anchored on the committed offset advancing, never on wall
    clock — and on the *whole* commit, not the first piece of one to arrive."""
    base = after
    settle_ms = settle_ms if settle_ms is not None else min(1500.0, 0.2 * cfg().ckpt_ms)
    t0 = time.time()
    while time.time() - t0 < timeout:
        for t in sampler_tail(8):
            if t.get("committed", -1) < 0:
                continue
            if base is None:
                base = t
                continue
            if t["committed"] > base["committed"] and t["ts"] > base["ts"]:
                # let the rest of this commit land before the window uses it
                time.sleep(settle_ms / 1000.0 + 0.2)
                return settle_boundary(sampler_tail(16), t, settle_ms)
            drained(t)
        time.sleep(0.4)
    raise Refusal("case", f"committed offset did not advance within {timeout}s")


def mib(v):
    """A docker memory string as MiB. "6g" -> 6144, "1024M" -> 1024."""
    if v is None:
        return None
    v = str(v).strip()
    if not v or not v[:-1].replace(".", "", 1).isdigit():
        return None
    return float(v[:-1]) * 1024 if v[-1] in "gG" else float(v[:-1])


def broker_cache_floor_mib():
    """The least page cache any recorded configuration produced a table with.

    Not a chosen number: read out of record/configs.json, where every entry is
    a configuration whose verdict is already known. Accepted entries leave the
    broker 4.25-5.00 GB after its heap. Clean-room run 31's default left 1.00 GB
    and lost a 44-minute suite -- the broker could not hold the backlog, read it
    back off disk, and three cases came back as ceilings.

    It is a floor from evidence, not a model: a much smaller backlog would
    presumably need less. It is checked at config time rather than from a
    running broker's limit hits because hits do not separate the two outcomes --
    run 23's 1-core case hit the limit 9,437 times at 99.6% of cap with no
    effect on its rate, and its table was accepted.
    """
    path = os.path.join(HERE, "record", "configs.json")
    if not os.path.exists(path):
        return None
    best = []
    for c in (json.load(open(path)).get("configs") or []):
        if c.get("expect") != "accept":
            continue
        caps = c.get("caps") or {}
        total, heap = mib(caps.get("kafkaMemory")), mib(caps.get("kafkaHeap"))
        if total and heap:
            best.append(total - heap)
    return min(best) if best else None


def no_result_reason(table, runs):
    """Pure. Why this run has no scaling result, or None when it has one.

    Comparing one core count with another IS the measurement, so a run whose
    surviving cases are all at the same size has measured nothing -- however
    many passes it ran and however clean they were. Clean-room run 46 kept two
    of ten cases, both at four cores, and results/DONE said "PASS 58.8 min".
    """
    if [r for r in (table.get("stepRatios") or []) if r.get("reportable")]:
        return None
    kept = [cs for cs in (table.get("cases") or {}).values() if cs.get("reportable")]
    thrown = sum(1 for r in runs if r.get("status") != "OK")
    where = (", ".join(f"{cs['cores']} core{'' if cs['cores'] == 1 else 's'}" for cs in kept)
             or "no case at all")
    return {"measured": len(runs), "thrownOut": thrown, "usableAt": where,
            "sentence": (f"{len(runs)} cases were measured and {thrown} were thrown out, leaving "
                         f"usable readings at {where}.")}


def missing_steps(case_list, table, runs):
    """Pure. The steps the configuration asks for that have no counted number.

    no_result_reason catches a run with no step at all. This catches the run
    that lost one of them: clean-room run 50 asked for 1, 2 and 4 cores, every
    1-core pass was thrown out, and the report judged 2->4 alone and said PASS.
    Returns [{"step": "1->2", "why": "..."}], empty when every step counted."""
    want = sorted(set(int(c) for c in case_list or []))
    have = {st.get("step"): st for st in (table.get("stepRatios") or [])}
    cases = {int(k): v for k, v in (table.get("cases") or {}).items()}
    out = []
    for a, b in zip(want, want[1:]):
        name = f"{a}->{b}"
        st = have.get(name)
        if st and st.get("reportable"):
            continue
        why = []
        for c in (a, b):
            cs = cases.get(c)
            if cs is None:
                gone = [r for r in runs if int(r.get("cores", 0)) == c]
                first = next((r.get("ceiling") or r.get("refusal") for r in gone
                              if r.get("status") != "OK" and (r.get("ceiling") or r.get("refusal"))), "")
                reason = re.split(r"(?<=[a-z%)])\. ", first, maxsplit=1)[0].rstrip(".")
                why.append(f"every {c}-core pass was thrown out ({len(gone)} of {len(gone)})"
                           + (f": {reason}" if reason else ""))
            elif not cs.get("reportable"):
                why.append(f"the {c}-core case does not count: {cs.get('unreportableReason')}")
        if not why and st:
            why.append((st.get("reason") or "").replace("voided: ", ""))
        out.append({"step": name, "why": "; ".join(w for w in why if w) or "no number"})
    return out


def broker_held_back(cases):
    """The case the broker could have held back, or None: the one with the most
    broker limit hits among cases whose worker was under its cap floor. A case
    at its cap was the constraint itself, whatever the broker did."""
    held = [r for r in cases if (r.get("brokerLimitHits") or 0) > T["brokerLimitHits"]
            and (r.get("tmCapFrac") or 0) < T["brokerHitsCapExempt"]]
    return max(held, key=lambda r: r.get("brokerLimitHits") or 0) if held else None


def size_broker_memory(limit_bytes, hits):
    """What Kafka's memory should be, given that it ran out `hits` times.

    Mirrors size_backlog: the tiny proof measures, and the caller is told the
    number to set before the suite is spent rather than after. The step is the
    one the broker ceiling already uses and the one run 21 measured -- 3,840
    MiB gave 995 hits, 6,144 gave none, so x1.6 rounded up to 256 MiB.

    Returns None when nothing needs changing.
    """
    if hits <= T["brokerLimitHits"] or not limit_bytes:
        return None
    return int(limit_bytes * 1.6 / 268435456) * 256


def broker_advice_fits(want_mb):
    """Can this machine give the broker what it is about to be told to give it?

    The sizing itself is right -- clean-room run 45 followed it from 4,096m to
    6,400m and the limit hits went from 1,771 to zero at every case. But on its
    9,937 MiB VM that left the four-core worker at 77.8% of cap against 94.6%
    before, because 3,840 of worker + 6,400 of broker + 1,600 of job manager is
    11,840 MiB. The advice was correct and unaffordable at the same time, and
    it said nothing about the second half.

    Returns None when it fits, or a sentence saying what to do instead.
    """
    c = cfg()
    info = sh("docker info --format '{{.MemTotal}}'", check=False).stdout.strip()
    vm = (int(info) / 1048576.0) if info.isdigit() else 0.0
    if not vm or not want_mb:
        return None
    top = max(c.cases)
    worker = (_mib(mem_for(c.tm_mem_per_core, top, c.tm_mem_base)) if c.tm_mem_per_core
              else _mib(c.tm_mem)) if tm_memory_capped() else 0.0
    need = worker + want_mb + 1600.0
    if need <= vm:
        return None
    over = need - vm
    smaller = sorted(n for n in set(c.cases) if n != top)
    return (f"but this machine cannot give it that. At {top} cores the worker wants "
            f"{worker:,.0f} MiB, the job manager takes 1,600, and {want_mb:,}m of broker on top "
            f"is {need:,.0f} MiB against a {vm:,.0f} MiB virtual machine -- over by {over:,.0f}. "
            f"Raising the broker anyway does not fail; the worker gives way instead, which reads "
            f"as a pipeline that does not scale. Three ways out, in order: raise the virtual "
            f"machine to about {need/1024:.0f} GB; or drop the {top}-core case and claim the step "
            f"into {smaller[-1] if smaller else top} cores; or keep the broker where it is, let "
            f"the {top}-core case come back as a ceiling, and report it as where scaling stops.")


def tm_memory_record():
    """What suite.json should say was held still about worker memory.

    Nothing, when nothing was capped. One figure when one flat figure was set.
    A figure per case when memory is a base plus a per-core share -- because
    then there is no single number, and recording Cfg.tm_mem's 4096m default
    is recording a setting that was never applied. Clean-room run 36's
    heldStill said 4096m while its own scorecard, correctly, said 1728m /
    2368m / 3648m.
    """
    c = cfg()
    if not tm_memory_capped():
        return None
    per = {str(n): tm_mem_of(n) for n in sorted(set(c.cases))}
    if len(set(per.values())) == 1:
        return per[str(sorted(set(c.cases))[0])]
    return {"perCase": per, "rule": "tmMemoryBase + tmMemoryPerCore x cores, so every "
                                    "subtask gets the same; the container figure is not held still"}


def tm_mem_of(cores):
    """What this case was actually given, not Cfg.tm_mem's 4096m default.

    With tmMemoryPerCore set, preflight prints 2560m / 3072m / 4096m and the
    scorecard printed 4096m on every row -- the default, which pipeline.json
    never set. The same phantom went into suite.json as heldStill. lib.py
    already records why that is wrong for the uncapped path; this is the
    capped one, found by clean-room run 35.
    """
    c = cfg()
    if not tm_memory_capped():
        return "uncapped"
    over = (c.per_case.get(cores) or {}).get("tmMemory")
    if over:
        return over
    if c.tm_mem_per_core:
        return mem_for(c.tm_mem_per_core, cores, c.tm_mem_base)
    return c.tm_mem


def bottleneck(rec):
    """What was holding this case back, in words, from what was measured.

    Every figure here is already recorded per case and was already used by the
    guards; nothing new is measured and nothing is inferred. The order is the
    order the guards themselves apply, so this never disagrees with a ceiling
    the run reported.

    The good answer is "the worker's cores" -- that is the component under test
    being the constraint, which is the condition the whole table depends on.
    """
    cap = rec.get("tmCapFrac") or 0
    n = rec.get("cores")
    # "99% of its cores" leaves the reader asking how many. Say the number when
    # the record carries it.
    its = f"its {n} cores" if n else "the cores it was given"
    if (rec.get("brokerLimitHits") or 0) > T["brokerLimitHits"] and cap < T["brokerHitsCapExempt"]:
        return (f"Kafka's memory, blocking higher throughput. Kafka hit its limit "
                f"{rec['brokerLimitHits']:,} times and had to read the test data back off disk, so the "
                f"pipeline was waiting on Kafka rather than using its CPU.")
    if (rec.get("gcFracOfCapacity") or 0) > T["gcCeil"]:
        return (f"Memory, blocking higher throughput. The pipeline spent {rec['gcFracOfCapacity']:.0%} "
                f"of the time cleaning up memory instead of working. Give it more memory, not more CPU.")
    if (rec.get("sourceIdle") or 0) > T["sourceIdleCeil"]:
        return (f"Nothing to read, blocking higher throughput. The pipeline sat idle "
                f"{rec['sourceIdle']:.0%} of the time waiting for input, so whatever feeds it is the "
                f"slow part, not the pipeline.")
    c = cfg()
    kcap = getattr(c, "kafka_cap", 0) or 0
    if kcap and (rec.get("kafkaCores") or 0) / kcap >= 0.90:
        return (f"Kafka's CPU, blocking higher throughput. Kafka used {rec['kafkaCores']:.2f} of the "
                f"{kcap:g} cores it is allowed, so the pipeline was waiting on Kafka rather than "
                f"working.")
    if cap >= T["capFloorOther"]:
        return (f"CPU at {cap:.0%} of {its}, blocking higher throughput. That is what we want, "
                f"because CPU is what we are adding.")
    bp = rec.get("sourceBackpressured") or 0
    if bp >= 0.30:
        return (f"Waiting to write to Kafka, blocking higher throughput. CPU at only {cap:.0%} of "
                f"{its} and held up {bp:.0%} of the time, because Kafka could not accept records fast "
                f"enough.")
    # Nothing measured accounts for it. "Investigating" is the honest label and
    # it is also an instruction: go and find out.
    return (f"Investigating. CPU at only {cap:.0%} of {its}, so something else was blocking higher "
            f"throughput, and nothing we measured says what.")


def n_cores(n):
    """\"1 core\", not \"1 cores\"."""
    return f"{n} core" if n == 1 else f"{n} cores"


def bottleneck_short(rec):
    """The bottleneck in two or three words, for a column."""
    # Four of these name a column in the table, so a reader can look the answer
    # up rather than take it on trust.
    long = bottleneck(rec)
    for needle, label in (("CPU at", "Pipeline CPU"), ("Memory,", "Pipeline memory"),
                          ("Kafka's CPU", "Kafka CPU"), ("Kafka's memory", "Kafka memory"),
                          ("Waiting to write", "Kafka writes"),
                          ("Nothing to read", "Input feed"),
                          ("Investigating", "Investigating")):
        if long.startswith(needle):
            return label
    return "Investigating"


def gib_str(byts):
    """A byte count in gigabytes, the one scale the scorecard uses.

    Mixing scales made a row unreadable: the column said 4g and the advice
    beside it said 6400m, so the reader had to convert before seeing that one
    was half as much again as the other. Docker takes a decimal -- 6.25g is
    accepted and is exactly 6400 MiB -- so both can be gigabytes.
    """
    if not byts:
        return None
    return f"{byts / 1073741824:g}g"


def host_arm_note(step_name):
    """What this host's own bare cores did on the same step, or None.

    preflight measures it every run and saves it, and nothing ever put it in
    front of a reader. Clean-room run 44 missed 1.90x on both steps with every
    diagnostic column saying the pipeline was the constraint; the figure that
    says whether 1.90x was reachable on that host at all sat in preflight.json
    and appeared in no rendering of the result.
    """
    try:
        with open(os.path.join(cfg().results, "preflight.json")) as f:
            hs = (json.load(f) or {}).get("hostScaling") or {}
    except Exception:
        return None
    of = hs.get("ofLinear") or {}
    alu = (of.get("alu") or {}).get(step_name)
    mem = (of.get("mem") or {}).get(step_name)
    if alu is None and mem is None:
        return None
    parts = []
    if mem is not None:
        parts.append(f"{mem:.3f} when it has to reach memory")
    if alu is not None:
        parts.append(f"{alu:.3f} when it does not")
    return ("this host's own cores returned " + " and ".join(parts)
            + " on the same step, with no pipeline involved")


def corrective_action(rec, step=None, is_baseline=False):
    """What to do, in two or three words, for the column.

    The numbers behind it go in action_detail, below the table. Putting them
    in the row took it to 175 characters, which is not a table anyone reads.
    """
    label = bottleneck_short(rec)
    if label == "Pipeline CPU":
        if is_baseline:
            return "check it matches"
        if not step or not step.get("reportable"):
            return "no usable step"
        if (step.get("ratioLowCI") or 0) > step["idealRatio"]:
            return "baseline reads low"
        if not step.get("meetsClaim"):
            # Run 44 was told "investigate" twice while every column beside it
            # said the pipeline was the constraint, the broker was idle and the
            # GC was at 0.3%. There was nothing in the rig left to investigate:
            # the step is the pipeline's, or the host's.
            return "check the host"
        return "add cores for more"
    return {"Pipeline memory": "more memory",
            "Kafka CPU": "more Kafka cores",
            "Kafka memory": "raise kafkaMemory",
            "Kafka writes": "compress the writes",
            "Input feed": "speed up the input"}.get(label, "investigate")


def action_detail(rec, cores, step=None, is_baseline=False):
    """The sentence behind the advice, or None when the row speaks for itself."""
    label = bottleneck_short(rec)
    if label == "Pipeline CPU":
        if is_baseline:
            return (f"{n_cores(cores)} is the baseline. Do not tune it: making the baseline faster makes "
                    f"the step off it smaller, and the steps are what is being claimed. The only thing "
                    f"worth fixing here is a way it differs from the other cases — a different garbage "
                    f"collector, a different job graph, a cap that did not apply.")
        if not step or not step.get("reportable"):
            return f"{n_cores(cores)}: no usable step into this case, so there is nothing to judge it by."
        ratio, ideal = step["ratio"], step["idealRatio"]
        need = ideal * T["scalingFloor"]
        if (step.get("ratioLowCI") or 0) > ideal:
            return (f"{n_cores(cores)}: doubling gave {ratio:.2f}x, more than the {ideal:.2f}x a doubling "
                    f"can give, so the smaller case reads too low.")
        if not step.get("meetsClaim"):
            lo = step.get("ratioLowCI")
            # 1.93x is not short of 1.90x. What is short is the lower bound the
            # claim is judged on, and the line has to say which number it means:
            # clean-room run 36 printed "1.93x, short of the 1.90x target" on
            # the same page as a step line that got it right.
            if ratio >= need:
                if lo:
                    return (f"{n_cores(cores)}: doubling gave {ratio:.2f}x, which clears the {need:.2f}x "
                            f"target — but the readings are far enough apart that it could be as low as "
                            f"{lo:.2f}x, and the lower bound is what is judged.")
                return (f"{n_cores(cores)}: doubling gave {ratio:.2f}x, which clears the {need:.2f}x "
                        f"target, but the step was not judged met — the claim is read from the "
                        f"lower bound across passes, not this figure.")
            return f"{n_cores(cores)}: doubling gave {ratio:.2f}x, short of the {need:.2f}x target."
        return None
    if label == "Kafka memory":
        have = rec.get("brokerLimitBytes") or 0
        want = size_broker_memory(have, rec.get("brokerLimitHits") or 0)
        if want:
            return (f"{n_cores(cores)}: raise kafkaMemory from {gib_str(have)} to about "
                    f"{gib_str(want * 1048576)}.")
    return None


def scorecard(out):
    """The run in the fewest lines that still say what happened."""
    t = out.get("table") or {}
    c = cfg()
    L = ["SCORECARD", ""]
    kcap = getattr(c, "kafka_cap", 0) or 0
    kmem = getattr(c, "kafka_mem", "") or "?"
    # the step that ends at each case, so the advice can ask whether it doubled
    step_into = {r["to"]: r for r in (t.get("stepRatios") or []) if r.get("to") is not None}
    # The configured baseline, not the lowest case left in the table: run 50 lost
    # every 1-core pass and was told "2 cores is the baseline. Do not tune it".
    lowest = out.get("baseline") or min((cs["cores"] for cs in t.get("cases", {}).values()), default=None)
    # Each column is what it was given, then how much of it was used, so a
    # reader sees the size and the utilisation without looking anything up.
    # One definition of the widths, used by the header and by every row, so the
    # two cannot drift apart. They did: each hard-coded its own numbers.
    W = dict(cores=5, speed=13, scaling=10, cpu=14, mem=18, kcpu=13, kmem=20, blocked=16)
    L.append(f"  {'cores':>{W['cores']}}{'speed':>{W['speed']}}{'scaling':>{W['scaling']}}   "
             f"{'pipeline CPU':>{W['cpu']}}{'pipeline memory':>{W['mem']}}"
             f"{'Kafka CPU':>{W['kcpu']}}{'Kafka memory':>{W['kmem']}}   "
             f"{'blocked by':<{W['blocked']}}what to do")
    # the key for those pairs goes under the table, where there is room for words

    notes = []
    for cs in t.get("cases", {}).values():
        last = next((r for r in reversed(out.get("runs") or [])
                     if r.get("cores") == cs["cores"] and r.get("status") in ("OK", "CEILING")), None)
        if not last:
            L.append(f"  {cs['cores']:>{W['cores']}}"
                     f"{cs['meanRecordsPerSec']:>{W['speed'] - 2},.0f}/s{'—':>{W['scaling']}}   "
                     f"{'—':>{W['cpu']}}{'—':>{W['mem']}}{'—':>{W['kcpu']}}{'—':>{W['kmem']}}   "
                     f"{'Investigating':<{W['blocked']}}find out what it is")
            continue
        gc = last.get("gcFracOfCapacity")
        kc = last.get("kafkaCores")
        hits = last.get("brokerLimitHits")
        cpu = "{} / {:.0%}".format(cs["cores"], last.get("tmCapFrac") or 0)
        mem = "{} / {:.1%}".format(tm_mem_of(cs["cores"]), gc) if gc is not None else "—"
        kcpu = "{:g} / {:.0%}".format(kcap, kc / kcap) if kc is not None and kcap else "—"
        # from the record, not from today's pipeline.json: a report rendered
        # against a changed config would otherwise show a limit the run never
        # had, beside advice computed from the limit it did have.
        # "6.25g / 0" needs the key to make sense; "6.25g, never full" does not.
        size = gib_str(last.get("brokerLimitBytes")) or kmem
        kmemcol = ("—" if hits is None else
                   f"{size}, never full" if hits == 0 else f"{size}, full {hits:,}x")
        # A dropped row is marked where the row is named, not after the advice:
        # "raise kafkaMemory to 6400m (not in the table)" read as one sentence.
        mark = "" if cs.get("reportable") else " *"
        st = step_into.get(cs["cores"])
        scale = f"{st['ratio']:.2f}x" if st and st.get("reportable") else "—"
        L.append(f"  {str(cs['cores']) + mark:>{W['cores']}}"
                 f"{cs['meanRecordsPerSec']:>{W['speed'] - 2},.0f}/s{scale:>{W['scaling']}}   "
                 f"{cpu:>{W['cpu']}}{mem:>{W['mem']}}{kcpu:>{W['kcpu']}}{kmemcol:>{W['kmem']}}"
                 f"   {bottleneck_short(last):<{W['blocked']}}"
                 f"{corrective_action(last, step_into.get(cs['cores']), cs['cores'] == lowest)}")
        detail = action_detail(last, cs["cores"], step_into.get(cs["cores"]),
                               cs["cores"] == lowest)
        if detail:
            notes.append("  " + detail)
        if not cs.get("reportable"):
            notes.append(f"  * the {cs['cores']}-core row is not counted in the table: "
                         f"{cs.get('unreportableReason')}. Its numbers are still shown, "
                         f"and what to do about them still applies.")
        # the full sentence only where it is not the answer we hoped for
        if bottleneck_short(last) != "Pipeline CPU":
            notes.append(f"  {n_cores(cs['cores'])}: {bottleneck(last)}")
    busy = [(cs["cores"], r.get("hostLoadClose") or r.get("hostLoadOpen"), r.get("hostCores"))
            for cs in (t.get("cases") or {}).values()
            for r in (out.get("runs") or [])
            if r.get("cores") == cs["cores"] and (r.get("hostLoadClose") or 0) > (r.get("hostCores") or 1e9)]
    seen = {}
    for cs in (t.get("cases") or {}).values():
        for r in (out.get("runs") or []):
            if r.get("cores") == cs["cores"] and r.get("gcNames"):
                seen[cs["cores"]] = ",".join(r["gcNames"])
    if len(set(seen.values())) > 1:
        notes.append("  the cases did not all run the same garbage collector: "
                     + "; ".join(f"{k} core{'' if k == 1 else 's'} {v}" for k, v in sorted(seen.items()))
                     + ". A case with a different collector is a different program, so the step into "
                       "it is not a scaling measurement. Pin it in flinkProperties "
                       "(env.java.opts.taskmanager: -XX:+UseG1GC) and measure again.")
    notes.extend(swap_note(out.get("runs") or [], t.get("cases")))
    if busy:
        worst = max(busy, key=lambda x: x[1])
        # which pass, not only how busy. Run 44 read "load reached 15.4 on 8
        # cores" and had to open suite.json to find out which of ten passes it
        # was talking about, and whether the fast ones or the slow ones were
        # the busy ones.
        who = next((f"the {r['cores']}-core {r.get('pass') or 'pass'}" for r in (out.get("runs") or [])
                    if (r.get("hostLoadClose") or 0) == worst[1]), None)
        where = f"{who}: " if who else ""
        # The load counts this run's own containers too. Runs 48, 49 and 50 were
        # told "something else" when nothing else was running.
        c = cfg()
        busiest = next((r for r in (out.get("runs") or []) if (r.get("hostLoadClose") or 0) == worst[1]), {})
        own = (busiest.get("cores") or 0) + c.kafka_cap + c.jm_cap
        notes.append(f"  {where}the machine's load reached {worst[1]:.1f} on {worst[2]} cores while measuring. "
                     f"This run's own containers are allowed about {own:.1f} of them, so some or all of "
                     f"that is the run itself. If this pass reads like the others at its size it cost "
                     f"nothing; if it does not, close what else is running and measure again.")
    L.append("")
    L.append("  Each pair is what it was allowed and how much of that went:")
    L.append("    scaling           what the step into this case gave — nothing on the baseline")
    L.append("    pipeline CPU      cores it could use / how much of them it used")
    L.append("    pipeline memory   memory it could use / share of the time spent tidying memory up")
    L.append("    Kafka CPU         cores Kafka could use / how much of them it used")
    L.append("    Kafka memory      memory Kafka could use, and how often it filled up")
    L.append("")
    for n in notes:
        line = "  "
        for word in n.split():
            if len(line) + len(word) + 1 > 96:
                L.append(line)
                line = "      " + word
            else:
                line += (" " if line.strip() else "") + word
        L.append(line)
    if notes:
        L.append("")
    L.append("")
    for r in t.get("stepRatios") or []:
        need = r["idealRatio"] * T["scalingFloor"]
        if not r.get("reportable"):
            L.append(f"  {r['step']} cores: not reported — {r.get('reason')}")
            continue
        # A step above its ideal clears the target, because the target is a
        # floor -- but reporting that as a plain "met" contradicts the note
        # above it saying the smaller case reads low. Say what it is instead.
        lo = r.get("ratioLowCI")
        if (lo or 0) > r["idealRatio"]:
            verdict = f"above {r['idealRatio']:.2f}x, so the smaller case reads low"
        elif r.get("meetsClaim"):
            verdict = "met"
        elif lo and r["ratio"] >= need:
            # the number shown clears the target and the verdict says missed,
            # which reads as a broken tool unless it says what was judged
            verdict = (f"missed — the readings are far enough apart that it could be as low "
                       f"as {lo:.2f}x, and that is what is judged")
        else:
            verdict = "missed"
        L.append(f"  {r['step']} cores: doubling gave {r['ratio']:.2f}x, target {need:.2f}x"
                 f"  ->  {verdict}")
        if verdict.startswith("missed"):
            arm = host_arm_note(r["step"])
            if arm:
                text = (f"for comparison, {arm}. The target is "
                        f"{T['scalingFloor']:.2f} of linear.")
                line = "     "
                for word in text.split():
                    if len(line) + len(word) + 1 > 96:
                        L.append(line)
                        line = "         " + word
                    else:
                        line += " " + word
                L.append(line)
    return "\n".join(L)


def progress(line, pct=None, eta_s=None):
    """One line a person can read, in results/PROGRESS.txt.

    A chain takes two to three hours and used to say nothing until it was
    over. harness.log has everything but is written for whoever is debugging
    it; this file holds one sentence, overwritten, so `cat results/PROGRESS.txt`
    answers "where is it up to" without reading anything.
    """
    c = cfg()
    bar = ""
    if pct is not None:
        filled = int(round(pct * 20))
        bar = f"[{'#' * filled}{'.' * (20 - filled)}] {pct:>4.0%}  "
    eta = ""
    if eta_s and eta_s > 0:
        m = int(eta_s // 60)
        eta = f"  about {m // 60}h {m % 60:02d}m left" if m >= 60 else f"  about {m}m left"
    text = f"{time.strftime('%H:%M:%S')}  {bar}{line}{eta}"
    try:
        with open(os.path.join(c.results, "PROGRESS.txt"), "w") as f:
            f.write(text + "\n")
    except Exception:
        pass
    return text


def run_case_retrying(run, on_retry=None, attempts=2):
    """Run one case, retrying once if it fails on this case's own data.

    Section 6 has said to do this for as long as it has existed -- "Retry the
    same case once if the failure is plainly transient" -- and nothing did it.
    Clean-room run 31 lost a whole chain attempt to one tiny-proof case whose
    two vantage points disagreed by 5.7%; the same case passed on the very next
    attempt with nothing changed.

    Only a `case`-scope failure is retried. A `rig` failure is not transient --
    a cap that did not apply at one core will not apply at two -- and a ceiling
    is a result, not a failure, so neither is retried.

    `run` is called with no arguments and either returns or raises. `on_retry`
    is called with the failure that is about to be retried.
    """
    last = None
    for attempt in range(1, attempts + 1):
        try:
            return run()
        except CaseRefused as e:
            if e.refusal.scope != "case" or attempt == attempts:
                raise
            last = e
            if on_retry:
                on_retry(e, attempt)
    raise last


def drained(tick):
    """The backlog ran out under the job: a sizing error of the caller's, named as such."""
    if tick.get("endIn", 0) > 0 and tick["committed"] >= tick["endIn"]:
        raise Refusal("case", f"the backlog ran out ({tick['endIn']:,} records) before the window closed. "
                              f"Make it bigger. It has to cover warm-up plus the window at the fastest "
                              f"case's rate.")


def warmup_verdict(rates, elapsed_s):
    """Pure: is this sequence of interval rates a flat trend? Returns (ok, detail)."""
    n = len(rates)
    mean = sum(rates) / n
    if mean <= 0:
        return False, {"mean": mean}
    xs = list(range(n))
    xm = sum(xs) / n
    slope = sum((xs[i] - xm) * (rates[i] - mean) for i in range(n)) / sum((x - xm) ** 2 for x in xs)
    drift = abs(slope * (n - 1)) / mean
    scatter = (max(rates) - min(rates)) / mean
    ok = (drift < T["warmupFlatTol"] and scatter <= T["warmupScatterTol"] and elapsed_s >= T["warmupMinS"])
    return ok, {"rates": rates, "mean": mean, "drift": round(drift, 4), "scatter": round(scatter, 4),
                "warmupS": round(elapsed_s, 1)}


def job_health(jid):
    """Is the job actually running, and has it been restarting?

    Every loop that watches a drain watched the committed offset and nothing
    else, so a job that could not run at all looked exactly like a rate that
    would not settle. Clean-room run 45's four-core case was crash-looping on
    `OutOfMemoryError: Direct buffer memory` -- fifteen restarts -- and the
    harness stopped it twice with "the rate never settled down within 120s.
    Last readings were [], drift None, scatter None", then retried it, because
    nothing ever asked the engine how the job was. One REST call it already
    knew how to make.
    """
    out = {"state": None, "restored": None, "cause": None}
    try:
        out["state"] = rest(f"/jobs/{jid}")["state"]
    except Exception:
        pass
    try:
        counts = (rest(f"/jobs/{jid}/checkpoints") or {}).get("counts") or {}
        out["restored"] = counts.get("restored")
    except Exception:
        pass
    try:
        ex = rest(f"/jobs/{jid}/exceptions") or {}
        root = ex.get("rootException") or ""
        if not root:
            hist = (ex.get("exceptionHistory") or {}).get("entries") or []
            root = (hist[0].get("stacktrace") or "") if hist else ""
        for line in root.splitlines():
            line = line.strip()
            # the line that names the fault, not the frames under it
            if line.startswith("Caused by:") or (line and not line.startswith("at ") and ":" in line):
                out["cause"] = line[:200]
                break
    except Exception:
        pass
    return out


def job_is_failing(jid, restored_at_start):
    """A plain sentence when the job is not running, or None when it is fine."""
    if not jid:
        return None
    h = job_health(jid)
    if h["state"] and h["state"] not in ("RUNNING", "CREATED", "RESTARTING", "INITIALIZING"):
        return (f"the job is {h['state'].lower()}, not running"
                + (f": {h['cause']}" if h["cause"] else ". Nothing is reading the backlog."))
    n = h["restored"]
    if n is not None and restored_at_start is not None and n > restored_at_start:
        times = n - restored_at_start
        return (f"the job has restarted {times} time{'' if times == 1 else 's'} since this case "
                f"started. It is failing and recovering, not reading at an unsteady rate"
                + (f": {h['cause']}" if h["cause"] else ".")
                + " Fix the job; nothing about this is a measurement.")
    return None


def wait_flat(deadline_s, jid=None):
    """Warm up to a flat trend across N commit intervals, not a round number."""
    boundaries, last, detail = [], None, {}
    ramp = {"flatAtS": None, "flatAtRates": None}
    t0 = time.time()
    # what the job's restart count was before this case began, so a restart
    # during it is visible rather than inferred from a rate that wanders
    restored0 = (job_health(jid)["restored"] if jid else None)
    checked_at = time.time()
    k = T["warmupIntervals"]
    left = None  # records still unread, as of the last tick seen
    while time.time() - t0 < deadline_s:
        for t in sampler_tail(6):
            if t.get("committed", -1) < 0:
                continue
            if t.get("endIn"):
                left = t["endIn"] - t["committed"]
            if last is None or t["committed"] > last[1]:
                if last is not None and t["ts"] > last[0]:
                    boundaries.append((t["ts"], t["committed"]))
                last = (t["ts"], t["committed"])
            drained(t)
        if len(boundaries) >= k + 1:
            b = boundaries[-(k + 1):]
            rates = [(b[i + 1][1] - b[i][1]) / ((b[i + 1][0] - b[i][0]) / 1000.0) for i in range(k)]
            elapsed = time.time() - t0
            ok, detail = warmup_verdict(rates, elapsed)
            # When the trend first went flat, as opposed to when the floor let
            # the case start. warmupMinS is 90 s, measured on run 10 where the
            # ramp took 46-136 s -- on pipelines an order of magnitude slower
            # than the ones run now. The floor multiplied by the rate is what
            # sizes every backlog, so a fast pipeline pays for it in disk: at
            # 1.7 million records a second, 90 s of warm-up is 157,000,000
            # records before anything is measured. Whether a fast pipeline still
            # needs 90 s to settle has never been measured, because the results
            # kept only these four summary rates and threw the ramp away. Now it
            # keeps both figures, so the next few runs answer it from the record
            # instead of from an argument.
            if ramp["flatAtS"] is None and warmup_verdict(rates, T["warmupMinS"])[0]:
                ramp["flatAtS"] = round(elapsed, 1)
                ramp["flatAtRates"] = [round(r, 1) for r in rates]
            if ok:
                detail["rampFlatAtS"] = ramp["flatAtS"]
                detail["rampFloorS"] = T["warmupMinS"]
                detail["rampWaitedExtraS"] = (round(elapsed - ramp["flatAtS"], 1)
                                              if ramp["flatAtS"] is not None else None)
                detail["rampFlatAtRates"] = ramp["flatAtRates"]
                return detail
        # Ask the engine how the job is, every few seconds. Watching only the
        # offset makes a job that cannot run look like one that is slow.
        if jid and time.time() - checked_at > 8:
            checked_at = time.time()
            why = job_is_failing(jid, restored0)
            if why:
                raise Refusal("rig", why)
        time.sleep(0.5)
    # A backlog running out looks exactly like a rate that will not settle,
    # because it is one: the readings fall away as the source runs dry. Clean-room
    # run 32 read 325,710 -> 129,873 -> 40,460 -> 176,347 on a tiny backlog that
    # held 94 seconds at the rate the next case measured, against a 55-second
    # warm-up and a 30-second window. Say how much is left, so the cause is in
    # the message rather than in someone's head.
    short = ""
    if left is not None:
        rates_seen = [r for r in (detail.get("rates") or []) if r > 0]
        if rates_seen:
            secs = left / (sum(rates_seen) / len(rates_seen))
            short = (f" The backlog had {left:,} records left, about {secs:.0f}s at the rate it was "
                     f"reading. A backlog that runs dry while warming up cannot settle: size it for "
                     f"the warm-up plus the window plus two checkpoint intervals at the fastest "
                     f"case's rate.") if secs < 120 else f" The backlog had {left:,} records left."
    why = job_is_failing(jid, restored0) if jid else None
    if why:
        raise Refusal("rig", why)
    seen = [round(r) for r in (detail.get("rates") or [])]
    if not seen:
        # No rate at all is not a rate that failed to settle. Run 45 read
        # "Last readings were [], drift None, scatter None" and spent 25
        # minutes on it; the empty list was the whole story.
        raise Refusal("rig", f"the pipeline did not commit a single offset in {deadline_s:.0f}s, so there "
                             f"is nothing to measure. It is not reading the backlog. Check the job is "
                             f"running and that the task manager has the memory it asked for.")
    raise Refusal("case", f"the rate never settled down within {deadline_s:.0f}s.{short} It has to hold steady for "
                          f"{T['warmupMinS']:.0f}s, drifting less than {T['warmupFlatTol']:.0%} with scatter under "
                          f"{T['warmupScatterTol']:.0%}. The last readings were {seen} records a second, "
                          f"drifting {detail.get('drift')} with scatter {detail.get('scatter')}.")


def host_swap_mb():
    """How much of the host's memory is moved out to disk right now, in MB.

    Clean-room run 47's passes scattered 54-147% while every worker sat at its
    cap. Swap separated them on all twelve passes of the test that followed:
    the three slow ones were measured with the Mac 4.3-6.0 GB into swap, the
    nine fast ones at 2.3 GB or less, with the broker's memory changed and
    unchanged. Load average could not show it. None if the host will not say."""
    try:
        r = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True, timeout=5)
        m = re.search(r"used = ([\d.]+)M", r.stdout)
        if m:
            return round(float(m.group(1)), 1)
    except Exception:
        pass
    try:
        info = dict(line.split(":", 1) for line in open("/proc/meminfo"))
        kb = lambda k: float(info[k].split()[0])
        return round((kb("SwapTotal") - kb("SwapFree")) / 1024, 1)
    except Exception:
        return None


def swap_note(runs, cases):
    """A sentence when the host's swap, not the pipeline, split a case's passes.

    Reported, never a reason to throw a pass out: one episode is not a
    threshold. It speaks only for a case whose passes are already too far apart
    to count, and only when there is a point in rate order where every slower
    pass was measured with more memory moved out to disk than every faster one
    -- the pattern run 47 showed, where the split was 3 slow passes against 1.

    The separation must be at least half a gigabyte. That number is chosen,
    not measured, and sits between what was: swap moved 0.2 GB across a whole
    quiet suite (run 48) and 2.0 GB between run 47's slow and fast passes.
    Because this only adds a sentence, a wrong call costs a sentence."""
    notes = []
    min_gap_mb = 512.0
    for cs in (cases or {}).values():
        if cs.get("reportable"):
            continue
        ps = sorted((r for r in runs if r.get("cores") == cs["cores"] and r.get("recordsPerSec")
                     and r.get("hostSwapOpenMB") is not None and r.get("hostSwapCloseMB") is not None),
                    key=lambda r: r["recordsPerSec"])
        lo = lambda rs: min(min(r["hostSwapOpenMB"], r["hostSwapCloseMB"]) for r in rs)
        hi = lambda rs: max(max(r["hostSwapOpenMB"], r["hostSwapCloseMB"]) for r in rs)
        best = None
        for k in range(1, len(ps)):
            slow, fast = ps[:k], ps[k:]
            if lo(slow) - hi(fast) >= min_gap_mb:
                gap = fast[0]["recordsPerSec"] / slow[-1]["recordsPerSec"]
                if best is None or gap > best[0]:
                    best = (gap, slow, fast)
        if not best:
            continue
        _, slow, fast = best
        gb = lambda mb: f"{mb / 1024:.1f}"
        rates = lambda rs: ", ".join(f"{r['recordsPerSec']:,.0f}/s" for r in rs)
        which = lambda rs, word: f"the {word} pass" if len(rs) == 1 else f"the {word} passes"
        was = "was" if len(slow) == 1 else "were"
        notes.append(f"  {n_cores(cs['cores'])}: {which(slow, 'slower')} ({rates(slow)}) {was} measured while "
                     f"the machine had {gb(lo(slow))}-{gb(hi(slow))} GB of its memory moved out to disk, "
                     f"{which(fast, 'faster')} ({rates(fast)}) at {gb(lo(fast))}-{gb(hi(fast))} GB. The machine "
                     f"was short of memory, not the pipeline. Close what else is using memory and measure again.")
    return notes


def check_case(rec, cores, is_baseline):
    """Pure guards on a finished case record. Every refusal here is about the
    case's DATA. `selftest` feeds this synthetic records."""
    c = cfg()
    if rec["boundaries"] < 3:
        raise Refusal("case", f"only {rec['boundaries']} commit boundaries inside the window")
    if rec["recordsConsumed"] <= 0 or rec["elapsedS"] <= 0:
        raise Refusal("case", f"measured rate is not positive: {rec['recordsConsumed']} records in {rec['elapsedS']}s")
    if rec.get("rateSource", "").startswith("engine"):
        raise Refusal("case", "rate came from the engine, not the transport")
    if rec["vantageDisagreement"] > T["vantageTol"]:
        # Two causes account for nearly every case of this, and neither is
        # obvious from the numbers. Clean-room run 41 spent about 35 minutes
        # and one wrong fix on the second before measuring it.
        # Which cause, from the sign of the difference, instead of two causes
        # and a guess. Clean-room run 45 was thrown out at four cores five
        # times out of five with the outputs BEHIND the source, while the
        # message's second cause -- the outputs are where the pipeline is now
        # -- predicts them AHEAD. It then measured the discriminating test, one
        # variable, checkpoint interval 10 s against 5 s:
        #   4 cores  10.5% -> 1.19%   (1 core 0.35% -> 1.72%, 2 cores 1.19% -> 0.93%)
        # an 8.8x fall at the only case that ever missed, and the case was
        # accepted for the first time in that run. The step size was ruled out
        # by arithmetic (one row held 36,000 records; the gap was 236-327 rows'
        # worth) and so was the pipeline's own in-flight state, which does not
        # depend on the checkpoint interval at all.
        behind = rec["vantageSinkRecords"] < rec["recordsConsumed"]
        ratio = (c.ckpt_s / max(rec["elapsedS"], 1e-9)) if rec.get("elapsedS") else None
        if cfg().vantage_mode != "command":
            why = ""
        elif behind:
            why = ("\n  The outputs are BEHIND the source, which points at checkpoint phase. The "
                   "committed offset moves a whole checkpoint at a time; the outputs move "
                   f"continuously, so they trail by up to one interval. Here that interval is "
                   f"{c.ckpt_s:.0f}s against a {rec.get('elapsedS', 0):.0f}s window"
                   + (f" -- {ratio:.0%} of it" if ratio else "")
                   + ". Shorten checkpointMs or lengthen the window until that ratio is well "
                     "under the tolerance. Run 45 took its four-core disagreement from 10.5% to "
                     "1.19% by halving the interval, changing nothing else.")
        else:
            why = ("\n  The outputs are AHEAD of the source, which points at the step size: a "
                   "windowed pipeline's progress jumps by a whole window. Work out how many input "
                   "records one window holds and compare it with how many a measurement window "
                   "consumes at the SMALLEST case -- that case reads slowest and needs the finest "
                   "signal. The fix is in the data or the job, not the harness.")
        raise Refusal("case", f"the two ways of counting do not agree. The source says "
                              f"{rec['recordsConsumed']:,} records went through; the outputs account for "
                              f"{rec['vantageSinkRecords']:,.0f}. That is {rec['vantageDisagreement']:.1%} apart "
                              f"and the limit is {T['vantageTol']:.0%}. One of them is wrong, so neither can "
                              f"be used." + why)
    if rec["backlogRemaining"] < rec["recordsPerSec"] * c.ckpt_s:
        raise Refusal("case", f"the data ran out before the measurement finished: "
                              f"{rec['backlogRemaining']:,} records left at the end of the window, "
                              f"which is only {rec['headroomS']:.1f}s of work at this rate. The window "
                              f"needs at least one checkpoint interval of data left over, or its last "
                              f"moments measured a pipeline with nothing to read. Adding more data is "
                              f"the fix ONLY if the warm-up settled first time. If the warm-up failed "
                              f"and was retried, the retry read the data twice and the size was never "
                              f"the problem -- clean-room run 43 raised its backlog from 750 million "
                              f"records to 1.25 billion chasing this message, when what was actually "
                              f"wrong was a rate that would not settle.")
    # Two different problems, so two different messages: the old one reported a
    # sample count even when the real fault was that no reading came back at all.
    if rec.get("sourceIdle") is None:
        raise Refusal("case", "no back-pressure reading came back for the source, so there is no way to "
                              "tell whether the pipeline was working or waiting on Kafka.")
    if (rec.get("bpSamples") or 0) < T["minBpSamples"]:
        raise Refusal("case", f"only {rec.get('bpSamples') or 0} back-pressure readings landed inside the "
                              f"window and {T['minBpSamples']} are needed. Without enough of them there is "
                              f"no way to tell whether the pipeline was working or waiting on Kafka.")
    # Read back, not assumed: the setting is only what was asked for.
    odd = not_g1(rec.get("gcNames"))
    if odd:
        raise Refusal("rig", f"the case at {n_cores(cores)} ran the {', '.join(odd)} garbage collector, not G1. "
                             f"The skill measures on G1 only, and the harness sets it; something in the image "
                             f"or the configuration overrode it. Every case would run the same way, so the "
                             f"suite stops here.")
    floor = T["capFloorBaseline"] if is_baseline else T["capFloorOther"]
    if rec["tmCapFrac"] < floor:
        why = "Something else was holding it back"
        hits = rec.get("brokerLimitHits") or 0
        if hits > T["brokerLimitHits"]:
            lim = rec.get("brokerLimitBytes") or 0
            # measured 2026-09-07 (run 21): 3,840 MiB gave 995 hits and 6,144 gave none,
            # so the step that worked was x1.6. Named here so the next run raises it once.
            hint = (f" Raise caps.kafkaMemory from {lim / 1048576:.0f}m to about "
                    f"{int(lim * 1.6 / 268435456) * 256:.0f}m.") if lim else ""
            why = (f"Kafka hit its memory limit {hits:,} times during the window and had to read the "
                   f"backlog back off disk ({rec.get('brokerRefaults', 0):,} page refaults), which is the "
                   f"likely reason.{hint} It")
            raise Ceiling(f"the pipeline only used {rec['tmCapFrac']:.1%} of the {cores} cores it was given, and "
                          f"it needs {floor:.0%} to count. {why} shows where scaling stops, and is kept in the "
                          f"table and left out of the ratios.", rec)
        raise Ceiling(f"the pipeline only used {rec['tmCapFrac']:.1%} of the {cores} cores it was given, and it needs "
                      f"{floor:.0%} to count. {why}, so this case shows where "
                      f"scaling stops. It is kept in the table and left out of the ratios.", rec)
    # A worker at its cap is not waiting on the broker, whatever the broker's
    # cgroup is doing: broker limit hits on a pass at or above the cap floor are
    # reported and nothing more. Below the floor they are named as the likely
    # reason, above. (Measured twice: run 23's 1-core case hit the limit 9,437
    # times at 99.6% of cap with no rate effect, while its 4-core case hit it
    # zero times -- on an idle VM the page cache reaches the cgroup limit, and
    # under load global reclaim trims first.)
    if (rec.get("gcFracOfCapacity") or 0) > T["gcCeil"]:
        raise Ceiling(f"garbage collection used {rec['gcFracOfCapacity']:.1%} of this case's time and the "
                      f"limit is {T['gcCeil']:.1%}. The pipeline ran short of memory, not cores. Give it more "
                      f"memory instead of more cores.", rec)
    if rec["sourceIdle"] > T["sourceIdleCeil"]:
        raise Ceiling(f"the source sat idle {rec['sourceIdle']:.1%} of the window and the limit is "
                      f"{T['sourceIdleCeil']:.0%}. It spent that time waiting for input, so whatever feeds "
                      f"the pipeline is the bottleneck, not the cores.", rec)


def comparable_shape(shape):
    """The part of a shape that is the same job at a different size.

    graph_shape also keeps the running plan, so the report can draw the graph
    the engine served. The plan carries the job id and each vertex's
    parallelism, both of which differ on every case by design -- a new job id
    per submission, and the parallelism IS what is being varied. Comparing the
    whole dict therefore refused every case after the first, for any pipeline,
    as a rig-scope failure that stopped the chain. Clean-room run 41 found it,
    and found the reason nothing else had: its first case was always a ceiling,
    so shape_ref stayed None and the guard never ran. The fitter its pipeline
    got, the sooner the harness refused it.
    """
    return {k: v for k, v in (shape or {}).items() if k != "plan"}


def check_shape(shape, shape_ref):
    """GUARD: the job graph differs from the other cases — a RIG refusal."""
    a, b = comparable_shape(shape), comparable_shape(shape_ref)
    if shape_ref is not None and a != b:
        raise Refusal("rig", f"job graph shape differs from the other cases:\n{a}\nvs\n{b}")


def run_case(cores, pass_id, run_id, shape_ref, is_baseline, manifest,
             min_boundaries=None, min_window_s=None, ckpt_ms=None, warmup_max_s=None, reporter_s=None,
             kafka_cap=None, parallelism=None, manifest_path=None):
    """One measured case. The job is torn down on every exit path.
    kafka_cap: the broker's cap *during this case* — the ceiling run steps it
    below the configured one, and the fraction must be read against the step.
    parallelism: slots and job parallelism, when they are not the core count
    (plan 12 phase 1C: the one-core case at the suite's parallelism)."""
    c = cfg()
    kafka_cap = kafka_cap or c.kafka_cap
    par = parallelism or cores
    min_boundaries = min_boundaries or T["minBoundaries"]
    min_window_s = min_window_s if min_window_s is not None else T["minWindowS"]
    ckpt_ms = ckpt_ms or c.ckpt_ms
    ckpt_s = ckpt_ms / 1000.0
    group = f"{c.project}-{run_id}-c{cores}-{pass_id}"
    jid = None
    rec = {"cores": cores, "pass": pass_id, "group": group}
    try:
        free = host_free_bytes()
        rec["hostFreeGB"] = round(free / 1e9, 1)
        # GUARD: host free disk before the case, not after the disk is full
        if free < T["diskFloorBytes"]:
            raise Refusal("rig", f"host free disk {free/1e9:.1f} GB is below the floor {T['diskFloorBytes']/1e9:.0f} GB")
        assert_cluster_idle()
        delete_group(group)
        recreate_output_topics()
        verify_backlog(manifest)

        rec["nanoCpus"] = start_tm(cores, slots=par, reporter_s=reporter_s)
        rec["reporterS"] = reporter_s or T["reporterS"]
        rec["parallelism"] = par
        o = rest("/overview")
        # GUARD: parallelism == allocated slots (== cap unless overridden)
        if o["slots-total"] != par:
            raise Refusal("rig", f"slots-total {o['slots-total']} != parallelism {par}")
        rec["slotsTotal"] = o["slots-total"]

        start_sampler(group)
        rec["tSubmit"] = time.time()
        jid = submit_job(par, group, ckpt_ms)
        wait_running(jid, par)
        rec["jobId"] = jid
        o = rest("/overview")
        if o["slots-available"] != 0:
            raise Refusal("rig", f"job is not using every slot: {o}")
        shape = graph_shape(jid)
        rec["shape"] = shape
        check_shape(shape, shape_ref)

        rec["warmup"] = wait_flat(warmup_max_s or T["warmupMaxS"], jid=jid)
        rec["tSteady"] = time.time()

        open_tick = next_boundary()
        cpu0_tm, cpu0_k = cgroup_cpu(c.tm), cgroup_cpu(c.kafka)
        mem0_k = cgroup_mem(c.kafka)
        rec["tOpen"] = time.time()
        rec["open"] = open_tick
        want_progress = c.vantage_mode == "command"
        mpath = manifest_path or manifest_path_of(manifest)
        open_progress = progress_from_outputs(mpath) if want_progress else None
        # What else the machine was doing. A cgroup cap is a share, not a
        # guarantee of cycles: on a busy host a case reads 100% of its cap and
        # does less work for it, which is invisible in every other column.
        try:
            rec["hostLoadOpen"] = round(os.getloadavg()[0], 2)
            rec["hostCores"] = os.cpu_count()
            rec["hostSwapOpenMB"] = host_swap_mb()
        except Exception:
            pass
        boundaries, last, close_tick = 1, open_tick, None
        while True:
            tick = next_boundary(after=last)
            boundaries += 1
            last = tick
            elapsed = (tick["ts"] - open_tick["ts"]) / 1000.0
            if boundaries - 1 >= min_boundaries and elapsed >= min_window_s:
                close_tick = tick
                break
            if elapsed > T["windowMaxS"]:
                raise Refusal("case", f"window never closed within {T['windowMaxS']:.0f} s")
        cpu1_tm, cpu1_k = cgroup_cpu(c.tm), cgroup_cpu(c.kafka)
        mem1_k = cgroup_mem(c.kafka)
        rec["brokerLimitHits"] = mem1_k["limitHits"] - mem0_k["limitHits"]
        rec["brokerRefaults"] = mem1_k["refaults"] - mem0_k["refaults"]
        rec["brokerFileCacheBytes"] = mem1_k["fileCache"]
        rec["brokerLimitBytes"] = mem1_k.get("limitBytes") or 0
        rec["tClose"] = time.time()
        rec["close"] = close_tick
        close_progress = progress_from_outputs(mpath) if want_progress else None
        try:
            rec["hostLoadClose"] = round(os.getloadavg()[0], 2)
            rec["hostSwapCloseMB"] = host_swap_mb()
        except Exception:
            pass
        rec["boundaries"] = boundaries - 1

        bp = backpressure_in_window(rec["tOpen"], rec["tClose"])
        rec["gcNames"] = bp.get("_gcNames") or []
        elapsed = (close_tick["ts"] - open_tick["ts"]) / 1000.0
        d_committed = close_tick["committed"] - open_tick["committed"]
        rec["elapsedS"] = round(elapsed, 2)
        rec["recordsConsumed"] = d_committed
        rate = d_committed / elapsed if elapsed > 0 else 0.0
        rec["rateSource"] = "kafka committed offsets on " + c.topic_in
        rec["recordsPerSec"] = round(rate, 1)
        if c.out_per_in is not None:
            rec["outputRecsPerSec"] = round(rate * c.out_per_in, 1)

        implied, how = vantage_delta(open_tick, close_tick, open_progress, close_progress)
        rec["vantageSource"] = how
        rec["vantageSinkRecords"] = round(implied, 1)
        rec["vantageDisagreement"] = round(abs(implied - d_committed) / d_committed, 4) if d_committed else 1.0

        remaining = close_tick["endIn"] - close_tick["committed"]
        rec["backlogRemaining"] = remaining
        rec["headroomS"] = round(remaining / rate, 1) if rate else 0.0

        tm_cores = (cpu1_tm["usage_usec"] - cpu0_tm["usage_usec"]) / 1e6 / elapsed
        k_cores = (cpu1_k["usage_usec"] - cpu0_k["usage_usec"]) / 1e6 / elapsed
        rec["tmCores"] = round(tm_cores, 3)
        rec["tmCapFrac"] = round(tm_cores / cores, 4)
        d_per = max(1, cpu1_tm["nr_periods"] - cpu0_tm["nr_periods"])
        rec["tmThrottledPeriodsPct"] = round(100.0 * (cpu1_tm["nr_throttled"] - cpu0_tm["nr_throttled"]) / d_per, 1)
        rec["kafkaCores"] = round(k_cores, 3)
        rec["kafkaCap"] = kafka_cap
        rec["kafkaCapFrac"] = round(k_cores / kafka_cap, 4)

        gcm = bp.pop("_gc", {})
        # Flink 1.20 reports an "All" collector alongside each real one, so
        # summing every .Time counts the same milliseconds twice. Measured
        # 2026-09-09 on a running task manager: All.Time 15, G1 Young 15,
        # G1 Old 0. Every GC figure recorded before this was double, including
        # the ones gcCeil was derived from.
        gc_ms = (gcm.get("All.Time") if "All.Time" in gcm
                 else sum(v for k, v in gcm.items() if k.endswith(".Time")))
        rec["gcMsInWindow"] = round(gc_ms, 1)
        rec["gcFracOfCapacity"] = round(gc_ms / 1000.0 / (elapsed * cores), 4)
        src_name = [n for n in bp if c.source_match.lower() in n.lower()]
        src = bp[src_name[0]] if src_name else {}
        rec["sourceIdle"] = src.get("idle")
        rec["sourceBusy"] = src.get("busy")
        rec["sourceBackpressured"] = src.get("backPressured")
        rec["bpSamples"] = src.get("samples")
        rec["backpressure"] = bp

        check_case(rec, cores, is_baseline)
        rec["status"] = "OK"
        return rec, shape
    except Ceiling as e:
        # measured, kept, and excluded from the ratios: this case is where
        # scaling stopped, which is a finding rather than a broken measurement
        rec["status"] = "CEILING"
        rec["ceiling"] = e.msg
        raise CaseRefused(rec, Refusal("ceiling", e.msg))
    except Refusal as e:
        rec["status"] = "DROPPED"
        rec["refusalScope"] = e.scope
        rec["refusal"] = e.msg
        raise CaseRefused(rec, e)
    finally:
        try:
            cancel_job(jid)
        finally:
            # keep the ticks: a vantage refusal on the noise rig (4c, 33.9%, 2026-09-04)
            # could not be examined because the sampler went down with its log
            try:
                rec["ticks"] = sampler_ticks_since(int((rec.get("tOpen") or time.time()) * 1000) - 20000)
            except Exception:
                rec["ticks"] = None
            stop_sampler()
            stop_tm()


# ------------------------------------------------------------------ the table

def build_table(runs, cases_order=None, quick=False):
    """Pure: per-case means, spreads, reportability, step ratios, order effect.
    Fed by the suite, by `selftest`, and by `replay` over the record."""
    ok = {}
    ceilings = []
    for r in runs:
        if r.get("status", "OK") == "OK":
            ok.setdefault(int(r["cores"]), []).append(r)
        elif r.get("status") == "CEILING":
            ceilings.append({"cores": int(r["cores"]), "pass": r.get("pass"),
                             "recordsPerSec": r.get("recordsPerSec"),
                             "tmCapFrac": r.get("tmCapFrac"), "why": r.get("ceiling")})
    cases = {}
    for cores, rs in sorted(ok.items()):
        rates = [r["recordsPerSec"] for r in rs]
        mean = sum(rates) / len(rates)
        spread = (max(rates) - min(rates)) / mean if mean else 1.0
        entry = {"cores": cores, "passes": len(rates), "meanRecordsPerSec": round(mean, 1),
                 "minRecordsPerSec": min(rates), "maxRecordsPerSec": max(rates),
                 "spread": round(spread, 4), "reportable": True}
        if quick:
            entry.update(quickLook=True, publishable=False)
        if len(rates) < T["minPasses"] and not quick:
            entry.update(reportable=False,
                         unreportableReason=f"only {len(rates)} usable reading"
                                            f"{'' if len(rates) == 1 else 's'}, and "
                                            f"{T['minPasses']} are needed to see how much it wobbles")
        elif spread > T["spreadCeil"]:
            entry.update(reportable=False,
                         unreportableReason=f"its readings are {spread:.0%} apart and the limit "
                                            f"is {T['spreadCeil']:.0%}")
        for k in ("tmCores", "tmCapFrac", "tmThrottledPeriodsPct", "kafkaCores", "sourceIdle",
                  "sourceBackpressured", "headroomS", "gcFracOfCapacity"):
            vals = [r[k] for r in rs if r.get(k) is not None]
            if vals:
                entry[k] = round(sum(vals) / len(vals), 4)
        vd = [r["vantageDisagreement"] for r in rs if r.get("vantageDisagreement") is not None]
        entry["vantageDisagreementMax"] = round(max(vd), 4) if vd else None
        cases[cores] = entry
    # A ratio pairs two cases measured minutes apart, and this rig moves between
    # sessions by more than any case's own spread: the same build read 2->4 =
    # 1.793 one evening and 1.962 the next morning, 9.4% apart, while the suites
    # that produced them reported per-case spreads of 0.5-4.7%. Pair each case
    # with its *neighbour in time* and take the median of those pairs, so drift
    # that is slower than one case cancels instead of landing in the answer.
    adjacent = {}
    ordered = [r for r in runs if r.get("status", "OK") == "OK"]
    for a, b in zip(ordered, ordered[1:]):
        lo, hi = sorted((int(a["cores"]), int(b["cores"])))
        if lo == hi:
            continue
        ra = a if int(a["cores"]) == lo else b
        rb = b if int(b["cores"]) == hi else a
        adjacent.setdefault(f"{lo}->{hi}", []).append(rb["recordsPerSec"] / ra["recordsPerSec"])

    ratios = []
    ks = sorted(cases)
    for i in range(len(ks) - 1):
        a, b = ks[i], ks[i + 1]
        ca, cb = cases[a], cases[b]
        entry = {"step": f"{a}->{b}", "from": a, "to": b, "idealRatio": b / a}
        if ca["reportable"] and cb["reportable"]:
            r = cb["meanRecordsPerSec"] / ca["meanRecordsPerSec"]
            import statistics as _st, math as _math
            pairs = sorted(adjacent.get(entry["step"], []))
            if pairs:
                mid = pairs[len(pairs) // 2] if len(pairs) % 2 else (pairs[len(pairs) // 2 - 1] + pairs[len(pairs) // 2]) / 2
                entry.update(adjacentPairs=[round(x, 3) for x in pairs],
                             ratioAdjacent=round(mid, 3),
                             adjacentSpread=round((pairs[-1] - pairs[0]) / mid, 4) if mid else None)
                # How far the ratio could be from what this many pairs measured.
                # Measured 2026-09-08, one build, 14 cases in an hour, nothing
                # changed: 13 adjacent pairs gave 1.849 with sd 2.8%, so two
                # pairs -- what --quick buys -- carry +-3.9% at 95%. The claim is
                # judged against the near edge of that interval, not the point.
                sd = _st.stdev(pairs) / mid if len(pairs) > 1 else T["ratioSdFallback"]
                half = 1.96 * sd / _math.sqrt(max(len(pairs), 1))
                entry.update(ratioHalfWidth=round(half, 4),
                             ratioLowCI=round(mid * (1 - half), 3),
                             ratioHighCI=round(mid * (1 + half), 3))
            entry.update(ratio=round(r, 3),
                         ratioLow=round(cb["minRecordsPerSec"] / ca["maxRecordsPerSec"], 3),
                         ratioHigh=round(cb["maxRecordsPerSec"] / ca["minRecordsPerSec"], 3),
                         efficiency=round(r / (b / a), 4), reportable=True)
        else:
            entry.update(reportable=False,
                         voidedBy=[x for x in (a, b) if not cases[x]["reportable"]],
                         reason="voided: " + "; ".join(f"{x}c {cases[x]['unreportableReason']}"
                                                      for x in (a, b) if not cases[x]["reportable"]))
        ga, gb = case_collectors(ok.get(a, [])), case_collectors(ok.get(b, []))
        if entry.get("reportable") and ga and gb and ga != gb:
            # SKILL.md: a case with a different collector is a different program,
            # so the step into it is not a scaling measurement. This was a note
            # printed beside a passing table; the published demo's 1->2 passed on it.
            entry.update(reportable=False, voidedBy=[a, b],
                         reason=(f"voided: {a}c ran {', '.join(sorted(ga))} and {b}c ran "
                                 f"{', '.join(sorted(gb))} -- a different garbage collector is a "
                                 f"different program, so this is not a scaling step"))
        ratios.append(entry)
    order = {}
    for cores, rs in sorted(ok.items()):
        asc = [r["recordsPerSec"] for r in rs if str(r.get("pass", "")).endswith("asc")]
        desc = [r["recordsPerSec"] for r in rs if str(r.get("pass", "")).endswith("desc")]
        o = {"ascendingMean": round(sum(asc) / len(asc), 1) if asc else None,
             "descendingMean": round(sum(desc) / len(desc), 1) if desc else None}
        if asc and desc:
            o["descOverAsc"] = round((sum(desc) / len(desc)) / (sum(asc) / len(asc)), 4)
        order[cores] = o
    # sentinel: the baseline measured first and last. No threshold of its own —
    # a drift shows up as baseline spread against the 20% ceiling, which the
    # record has replayed; plan 12 phase 1 measured a 10% spread with no drift.
    sentinel = None
    sent = [r for r in runs if r.get("pass") == "sentinel" and r.get("status", "OK") == "OK"]
    if sent:
        last = sent[-1]
        first = next((r for r in runs if int(r["cores"]) == int(last["cores"]) and r is not last
                      and r.get("status", "OK") == "OK"), None)
        if first:
            f, l = first["recordsPerSec"], last["recordsPerSec"]
            sentinel = {"cores": int(last["cores"]), "firstPass": first.get("pass"), "firstRecordsPerSec": f,
                        "lastRecordsPerSec": l, "drift": round((l - f) / ((f + l) / 2), 4)}
    for r in ratios:
        if r.get("reportable") and r.get("efficiency") is not None:
            r["study"] = "capacity" if cfg().per_case else "scaling"
            # judge the claim on the interval, not the point: a two-pass ratio
            # carries about +-4% here, so a point estimate decides on noise
            # The burden is on the claim, so the *whole* interval must clear the
            # floor: a ratio that might be linear has not been shown to be.
            lo = r.get("ratioLowCI")
            eff_lo = (lo / r["idealRatio"]) if lo else r["efficiency"]
            r["meetsClaim"] = eff_lo >= T["scalingFloor"]
            r["claimJudgedOn"] = "lower bound of the ratio's interval" if lo else "point estimate"
            r["claimEfficiencyLow"] = round(eff_lo, 4)
            if not r["meetsClaim"]:
                r["claimShortfall"] = round(1 - eff_lo, 4)
    return {"cases": cases, "stepRatios": ratios, "orderEffect": order, "sentinel": sentinel,
            "ceilings": ceilings,
            "quickLook": quick, "publishable": not quick}


def suite_span(out):
    """When the suite started and finished, as epoch seconds.

    The dashboard is read after the run, and the run is a drain: every panel
    goes flat when the last case closes. A dashboard left on a five-minute
    default is empty for everyone who opens it afterwards, so the span it
    should be showing is printed beside the table (skill section 7).

    Prefers the epoch stamps the suite writes; falls back to the extremes of
    the cases' own window timestamps so a results file written by an older
    harness still reports a span. Returns None when neither is present."""
    a, b = out.get("startedAtEpoch"), out.get("savedAtEpoch")
    if not (a and b):
        ts = [r[k] for r in out.get("runs") or [] for k in ("tSteady", "tOpen", "tClose")
              if isinstance(r.get(k), (int, float))]
        if not ts:
            return None
        a, b = a or min(ts), b or max(ts)
    return (a, b) if b >= a else None


def span_line(out):
    """The span as one human interval plus the epoch-ms pair a dashboard URL
    takes, so a range that does not cover the suite is visible next to the
    numbers it failed to show."""
    span = suite_span(out)
    if not span:
        return None
    a, b = span
    fmt = lambda e: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e))
    m = int(round((b - a) / 60.0))
    dur = f"{m // 60}h {m % 60:02d}m" if m >= 60 else f"{m}m"
    return (f"{fmt(a)} -> {fmt(b)} {time.strftime('%Z', time.localtime(b))} ({dur})"
            f"  dashboard: from={int(a * 1000)}&to={int(b * 1000)}")


def render_table(out):
    """The table as text, from the results file. Every artifact that carries a
    number is generated from here."""
    t = out["table"]
    c = cfg()
    L = [scorecard(out), "", "=" * 118]
    if out.get("table", {}).get("quickLook"):
        L.append("!! QUICK LOOK — " + QUICK_BANNER)
        L.append("=" * 118)
    L.append(f"axis                 : {out['axis']}")
    L.append(f"API level            : {out['apiLevel']}")
    L.append(f"guarantee            : state = {out['guarantee']['state']}; sink = {out['guarantee']['sink']}")
    L.append(f"checkpoint interval  : {out['checkpointIntervalMs']} ms")
    L.append(f"build hash           : {out['buildHash']}  (completeness passed for {out['completenessBuild']})")
    L.append(f"passes per case      : {out['passesPerCase']}")
    L.append(f"study                : {out.get('study', 'scaling: every case configured identically')}")
    L.append(f"workload             : {workload_line(out)}")
    L.append(f"backlog              : {out['backlogRecords']:,} records, {out['partitions']} partitions"
             + (f", {c.out_per_in:g} outputs per input" if c.out_per_in is not None
                else "; outputs are not a constant multiple of the input"))
    sl = span_line(out)
    if sl:
        L.append(f"suite span           : {sl}")
    L.append("=" * 118)
    hdr = (f"{'cores':>5} {'pass':>8} {'records/s':>11} {'out rec/s':>11} {'tm cpu':>10} {'%cap':>6} "
           f"{'thr%':>5} {'kafka':>10} {'srcIdle':>8} {'srcBP':>7} {'hdrm':>6} {'vant':>6} {'status':>8}")
    L.append(hdr)
    L.append("-" * len(hdr))
    for r in out["runs"]:
        if r.get("status") in ("OK", "CEILING"):
            # A ceiling was measured: it keeps its rate and its resource columns,
            # and is excluded from the ratios rather than from the table.
            # A pipeline whose outputs are per window, not per input, leaves
            # outputsPerInput unset, and then this key is never written at all
            # (see where recordsPerSec is set). The MEAN row below has always
            # known that; this row did not, so every windowed pipeline lost its
            # entire report to a KeyError here -- including the skill's own
            # second shipped example. Clean-room run 42 finished with a 0-byte
            # suite.txt, no suite.md and no DONE.
            out_col = (f"{r['outputRecsPerSec']:>11,.0f}"
                       if r.get("outputRecsPerSec") is not None else f"{'-':>11}")
            L.append(f"{r['cores']:>5} {r['pass']:>8} {r['recordsPerSec']:>11,.0f} {out_col} "
                     f"{r['tmCores']:>6.2f}/{r['cores']:<3} {r['tmCapFrac']:>5.1%} {r['tmThrottledPeriodsPct']:>5.0f} "
                     f"{r['kafkaCores']:>6.2f}/{c.kafka_cap:<3g} {r['sourceIdle']:>7.1%} {r['sourceBackpressured']:>6.1%} "
                     f"{r['headroomS']:>5.0f}s {r['vantageDisagreement']:>5.1%} {r.get('status', 'OK'):>8}")
            if r.get("status") == "CEILING":
                L.append(f"        ceiling: {r.get('ceiling')}")
        else:
            L.append(f"{r['cores']:>5} {r['pass']:>8} {'—':>11} {'—':>11} {'—':>10} {'—':>6} {'—':>5} "
                     f"{'—':>10} {'—':>8} {'—':>7} {'—':>6} {'—':>6} {'FAILED':>8}")
            L.append(f"        refusal ({r.get('refusalScope')}): {r.get('refusal')}")
    L.append("-" * len(hdr))
    for cs in t["cases"].values():
        mark = "" if cs["reportable"] else f"   UNREPORTABLE ({cs['unreportableReason']})"
        out_col = (f"{cs['meanRecordsPerSec'] * c.out_per_in:>11,.0f}" if c.out_per_in is not None
                   else f"{'-':>11}")
        L.append(f"{cs['cores']:>5} {'MEAN':>8} {cs['meanRecordsPerSec']:>11,.0f} "
                 f"{out_col} "
                 f"{cs.get('tmCores',0):>6.2f}/{cs['cores']:<3} {cs.get('tmCapFrac',0):>5.1%} "
                 f"{cs.get('tmThrottledPeriodsPct',0):>5.0f} {cs.get('kafkaCores',0):>6.2f}/{c.kafka_cap:<3g} "
                 f"{cs.get('sourceIdle',0):>7.1%} {cs.get('sourceBackpressured',0):>6.1%} "
                 f"{cs.get('headroomS',0):>5.0f}s {cs.get('vantageDisagreementMax') or 0:>5.1%} "
                 f"  spread {cs['spread']:.1%}{mark}")
    L.append("=" * 118)
    for r in t["stepRatios"]:
        if r["reportable"]:
            L.append(f"STEP {r['step']} cores: {r['ratio']:.3f}x  (ideal {r['idealRatio']:.0f}x, "
                     f"target {r['idealRatio'] * T['scalingFloor']:.2f}x, range across passes "
                     f"{r['ratioLow']:.3f}x-{r['ratioHigh']:.3f}x)")
        else:
            L.append(f"STEP {r['step']} cores: NOT REPORTED — {r['reason']}")
    L.append("order effect (descending / ascending): " +
             str({k: v.get("descOverAsc") for k, v in t["orderEffect"].items()}))
    sd = t.get("sentinel")
    if sd:
        L.append(f"sentinel: {sd['cores']}c first {sd['firstRecordsPerSec']:,.0f} -> last {sd['lastRecordsPerSec']:,.0f} "
                 f"rec/s, drift {sd['drift']:+.1%} across the suite (counted in the {sd['cores']}c spread)")
    if out.get("stoppedEarly"):
        L.append(f"STOPPED EARLY: {out['stoppedEarly']['reason']} — {out['stoppedEarly']['message']}")
    L.append("=" * 118)
    return "\n".join(L)


# Fields of the manifest the harness reads for itself, so the workload line can
# report everything else the generator chose to record without knowing what any
# of it means. The record count is excluded separately, by whatever name
# manifestCountField gives it -- naming the demo's "tradeCount" here would put
# the default business case back into a harness that should not know it.
MANIFEST_OWN = ("seed", "count", "records", "outputsperinput")


def workload_line(out):
    """The generator's own shape figures, so two runs of "the same" workload can
    be told apart. Runs 27 and 28 differed by 32 symbol keys against 8, and runs
    21 and 26 by 32,768 against 64, with nothing in the table saying so.

    Whatever the generator put in its manifest is what gets reported. An earlier
    version looked for fields whose names contained "symbol", "account" or
    "key", which is the default business case written into a harness that is
    supposed to be indifferent to what you build: a pipeline about sensors or
    invoices got a line with nothing on it.
    """
    w = out.get("workload") or {}
    count_field = (cfg().count_field or "").lower()
    keys = [f"{k} {v:,}" for k, v in w.items()
            if isinstance(v, int) and not isinstance(v, bool)
            and k.lower() not in MANIFEST_OWN and k.lower() != count_field]
    fan = out.get("outputsPerInput")
    return (f"{out.get('backlogRecords', 0):,} records"
            + (", " + ", ".join(keys) if keys else "")
            + (f", {fan:g} outputs per input" if fan is not None else ""))


def render_markdown(out):
    """The same table for a report."""
    t = out["table"]
    c = cfg()
    L = ([f"> **QUICK LOOK — not a result.** {QUICK_BANNER}", ""]
         if out.get("table", {}).get("quickLook") else [])
    L += ["| field | value |", "|---|---|",
         f"| axis | {out['axis']} |", f"| API level | {out['apiLevel']} |",
         f"| guarantee | state: {out['guarantee']['state']}; sink: {out['guarantee']['sink']} |",
         f"| checkpoint interval | {out['checkpointIntervalMs']} ms |",
         f"| build hash | `{out['buildHash']}` (completeness passed for `{out['completenessBuild']}`) |",
         f"| passes per case | {out['passesPerCase']} |",
         f"| study | {out.get('study', 'scaling: every case configured identically')} |",
         f"| workload | {workload_line(out)} |",
         f"| rate source | committed broker offsets on `{c.topic_in}` |",
         f"| CPU source | cgroup `cpu.stat usage_usec` |"]
    sl = span_line(out)
    if sl:
        L.append(f"| suite span | {sl} |")
    L.append("")
    for r in t["stepRatios"]:
        if r["reportable"] and t.get("quickLook"):
            # one pass per case: min and max are the same measurement, so a
            # "range across passes" here would be an invented interval.
            L.append(f"**{r['step'].replace('->', '→')} cores: {r['ratio']:.2f}× "
                     f"(target {r['idealRatio'] * T['scalingFloor']:.2f}×) — one pass per case, no spread measured.**")
        elif r["reportable"]:
            L.append(f"**{r['step'].replace('->', '→')} cores: {r['ratio']:.2f}× "
                     f"(target {r['idealRatio'] * T['scalingFloor']:.2f}×), "
                     f"range {r['ratioLow']:.2f}–{r['ratioHigh']:.2f}× across passes.**")
        else:
            L.append(f"**{r['step'].replace('->', '→')} cores: not reported — {r['reason']}.**")
    kcap = getattr(c, "kafka_cap", 0) or 0
    step_into = {r["to"]: r for r in (t.get("stepRatios") or []) if r.get("to") is not None}
    # The configured baseline, not the lowest case left in the table: run 50 lost
    # every 1-core pass and was told "2 cores is the baseline. Do not tune it".
    lowest = out.get("baseline") or min((cs["cores"] for cs in t.get("cases", {}).values()), default=None)
    L += ["", "| cores | speed | scaling | pipeline CPU | pipeline memory | Kafka CPU | Kafka memory | "
          "blocked by | what to do |", "|---:|---:|---:|---|---|---|---|---|---|",
          "| | | what the step into it gave | cores it could use / how much it used | memory it could use / share of the time "
          "spent tidying memory up | cores Kafka could use / how much it used | memory Kafka could "
          "use / how many times it filled up | | |"]
    for cs in t.get("cases", {}).values():
        last = next((r for r in reversed(out.get("runs") or [])
                     if r.get("cores") == cs["cores"] and r.get("status") in ("OK", "CEILING")), None)
        if not last:
            L.append(f"| {cs['cores']} | {cs['meanRecordsPerSec']:,.0f}/s | — | — | — | — | — | "
                     f"Investigating | find out what it is |")
            continue
        gc = last.get("gcFracOfCapacity")
        kc = last.get("kafkaCores")
        hits = last.get("brokerLimitHits")
        kmem = getattr(c, "kafka_mem", "") or "?"
        cpu = "{} / {:.0%}".format(cs["cores"], last.get("tmCapFrac") or 0)
        mem = "{} / {:.1%}".format(tm_mem_of(cs["cores"]), gc) if gc is not None else "—"
        kcpu = "{:g} / {:.0%}".format(kcap, kc / kcap) if kc is not None and kcap else "—"
        # from the record, not from today's pipeline.json: a report rendered
        # against a changed config would otherwise show a limit the run never
        # had, beside advice computed from the limit it did have.
        # "6.25g / 0" needs the key to make sense; "6.25g, never full" does not.
        size = gib_str(last.get("brokerLimitBytes")) or kmem
        kmemcol = ("—" if hits is None else
                   f"{size}, never full" if hits == 0 else f"{size}, full {hits:,}x")
        mark = "" if cs.get("reportable") else " \\*"
        st = step_into.get(cs["cores"])
        scale = f"{st['ratio']:.2f}x" if st and st.get("reportable") else "—"
        L.append(f"| {cs['cores']}{mark} | {cs['meanRecordsPerSec']:,.0f}/s | {scale} | {cpu} | {mem} | "
                 f"{kcpu} | {kmemcol} | {bottleneck_short(last)} | "
                 f"{corrective_action(last, step_into.get(cs['cores']), cs['cores'] == lowest)} |")
    for cs in t.get("cases", {}).values():
        last = next((r for r in reversed(out.get("runs") or [])
                     if r.get("cores") == cs["cores"] and r.get("status") in ("OK", "CEILING")), None)
        if last and bottleneck_short(last) != "Pipeline CPU":
            L.append("")
            L.append(f"**{n_cores(cs['cores'])}:** {bottleneck(last)}")
    L += ["", "| cores | pass | records/s | tm cores | % of cap | throttled | broker cores | src idle | src BP | headroom | vantage |",
          "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in out["runs"]:
        if r.get("status") in ("OK", "CEILING"):
            mark = " **(ceiling)**" if r.get("status") == "CEILING" else ""
            L.append(f"| {r['cores']} | {r['pass']}{mark} | {r['recordsPerSec']:,.0f} | {r['tmCores']:.2f} | {r['tmCapFrac']:.1%} | "
                     f"{r['tmThrottledPeriodsPct']:.0f}% | {r['kafkaCores']:.2f} / {c.kafka_cap:g} | {r['sourceIdle']:.1%} | "
                     f"{r['sourceBackpressured']:.1%} | {r['headroomS']:.0f} s | {r['vantageDisagreement']:.2%} |")
        else:
            L.append(f"| {r['cores']} | {r['pass']} | FAILED ({r.get('refusalScope')}) — {r.get('refusal','')[:80]} | | | | | | | | |")
    L += ["", "| cores | passes | mean records/s | spread | reportable |", "|---:|---:|---:|---:|---|"]
    for cs in t["cases"].values():
        L.append(f"| {cs['cores']} | {cs['passes']} | {cs['meanRecordsPerSec']:,.0f} | {cs['spread']:.1%} | "
                 f"{'yes' if cs['reportable'] else 'no — ' + cs['unreportableReason']} |")
    sd = t.get("sentinel")
    if sd:
        L += ["", f"Sentinel: the {sd['cores']}-core case first ({sd['firstRecordsPerSec']:,.0f} rec/s) and last "
                  f"({sd['lastRecordsPerSec']:,.0f} rec/s), drift {sd['drift']:+.1%} across the suite; "
                  f"counted in that case's spread."]
    # The graph that ran, drawn from the plan the engine served rather than by
    # hand. Compare it with the picture in the interview: two runs in a row
    # built one market-value sink where the business case asks for two, and
    # neither drawing was ever held against the job.
    graph = graph_mermaid(((out.get("runs") or [{}])[0].get("shape") or {}).get("plan"))
    if graph:
        L += ["", "### The job graph that ran", "",
              "Read off the running plan, not drawn. Every case ran this shape — a row whose "
              "shape differed would have been thrown out.", "", "```mermaid", graph, "```"]
    return "\n".join(L) + "\n"
