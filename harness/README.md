# The harness

This directory is the skill's guards as code. **Use it verbatim.** An agent
following the skill supplies a pipeline and a `pipeline.json`; it does not
write a sampler, a suite runner, a spread rule, a warm-up rule or a report
table. Ten clean-room runs each rewrote those from prose, and every one
re-decided something the rule had already decided — what a failed check does, what
the window is anchored on, what counts as flat — and paid for it in hours.

**Write `pipeline.json` yourself, from the field table below. Do not copy an
example.** Two shipped examples exist to be *read* — one complete, valid
configuration of each shape — and every value in them belongs to the pipeline
they describe:

| file | the pipeline it configures |
|---|---|
| `pipeline.example.json` | outputs that grow with the input: positions per order, a constant five rows in, five rows out. Its second measurement divides sink rows by that constant |
| `pipeline.example.windowed.json` | outputs that are **per window**: one average per location per hour, however many readings arrived. No constant fan-out anywhere, so it supplies a command for the second measurement instead |

Copying is how one pipeline's numbers end up in another's run. Two clean-room
runs out of two took a shipped backlog count verbatim rather than deriving it
from their own measured rate, and one of them had not measured anything yet.
`outputsPerInput` is worse: its own comment says *derive it, do not copy this*,
and a wrong one makes the two measurements disagree by construction, which
reads as a broken rig rather than a wrong number.

```
H=~/.claude/skills/scalable-flink-skill/harness/prove.py
nohup python3 $H all > results/all.log 2>&1 &        # the whole chain below, one stack session;
                                                     # wait on results/DONE, read results/phases.log
```

`all` is `up → preflight → completeness → tinyproof → fill → suite → report`,
stopping at the first step that does not pass; `results/DONE` holds the
verdict and the wall time, `results/phases.log` the timestamps the harness
wrote (run 11 wrote its own by hand and spent 20 minutes between commands).
Wait with `until [ -f results/DONE ]; do sleep 30; done` and nothing more.

While it runs, `results/PROGRESS.txt` holds one sentence, overwritten — which
step of seven, which case of ten, a bar and an estimate of what is left:

```
16:12:44  [######..............]  30%  suite: case 4 of 10 (1 cores, pass p2-desc)  about 29m left
```

`cat` it when you have a reason to — a step boundary, a case that finished —
and not on a timer. Every check is a whole turn for an agent, and a turn
re-reads the conversation before it does anything: a three-hour chain polled
every thirty seconds is 360 of them against seven for the seven steps. Wait on
`results/DONE` and read `PROGRESS.txt` when something has happened.
`harness.log` has everything but is written for whoever is debugging it.
### Watching a run from an agent — read this before you start one

**During `tinyproof` and `down` the reaper kills any host process whose
command line names the project directory** — not only one naming `prove.py`.
A plain `tail -4 <project>/results/all.log` is enough to be killed, as clean-room
run 30 found; the author's own `pgrep -f 'prove.py all'` loop went the same way.
Check on a run from a script whose own command line does not contain the
project path. **That is the only advice that survives an agent harness**: a
tool that runs `bash -c "cd /path/to/project && until ...; do sleep 30; done"`
puts the path on the command line *because* it had to `cd`, so "a shell whose
working directory is the project" is unreachable by that route — clean-room
run 36 lost two waiting shells to exit 144 that way, and the tiny proof's own
self-test reported them. Copy the results path into a variable in a file the
shell reads, or watch from a directory that is not the project and refer to it
by a path the command line does not spell out.
**`--quick` is a smoke run, not a result.** `prove.py all --quick` runs two
passes per case instead of the configured number (the sentinel still follows,
so the baseline is measured three times). It measured *one* pass until
2026-09-06, when a one-pass ratio was found to wander: run 18's build read
1.539x from a single pass against 1.678x from three, because a ratio compounds
the error of both cases. Two passes reproduce the three-pass answer to 0.2% and stamps `quickLook`/`publishable:false`
on the table it writes, with a banner in `suite.md` and `suite.txt`. Every
per-case guard stays live, so it answers "does this rig run clean, and
roughly how fast" — about 48 min here against 60, because the gates
(completeness, tiny proof, fill) do not shrink. It answers nothing about the
ratio: replayed against the record, single passes of the recorded suites read
2.039-2.273x where the suite reported 2.154x, and 1.837-1.859x where it
reported 1.850x — a band wider than the accept line. Never quote a quick
table, and never put one in `record/`.

The flag reaches exactly one place — the number of passes for this run — and
is carried on the table it produced (`quickLook` in `suite.json`), never on
the record or the guards. The first version of it was a process-wide global
that reached `build_table`, and on 2026-09-05 that stopped a run twice, both
times correctly: `replay` re-derived the record with `minPasses` bypassed and
found three recorded-invalid suites it would now report, and the live
self-test's "a case measured only once" guard stopped firing. A guard that
does not fire, or a record that changes under a flag, is a broken harness.

**Give the broker enough memory to hold the working set.** The harness now
fails any case where the broker hit its container memory limit inside the
window. Measured 2026-09-05, one rig, one build, one backlog, one variable:
at a 2 GiB limit the broker hit it 310,423 times with 6.3M file-page refaults
and 649 MB of cache, and all three 4-core passes failed at 93.1-93.8%
of cap; at 4 GiB the same case held 99.6-100.1% of cap at 651,653 rec/s and
the cache grew to 2.14 GB. Back-to-back single cases minutes apart: 2 GiB
failed with 30,927 hits at 562,907 rec/s and **96.4% of cap** — above the
cap floor, so nothing else would have caught it — and 4 GiB clean with zero
hits at 646,423 rec/s. A 264M-record backlog wanted 4 GiB here.

**Pipeline memory is set per subtask, and passing nothing stops the run.** This
paragraph used to say memory was not capped by default, which contradicted
section 6 of the skill and the code, which stops the run: passing nothing does not
leave memory to the engine, it leaves the image's flat 1728m, so every case runs
on the same total and the largest one measures memory pressure rather than
cores. Clean-room run 36 read 2→4 at 1.510 on the image default against 1.743
with memory per subtask, and the GC ceiling did not catch it — GC was at its
lowest on the case losing the most. Set `caps.tmMemoryBase` and
`caps.tmMemoryPerCore` so every subtask in every case gets the same memory.
Earlier flat caps starved things instead, and runs 14, 18, 21, 23 and 25 each
lost time to that; what a scaling claim needs is that *CPU* is the constraint.
A case whose garbage collection takes more than 5.5% of its capacity is a
ceiling, not a result. That figure is measured, not chosen: across fourteen
recorded runs every case that behaved sat at 0.35-4.8%, and every case above
5.5% came with a distorted one — run 21's 1-core case at 13.2%, run 25's at
7.75% with a 14.3% spread and -14.5% sentinel drift, run 18's at 6.45%.

Those numbers are half what this repository recorded before 2026-09-09, when
run 28's agent noticed the harness was double-counting: Flink 1.20 reports an
`All` collector alongside each real one, and summing every `.Time` counts the
same milliseconds twice. Confirmed on a running task manager — `All.Time 15`,
`G1 Young Generation.Time 15`, `G1 Old Generation.Time 0` — so the harness now
takes `All` when it is present, and the ceiling was re-derived from the halved
record rather than left at a figure that no longer meant anything.

The example pipeline ships **no pipeline memory keys at all**, so a pipeline
copied from it inherits the uncapped default. Run 27's agent capped anyway
(`2048m + 256m` per core) because the example still carried those keys and it
copied them — which is how a default that exists only in the code fails to
reach the people using it.

Cap it deliberately if the study is about memory: `tmMemoryPerCore` (with
`tmMemoryBase`) gives every subtask the same, and `perCase` gives a case its
own. A flat `tmMemory` across more than one case still fails, because it
divides across each case's subtasks.

**When capped, pipeline memory is a fixed base plus a per-subtask share.**
`caps.tmMemoryBase` covers what does not scale with cores — metaspace, JVM
overhead, the network buffer floor — and `caps.tmMemoryPerCore` (with optional
`tmMemoryLimitPerCore`) is multiplied by the case's core count, so every case
gives each subtask the same memory; a flat `tmMemory` fails when there is
more than one case. Measured 2026-09-07 on one build, cap == parallelism,
cases interleaved: flat 2048m gave 2c 558,059 and 4c 917,807 rec/s — 2→4 =
1.645, GC 9.3% at four cores — and per-core memory gave 2c 549,380 (unchanged)
and 4c 1,049,130 — 2→4 = 1.910, GC 2.3%. The fourth core was starved of heap,
not short of CPU, and every case before this change shared that flaw.

Scaling the *whole* figure by cores then starves the other end: at 1280m per
core with no base, the rig read GC 17.4% at one core against 3.4% at two and
1.1% at four, because Flink's fixed overheads are most of a small process
size. Hence the base term.

**The tiny proof is the tuning loop's measurement, and it re-runs after the
fill.** It measures every case the suite will run on one build in minutes. Its
disk projection credits whatever the suite's input topic already holds, so a
backlog that is already on the broker is not projected a second time and the
topic does not have to be deleted and re-filled between changes.

**The tiny proof sizes the backlog.** It measures the largest case's rate and
fails the chain if `backlog.count` is short of what that case needs to
survive warm-up, the window and more than one checkpoint interval of headroom
(x1.5). Every clean-room run from 15 to 20 lost an attempt to a backlog sized
by guess before anything ran — run 18 sized for 500k rec/s against an actual
930k, run 20 drained 50M records mid-window. Preflight states the ceiling the
current guess covers, and the message names the number to use.

**Broker memory is named, not guessed.** When the broker hits its cgroup
limit inside a window the message now carries the figure to use — the step that
worked on this host was x1.6 (3,840 MiB gave 995 hits, 6,144 gave none) — and
preflight fails a configuration where the pipeline at its largest case plus the
broker plus the job manager do not leave the VM a spare gigabyte. Runs 20 and
21 lost five tiny proofs between them discovering both by trial.

**The claim is gated separately from the measurement.** A table can be beyond
reproach and still say the pipeline does not scale. `report` marks each step
`meetsClaim` against `scalingFloor` (1.90x on a doubling) and exits non-zero when a
step misses, so `all` ends FAIL rather than PASS. The floor comes from this
repository's own demo, which reads 1.99x from 2 to 4 cores on the same laptop,
less the +-3% a two-pass ratio carries. Replayed against the record before it
shipped: 1->2 meets it in 11 of 12 recorded runs, 2->4 in three.

When a step misses, the report prints both cases side by side — per-core rate,
cap, source idle, GC, back-pressure — and what this rig has already shown costs
what: pipeline memory that does not scale per subtask, about 14%; a broker
starved of page cache, about 13%; four subtasks instead of two on the same
cores, about 8%, of which roughly 3 points is the source idling. Partition
count (8 against 16) and network buffer fraction (0.15 against 0.30) were each
tested with the cases interleaved and changed nothing: 16 partitions failed
every parallelism-4 case for an unstable warm-up, and the buffers moved the
per-core rate 0.6%.

**The claim is judged on the interval, not the point, and the burden is on the
claim.** Each step carries `ratioLowCI`/`ratioHighCI` from the spread of its
adjacent pairs, and `meetsClaim` is true only when the *lower* bound clears
`scalingFloor`: a ratio that might be linear has not been shown to be. The
interval width comes from measurement, not assumption — 14 cases on one
unchanged build in an hour gave 13 pairs at 1.849 with sd 2.8%, so the two
pairs `--quick` buys carry about ±3.9% at 95% confidence, and `ratioSdFallback`
holds that figure for a step with a single pair.

Replayed against the record: of twelve recorded runs, one (run 12) has a 2→4
whose whole interval clears 1.90x on a doubling. That is the honest state of these
pipelines, not a reason to move the floor.

**Each step ratio also carries its adjacent pairs.** `ratioAdjacent` is the
median of the ratios between cases measured next to each other in time, with
`adjacentPairs` and `adjacentSpread` beside it. It is a diagnostic, not a cure:
run 23's pairs were 1.782 and 1.804 while the same build read 1.962 the next
morning — 9.4% away, with its 2-core figure down 5.6% and its 4-core up 3.3%.
Tight pairs next to a ratio that moved between sessions locate the movement
outside the suite. What moves it is not known.

**A case that is not the constraint is a ceiling, not a failure.** When the
pipeline sits below its cap, the source idles past the ceiling, or the broker
hits its memory limit while the pipeline is off its cap, the case is measured,
kept, reported as `CEILING` with its rate — and excluded from every ratio,
because a ratio built on it is not a statement about the component under test.
This repository's own demo publishes exactly such a row (8 units: 4.98 of 8
cores, 1.18x) and the harness used to delete it instead of saying what it
shows. `record/cases.json` holds the classifications whose answer is known —
run 5's retracted 94% case, the 2 GiB starved broker, run 23's 1-core case that
hit the broker limit at 99.6% of cap with no rate effect — and `replay` checks
them, because the suite record carries no cap fractions and cannot.

**Preflight measures what this host's own cores do.** `probe/Spin.java` runs
in the same image and under the same caps as the pipeline, in two arms: a
register-only loop and a random read-modify-write over 4 MB per thread. The
figures go into `preflight.json` as `hostScaling` and are printed beside any
missed claim, because a pipeline cannot beat its machine. This machine,
measured twice on different days: register-only 1→2 = 98%, 2→4 = 97%;
memory-bound 1→2 = 90%, 2→4 = **69-78%**. Every 2→4 figure in this record sits
between those bounds, and the days spent on broker caps, checkpoint intervals,
partition counts, network buffers, compression and fetch sizes were spent
inside a range the hardware had already fixed. The probe came from the agent of
clean-room run 24, which ran it before offering any mechanism of its own.

**Two studies, and the table says which.** By default every case is
configured identically, so the ratio is a property of the component: that is a
scaling proof. Declaring `perCase` in `pipeline.json` — `{"4": {"tmMemory":
"7168m"}}` — tunes a case separately and the table is stamped *capacity curve:
each case configured separately, declared before the run*. Both are legitimate
and they answer different questions: an operator buys against the best
configuration at each size, while a scaling claim needs everything but the
component held still. `perCase` must be in the file before the run, because
tuning after seeing the number turns a curve into a story. (This repository's
own demo sidesteps the question: its task manager has no memory limit at all,
so memory is never its constraint.)

**The table names the workload, not just the backlog.** `suite.json` keeps
every scalar the generator's manifest declares, and the rendered table shows
the key counts beside the record count. Without it two runs of "the same"
workload are indistinguishable in the results: runs 27 and 28 differed by 32
symbol keys against 8, runs 21 and 26 by 32,768 against 64, and their step
ratios were compared for days as though they were the same problem. Agents name
these fields differently, so the harness keeps them all rather than assuming a
schema.

Type the steps yourself only when one of them needs re-running:

```
python3 $H replay          # thresholds vs the recorded runs — seconds, no stack
python3 $H up              # stack/compose.yml generated, broker + job manager up, sampler compiled
python3 $H preflight       # §3, one PASS/FAIL row per check
python3 $H tinyproof       # two cases on a small backlog, ratio bounded, every guard broken on purpose
nohup python3 $H fill > results/fill.log 2>&1 &        # the full backlog; build the dashboard meanwhile
python3 $H completeness    # process a small test data set twice (once cleanly, once killed), check
python3 $H suite           # the table
python3 $H ceiling         # optional: starve the broker in steps at the largest case
python3 $H down            # everything this project started, gone; asserted; fstrim
```

Every command writes to `results/` next to `pipeline.json` and appends to
`results/harness.log`. `suite` will not start unless `tinyproof` (with its
self-test) and `completeness` have passed **for the same build hash**.

## What the pipeline supplies

| field | what |
|---|---|
| `project` | short lowercase token; every container, volume and network is prefixed with it, and `down` asserts nothing with the prefix survives — nor any host process holding a file under `results/` open, naming the project directory on its command line, or naming `prove.py` while running from inside the project (those are killed and listed; a survivor fails the run — another project's harness, or a shell merely sitting in the directory, is left alone) |
| `topics.in`, `topics.out[]` | **the topics the harness owns, not every topic the job uses.** `topics.out` may be empty when `secondVantage` is a command. `topics.in` is the one input it fills, measures and drains — the one being scaled to capacity. `topics.out` is every topic whose growth is a **constant** multiple of that input, because their rows divided by `outputsPerInput` are the second vantage point. A second *input* — a price feed, a reference stream — is yours: create it, fill it, read it, and leave it out of both lists; the harness will not touch it. A timer-driven *output* is also yours and must stay out of `topics.out`, because its rows are per interval and not per input, which is what makes the two vantage points disagree. Set `retention.bytes` on anything you own that is written and never drained (§3). The harness sets retention on `topics.out` and recreates them per case |
| `outputsPerInput` | records written to all outputs per input record. The two-vantage guard divides sink growth by this and compares to committed source offsets. **Optional**, and meaningless for a pipeline whose outputs are per window rather than per input — see `secondVantage` |
| `topicsAlsoWritten` | every topic the pipeline writes that the harness does not own — the throttled ones, the per-window ones, anything outside `topics.out`. The harness cannot infer them and cannot read a diagram, so the run declares them and the completeness drain stops the run on any that stayed empty. They are **emptied between the two completeness arms** along with `topics.out`, so a verifier asked for different assertions on the clean drain and the killed one is not handed both arms' rows at once. Set `retention.bytes` on them yourself at creation; the harness only applies its own retention when it recreates them |
| `secondVantage` | how the harness gets its second, independent reading of how much input went through. Default `{"mode": "constantFanOut"}`, which needs `outputsPerInput` and at least one topic in `topics.out`. For a pipeline with no constant fan-out anywhere — an hourly average per location emits one row an hour whatever the input rate — use `{"mode": "command", "cmd": "…"}`. The command is given `{bootstrapExt}`, `{jar}`, `{java}` and `{manifest}`, and prints one JSON object: `{"inputRecordsProcessed": N}`, how much input its own outputs account for. It runs at each end of the window and the difference is compared with the committed offsets. Declaring neither stops the run: one measurement cannot tell a fast pipeline from a stuck consumer group |
| `job.jar`, `job.mainClass`, `job.args` | the job. `args` is a template: `{bootstrap}` `{in}` `{out0}` `{out1}`… `{group}` `{par}` `{ckptMs}`. The job **must** consume `{in}` with consumer group `{group}`, commit offsets on checkpoint, and run at parallelism `{par}` |
| `job.sourceVertexMatch` | substring of the source vertex name in the running plan (busy/idle/back-pressure are read for it) |
| `generator.cmd` | fills `{topic}` with `{count}` records from `{seed}` and writes `{manifest}` (JSON) — deterministic, bootstrap `{bootstrapExt}` |
| `generator.manifestCmd` | same without producing (the determinism preflight runs it twice) |
| `generator.manifestCountField` | the manifest field holding the record count |
| `verifier.cmd` | reads the outputs and `{manifest}`; exits 0 iff every completeness assertion holds with no tolerance. It is also given `{arm}`, which is `clean` on the clean drain and `killed` on the one killed mid-run — section 4 asks for **different** assertions on the two (never backwards, against backwards at most once per key), and until the harness said which arm it was running every pipeline had to work it out for itself. A command that does not use `{arm}` is unaffected |
| `cases`, `baseline`, `passes` | the cases, which one is the baseline, passes per case (≥2; odd numbers alternate asc/desc/asc). The suite then measures the baseline once more as a **sentinel** — the first and last measurements of the suite are the same case, so a rig that drifts across the suite shows up as baseline spread rather than hiding inside the alternation. No threshold of its own: the 20% ceiling counts it. `suite.md` reports the first→last drift |
| `flinkProperties` | optional. Extra Flink settings, applied to the job manager and to every task manager. This is how a metrics reporter gets in -- section 7 asks for panels that need one, and the rest of the properties are the harness's. Anything that decides what is being measured stops the run: slots, parallelism, pipeline memory, the checkpoint store, and `metrics.reporter.slf4j.*`, which is where busy, idle and back-pressure are read from. Add settings, do not replace these |
| `flinkEnv` | optional. Environment variables for the job manager and every task manager. **Not needed for metrics on `flink:1.20.x`** — those images ship the reporters already installed under `/opt/flink/plugins/metrics-*`, so `flinkProperties` alone is enough, and setting `ENABLE_BUILT_IN_PLUGINS` there makes the entrypoint hunt for a jar in `/opt/flink/opt` that is not present and exit before the job manager starts. Use it for images that do keep reporters in `/opt/flink/opt`, or for any other environment the engine needs |
| `extraServices` | optional. Services to splice into the generated stack -- a dashboard, an exporter -- as a map of name to compose service body. The container name must start with the project prefix or `down` fails for a survivor it did not create, and it must carry a CPU cap: anything sharing the cores under test changes the number being measured. Recorded in `suite.json` under `heldStill.extraServices`. Section 7 of the skill asks for a dashboard and section 6 requires it to live in the compose file; this is how both are satisfied without forking the harness |
| `keySets` | which manifest fields hold a keyed stage's key set. **The key set must not depend on the record count**: preflight reads it from the manifest the determinism check generates at 200,000 records, so a generator whose keys grow with the backlog would be checked on the wrong set. Also, as `{"stage name": "manifest field"}`. Preflight asks Flink's own key-group assignment, out of the image under test, where each of those keys would land at every case's parallelism. A small key set does not spread over subtasks by itself: the demo's sixteen account keys land **5/3/4/4** at Flink's default of 128 key groups, which bounds that stage at 0.80 of linear at four cores. When that happens the row names the `pipeline.max-parallelism` to add to `flinkProperties` — 1115 for those key names, which makes it 4/4/4/4. Leave it out and the row fails saying it cannot check |
| `backlog.count`, `.seed`, `.smallCount`, `.tinyCount`, `.killAtFraction` | the drain backlog; the completeness backlog (must drain to the last record); the tiny-proof backlog; where the pipeline is killed. **Size the backlogs for the largest case's rate × (warm-up ceiling + window + two checkpoint intervals)**: the suite at up to 240 + 70 + 20 s, the tiny proof at 120 + 40 + 20 s. The completeness backlog must span **several checkpoint intervals** at the baseline rate, or the kill cannot land where `killAtFraction` says (offsets commit once per interval). A backlog that drains under the job fails the run, and says so. **On a first attempt you cannot know the rate** — that is what the tiny proof measures. Guess high, run the tiny proof, and re-size from the rate it reports: a wrong guess costs one tiny proof, a too-small suite backlog costs the suite. For a fast pipeline `tinyCount` is not small — clean-room run 30 needed 150,000,000, which was 43% of its suite backlog |
| `caps` | `kafka`, `jobmanager` CPU caps; `tmMemory` (Flink process size), `tmMemoryLimit`, `kafkaMemory`, `kafkaHeap` |
| `images.flink`, `images.kafka` | pinned tags; preflight checks they are native to the host |
| `jdk` | the host JDK home; preflight checks its major version matches the engine image |
| `axis`, `apiLevel`, `guarantee.state`, `guarantee.sink`, `checkpointMs` | the header fields of §9, verbatim into the report |

### The broker's CPU, for the dashboard

`flinkProperties` reaches the job manager and the workers. Section 7's **CPU
per component** panel is about the component beside them — the broker that can
be the ceiling at half a core — and nothing reports that. cAdvisor does not
work on Docker Desktop for macOS: its Docker factory registers against the
socket and it still reports one series, because the cgroup tree it reads inside
the VM does not contain the containers.

`harness/dashboard/docker_cpu_exporter.py` reads cumulative CPU nanoseconds per
container off the Docker API and serves them as a Prometheus counter. Copy it
next to your provisioning files and mount **the directory**, never the single
file — `sed -i ''` and friends replace the inode, and the container then holds
a file that no longer exists while looking perfectly healthy.

```json
"extraServices": {
  "statsexporter": {
    "image": "python:3.12-alpine",
    "container_name": "myproj-statsexporter",
    "hostname": "myproj-statsexporter",
    "cpus": 0.2,
    "mem_limit": "128m",
    "environment": {"PREFIX": "myproj-"},
    "command": ["python3", "/app/docker_cpu_exporter.py"],
    "volumes": [
      "/abs/path/to/dashboard/exporter:/app:ro",
      "/var/run/docker.sock:/var/run/docker.sock:ro"
    ]
  }
}
```

Scrape `myproj-statsexporter:9110`, and plot
`rate(docker_container_cpu_seconds_total[1m])` by `name`. `PREFIX` is required:
without it the exporter would publish every container on the machine. Nothing
it reports is ever quoted as a result — the harness reads the cgroup counter
directly for the table.

## What the harness owns, and the agent does not change

The thresholds in `lib.py` (`T`), each with the measurement it was set from.
`replay` also checks the harness for names that only exist in another scope.
Nothing else can: the pure self-test and the suite replay never run preflight,
which needs Docker, so on 2026-09-08 a host-probe check that wrote to a name
belonging to a different function reached the rig and failed a chain at
preflight in 1.7 minutes. `namecheck.py` uses Python's own scope analysis, takes
milliseconds, and reports that defect exactly.

Config guards are replayed too: `record/configs.json` holds configurations
whose verdict is already known — run 20's and run 21's, the rig's, plus two
that must fail (flat pipeline memory, six partitions with a four-core
case) — and `replay` builds each one and checks it still gets that verdict. It
exists because #68 shipped a rule that failed two configurations which had
already produced accepted runs, and run 22 spent two chains and produced no
ratios finding out. With this in place that rule fails replay in a second.

`prove.py probe` measures what this host's own cores do at each case size with
no pipeline in the way, and prints each arm separately — a register-only arm and
a memory-bound one. It takes `--repeats`; more repeats widen the full range and
settle the middle half rather than shrinking either. Run it before blaming a
pipeline for a step it missed.

`prove.py replay` re-derives every recorded suite in `record/` with the current
thresholds before any command that touches a stack, and will not run if a
threshold would void a table the record marks valid or report one it marks
invalid. **To change a threshold: change it, run `replay`, and if it fails, the
threshold is wrong — not the record.** A new suite worth remembering goes into
`record/` as per-pass rates per case plus a `validSteps` verdict.

What the harness measures and how:

- rate = committed source offsets over a window opened on the tick the offset
  advances and closed after ≥ `minBoundaries` further commit boundaries and ≥
  `minWindowS`; never the engine's own meter
- CPU = cgroup `cpu.stat usage_usec` at open and close; throttled periods from
  the same file
- busy / idle / back-pressured per vertex from the pipeline's slf4j reporter,
  averaged over samples inside the window; internal back-pressure is a column,
  source idle is the external-boundary guard
- the cap is read back from `NanoCpus` and `cpu.max`; slots from the engine;
  vertex parallelism and graph shape from the running plan
- every case tears the job, sampler and pipeline down on every exit path
- a failure about the **rig** stops the suite; a failure about a **case's data**
  marks the case, voids the ratios it is in, and moves on

## Results files

| file | written by |
|---|---|
| `preflight.json` | preflight |
| `tinyproof.json`, `selftest.json` | tinyproof |
| `manifest.json`, `manifest-small.json`, `manifest-tiny.json` | fill, completeness, tinyproof |
| `completeness.json` | completeness |
| `suite.json`, `suite.txt`, `suite.md` | suite (and `report`, which regenerates the last two) |
| `ceiling.json` | ceiling |
| `all.json`, `phases.log`, `DONE` | all (per-step rc and seconds; timestamps; the verdict to wait on) |
| `harness.log` | everything |

`suite.md` is the table for the report: header fields, step ratios with their
range across passes, per-pass rows, per-case means with spread and
reportability. Paste it; do not retype a number.
