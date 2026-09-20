---
name: scalable-flink-skill
description: Use when the user wants to build a data pipeline or service AND demonstrate that it scales — a training project, a capacity study, a proof for a demo or talk. Interviews one question at a time before building, then enforces a measurement discipline that produces numbers the audience cannot pick apart.
---

# Prove it scales

Building the thing is the easy half. Producing a number that survives a skeptical
reader is the hard half. Everything below is either a **question** to ask before
building, a **preflight** check that prints PASS/FAIL, a **guard** the harness
fails a run on, or a **rule of judgment** kept short enough to read. The
validation record behind each rule lives with the project that wrote it, not
here.

**The measurement harness ships with this skill. Use it; do not write one.**
`harness/prove.py` (next to this file) is the preflight, the tiny proof, the
completeness run, the suite, the guards and the report table, with a self-test
per guard and a replay of every threshold against the recorded runs before
any command touches a stack; `prove.py all` runs the whole chain detached and
writes `results/DONE` when it is over. You supply the pipeline: a job jar, a
deterministic generator, a verifier that exits non-zero on any loss, and a
`pipeline.json` describing them — `harness/README.md` is the contract. Ten
runs each rewrote the harness from this prose, and every one re-decided
something it had already decided: what a failed check does, what the window is
anchored on, what counts as flat. Sections 3–6 below say what the harness
enforces and why, so you can read why it stopped a run; they are not a specification
to re-implement. If the harness cannot express your pipeline, say so in the
report and stop — do not fork it.

## 1. Interview before building

**Six questions, one at a time. Wait for each answer.** Offer the default with
each so the user can say "yes". Do not start building until the spec and the
fan-out are known.

**Ask only what changes what gets built.** Everything else the skill decides
and states, it does not request: it always runs on a laptop in Docker, always
builds the dashboard, always proves completeness, always checks correctness,
and always measures 2 and 4 cores for near-linear scaling. Asking permission
for those invites a "no" that will not be honoured, and spends a question. They
are declared in the plan (§1a) instead, where the user can object to all of
them at once.

**When there is no human to ask** — a clean-room run, an unattended agent — do
not stall and do not skip the interview. Answer all six yourself, write each
answer down as a stated assumption in `ASSUMPTIONS.md`, and build against that.
A reader can then see what was assumed rather than agreed.

1. **What goes into the pipeline, and what comes out?** A few sentences, in the
   user's own words. This becomes the spec.

   *Default: an order arrives with a unique id and symbol, order quantity and a
   list of allocations; each allocation is keyed by account / sub-account and
   carries a quantity. The pipeline maintains positions by
   account+subaccount+symbol and by symbol. That input is the one scaled up to
   drive the pipeline to capacity. A second input carries prices, keyed by
   symbol and timestamp; the pipeline joins those to the positions and emits a
   position and market value every 10 seconds.*

2. **Does one input produce more than one output?**

   *Default: one trade input produces 1 position per symbol and one position
   per allocation. If the order has 4 allocations it emits 5 records. For the
   market value we want to throttle it to a configurable interval defaulting to
   10 seconds.*

   The throttled emit is deliberately not counted in the 5: fan-out counts
   outputs **per input**, and a throttled emit is per interval. Counting a
   timer-driven output as fan-out is what makes the two-vantage guard disagree.

3. **What are the keys, and how many distinct ones?**

   *Default: two key spaces. Symbol, and account / sub-account / symbol.
   4 symbols, 2 accounts and each has 2 sub-accounts.*

   *Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
   impact the application design.*

4. **What has to be exactly right?**

   *Default: positions and market values must be published in order. At the end
   the positions, at both symbol and account / sub-account / symbol, must match
   the input, and market values must be final position × latest price.
   Duplicates have to be handled and not double counted, in all cases.*

   Ordering holds **per key** within a keyed stream — not across keys, and not
   across a rebalance. Say so if the user's answer assumes otherwise. How the
   duplicates are handled is a build decision, not a question: §4.

5. **Which Flink API should we use?** DataStream, where the developer has more
   control over the execution graph, or SQL, where the optimizer makes more of
   the decisions. Neither is more valid, but the claim differs, and it is said
   next to the numbers.

   *Default: DataStream.*

6. **Which Kafka do you want to use?** Apache or Confluent.

   *Default: Apache.*

   Both are driven by `images.kafka` and `images.kafkaLibs` in `pipeline.json`
   — the harness mines the broker image for its client jar, so the library path
   moves with the vendor.

## 1a. Then the plan, and stop

**Before building anything, describe the test, show the plan, and ask to run
it. Wait for the answer.** One approval for the whole design beats six
permissions for things that were never optional.

The description states every unconditional, so nothing is hidden by not having
been asked:

| | |
|---|---|
| the objective | near-linear scaling across 2 and 4 cores, judged against ≥95% of linear per step. The one-core case is not run: §5 says not to, and it is the case that reads low and makes the step off it look impossible |
| where it runs | a laptop, in Docker |
| the stack | the images in `pipeline.json` — Flink, and the Kafka chosen in question 6 |
| what is measured | the drain rate of a fixed backlog at each core count, read from committed broker offsets, with the resource columns beside it |
| what is proved first | completeness with no tolerances, re-checked after killing a worker; no throughput table is published for a build that has not passed |
| what is built | the pipeline, a deterministic generator, a verifier, and the dashboard |

Then the six answers, and every assumption made where a question was not asked.
If the user objects, change it and show the plan again. **Do not start until
they say yes.**

**When there is no human to say yes** — a clean-room run, an unattended agent —
write the plan to `PLAN.md` anyway and build against it. It is the record of
what was decided and it is worth as much when nobody approved it: a reader can
see what the run committed to before it had any numbers. Say in the file that
no one approved it. §1's rule is the same one — answer the questions yourself
and write them down — and this step should not be the one that stalls.

## 2. Build in reviewable steps

One branch per step, squash-merged. Each step ends with the system **running and
measured**, not compiling. Pause for review between steps; run without prompting
inside one. Keep a journal: what drove the step, what was decided, how it was
verified.

## 3. Preflight: assertions that print PASS or FAIL before anything is built

Each is one command. The cost of a violation scales with how late it is found —
a wrong architecture voids the study, a contaminated baseline voids the suite, a
bad window voids one case — so anything checkable now is checked now.

| check | how | fails as |
|---|---|---|
| every image is native to the host arch | `docker image inspect` `.Architecture` vs `uname -m` | emulated CPU numbers with no warning |
| the JDK the engine needs resolves | print the resolved version and pin it | wrong `java` on PATH |
| the engine can write its state directory | write a file as the runtime user | every job rejected for a directory it cannot create |
| the metrics reporter is not duplicated | look in the plugins dir before copying a jar into `lib/` | container dies at startup |
| disk budget on the **host**, not the container | `backlog + backlog × fan-out × undrained cases + checkpoint state` against host `df` | full disk with no shell to recover in |
| retention on every topic written but never drained | `retention.bytes` set; it is a periodic sweep, not a bound | sink log 7× its cap between sweeps |
| the generator is deterministic | two fills with one seed are byte-identical | no expected answer can be computed |
| CPU cap mechanism chosen once | `--cpus` throughout **or** quota/period throughout; `--cpus 0` is a no-op, `--cpu-quota=-1` sets a period the daemon then will not change | second case measured at the first case's cap |
| the broker can cache the backlog | `kafkaMemory` minus `kafkaHeap` against the least any recorded configuration produced a table with | the broker reads the backlog back off disk, becomes the constraint instead of the worker, and the cases come back as ceilings — 44 minutes to find out |
| slots ≥ parallelism × jobs | compare before submitting | job waits for resources while the harness times an empty pipeline |
| transactional-ID prefix and consumer group are scoped per run | include the run id | 470-second cold start after ten runs; 22 dead series on the backlog panel |
| back-pressure counters exist on the endpoint you will read | dump the endpoint and read what is there | ten minutes on a deprecated path |
| the VM trim command is known | `docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker` | space freed inside a Docker Desktop VM never returns to the host |

**Then the tiny proof, before any fill.** A few thousand records, end to end:

- run **every case the suite will run**, and assert cap consumption in each.
  With the default two cases that is the two cases; a third case costs one
  short run here and saves the suite. Every step the suite will report gets
  bounded now, including the middle one — clean-room run 31 measured only its
  smallest and largest cases here, so a 1→2 step that was arithmetically
  impossible went unseen until the report, 45 minutes later;
- **bound the ratio to 0.75×–1.25× of the ideal** — 1.5×–2.5× when the two
  cases are one unit and two, 3×–5× when they are one and four. Beating the
  ideal is not an error in itself, and nothing fails a run for it outside the tiny
  proof: the suite has a floor, not a ceiling. Read a *large* overshoot from
  the other end — one core of two should return about **half** the two-core
  rate, so a ratio of 3× is a baseline at a third of its share, and it is the
  slow case that wants investigating, not the fast one. Run 8's baseline
  time-shared six unchained tasks on one core, passed the cap guard at 99.9%,
  and made the step read 3.73×. That is the baseline-shape problem §5 says to
  read off the job graph, and the harness prints the share beside the ratio so
  the short case names itself;
- **kill a worker mid-drain and re-assert the totals** — a guarantee is a
  claim about failure and is untested until something has failed; finding out
  after the suite discards the suite;
- run one full case with a 10–15 s window so every line of the harness
  executes, **and fail one check on purpose** so you know the checks can fail.

Anything that can void the whole table is tested before the table exists.

## 4. Prove nothing was lost, separately from proving it is fast

A pipeline that drops one record in ten thousand looks fine in every throughput
column. Completeness is a separate run on a backlog small enough to **drain to
the last record**; measurement is a slice of a steady state and cannot check it.

The expected answer comes from the **input**, never from the pipeline: the
generator writes a manifest (record count, totals per key) and the sinks are
compared to that. Assert, with no tolerances:

| assertion | meaning of a miss |
|---|---|
| distinct keys = the number predicted in the interview | a key you did not intend, or one that never arrived |
| every aggregation sums to the manifest exactly | a lost or duplicated record |
| two paths over the same input agree exactly | same, located |
| after killing a worker mid-drain, all of the above still hold | the guarantee you configured is not the one you have |

**Two settings, not one, and both are needed.** The user is asked what must be
exactly right (§1 q4); which settings deliver it is a build decision made here:

- **Sink** — emit the *absolute* value per key, not a delta. A repeat is then
  harmless, with no transactions and no commit-interval latency floor.
- **State** — exactly-once *checkpointing*. Without it a replayed record is
  folded into the snapshot twice, and the sink setting does nothing about it.

The usual answer is *exactly-once checkpointing, at-least-once sink*. Name both
in the report (§9), and expect the kill test above to be what proves them.

Record the build hash beside every number, and **gate throughput on this**: no
table is published for a build that has not passed. Put the same script in CI
from a cold start, so it stays true after this morning's change.

## 5. Measurement discipline

**Cap the component under test; hold everything else still.** Capping a task
manager at 1, 2 and 4 cores on one laptop reproduces the curve that took 32
vCPUs and three brokers the expensive way, and it does not teach the audience
that the result needs a cluster.

**Buy the resource and the parallelism together**, one core and one degree per
step — that is what a vendor sells, so the number prices.

**You can only show that something scales when it is the thing constrained.**
This is the rule runs break most, always with the evidence in their own table,
so the harness owns it (§6): ≥95% of cap at every case (the baseline included),
and no material back-pressure **at the boundary to the external component**.
Internal back-pressure inside a capped single-slot worker is expected — the
source waits on the aggregation threads sharing its core — so it is reported in
a column and gated on nothing.

**The baseline is a case, not a reference point.** Anything true of the other
cases and not of it — a shuffle, a network hop, a second JVM — is a difference
you are attributing to scaling. At parallelism 1 the same job can be written to
chain into one vertex with no serialization; the cases above it cannot. Measured
on one build, changing only that:

| baseline | throughput | 1→4 |
|---|---:|---:|
| chained, no shuffle | 211,533 | 2.16× |
| same graph as every other case | 140,308 | **3.26×** |

So **read the job graph back off the running plan and fail any row whose
shape differs from the others**, and **lead with step ratios** — 2→4, not 1→4.
A step ratio has no privileged case in it, a faster single-thread
implementation cannot be punished by it, and it is the step someone will
actually buy. Quote the baseline ratio second, and say what it is measured
against. **If the claim is a step from two units up, do not run the one-unit
case at all** — it is the structurally weakest case and the noisiest in every
run that repeated it, and every case you run is time and a guard that can
fire. Run the cases the claim needs, plus one above if the ceiling is in
scope.

**Measure a drain, not a live generator.** Fill a backlog larger than the page
cache, stop the producer, measure the drain. Hold partitions, checkpoint
interval and backlog constant across cases; nothing varies but the one thing
under test. Do not shorten the interval to save time once cases have run — it
changes the number.

**Read throughput from the transport, not the engine.** At 100% CPU the
engine's metric service is starved with everything else; one case
under-reported itself by 3×. Use committed broker offsets (committed only under
exactly-once — the log end includes open transactions), rows in the sink, files
closed.

**Anchor the window on the committed offset advancing**, not on wall clock and
not on checkpoint completion — the commit lands asynchronously after the
checkpoint, and a window opened on completion reads a number that has not moved
yet. Open on the tick the offset changes, close after at least three commit
boundaries. This took one run's vantage-point disagreement from 25% to 0.5%.

**Read CPU from the cumulative cgroup counter** (`cpu.stat usage_usec` at open
and close, divided by elapsed), not `docker stats`, which samples: a valid case
failed at 94.4% sampled while the counter said 97.8%. The same file gives
`throttled_usec`, which is direct evidence the cap is what binds.

**Put the resource columns next to the throughput** for every case: what the
component under test used (3.94 of 4), what every other component used (a
broker at 0.48 cores can still be the ceiling — it ran out of write throughput,
not CPU), and back-pressure, so waiting is distinguishable from working.

**Warm up to a flat trend, not a round number.** Fit four intervals and require
a flat slope; two neighbours agreeing is a coin flip against ±10% noise.

**Every case at least twice, and report the spread.** The same case measured
three times spread 10–17% — wider than a step ratio's effect. A case whose
spread exceeds 20% — set above the band the valid cases occupy and below every
outlier — is **unreportable on its own and voids every ratio it is part of**;
it does not void the suite. One run's 1-core case spread 14–42% in
seven consecutive suites while its 2- and 4-core cases held under 6%, and a
failing the whole suite threw away six valid 2→4 measurements.

**Ascending then descending.** If the curve differs, something warms or
accumulates between cases and the shape is partly the order.

**To find the ceiling, starve the other component.** Hold the component under
test at its largest size and cap the thing beside it in steps. The handover is
unambiguous: the squeezed component pins at ~100% while the component under
test falls off its own cap. Three short runs locate it on a laptop, and the idle
fraction says roughly how many more units it would take. A starved input shows
as **busy falling with back-pressure at zero** — not overwhelmed, waiting.

**Every number in the table comes from one build.** Change the job, re-run the
rows. A table from different jars is a collection, not a curve.

### Budget the suite before running it

A case is warm-up + window + submit-and-settle, and across one suite the gaps
between windows equalled the windows. Count cases like money: the window is a
floor set by the checkpoint interval (three boundaries; four is prudent — 40 s
at a 10 s interval, not 60); the descending pass detects order effects and is
not a second table; size the backlog for the longest single *use* — including a
dashboard image — plus headroom, not for the suite. Record submit, steady,
open and close timestamps per case so the next budget is measured. The
harness ends every suite with a **sentinel** — the baseline case once more —
so the suite's first and last measurements are the same case and a rig that
drifted across it is visible as baseline spread (one rig read its four-core
case 12% higher ten minutes after the suite than in it). Budget one extra case.

**Start the fill the moment the tiny proof passes** and build the dashboard's
panels while it runs. Nothing but the cases depends on them. The dashboard's
*service* is not so free: `extraServices` is read when the stack comes up, so
it has to be in `pipeline.json` before the first `up` — decide at §1a that
there will be a dashboard, and write the service in then, even if the panels
come later.

## 6. The checks that fail a run

**A benchmark that prints a number for every input will eventually print a
wrong one.** The shipped harness implements and self-tests every guard below;
`prove.py tinyproof` runs the self-test live and `suite` will not start until
it has passed for the build under test. Each guard exists because a run paid
for it.

| what fails a run | how it checks |
|---|---|
| the resource cap was not applied | read it back from the container, never the environment variable |
| parallelism ≠ cap ≠ allocated slots | all three read back from the engine on every case |
| the job graph differs from the other cases | vertex count and edge ship strategies read off the running plan |
| the component under test is not the constraint | ≥95% of cap at every case, baseline included; external-boundary back-pressure not material; the broker never hits its own memory limit inside a window (a starved page cache depresses the rate while the worker still reads 96% of cap) . A case that misses is a **ceiling**: measured, reported with its rate as where scaling stops, and excluded from the ratios — never deleted |
| the input divides evenly across subtasks | partition count divisible by every parallelism under test (8 partitions serves 1, 2, 4; 6 would leave the 4-core case reading 2/2/1/1 and never reaching its cap) |
| memory is not the constraint | worker memory uncapped by default (the demo caps none); a case whose GC exceeds 5.5% of its capacity is a ceiling, not a result. Cap deliberately with `tmMemoryPerCore` or `perCase` when the study is about memory |
| the claim itself | each step returns ≥95% of linear, or the chain fails with the per-core, idle, GC and cap figures for both cases — a valid table that does not scale is a result about the pipeline, not a table to publish |
| a failed case still owns the cluster | job torn down on **every** exit path |
| no job is actually running | engine reports RUNNING with the expected parallelism |
| the cluster is still busy from the last case | assert idle by asking the engine, not by killing what you think is there |
| the backlog lacks headroom at window close | a full checkpoint interval of records remains **at the measured rate** — not merely `remaining > 0` |
| the window is not anchored on commit boundaries | ≥3 boundaries inside the window |
| two vantage points disagree | transport and manifest agree within a stated tolerance |
| a rate came from the engine | the rate source is the transport's committed offsets |
| the measured rate is zero or negative | — |
| a case's passes spread >20% | every case run ≥2×; that case and every ratio it is part of are marked unreportable — the other cases still report |
| rows came from different builds | one build hash across the table |
| observed cardinality ≠ predicted | distinct keys vs the interview's answer |
| completeness has not passed for this build | §4, with no tolerances |
| host free disk is below the next case's write | checked **before** the case — a full disk takes the shell down with it |
| a monitor outlived the thing it watched | at teardown, no child the run started survives, and no host process watching `results/`, naming the project, or running `prove.py`-shaped loops from inside it either |

**When the claim is not met, show the whole picture and ask before iterating.**
The report prints every case and every step together, then the steps to fix in
order — a step reading *above* 2× first, because nothing does more than double
the work on double the cores, so its lower case read too low and every step it
appears in means less than it looks like. Fixing the shortfall first while a
case is under-reading is work against a moving target. Take that list to
whoever asked for the run as a plan — what you would change, in what order,
what you expect it to move — and get a yes before measuring again. A re-run
costs what the last one cost.

**A failed check stops the suite — when it is about the rig.** A cap that did not
apply at one core will not apply at two; a busy cluster, a bad window anchor,
disagreeing vantage points are the same at every case. A guard about one
case's *data* — spread, headroom at close — marks that case and moves on. Retry the same case once if the failure is
plainly transient; if it fails again, say *"stopping here: the remaining
cases would fail the same way"* and exit. "FAILED — continuing" produces a
table with holes that look like data.

**Assert the effect, never the exit code.** `docker update --cpus 0` reports
success and does nothing; a metrics reload returned 200 over a half-written
file; a topic delete removed one of two topics and said nothing. Every
state-changing command gets a read-back, and the read-back is the guard. After
any destructive or recreating infrastructure command — a `compose up` of one
service recreates its dependencies — **re-verify the backlog against its
recorded manifest** before trusting it.

**A guard that has never fired is a guess.** Break each one on purpose — wrong
cap, stopped cluster, truncated backlog — and confirm it fails the run. Assert that
anything launched unattended is alive before waiting on it, and log its stderr
to a file from the first version; `DEVNULL` turns a one-line diagnosis into a
half-hour one.

**Anything that watches the stack runs as part of the stack.** Four
consecutive runs left a host-side sampler running with the teardown assertion
passing honestly, because the harness cannot see a process it did not start.
Put samplers in the compose file; `compose down` reaps them. The rule was
prose for five runs and broken on every one of them, so `down` now also looks
for strangers: any host process holding a file under `results/` open, naming
the project directory on its command line, or naming `prove.py` on its
command line while running from inside the project, is killed and listed, and
`down` fails if one survives. Another project's harness and a shell that
merely sits in the directory are left alone — an earlier, wider rule killed a
live tiny proof in another directory. A watcher you start on the host will be
killed by the teardown it was waiting for — start none.

**The promotion rule:** when a run breaks a rule, that rule becomes a guard
in `harness/lib.py` with a self-test in `prove.py selftest`, or it is deleted. **And before any new guard or threshold
goes into a run, replay it against every result already recorded**: if it
would have failed a table considered valid, it is wrong, and it is cheaper
to learn that in a minute than in a run. Thresholds come from measured
spread, not round numbers — a 10% spread guard written when the record
already showed 10–17% cost two runs; `prove.py replay` is that check, and
it runs before every command. Sort every new rule into one of three piles
— *checkable while running* (a guard above), *checkable before running* (a
preflight row in §3), *judgment* (prose, and keep that pile small).

## 7. The dashboard explains; the harness measures

**Add it through `extraServices` in `pipeline.json`** — a map of service name to
a compose service body, spliced into the stack the harness generates. Two rules,
because the measurement depends on them: the container name must start with the
project prefix, or teardown leaves it behind and then fails for a survivor it
did not create; and give it a CPU cap, because anything sharing the cores under
test changes the number being measured. Whatever you add is recorded in the
results header, so a reader knows what else was on the machine. That is the only
sanctioned way — the harness is still not to be forked.

Build it, provision it from a file that ships with the stack, and never take a
reported number off it — engine meters are ~60 s moving averages and its
metric service starves at exactly the load you care about. Render images
server-side, with the timezone passed explicitly, so the picture in the
write-up is the window the number came from.

**Set the default time range to cover the whole suite, and the metrics store's
retention to outlast it.** What is being measured is a drain: the producer is
stopped, the backlog empties, and every panel goes flat the moment the last
case ends. A dashboard left on a five-minute default is therefore empty for
everyone who opens it afterwards — which is everyone except the run. You do
not have to work the span out: the report prints it as `suite span`, as a human
interval and as the `from=`/`to=` epoch pair a dashboard URL takes, so a range
that does not cover the suite is visible beside the numbers it failed to show.

| panel | the question it answers |
|---|---|
| rate per stage | is the fan-out real? lines a constant factor apart |
| distinct keys per aggregation | is the predicted cardinality the one you got? |
| the two paths, overlaid | do two independent aggregations agree? |
| busiest vs most back-pressured task | at the limit, falling behind, or **starved**? |
| CPU per component | which one is in the way — including the idle one |
| backlog remaining | is this a drain, and did it run out? |
| checkpoint duration | what does the guarantee cost? |

On the first render check five things: the legend fits; nothing is secretly on
a second axis; the timezone is right; every panel has data (a "No data" panel
usually means provisioning half-applied — verify the artifact loaded, the
success code lies — and check this at the suite's range once the suite has
ended, not only while it is running: a panel that was full live and is empty at
rest is a range or a retention problem, not a provisioning one); and no panel
is a ratio of counters on different clocks or
a signed sum, because both are unstable for reasons unrelated to the pipeline.
**A panel you cannot explain is a liability** — either it says what the number
means or it goes.

## 8. An explanation is a measurement, not a story

A number short of expectation invites a reason, and a plausible reason is cheap
to produce and expensive to be wrong about. A mechanism is a claim about cause,
so it needs the same evidence as a throughput claim: **one rig, one build, one
variable changed, both arms measured.** Anything less is a hypothesis and is
labelled as one or left out.

Two failures are easy and expensive: **explaining your number with someone
else's run** — absolute throughput is not comparable across implementations,
and neither are explanations of it; one investigation spent hours on a
ten-percent shortfall that belonged to a different pipeline entirely — and
**arithmetic offered as evidence**, which runs backwards easily: fixed
per-worker cost *flatters* wider cases, and was offered to explain a
sub-linear one.

When a result is short: say what you measured, say what you have ruled out and
with what evidence, and say the cause is unknown. *"I do not know yet"* costs
one line.

## 9. Reporting

- **Lead with the step ratio** the reader would buy — two units to four — with
  its efficiency and spread. Baseline ratio second, stating what it is against.
- **Lead with the outcome, not the road to it**, and stop after the evidence.
- **Header fields:** axis — always *one machine, more cores*, since that is
  what §1a declares and what 1, 2 and 4 cores measure — API level,
  guarantee as two settings, checkpoint interval, build hash, passes per case.
- **State the scope once**, where the technical reader will meet it: one
  pipeline supports "this pipeline scaled linearly", not "the engine scales".
- **Say where it stops.** Naming the ceiling makes the rest credible.
- **Generate every artifact that carries a number** from the results file, then
  render it and look — layout collisions are found by looking, never by
  reading the code.

## 10. When something is blocked

Try twice, maybe three times. Then stop, say what was tried and why it failed,
and route around it. Kill every watcher you started for the thing you abandoned.
