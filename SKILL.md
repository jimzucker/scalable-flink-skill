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

**Two worked configurations ship with it, and neither is a template.**
`harness/pipeline.example.json` configures a pipeline whose outputs grow
with its input; `harness/pipeline.example.windowed.json` one whose outputs
are per window and grow with the clock instead. Read whichever is the
shape of yours, then write your own from the field table in
`harness/README.md`. Copying is how one pipeline's numbers end up in
another's run — two clean-room runs out of two took a shipped backlog count
verbatim, one of them before it had measured anything.

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
and always measures 1, 2 and 4 cores for near-linear scaling. Asking permission
for those invites a "no" that will not be honoured, and spends a question. They
are declared in the plan (§1a) instead, where the user can object to all of
them at once.

**When there is no human to ask** — a clean-room run, an unattended agent — do
not stall and do not skip the interview. Answer all six yourself, write each
answer down as a stated assumption in `ASSUMPTIONS.md`, and build against that.
A reader can then see what was assumed rather than agreed.

1. **What goes into the pipeline, and what comes out?** A few sentences, in the
   user's own words. This becomes the spec — **the topics, the generator and
   the verifier are derived from it**, not from the example config. The example
   shows one pipeline's answer; the harness constrains only two things, and
   neither is a design: which topic it fills and measures, and that the
   fan-out into `topics.out` is a constant per input.

   *Default: an order arrives with a unique id and symbol, order quantity and a
   list of allocations; each allocation is keyed by account / sub-account and
   carries a quantity. The pipeline maintains positions by
   account+subaccount+symbol and by symbol. That input is the one scaled up to
   drive the pipeline to capacity. A second input carries prices, keyed by
   symbol and timestamp; the pipeline joins those to **each** of the position
   outputs and emits a market value every 10 seconds for both — symbol /
   position / price / market value, and account / sub-account / symbol /
   position / price / market value. Positions themselves are published as they
   change, one per input — only the market value is throttled.*

   The default business case, drawn. **Two position outputs and two market
   values**, and the prices reaching both aggregations by broadcast:

   ```mermaid
   flowchart LR
     O([orders]) --> P[parse once]
     P -->|main| KS[keyBy symbol]
     P -->|side output| KA[keyBy account/sub/symbol]
     KS --> RS[running position] --> SS([positions-by-symbol])
     KA --> RA[running position] --> SA([positions-by-account])
     PR([prices]) -. broadcast .-> MS
     PR -. broadcast .-> MA
     RS --> MS[market value<br/>every 10 s] --> MVS([market-values-by-symbol])
     RA --> MA[market value<br/>every 10 s] --> MVA([market-values-by-account])
   ```

   Only `orders`, `positions-by-symbol` and `positions-by-account` belong to
   the harness. `prices` and the two market-value topics are the pipeline's
   own — see question 2. **Name the two market-value topics in
   `topicsAlsoWritten`**: a picture cannot be checked, but a list of topics
   that must receive records can, and the completeness drain refuses any that
   stayed empty. That is what turns this diagram from a drawing into
   something the run is held to.

   **"The positions" is both of them.** Question 1 asks for two aggregations
   and the price join applies to each, so there are two market-value outputs,
   not one. Saying it once was not enough: clean-room run 37 read "joins those
   to the positions" as the symbol side only, wrote the narrowing down as a
   decision of its own, and built a pipeline missing half of what was asked
   for. **The account side is what makes this a design decision** — it is keyed
   on account / sub-account / symbol, so it cannot be joined to a symbol-keyed
   price stream by key at all. Broadcast the prices to both aggregations
   instead.

2. **Does one input produce more than one output?**

   *Default: one trade input produces 1 position per symbol and one position
   per allocation. If the order has 4 allocations it emits 5 records. For the
   market value we want to throttle it to a configurable interval defaulting to
   10 seconds.*

   The throttled emit is deliberately not counted in the 5: fan-out counts
   outputs **per input**, and a throttled emit is per interval. Counting a
   timer-driven output as fan-out is what makes the two-vantage guard disagree.

   **A default with two inputs is not a problem, it is two topics.** The
   harness owns exactly one input — the one it fills, measures and drains, the
   one being scaled to capacity — and the outputs whose growth is a constant
   multiple of it. A price feed is a topic you create, fill and read, named in
   neither list, and the harness leaves it alone. The same goes for the
   timer-driven market value on the way out. Three clean-room runs each stopped
   to work this out and each reached the same answer, because the contract said
   "the input topic" and "every topic it writes" and left the rest to be
   inferred.

3. **What are the keys, and how many distinct ones?**

   *Default: two key spaces. Symbol, and account / sub-account / symbol.
   4 symbols, 2 accounts and each has 2 sub-accounts.*

   *Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
   impact the application design.*

   **A key set does not spread over the cores by itself, and a small one
   rarely does.** Flink hashes each key into one of `maxParallelism` key groups
   and hands every subtask a contiguous range, so an even split is luck rather
   than arithmetic at any cardinality: 16 keys land 5/3/4/4 over four subtasks
   at the default 128 groups, and 100 keys land 29/27/21/23. The busiest
   subtask does proportionally more work, so its share sets a ceiling on that
   stage — 0.80 of linear in the first case, 0.86 in the second. Preflight
   measures yours and names the `pipeline.max-parallelism` that evens it out.
   Taking the first of those as the worked example: one core does a quarter more work than an even split,
   that stage cannot return more than 0.80 of linear at four cores, and nothing
   in the table says "key skew" — it looks like a pipeline that did not scale.
   Preflight measures it (§3) and names the `pipeline.max-parallelism` that
   makes it even; for those key names it is 1115, and the layout becomes
   4/4/4/4. Name the key sets in `pipeline.json`'s `keySets` so it can.

4. **What has to be exactly right?**

   *Default: positions and market values must be published in order. At the end
   the positions, at both symbol and account / sub-account / symbol, must match
   the input, and market values — **at both of those key levels** — must be
   final position × latest price. Duplicates have to be handled and not double
   counted, in all cases.*

   Ordering holds **per key** within a keyed stream — not across keys, and not
   across a rebalance. Say so if the user's answer assumes otherwise. It is
   not a caveat you mention and move past: §4 requires the verifier to assert
   it, on both arms. How the duplicates are handled is a build decision, not a
   question: §4.

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
| the objective | near-linear scaling across 1, 2 and 4 cores. Each step must return **1.90× or better** on a doubling. Two steps are reported, 1→2 and 2→4 |
| where it runs | a laptop, in Docker |
| the stack | the images in `pipeline.json` — Flink, and the Kafka chosen in question 6 |
| what is measured | the drain rate of a fixed backlog at each core count, read from committed broker offsets, with the resource columns beside it |
| what is proved first | completeness with no tolerances, re-checked after killing the pipeline mid-run; no throughput table is published for a build that has not passed |
| what is built | the pipeline, a deterministic generator, a verifier, and the dashboard |
| the shape of the suite | the passes per case from `pipeline.json`, ascending then descending, then the baseline once more as a drift check. State the number — it is most of the wall clock |
| the guarantee | exactly-once checkpointing at the configured interval; an at-least-once sink made idempotent by emitting the absolute value per key. State both, and the interval |
| how long it takes | state an estimate in hours. Clean-room runs have taken two to three, and a user who expected twenty minutes will stop it halfway |
| what it writes | the three backlogs from `pipeline.json` — suite, tiny proof and completeness — as record counts, and that they are tens of gigabytes of Kafka log. Host free disk is checked before every case, but a user who did not know should not find out from a guard |
| what it occupies | the ports in `pipeline.json`, the partition count, and containers named for the project. Anything already on those ports will not start |
| where to watch it | `results/PROGRESS.txt` — one sentence, overwritten: which step of seven, which case of ten, and roughly how long is left. Say this **in the plan**, before the yes, because the next thing that happens is two hours of quiet |

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

**Group the calls that do not depend on each other.** An agent's turn re-reads
the whole conversation before it does anything, so ten one-line commands cost
ten times the context of one command that does ten things — and this build is
mostly reading: the job, the generator, the verifier, the config, the
dashboard, the results. Read them together. Only a call whose input comes from
the previous answer has to wait for it. Measured on the session that wrote this
skill: 97% of its tokens were the conversation being read again, about 410,000
per turn, and a good share of the turns were single `cat`s that could have
travelled with their neighbours.

**The same rule decides how to watch a run.** Wait on `results/DONE`; do not
poll on a timer. A three-hour chain checked every thirty seconds is 360 turns
and 360 re-reads of everything said so far; checked at each of the seven step
boundaries it is seven. `results/PROGRESS.txt` is there to be read when you
have a reason to read it, not on a clock.

**Say where the run is up to, without being asked.** The chain takes two to
three hours and most of it is silent. The harness keeps one sentence in
`results/PROGRESS.txt`, overwritten as it goes — which step of seven, which
case of ten, and roughly how long is left. While waiting on `results/DONE`,
read it and pass it on at each step boundary and each case, with the number
just measured. A person who has approved two hours of their laptop is owed
more than silence, and a case that comes back wrong is worth knowing about
before the other nine have run.

**If you cannot speak to anyone until you finish** — a background run, a
detached agent — then say so in the plan and name `results/PROGRESS.txt` as the
place to watch. Promising updates you have no channel to deliver is worse than
promising nothing: the reader waits for a message that cannot arrive. Whoever
started the run is then the one who relays it, and they can only do that if
they were told where to look.

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
| disk budget on the **host**, not the container, as a **placeholder** — it assumes a record size until one has been measured, and the tiny proof replaces it minutes later with the bytes per record the broker actually stored. Read the tiny proof's figure, not this one | `backlog + backlog × fan-out × undrained cases + checkpoint state` against host `df` | full disk with no shell to recover in |
| retention on every topic written but never drained | `retention.bytes` set; it is a periodic sweep, not a bound | sink log 7× its cap between sweeps |
| the generator is deterministic | two fills with one seed are byte-identical | no expected answer can be computed |
| CPU cap mechanism chosen once | `--cpus` throughout **or** quota/period throughout; `--cpus 0` is a no-op, `--cpu-quota=-1` sets a period the daemon then will not change | second case measured at the first case's cap |
| the broker can cache the backlog | `kafkaMemory` minus `kafkaHeap` against the least any recorded configuration produced a table with | the broker reads the backlog back off disk, becomes the constraint instead of the cores, and the cases come back as ceilings — 44 minutes to find out |
| the keys divide evenly across subtasks | Flink's own key-group assignment, run out of the image under test, for every key set named in `keySets` | four keys do not spread over four subtasks by themselves. The demo's sixteen account keys land 5/3/4/4 at the default 128 key groups, which bounds that stage at 0.80 of linear at four cores and reads as a pipeline that did not scale |
| slots ≥ parallelism × jobs | compare before submitting | job waits for resources while the harness times an empty pipeline |
| transactional-ID prefix and consumer group are scoped per run | include the run id | 470-second cold start after ten runs; 22 dead series on the backlog panel |
| back-pressure counters exist on the endpoint you will read | dump the endpoint and read what is there | ten minutes on a deprecated path |
| every case runs the same collector | the collector name off the engine's metrics, on every case | `--cpus 1` picks the serial collector and every case above it runs G1, so the baseline is a different program — worth +19% and a whole superlinear step |
| the interview and the plan were written down | `ASSUMPTIONS.md` and `PLAN.md` exist before the stack does, and the plan names every disclosure | a run that never wrote down what it decided leaves a reader unable to tell what was agreed from what was assumed — and the six answers are the spec |
| nothing else is using the cores | load average against the core count, and what is busiest | a cap is a **share**, not a promise of cycles: on a busy host every case reads 100% of its cap and does less work for it, and no other column shows it |
| the VM trim command is known | `docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker` | space freed inside a Docker Desktop VM never returns to the host |

**Then the tiny proof, before any fill.** It is called tiny because it is short, **not because it is small**: it measures the drain rate that sizes everything else, so it needs enough data to warm up and hold a steady window at the fastest case. That is `rate x (warm-up + window + headroom)`, which for a fast pipeline is hundreds of millions of records — clean-room run 42 ran 400,000,000 and run 43 was told to run 391,613,400. This line used to say "a few thousand records", which is out by five orders of magnitude and led two runs to budget for a smoke test and get a second full one. Budget the disk and the minutes for it, and know that **every tuning change re-creates it**. End to end:

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
- a case that is **not the constraint is a ceiling, not a failure** — kept,
  reported, and left out of the steps, exactly as in the suite. The steps it
  would have been part of are not bounded, and the rest still are;
- **kill the pipeline mid-run and re-assert the totals** — a guarantee is a
  claim about failure and is untested until something has failed; finding out
  after the suite discards the suite;
- run one full case with a 10–15 s window so every line of the harness
  executes, **and fail one check on purpose** so you know the checks can fail.

Anything that can void the whole table is tested before the table exists.

## 4. Prove nothing was lost, separately from proving it is fast

A pipeline that drops one record in ten thousand looks fine in every throughput
column. Completeness is a separate run on a test data set small enough to **process
every last record**; measurement is a slice of a steady state and cannot check it.

The expected answer comes from the **input**, never from the pipeline: the
generator writes a manifest (record count, totals per key) and the sinks are
compared to that. Assert, with no tolerances:

| assertion | meaning of a miss |
|---|---|
| distinct keys = the number predicted in the interview | a key you did not intend, or one that never arrived |
| every aggregation sums to the manifest exactly | a lost or duplicated record |
| two paths over the same input agree exactly | same, located |
| **each key's published values never go backwards** | order within a key is the one ordering guarantee a keyed stream makes, and the interview asked for it. A key that goes backwards on a clean run means the sink is not keyed by the aggregation key, or a rebalance sits between the aggregation and the sink |
| **each key appears in exactly one partition** | per-key order cannot survive a key split across partitions, whatever the pipeline does. This is what makes the assertion above mean anything end to end |
| after killing the pipeline mid-run, all of the above still hold | the guarantee you configured is not the one you have |

**The killed arm is where the ordering rule earns its keep, and it needs
stating carefully.** A restart replays, so a key may step backwards **once**
and must then reach the same final value; more than once per key is a
different fault, and never going backwards at all on a *clean* run is not
negotiable. Clean-room run 35 measured exactly this shape — exactly one
backward step per key across 4 symbol and 16 account keys, every total still
exact — which is what an at-least-once sink made idempotent by the absolute
value is supposed to look like.

**Two settings, not one, and both are needed.** The user is asked what must be
exactly right (§1 q4); which settings deliver it is a build decision made here:

- **Sink** — emit the *absolute* value per key, not a delta. A repeat is then
  harmless, with no transactions and no commit-interval latency floor.
- **State** — exactly-once *checkpointing*. Without it a replayed record is
  folded into the snapshot twice, and the sink setting does nothing about it.

The usual answer is *exactly-once checkpointing, at-least-once sink*. Name both
in the report (§9), and expect the kill test above to be what proves them.

**Then diff the design against the build, and correct it before going on.**
Completeness proves the numbers in the topics the harness owns. It says
nothing about whether the job is the pipeline the interview asked for — and
that is a real gap, not a theoretical one: **two clean-room runs in a row
built one market value where the default business case asks for two**, by
symbol and by account / sub-account / symbol. Both drew a diagram of what they
meant. Every guard passed. Nobody held the job to the picture.

So the design is declared as lists that can be diffed, in `pipeline.json`'s
`design` block — the operators, the topics read, the topics written — and the
completeness run looks for each one in the running plan and on the broker:

| area | declared | built | what was found |
|---|---|---|---|
| operator | `sink-mv-by-account` | **NO** | no vertex in the running plan mentions it |
| output | `market-values-by-account` | **NO** | written by nothing — the topic is empty |

Anything declared and missing **fails the run**. Anything built and not
declared is printed and allowed — Flink adds vertices of its own, and the row
is there so a reader can see what else is in the graph.

**Then fix it and run completeness again. No fill, no suite, until the diff is
clean.** Usually the build is missing something the business case asked for, so
the build is what changes. If instead the design named something the interview
never asked for, change the design — and write in `ASSUMPTIONS.md` that you
did, and why. Either way the loop ends the same way: the two agree, and only
then is anything measured. Measuring first costs two to three hours to produce
a table for the wrong pipeline.

The suite report carries the graph a second way: **a Mermaid diagram rendered
from the plan the engine served**, not drawn. A drawing is a claim; that one
is the job.

**An assertion with nothing to compare is written down, not skipped.** The
table above is four assertions for a pipeline with two paths over one input.
A pipeline with one path cannot compare two, and a pipeline with no key the
interview predicted cannot check cardinality. Say in `ASSUMPTIONS.md` which
assertion does not apply and why. A reader can then tell a check that passed
from one that was never made, which is the whole point of a list with no
tolerances in it.

Record the build hash beside every number, and **gate throughput on this**: no
table is published for a build that has not passed. Put the same script in CI
from a cold start, so it stays true after this morning's change.

## 5. Measurement discipline

**Every case must run the same code, and the baseline is where that breaks.**
§5 lists what must not differ between the baseline and the other cases — a
shuffle, a network hop, a second JVM. Add the garbage collector: `--cpus 1`
makes the JVM see one processor and pick the **serial** collector, while every
case above it runs G1. Clean-room run 35 measured it. Pinning the collector
gave the one-core case **+19%** and moved 1→2 from **2.364× to 1.997×** — the
superlinear first step that three runs had reported was the baseline running
different code, not the pipeline scaling. Pin it in `flinkProperties`
(`env.java.opts.taskmanager: -XX:+UseG1GC`) and read it back off the engine's
own metrics, the same way the job graph is read back.

**Hold the machine still too.** A browser and a word processor are enough:
clean-room run 34 read 100% of cap in all ten cases and produced no usable
number at all, its readings 37%, 25% and 20% apart, because the host load
reached 7.18 on 8 cores while it measured. A cgroup cap is a share of what
the host has left, so the case still pins at its cap and simply does less
work per cycle — the one failure mode every resource column in the table is
blind to. The load average is recorded at the open and close of every window
for exactly this reason.

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
Internal back-pressure inside a single-slot case is expected — the
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
against. **The one-unit case is the weakest and the noisiest, so measure it
rather than assume it** — six recorded suites returned 1.978–2.161× on 1→2,
which is the ideal 2.00× within noise, so a healthy one-unit case is the norm
and not a hope. A 1→2 well above 2.00× is that case reading low, and it is
worth knowing: it says the step someone buys first is not what it appears to
be. The tiny proof bounds every step the suite will report (§3), so a bad
one-unit case costs ten minutes rather than a suite. Drop it only when the
claim genuinely starts higher up.

**Measure a drain, not a live generator.** Fill a backlog larger than the page
cache, stop the producer, measure the drain. Hold partitions, checkpoint
interval and backlog constant across cases; nothing varies but the one thing
under test. Do not shorten the interval to save time once cases have run — it
changes the number.

**Measure how much went through two independent ways.** The first is always
the committed offsets on the input topic. The second must not come from the
same place, or a stuck consumer group reads as a fast pipeline. The obvious
second reading is the outputs — their rows divided by a constant fan-out —
and it works for any pipeline whose outputs grow with its input.

**It does not work for a pipeline whose outputs are per window.** An hourly
average per location emits the same number of rows whether it read a thousand
readings an hour or a million; there is no fan-out to divide by, anywhere in
the job. Such a pipeline declares `secondVantage: {"mode": "command", "cmd":
…}` instead, and supplies a small program that prints
`{"inputRecordsProcessed": N}` — how much input its own outputs account for.
The harness runs it at each end of the window and compares the difference with
the committed offsets. This is delegated for the same reason correctness is
delegated to `verifier.cmd`: the harness cannot read progress out of an
arbitrary output, and whoever wrote the pipeline can. A pipeline that declares
neither is refused rather than measured once and called measured.

**Two things make a delegated second reading disagree, and neither shows in
the numbers.** *It moves in steps.* A windowed pipeline's progress jumps by a
whole window: work out how many input records **one window holds** and compare
it with how many a measurement window consumes **at the smallest case** — that
case reads slowest and has the finest requirement. One hour of a thousand
sensors at a reading a second is 3.6 million records; a one-core window that
consumes 47 million is 7.6% per step, against a 5% tolerance, and the two
counts then disagree at random. The fix is in the data or the job, not the
harness. *And they are read at different moments.* The committed offset is
where the source was at the **last checkpoint**; the outputs are where the
pipeline is **now**. If the rate is not flat across the window they differ by
the change in that lead — measured on one rig at 2 to 3 seconds of drain.

**A fan-out is a property of the job, not of the test data.** A uniform
generator makes any windowed pipeline *look* like it has one — 36,000 readings
a row, so `outputsPerInput: 0.0000278` would pass. It would be a lie: change
the data and the number changes. If the ratio is not fixed by what the job
does, declare a command instead.

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

**Do not poll the stack while it is measuring.** Anything that asks the engine,
the broker or Prometheus for a number is work on the same machine and inside
the same measurement. Clean-room run 42 opened two of its passes at load
averages of 8.2 and 13.5 on eight cores because it was querying Prometheus
mid-suite to watch progress. Read `results/PROGRESS.txt`, which the harness
writes anyway, and wait for `results/DONE`.

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
| the job graph differs from the other cases | vertex count, edge ship strategies and the key-group count read off the running plan. Flink picks `maxParallelism` from the parallelism when nothing sets it, so two cases can be given different key layouts — the same class of difference as a baseline with a different graph, one level down |
| the component under test is not the constraint | ≥95% of cap at every case, baseline included; external-boundary back-pressure not material; the broker never hits its own memory limit inside a window (a starved page cache depresses the rate while the cores still read 96% of cap). **Measured once where it did not:** run 42's broker hit its limit 2,771-2,840 times a window, the guard classified the two fastest four-core passes as ceilings on a tenth of a point of the 99% cap exemption, and kept the slowest -- leaving one usable reading where two are needed, so the step was voided. Giving the broker its page cache removed every hit and moved the four-core mean 0.5%, and the two discarded readings sat inside the band the case then produced. The guard found the right thing to change; what it cost was a 40-minute suite's headline number. One run is not a threshold, so nothing moves on it -- it is recorded so the next run that sees it is not the first. A case that misses is a **ceiling**: measured, reported with its rate as where scaling stops, and excluded from the ratios — never deleted |
| the input divides evenly across subtasks | partition count divisible by every parallelism under test (8 partitions serves 1, 2, 4; 6 would leave the 4-core case reading 2/2/1/1 and never reaching its cap) |
| an output the business case asks for was never written | after the completeness drain, every topic named in `topicsAlsoWritten` has records. These are the pipeline's own outputs — the throttled ones, outside `topics.out` — and nothing else looks at them. Two clean-room runs in a row built **one** market value where the default asks for two, and passed every other guard |
| the keys divide evenly across subtasks | the engine's own key-group assignment says where every key in `keySets` would land at every case. A subtask with **no keys** always refuses; an uneven one refuses when a `pipeline.max-parallelism` exists that would even it out, and is reported in the row when none does |
| memory is not the constraint | **every case gives its subtasks the same memory**, as a base plus a per-core share. Passing nothing does not leave memory to the engine: the image ships a flat figure — `flink:1.20.1` sets 1728m — so every case runs on the same total, which is the configuration this rule exists to refuse. Clean-room run 36 measured 2→4 at 1.510 on the image default against 1.743 with memory per subtask, and the GC ceiling did **not** catch it: GC was at its lowest, 1.40%, on the case losing the most. A case whose GC exceeds 5.5% of its capacity is still a ceiling, not a result |
| the claim itself | each step returns **1.90× or better** on a doubling, or the chain fails with the per-core, idle, GC and cap figures for both cases — a valid table that does not scale is a result about the pipeline, not a table to publish |
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

**A short step with every case pinned on CPU has nothing to change, so
measure before you guess.** Two cheap measurements, in this order, and only
then a change:

1. **`prove.py probe --repeats 9`** — can this machine do the step at all,
   with no pipeline in the way? Minutes, starts nothing. If the machine
   cannot, stop: there is nothing in the pipeline to find.
2. **`prove.py ceiling`** — is the largest case already against a ceiling?
   It holds that case at its size and starves the component beside it in
   steps. If the rate barely moves while the broker is squeezed, the broker
   is not the ceiling and the pipeline is at its own. A few short cases on
   the stack that is already up.

Neither is optional when a step falls short and the scorecard says CPU,
because *CPU* means every setting the run controls was already at its limit.
Guessing from there is what §8 exists to prevent, and a re-run costs what the
last one cost.

**Before looking at the pipeline, measure the machine.** A step that falls
short is being compared against a doubling the *host* may not deliver either.
`prove.py probe` runs the bare cores with no pipeline involved — minutes, and
it starts nothing — and preflight's three repeats are usually too loose to
settle it: run 31's memory-heavy 2→4 read 1.52× with a range of 1.41–1.76×,
wider than the shortfall it was offered to explain. **More repeats do not
narrow that range** — it runs from the lowest reading to the highest, and more
samples can only find more of the distribution. Clean-room run 36 raised its
repeats from three to nine, as it had been told to, and watched the range go
from 9% to 15%. What more repeats buy is the **middle half**, which the probe prints
from four repeats up. **It does not shrink either** — it converges on how
variable the machine actually is, which may be a lot: measured on one rig, the
memory-bound arm read 6% over nine repeats and 11% over twenty-five. An honest
figure, not a smaller one.

**Read the arms separately.** On that same run the register-only arm's middle
half was **0%** and the memory-bound arm's was **10%**, so one arm settled the
question and the other could not, and a single worst-of figure hid it. Compare
the relevant arm with the shortfall; if it is still wider, say the machine
cannot be ruled in or out here — that is a complete answer, and it costs one
line.
This is the first step of investigating a short step, not an aside — it is
cheap, and it decides whether there is anything in the pipeline to look for.

**When there is no human to say yes, tune until you run out of levers.**
**What is being tuned is the steps, not any case's speed.** The claim is
1→2 and 2→4, each 1.90× or better. Making the baseline faster makes the step
off it *smaller*, so a faster one-core case is not progress and can be the
opposite — the only thing worth fixing on a baseline is a way it differs from
the cases above it, and clean-room run 35 found exactly one: a different
garbage collector. Judge every change by what it did to the steps.

§6a is a finite list, so this terminates. One at a time, in its order:

1. Apply **one** change. Write in `FIXES.md` what you changed, which row of
   §6a it is, and what you expect it to move — *before* measuring, so the
   prediction can be wrong in public.
2. Measure. One change per measurement is what makes the number mean anything.
3. **If the step got worse, revert it.** A change that cost 0.05× is not a
   step toward anything, and leaving it in place means the next lever is
   measured against a pipeline you already know is worse. Clean-room run 35
   applied its one permitted change, moved 2→4 from 1.815× to 1.769×, and
   stopped there — worse than it started, because the rule said stop.
4. Stop when the target is met, when §6a has no untried row whose symptom
   matches, or after **four** changes. Four is not a measured number; it is a
   budget, and a run that spends it without meeting the target has found
   something worth a person's attention rather than another attempt.

Report every attempt, kept or reverted, with its prediction and what actually
happened. The ones that failed are the more useful half: run 35's memory fix
did nothing for the case it was named for, and its subtask fix moved source
idle exactly as predicted while the throughput went to the wrong cases.

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

**The report opens with a scorecard**, because a reader who gets one thing
should get this one: the speed at each size, what was holding each case back,
and whether doubling the resource doubled the work.

```
SCORECARD

  cores        speed   scaling     pipeline CPU   pipeline memory    Kafka CPU        Kafka memory   blocked by      what to do
      2    374,507/s         —         2 / 100%   uncapped / 2.0%    2.5 / 12%      6g, never full   Pipeline CPU    check it matches
      4    673,414/s     1.80x          4 / 97%   uncapped / 1.2%    2.5 / 25%      6g, never full   Pipeline CPU    investigate

  Each pair is what it was allowed and how much of that went:
    scaling           what the step into this case gave — nothing on the baseline
    pipeline CPU      cores it could use / how much of them it used
    pipeline memory   memory it could use / share of the time spent tidying memory up
    Kafka CPU         cores Kafka could use / how much of them it used
    Kafka memory      memory Kafka could use, and how often it filled up

  2 cores is the baseline. Do not tune it: making the baseline faster makes the step
      off it smaller, and the steps are what is being claimed. The only thing worth
      fixing here is a way it differs from the other cases — a different garbage
      collector, a different job graph, a cap that did not apply.
  4 cores: doubling gave 1.80x, short of the 1.90x target.

  2->4 cores: doubling gave 1.80x, target 1.90x  ->  missed
```

**The shape is a rule, not a preference**, because it drifted back twice in one
afternoon and each drift was reasonable on its own:

- **Two or three words per cell, and every sentence under the table.** Writing
  the advice into the row took it to 175 characters, one addition at a time.
  `prove.py selftest-pure` renders the longest branch there is and fails over
  136 — break it on purpose when adding a column, because a guard whose own
  failure case is unchecked is weaker than it looks. Writing that one found
  two columns colliding at seven-figure values.
- **A cell reads without the key.** `6.25g / 0` needed the legend; `6.25g,
  never full` does not.
- **The verdict names a column**, so a reader can check it against the numbers
  on the same row rather than take it on trust.
- **The advice reads the step, not just the case.** CPU being the limit is only
  good news if that step actually doubled.
- **A step above its ideal is never reported as met.** It clears the floor
  arithmetically and it is not a result: something below it read low. Say that,
  beside the number.
- **Say "target", not "needed"**, and get the plurals right — *1 core*, not
  *1 cores*. Both are self-tested, in the scorecard, the suite table and
  `suite.md` alike — three renderings of the same figures, and the rule held
  in one of them until clean-room run 36 read all three.
- **A ratio above its target is never called "short of" it.** 1.93× is not
  short of 1.90×; what is short is the lower bound the claim is judged on, and
  the line has to say which number it means.

The four measurements sit beside the answer rather than behind it, so a
reader can see why it says what it says. Each column is what the component
was given and how much of it went: the pipeline's cores,
the share of its time spent on memory, Kafka's share of its own cores, and
the number of times Kafka hit its memory limit. Where the answer is not CPU,
the sentence explaining it follows the table. Nothing here is new — the
figures were already recorded per case and already used by the guards, in
the order the guards apply them, so the answer never contradicts a ceiling
the run reported. Four of the answers name a column, so a reader can look
the verdict up rather than take it on trust, and each carries what to do
about it. CPU being the limit is only good news if the step into that case
actually doubled, so the advice reads the step as well as the case: a
baseline has nothing below it to compare against, a short step says investigate,
and a step above 2× means the smaller case reads low. The column holds two or
three words and the numbers go under the table, because advice written into
the row took it to 175 characters and stopped being a table. For
Kafka's memory it names the size to try, not just "more". *Pipeline CPU*
is the answer
the table depends on; anything else means the number measures something
other than what it claims to. **A step ratio without this column beside it is
a number with no idea what produced it.**

## 6a. Tuning what you built

Everything above measures. This is what to change, and it is the part a fresh
agent cannot work out for itself — each line was paid for by a run. **Ordered
by measured effect, one change at a time, both arms measured on one build with
the cases interleaved.** Anything else is a guess wearing a number.

| change | what it was worth | when to reach for it |
|---|---|---|
| **read the input once** | **+22.5% at 1 core, +24.4% at 4** | the source topic is read once per aggregation. Parse once and fan out through a side output — not a second source read, and not two chained operators, which copy the record per consumer with object reuse off |
| **memory per subtask, not one flat figure** | **about 14%** | a flat `tmMemory` divides across each case's subtasks. Measured: flat 2048m read 2→4 = 1.645 with GC at 9.3%; the same build with memory scaled per core read 1.910 with GC at 2.3%, and the 2-core figure did not move |
| **give the broker its page cache** | **about 13%** | a broker that cannot hold the backlog reads it off disk. `kafkaMemory` minus `kafkaHeap` is what it caches with |
| **compress the sink writes** | **−16% raw, and the claim becomes measurable** | the top case is *waiting to write*. Run 32's 4-core case sat at 93.7% of cap uncompressed and 99.0% with lz4: slower, and the first table of the two that was worth publishing |
| **fewer subtasks for the same cores** | **about 8%**, ~3 points of it the source idling | four subtasks where two would do |
| **make the pipeline cost something per record** | **the difference between measuring cores and measuring the broker** | every case sits below its cap with the source idle. A pipeline that is cheap per record is bound by the transport long before its cores: run 41 read 1.6 million records a second on one core and no case was CPU-bound until the input side was fixed — fetch sizing, record size, the broker's page cache. Look at source idle, not at the rate |
| **spread the keys evenly** | **up to 0.80 → 1.00 of linear** at four cores on the demo's own keys | the preflight row says the keyed stage's keys land unevenly. Add the `pipeline.max-parallelism` it names to `flinkProperties`. This is arithmetic, not a measurement: a subtask holding a quarter more keys than its neighbours does a quarter more work |

**Measure each change with `prove.py tinyproof`, not with a suite.** It runs
every case the suite will run, back to back on one build, and takes minutes
rather than most of an hour. **It can be re-run after the fill** — the disk
projection credits the backlog already on the broker, so the suite's input
topic stays where it is between levers. Before that credit existed the
projection counted the same backlog as used *and* as still to write, refused
a suite that fitted, and made every lever cost a delete and a re-fill, about
twelve minutes of broker I/O each (clean-room run 36). Confirm the winner with
the suite once, at the end.

**Two of these rows do not apply to every pipeline, let alone every
laptop.** *Read the input once* needs a pipeline that reads it twice, which
means two or more aggregations. *Compress the sink writes* needs a sink under
load. A pipeline with one aggregation and a throttled output has neither, and
arrives at the tuning loop with four levers rather than six — the two largest
numbers in the table among the missing. Check which rows your pipeline can
even use before you count how many changes you have left.

**The levers transfer; the percentages do not.** Every figure above was
measured on one pipeline on one laptop. Reading the input once is worth
something to any pipeline that reads it twice; whether it is worth 22% to
yours is a question your own two arms answer. Quote your number, not this
one.

**Two things were tested and changed nothing**, so do not spend a run on them:
partition count (8 against 16 — and 16 failed every parallelism-4 case for an
unstable warm-up) and network buffer fraction (0.15 against 0.30).

**The fourth row is the one that surprises people.** Compressing the writes
makes the pipeline *slower* and makes the measurement *valid*: a case that is
waiting on its sink is not measuring cores at all, so its number answers no
question. A slower table that means something beats a faster one that does not.

## 7. The dashboard explains; the harness measures

**Add it through `extraServices` in `pipeline.json`** — a map of service name to
a compose service body, spliced into the stack the harness generates. Two rules,
because the measurement depends on them: the container name must start with the
project prefix, or teardown leaves it behind and then fails for a survivor it
did not create; and give it a CPU cap, because anything sharing the cores under
test changes the number being measured. Whatever you add is recorded in the
results header, so a reader knows what else was on the machine. That is the only
sanctioned way — the harness is still not to be forked.

**Five of the seven panels need engine metrics, and those need a reporter**: set one through `flinkProperties` in `pipeline.json`, which reaches the job manager and every task manager. On `flink:1.20.x` that is all you need: the reporters ship **already installed** as plugins in `/opt/flink/plugins/metrics-*`, so the factory class resolves with no further help. **Do not set `ENABLE_BUILT_IN_PLUGINS` on these images.** That variable tells the entrypoint to link a jar out of `/opt/flink/opt`, which on `flink:1.20.1-scala_2.12-java17` contains no reporter at all; the entrypoint prints `Plugin … does not exist. Exiting.` and the container dies before the job manager starts. Verified by listing both directories in the image. This paragraph previously said the opposite and gave the variable as the fix — clean-room run 43 followed it verbatim and lost its first `prove.py up` to it, which is worse than the forty minutes run 42 lost having no guidance at all. `flinkEnv` remains for images that do keep reporters in `/opt/flink/opt` (older tags and the slim variants) and for any other environment the engine needs. Clean-room run 32 had no such hook, could not fork the harness, and spent about fifty minutes rebuilding the numbers from outside the engine — Kafka offsets, a tail of each sink, the REST API and the docker socket. The settings the measurement depends on are refused rather than silently overridden.

**The CPU-per-component panel needs the broker, and no reporter can give it to
you.** `flinkProperties` reaches the job manager and the workers; the panel's
whole point is the component *beside* them — §5's broker at 0.48 cores that is
still the ceiling. cAdvisor is the obvious answer and it does not work on
Docker Desktop for macOS: its Docker factory registers against the socket and
it still reports one series, because the cgroup tree it reads inside the VM
does not contain the containers. Use
**`harness/dashboard/docker_cpu_exporter.py`**, which ships with this skill:
about a hundred lines that read cumulative CPU nanoseconds per container off
the Docker API and serve them as a Prometheus counter. Copy it next to your
provisioning files, mount that **directory** — a single-file bind mount breaks
the moment the host file is rewritten by a tool that replaces the inode — and
add the `extraServices` entry from `harness/README.md`. Clean-room run 36 spent
half an hour on this, wrote the same thing, and said every run would write it
again. Nothing off it is ever quoted as a result: the harness reads the cgroup
counter directly for the table, and this is for looking at.

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

**The panel list is for a pipeline with fan-out; yours may not have one.**
Two of those seven ask a fan-out question — *rate per stage* and *the two
paths overlaid* — and a pipeline with one aggregation and a throttled output
can answer neither. Replace them with the same question in its own shape:
records read per second against rows published per second on a log scale, and
the parallel subtasks of the one aggregation overlaid. Keep the question, not
the panel.

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
per-unit cost *flatters* wider cases, and was offered to explain a
sub-linear one.

When a result is short: say what you measured, say what you have ruled out and
with what evidence, and say the cause is unknown. *"I do not know yet"* costs
one line.

## 9. Reporting

- **Lead with the step ratio** the reader would buy — two units to four — with
  what it needed and its spread. Baseline ratio second, stating what it is against.
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
