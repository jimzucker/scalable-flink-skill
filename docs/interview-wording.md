# The interview, as a stranger meets it

**A working document for one revision, not a second source of truth.** The
interview itself lives in [`SKILL.md`](../SKILL.md) §1 and that is the only
copy that runs. This file exists so the wording can be read as a script and
diffed before it is changed; once the change lands in `SKILL.md`, this becomes
a record of what changed and why.

## Why it is being revised

The interview is the first thing a stranger meets, and it is written in this
project's private vocabulary. Two failures, both reported by the author on
2026-09-20 after watching the questions asked back:

- **Question 1's offered default was "the same block-trade problem"** — the
  problem this repository happens to have tested. To anyone who has not read
  the validation record that names nothing. A default should demonstrate the
  *shape* of a usable answer, not presume the reader shares ours.
- **Question 8 does not parse.** *"One worker growing"*, *"workers
  multiplying"*, *"a growing worker amortises its fixed cost"* — the author,
  who commissioned the skill, could not read it. It is the question that
  decides what gets measured, so it is the worst one to lose a reader on.

Two more have the same defect and are included:

- **Question 2** leads with "fan-out ratio".
- **Question 4** puts *idempotent sink*, *keyed state* and *exactly-once
  checkpointing* in one breath. The subject is genuinely technical, but the
  question can be plain even when the answer is not.

The other five are left alone.

## Current wording

Verbatim from `SKILL.md` §1.

1. **What is the input event, and what comes out?** One sentence, in domain
   language. This is the spec.
2. **Does one input become several outputs?** The fan-out ratio decides where
   the load lands: at 5× the write side is five times the read side and is
   usually what saturates first — and what fills the disk.
3. **What are the keys, and how many distinct ones?** Small fixed key counts
   make outputs arithmetic, so you can assert exact numbers instead of
   tolerances.
4. **What has to be exactly right?** If an output is a running sum, a replayed
   record is a wrong number, not a duplicate. Then decide **two settings, not
   one**: emitting the *absolute* value per key makes the **sink** idempotent
   (no transactions, no commit-interval latency floor); it does nothing for the
   keyed **state** the value is computed from, which needs exactly-once
   *checkpointing* or replayed records are folded into the snapshot twice. The
   usual answer is *exactly-once checkpointing, at-least-once sink*. Name both
   in the report, and expect to test it by killing a worker (§4).
5. **Who watches the demo, and what must they believe at the end?** Capacity for
   managers and correctness for engineers are different builds.
6. **Where does it run?** Default to a laptop; make the user argue you out of it.
7. **What claim do you want to make?** Write it verbatim. Every later decision
   is judged against that sentence.
8. **Which axis is the claim about?** *One worker growing* (cap one container's
   CPU and raise its parallelism — the laptop proxy) or *workers multiplying* (a
   second JVM with its own heap, GC and network — what a vendor sells as a
   unit). They are not the same measurement: a growing worker amortises its
   fixed cost, a multiplied one pays it again. Record the axis as a field in
   the results header, and on every case **parallelism = CPU cap = allocated
   slots**, read back from the engine.
9. **Which API level?** A declarative/SQL layer plans the graph for you and the
   plan can change between versions; hand-written operators are a graph you
   own. Neither is more valid, but the claim differs. Default to what the
   audience runs in production, and say which next to the numbers.

## Proposed wording

Four changed, five untouched. The rule applied: **the question is plain even
when the answer is not**, and a default shows the shape of a usable answer
rather than presuming the reader shares ours.

### 1 — the default is described, not referred to

> **What goes into the pipeline, and what comes out?** A few sentences, in your
> own words. This becomes the spec.
>
> *(For example: "An order arrives with a unique id and symbol, order quantity
> and a list of allocations; each allocation is keyed by account / sub-account
> and carries a quantity. The pipeline maintains positions by
> account+subaccount+symbol and by symbol. That input is the one scaled up to
> drive the pipeline to capacity. A second input carries prices, keyed by symbol
> and timestamp; the pipeline joins those to the positions and emits a position
> and market value every 10 seconds.")*

The symbol is on the **order**, not on each allocation, and an allocation is
keyed by account / sub-account alone — which is what the code does:
`AccountKey.of(allocation, trade.symbol())` takes the symbol from the trade. An
earlier draft of this example had the symbol on the allocation and was wrong.

**The first diagnosis was wrong.** The fault was not that the default used this
project's problem — it was that it *referred* to it ("the same block-trade
problem") instead of *describing* it, so it named nothing to anyone who had not
read the validation record. Written out, the same example stands alone and does
more work than a neutral one-liner: it shows two inputs, nested structure,
composite keys, two aggregations, a join, an emit cadence, and which input
drives the load. A reader with a different problem now knows the level of detail
expected of them.

The author supplied this wording on 2026-09-20; it is used as given.

Consequence: **"One sentence" becomes "a few sentences."** The old instruction
argued against the example — an answer with this much in it cannot be one
sentence, and the detail is the point.

### 2 — "fan-out ratio" is not a phrase anyone arrives with

> **Does one input produce more than one output?**
>
> *Default: one trade input produces 1 position per symbol and one position per
> allocation. If the order has 4 allocations it emits 5 records. For the market
> value we want to throttle it to a configurable interval defaulting to 10
> seconds.*

The term is gone. The default is the author's
wording, and it settles the market value in the right place: a throttle you
choose, not an accident of the design. It stays out of the 5 because the
fan-out number counts outputs *per input*, and a throttled emit is per
interval. Counting a timer-driven output as fan-out is what makes the
two-vantage guard disagree.

### 8 — the question that decides what gets measured

> **When you say it scales, which do you mean?**
>
> **(a)** Give **one machine more cores** and it does proportionally more work.
> **(b)** Add **more machines** and it does proportionally more work.
>
> *These are different measurements and give different numbers, so the answer
> decides what gets built and measured. (a) is what a laptop can prove.
> (b) needs real machines, because every new machine pays its own start-up
> cost — its own memory, its own garbage collection, its own network hops —
> while adding cores to one machine does not.*
>
> *(Default: (a), one machine with more cores.)*

Gone: *axis*, *one worker growing*, *workers multiplying*, *amortises its fixed
cost*, and the parenthetical about parallelism and slots — which is an
instruction to the harness, not a question to a person, and already lives in §5.

### 4 — plain question, technical answer

> **What has to be exactly right?**
>
> *If an output is a running total, a record processed twice is a wrong number,
> not a duplicate — so this usually matters more than it first sounds.*
>
> *If it does matter, there are two separate settings and both are needed:*
> - *the **sink**, where sending the absolute total per key rather than "add 3"
>   makes a repeat harmless;*
> - *the **saved state** the total is computed from, which needs exactly-once
>   checkpointing, or a replayed record is counted twice inside the snapshot.*
>
> *(Default: exactly-once checkpointing, at-least-once sink. Both get named in
> the report, and it gets tested by killing a worker mid-run.)*

The question is now one short line. The jargon survives, in the explanation,
where a reader who needs it will find it and a reader who does not can take the
default.

### 3 — the question stands; the default says to specify cardinality

Question text unchanged. The default is new:

> **What are the keys, and how many distinct ones?**
>
> *Default: two key spaces. Symbol, and account / sub-account / symbol.*
>
> *Make sure the cardinality is realistic, as 4K symbols vs 4 will materially
> impact the application design.*

The old default read "symbol, 4 of them; and account+symbol, 16" with no
indication of where the numbers came from, and it is wrong besides: the
account key is account+subaccount+symbol.

The author's second line replaces a weaker one of mine that said cardinality
"changes the memory budget". It changes the design: state size, garbage
collection, partitioning, and whether asserting an exact total per key is
practical at all. And it asks for the right thing — a *realistic* number, not
merely a stated one. This project measured the difference: 4,096 keys ran
clean, and 32,768 could not be measured on a 7.8 GB VM by either pipeline,
both hitting the garbage-collection ceiling.

Small fixed counts are still what make the verifier exact, which is why the
worked example uses small ones. That is a property of the example, not advice
to shrink a real problem.

### Left alone

5, 6, 7 and 9. They already read plainly, and 7 in particular should stay
blunt: *"What claim do you want to make? I write it down verbatim."*

Their **defaults** follow from question 1 rather than being canned. If the
reader takes the worked example, the example continues into them — keys are
account+symbol and symbol, the claim is about orders per second. If the reader
describes their own problem, the defaults are derived from that instead. Only
question 5 has no default at all: nobody but the reader knows their audience.

## What this costs

Nothing in behaviour — the interview asks the same nine things in the same
order and gates on the same two. It is a wording change to the first thing a
stranger reads.

## What the reader actually hears

The same nine, as they are put to someone one at a time. Recorded because the
defect is in what reaches the reader, and that is not identical to what the
file says.

1. What is the input event, and what comes out? One sentence, in domain
   language. *(Default: the same block-trade problem — trades in, running
   positions per account and per symbol out.)*
2. Does one input become several outputs? *(Default: yes, 5 — one position
   update per trade on the symbol side, plus one per allocation on the account
   side, at four allocations a trade.)*
3. What are the keys, and how many distinct ones? *(Default: symbol, 4 of them;
   and account+symbol, 16.)*
4. What has to be exactly right? *(Default: exactly-once checkpointing for the
   keyed state, at-least-once sink made idempotent by emitting the absolute
   position per key.)*
5. Who watches the demo, and what must they believe at the end? *(Default:
   engineers, who need the numbers exact and the thing to scale.)*
6. Where does it run? *(Default: this laptop, in Docker.)*
7. What claim do you want to make? I write it down verbatim. *(Default:
   "Doubling the worker's cores doubles the throughput, from two to four.")*
8. Which axis is the claim about — one worker growing, or workers multiplying?
   *(Default: one worker growing.)*
9. Which API level? *(Default: DataStream with hand-written operators.)*

With every default accepted it stops after 7, carrying 8 and 9 as stated
assumptions: the claim and the fan-out are the gate, and they are both known by
then.
