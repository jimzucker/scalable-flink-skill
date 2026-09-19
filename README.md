# scalable-flink-skill

[![CI](https://github.com/jimzucker/scalable-flink-skill/actions/workflows/ci.yml/badge.svg)](https://github.com/jimzucker/scalable-flink-skill/actions/workflows/ci.yml)

A free Claude Code skill that builds your data pipeline and proves it scales.

![What the skill does, and the scaling table it produces](docs/card.png)

Ask Claude to build a pipeline and prove it scales. About an hour later you
have the pipeline — and a scaling table, with the resource columns beside the
throughput, that holds up when someone pushes on it.

## Install

Clone it straight into your Claude Code skills directory:

```bash
git clone https://github.com/jimzucker/scalable-flink-skill \
  ~/.claude/skills/scalable-flink-skill
```

Then ask Claude to build your pipeline and prove it scales. The skill starts
the interview.

**You need** Docker, Python 3 (the harness is standard library only), and
JDK 17. The stack runs `flink:1.20.1-scala_2.12-java17` and
`apache/kafka:3.9.0` on one machine.

## What comes out

One measured suite, at 4,096 distinct keys:

| capacity | throughput | step |
|---|---:|---|
| 1 CPU | 58,326 records/s | |
| 2 CPUs | 120,115 records/s | **2.06×** [1.92, 2.18] |
| 4 CPUs | 238,804 records/s | **1.99×** [1.95, 2.03] |

Every case ran at 96.3–98.1% of its CPU cap, so the worker was the constraint
and not something beside it. Garbage collection was 3.8 / 0.8 / 0.3% of
capacity.

Make each message six times larger and the same pipeline still returns **4.57×**
across 1→4 CPUs.

Both results, with their raw output, are in the validation repository linked
under [Evidence](#evidence).

## What it does

1. **Interviews you before it builds.** One question at a time, each with a
   default: what goes in and what comes out, whether one input becomes several
   outputs, how many distinct keys, what has to be exactly right, who is
   watching, and — written down verbatim — the claim you want to make. Every
   later decision is judged against that sentence.
2. **Builds in reviewable steps.** One branch per step, each ending with the
   system running and measured rather than compiling.
3. **Proves nothing is lost, separately from proving it is fast.** A backlog
   small enough to drain to the last record, checked against a manifest
   computed from the input alone, with no tolerances — then again after killing
   a worker mid-drain. No throughput table is published for a build that has
   not passed.
4. **Measures each step up in capacity.** It caps one worker's CPU, raises
   parallelism to match, drains a fixed backlog, and leads with the step ratio
   you would actually buy.

## What you supply

Claude builds these with you; the harness expects them.

| piece | contract |
|---|---|
| a job jar | the Flink job, taking bootstrap, topics, parallelism and checkpoint interval as arguments |
| a generator | deterministic — two fills with one seed are byte-identical — writing a manifest of expected totals per key |
| a verifier | drains the outputs and exits non-zero on any loss |
| `pipeline.json` | describes the three, plus cases, passes, backlog and caps; start from [`harness/pipeline.example.json`](harness/pipeline.example.json) |

The full contract is [`harness/README.md`](harness/README.md).

## Running it

```bash
H=~/.claude/skills/scalable-flink-skill/harness/prove.py
nohup python3 $H all > results/all.log 2>&1 &
```

`all` is `up → preflight → completeness → tinyproof → fill → suite → report`,
stopping at the first step that does not pass. It writes `results/DONE` with
the verdict and the wall time. A full run takes about an hour on a laptop.

`--quick` takes about 48 minutes and stamps its table unpublishable — it tells
you the rig runs clean and roughly how fast, not what the ratio is.

## Limits

- **Flink on Kafka, today.** The interview and the measurement rules are
  general; the harness is not.
- **One machine.** The axis it measures is one worker growing, not workers
  multiplying across a network — a different measurement, with fixed costs paid
  again per worker.
- **About an hour per full run.** The completeness gate, the tiny proof and the
  fill do not shrink with `--quick`.

## Evidence

The skill was validated by running it 29 times from a clean room — a fresh
directory, one prompt, no human help, and Claude not allowed to read the
repository that wrote the rules. Every rule a run broke became a guard with a
self-test, or was deleted. The harness carries 38 such guards, and
`prove.py selftest` breaks all of them on purpose to check each one fires.
`harness/record/` holds the recorded runs every new threshold is replayed
against before it can block anything.

CI runs both on every push: `prove.py replay` against the recorded runs, and
`prove.py selftest-pure`, which breaks all 32 stack-free guards on purpose and
checks each one fires. Neither needs Docker.

Every run, and every measured pass, is public in the repository the skill was
built and validated in:

| what | where |
|---|---|
| the runs and what each one showed | [flink-training/docs/skill-validation](https://github.com/jimzucker/flink-training/tree/main/docs/skill-validation) |
| every measured pass in one file | [flink-training/docs/runs/ledger.csv](https://github.com/jimzucker/flink-training/blob/main/docs/runs/ledger.csv) |
| the 4,096-key suite above | [demo-under-harness.md](https://github.com/jimzucker/flink-training/blob/main/docs/skill-validation/demo-under-harness.md) |
| the six-times-larger message result | [payload-2k.md](https://github.com/jimzucker/flink-training/blob/main/docs/skill-validation/payload-2k.md) |

Those records call the skill `prove-it-scales`, its name until 2026-09-19.

## Licence

[Apache License 2.0](LICENSE) — use it, change it, ship it.
