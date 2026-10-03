"""Confluent Cloud: Kafka and Flink SQL as a managed service.

What is built: the stack's life -- up, size, read back, clear, down -- and the
checks around it: a budget guard before anything is created, a state file of
everything created so `down` can clean up after a crash, and a teardown that
lists the account afterwards and stops on any survivor. What is not built yet
(phase 3): submitting the SQL job, and the engine and broker readings a case is
judged on. Those stop with a plain sentence rather than a traceback.

The skill creates its own infrastructure and deletes it, because whatever
creates it is the only thing that can prove it is gone. An idle cluster costs
money. Credentials go to the file `credentials` names, readable by its owner
only, and are never printed, logged or written to the state file.

Every call goes through one runner, so the self-test can stand in a fake
Confluent CLI and nothing here needs a cloud account to be tested.
"""
import json
import os
import re
import stat
import subprocess
import time

from platforms import Platform, Refusal

# Prices read from Confluent's pricing page and Flink billing docs on
# 2026-10-03 (GCP us-east1). The budget guard uses them; when they move, the
# guard is wrong in the direction of whichever way they moved, so check them.
PRICE = {"flink_cfu_hour": 0.21, "kafka_gb_in": 0.05, "kafka_gb_out": 0.05,
         "basic_ecku_hour_after_first": 0.14}

# The JSON field names the CLI returns. Kept together so the first real run
# can correct them in one place.
F = {"id": "id", "status": "status", "endpoint": "endpoint", "name": "name",
     "max_cfu": "max_cfu", "current_cfu": "current_cfu",
     "api_key": "api_key", "api_secret": "api_secret", "key": "key",
     "description": "description"}

READY = {"UP", "PROVISIONED", "RUNNING", "READY"}

# A compute pool's maximum can only be one of these. Measured on 2026-10-03:
# creating one at 1 CFU was answered "MaxCfu is not one of 5, 10, 20, 30, 40,
# 50: 1". A case's size is that maximum, so the cases are 5, 10, 20 -- still
# two doublings -- and never 1, 2, 4.
POOL_SIZES = (5, 10, 20, 30, 40, 50)


def run_cli(args, timeout=600):
    """The real Confluent CLI. Returns (exit code, stdout, stderr)."""
    p = subprocess.run(["confluent"] + list(args), capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def estimate_usd(cases, passes, case_minutes=6.0, settle_cases=6, gb_per_case=6.0, fill_gb=20.0,
                 hours_up=None, max_ecku=10):
    """Pure. What one suite is expected to cost, from the prices above. Rough
    by design and on the high side: every case is billed at its full size for
    its whole length, and every case reads and writes gb_per_case."""
    sizes = list(cases) * passes + [min(cases)] + [max(cases)] * settle_cases
    cfu_hours = sum(sizes) * case_minutes / 60.0
    data_gb = fill_gb + gb_per_case * len(sizes)
    # Every GB billed both ways, in and out: an over-estimate, on purpose.
    total = cfu_hours * PRICE["flink_cfu_hour"] + data_gb * (PRICE["kafka_gb_in"] + PRICE["kafka_gb_out"])
    # The cluster can scale itself up to max_ecku while the stack is up; the
    # first eCKU is free. Billed as if it sat at the cap the whole time.
    hours = hours_up if hours_up is not None else len(sizes) * case_minutes / 60.0 + 1.0
    total += hours * max(0, max_ecku - 1) * PRICE["basic_ecku_hour_after_first"]
    return round(total, 2)


class ConfluentCloud(Platform):
    kind = "confluent-cloud"
    unit = "CFU"

    def __init__(self, raw, runner=None, log=print, state_dir="results"):
        raw = raw or {}
        self.environment = raw.get("environment")
        if not self.environment:
            raise Refusal("rig", "platform.environment is not set: name the Confluent Cloud environment "
                                 "(env-...) the run may create things in")
        self.cloud = raw.get("cloud", "gcp")
        self.region = raw.get("region", "us-east1")
        self.prefix = raw.get("prefix") or "fsk"
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.prefix):
            raise Refusal("rig", f"platform.prefix {self.prefix!r} must be lower-case letters, digits "
                                 f"and dashes: every resource the run creates carries it")
        self.credentials = os.path.expanduser(raw.get("credentials", "~/.confluent/flink-skill.env"))
        self.budget = float(raw.get("budgetUsd", 150.0))
        # A Basic cluster scales itself up to 50 eCKUs by default ("max_ecku": 50
        # in its own description), each after the first billed by the hour. The
        # run caps it, so a busy suite cannot quietly cost $7 an hour.
        self.max_ecku = int(raw.get("maxEcku", 10))
        self.estimate = raw.get("estimateUsd")          # set by the caller from the suite plan
        self.run = runner or run_cli
        self.log = log
        self.state_path = os.path.join(raw.get("stateDir") or state_dir, "confluent-state.json")
        self.state = self._load_state()

    # ------------------------------------------------------------ plumbing
    def _load_state(self):
        try:
            return json.load(open(self.state_path))
        except (OSError, ValueError):
            return {"cluster": None, "pool": None, "keys": [], "statements": []}

    def _save_state(self):
        os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=1)           # ids only: never a secret

    # Commands that take no --environment. Measured 2026-10-03: `api-key delete`
    # answered "unknown flag: --environment", and the run's Flink key survived
    # the teardown that found it.
    NO_ENVIRONMENT = (("api-key", "delete"),)

    def cli(self, *args, quiet=False):
        """Run one CLI command with the environment set where the command takes
        one. `quiet` keeps the output out of every message: an API key's secret
        travels in it."""
        env = [] if tuple(args[:2]) in self.NO_ENVIRONMENT else ["--environment", self.environment]
        rc, out, err = self.run(list(args) + env)
        if rc != 0:
            what = " ".join(args)
            raise Refusal("rig", f"confluent {what} did not succeed: "
                                 + ("(output withheld: it may hold a secret)" if quiet else (err or out).strip()[:300]))
        return out

    def cli_json(self, *args, quiet=False):
        out = self.cli(*args, "-o", "json", quiet=quiet)
        try:
            return json.loads(out) if out.strip() else None
        except ValueError:
            raise Refusal("rig", "the Confluent CLI did not return JSON"
                                 + ("" if quiet else f": {out.strip()[:200]}"))

    poll_s = 10      # the self-test sets 0

    def _wait(self, describe, what, timeout_s=1200, every=None):
        every = self.poll_s if every is None else every
        t0 = time.time()
        while True:
            d = describe() or {}
            if str(d.get(F["status"], "")).upper() in READY:
                return d
            if time.time() - t0 > timeout_s:
                raise Refusal("rig", f"{what} was still {d.get(F['status'])!r} after {timeout_s} s")
            time.sleep(every)

    # ------------------------------------------------------------ the contract
    def up(self):
        # Budget first, before anything is created or paid for.
        if self.estimate is not None and float(self.estimate) > self.budget:
            raise Refusal("rig", f"this run is estimated at ${float(self.estimate):.2f}, over the "
                                 f"${self.budget:.2f} budget. Nothing was created.")
        if not self.state.get("cluster"):
            c = self.cli_json("kafka", "cluster", "create", f"{self.prefix}-kafka", "--cloud", self.cloud,
                              "--region", self.region, "--type", "basic", "--max-ecku", str(self.max_ecku))
            self.state["cluster"] = c[F["id"]]
            self._save_state()
        cluster = self._wait(lambda: self.cli_json("kafka", "cluster", "describe", self.state["cluster"]),
                             f"Kafka cluster {self.state['cluster']}")
        if not self.state.get("pool"):
            p = self.cli_json("flink", "compute-pool", "create", f"{self.prefix}-flink", "--cloud", self.cloud,
                              "--region", self.region, "--max-cfu", str(POOL_SIZES[0]))
            self.state["pool"] = p[F["id"]]
            self._save_state()
        self._wait(lambda: self.cli_json("flink", "compute-pool", "describe", self.state["pool"]),
                   f"Flink compute pool {self.state['pool']}")
        secrets = {}
        if not self.state.get("keys"):
            for label, extra in (("KAFKA", ["--resource", self.state["cluster"]]),
                                 ("FLINK", ["--resource", "flink", "--cloud", self.cloud, "--region", self.region])):
                k = self.cli_json("api-key", "create", *extra, "--description", f"{self.prefix} {label.lower()}",
                                  quiet=True)
                self.state["keys"].append(k[F["api_key"]])
                self._save_state()
                secrets[f"{label}_API_KEY"] = k[F["api_key"]]
                secrets[f"{label}_API_SECRET"] = k[F["api_secret"]]
            self._write_credentials(secrets, cluster)
        self.log(f"  confluent-cloud up: cluster {self.state['cluster']}, pool {self.state['pool']}, "
                 f"{len(self.state['keys'])} API keys; credentials in {self.credentials}")
        return True

    def _write_credentials(self, secrets, cluster):
        lines = {"CONFLUENT_ENVIRONMENT": self.environment, "CONFLUENT_CLOUD": self.cloud,
                 "CONFLUENT_REGION": self.region, "KAFKA_CLUSTER_ID": self.state["cluster"],
                 "KAFKA_BOOTSTRAP": str(cluster.get(F["endpoint"], "")).replace("SASL_SSL://", ""),
                 "FLINK_COMPUTE_POOL": self.state["pool"], **secrets}
        os.makedirs(os.path.dirname(self.credentials), exist_ok=True)
        fd = os.open(self.credentials, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            for k, v in lines.items():
                f.write(f"{k}={v}\n")
        os.chmod(self.credentials, 0o600)
        # Assert the effect: the file exists, only its owner can read it, and
        # every key is in it -- checked without reading a value back out.
        mode = stat.S_IMODE(os.stat(self.credentials).st_mode)
        names = {l.split("=", 1)[0] for l in open(self.credentials) if "=" in l}
        if mode != 0o600 or not set(lines) <= names:
            raise Refusal("rig", f"the credentials file was not written as intended (mode {oct(mode)})")

    def set_size(self, units):
        if int(units) not in POOL_SIZES:
            raise Refusal("rig", f"a Confluent Cloud compute pool cannot be {units} CFU: its size can only "
                                 f"be {', '.join(map(str, POOL_SIZES))}. Set cases to sizes from that list, "
                                 f"for example 5, 10 and 20")
        self.cli("flink", "compute-pool", "update", self.state["pool"], "--max-cfu", str(int(units)))

    def read_size(self):
        d = self.cli_json("flink", "compute-pool", "describe", self.state["pool"]) or {}
        return int(d.get(F["max_cfu"], -1))

    def current_cfu(self):
        """What the pool is using now, as Confluent reports it."""
        d = self.cli_json("flink", "compute-pool", "describe", self.state["pool"]) or {}
        return int(d.get(F["current_cfu"], 0))

    def _statements(self):
        if not self.state.get("pool"):
            return []
        rows = self.cli_json("flink", "statement", "list", "--compute-pool", self.state["pool"],
                             "--cloud", self.cloud, "--region", self.region) or []
        return [r[F["name"]] for r in rows if str(r.get(F["name"], "")).startswith(self.prefix)]

    def clear_size(self):
        names = self._statements()
        if names:
            self.cli("flink", "statement", "delete", *names, "--cloud", self.cloud, "--region", self.region,
                     "--force")
        left = self._statements()
        if left:
            raise Refusal("rig", f"statements still in the pool after deleting them: {', '.join(left)}")

    def surviving(self):
        """Everything this run's prefix names that still exists, as Confluent lists it."""
        out = []
        for c in self.cli_json("kafka", "cluster", "list") or []:
            if str(c.get(F["name"], "")).startswith(self.prefix):
                out.append(f"Kafka cluster {c[F['id']]}")
        for p in self.cli_json("flink", "compute-pool", "list") or []:
            if str(p.get(F["name"], "")).startswith(self.prefix):
                out.append(f"Flink compute pool {p[F['id']]}")
        for k in self.cli_json("api-key", "list") or []:
            if str(k.get(F["description"], "")).startswith(self.prefix + " "):
                out.append(f"API key {k.get(F['key'])}")
        return out

    def down(self):
        """Delete everything the run created, then list the account and stop on
        any survivor. Each deletion is attempted even if one before it failed."""
        problems = []
        try:
            self.clear_size()
        except Refusal as e:
            problems.append(e.msg)
        for kind, ids, cmd in (("API key", list(self.state.get("keys") or []), ["api-key", "delete"]),
                               ("pool", [self.state["pool"]] if self.state.get("pool") else [],
                                ["flink", "compute-pool", "delete"]),
                               ("cluster", [self.state["cluster"]] if self.state.get("cluster") else [],
                                ["kafka", "cluster", "delete"])):
            for i in ids:
                try:
                    self.cli(*cmd, i, "--force")
                except Refusal as e:
                    problems.append(e.msg)
        left = self.surviving()
        if os.path.exists(self.credentials):
            os.remove(self.credentials)                 # its keys no longer exist
        if left:
            raise Refusal("rig", "after teardown these still exist on Confluent Cloud and may cost "
                                 "money: " + "; ".join(left)
                                 + (". Deleting them reported: " + "; ".join(problems) if problems else ""))
        self.state = {"cluster": None, "pool": None, "keys": [], "statements": []}
        self._save_state()
        self.log("  confluent-cloud down: nothing with prefix " + self.prefix + " survives")
        return True

    # ------------------------------------------------------------ phase 3
    def _not_yet(self, what):
        raise Refusal("rig", f"{what} on Confluent Cloud is not built yet: the stack can be created, "
                             f"sized and torn down, but the SQL job and the readings a case is judged "
                             f"on come next")

    def submit(self, par, group, ckpt_ms=None):
        self._not_yet("submitting the job")

    def cpu_stat(self, component):
        self._not_yet("reading the engine's and broker's use")

    def mem_stat(self, component):
        self._not_yet("reading the broker's memory")
