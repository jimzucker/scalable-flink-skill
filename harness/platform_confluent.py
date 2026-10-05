"""Confluent Cloud: Kafka and Flink SQL as a managed service.

What is built: the stack's life -- up, size, read back, clear, down -- and the
checks around it: a budget guard before anything is created, a state file of
everything created so `down` can clean up after a crash, and a teardown that
lists the account afterwards and stops on any survivor. Also the readings a
cloud case needs: the Kafka log end of a topic, read with the Kafka tools
against the cloud cluster, and Confluent's per-minute metrics for a statement
-- CFU in use, records read, busy, held back and idle time. What is not built
yet: the harness's case loop calling these. Until then `submit` and the
cgroup-shaped readings stop with a plain sentence rather than a traceback.

Measured on 2026-10-03, and why the code is shaped as it is:
- A pool's size cannot be lowered ("Reducing the max_cfu of a compute pool is
  currently unsupported"), and a suite comes back down to its baseline. So
  every case gets a new pool of its own size, and the last one is deleted.
  `up` makes one more pool, for setup statements and the fill.
- A statement starts at 1 CFU and took about three minutes to grow to a 10 CFU
  pool's limit. A case's warm-up has to cover that.
- Confluent keeps no consumer-group offsets for a statement's reads: Kafka's
  own tools listed none while a drain ran. The window cannot be anchored on
  committed offsets the way it is locally.
- The per-minute "records read" of five drains added up to 72-94% of the
  records the backlog held, while the fill's "records written" matched its
  topic to 0.002%. Why is not known; records read is reported, never used as
  the rate.
- A drain in a 20 CFU pool held at 10 CFU for minutes with tens of millions of
  records still waiting, three times out of three, at 24 and at 48 input
  partitions alike, held back 700-850 ms of every second. Why is not known.
  Until it is, a 20 CFU case cannot show scaling: its pool is not what binds.
- The fill: one generator job used 1 CFU and wrote about 12,700 records a
  second; eight side by side wrote 75,000; sixteen wrote 187,000.

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

# Watermark alignment is on by default on Confluent Cloud: a partition whose
# event times run ahead of the others is paused ("Blocked" in the Query
# Profiler) until the slow ones catch up. Measured 2026-10-04 (flink-training
# findings §8): 18 of 24 partitions paused 20-66% of the time; with the
# allowed drift raised to a day none paused and the copy read 46-47 M/min
# against 30.6-31.6. The laptop's jobs have no alignment, so every job
# statement the harness runs turns it off, and reads the setting back.
ALIGNMENT_OFF = {"sql.tables.scan.watermark-alignment.max-allowed-drift": "1 d"}


def uneven_cases(partitions, cases):
    """Pure. The cases whose subtask count does not divide the partitions:
    1 CFU ran one subtask (Query Profiler, findings §8), so a case's size is
    its subtask count, and the laptop's rule applies unchanged."""
    return [n for n in cases if partitions % n]


# The metrics API. The query endpoint is the only place busy, held-back and idle
# time exist: they are "exportable": false, so never in the Prometheus export.
METRICS_API = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"
STATEMENT_METRICS = {"cfu": "io.confluent.flink/statement_utilization/current_cfus",
                     "recordsIn": "io.confluent.flink/num_records_in",
                     "recordsOut": "io.confluent.flink/num_records_out",
                     "pending": "io.confluent.flink/pending_records",
                     "busyMsPerS": "io.confluent.flink/task/busy_time_ms_per_second",
                     "heldBackMsPerS": "io.confluent.flink/task/backpressure_time_ms_per_second",
                     "idleMsPerS": "io.confluent.flink/task/idle_time_ms_per_second"}


def run_cli(args, timeout=600):
    """The real Confluent CLI. Returns (exit code, stdout, stderr)."""
    p = subprocess.run(["confluent"] + list(args), capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def run_docker(args, timeout=240):
    """The real docker command. Returns (exit code, stdout, stderr)."""
    p = subprocess.run(["docker"] + list(args), capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def http_json(url, auth_b64, body=None, timeout=60):
    """One call to the metrics API. Returns (HTTP status, parsed reply or text)."""
    import urllib.request
    import urllib.error
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Basic " + auth_b64,
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            status = r.status
    except urllib.error.HTTPError as e:
        text, status = e.read().decode(), e.code
    try:
        return status, json.loads(text)
    except ValueError:
        return status, text


def rest_json(method, url, auth_b64, body=None, timeout=60):
    """One call to the Flink REST API. Returns (HTTP status, parsed reply or text)."""
    import urllib.request
    import urllib.error
    ctype = "application/json-patch+json" if method == "PATCH" else "application/json"
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Basic " + auth_b64, "Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text, status = r.read().decode(), r.status
    except urllib.error.HTTPError as e:
        text, status = e.read().decode(), e.code
    try:
        return status, json.loads(text)
    except ValueError:
        return status, text


def parse_offsets(text, topic):
    """kafka-get-offsets.sh output -> (total, {partition: offset}). Pure."""
    per = {}
    for line in text.split():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[0] == topic and parts[1].isdigit() and parts[2].isdigit():
            per[int(parts[1])] = int(parts[2])
    return sum(per.values()), per


def minutes_of(reply):
    """A metrics query reply -> [(ISO minute, value)], sorted. Values arrive as
    numbers or strings; both are read as floats. Pure."""
    rows = []
    for r in (reply or {}).get("data") or []:
        try:
            rows.append((str(r["timestamp"]), float(r["value"])))
        except (KeyError, TypeError, ValueError):
            continue
    return sorted(rows)


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
        # An environment id (env-...) or a name. A name that does not exist yet
        # is created by up and deleted by down; an existing one is only used.
        # Assets are named after the project, not left in "default" (asked for
        # on 2026-10-03): platform.environment "flink-training" and, unless set,
        # the same prefix.
        self.environment_ref = raw.get("environment")
        if not self.environment_ref:
            raise Refusal("rig", "platform.environment is not set: give the Confluent Cloud environment's "
                                 "id (env-...) or a name, such as the project's; a name that does not "
                                 "exist yet is created and later deleted")
        self.cloud = raw.get("cloud", "gcp")
        self.region = raw.get("region", "us-east1")
        self.prefix = raw.get("prefix") or (None if self.environment_ref.startswith("env-")
                                             else self.environment_ref.lower())
        if not self.prefix:
            raise Refusal("rig", "platform.prefix is not set: with an environment id there is no name to "
                                 "take it from, and every resource the run creates carries it")
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
        # The pool `up` makes for setup statements and the fill: each fill job
        # used 1 CFU when measured, so this is also how many fill jobs can run.
        self.setup_cfu = int(raw.get("setupCfu", 20))
        if self.setup_cfu not in POOL_SIZES:
            raise Refusal("rig", f"platform.setupCfu {self.setup_cfu} is not a size Confluent allows: "
                                 f"{', '.join(map(str, POOL_SIZES))}")
        self.kafka_image = raw.get("kafkaImage", "apache/kafka:3.9.2")
        self.run = runner or run_cli
        self.docker = raw.get("_docker") or run_docker
        self.http = raw.get("_http") or http_json
        self.rest = raw.get("_rest") or rest_json
        self.retry_wait_s = 30
        self.log = log
        self.state_path = os.path.join(raw.get("stateDir") or state_dir, "confluent-state.json")
        self.state = self._load_state()
        self.state.setdefault("statements", [])
        self.state.setdefault("pools", [])
        self.state.setdefault("casePool", None)
        self.environment = self.state.get("environment") or (
            self.environment_ref if self.environment_ref.startswith("env-") else None)

    # ------------------------------------------------------------ plumbing
    def _load_state(self):
        try:
            return json.load(open(self.state_path))
        except (OSError, ValueError):
            return {"environment": None, "environmentCreated": False, "cluster": None, "pool": None,
                    "casePool": None, "pools": [], "keys": [], "statements": []}

    def _save_state(self):
        os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=1)           # ids only: never a secret

    # Commands that take no --environment. Measured 2026-10-03: `api-key delete`
    # answered "unknown flag: --environment", and the run's Flink key survived
    # the teardown that found it. `organization list` answered the same on
    # 2026-10-04, when a confirming case asked it for the baseline's REST URL.
    NO_ENVIRONMENT = (("api-key", "delete"), ("environment", "list"), ("environment", "create"),
                      ("environment", "delete"), ("organization", "list"))

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

    def _environment(self, create=False):
        """The environment's id, found by id or name; created when asked and
        it does not exist."""
        if self.environment:
            return self.environment
        ref = self.environment_ref
        for e in self.cli_json("environment", "list") or []:
            if ref in (e.get(F["id"]), e.get(F["name"])):
                self.environment = e[F["id"]]
                self.state["environment"] = self.environment
                self._save_state()
                return self.environment
        if not create:
            return None
        e = self.cli_json("environment", "create", ref)
        self.environment = self.state["environment"] = e[F["id"]]
        self.state["environmentCreated"] = True
        self._save_state()
        self.log(f"  confluent-cloud: created environment {ref} ({self.environment})")
        return self.environment

    # ------------------------------------------------------------ the contract
    def up(self):
        # Budget first, before anything is created or paid for.
        if self.estimate is not None and float(self.estimate) > self.budget:
            raise Refusal("rig", f"this run is estimated at ${float(self.estimate):.2f}, over the "
                                 f"${self.budget:.2f} budget. Nothing was created.")
        self._environment(create=True)
        if not self.state.get("cluster"):
            c = self.cli_json("kafka", "cluster", "create", f"{self.prefix}-kafka", "--cloud", self.cloud,
                              "--region", self.region, "--type", "basic", "--max-ecku", str(self.max_ecku))
            self.state["cluster"] = c[F["id"]]
            self._save_state()
        cluster = self._wait(lambda: self.cli_json("kafka", "cluster", "describe", self.state["cluster"]),
                             f"Kafka cluster {self.state['cluster']}")
        if not self.state.get("pool"):
            p = self.cli_json("flink", "compute-pool", "create", f"{self.prefix}-flink", "--cloud", self.cloud,
                              "--region", self.region, "--max-cfu", str(self.setup_cfu))
            self.state["pool"] = p[F["id"]]
            self.state["pools"].append(p[F["id"]])
            self._save_state()
        self._wait(lambda: self.cli_json("flink", "compute-pool", "describe", self.state["pool"]),
                   f"Flink compute pool {self.state['pool']}")
        secrets = {}
        if not self.state.get("keys"):
            for label, extra in (("KAFKA", ["--resource", self.state["cluster"]]),
                                 ("FLINK", ["--resource", "flink", "--cloud", self.cloud, "--region", self.region]),
                                 ("METRICS", ["--resource", "cloud"])):
                k = self.cli_json("api-key", "create", *extra, "--description", f"{self.prefix} {label.lower()}",
                                  quiet=True)
                self.state["keys"].append(k[F["api_key"]])
                self._save_state()
                secrets[f"{label}_API_KEY"] = k[F["api_key"]]
                secrets[f"{label}_API_SECRET"] = k[F["api_secret"]]
            self._write_credentials(secrets, cluster)
        self._ready(cluster)
        self.log(f"  confluent-cloud up: cluster {self.state['cluster']}, pool {self.state['pool']}, "
                 f"{len(self.state['keys'])} API keys; credentials in {self.credentials}")
        return True

    ready_tries, ready_wait_s = 10, 60      # the self-test shortens these

    def _ready(self, cluster):
        """A new stack takes minutes before Confluent will run a statement in it.
        Measured 2026-10-03 in a new environment: the same CREATE TABLE failed
        0.7 min after up ("technical difficulties on our end") and completed at
        2.7 min. So up ends by running a tiny statement until one completes.

        A table that can be created is not yet a stack that works. Measured
        2026-10-04 (findings §8): two stacks ran their first CREATE TABLE in 5
        and 7 s, and a table created next stayed invisible to every INSERT for
        two and a half minutes; the ten stacks that took 102-108 s all worked.
        So ready means: a table created, a row written into it, and that row
        found on its Kafka topic. If that never happens, up tears the stack
        down and says so, before a fill is spent on it."""
        t0 = time.time()
        name_db = cluster.get(F["name"]) or self.state["cluster"]
        last = None
        for i in range(self.ready_tries):
            try:
                table = f"{self.prefix.replace('-', '_')}_ready{i}"
                self.run_statement(f"{self.prefix}-ready{i}", f"CREATE TABLE {table} (a INT)", name_db, timeout_s=240)
                self.state["readyAfterS"] = round(time.time() - t0, 1)
                self._save_state()
                self.log(f"  confluent-cloud: the stack ran its first statement {self.state['readyAfterS']:.0f} s "
                         f"after it was created")
                try:
                    self.run_statement(f"{self.prefix}-ready{i}-write", f"INSERT INTO {table} VALUES (1)", name_db,
                                       timeout_s=240)
                    end, _ = self.log_end(table)
                    if end < 1:
                        raise Refusal("rig", f"the row written to {table} is not on its Kafka topic (log end {end})")
                except Refusal as e:
                    self.log(f"  confluent-cloud: the stack is not usable: {e.msg}. Tearing it down.")
                    try:
                        self.down()
                    finally:
                        raise Refusal("rig", f"the new stack could create a table but not use it ({e.msg}). It was "
                                             f"torn down; run up again to get a new one")
                self.state["usableAfterS"] = round(time.time() - t0, 1)
                self._save_state()
                self.log(f"  confluent-cloud: a row written to a new table was read back from Kafka "
                         f"{self.state['usableAfterS']:.0f} s after the stack was created")
                return
            except Refusal as e:
                last = e.msg
                if "was torn down" in e.msg or (
                        "technical difficulties" not in e.msg and "not ready" not in e.msg.lower()):
                    raise
            time.sleep(self.ready_wait_s)
        raise Refusal("rig", f"the stack never ran a statement in {self.ready_tries} tries over "
                             f"{(time.time() - t0) / 60:.0f} min; the last answer was: {last}")

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
        """A new pool of exactly this size for the next case, and the last
        case's pool deleted. A pool cannot be made smaller, and a pool reused
        across cases would carry one case's scaling into the next."""
        units = int(units)
        if units not in POOL_SIZES:
            raise Refusal("rig", f"a Confluent Cloud compute pool cannot be {units} CFU: its size can only "
                                 f"be {', '.join(map(str, POOL_SIZES))}. Set cases to sizes from that list, "
                                 f"for example 5, 10 and 20")
        old = self.state.get("casePool")
        if old:
            self._clear_pool(old)
            self.cli("flink", "compute-pool", "delete", old, "--force")
            self.state["pools"] = [x for x in self.state["pools"] if x != old]
            self.state["casePool"] = None
            self._save_state()
        n = len(self.state.get("statements") or []) + len(self.state["pools"])
        p = self.cli_json("flink", "compute-pool", "create", f"{self.prefix}-case{units}-{n}", "--cloud",
                          self.cloud, "--region", self.region, "--max-cfu", str(units))
        self.state["casePool"] = p[F["id"]]
        self.state["pools"].append(p[F["id"]])
        self._save_state()
        self._wait(lambda: self.cli_json("flink", "compute-pool", "describe", p[F["id"]]),
                   f"Flink compute pool {p[F['id']]}")

    def case_pool(self):
        """The pool a case's job runs in: the case's own, else the setup pool."""
        return self.state.get("casePool") or self.state["pool"]

    def read_size(self):
        d = self.cli_json("flink", "compute-pool", "describe", self.case_pool()) or {}
        return int(d.get(F["max_cfu"], -1))

    def current_cfu(self):
        """What the pool is using now, as the CLI reports it. Measured
        2026-10-03: this read 0 while a job ran; use statement_minutes."""
        d = self.cli_json("flink", "compute-pool", "describe", self.case_pool()) or {}
        return int(d.get(F["current_cfu"], 0))

    # A statement's states, as its describe reports them. `--wait` returns once a
    # statement is running OR has failed, with exit code 0 either way: on
    # 2026-10-03 four statements failed and the probe that created them read
    # success. The status is read back, never the exit code.
    STATEMENT_OK = {"RUNNING", "COMPLETED"}
    STATEMENT_BAD = {"FAILED", "FAILING", "STOPPED", "DEGRADED", "DELETED", "GONE"}

    def statement_status(self, name):
        """(status, detail, the whole reply) for one statement, as Confluent
        describes it. A statement that no longer exists is "GONE", not an
        error: measured 2026-10-04, a probe that read the status of a
        statement deleted under it stopped and tore its stack down."""
        try:
            d = self.cli_json("flink", "statement", "describe", name, "--cloud", self.cloud,
                              "--region", self.region) or {}
        except Refusal as e:
            if "does not exist" in e.msg or "not found" in e.msg.lower():
                return "GONE", f"statement {name} no longer exists", {}
            raise
        status = str(d.get("status") or d.get("phase") or "").upper()
        detail = d.get("status_detail") or d.get("detail") or d.get("status_message") or ""
        return status, str(detail), d

    # A table just created is sometimes not visible to the next statement yet.
    # Seen 2026-10-03: an INSERT right after its CREATE TABLE failed with
    # "Cannot find table"; the same INSERT ran when retried 30 s later.
    LATE_TABLE = "Cannot find table"
    late_table_tries = 5

    def run_statement(self, name, sql, database, timeout_s=600, pool=None, properties=None):
        """Create a statement and wait until Confluent reports it running or
        completed. Returns its status; the name it ran under is in
        `last_statement` (a retry runs under a new name). A failure stops with
        Confluent's own detail; a reply this code does not recognise stops
        with the reply itself."""
        for attempt in range(1, self.late_table_tries + 1):
            nm = name if attempt == 1 else f"{name}-r{attempt}"
            try:
                status = self._run_statement(nm, sql, database, timeout_s, pool, properties)
                self.last_statement = nm
                return status
            except Refusal as e:
                if self.LATE_TABLE not in e.msg or attempt == self.late_table_tries:
                    raise
                self.log(f"  confluent-cloud: statement {nm} could not see a table created just before it; "
                         f"retrying in {self.retry_wait_s} s as {name}-r{attempt + 1}")
                self.cli("flink", "statement", "delete", nm, "--cloud", self.cloud, "--region", self.region,
                         "--force")
                time.sleep(self.retry_wait_s)

    def _run_statement(self, name, sql, database, timeout_s, pool, properties=None):
        if not name.startswith(self.prefix):
            raise Refusal("rig", f"statement {name!r} does not carry the run's prefix {self.prefix!r}, so "
                                 f"teardown would not find it")
        try:
            extra = []
            for k, v in (properties or {}).items():
                extra += ["--property", f"{k}={v}"]
            self.cli("flink", "statement", "create", name, "--sql", sql, "--compute-pool",
                     pool or self.case_pool(), "--database", database, "--cloud", self.cloud,
                     "--region", self.region, *extra)
        except Refusal as e:
            # Seen 2026-10-03 in a new environment: "already exists" for a name
            # never used before. Whatever made it, describe what is there.
            if "already exists" not in e.msg:
                raise
            self.log(f"  confluent-cloud: statement {name} already existed when created; reading its status")
        if name not in self.state["statements"]:
            self.state["statements"].append(name)
            self._save_state()
        t0 = time.time()
        while True:
            status, detail, whole = self.statement_status(name)
            if status in self.STATEMENT_OK:
                return status
            if status in self.STATEMENT_BAD:
                raise Refusal("rig", f"statement {name} did not run on Confluent Cloud ({status.lower()}): "
                                     f"{detail or json.dumps(whole)[:600]}")
            if time.time() - t0 > timeout_s:
                raise Refusal("rig", f"statement {name} was still {status or 'without a status'} after "
                                     f"{timeout_s} s: {json.dumps(whole)[:600]}")
            time.sleep(self.poll_s)

    def _statements(self, pool=None):
        pools = [pool] if pool else [x for x in dict.fromkeys([self.state.get("pool")] + self.state["pools"]) if x]
        listed = []
        for x in pools:
            rows = self.cli_json("flink", "statement", "list", "--compute-pool", x,
                                 "--cloud", self.cloud, "--region", self.region) or []
            listed += [r[F["name"]] for r in rows if str(r.get(F["name"], "")).startswith(self.prefix)]
        return sorted(set(listed))

    clear_tries = 6          # the self-test sets poll_s 0, so these cost nothing there

    def _clear_pool(self, pool=None):
        """Delete every statement the run owns in `pool` (every pool when None),
        one at a time, then list until none is left. Measured 2026-10-04: the
        list still named a statement deleted moments before; deleting the list
        in one command then stopped on "not found" and left a 20 CFU pool
        behind. A statement already gone counts as deleted."""
        left = self._statements(pool)
        for _ in range(self.clear_tries):
            for name in left:
                try:
                    self.cli("flink", "statement", "delete", name, "--cloud", self.cloud, "--region", self.region,
                             "--force")
                except Refusal as e:
                    if "not found" not in e.msg.lower():
                        raise
            left = self._statements(pool)
            if not left:
                return
            time.sleep(self.poll_s)
        raise Refusal("rig", f"statements still in the pool after deleting them {self.clear_tries} times: "
                             f"{', '.join(left)}")

    def clear_size(self):
        """No statement left in any pool the run created, confirmed by listing."""
        self._clear_pool()

    def surviving(self):
        """Everything this run's prefix names that still exists, as Confluent lists it."""
        out = []
        if not self._environment():
            return out              # no environment, so nothing in it
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
        if not self._environment():
            self.log(f"  confluent-cloud down: environment {self.environment_ref} does not exist, "
                     f"so nothing in it survives")
            return True
        try:
            self.clear_size()
        except Refusal as e:
            problems.append(e.msg)
        for kind, ids, cmd in (("API key", list(self.state.get("keys") or []), ["api-key", "delete"]),
                               ("pool", [x for x in dict.fromkeys([self.state.get("pool")] + self.state["pools"]) if x],
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
        if self.state.get("environmentCreated") and self.environment:
            self.cli("environment", "delete", self.environment, "--force")
            if any(e.get(F["id"]) == self.environment for e in self.cli_json("environment", "list") or []):
                raise Refusal("rig", f"environment {self.environment_ref} ({self.environment}) was emptied "
                                     f"but is still listed after deleting it")
            self.log(f"  confluent-cloud: deleted environment {self.environment_ref}, which up created")
        self.environment = None
        self.state = {"environment": None, "environmentCreated": False, "cluster": None, "pool": None,
                      "casePool": None, "pools": [], "keys": [], "statements": []}
        self._save_state()
        self.log("  confluent-cloud down: nothing with prefix " + self.prefix + " survives")
        return True

    # ------------------------------------------------------------ readings
    def _secret(self, name):
        """One value from the owner-only credentials file. Never logged."""
        for line in open(self.credentials):
            if line.startswith(name + "="):
                return line.rstrip("\n").split("=", 1)[1]
        raise Refusal("rig", f"{name} is not in the credentials file {self.credentials}; run up first")

    def kafka_tool(self, tool, *args):
        """One Kafka command-line tool against the cloud cluster, from the Kafka
        image, with an owner-only client file removed afterwards. Returns
        (exit code, stdout, stderr); stderr is never a secret."""
        import tempfile
        d = tempfile.mkdtemp(prefix="fsk-kafka-")
        client = os.path.join(d, "client.properties")
        try:
            fd = os.open(client, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write("security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\n"
                        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
                        f"username=\"{self._secret('KAFKA_API_KEY')}\" "
                        f"password=\"{self._secret('KAFKA_API_SECRET')}\";\n")
            return self.docker(["run", "--rm", "-v", f"{d}:/fsk:ro", self.kafka_image,
                                f"/opt/kafka/bin/{tool}", "--bootstrap-server", self._secret("KAFKA_BOOTSTRAP"),
                                "--command-config", "/fsk/client.properties", *args])
        finally:
            if os.path.exists(client):
                os.remove(client)
            os.rmdir(d)

    def log_end(self, topic):
        """The topic's log end on the cloud cluster: (total, {partition: offset})."""
        rc, out, err = self.kafka_tool("kafka-get-offsets.sh", "--topic", topic)
        if rc:
            raise Refusal("rig", f"could not read the log end of {topic} on Confluent Cloud: {err.strip()[-300:]}")
        total, per = parse_offsets(out, topic)
        if not per:
            raise Refusal("rig", f"the log end of {topic} came back empty: {out.strip()[:200]}")
        return total, per

    def statement_minutes(self, statement, t0, t1):
        """Confluent's per-minute readings for one statement between t0 and t1
        (epoch seconds): {reading: [(minute, value)]} for every name in
        STATEMENT_METRICS. They arrive about three minutes late."""
        import base64
        auth = base64.b64encode(f"{self._secret('METRICS_API_KEY')}:{self._secret('METRICS_API_SECRET')}"
                                .encode()).decode()
        iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
        out = {}
        for key, metric in STATEMENT_METRICS.items():
            body = {"aggregations": [{"metric": metric}], "group_by": ["resource.flink_statement.name"],
                    "filter": {"op": "AND", "filters": [
                        {"field": "resource.compute_pool.id", "op": "EQ", "value": self.case_pool()},
                        {"field": "resource.flink_statement.name", "op": "EQ", "value": statement}]},
                    "granularity": "PT1M", "intervals": [f"{iso(t0)}/{iso(t1)}"], "limit": 1000}
            status, reply = self.http(f"{METRICS_API}/query", auth, body)
            if status != 200:
                raise Refusal("rig", f"Confluent's metrics API answered HTTP {status} for {metric}: "
                                     f"{str(reply)[:300]}")
            out[key] = minutes_of(reply)
        return out

    # ------------------------------------------------------------ a case's job
    def _rest_auth(self):
        import base64
        return base64.b64encode(f"{self._secret('FLINK_API_KEY')}:{self._secret('FLINK_API_SECRET')}"
                                .encode()).decode()

    def _organization(self):
        if not self.state.get("organization"):
            orgs = self.cli_json("organization", "list") or []
            cur = [o for o in orgs if o.get("is_current")] or orgs[:1]
            if not cur:
                raise Refusal("rig", "the Confluent CLI lists no organization; log in with `confluent login`")
            self.state["organization"] = cur[0][F["id"]]
            self._save_state()
        return self.state["organization"]

    def _statement_url(self, name):
        return (f"https://flink.{self.region}.{self.cloud}.confluent.cloud/sql/v1/organizations/"
                f"{self._organization()}/environments/{self.environment}/statements/{name}")

    def set_baseline(self, name, units):
        """GUARD: the statement keeps at least `units` CFU. Without it the
        autoscaler decides how much of the pool to use: measured 2026-10-04, it
        stopped at 10 CFU of 20 and reported "OK" with 62 million records
        waiting (findings §8, run 06). The CLI has no flag for it, so the Flink
        REST API sets it, and the value is read back from the statement."""
        status, reply = self.rest("PATCH", self._statement_url(name), self._rest_auth(),
                                  [{"op": "add", "path": "/spec/scaling", "value": {"baseline_cfu": int(units)}}])
        status, reply = self.rest("GET", self._statement_url(name), self._rest_auth())
        got = ((reply.get("spec") or {}).get("scaling") or {}).get("baseline_cfu") if isinstance(reply, dict) else None
        if status != 200 or got != int(units):
            raise Refusal("rig", f"the baseline of {units} CFU did not apply to statement {name}: the statement "
                                 f"reports {got!r} (HTTP {status})")
        return got

    def start_job(self, name, sql, database, units, properties=None):
        """Start a case's job in the case's pool, with watermark alignment off
        and the baseline at the pool's size, both read back. Returns the name
        it runs under."""
        props = dict(ALIGNMENT_OFF, **(properties or {}))
        self.run_statement(name, sql, database, properties=props)
        name = self.last_statement
        _, _, whole = self.statement_status(name)
        got = whole.get("properties") or {}
        missing = {k: v for k, v in props.items() if str(got.get(k)) != str(v)}
        if missing:
            raise Refusal("rig", f"statement {name} does not report the settings it was given: "
                                 + ", ".join(f"{k} should be {v!r}, reads {got.get(k)!r}" for k, v in missing.items()))
        self.set_baseline(name, units)
        return name

    at_size_wait_s = 900      # a statement took 3-5 minutes to reach its pool's size (findings §8)

    def wait_at_size(self, name, units, t_started):
        """GUARD: the window opens only once the statement uses its whole pool,
        the cloud's "every slot in use". Confluent's per-minute CFU reading
        arrives about three minutes late, so this waits on it, and stops if
        the statement never gets there."""
        t0 = time.time()
        last = []
        while True:
            last = self.statement_minutes(name, t_started - 60, time.time())["cfu"]
            if last and last[-1][1] >= units:
                return last[-1][0]
            if time.time() - t0 > self.at_size_wait_s:
                seen = ", ".join(f"{m[11:16]} {v:g}" for m, v in last[-5:]) or "no reading"
                raise Refusal("rig", f"statement {name} never used its {units} CFU pool in "
                                     f"{self.at_size_wait_s // 60} minutes (CFU by minute: {seen}). Check that "
                                     f"its baseline reads {units}")
            time.sleep(self.poll_s * 6)

    def check_partitions(self, topic, cases):
        """GUARD: the input divides evenly across every case's subtasks, read
        back from Kafka, not from the configuration. The 24-partition topic
        the first cloud probes used does not divide by 5, 10 or 20."""
        _, per = self.log_end(topic)
        bad = uneven_cases(len(per), cases)
        if bad:
            step = 1
            for n in cases:
                step = step * n // __import__("math").gcd(step, n)
            raise Refusal("rig", f"topic {topic} has {len(per)} partitions, which do not divide evenly by "
                                 f"{', '.join(map(str, bad))} subtasks (1 CFU runs one subtask). Use a multiple of "
                                 f"{step}, for example {step}, {2 * step} or {3 * step}")
        return len(per)

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
