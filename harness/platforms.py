"""Where the stack runs. `local` is the laptop and is what every recorded run
used: Docker containers, a CPU cap on the worker container, the host's disk,
memory and swap. Everything here exists so a managed service can be measured by
the same chain without pretending it is a laptop.

A platform supplies the operations below. The harness asks for them instead of
calling Docker directly, and on `local` each one is the code that ran before
this file existed. The measurement itself -- committed offsets, the window, the
guards on the numbers, completeness against the generator's record -- does not
change with the platform: every service here speaks the Kafka protocol.

What does change is which checks mean anything. A laptop check on a managed
service is reported as "not checked on <platform>" and never silently passed:
a row that says PASS about a machine the run does not use is the kind of
wrong answer this harness exists to prevent.
"""


class Refusal(Exception):
    """Same shape as lib.Refusal; lib re-raises it as its own."""
    def __init__(self, scope, msg):
        super().__init__(msg)
        self.scope, self.msg = scope, msg


class Platform:
    """The contract. `unit` is what one step of size is called on this platform
    (a core, a CFU, a KPU) -- the thing bought and the thing capped together.
    Credentials are read from the file `platform.credentials` names, outside
    every repository, and never printed."""
    kind = "?"
    unit = "unit"

    def up(self):                   # provision the broker side and the engine, ready for a job
        raise NotImplementedError
    def down(self):                 # tear everything down, on every exit path: idle clusters cost money
        raise NotImplementedError
    def set_size(self, units):      # the engine's size and parallelism for the next case
        raise NotImplementedError
    def read_size(self):            # the size the platform itself reports, never the one asked for
        raise NotImplementedError
    def clear_size(self):           # no job running and nothing allocated, confirmed by the platform
        raise NotImplementedError
    def cpu_stat(self, component):  # "engine" or "broker": cumulative counters shaped like cgroup
        raise NotImplementedError    #   cpu.stat -- usage_usec, nr_periods, nr_throttled -- in the
                                     #   platform's own units; never zeros standing in for a reading
    def mem_stat(self, component):  # "broker": limitHits, refaults, fileCache, limitBytes, or what
        raise NotImplementedError    #   the service reports in their place
    def submit(self, par, group, ckpt_ms=None):   # start the job; returns the platform's id for it
        raise NotImplementedError
    def surviving(self):            # anything this run created that still exists
        raise NotImplementedError


# What each managed service still needs before it can run. Named here so a
# config that asks for one stops with this sentence instead of a traceback, and
# so the plan and the code say the same thing.
NOT_BUILT = {
    "aws": ("Amazon MSK with Managed Service for Apache Flink. It needs an AWS account and region, "
            "an MSK cluster, and applications sized by parallelism and parallelism per KPU with "
            "autoscaling turned off"),
    "gcp": ("Google Cloud: Managed Service for Apache Kafka, with Flink either on Google's managed "
            "Flink service or on GKE with the Flink Kubernetes Operator. Which of those exists and "
            "is supported has not been checked yet"),
}

_BUILT = {}          # kind -> Platform subclass. `local` is handled by lib itself.


def register(kind, cls):
    """Make a platform runnable. The self-tests register a fake one."""
    _BUILT[kind] = cls


def unregister(kind):
    _BUILT.pop(kind, None)


def platform_kind(raw):
    """`platform` in pipeline.json: a string, or an object with `kind`. Default local."""
    p = raw.get("platform", "local")
    kind = str(p.get("kind") if isinstance(p, dict) else p).strip().lower()
    known = ["local"] + sorted(NOT_BUILT) + sorted(k for k in _BUILT if k not in NOT_BUILT)
    if kind not in known:
        raise Refusal("rig", f"pipeline.json platform must be one of {', '.join(known)}, got {p!r}")
    return kind


def platform_for(kind, raw=None):
    """The platform object, or None for local. A named platform that is not
    built yet stops the run here, before anything is provisioned or paid for."""
    if kind == "local":
        return None
    if kind in _BUILT:
        return _BUILT[kind](raw or {})
    raise Refusal("rig", f"platform {kind!r} is named in pipeline.json, but the harness cannot run "
                         f"on it yet. {NOT_BUILT[kind]}.")


def set_and_read_back(p, units):
    """GUARD: the size is read back from the platform, never taken from the request."""
    p.set_size(units)
    return read_back(p, units)


def read_back(p, units):
    """GUARD: the size the platform reports for this case, compared with the case."""
    got = p.read_size()
    if got != units:
        raise Refusal("rig", f"the {p.unit} size did not apply on {p.kind}: the case is {units}, "
                             f"the platform reports {got}")
    return got


# Preflight rows that describe the laptop and nothing else, and why. On any
# other platform each is reported as not checked, with this reason.
LAPTOP_ONLY = {
    "every image is native to the host arch": "the service runs its own hardware",
    "the engine can write its state directory": "the service owns the state directory",
    "the metrics reporter is not duplicated": "the service owns the engine's plugins",
    "disk budget on the HOST, not the container": "the broker's disk is the service's",
    "CPU cap mechanism chosen once": "the size is the service's unit, read back every case",
    "memory is per subtask, not per container": "the service sizes memory with its unit",
    "nothing else is using the cores (reported)": "the cores measured are not this machine's",
    "what this host's own cores do": "the cores measured are not this machine's",
    "the Docker VM against the machine's memory (reported)": "there is no Docker VM in the stack",
    "pipeline, broker and job manager against the VM (reported)": "there is no Docker VM in the stack",
    "the VM trim command is known": "there is no Docker VM in the stack",
}


def not_checked(kind, row):
    """Why a preflight row does not apply on this platform, or None when it does."""
    if kind == "local" or row not in LAPTOP_ONLY:
        return None
    return f"not checked on {kind}: {LAPTOP_ONLY[row]}"


# ------------------------------------------------------------------ one app, two platforms
# The same SQL INSERT runs on the laptop and on a managed service; only the
# tables around it differ, because each platform connects a table to Kafka its
# own way. The harness writes the tables, the app writes the INSERT.

def table_ddl(kind, name, columns, partitions, key=None, bootstrap=None):
    """CREATE TABLE for one Kafka-backed table on `kind`. `columns` is a list of
    (name, SQL type). Pure: the same name and columns on every platform, so an
    INSERT that reads and writes these tables runs unchanged on each."""
    cols = ", ".join(f"{c} {t}" for c, t in columns)
    if kind == "confluent-cloud":
        # Confluent Cloud makes the topic from the table: the partition count is
        # DISTRIBUTED INTO, and the key, if any, is what the rows are hashed on.
        dist = f"DISTRIBUTED BY ({', '.join(key)}) " if key else "DISTRIBUTED "
        return f"CREATE TABLE {name} ({cols}) {dist}INTO {int(partitions)} BUCKETS"
    if kind == "local":
        if not bootstrap:
            raise Refusal("rig", f"table {name}: the laptop's tables need the broker's address")
        opts = {"connector": "kafka", "topic": name, "properties.bootstrap.servers": bootstrap,
                "format": "json", "scan.startup.mode": "earliest-offset"}
        if key:
            opts["key.format"] = "json"
            opts["key.fields"] = ";".join(key)
        with_ = ", ".join(f"'{k}' = '{v}'" for k, v in opts.items())
        return f"CREATE TABLE {name} ({cols}) WITH ({with_})"
    raise Refusal("rig", f"no table definition for platform {kind!r} yet")


def _sql_words(sql):
    return " ".join(str(sql or "").replace(";", " ").split()).lower()


def local_first_reason(local_pipeline, cloud_sql, read_json=None):
    """Why a cloud run may not start yet, or None. A run on a paid service
    starts only after the same app passed on the laptop: completeness and the
    tiny proof, and the same job SQL. The laptop's SQL is the `job.sql` its
    pipeline.json declares -- declared, not read back from the jar."""
    import json as _json
    import os as _os
    read = read_json or (lambda p: _json.load(open(p)))
    if not local_pipeline:
        return ("platform.localPipeline is not set: name the laptop run's pipeline.json. A run on a paid "
                "service starts only after the same app passed completeness and the tiny proof locally")
    try:
        local = read(local_pipeline)
    except (OSError, ValueError) as e:
        return f"the laptop run's pipeline.json could not be read ({local_pipeline}): {e}"
    results = _os.path.join(_os.path.dirname(local_pipeline), local.get("results", "results"))
    for name, what in (("completeness.json", "completeness"), ("tinyproof.json", "the tiny proof")):
        try:
            got = read(_os.path.join(results, name)).get("result")
        except (OSError, ValueError):
            got = None
        if got != "PASS":
            return (f"{what} has not passed on the laptop for this app ({_os.path.join(results, name)}: "
                    f"{got or 'not found'}). Run it locally first; it costs nothing there")
    local_sql = (local.get("job") or {}).get("sql")
    if not local_sql:
        return ("the laptop run's pipeline.json declares no job.sql, so there is no telling whether the cloud "
                "would run the same app. Add the INSERT the laptop's job runs as job.sql")
    if _sql_words(local_sql) != _sql_words(cloud_sql):
        return ("the cloud's platform.jobSql is not the INSERT the laptop's job declares (job.sql). The same "
                "app must run on both; only the tables around it differ")
    return None
