"""The dashboard the harness builds for any pipeline (SKILL.md section 7).

Every run used to build its own dashboard by hand, from prose, and each one
answered the same questions differently or not at all: no rate per topic, no
lag on the outputs, nothing about the broker's disk or network. This writes
the same rows, in the same order, for every pipeline, from nothing but
pipeline.json -- its input topics, its output topics and its key sets -- so no
name from any one pipeline, the shipped example included, ever reaches it.

The rows, in order (the order is the pattern, and `check` holds it):

    1. Input          records written (the generator) and read (the pipeline)
                      per input topic, the total on each, the keys generated
    2. Input lag      records waiting on each input topic, per consumer group
    3. Output         records written per output topic, the total on each, the
                      distinct keys on each, and the lag of every reader
    4. Checkpoints    completed, how long they take, their size, failures,
                      restarts
    5. Broker         memory, CPU, network, disk per topic
    6. Flink          CPU, memory, heap, garbage collection, busy and
                      back-pressured time per task, slots and threads
    7. Redis store    only with "dashboard": {"store": "redis"}

Consumer lag is required on every topic, input and output. Lag is read from
the broker -- every group's committed position against each topic's end -- so
inputs and outputs are measured the same way. An output has no consumer of its
own on a measuring rig, so the Kafka metrics service runs one reader over the
outputs, and its lag is the output's lag.

Pure: nothing here touches Docker, the network or the disk, except
`write_files`, which only writes the files it is given.
"""
import json
import os
import re

ROWS = (
    ("input", "1. Input"),
    ("input_lag", "2. Input lag"),
    ("output", "3. Output"),
    ("checkpoints", "4. Checkpoints"),
    ("broker", "5. Broker"),
    ("flink", "6. Flink"),
    ("store", "7. Redis store"),
)
ROW_TITLES = dict(ROWS)
DS_UID = "harnessprom"
DEFAULT_PORTS = {"grafana": 13000, "prometheus": 19090, "redisinsight": 15540}
IMAGES = {
    "grafana": "grafana/grafana:11.2.0",
    "prometheus": "prom/prometheus:v2.53.2",
    "exporter": "python:3.12-alpine",
    "redis": "redis:7.4.2",
    "redisinsight": "redis/redisinsight:2.70.0",
}
# Variants of the scaled input the harness drains under its own names: the
# tiny proof's topic and the completeness backlog's.
INPUT_VARIANTS = ("", "-tiny", "-small")
MANIFESTS = ("manifest.json", "manifest-small.json", "manifest-tiny.json")


class BoardError(Exception):
    """pipeline.json asks for a dashboard this cannot build; the message says why."""


def settings(raw):
    """Pure. What pipeline.json asks for, or None when the harness builds no
    dashboard: `"dashboard": false`, or a Grafana of the run's own already in
    extraServices with no `dashboard` key (every run before this existed)."""
    d = raw.get("dashboard")
    if d is False:
        return None
    own = any("grafana" in str((b or {}).get("image", "")).split(":")[0]
              for b in (raw.get("extraServices") or {}).values())
    if d is None and own:
        return None
    if own:
        raise BoardError("pipeline.json asks for the harness's dashboard and also has a Grafana of its own in "
                         "extraServices. Keep one: remove the Grafana from extraServices, or set "
                         '"dashboard": false to keep yours')
    d = dict(d or {}) if d is not True else {}
    store = d.get("store")
    if store not in (None, "redis"):
        raise BoardError(f'dashboard.store is {store!r}; the one store the harness can add is "redis"')
    panels = d.get("panels") or {}
    for row in panels:
        if row not in ROW_TITLES:
            raise BoardError(f"dashboard.panels names a row {row!r} that does not exist. The rows are: "
                             + ", ".join(k for k, _ in ROWS))
    if panels.get("store") and not store:
        raise BoardError('dashboard.panels adds to the Redis store row, and the store is not on '
                         '(dashboard.store = "redis")')
    ports = dict(DEFAULT_PORTS)
    ports.update({k: int(v) for k, v in (d.get("ports") or {}).items()})
    return {"ports": ports, "store": store, "panels": panels}


def topic_sets(raw):
    """Pure. (scaled input, other inputs, outputs). Outputs are topics.out and
    topicsAlsoWritten together: lag is required on every topic the job writes,
    not only the ones the harness measures."""
    t = raw["topics"]
    scaled = t["in"]
    others = [x for x in ((raw.get("design") or {}).get("inputs") or []) if x != scaled]
    outs = []
    for x in list(t.get("out") or []) + list(raw.get("topicsAlsoWritten") or []):
        if x not in outs:
            outs.append(x)
    return scaled, others, outs


def _re_escape(name):
    """A topic name as a literal inside a PromQL regular expression, written
    inside a PromQL string, where a backslash is itself escaped."""
    return re.sub(r"([.+*?()\[\]{}|^$\\])", r"\\\\\1", name)


def topic_regex(names):
    """Pure. One PromQL regular expression matching exactly these topics."""
    return "|".join(_re_escape(n) for n in names) or "^$"


def input_regex(raw):
    scaled, others, _ = topic_sets(raw)
    return topic_regex([scaled + v for v in INPUT_VARIANTS] + others)


def input_topics(raw):
    scaled, others, _ = topic_sets(raw)
    return [scaled + v for v in INPUT_VARIANTS] + others


def _matches(regex, topic):
    """Pure. Whether a PromQL label regex (fully anchored, as Prometheus
    anchors it) matches a topic name."""
    return re.fullmatch(regex.replace("\\\\", "\\"), topic) is not None


class _Layout:
    def __init__(self):
        self.panels, self.y, self.x, self.h, self.ids = [], 0, 0, 0, 0

    def nid(self):
        self.ids += 1
        return self.ids

    def row(self, key):
        self.newline()
        self.panels.append({"type": "row", "id": self.nid(), "title": ROW_TITLES[key], "collapsed": False,
                            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y}, "panels": [], "_row": key})
        self.y += 1

    def newline(self):
        if self.x:
            self.y += self.h
            self.x, self.h = 0, 0

    def place(self, panel, row, w, h):
        if self.x + w > 24:
            self.newline()
        panel["gridPos"] = {"h": h, "w": w, "x": self.x, "y": self.y}
        panel["id"] = self.nid()
        panel["_row"] = row
        panel.setdefault("datasource", {"type": "prometheus", "uid": DS_UID})
        for t in panel.get("targets") or []:
            t.setdefault("datasource", {"type": "prometheus", "uid": DS_UID})
        self.panels.append(panel)
        self.x += w
        self.h = max(self.h, h)


def _ts(title, desc, targets, unit="short", maxv=None, thresh=None, stack=False):
    d = {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never",
                                  "spanNulls": False, "axisSoftMin": 0}}
    if stack:
        d["custom"]["stacking"] = {"mode": "normal"}
    if maxv is not None:
        d["max"] = maxv
    if thresh is not None:
        d["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                          {"color": "red", "value": thresh}]}
        d["custom"]["thresholdsStyle"] = {"mode": "line"}
    return {"type": "timeseries", "title": title, "description": desc,
            "fieldConfig": {"defaults": d, "overrides": []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
            "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l} for i, (l, e) in enumerate(targets)]}


def _stat(title, desc, targets, unit="short", red_above=None):
    steps = [{"color": "green", "value": None}]
    if red_above is not None:
        steps.append({"color": "red", "value": red_above})
    return {"type": "stat", "title": title, "description": desc,
            "fieldConfig": {"defaults": {"unit": unit, "decimals": 0,
                                         "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "none", "graphMode": "none",
                        "textMode": "value_and_name", "justifyMode": "center"},
            "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "instant": False}
                        for i, (l, e) in enumerate(targets)]}


def board(raw, project, gc_ceiling=0.055, store=None, extra_panels=None):
    """Pure. The dashboard, as Grafana's JSON, for this pipeline."""
    _, _, outs = topic_sets(raw)
    IN = input_regex(raw)
    OUT = topic_regex(outs)
    p = re.escape(project)
    KAFKA = f'name="{project}-kafka"'
    FLINK = f'name=~"{p}-(jm|tm)"'
    extra_panels = extra_panels or {}
    L = _Layout()

    def extras(row):
        for panel in extra_panels.get(row) or []:
            panel = json.loads(json.dumps(panel))
            L.place(panel, row, int((panel.get("gridPos") or {}).get("w", 12)),
                    int((panel.get("gridPos") or {}).get("h", 8)))

    L.row("input")
    L.place(_ts("Records written per second, by input topic — the generator, while it fills",
                "How fast records are arriving on each input topic, read from the broker: the rise in the sum of "
                "the topic's end offsets. On a measuring rig this is the generator filling a backlog; on a "
                "pipeline left running it is the load being offered.",
                [("{{topic}}", f'sum by (topic) (rate(kafka_topic_records_written_total{{topic=~"{IN}"}}[1m]))')]),
            "input", 12, 8)
    L.place(_ts("Records read per second, by input topic — flat during a case",
                "How fast the pipeline is reading each input topic: the rise in its consumer group's committed "
                "position. It moves once per checkpoint, when the position is committed, so read it over a "
                "minute, not a second.",
                [("{{topic}} — {{group}}", f'sum by (topic, group) (rate(kafka_group_records_committed_total{{topic=~"{IN}"}}[1m]))')]),
            "input", 12, 8)
    L.place(_stat("Records on each input topic — the backlog",
                  "Records written to each input topic, from its end offsets.",
                  [("{{topic}}", f'sum by (topic) (kafka_topic_records_written_total{{topic=~"{IN}"}})')]),
            "input", 12, 5)
    if raw.get("keySets"):
        L.place(_stat("Distinct keys the generator wrote, by backlog and key set — the interview's answer",
                      "Counted from each backlog's manifest, so it is what the generator wrote, not what the job "
                      "saw. The distinct keys on each output are in the output row.",
                      [("{{manifest}} {{key_set}}", "max by (manifest, key_set) (pipeline_generator_keys)")]),
                "input", 12, 5)
    extras("input")

    L.row("input_lag")
    L.place(_ts("Records waiting, by input topic and consumer group — falls steadily in each case",
                "Records written to the topic that the group has not committed past: the end offsets less the "
                "group's committed position, from the broker. In a measured drain it falls steadily; flat at "
                "zero means the case ran out of input; climbing on a pipeline left running means it is not "
                "keeping up. A case's group is deleted when the case ends, so each case is its own line.",
                [("{{topic}} — {{group}}", f'sum by (topic, group) (kafka_group_lag_records{{topic=~"{IN}"}})')]),
            "input_lag", 24, 8)
    extras("input_lag")

    L.row("output")
    if outs:
        L.place(_ts("Records written per second, by output topic",
                    "How fast the pipeline writes each output, from the broker's end offsets. An output written "
                    "once per input record moves with the input; one written per window or per interval moves "
                    "with the clock. On an exactly-once output the offsets include one transaction marker per "
                    "partition per checkpoint, so this reads very slightly high.",
                    [("{{topic}}", f'sum by (topic) (rate(kafka_topic_records_written_total{{topic=~"{OUT}"}}[1m]))')]),
                "output", 12, 8)
        L.place(_ts("Records waiting, by output topic and reader — near zero",
                    "Lag on every output, for every group reading it: the dashboard's own reader always, and the "
                    "Redis store's loader when it is on. A reader that falls behind shows here and never slows "
                    "the pipeline down.",
                    [("{{topic}} — {{group}}", f'sum by (topic, group) (kafka_group_lag_records{{topic=~"{OUT}"}})')]),
                "output", 12, 8)
        L.place(_stat("Records on each output topic",
                      "Records written to each output, from its end offsets (transaction markers included).",
                      [("{{topic}}", f'sum by (topic) (kafka_topic_records_written_total{{topic=~"{OUT}"}})')]),
                "output", 12, 5)
        L.place(_stat("Distinct keys on each output topic — the interview's answer",
                      "Counted by the dashboard's reader from the records it read, committed records only, since "
                      "the topic was last created. Counting stops at two million keys a topic, and the panel then "
                      "shows that number.",
                      [("{{topic}}", f'max by (topic) (kafka_reader_unique_keys{{topic=~"{OUT}"}})')]),
                "output", 12, 5)
    extras("output")

    L.row("checkpoints")
    L.place(_ts("Checkpoints completed per minute — steady",
                "Checkpoints that completed in each minute. Each one commits the input position and the outputs, "
                "so a gap here is a gap in what downstream readers see.",
                [("completed", "sum(increase(flink_jobmanager_job_numberOfCompletedCheckpoints[1m]))")]),
            "checkpoints", 8, 8)
    L.place(_ts("Checkpoint duration — flat",
                "How long the last checkpoint took. Rising at a steady rate means the state is getting harder to "
                "save.", [("last checkpoint", "max(clamp_min(flink_jobmanager_job_lastCheckpointDuration, 0))")],
                unit="ms"), "checkpoints", 8, 8)
    L.place(_ts("Checkpoint size — levels off",
                "The size of the last checkpoint. On a pipeline left running it should level off; a line that "
                "keeps climbing is keys that never expire.",
                [("last checkpoint", "max(flink_jobmanager_job_lastCheckpointSize)")], unit="bytes"),
            "checkpoints", 8, 8)
    L.place(_stat("Failed checkpoints — 0", "Checkpoints that did not complete. While they fail, a crash would "
                  "replay from further back.", [("failed", "max(flink_jobmanager_job_numberOfFailedCheckpoints)")],
                  red_above=1), "checkpoints", 12, 4)
    L.place(_stat("Job restarts — 0", "Times the job failed and recovered. Above zero outside a planned kill means "
                  "readings were taken across a failure.", [("restarts", "max(flink_jobmanager_job_numRestarts)")],
                  red_above=1), "checkpoints", 12, 4)
    extras("checkpoints")

    L.row("broker")
    L.place(_ts("Broker memory and its limit — filling to the limit is normal",
                "The broker container's memory, page cache included, against its limit. The cache grows until it "
                "meets the limit, so touching the line is normal; the harness's own broker-memory check says "
                "whether reads were pushed to disk.",
                [("in use", f"max(docker_container_memory_bytes{{{KAFKA}}})"),
                 ("limit", f"max(docker_container_memory_limit_bytes{{{KAFKA}}})")], unit="bytes"),
            "broker", 12, 8)
    L.place(_ts("Broker CPU, cores — under its cap",
                "Cores the broker container is using. A broker pinned at its cap while the pipeline falls off "
                "its own is the broker being the constraint.",
                [("broker", f"sum(rate(docker_container_cpu_seconds_total{{{KAFKA}}}[1m]))")]),
            "broker", 12, 8)
    L.place(_ts("Broker network, bytes per second — in and out",
                "Bytes into the broker (records produced) and out of it (records read by every consumer, the "
                "dashboard's reader included).",
                [("in", f"sum(rate(docker_container_network_receive_bytes_total{{{KAFKA}}}[1m]))"),
                 ("out", f"sum(rate(docker_container_network_transmit_bytes_total{{{KAFKA}}}[1m]))")],
                unit="Bps"), "broker", 12, 8)
    L.place(_ts("Broker disk, by topic — the backlog and the outputs",
                "Bytes each topic holds on the broker's disk, as the broker reports its log directories. "
                "Retention is what keeps the outputs from growing without limit.",
                [("{{topic}}", "sum by (topic) (kafka_topic_log_bytes)")], unit="bytes", stack=True),
            "broker", 12, 8)
    extras("broker")

    L.row("flink")
    L.place(_ts("Flink CPU, cores, by container — the worker at its cap",
                "Cores each Flink container is using. A measured case gives the worker exactly its cores, so it "
                "should sit at its cap; below it, something else is the constraint.",
                [("{{name}}", f"sum by (name) (rate(docker_container_cpu_seconds_total{{{FLINK}}}[1m]))")]),
            "flink", 12, 8)
    L.place(_ts("Flink memory, by container — under its limit",
                "Each Flink container's memory against its limit.",
                [("{{name}} in use", f"max by (name) (docker_container_memory_bytes{{{FLINK}}})"),
                 ("{{name}} limit", f"max by (name) (docker_container_memory_limit_bytes{{{FLINK}}})")],
                unit="bytes"), "flink", 12, 8)
    L.place(_ts("Worker heap in use, share of the maximum — a saw-tooth under 100%",
                "Heap in use against the most the worker may use. A floor that keeps rising between collections "
                "is state the heap cannot hold.",
                [("heap", "sum(flink_taskmanager_Status_JVM_Memory_Heap_Used) / "
                          "sum(flink_taskmanager_Status_JVM_Memory_Heap_Max)")], unit="percentunit", maxv=1),
            "flink", 12, 8)
    L.place(_ts(f"Garbage collection, share of the cores — under {gc_ceiling:.1%}",
                f"Time spent collecting garbage, as a share of the cores the worker has. The harness calls a case "
                f"over {gc_ceiling:.1%} a ceiling; the red line is that limit.",
                [("garbage collection", "sum(flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond) "
                                        "/ 1000 / scalar(max(flink_jobmanager_taskSlotsTotal))")],
                unit="percentunit", thresh=gc_ceiling), "flink", 12, 8)
    L.place(_ts("Busy time, by task — the busiest near 100%",
                "The share of each second each task spent working. The busiest task is the one that sets the "
                "rate.", [("{{task_name}}", "max by (task_name) (flink_taskmanager_job_task_busyTimeMsPerSecond) / 1000")],
                unit="percentunit", maxv=1), "flink", 12, 8)
    L.place(_ts("Back-pressured time, by task — what waits on the busiest",
                "The share of each second each task spent waiting for the task after it. The task after the "
                "last back-pressured one is the constraint.",
                [("{{task_name}}", "max by (task_name) (flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 1000")],
                unit="percentunit", maxv=1), "flink", 12, 8)
    L.place(_stat("Free task slots", "Slots no job is using. During a measured case this is 0 on purpose; a "
                  "pipeline left running needs spares to restart or rescale.",
                  [("free", "max(flink_jobmanager_taskSlotsAvailable)")]), "flink", 12, 4)
    L.place(_stat("Worker threads — not climbing", "Threads in the worker. A slow climb over hours is a leak.",
                  [("threads", "max(flink_taskmanager_Status_JVM_Threads_Count)")]), "flink", 12, 4)
    extras("flink")

    if store:
        L.row("store")
        L.place(_ts("Store loader, records read and keys written per second, by topic — keys well under records",
                    "Records the loader read from each output, against keys it wrote to Redis after cutting each "
                    "batch to the last record per key. Its lag is in the output row, with every other reader's.",
                    [("{{topic}} read", "sum by (topic) (rate(store_records_read_total[1m]))"),
                     ("{{topic}} written", "sum by (topic) (rate(store_keys_written_total[1m]))")]),
                "store", 12, 8)
        L.place(_ts("Redis memory — under its limit",
                    "Memory Redis reports in use.",
                    [("used", "max(store_redis_used_memory_bytes)")], unit="bytes"), "store", 12, 8)
        L.place(_stat("Keys in Redis, by topic — equal to the output's distinct keys",
                      "Keys the store holds for each output. It should equal the distinct keys the output row "
                      "counts once the loader has caught up.", [("{{topic}}", "max by (topic) (store_keys)")]),
                "store", 12, 5)
        L.place(_stat("Times Redis did not answer — 0",
                      "Batches Redis did not take. Each is read again once Redis is back, so nothing is lost; "
                      "the loader's lag shows the delay.", [("errors", "max(store_redis_errors_total)")],
                      red_above=1), "store", 12, 5)
        extras("store")
    L.newline()
    return {"title": f"{project} — input, lag, output, checkpoints, broker, Flink",
            "uid": f"{project}-harness", "timezone": "browser", "schemaVersion": 39,
            "time": {"from": "now-90m", "to": "now"}, "refresh": "10s",
            "tags": ["scalable-flink-skill"], "panels": L.panels, "editable": True}


def check(b, raw, example_names=()):
    """Pure. Every way the dashboard breaks the pattern, one sentence each:
    rows out of order, a topic with no lag panel, a panel with no query, or
    a name from the shipped example that is not one of this pipeline's own."""
    problems = []
    panels = b.get("panels") or []
    rows = [p.get("_row") for p in panels if p.get("type") == "row"]
    order = [k for k, _ in ROWS]
    if [r for r in order if r in rows] != rows:
        problems.append("the rows are out of order: " + ", ".join(ROW_TITLES.get(r, str(r)) for r in rows)
                        + ". The order is " + ", ".join(t for _, t in ROWS))
    for need in ("input", "input_lag", "output", "checkpoints", "broker", "flink"):
        if need not in rows:
            problems.append(f"the {ROW_TITLES[need]} row is missing")
    lag = []
    for p in panels:
        for t in p.get("targets") or []:
            m = re.search(r'kafka_group_lag_records\{topic=~"([^"]*)"\}', t.get("expr", ""))
            if m:
                lag.append((p.get("_row"), m.group(1)))
    scaled, others, outs = topic_sets(raw)
    for topic in [scaled] + others + outs:
        want = "input_lag" if topic in [scaled] + others else "output"
        if not any(row == want and _matches(rx, topic) for row, rx in lag):
            problems.append(f"no lag panel in the {ROW_TITLES[want]} row covers the topic {topic}. Consumer lag "
                            "is required on every topic")
    own = set([scaled] + others + outs + list((raw.get("design") or {}).get("operators") or []))
    for p in panels:
        if p.get("type") == "row":
            continue
        if not [t for t in p.get("targets") or [] if t.get("expr")]:
            problems.append(f"the panel \"{p.get('title')}\" has no query")
        text = json.dumps({k: p.get(k) for k in ("title", "description", "targets")})
        for name in example_names:
            if name not in own and re.search(r"(?<![\w-])" + re.escape(name) + r"(?![\w-])", text):
                problems.append(f"the panel \"{p.get('title')}\" names {name!r}, which belongs to the shipped "
                                "example and not to this pipeline")
    return problems


def example_names(example_raw):
    """Pure. The topic and operator names the shipped example uses."""
    scaled, others, outs = topic_sets(example_raw)
    return sorted(set([scaled] + others + outs + list((example_raw.get("design") or {}).get("operators") or [])))


def prometheus_yml(project, store):
    jobs = [("flink-jobmanager", f"{project}-jm:9249"), ("flink-taskmanager", f"{project}-tm:9249"),
            ("containers", f"{project}-statsexporter:9110"), ("kafka", f"{project}-kafkametrics:9500")]
    if store:
        jobs.append(("store", f"{project}-storeloader:9400"))
    out = ["# Written by the harness from pipeline.json (harness/board.py). Edits are overwritten at `up`.",
           "global:", "  scrape_interval: 5s", "  scrape_timeout: 4s", "  evaluation_interval: 30s", "",
           "scrape_configs:"]
    for name, target in jobs:
        out += [f"  - job_name: {name}", "    static_configs:", f'      - targets: ["{target}"]']
    return "\n".join(out) + "\n"


def datasource_yml(project):
    return "\n".join([
        "apiVersion: 1", "datasources:", "  - name: Prometheus", f"    uid: {DS_UID}", "    type: prometheus",
        "    access: proxy", f"    url: http://{project}-prometheus:9090", "    isDefault: true", "    jsonData:",
        "      timeInterval: 5s", ""])


def provider_yml():
    return "\n".join([
        "apiVersion: 1", "providers:", "  - name: harness", "    orgId: 1", "    folder: ''", "    type: file",
        "    disableDeletion: false", "    allowUiUpdates: true", "    options:",
        "      path: /var/lib/grafana/dashboards", ""])


def services(raw, project, s, dash_dir, km_dir, exporter_dir, kafka_img, kafka_libs, boot_int):
    """Pure. The compose services the dashboard needs, each named with the
    project prefix and each with a CPU cap: anything sharing the cores under
    test changes the number being measured."""
    _, _, outs = topic_sets(raw)
    ports = s["ports"]
    java = lambda heap, cls: ["java", f"-Xmx{heap}", "-cp", f"{kafka_libs}/*:/km", cls]
    out = {
        "statsexporter": {
            "image": IMAGES["exporter"], "container_name": f"{project}-statsexporter",
            "hostname": f"{project}-statsexporter", "cpus": 0.2, "mem_limit": "64m",
            "environment": {"PREFIX": f"{project}-", "INTERVAL_S": "5"},
            "command": ["python3", "/app/docker_cpu_exporter.py"],
            "volumes": [f"{exporter_dir}:/app:ro", "/var/run/docker.sock:/var/run/docker.sock:ro"]},
        "kafkametrics": {
            "image": kafka_img, "container_name": f"{project}-kafkametrics", "hostname": f"{project}-kafkametrics",
            "cpus": 0.5, "mem_limit": "384m", "depends_on": ["kafka"],
            "entrypoint": java("256m", "KafkaMetrics"),
            "environment": {"BOOTSTRAP": boot_int, "READ_TOPICS": ",".join(outs),
                            "READER_GROUP": f"{project}-dashboard", "EXTRA_FILE": "/dash/extra.prom"},
            "volumes": [f"{km_dir}:/km:ro", f"{dash_dir}:/dash:ro"]},
        "prometheus": {
            "image": IMAGES["prometheus"], "container_name": f"{project}-prometheus",
            "hostname": f"{project}-prometheus", "cpus": 0.3, "mem_limit": "256m",
            "ports": [f"{ports['prometheus']}:9090"],
            "command": ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus",
                        "--storage.tsdb.retention.time=30d", "--web.enable-lifecycle"],
            "volumes": [f"{dash_dir}/prometheus:/etc/prometheus:ro"]},
        "grafana": {
            "image": IMAGES["grafana"], "container_name": f"{project}-grafana", "hostname": f"{project}-grafana",
            "cpus": 0.2, "mem_limit": "256m", "ports": [f"{ports['grafana']}:3000"],
            "environment": {"GF_AUTH_ANONYMOUS_ENABLED": "true", "GF_AUTH_ANONYMOUS_ORG_ROLE": "Admin",
                            "GF_AUTH_BASIC_ENABLED": "false", "GF_USERS_DEFAULT_THEME": "light"},
            "volumes": [f"{dash_dir}/grafana/provisioning:/etc/grafana/provisioning:ro",
                        f"{dash_dir}/grafana/dashboards:/var/lib/grafana/dashboards:ro"]},
    }
    if s.get("store"):
        out["redis"] = {
            "image": IMAGES["redis"], "container_name": f"{project}-redis", "hostname": f"{project}-redis",
            "cpus": 0.5, "mem_limit": "1g",
            "command": ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec",
                        "--maxmemory", "768mb", "--maxmemory-policy", "noeviction"]}
        out["storeloader"] = {
            "image": kafka_img, "container_name": f"{project}-storeloader", "hostname": f"{project}-storeloader",
            "cpus": 1, "mem_limit": "768m", "depends_on": ["kafka", "redis"],
            "entrypoint": java("512m", "StoreLoader"),
            "environment": {"BOOTSTRAP": boot_int, "TOPICS": ",".join(outs), "GROUP": f"{project}-store",
                            "REDIS_HOST": f"{project}-redis", "REDIS_PORT": "6379"},
            "volumes": [f"{km_dir}:/km:ro"]}
        out["redisinsight"] = {
            "image": IMAGES["redisinsight"], "container_name": f"{project}-redisinsight",
            "hostname": f"{project}-redisinsight", "cpus": 0.2, "mem_limit": "384m",
            "ports": [f"{ports['redisinsight']}:5540"], "depends_on": ["redis"],
            "environment": {"RI_REDIS_HOST": f"{project}-redis", "RI_REDIS_PORT": "6379",
                            "RI_REDIS_ALIAS": f"{project} store"}}
    return out


def expected_keys_text(results_dir, key_sets):
    """The distinct keys in each manifest's key sets, as Prometheus lines for
    the Kafka metrics service to serve. Reads the manifests in results_dir."""
    lines = ["# HELP pipeline_generator_keys Distinct keys in the backlog's key set, from its manifest",
             "# TYPE pipeline_generator_keys gauge"]
    for m in MANIFESTS:
        path = os.path.join(results_dir, m)
        if not os.path.exists(path):
            continue
        try:
            man = json.load(open(path))
        except Exception:
            continue
        for stage, field in (key_sets or {}).items():
            v = man.get(field)
            n = len(v) if isinstance(v, (list, dict)) else (int(v) if isinstance(v, (int, float)) else None)
            if n is not None:
                lines.append(f'pipeline_generator_keys{{manifest="{m}",key_set="{stage}"}} {n}')
    return "\n".join(lines) + "\n"


def write_files(dash_dir, raw, project, s, gc_ceiling):
    """Writes Prometheus's and Grafana's files under dash_dir; returns the board."""
    b = board(raw, project, gc_ceiling=gc_ceiling, store=s.get("store"), extra_panels=s.get("panels"))
    for sub in ("prometheus", "grafana/provisioning/datasources", "grafana/provisioning/dashboards",
                "grafana/dashboards"):
        os.makedirs(os.path.join(dash_dir, sub), exist_ok=True)
    files = {
        "prometheus/prometheus.yml": prometheus_yml(project, s.get("store")),
        "grafana/provisioning/datasources/prometheus.yml": datasource_yml(project),
        "grafana/provisioning/dashboards/dashboards.yml": provider_yml(),
        "grafana/dashboards/pipeline.json": json.dumps(b, indent=1, ensure_ascii=False) + "\n",
    }
    for rel, text in files.items():
        path = os.path.join(dash_dir, rel)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            f.write(text)
        os.replace(tmp, path)
    extra = os.path.join(dash_dir, "extra.prom")
    if not os.path.exists(extra):
        open(extra, "w").write("")
    return b
