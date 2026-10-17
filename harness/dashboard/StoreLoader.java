import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The optional Redis store (pipeline.json "dashboard": {"store": "redis"}):
 * the latest value of every key on every output, updated in place, without
 * slowing the pipeline down.
 *
 * <p>It is not part of the job. It reads the outputs the job already writes,
 * as one more consumer group, committed records only. If Redis is slow or
 * stopped it falls behind and Kafka holds the difference; the job never waits
 * for it. Its lag is on the dashboard with every other group's.
 *
 * <p>Each batch is cut to the last record per key before anything is written,
 * so Redis sees about one write per key per batch, not one per record. A
 * write lands only if its offset is newer than the one stored beside the
 * value, so a batch read again after a restart never moves a key back. A
 * deleted and recreated topic starts its offsets again at zero; the topic id
 * tells the new one from the old, the new one always wins, and keys only the
 * old one had are removed.
 *
 * <p>One hash per key, named prefix:key. A value that is a JSON object is
 * stored field by field, each as written on the topic; anything else is
 * stored whole in the field "value". Beside it: kafka_topic_id, kafka_partition,
 * kafka_offset.
 *
 * <p>Standard library and kafka-clients only, with a few lines of the Redis
 * protocol written out, so it compiles against the broker image's own jars
 * like the offset sampler. Environment: BOOTSTRAP; TOPICS, comma-separated
 * topic or topic=prefix; GROUP; REDIS_HOST; REDIS_PORT (6379); PORT (9400).
 */
public final class StoreLoader {

    static final String TOPIC_ID = "kafka_topic_id";
    static final String OFFSET = "kafka_offset";
    static final String PARTITION = "kafka_partition";

    /**
     * KEYS[1] the hash; ARGV[1] topic id, ARGV[2] offset, ARGV[3..] field/value pairs.
     * Returns 2 when the key is new, 1 when it moved forward, 0 when the stored offset is newer.
     */
    static final String WRITE_IF_NEWER =
            "local id = redis.call('HGET', KEYS[1], '" + TOPIC_ID + "') "
            + "if id == ARGV[1] then "
            + "  local off = redis.call('HGET', KEYS[1], '" + OFFSET + "') "
            + "  if off and tonumber(off) >= tonumber(ARGV[2]) then return 0 end "
            + "end "
            + "local created = redis.call('EXISTS', KEYS[1]) == 0 "
            + "redis.call('DEL', KEYS[1]) "
            + "redis.call('HSET', KEYS[1], unpack(ARGV, 3)) "
            + "if created then return 2 end return 1";

    static final Map<String, AtomicLong> READ = new ConcurrentHashMap<>();
    static final Map<String, AtomicLong> WRITTEN = new ConcurrentHashMap<>();
    static final Map<String, AtomicLong> KEYS = new ConcurrentHashMap<>();
    static final AtomicLong REDIS_ERRORS = new AtomicLong();
    static final AtomicLong REDIS_MEMORY = new AtomicLong();

    public static void main(String[] args) throws Exception {
        String bootstrap = Objects.requireNonNull(System.getenv("BOOTSTRAP"), "BOOTSTRAP is not set");
        Map<String, String> prefixes = new LinkedHashMap<>();
        for (String s : Objects.requireNonNull(System.getenv("TOPICS"), "TOPICS is not set").split(",")) {
            if (s.isBlank()) {
                continue;
            }
            String[] kv = s.trim().split("=", 2);
            prefixes.put(kv[0], kv.length > 1 ? kv[1] : kv[0]);
        }
        String group = env("GROUP", "store");
        String host = env("REDIS_HOST", "redis");
        int port = Integer.parseInt(env("REDIS_PORT", "6379"));
        int metricsPort = Integer.parseInt(env("PORT", "9400"));
        for (String t : prefixes.keySet()) {
            READ.put(t, new AtomicLong());
            WRITTEN.put(t, new AtomicLong());
            KEYS.put(t, new AtomicLong());
        }
        serveMetrics(metricsPort, prefixes);

        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        Properties cp = new Properties();
        cp.putAll(ap);
        cp.put("group.id", group);
        cp.put("enable.auto.commit", "false");
        cp.put("auto.offset.reset", "earliest");
        cp.put("isolation.level", "read_committed");
        cp.put("allow.auto.create.topics", "false");
        cp.put("check.crcs", "false");
        cp.put("fetch.min.bytes", Integer.toString(1 << 20));
        cp.put("fetch.max.wait.ms", "200");
        cp.put("fetch.max.bytes", Integer.toString(64 << 20));
        cp.put("max.partition.fetch.bytes", Integer.toString(8 << 20));
        cp.put("max.poll.records", "20000");
        cp.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cp.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        Map<String, String> ids = new HashMap<>();
        Map<TopicPartition, String> idAtAssign = new HashMap<>();
        List<TopicPartition> assigned = new ArrayList<>();
        Redis redis = null;
        long lastCheck = 0;
        long lastInfo = 0;
        System.out.println("{\"storeloader\":\"up\",\"topics\":\"" + prefixes + "\"}");
        System.out.flush();
        try (Admin admin = Admin.create(ap); KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            while (true) {
                try {
                    if (redis == null) {
                        redis = new Redis(host, port);
                        for (String t : prefixes.keySet()) {
                            KEYS.get(t).set(redis.countKeys(prefixes.get(t) + ":*"));
                        }
                    }
                    if (System.currentTimeMillis() - lastInfo > 5000) {
                        lastInfo = System.currentTimeMillis();
                        REDIS_MEMORY.set(redis.usedMemory());
                    }
                    if (System.currentTimeMillis() - lastCheck > 2000) {
                        lastCheck = System.currentTimeMillis();
                        Set<String> exist = admin.listTopics().names().get();
                        List<String> present = new ArrayList<>();
                        for (String t : prefixes.keySet()) {
                            if (exist.contains(t)) {
                                present.add(t);
                            }
                        }
                        Map<String, TopicDescription> desc = present.isEmpty() ? Map.of()
                                : admin.describeTopics(present).allTopicNames().get();
                        List<TopicPartition> want = new ArrayList<>();
                        List<TopicPartition> fresh = new ArrayList<>();
                        for (String t : prefixes.keySet()) {
                            TopicDescription d = desc.get(t);
                            if (d == null) {
                                continue;
                            }
                            String id = d.topicId().toString();
                            if (!id.equals(ids.get(t))) {
                                long gone = redis.removeOtherTopicIds(prefixes.get(t) + ":*", id);
                                KEYS.get(t).addAndGet(-gone);
                                ids.put(t, id);
                            }
                            for (TopicPartitionInfo p : d.partitions()) {
                                TopicPartition tp = new TopicPartition(t, p.partition());
                                want.add(tp);
                                if (!id.equals(idAtAssign.get(tp))) {
                                    fresh.add(tp);
                                    idAtAssign.put(tp, id);
                                }
                            }
                        }
                        if (!want.equals(assigned) || !fresh.isEmpty()) {
                            c.assign(want);
                            assigned = want;
                            if (!fresh.isEmpty()) {
                                c.seekToBeginning(fresh);
                            }
                        }
                    }
                    if (assigned.isEmpty()) {
                        Thread.sleep(500);
                        continue;
                    }
                    Map<TopicPartition, Long> batchStart = new HashMap<>();
                    for (TopicPartition tp : assigned) {
                        batchStart.put(tp, c.position(tp));
                    }
                    // the last record per key, in the order first seen
                    Map<String, ConsumerRecord<byte[], byte[]>> latest = new LinkedHashMap<>();
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                        READ.get(r.topic()).incrementAndGet();
                        if (r.key() == null || r.value() == null) {
                            continue;
                        }
                        latest.put(r.topic() + '\u0000' + new String(r.key(), StandardCharsets.UTF_8), r);
                    }
                    if (latest.isEmpty()) {
                        continue;
                    }
                    try {
                        List<String[]> calls = new ArrayList<>();
                        List<String> topicOf = new ArrayList<>();
                        for (ConsumerRecord<byte[], byte[]> r : latest.values()) {
                            List<String> argv = new ArrayList<>();
                            argv.add(ids.get(r.topic()));
                            argv.add(Long.toString(r.offset()));
                            fields(new String(r.value(), StandardCharsets.UTF_8), argv);
                            argv.add(TOPIC_ID);
                            argv.add(ids.get(r.topic()));
                            argv.add(PARTITION);
                            argv.add(Integer.toString(r.partition()));
                            argv.add(OFFSET);
                            argv.add(Long.toString(r.offset()));
                            calls.add(argv.toArray(new String[0]));
                            topicOf.add(r.topic());
                        }
                        List<String> keys = new ArrayList<>();
                        for (ConsumerRecord<byte[], byte[]> r : latest.values()) {
                            keys.add(prefixes.get(r.topic()) + ":" + new String(r.key(), StandardCharsets.UTF_8));
                        }
                        long[] results = redis.writeIfNewer(keys, calls);
                        for (int i = 0; i < results.length; i++) {
                            if (results[i] > 0) {
                                WRITTEN.get(topicOf.get(i)).incrementAndGet();
                            }
                            if (results[i] == 2) {
                                KEYS.get(topicOf.get(i)).incrementAndGet();
                            }
                        }
                        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
                        for (TopicPartition tp : assigned) {
                            commit.put(tp, new OffsetAndMetadata(c.position(tp)));
                        }
                        c.commitSync(commit);
                    } catch (IOException e) {
                        // Redis is not there: read the batch again once it is
                        REDIS_ERRORS.incrementAndGet();
                        System.out.println("Redis did not take the batch, will read it again: " + e);
                        batchStart.forEach(c::seek);
                        redis.closeQuietly();
                        redis = null;
                        Thread.sleep(1000);
                    }
                } catch (IOException e) {
                    REDIS_ERRORS.incrementAndGet();
                    System.out.println("Redis is not answering: " + e);
                    if (redis != null) {
                        redis.closeQuietly();
                    }
                    redis = null;
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    System.out.println("the loader did not get a batch: " + e);
                    Thread.sleep(1000);
                }
            }
        }
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v;
    }

    /**
     * Pure. A JSON object's top-level fields as name/value pairs, each value as
     * written on the topic (a string without its quotes, anything else as its
     * text, so 12000000.50 stays 12000000.50); anything that is not a JSON
     * object goes in whole as "value".
     */
    static void fields(String json, List<String> out) {
        String s = json.trim();
        List<String> pairs = new ArrayList<>();
        if (s.startsWith("{") && s.endsWith("}") && parseObject(s, pairs)) {
            out.addAll(pairs);
        } else {
            out.add("value");
            out.add(json);
        }
    }

    static boolean parseObject(String s, List<String> out) {
        int[] i = {1};
        skip(s, i);
        if (s.charAt(i[0]) == '}') {
            return true;
        }
        while (i[0] < s.length()) {
            skip(s, i);
            if (s.charAt(i[0]) != '"') {
                return false;
            }
            String name = readString(s, i);
            if (name == null) {
                return false;
            }
            skip(s, i);
            if (s.charAt(i[0]) != ':') {
                return false;
            }
            i[0]++;
            skip(s, i);
            String value;
            if (s.charAt(i[0]) == '"') {
                value = readString(s, i);
                if (value == null) {
                    return false;
                }
            } else {
                int from = i[0];
                int depth = 0;
                boolean inStr = false;
                while (i[0] < s.length()) {
                    char ch = s.charAt(i[0]);
                    if (inStr) {
                        if (ch == '\\') {
                            i[0]++;
                        } else if (ch == '"') {
                            inStr = false;
                        }
                    } else if (ch == '"') {
                        inStr = true;
                    } else if (ch == '{' || ch == '[') {
                        depth++;
                    } else if (ch == '}' || ch == ']') {
                        if (depth == 0) {
                            break;
                        }
                        depth--;
                    } else if (ch == ',' && depth == 0) {
                        break;
                    }
                    i[0]++;
                }
                value = s.substring(from, i[0]).trim();
            }
            out.add(name);
            out.add(value);
            skip(s, i);
            if (i[0] >= s.length()) {
                return false;
            }
            char ch = s.charAt(i[0]);
            i[0]++;
            if (ch == '}') {
                return i[0] == s.length();
            }
            if (ch != ',') {
                return false;
            }
        }
        return false;
    }

    static void skip(String s, int[] i) {
        while (i[0] < s.length() && Character.isWhitespace(s.charAt(i[0]))) {
            i[0]++;
        }
    }

    static String readString(String s, int[] i) {
        StringBuilder b = new StringBuilder();
        i[0]++;
        while (i[0] < s.length()) {
            char ch = s.charAt(i[0]++);
            if (ch == '"') {
                return b.toString();
            }
            if (ch == '\\' && i[0] < s.length()) {
                char e = s.charAt(i[0]++);
                switch (e) {
                    case 'n' -> b.append('\n');
                    case 't' -> b.append('\t');
                    case 'r' -> b.append('\r');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'u' -> {
                        b.append((char) Integer.parseInt(s.substring(i[0], i[0] + 4), 16));
                        i[0] += 4;
                    }
                    default -> b.append(e);
                }
            } else {
                b.append(ch);
            }
        }
        return null;
    }

    static void serveMetrics(int port, Map<String, String> prefixes) throws IOException {
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/metrics", ex -> {
            StringBuilder sb = new StringBuilder();
            sb.append("# HELP store_records_read_total Committed records the store loader has read\n");
            sb.append("# TYPE store_records_read_total counter\n");
            READ.forEach((t, v) -> sb.append("store_records_read_total{topic=\"").append(t).append("\"} ").append(v.get()).append('\n'));
            sb.append("# HELP store_keys_written_total Keys written to Redis, after each batch is cut to the last record per key\n");
            sb.append("# TYPE store_keys_written_total counter\n");
            WRITTEN.forEach((t, v) -> sb.append("store_keys_written_total{topic=\"").append(t).append("\"} ").append(v.get()).append('\n'));
            sb.append("# HELP store_keys Keys in Redis for the topic\n");
            sb.append("# TYPE store_keys gauge\n");
            KEYS.forEach((t, v) -> sb.append("store_keys{topic=\"").append(t).append("\"} ").append(v.get()).append('\n'));
            sb.append("# HELP store_redis_used_memory_bytes Memory Redis reports in use\n");
            sb.append("# TYPE store_redis_used_memory_bytes gauge\n");
            sb.append("store_redis_used_memory_bytes ").append(REDIS_MEMORY.get()).append('\n');
            sb.append("# HELP store_redis_errors_total Times Redis did not answer or did not take a batch\n");
            sb.append("# TYPE store_redis_errors_total counter\n");
            sb.append("store_redis_errors_total ").append(REDIS_ERRORS.get()).append('\n');
            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        http.start();
    }

    /** The few Redis commands the loader needs, over the plain Redis protocol. */
    static final class Redis {
        final Socket socket;
        final BufferedOutputStream out;
        final InputStream in;
        String sha;

        Redis(String host, int port) throws IOException {
            socket = new Socket(host, port);
            socket.setSoTimeout(30_000);
            out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
            in = new BufferedInputStream(socket.getInputStream(), 1 << 16);
            sha = (String) call("SCRIPT", "LOAD", WRITE_IF_NEWER);
        }

        void closeQuietly() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already gone
            }
        }

        Object call(String... args) throws IOException {
            send(args);
            out.flush();
            return reply();
        }

        long[] writeIfNewer(List<String> keys, List<String[]> argvs) throws IOException {
            for (int i = 0; i < keys.size(); i++) {
                String[] a = new String[argvs.get(i).length + 4];
                a[0] = "EVALSHA";
                a[1] = sha;
                a[2] = "1";
                a[3] = keys.get(i);
                System.arraycopy(argvs.get(i), 0, a, 4, argvs.get(i).length);
                send(a);
            }
            out.flush();
            long[] r = new long[keys.size()];
            boolean reload = false;
            for (int i = 0; i < r.length; i++) {
                Object o = reply();
                if (o instanceof RedisError e && e.getMessage().startsWith("NOSCRIPT")) {
                    reload = true;
                } else if (o instanceof RedisError e) {
                    throw new IOException(e.getMessage());
                } else {
                    r[i] = (Long) o;
                }
            }
            if (reload) {
                // Redis restarted and forgot the script: load it and send the batch again
                sha = (String) call("SCRIPT", "LOAD", WRITE_IF_NEWER);
                return writeIfNewer(keys, argvs);
            }
            return r;
        }

        long countKeys(String match) throws IOException {
            long n = 0;
            String cursor = "0";
            do {
                List<?> page = (List<?>) call("SCAN", cursor, "MATCH", match, "COUNT", "1000");
                cursor = (String) page.get(0);
                n += ((List<?>) page.get(1)).size();
            } while (!cursor.equals("0"));
            return n;
        }

        /** Removes the keys under the prefix written from any other topic id; returns how many. */
        long removeOtherTopicIds(String match, String id) throws IOException {
            long n = 0;
            String cursor = "0";
            do {
                List<?> page = (List<?>) call("SCAN", cursor, "MATCH", match, "COUNT", "1000");
                cursor = (String) page.get(0);
                for (Object k : (List<?>) page.get(1)) {
                    Object stored = call("HGET", (String) k, TOPIC_ID);
                    if (!id.equals(stored)) {
                        n += (Long) call("UNLINK", (String) k);
                    }
                }
            } while (!cursor.equals("0"));
            return n;
        }

        long usedMemory() throws IOException {
            String info = (String) call("INFO", "memory");
            for (String line : info.split("\r\n")) {
                if (line.startsWith("used_memory:")) {
                    return Long.parseLong(line.substring("used_memory:".length()).trim());
                }
            }
            return 0;
        }

        void send(String... args) throws IOException {
            out.write(('*' + Integer.toString(args.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
            for (String a : args) {
                byte[] b = a.getBytes(StandardCharsets.UTF_8);
                out.write(('$' + Integer.toString(b.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                out.write(b);
                out.write('\r');
                out.write('\n');
            }
        }

        Object reply() throws IOException {
            int t = in.read();
            if (t < 0) {
                throw new IOException("Redis closed the connection");
            }
            String line = line();
            switch (t) {
                case '+':
                    return line;
                case '-':
                    return new RedisError(line);
                case ':':
                    return Long.parseLong(line);
                case '$': {
                    int n = Integer.parseInt(line);
                    if (n < 0) {
                        return null;
                    }
                    byte[] b = in.readNBytes(n + 2);
                    return new String(b, 0, n, StandardCharsets.UTF_8);
                }
                case '*': {
                    int n = Integer.parseInt(line);
                    List<Object> l = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        l.add(reply());
                    }
                    return l;
                }
                default:
                    throw new IOException("unexpected reply from Redis: " + (char) t + line);
            }
        }

        String line() throws IOException {
            StringBuilder b = new StringBuilder();
            int ch;
            while ((ch = in.read()) != '\r') {
                if (ch < 0) {
                    throw new IOException("Redis closed the connection");
                }
                b.append((char) ch);
            }
            in.read();
            return b.toString();
        }
    }

    static final class RedisError extends RuntimeException {
        private static final long serialVersionUID = 1L;

        RedisError(String m) {
            super(m, null, false, false);
        }
    }

    private StoreLoader() {
    }
}
