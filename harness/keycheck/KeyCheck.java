import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Where Flink would put each key, asked of Flink.
 *
 * A keyed stream does not spread over subtasks by itself. Flink hashes the key
 * into one of maxParallelism key groups and hands each subtask a contiguous
 * range of groups, so a small key set can land two keys on one subtask and none
 * on another. The largest case then sits below its CPU cap with an idle
 * subtask, and the table is measuring key skew rather than cores.
 *
 * The harness could work this out itself -- it is a hash and a division -- but
 * the answer would then be the harness's opinion of what the engine does. This
 * calls {@link KeyGroupRangeAssignment} out of the image under test, the same
 * class the running job uses, compiled with the host JDK against the image's
 * own flink-dist jar. It is the pattern the offset sampler already uses with
 * the broker image's kafka-clients jar.
 *
 * Three modes, all reading standard input:
 *
 *   layout  &lt;maxParallelism&gt; &lt;pars&gt;   keys, one per line
 *        -&gt; key TAB keyGroup TAB subtask at each parallelism
 *   suggest &lt;pars&gt; &lt;howMany&gt;          lines of setName TAB key
 *        -&gt; maxParallelism values that divide every set evenly at every
 *           parallelism, one per line, smallest first
 *   default &lt;pars&gt;                    nothing
 *        -&gt; parallelism TAB the maxParallelism Flink would choose for it
 *
 * The arithmetic on top of this -- shares, ceilings, what to do about it --
 * belongs to the harness, where it can be self-tested without a stack.
 */
public final class KeyCheck {

    /** Flink's own bounds on an explicit or searched maxParallelism. */
    static final int LOWER = 128;
    static final int UPPER = 32768;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: KeyCheck layout <maxParallelism> <pars> | "
                    + "suggest <pars> <howMany> | default <pars>");
            System.exit(2);
        }
        String mode = args[0];
        if (mode.equals("layout")) {
            layout(Integer.parseInt(args[1].trim()), ints(args[2]));
        } else if (mode.equals("suggest")) {
            suggest(ints(args[1]), Integer.parseInt(args[2].trim()));
        } else if (mode.equals("default")) {
            for (int par : ints(args[1])) {
                System.out.println(par + "\t" + KeyGroupRangeAssignment.computeDefaultMaxParallelism(par));
            }
        } else {
            System.err.println("unknown mode: " + mode);
            System.exit(2);
        }
    }

    private static int[] ints(String csv) {
        String[] parts = csv.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }

    private static void layout(int maxPar, int[] pars) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        String key;
        while ((key = in.readLine()) != null) {
            if (key.isEmpty()) {
                continue;
            }
            StringBuilder out = new StringBuilder(key);
            out.append('\t').append(KeyGroupRangeAssignment.assignToKeyGroup(key, maxPar));
            for (int par : pars) {
                out.append('\t').append(
                        KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxPar, par));
            }
            System.out.println(out);
        }
    }

    /**
     * maxParallelism values that give every set the same number of keys on
     * every subtask, at every parallelism under test. Nothing is printed when
     * there is none, which is the answer for a key set too small or too lumpy
     * to divide at all -- then the keys themselves have to change.
     */
    private static void suggest(int[] pars, int howMany) throws Exception {
        Map<String, List<String>> sets = new LinkedHashMap<>();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            int tab = line.indexOf('\t');
            if (tab < 0) {
                throw new IllegalArgumentException("suggest wants setName TAB key, got: " + line);
            }
            sets.computeIfAbsent(line.substring(0, tab), k -> new ArrayList<>())
                    .add(line.substring(tab + 1));
        }
        int found = 0;
        for (int maxPar = LOWER; maxPar <= UPPER && found < howMany; maxPar++) {
            boolean ok = true;
            for (List<String> keys : sets.values()) {
                for (int par : pars) {
                    if (!even(keys, maxPar, par)) {
                        ok = false;
                        break;
                    }
                }
                if (!ok) {
                    break;
                }
            }
            if (ok) {
                System.out.println(maxPar);
                found++;
            }
        }
    }

    private static boolean even(List<String> keys, int maxPar, int par) {
        if (keys.size() % par != 0) {
            return false;
        }
        int[] n = new int[par];
        for (String k : keys) {
            n[KeyGroupRangeAssignment.assignKeyToParallelOperator(k, maxPar, par)]++;
        }
        int want = keys.size() / par;
        for (int v : n) {
            if (v != want) {
                return false;
            }
        }
        return true;
    }

    private KeyCheck() {
    }
}
