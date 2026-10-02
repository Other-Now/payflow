package com.payflow.loadtest;

import java.util.HashMap;
import java.util.Map;

/**
 * java -Dsqlite4java.library.path=loadtest/target/native-libs -jar loadtest/target/loadtest.jar chaos|bench [--key=value ...]
 *
 * Both modes start the whole system themselves: DynamoDB Local in this JVM,
 * and mock-psp, eligibility-service and payment-service as child JVMs.
 */
public final class Main {

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("usage: chaos|bench [--key=value ...]");
            System.exit(2);
        }
        Map<String, String> args = new HashMap<>();
        for (int i = 1; i < argv.length; i++) {
            String a = argv[i].replaceFirst("^--", "");
            int eq = a.indexOf('=');
            args.put(eq < 0 ? a : a.substring(0, eq), eq < 0 ? "true" : a.substring(eq + 1));
        }
        int code = switch (argv[0]) {
            case "chaos" -> new Chaos(new Args(args)).run();
            case "bench" -> new Bench(new Args(args)).run();
            default -> {
                System.err.println("unknown mode " + argv[0]);
                yield 2;
            }
        };
        System.exit(code);
    }

    record Args(Map<String, String> m) {
        String str(String k, String def) { return m.getOrDefault(k, def); }
        int integer(String k, int def) { return m.containsKey(k) ? Integer.parseInt(m.get(k)) : def; }
        long lng(String k, long def) { return m.containsKey(k) ? Long.parseLong(m.get(k)) : def; }
        double dbl(String k, double def) { return m.containsKey(k) ? Double.parseDouble(m.get(k)) : def; }
        boolean bool(String k) { return Boolean.parseBoolean(m.getOrDefault(k, "false")); }
    }
}
