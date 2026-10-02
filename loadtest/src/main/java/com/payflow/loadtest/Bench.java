package com.payflow.loadtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.payflow.proto.Device;
import com.payflow.proto.EligibilityGrpc;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.EligibilityResponse;
import com.payflow.proto.FaultConfig;
import com.payflow.proto.LineOfBusiness;
import org.HdrHistogram.Histogram;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * Open-loop latency bench. Requests are fired on a fixed schedule regardless of
 * how fast responses come back, and latency is measured from the time a request
 * was SUPPOSED to be sent. A closed loop (send, wait, send) would quietly slow
 * down when the server does and hide exactly the tail we want to see
 * (coordinated omission).
 */
final class Bench {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Main.Args a;

    Bench(Main.Args a) {
        this.a = a;
    }

    int run() throws Exception {
        Path out = Path.of(a.str("out", "results"));
        int seconds = a.integer("seconds", 20), warmup = a.integer("warmup-seconds", 5);
        try (Cluster c = new Cluster(Path.of(a.str("root", ".")), out.resolve("logs").resolve("bench"), List.of())) {
            c.start();
            c.setFaults(FaultConfig.getDefaultInstance());
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("machine", Map.of("cpus", Runtime.getRuntime().availableProcessors(),
                    "os", System.getProperty("os.name"), "note", "all services + DynamoDB Local + load generator on one box"));

            EligibilityGrpc.EligibilityFutureStub elig = EligibilityGrpc.newFutureStub(c.eligibilityChannel);
            Function<Long, CompletableFuture<Boolean>> eligCall = n -> toCf(elig.withDeadlineAfter(1, TimeUnit.SECONDS)
                    .getEligibleMethods(EligibilityRequest.newBuilder().setCountry("US").setCurrency("USD")
                            .setLob(LineOfBusiness.HOTEL).setDevice(Device.WEB).setAmountMinor(10_000 + n).build()));
            List<Object> eligRuns = new ArrayList<>();
            for (String r : a.str("eligibility-rates", "500,1000,2000").split(",")) {
                int rate = Integer.parseInt(r.trim());
                run(eligCall, rate, warmup);
                eligRuns.add(run(eligCall, rate, seconds));
            }
            report.put("eligibility_grpc", eligRuns);

            Function<Long, CompletableFuture<Boolean>> authCall = n -> c.http.sendAsync(
                            HttpRequest.newBuilder(URI.create("http://localhost:" + Cluster.HTTP + "/v1/payments"))
                                    .timeout(Duration.ofSeconds(10))
                                    .header("Idempotency-Key", UUID.randomUUID().toString())
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(cardBody(n))).build(),
                            HttpResponse.BodyHandlers.discarding())
                    .thenApply(resp -> resp.statusCode() == 201);
            List<Object> authRuns = new ArrayList<>();
            for (String r : a.str("authorize-rates", "50,100,200").split(",")) {
                int rate = Integer.parseInt(r.trim());
                run(authCall, rate, warmup);
                authRuns.add(run(authCall, rate, seconds));
            }
            report.put("authorize_http", authRuns);

            Files.createDirectories(out);
            JSON.writeValue(out.resolve("bench.json").toFile(), report);
            System.out.println(JSON.writeValueAsString(report));
            return 0;
        }
    }

    private static Map<String, Object> run(Function<Long, CompletableFuture<Boolean>> call, int rate, int seconds)
            throws InterruptedException {
        Histogram h = new Histogram(TimeUnit.SECONDS.toNanos(30), 3);
        AtomicInteger ok = new AtomicInteger(), failed = new AtomicInteger();
        long total = (long) rate * seconds, intervalNs = 1_000_000_000L / rate;
        List<CompletableFuture<?>> pending = new ArrayList<>((int) total);
        long start = System.nanoTime();
        for (long i = 0; i < total; i++) {
            long intended = start + i * intervalNs;
            long wait;
            while ((wait = intended - System.nanoTime()) > 0) LockSupport.parkNanos(wait);
            pending.add(call.apply(i).handle((success, err) -> {
                long latency = System.nanoTime() - intended;
                synchronized (h) {
                    h.recordValue(Math.min(latency, h.getHighestTrackableValue()));
                }
                if (err == null && Boolean.TRUE.equals(success)) ok.incrementAndGet();
                else failed.incrementAndGet();
                return null;
            }));
        }
        CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).join();
        double achieved = total / ((System.nanoTime() - start) / 1e9);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("targetRps", rate);
        m.put("achievedRps", Math.round(achieved));
        m.put("requests", total);
        m.put("ok", ok.get());
        m.put("failed", failed.get());
        m.put("p50ms", ms(h.getValueAtPercentile(50)));
        m.put("p90ms", ms(h.getValueAtPercentile(90)));
        m.put("p99ms", ms(h.getValueAtPercentile(99)));
        m.put("p999ms", ms(h.getValueAtPercentile(99.9)));
        m.put("maxMs", ms(h.getMaxValue()));
        System.out.println("[bench] " + m);
        return m;
    }

    private static double ms(long ns) {
        return Math.round(ns / 10_000.0) / 100.0;
    }

    private static String cardBody(long n) {
        long amount = 10_000 + n % 1000;
        return """
                {"orderRef":"bench_%d_%s","country":"US","currency":"USD","lob":"HOTEL","device":"WEB",
                 "amountMinor":%d,"tenders":[{"method":"CARD","amountMinor":%d,"token":"tok_visa"}]}"""
                .formatted(n, UUID.randomUUID().toString().substring(0, 6), amount, amount);
    }

    private static CompletableFuture<Boolean> toCf(com.google.common.util.concurrent.ListenableFuture<EligibilityResponse> f) {
        CompletableFuture<Boolean> cf = new CompletableFuture<>();
        Futures.addCallback(f, new FutureCallback<>() {
            public void onSuccess(EligibilityResponse r) { cf.complete(r.getDecisionsCount() > 0); }
            public void onFailure(Throwable t) { cf.completeExceptionally(t); }
        }, MoreExecutors.directExecutor());
        return cf;
    }
}
