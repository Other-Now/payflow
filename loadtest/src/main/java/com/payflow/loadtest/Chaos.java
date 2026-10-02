package com.payflow.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.payflow.proto.AuthStatus;
import com.payflow.proto.FaultConfig;
import com.payflow.proto.LedgerEntry;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Chaos run. Clients behave like real checkout clients (retry with the same
 * Idempotency-Key on errors and 202s, sometimes double-submit), the provider
 * injects declines, lost requests and timeouts-after-commit, and payment-service
 * is SIGKILLed and restarted mid-run. Afterwards the database is audited against
 * the provider's own ledger.
 */
final class Chaos {

    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final Main.Args a;
    private final int orders, concurrency, maxKills;
    private final long killEveryMs;
    private final double dupRate;
    private final boolean unsafe;
    private final FaultConfig faults;

    // client-side counters
    private final AtomicInteger posts = new AtomicInteger(), connectionErrors = new AtomicInteger(),
            inFlightPolls = new AtomicInteger(), duplicatesSent = new AtomicInteger(), replaysSeen = new AtomicInteger(),
            serverErrors = new AtomicInteger(), conflicts = new AtomicInteger(), giveUps = new AtomicInteger(),
            unexpected4xx = new AtomicInteger(), kills = new AtomicInteger();

    Chaos(Main.Args a) {
        this.a = a;
        this.orders = a.integer("orders", 2000);
        this.concurrency = a.integer("concurrency", 32);
        this.maxKills = a.integer("kills", 8);
        this.killEveryMs = a.lng("kill-every-ms", 6000);
        this.dupRate = a.dbl("dup-rate", 0.2);
        this.unsafe = a.bool("unsafe");
        this.faults = FaultConfig.newBuilder()
                .setDeclineRate(a.dbl("decline", 0.08))
                .setUnavailableRate(a.dbl("unavailable", 0.03))
                .setTimeoutBeforeRate(a.dbl("timeout-before", 0.03))
                .setTimeoutAfterRate(a.dbl("timeout-after", 0.05))
                .setHangMs(a.lng("hang-ms", 1200))
                .build();
    }

    int run() throws Exception {
        String name = unsafe ? "chaos-unsafe" : "chaos";
        Path out = Path.of(a.str("out", "results"));
        List<String> paymentArgs = List.of(
                "--payflow.psp.deadline-ms=" + a.lng("deadline-ms", 500),
                "--payflow.recon.stale-after-ms=" + a.lng("stale-after-ms", 5000),
                "--payflow.recon.interval-ms=1000",
                "--payflow.unsafe-mode=" + unsafe);

        try (Cluster c = new Cluster(Path.of(a.str("root", ".")), out.resolve("logs").resolve(name), paymentArgs)) {
            c.start();
            c.setFaults(faults);
            long t0 = System.currentTimeMillis();

            AtomicBoolean clientsDone = new AtomicBoolean();
            Thread killer = Thread.ofPlatform().start(() -> killLoop(c, clientsDone));

            List<Future<?>> futures = new ArrayList<>();
            Semaphore slots = new Semaphore(concurrency);
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < orders; i++) {
                    int n = i;
                    slots.acquire();
                    futures.add(pool.submit(() -> {
                        try {
                            runOrder(c, n);
                        } finally {
                            slots.release();
                        }
                        return null;
                    }));
                }
                for (Future<?> f : futures) f.get();
            }
            clientsDone.set(true);
            killer.join();
            long clientMs = System.currentTimeMillis() - t0;
            log("clients done in %.1fs; settling", clientMs / 1000.0);

            // Let the reconciler finish everything still in flight. Faults stay on.
            long settleStart = System.currentTimeMillis();
            JsonNode payments = payments(c);
            while (inFlight(payments) > 0 && System.currentTimeMillis() - settleStart < a.lng("settle-timeout-ms", 120_000)) {
                Thread.sleep(1000);
                payments = payments(c);
            }
            long settleMs = System.currentTimeMillis() - settleStart;

            Map<String, Object> report = new LinkedHashMap<>();
            report.put("mode", unsafe ? "UNSAFE (no idempotency, new provider key per retry)" : "safe");
            report.put("config", Map.of("orders", orders, "concurrency", concurrency, "dupRate", dupRate,
                    "faults", Map.of("decline", faults.getDeclineRate(), "unavailable", faults.getUnavailableRate(),
                            "timeoutBefore", faults.getTimeoutBeforeRate(), "timeoutAfter", faults.getTimeoutAfterRate(),
                            "hangMs", faults.getHangMs()),
                    "pspDeadlineMs", a.lng("deadline-ms", 500)));
            report.put("timing", Map.of("clientPhaseSec", clientMs / 1000.0, "settleSec", settleMs / 1000.0));
            report.put("chaos", Map.of("paymentServiceKills", kills.get()));
            report.put("client", clientStats());
            report.putAll(audit(payments, c.ledger().getEntriesList()));

            Files.createDirectories(out);
            Path file = out.resolve(name + ".json");
            JSON.writeValue(file.toFile(), report);
            System.out.println(JSON.writeValueAsString(report));
            log("wrote %s", file);

            @SuppressWarnings("unchecked")
            Map<String, Object> verdict = (Map<String, Object>) report.get("verdict");
            return unsafe || Boolean.TRUE.equals(verdict.get("pass")) ? 0 : 1;
        }
    }

    // ------------------------------------------------------------------ clients

    private void runOrder(Cluster c, int n) throws InterruptedException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String orderRef = "ord_" + n + "_" + UUID.randomUUID().toString().substring(0, 8);
        String key = UUID.randomUUID().toString();
        String body = orderBody(orderRef, rnd);

        if (rnd.nextDouble() < dupRate) { // impatient double-click: a concurrent copy of the same request
            duplicatesSent.incrementAndGet();
            Thread.ofVirtual().start(() -> {
                try {
                    send(c, "POST", "/v1/payments", key, body);
                } catch (Exception ignored) {
                    // the original request's loop is what we check
                }
            });
        }

        JsonNode p = null;
        long deadline = System.currentTimeMillis() + 180_000;
        while (p == null && System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> r = send(c, "POST", "/v1/payments", key, body);
                if ("true".equals(r.headers().firstValue("Idempotent-Replayed").orElse(""))) replaysSeen.incrementAndGet();
                switch (r.statusCode()) {
                    case 201, 402 -> p = JSON.readTree(r.body());
                    case 202 -> { inFlightPolls.incrementAndGet(); Thread.sleep(400); }
                    case 400, 404, 422 -> { unexpected4xx.incrementAndGet(); return; }
                    default -> { serverErrors.incrementAndGet(); Thread.sleep(300); }
                }
            } catch (IOException e) {
                connectionErrors.incrementAndGet(); // payment-service is down: retry with the same key
                Thread.sleep(300);
            }
        }
        if (p == null) {
            giveUps.incrementAndGet();
            return;
        }
        if (!"AUTHORIZED".equals(p.get("status").asText())) return;

        String id = p.get("id").asText();
        double r = rnd.nextDouble();
        if (r < 0.70) {
            JsonNode captured = act(c, id, "capture", "CAPTURED");
            if (captured != null && "CAPTURED".equals(captured.get("status").asText()) && rnd.nextDouble() < 0.25) {
                act(c, id, "refund", "REFUNDED");
            }
        } else if (r < 0.85) {
            act(c, id, "void", "VOIDED");
        } // else: leave the hold open (customer hasn't checked out the trip yet)
    }

    /** Retries an action until the payment reaches {@code target}; 409s are resolved by reading the state. */
    private JsonNode act(Cluster c, String id, String op, String target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpResponse<String> r = send(c, "POST", "/v1/payments/" + id + "/" + op, null, null);
                if (r.statusCode() == 200) return JSON.readTree(r.body());
                if (r.statusCode() == 409) {
                    conflicts.incrementAndGet(); // usually the reconciler holding the lock
                    JsonNode now = JSON.readTree(send(c, "GET", "/v1/payments/" + id, null, null).body());
                    if (target.equals(now.get("status").asText())) return now;
                } else if (r.statusCode() == 202) {
                    inFlightPolls.incrementAndGet();
                } else {
                    serverErrors.incrementAndGet();
                }
            } catch (IOException e) {
                connectionErrors.incrementAndGet();
            }
            Thread.sleep(400);
        }
        giveUps.incrementAndGet();
        return null;
    }

    private HttpResponse<String> send(Cluster c, String method, String path, String key, String body)
            throws IOException, InterruptedException {
        posts.incrementAndGet();
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + Cluster.HTTP + path))
                .timeout(Duration.ofSeconds(30));
        if (key != null) b.header("Idempotency-Key", key);
        if (body != null) b.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        else if (method.equals("POST")) b.POST(HttpRequest.BodyPublishers.noBody());
        else b.GET();
        return c.http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** 40% card, 20% wallet, 40% split gift card + card. */
    private static String orderBody(String orderRef, ThreadLocalRandom rnd) {
        ObjectNode o = JSON.createObjectNode();
        long amount = 5_000 + rnd.nextLong(95_000);
        double kind = rnd.nextDouble();
        o.put("orderRef", orderRef).put("country", "US").put("currency", "USD").put("lob", "HOTEL")
                .put("device", kind < 0.6 && kind >= 0.4 ? "IOS" : "WEB").put("amountMinor", amount);
        var tenders = o.putArray("tenders");
        if (kind < 0.4) {
            tenders.addObject().put("method", "CARD").put("amountMinor", amount).put("token", "tok_visa");
        } else if (kind < 0.6) {
            tenders.addObject().put("method", "WALLET").put("amountMinor", amount).put("token", "tok_wallet");
        } else {
            long gift = 1_000 + rnd.nextLong(amount / 2);
            tenders.addObject().put("method", "GIFT_CARD").put("amountMinor", gift).put("token", "tok_gc");
            tenders.addObject().put("method", "CARD").put("amountMinor", amount - gift).put("token", "tok_visa");
        }
        return o.toString();
    }

    private void killLoop(Cluster c, AtomicBoolean clientsDone) {
        try {
            while (!clientsDone.get() && kills.get() < maxKills) {
                Thread.sleep(killEveryMs / 2 + ThreadLocalRandom.current().nextLong(killEveryMs));
                if (clientsDone.get()) break;
                c.killPayment();
                kills.incrementAndGet();
                log("kill -9 payment-service (#%d)", kills.get());
                Thread.sleep(500);
                c.startPayment();
                log("payment-service back up");
            }
        } catch (Exception e) {
            throw new IllegalStateException("killer failed", e);
        }
    }

    private Map<String, Object> clientStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("httpRequests", posts.get());
        m.put("connectionErrorsRetried", connectionErrors.get());
        m.put("inFlight202Polls", inFlightPolls.get());
        m.put("concurrentDuplicatesSent", duplicatesSent.get());
        m.put("idempotentReplaysSeen", replaysSeen.get());
        m.put("serverErrors", serverErrors.get());
        m.put("conflicts409", conflicts.get());
        m.put("unexpected4xx", unexpected4xx.get());
        m.put("gaveUp", giveUps.get());
        return m;
    }

    // -------------------------------------------------------------------- audit

    private static JsonNode payments(Cluster c) throws Exception {
        return JSON.readTree(c.get("/internal/payments").body());
    }

    private static final Set<String> IN_FLIGHT = Set.of("AUTHORIZING", "COMPENSATING", "CAPTURING", "VOIDING", "REFUNDING");

    private static long inFlight(JsonNode payments) {
        long n = 0;
        for (JsonNode p : payments) if (IN_FLIGHT.contains(p.get("status").asText())) n++;
        return n;
    }

    /** What the provider must show for a leg, given what we recorded. */
    private static boolean legAgrees(String ours, LedgerEntry psp) {
        AuthStatus s = psp == null ? AuthStatus.NOT_FOUND : psp.getStatus();
        return switch (ours) {
            case "AUTHORIZED" -> s == AuthStatus.AUTHORIZED;
            case "CAPTURED" -> s == AuthStatus.CAPTURED;
            case "REFUNDED" -> s == AuthStatus.REFUNDED;
            case "VOIDED" -> s == AuthStatus.VOIDED;
            // declined, or never sent because the breaker was open (possibly tombstoned later)
            case "DECLINED" -> s == AuthStatus.DECLINED || s == AuthStatus.NOT_FOUND || (s == AuthStatus.VOIDED && psp.getTombstone());
            default -> false; // PENDING / UNKNOWN after settling is itself a failure
        };
    }

    private static boolean paymentConsistent(String status, List<String> legs) {
        return switch (status) {
            case "AUTHORIZED", "CAPTURED", "VOIDED", "REFUNDED" -> legs.stream().allMatch(status::equals);
            case "FAILED" -> legs.stream().allMatch(l -> l.equals("VOIDED") || l.equals("DECLINED"));
            default -> false;
        };
    }

    private static Map<String, Object> audit(JsonNode payments, List<LedgerEntry> ledger) {
        Map<String, LedgerEntry> pspByKey = ledger.stream().collect(Collectors.toMap(LedgerEntry::getIdempotencyKey, e -> e));
        Set<String> ourKeys = new HashSet<>();
        Map<String, Integer> paymentsPerOrder = new HashMap<>();
        Map<String, Long> byStatus = new java.util.TreeMap<>();
        long stuck = 0, legMismatches = 0, inconsistentPayments = 0;
        long splitPayments = 0, compensationsRequired = 0, compensationsSucceeded = 0;
        long dbHeld = 0, dbCaptured = 0, dbRefunded = 0, dbVoided = 0;

        for (JsonNode p : payments) {
            String status = p.get("status").asText();
            byStatus.merge(status, 1L, Long::sum);
            paymentsPerOrder.merge(p.get("orderRef").asText(), 1, Integer::sum);
            if (IN_FLIGHT.contains(status)) stuck++;
            List<String> legStatuses = new ArrayList<>();
            for (JsonNode t : p.get("tenders")) {
                String key = t.get("pspKey").asText(), ls = t.get("status").asText();
                long amt = t.get("amountMinor").asLong();
                ourKeys.add(key);
                legStatuses.add(ls);
                if (!legAgrees(ls, pspByKey.get(key))) legMismatches++;
                switch (ls) {
                    case "AUTHORIZED" -> dbHeld += amt;
                    case "CAPTURED" -> dbCaptured += amt;
                    case "REFUNDED" -> dbRefunded += amt;
                    case "VOIDED" -> {
                        LedgerEntry e = pspByKey.get(key);
                        if (e != null && !e.getTombstone()) dbVoided += amt; // a real hold that was released
                    }
                    default -> { }
                }
            }
            if (!IN_FLIGHT.contains(status) && !paymentConsistent(status, legStatuses)) inconsistentPayments++;

            if (p.get("tenders").size() == 2) {
                splitPayments++;
                JsonNode gift = p.get("tenders").get(0);
                LedgerEntry g = pspByKey.get(gift.get("pspKey").asText());
                boolean giftWasCharged = g != null && !g.getTombstone() && g.getStatus() != AuthStatus.DECLINED;
                if ("FAILED".equals(status) && giftWasCharged) {
                    compensationsRequired++;
                    if (g.getStatus() == AuthStatus.VOIDED) compensationsSucceeded++;
                }
            }
        }

        // Provider-side truth.
        long pspHeld = 0, pspCaptured = 0, pspRefunded = 0, pspVoided = 0, orphanHolds = 0, dedupedRetries = 0;
        Map<String, Integer> livePerOrderMethod = new HashMap<>();
        for (LedgerEntry e : ledger) {
            if (e.getTombstone()) continue;
            dedupedRetries += Math.max(0, e.getAuthorizeCalls() - 1);
            switch (e.getStatus()) {
                case AUTHORIZED -> pspHeld += e.getAmountMinor();
                case CAPTURED -> pspCaptured += e.getAmountMinor();
                case REFUNDED -> pspRefunded += e.getAmountMinor();
                case VOIDED -> pspVoided += e.getAmountMinor();
                default -> { }
            }
            boolean charged = e.getStatus() == AuthStatus.AUTHORIZED || e.getStatus() == AuthStatus.CAPTURED
                    || e.getStatus() == AuthStatus.REFUNDED;
            if (charged) livePerOrderMethod.merge(e.getOrderRef() + "|" + e.getMethod(), 1, Integer::sum);
            boolean holdsMoney = e.getStatus() == AuthStatus.AUTHORIZED || e.getStatus() == AuthStatus.CAPTURED;
            if (holdsMoney && !ourKeys.contains(e.getIdempotencyKey())) orphanHolds++;
        }
        Set<String> doubleChargedOrders = livePerOrderMethod.entrySet().stream().filter(x -> x.getValue() > 1)
                .map(x -> x.getKey().substring(0, x.getKey().indexOf('|'))).collect(Collectors.toSet());
        long duplicatePayments = paymentsPerOrder.values().stream().mapToLong(v -> v - 1).sum();
        long pspApproved = pspHeld + pspCaptured + pspRefunded + pspVoided;
        long dbAccounted = dbHeld + dbCaptured + dbRefunded + dbVoided;

        Map<String, Object> db = new LinkedHashMap<>();
        db.put("payments", payments.size());
        db.put("byStatus", byStatus);
        db.put("duplicatePaymentsForSameOrder", duplicatePayments);
        db.put("stuckInFlight", stuck);

        Map<String, Object> psp = new LinkedHashMap<>();
        psp.put("authorizations", ledger.stream().filter(e -> !e.getTombstone()).count());
        psp.put("tombstones", ledger.stream().filter(LedgerEntry::getTombstone).count());
        psp.put("retriesDeduplicatedByIdempotencyKey", dedupedRetries);

        Map<String, Object> money = new LinkedHashMap<>();
        money.put("pspApprovedMinor", pspApproved);
        money.put("pspCapturedMinor", pspCaptured);
        money.put("pspRefundedMinor", pspRefunded);
        money.put("pspVoidedMinor", pspVoided);
        money.put("pspOpenHoldsMinor", pspHeld);
        money.put("ourAccountedMinor", dbAccounted);
        money.put("approvedMinusAccounted", pspApproved - dbAccounted);
        money.put("bucketsMatch", pspCaptured == dbCaptured && pspRefunded == dbRefunded
                && pspVoided == dbVoided && pspHeld == dbHeld);

        Map<String, Object> split = new LinkedHashMap<>();
        split.put("splitTenderPayments", splitPayments);
        split.put("giftCardChargedThenPaymentFailed", compensationsRequired);
        split.put("giftCardCompensated", compensationsSucceeded);

        long discrepancies = legMismatches + inconsistentPayments + orphanHolds + stuck;
        Map<String, Object> verdict = new LinkedHashMap<>();
        verdict.put("doubleChargedOrders", doubleChargedOrders.size());
        verdict.put("ledgerDiscrepancies", discrepancies);
        verdict.put("legMismatches", legMismatches);
        verdict.put("internallyInconsistentPayments", inconsistentPayments);
        verdict.put("orphanHoldsAtProvider", orphanHolds);
        verdict.put("compensationFailures", compensationsRequired - compensationsSucceeded);
        verdict.put("pass", doubleChargedOrders.isEmpty() && discrepancies == 0 && duplicatePayments == 0
                && compensationsRequired == compensationsSucceeded && pspApproved == dbAccounted);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("database", db);
        out.put("provider", psp);
        out.put("money", money);
        out.put("splitTender", split);
        out.put("verdict", verdict);
        return out;
    }

    private static void log(String fmt, Object... args) {
        System.out.printf("[chaos] " + fmt + "%n", args);
    }
}
