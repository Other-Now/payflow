package com.payflow.payment;

import com.payflow.payment.api.CreatePaymentRequest;
import com.payflow.payment.api.CreatePaymentRequest.TenderRequest;
import com.payflow.payment.domain.Payment;
import com.payflow.payment.domain.PaymentStatus;
import com.payflow.payment.domain.Tender;
import com.payflow.payment.grpc.ProviderClient;
import com.payflow.payment.saga.Reconciler;
import com.payflow.payment.store.PaymentStore;
import com.payflow.proto.AuthStatus;
import com.payflow.proto.AuthorizeRequest;
import com.payflow.proto.Device;
import com.payflow.proto.FaultConfig;
import com.payflow.proto.LedgerEntry;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.PaymentMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end through HTTP -> saga -> gRPC -> mock PSP, with state in DynamoDB
 * Local. Assertions check BOTH what payment-service reports and what the
 * provider's own ledger says, because the second one is where money lives.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "payflow.psp.deadline-ms=300",
        "payflow.psp.breaker-window=10",
        "payflow.psp.breaker-open-ms=60000",
        "payflow.recon.interval-ms=3600000",   // tests drive the reconciler explicitly
        "payflow.recon.stale-after-ms=0",
})
class PaymentFlowTest {

    static final long HANG_MS = 700; // > deadline, so the client gives up

    @DynamicPropertySource
    static void infra(DynamicPropertyRegistry r) {
        TestInfra.register(r);
    }

    @Autowired TestRestTemplate http;
    @Autowired Reconciler reconciler;
    @Autowired PaymentStore store;
    @Autowired ProviderClient providers;

    @BeforeEach
    void reset() {
        TestInfra.PSP.setFaults(FaultConfig.getDefaultInstance());
        TestInfra.DENIED.clear();
        for (PaymentMethod m : List.of(PaymentMethod.CARD, PaymentMethod.WALLET, PaymentMethod.GIFT_CARD)) {
            providers.breaker(m).reset();
        }
    }

    // ------------------------------------------------------------------ happy path

    @Test
    void authorizeCaptureRefund() {
        String order = order();
        var created = post(UUID.randomUUID().toString(), cardOnly(order, "tok_visa"));
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String id = created.getBody().id();

        assertThat(action(id, "capture").getBody().status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(action(id, "capture").getStatusCode()).isEqualTo(HttpStatus.OK); // idempotent
        assertThat(action(id, "refund").getBody().status()).isEqualTo(PaymentStatus.REFUNDED);
        assertThat(psp(order)).singleElement().satisfies(e -> assertThat(e.getStatus()).isEqualTo(AuthStatus.REFUNDED));
    }

    // ----------------------------------------------------------------- idempotency

    @Test
    void sameKeyReplaysTheSamePayment_differentBodyIsRejected() {
        String order = order(), key = UUID.randomUUID().toString();
        var first = post(key, cardOnly(order, "tok_visa"));
        var second = post(key, cardOnly(order, "tok_visa"));
        assertThat(second.getBody().id()).isEqualTo(first.getBody().id());
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");

        var reused = http.postForEntity("/v1/payments", entity(key, cardOnly(order, "tok_other")), String.class);
        assertThat(reused.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(psp(order)).hasSize(1);
    }

    @Test
    void concurrentDuplicatesCreateOnePaymentAndOneHold() throws Exception {
        String order = order(), key = UUID.randomUUID().toString();
        List<Callable<ResponseEntity<Payment>>> calls = new ArrayList<>();
        for (int i = 0; i < 16; i++) calls.add(() -> post(key, cardOnly(order, "tok_visa")));
        List<String> ids = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(16)) {
            for (Future<ResponseEntity<Payment>> f : pool.invokeAll(calls)) ids.add(f.get().getBody().id());
        }
        assertThat(ids).containsOnly(ids.get(0));
        assertThat(psp(order)).hasSize(1);
    }

    // ---------------------------------------------------------------- split tender

    @Test
    void splitTender_cardDeclined_giftCardIsCompensated() {
        String order = order();
        var r = post(UUID.randomUUID().toString(), split(order, "tok_decline_card"));
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.PAYMENT_REQUIRED);
        Payment p = r.getBody();
        assertThat(p.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(p.tenders().get(0).method()).isEqualTo(PaymentMethod.GIFT_CARD); // charged first
        assertThat(p.tenders()).extracting(Tender::status).containsExactly(Tender.Status.VOIDED, Tender.Status.DECLINED);
        assertThat(psp(order)).extracting(LedgerEntry::getStatus)
                .containsExactlyInAnyOrder(AuthStatus.VOIDED, AuthStatus.DECLINED);
    }

    @Test
    void splitTender_happyPathCapturesBothLegs() {
        String order = order();
        String id = post(UUID.randomUUID().toString(), split(order, "tok_visa")).getBody().id();
        assertThat(action(id, "capture").getBody().status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(psp(order)).extracting(LedgerEntry::getStatus).containsOnly(AuthStatus.CAPTURED).hasSize(2);
    }

    // ------------------------------------------------------------ unknown outcomes

    @Test
    void timeoutAfterProviderCommitted_isResolvedToAuthorized_withOneHold() {
        TestInfra.PSP.setFaults(FaultConfig.newBuilder().setTimeoutAfterRate(1).setHangMs(HANG_MS).build());
        String order = order();
        var r = post(UUID.randomUUID().toString(), cardOnly(order, "tok_visa"));
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(r.getBody().tenders().get(0).status()).isEqualTo(Tender.Status.UNKNOWN);

        // three attempts reached the provider, but the shared key made them one hold
        assertThat(psp(order)).singleElement().satisfies(e -> {
            assertThat(e.getAuthorizeCalls()).isEqualTo(3);
            assertThat(e.getStatus()).isEqualTo(AuthStatus.AUTHORIZED);
        });

        TestInfra.PSP.setFaults(FaultConfig.getDefaultInstance());
        reconciler.sweepOnce();
        assertThat(get(r.getBody().id()).status()).isEqualTo(PaymentStatus.AUTHORIZED);
    }

    @Test
    void lostRequest_isRolledBack_andTheKeyIsTombstoned() {
        TestInfra.PSP.setFaults(FaultConfig.newBuilder().setTimeoutBeforeRate(1).setHangMs(HANG_MS).build());
        String order = order();
        var r = post(UUID.randomUUID().toString(), cardOnly(order, "tok_visa"));
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(psp(order)).isEmpty();

        TestInfra.PSP.setFaults(FaultConfig.getDefaultInstance());
        reconciler.sweepOnce();
        Payment p = get(r.getBody().id());
        assertThat(p.status()).isEqualTo(PaymentStatus.FAILED);

        // a straggler Authorize with the same key arrives after the rollback: no hold
        var late = TestInfra.PSP.ledger().getEntriesList().stream()
                .filter(e -> e.getIdempotencyKey().equals(p.tenders().get(0).pspKey())).findFirst().orElseThrow();
        assertThat(late.getTombstone()).isTrue();
        assertThat(late.getStatus()).isEqualTo(AuthStatus.VOIDED);
    }

    // -------------------------------------------------------------- crash recovery

    @Test
    void crashAfterProviderAuthorizedButBeforeSave_rollsForwardWhenAllLegsLanded() {
        String order = order();
        Payment p = seedInFlight(order);
        authorizeDirectly(p, 0);
        authorizeDirectly(p, 1);
        reconciler.sweepOnce();
        assertThat(get(p.id()).status()).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(psp(order)).extracting(LedgerEntry::getStatus).containsOnly(AuthStatus.AUTHORIZED);
    }

    @Test
    void crashMidSaga_rollsBackWhenOnlySomeLegsLanded() {
        String order = order();
        Payment p = seedInFlight(order);
        authorizeDirectly(p, 0); // gift card landed, process died before the card leg
        reconciler.sweepOnce();
        assertThat(get(p.id()).status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(psp(order)).extracting(LedgerEntry::getStatus).containsOnly(AuthStatus.VOIDED);
    }

    // ------------------------------------------------------------------ concurrency

    @Test
    void captureVsVoidRace_hasExactlyOneWinner() throws Exception {
        for (int round = 0; round < 5; round++) {
            String order = order();
            String id = post(UUID.randomUUID().toString(), cardOnly(order, "tok_visa")).getBody().id();
            List<Callable<ResponseEntity<String>>> calls = List.of(
                    () -> http.postForEntity("/v1/payments/" + id + "/capture", null, String.class),
                    () -> http.postForEntity("/v1/payments/" + id + "/void", null, String.class));
            List<HttpStatus> codes = new ArrayList<>();
            try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
                for (var f : pool.invokeAll(calls)) codes.add((HttpStatus) f.get().getStatusCode());
            }
            PaymentStatus end = get(id).status();
            assertThat(end).isIn(PaymentStatus.CAPTURED, PaymentStatus.VOIDED);
            assertThat(codes).contains(HttpStatus.OK);
            // whatever we say, the provider agrees
            AuthStatus expected = end == PaymentStatus.CAPTURED ? AuthStatus.CAPTURED : AuthStatus.VOIDED;
            assertThat(psp(order)).singleElement().satisfies(e -> assertThat(e.getStatus()).isEqualTo(expected));
        }
    }

    @Test
    void staleWriteIsRejectedByTheVersionCheck() {
        Payment p = seedInFlight(order());
        store.save(p); // someone else moved it to version 1
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> store.save(p))
                .isInstanceOf(PaymentStore.StaleVersionException.class);
    }

    // -------------------------------------------------------------------- resilience

    @Test
    void openCircuitFailsFastWithoutUnknowns_andOnlyForThatProvider() {
        TestInfra.PSP.setFaults(FaultConfig.newBuilder().setUnavailableRate(1).setOnlyMethod(PaymentMethod.WALLET).build());
        // UNAVAILABLE after send is still "unknown"; it trips the breaker
        for (int i = 0; i < 4; i++) post(UUID.randomUUID().toString(), walletOnly(order()));
        assertThat(providers.breaker(PaymentMethod.WALLET).getState().name()).isEqualTo("OPEN");

        String order = order();
        var r = post(UUID.randomUUID().toString(), walletOnly(order));
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.PAYMENT_REQUIRED);
        assertThat(r.getBody().tenders().get(0).declineReason()).isEqualTo("provider_unavailable");
        assertThat(psp(order).stream().filter(e -> !e.getTombstone())).isEmpty();

        assertThat(post(UUID.randomUUID().toString(), cardOnly(order(), "tok_visa")).getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void ineligibleMethodIsRejectedBeforeAnyMoneyMoves() {
        TestInfra.DENIED.add(PaymentMethod.GIFT_CARD);
        String order = order();
        var r = http.postForEntity("/v1/payments", entity(UUID.randomUUID().toString(), split(order, "tok_visa")), String.class);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(r.getBody()).contains("GIFT_CARD");
        assertThat(psp(order)).isEmpty();
    }

    // ----------------------------------------------------------------------- helpers

    static String order() {
        return "ord_" + UUID.randomUUID();
    }

    static CreatePaymentRequest cardOnly(String order, String token) {
        return new CreatePaymentRequest(order, "US", "USD", LineOfBusiness.HOTEL, Device.WEB, 20_000,
                List.of(new TenderRequest(PaymentMethod.CARD, 20_000, token)));
    }

    static CreatePaymentRequest walletOnly(String order) {
        return new CreatePaymentRequest(order, "US", "USD", LineOfBusiness.HOTEL, Device.IOS, 20_000,
                List.of(new TenderRequest(PaymentMethod.WALLET, 20_000, "tok_wallet")));
    }

    /** Card listed first on purpose: the service must still charge the gift card first. */
    static CreatePaymentRequest split(String order, String cardToken) {
        return new CreatePaymentRequest(order, "US", "USD", LineOfBusiness.HOTEL, Device.WEB, 25_000,
                List.of(new TenderRequest(PaymentMethod.CARD, 20_000, cardToken),
                        new TenderRequest(PaymentMethod.GIFT_CARD, 5_000, "tok_gc")));
    }

    HttpEntity<CreatePaymentRequest> entity(String key, CreatePaymentRequest body) {
        HttpHeaders h = new HttpHeaders();
        h.set("Idempotency-Key", key);
        return new HttpEntity<>(body, h);
    }

    ResponseEntity<Payment> post(String key, CreatePaymentRequest body) {
        return http.postForEntity("/v1/payments", entity(key, body), Payment.class);
    }

    ResponseEntity<Payment> action(String id, String op) {
        return http.postForEntity("/v1/payments/" + id + "/" + op, null, Payment.class);
    }

    Payment get(String id) {
        return http.getForObject("/v1/payments/" + id, Payment.class);
    }

    static List<LedgerEntry> psp(String order) {
        return TestInfra.PSP.ledger().getEntriesList().stream().filter(e -> e.getOrderRef().equals(order)).toList();
    }

    /** A split payment stored as AUTHORIZING with both legs PENDING: what a crash right after creation leaves. */
    Payment seedInFlight(String order) {
        String id = "pay_seed_" + UUID.randomUUID().toString().replace("-", "");
        Instant old = Instant.now().minusSeconds(60);
        Payment p = new Payment(id, order, "US", "USD", LineOfBusiness.HOTEL, Device.WEB, 25_000,
                PaymentStatus.AUTHORIZING,
                List.of(new Tender(PaymentMethod.GIFT_CARD, 5_000, "tok_gc", Tender.Status.PENDING, id + ":0", null, null),
                        new Tender(PaymentMethod.CARD, 20_000, "tok_visa", Tender.Status.PENDING, id + ":1", null, null)),
                null, 0, old, old);
        store.create(p);
        return p;
    }

    /** Simulates the provider having processed a leg whose answer we never recorded. */
    static void authorizeDirectly(Payment p, int leg) {
        Tender t = p.tenders().get(leg);
        TestInfra.PSP.provider.authorize(AuthorizeRequest.newBuilder()
                        .setIdempotencyKey(t.pspKey()).setMethod(t.method()).setAmountMinor(t.amountMinor())
                        .setCurrency("USD").setToken(t.token()).setOrderRef(p.orderRef()).build(),
                new io.grpc.stub.StreamObserver<>() {
                    public void onNext(com.payflow.proto.ProviderResult v) { }
                    public void onError(Throwable t) { throw new AssertionError(t); }
                    public void onCompleted() { }
                });
    }
}
