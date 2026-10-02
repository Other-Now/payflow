package com.payflow.psp;

import com.payflow.proto.AuthStatus;
import com.payflow.proto.AuthorizeRequest;
import com.payflow.proto.Empty;
import com.payflow.proto.FaultConfig;
import com.payflow.proto.Ledger;
import com.payflow.proto.LedgerEntry;
import com.payflow.proto.OperationRequest;
import com.payflow.proto.PaymentMethod;
import com.payflow.proto.PaymentProviderGrpc;
import com.payflow.proto.ProviderResult;
import com.payflow.proto.PspAdminGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A fake payment provider. Its ledger is the ground truth the chaos run checks
 * against: "was the customer charged twice" is answered here, not by asking
 * payment-service what it thinks happened.
 *
 * Every read and write of an {@link Auth} happens inside ConcurrentHashMap.compute
 * for its key, so per-key operations are serialised without extra locks.
 */
public final class MockPsp {

    static final class Auth {
        final String key;
        final PaymentMethod method;
        final String orderRef;
        final long amountMinor;
        final String providerRef = "psp_" + UUID.randomUUID().toString().substring(0, 12);
        final boolean tombstone;
        AuthStatus status;
        String declineReason = "";
        int authorizeCalls;

        Auth(String key, PaymentMethod method, String orderRef, long amountMinor, AuthStatus status, boolean tombstone) {
            this.key = key;
            this.method = method;
            this.orderRef = orderRef;
            this.amountMinor = amountMinor;
            this.status = status;
            this.tombstone = tombstone;
        }

        ProviderResult result() {
            return ProviderResult.newBuilder()
                    .setStatus(status).setProviderRef(providerRef).setDeclineReason(declineReason).build();
        }

        LedgerEntry entry() {
            return LedgerEntry.newBuilder()
                    .setIdempotencyKey(key).setMethod(method).setOrderRef(orderRef)
                    .setAmountMinor(amountMinor).setStatus(status)
                    .setAuthorizeCalls(authorizeCalls).setTombstone(tombstone).build();
        }
    }

    private enum Fault { NONE, UNAVAILABLE, TIMEOUT_BEFORE, TIMEOUT_AFTER }

    private static final ProviderResult NOT_FOUND =
            ProviderResult.newBuilder().setStatus(AuthStatus.NOT_FOUND).build();

    private final Map<String, Auth> ledger = new ConcurrentHashMap<>();
    private volatile FaultConfig faults = FaultConfig.getDefaultInstance();

    public final PaymentProviderGrpc.PaymentProviderImplBase provider = new Provider();
    public final PspAdminGrpc.PspAdminImplBase admin = new Admin();

    public void setFaults(FaultConfig f) {
        faults = f;
    }

    public Ledger ledger() {
        Ledger.Builder b = Ledger.newBuilder();
        for (String key : ledger.keySet()) {
            ledger.computeIfPresent(key, (k, a) -> {
                b.addEntries(a.entry());
                return a;
            });
        }
        return b.build();
    }

    /** Runs fn atomically for one key and returns the resulting state (or NOT_FOUND). */
    private ProviderResult atomically(String key, BiFunction<String, Auth, Auth> fn) {
        ProviderResult[] out = {NOT_FOUND};
        ledger.compute(key, (k, existing) -> {
            Auth a = fn.apply(k, existing);
            if (a != null) out[0] = a.result();
            return a;
        });
        return out[0];
    }

    // ---------------------------------------------------------------- provider

    private final class Provider extends PaymentProviderGrpc.PaymentProviderImplBase {

        @Override
        public void authorize(AuthorizeRequest r, StreamObserver<ProviderResult> out) {
            faulted(r.getMethod(), out, () -> {
                boolean[] amountMismatch = {false};
                ProviderResult res = atomically(r.getIdempotencyKey(), (k, existing) -> {
                    if (existing != null) {
                        existing.authorizeCalls++;
                        amountMismatch[0] = !existing.tombstone && existing.amountMinor != r.getAmountMinor();
                        return existing;
                    }
                    boolean decline = r.getToken().startsWith("tok_decline")
                            || ThreadLocalRandom.current().nextDouble() < faults.getDeclineRate();
                    Auth fresh = new Auth(k, r.getMethod(), r.getOrderRef(), r.getAmountMinor(),
                            decline ? AuthStatus.DECLINED : AuthStatus.AUTHORIZED, false);
                    if (decline) fresh.declineReason = "insufficient_funds";
                    fresh.authorizeCalls = 1;
                    return fresh;
                });
                if (amountMismatch[0]) {
                    // Same key, different request: a real PSP rejects this too.
                    throw Status.INVALID_ARGUMENT
                            .withDescription("idempotency key reused with a different amount").asRuntimeException();
                }
                return res;
            });
        }

        @Override
        public void capture(OperationRequest r, StreamObserver<ProviderResult> out) {
            transition(r, out, a -> {
                if (a.status == AuthStatus.AUTHORIZED) a.status = AuthStatus.CAPTURED;
            });
        }

        @Override
        public void refund(OperationRequest r, StreamObserver<ProviderResult> out) {
            transition(r, out, a -> {
                if (a.status == AuthStatus.CAPTURED) a.status = AuthStatus.REFUNDED;
            });
        }

        /**
         * Void on an unknown key writes a tombstone, so an Authorize that is
         * still in flight (or retried later) with that key can never place a hold.
         */
        @Override
        public void void_(OperationRequest r, StreamObserver<ProviderResult> out) {
            faulted(r.getMethod(), out, () -> atomically(r.getIdempotencyKey(), (k, existing) -> {
                if (existing == null) return new Auth(k, r.getMethod(), "", 0, AuthStatus.VOIDED, true);
                if (existing.status == AuthStatus.AUTHORIZED) existing.status = AuthStatus.VOIDED;
                return existing;
            }));
        }

        /** The reconciler's source of truth, so it is never faulted. */
        @Override
        public void getStatus(OperationRequest r, StreamObserver<ProviderResult> out) {
            out.onNext(atomically(r.getIdempotencyKey(), (k, existing) -> existing));
            out.onCompleted();
        }

        private void transition(OperationRequest r, StreamObserver<ProviderResult> out, Consumer<Auth> step) {
            faulted(r.getMethod(), out, () -> atomically(r.getIdempotencyKey(), (k, existing) -> {
                if (existing != null) step.accept(existing);
                return existing;
            }));
        }
    }

    /** Applies the configured fault around a side-effecting call. */
    private void faulted(PaymentMethod method, StreamObserver<ProviderResult> out, Supplier<ProviderResult> sideEffect) {
        FaultConfig f = faults;
        sleep(f.getLatencyMs());
        Fault fault = roll(f, method);
        try {
            if (fault == Fault.UNAVAILABLE) {
                throw Status.UNAVAILABLE.withDescription("injected").asRuntimeException();
            }
            if (fault == Fault.TIMEOUT_BEFORE) {
                sleep(f.getHangMs());
                throw Status.UNAVAILABLE.withDescription("injected: request lost").asRuntimeException();
            }
            ProviderResult result = sideEffect.get();
            if (fault == Fault.TIMEOUT_AFTER) sleep(f.getHangMs()); // committed, but the caller has timed out
            out.onNext(result);
            out.onCompleted();
        } catch (StatusRuntimeException e) {
            out.onError(e);
        }
    }

    private static Fault roll(FaultConfig f, PaymentMethod method) {
        if (f.getOnlyMethod() != PaymentMethod.PAYMENT_METHOD_UNSPECIFIED && f.getOnlyMethod() != method) {
            return Fault.NONE;
        }
        double x = ThreadLocalRandom.current().nextDouble();
        if ((x -= f.getUnavailableRate()) < 0) return Fault.UNAVAILABLE;
        if ((x -= f.getTimeoutBeforeRate()) < 0) return Fault.TIMEOUT_BEFORE;
        if ((x -= f.getTimeoutAfterRate()) < 0) return Fault.TIMEOUT_AFTER;
        return Fault.NONE;
    }

    private static void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ admin

    private final class Admin extends PspAdminGrpc.PspAdminImplBase {
        @Override
        public void setFaults(FaultConfig f, StreamObserver<Empty> out) {
            MockPsp.this.setFaults(f);
            out.onNext(Empty.getDefaultInstance());
            out.onCompleted();
        }

        @Override
        public void dumpLedger(Empty e, StreamObserver<Ledger> out) {
            out.onNext(ledger());
            out.onCompleted();
        }
    }
}
