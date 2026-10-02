package com.payflow.payment.saga;

import com.payflow.payment.domain.Payment;
import com.payflow.payment.domain.PaymentStatus;
import com.payflow.payment.domain.Tender;
import com.payflow.payment.grpc.ProviderClient;
import com.payflow.payment.grpc.ProviderClient.Outcome;
import com.payflow.payment.store.PaymentStore;
import com.payflow.proto.AuthStatus;
import com.payflow.proto.ProviderResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiFunction;

import static com.payflow.payment.domain.PaymentStatus.*;

/**
 * The payment saga. Rules it never breaks:
 *
 * <ol>
 *   <li>Every state change is one optimistic-locked write of the payment item.
 *       If someone else (the reconciler) changed it first, we stop.</li>
 *   <li>A leg whose outcome is unknown is never assumed failed or succeeded.
 *       The payment stays in flight until the reconciler asks the provider.</li>
 *   <li>Split tender: if any leg is declined, every leg not provably declined
 *       is voided (compensation). Voiding a leg the provider never saw writes a
 *       tombstone, so a late or retried Authorize can't place a hold afterwards.</li>
 * </ol>
 */
@Component
public class PaymentOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(PaymentOrchestrator.class);

    /** Requested transition is not legal from the current state. */
    public static class IllegalTransitionException extends RuntimeException {
        public IllegalTransitionException(Payment p, String op) {
            super("cannot " + op + " a payment in state " + p.status());
        }
    }

    private final PaymentStore store;
    private final ProviderClient psp;
    private final SagaStats stats;

    public PaymentOrchestrator(PaymentStore store, ProviderClient psp, SagaStats stats) {
        this.store = store;
        this.psp = psp;
        this.stats = stats;
    }

    // ------------------------------------------------------------- authorize

    /** Authorizes legs in order (gift card first). Returns the last saved state. */
    public Payment authorize(Payment p) {
        for (int i = 0; i < p.tenders().size(); i++) {
            Tender t = p.tenders().get(i);
            if (t.status() != Tender.Status.PENDING) continue;

            Outcome o = psp.authorize(t.method(), t.pspKey(), t.amountMinor(), p.currency(), t.token(), p.orderRef());
            switch (o) {
                case Outcome.Answered a when a.result().getStatus() == AuthStatus.AUTHORIZED ->
                        p = store.save(p.withTender(i, x -> x.withKey(a.keyUsed())
                                .with(Tender.Status.AUTHORIZED, a.result().getProviderRef(), null)));
                case Outcome.Answered a -> {
                    // DECLINED, or VOIDED because the reconciler tombstoned this key: either way, no hold.
                    String reason = a.result().getStatus() == AuthStatus.DECLINED
                            ? a.result().getDeclineReason() : "authorization_" + a.result().getStatus().name().toLowerCase();
                    p = p.withTender(i, x -> x.with(Tender.Status.DECLINED, a.result().getProviderRef(), reason));
                    return compensate(p.failed(COMPENSATING, t.method() + ": " + reason));
                }
                case Outcome.NotSent n -> {
                    p = p.withTender(i, x -> x.with(Tender.Status.DECLINED, null, "provider_unavailable"));
                    return compensate(p.failed(COMPENSATING, t.method() + ": provider_unavailable"));
                }
                case Outcome.Unknown u -> {
                    stats.unknownOutcomes.incrementAndGet();
                    log.warn("payment {} leg {} outcome unknown ({}); leaving for reconciler", p.id(), i, u.why());
                    return store.save(p.withTender(i, x -> x.with(Tender.Status.UNKNOWN)));
                }
            }
        }
        return store.save(p.withStatus(AUTHORIZED));
    }

    /**
     * Rolls back: void every leg that isn't provably declined or already voided.
     * Saves COMPENSATING first, so a crash mid-way is finished by the reconciler.
     */
    Payment compensate(Payment p) {
        if (p.status() != COMPENSATING) p = p.withStatus(COMPENSATING);
        p = store.save(p);
        boolean done = true;
        for (int i = 0; i < p.tenders().size(); i++) {
            Tender t = p.tenders().get(i);
            if (t.status() == Tender.Status.DECLINED || t.status() == Tender.Status.VOIDED) continue;
            Outcome o = psp.voidAuth(t.method(), t.pspKey());
            if (o instanceof Outcome.Answered a
                    && (a.result().getStatus() == AuthStatus.VOIDED || a.result().getStatus() == AuthStatus.DECLINED)) {
                Tender.Status s = a.result().getStatus() == AuthStatus.VOIDED ? Tender.Status.VOIDED : Tender.Status.DECLINED;
                p = p.withTender(i, x -> x.with(s));
                if (t.status() == Tender.Status.AUTHORIZED) stats.compensatedLegs.incrementAndGet();
            } else {
                done = false;
            }
        }
        return store.save(done ? p.withStatus(FAILED) : p);
    }

    // ---------------------------------------------- capture / void / refund

    public Payment capture(Payment p) {
        return settle(p, "capture", AUTHORIZED, CAPTURING, CAPTURED,
                Tender.Status.AUTHORIZED, Tender.Status.CAPTURED, AuthStatus.CAPTURED, psp::capture);
    }

    public Payment voidPayment(Payment p) {
        return settle(p, "void", AUTHORIZED, VOIDING, VOIDED,
                Tender.Status.AUTHORIZED, Tender.Status.VOIDED, AuthStatus.VOIDED, psp::voidAuth);
    }

    public Payment refund(Payment p) {
        return settle(p, "refund", CAPTURED, REFUNDING, REFUNDED,
                Tender.Status.CAPTURED, Tender.Status.REFUNDED, AuthStatus.REFUNDED, psp::refund);
    }

    /**
     * Shared shape of capture/void/refund: claim the payment by moving it to the
     * in-flight state (the optimistic lock makes capture-vs-void races have
     * exactly one winner), then apply the op to every leg. The provider ops are
     * idempotent by state, so re-driving a half-finished payment is always safe.
     */
    private Payment settle(Payment p, String op, PaymentStatus from, PaymentStatus during, PaymentStatus to,
                           Tender.Status legFrom, Tender.Status legTo, AuthStatus pspTo,
                           BiFunction<com.payflow.proto.PaymentMethod, String, Outcome> call) {
        if (p.status() == to) return p;                       // replay of a finished request
        if (p.status() == from) p = store.save(p.withStatus(during));
        else if (p.status() != during) throw new IllegalTransitionException(p, op);

        boolean done = true;
        for (int i = 0; i < p.tenders().size(); i++) {
            Tender t = p.tenders().get(i);
            if (t.status() != legFrom) continue;
            Optional<ProviderResult> r = answered(call.apply(t.method(), t.pspKey()));
            if (r.isPresent() && r.get().getStatus() == pspTo) {
                p = p.withTender(i, x -> x.with(legTo));
            } else {
                done = false;
            }
        }
        return store.save(done ? p.withStatus(to) : p);
    }

    // ------------------------------------------------------------- recovery

    /**
     * Finish a payment that was left in flight (crash, timeout, lost race).
     * Called only by the reconciler on payments idle for longer than any live
     * request could take.
     */
    public Payment resume(Payment p) {
        return switch (p.status()) {
            case AUTHORIZING -> resolveAuthorizing(p);
            case COMPENSATING -> compensate(p);
            case CAPTURING -> capture(p);
            case VOIDING -> voidPayment(p);
            case REFUNDING -> refund(p);
            default -> p;
        };
    }

    /**
     * Ask the provider what really happened to every unresolved leg. If every
     * leg turns out authorized, roll forward; otherwise roll back. A PENDING leg
     * might have been sent just before a crash, so it's asked about too.
     */
    private Payment resolveAuthorizing(Payment p) {
        for (int i = 0; i < p.tenders().size(); i++) {
            Tender t = p.tenders().get(i);
            if (t.status() != Tender.Status.PENDING && t.status() != Tender.Status.UNKNOWN) continue;
            Optional<ProviderResult> r = answered(psp.status(t.method(), t.pspKey()));
            if (r.isEmpty()) return p; // provider unreachable: try again next sweep
            Tender.Status s = switch (r.get().getStatus()) {
                case AUTHORIZED -> Tender.Status.AUTHORIZED;
                case DECLINED -> Tender.Status.DECLINED;
                case VOIDED -> Tender.Status.VOIDED;
                default -> t.status(); // NOT_FOUND: never landed (compensation tombstones it)
            };
            String ref = r.get().getProviderRef();
            p = p.withTender(i, x -> x.with(s, ref, x.declineReason()));
        }
        stats.reconciledAuthorizations.incrementAndGet();
        if (p.allTenders(Tender.Status.AUTHORIZED)) return store.save(p.withStatus(AUTHORIZED));
        return compensate(p.failed(COMPENSATING, "rolled_back_by_reconciler"));
    }

    private static Optional<ProviderResult> answered(Outcome o) {
        return o instanceof Outcome.Answered a ? Optional.of(a.result()) : Optional.empty();
    }
}
