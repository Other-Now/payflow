package com.payflow.payment.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payment.PayflowProperties;
import com.payflow.payment.domain.Payment;
import com.payflow.payment.domain.PaymentStatus;
import com.payflow.payment.domain.Tender;
import com.payflow.payment.grpc.EligibilityClient;
import com.payflow.payment.saga.PaymentOrchestrator;
import com.payflow.payment.saga.SagaStats;
import com.payflow.payment.store.PaymentStore;
import com.payflow.payment.store.PaymentStore.IdempotencyRecord;
import com.payflow.proto.Device;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.MethodDecision;
import com.payflow.proto.PaymentMethod;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class PaymentService {

    public static class BadRequestException extends RuntimeException {
        public BadRequestException(String m) { super(m); }
    }

    /** 422: well-formed but not allowed (ineligible method, key reused with a different body). */
    public static class UnprocessableException extends RuntimeException {
        public UnprocessableException(String m) { super(m); }
    }

    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String id) { super("payment " + id + " not found"); }
    }

    public record Result(Payment payment, boolean replayed) {}

    private final PaymentStore store;
    private final EligibilityClient eligibility;
    private final PaymentOrchestrator saga;
    private final SagaStats stats;
    private final ObjectMapper json;
    private final boolean unsafeMode;

    public PaymentService(PaymentStore store, EligibilityClient eligibility, PaymentOrchestrator saga,
                          SagaStats stats, ObjectMapper json, PayflowProperties props) {
        this.store = store;
        this.eligibility = eligibility;
        this.saga = saga;
        this.stats = stats;
        this.json = json;
        this.unsafeMode = props.unsafeMode();
    }

    public Result create(String idempotencyKey, CreatePaymentRequest req) {
        validate(req);
        String hash = hash(req);
        if (!unsafeMode) {
            if (idempotencyKey == null || idempotencyKey.isBlank()) throw new BadRequestException("Idempotency-Key header is required");
            Optional<IdempotencyRecord> existing = store.findIdempotency(idempotencyKey);
            if (existing.isPresent()) return replay(existing.get(), hash);
        }

        Payment p = newPayment(req);
        checkEligibility(p);

        if (unsafeMode) {
            store.create(p);
        } else {
            Optional<IdempotencyRecord> raced = store.createWithIdempotencyKey(idempotencyKey, hash, p);
            if (raced.isPresent()) return replay(raced.get(), hash);
        }
        try {
            return new Result(saga.authorize(p), false);
        } catch (PaymentStore.StaleVersionException e) {
            stats.optimisticLockConflicts.incrementAndGet();
            return new Result(get(p.id()), false); // the reconciler took over; report what it did
        }
    }

    public Payment get(String id) {
        return store.get(id).orElseThrow(() -> new NotFoundException(id));
    }

    // ------------------------------------------------------------------------

    private Result replay(IdempotencyRecord rec, String hash) {
        if (!rec.requestHash().equals(hash)) {
            throw new UnprocessableException("Idempotency-Key was already used with a different request body");
        }
        stats.idempotentReplays.incrementAndGet();
        return new Result(get(rec.paymentId()), true);
    }

    private void checkEligibility(Payment p) {
        Map<PaymentMethod, MethodDecision> decisions = eligibility.check(p);
        List<String> denied = new ArrayList<>();
        for (Tender t : p.tenders()) {
            MethodDecision d = decisions.get(t.method());
            if (d == null || !d.getEligible()) {
                denied.add(t.method() + ": " + (d == null ? "not_offered" : d.getReason()));
            }
        }
        if (!denied.isEmpty()) throw new UnprocessableException("payment method not eligible: " + String.join(", ", denied));
    }

    /** Gift card first: if the card is then declined, the gift card is what gets compensated. */
    private static Payment newPayment(CreatePaymentRequest req) {
        String id = "pay_" + UUID.randomUUID().toString().replace("-", "");
        List<CreatePaymentRequest.TenderRequest> legs = new ArrayList<>(req.tenders());
        legs.sort(Comparator.comparing(t -> t.method() == PaymentMethod.GIFT_CARD ? 0 : 1));
        List<Tender> tenders = new ArrayList<>();
        for (int i = 0; i < legs.size(); i++) {
            var t = legs.get(i);
            tenders.add(new Tender(t.method(), t.amountMinor(), t.token(), Tender.Status.PENDING,
                    id + ":" + i, null, null));
        }
        Instant now = Instant.now();
        return new Payment(id, req.orderRef(), req.country(), req.currency(), req.lob(), req.device(),
                req.amountMinor(), PaymentStatus.AUTHORIZING, tenders, null, 0, now, now);
    }

    private static void validate(CreatePaymentRequest req) {
        if (req.lob() == LineOfBusiness.LOB_UNSPECIFIED || req.lob() == LineOfBusiness.UNRECOGNIZED
                || req.device() == Device.DEVICE_UNSPECIFIED || req.device() == Device.UNRECOGNIZED) {
            throw new BadRequestException("lob and device are required");
        }
        long sum = 0;
        for (var t : req.tenders()) {
            if (t.method() == PaymentMethod.PAYMENT_METHOD_UNSPECIFIED || t.method() == PaymentMethod.UNRECOGNIZED) {
                throw new BadRequestException("unknown payment method");
            }
            sum += t.amountMinor();
        }
        if (sum != req.amountMinor()) throw new BadRequestException("tender amounts must sum to amountMinor");
        if (req.tenders().stream().map(CreatePaymentRequest.TenderRequest::method).distinct().count() != req.tenders().size()) {
            throw new BadRequestException("at most one tender per payment method");
        }
    }

    private String hash(CreatePaymentRequest req) {
        try {
            byte[] body = json.writeValueAsBytes(req); // record component order is fixed
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
