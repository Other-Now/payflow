package com.payflow.payment.domain;

import com.payflow.proto.Device;
import com.payflow.proto.LineOfBusiness;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * A payment and all its tenders, stored as ONE DynamoDB item so every state
 * change (payment status + leg statuses) is a single conditional write.
 *
 * @param version optimistic-lock counter; a save only succeeds if the stored
 *                version is still the one this copy was read at.
 */
public record Payment(
        String id,
        String orderRef,
        String country,
        String currency,
        LineOfBusiness lob,
        Device device,
        long amountMinor,
        PaymentStatus status,
        List<Tender> tenders,
        String failureReason,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    public Payment {
        tenders = List.copyOf(tenders);
    }

    public Payment withStatus(PaymentStatus s) {
        return new Payment(id, orderRef, country, currency, lob, device, amountMinor, s, tenders,
                failureReason, version, createdAt, updatedAt);
    }

    public Payment failed(PaymentStatus s, String reason) {
        return new Payment(id, orderRef, country, currency, lob, device, amountMinor, s, tenders,
                reason, version, createdAt, updatedAt);
    }

    public Payment withTender(int i, UnaryOperator<Tender> change) {
        List<Tender> next = new ArrayList<>(tenders);
        next.set(i, change.apply(next.get(i)));
        return new Payment(id, orderRef, country, currency, lob, device, amountMinor, status, next,
                failureReason, version, createdAt, updatedAt);
    }

    /** The copy that gets written: next version, fresh timestamp. */
    public Payment nextVersion(Instant now) {
        return new Payment(id, orderRef, country, currency, lob, device, amountMinor, status, tenders,
                failureReason, version + 1, createdAt, now);
    }

    public boolean allTenders(Tender.Status s) {
        return tenders.stream().allMatch(t -> t.status() == s);
    }
}
