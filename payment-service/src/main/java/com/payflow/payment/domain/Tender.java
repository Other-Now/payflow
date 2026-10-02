package com.payflow.payment.domain;

import com.payflow.proto.PaymentMethod;

/**
 * One leg of a (possibly split-tender) payment, e.g. "$50 on a gift card".
 *
 * @param pspKey the idempotency key this leg's Authorize uses at the provider.
 *               It is derived from the payment id, so every retry, every
 *               restart and the reconciler all address the same authorization.
 */
public record Tender(
        PaymentMethod method,
        long amountMinor,
        String token,
        Status status,
        String pspKey,
        String providerRef,
        String declineReason) {

    public enum Status {
        PENDING,     // not sent yet (or sent, and we crashed before recording the answer)
        UNKNOWN,     // sent, no answer (timeout): may or may not hold money
        AUTHORIZED,
        DECLINED,
        CAPTURED,
        VOIDED,
        REFUNDED
    }

    public Tender with(Status s) {
        return new Tender(method, amountMinor, token, s, pspKey, providerRef, declineReason);
    }

    public Tender with(Status s, String ref, String reason) {
        return new Tender(method, amountMinor, token, s, pspKey, ref == null || ref.isEmpty() ? providerRef : ref, reason);
    }

    public Tender withKey(String key) {
        return new Tender(method, amountMinor, token, status, key, providerRef, declineReason);
    }
}
