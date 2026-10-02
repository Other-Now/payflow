package com.payflow.payment.domain;

/**
 * Payment lifecycle. "In flight" states mean work was started and may not have
 * finished; they are the only ones the reconciler ever looks at.
 *
 * <pre>
 * AUTHORIZING ──► AUTHORIZED ──► CAPTURING ──► CAPTURED ──► REFUNDING ──► REFUNDED
 *      │               └──────► VOIDING ───► VOIDED
 *      └──► COMPENSATING ──► FAILED
 * </pre>
 */
public enum PaymentStatus {
    AUTHORIZING(true),
    AUTHORIZED(false),
    COMPENSATING(true),
    FAILED(false),
    CAPTURING(true),
    CAPTURED(false),
    VOIDING(true),
    VOIDED(false),
    REFUNDING(true),
    REFUNDED(false);

    public final boolean inFlight;

    PaymentStatus(boolean inFlight) {
        this.inFlight = inFlight;
    }
}
