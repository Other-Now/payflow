package com.payflow.payment.api;

import com.payflow.payment.domain.Payment;
import com.payflow.payment.domain.PaymentStatus;
import com.payflow.payment.saga.PaymentOrchestrator;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.function.UnaryOperator;

/**
 * Status codes tell the client what to do next:
 * 201/200 done · 202 still in flight, poll or retry with the same key ·
 * 402 declined (nothing held) · 409 conflicting operation · 422 not allowed.
 */
@RestController
@RequestMapping("/v1/payments")
public class PaymentController {

    private final PaymentService payments;
    private final PaymentOrchestrator saga;

    public PaymentController(PaymentService payments, PaymentOrchestrator saga) {
        this.payments = payments;
        this.saga = saga;
    }

    @PostMapping
    public ResponseEntity<Payment> create(@RequestHeader(value = "Idempotency-Key", required = false) String key,
                                          @Valid @RequestBody CreatePaymentRequest req) {
        PaymentService.Result r = payments.create(key, req);
        Payment p = r.payment();
        HttpStatus code = p.status().inFlight ? HttpStatus.ACCEPTED
                : p.status() == PaymentStatus.FAILED ? HttpStatus.PAYMENT_REQUIRED
                : HttpStatus.CREATED;
        return ResponseEntity.status(code).header("Idempotent-Replayed", Boolean.toString(r.replayed())).body(p);
    }

    @GetMapping("/{id}")
    public Payment get(@PathVariable String id) {
        return payments.get(id);
    }

    @PostMapping("/{id}/capture")
    public ResponseEntity<Payment> capture(@PathVariable String id) {
        return act(id, saga::capture);
    }

    @PostMapping("/{id}/void")
    public ResponseEntity<Payment> voidPayment(@PathVariable String id) {
        return act(id, saga::voidPayment);
    }

    @PostMapping("/{id}/refund")
    public ResponseEntity<Payment> refund(@PathVariable String id) {
        return act(id, saga::refund);
    }

    private ResponseEntity<Payment> act(String id, UnaryOperator<Payment> op) {
        Payment p = op.apply(payments.get(id));
        return ResponseEntity.status(p.status().inFlight ? HttpStatus.ACCEPTED : HttpStatus.OK).body(p);
    }
}
