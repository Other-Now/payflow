package com.payflow.payment.api;

import com.payflow.payment.domain.Payment;
import com.payflow.payment.saga.Reconciler;
import com.payflow.payment.saga.SagaStats;
import com.payflow.payment.store.PaymentStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Test/ops endpoints. In a real deployment these sit behind an internal-only listener. */
@RestController
@RequestMapping("/internal")
public class InternalController {

    private final PaymentStore store;
    private final SagaStats stats;
    private final Reconciler reconciler;

    public InternalController(PaymentStore store, SagaStats stats, Reconciler reconciler) {
        this.store = store;
        this.stats = stats;
        this.reconciler = reconciler;
    }

    @GetMapping("/payments")
    public List<Payment> all() {
        return store.scanAll();
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return stats.snapshot();
    }

    @PostMapping("/reconcile")
    public Map<String, Integer> reconcile() {
        return Map.of("resumed", reconciler.sweepOnce());
    }
}
