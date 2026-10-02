package com.payflow.payment.saga;

import com.payflow.payment.PayflowProperties;
import com.payflow.payment.domain.Payment;
import com.payflow.payment.store.PaymentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Finishes payments left in flight: provider timeouts, a crash mid-saga, a
 * client that went away. It runs in every replica; two replicas picking the
 * same payment is safe because every write is optimistic-locked, so one of
 * them simply loses with a StaleVersionException.
 */
@Component
public class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    private final PaymentStore store;
    private final PaymentOrchestrator saga;
    private final SagaStats stats;
    private final long staleAfterMs;

    public Reconciler(PaymentStore store, PaymentOrchestrator saga, SagaStats stats, PayflowProperties props) {
        this.store = store;
        this.saga = saga;
        this.stats = stats;
        this.staleAfterMs = props.recon().staleAfterMs();
    }

    @Scheduled(fixedDelayString = "${payflow.recon.interval-ms}", initialDelayString = "${payflow.recon.interval-ms}")
    public void sweep() {
        try {
            sweepOnce();
        } catch (RuntimeException e) {
            log.warn("reconciler sweep failed: {}", e.toString());
        }
    }

    public int sweepOnce() {
        Instant cutoff = Instant.now().minusMillis(staleAfterMs);
        int resumed = 0;
        for (String id : store.findStaleInFlight(cutoff, 100)) {
            // The index is eventually consistent: re-read and re-check before acting.
            Optional<Payment> p = store.get(id);
            if (p.isEmpty() || !p.get().status().inFlight || p.get().updatedAt().isAfter(cutoff)) continue;
            try {
                saga.resume(p.get());
                stats.reconcilerResumes.incrementAndGet();
                resumed++;
            } catch (PaymentStore.StaleVersionException e) {
                stats.optimisticLockConflicts.incrementAndGet();
            } catch (RuntimeException e) {
                log.warn("resume of {} failed: {}", id, e.toString());
            }
        }
        return resumed;
    }
}
