package com.payflow.payment.saga;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Counters the chaos harness reads from /internal/stats. */
@Component
public class SagaStats {
    public final AtomicLong unknownOutcomes = new AtomicLong();
    public final AtomicLong compensatedLegs = new AtomicLong();
    public final AtomicLong reconciledAuthorizations = new AtomicLong();
    public final AtomicLong reconcilerResumes = new AtomicLong();
    public final AtomicLong idempotentReplays = new AtomicLong();
    public final AtomicLong optimisticLockConflicts = new AtomicLong();

    public Map<String, Long> snapshot() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("unknownOutcomes", unknownOutcomes.get());
        m.put("compensatedLegs", compensatedLegs.get());
        m.put("reconciledAuthorizations", reconciledAuthorizations.get());
        m.put("reconcilerResumes", reconcilerResumes.get());
        m.put("idempotentReplays", idempotentReplays.get());
        m.put("optimisticLockConflicts", optimisticLockConflicts.get());
        return m;
    }
}
