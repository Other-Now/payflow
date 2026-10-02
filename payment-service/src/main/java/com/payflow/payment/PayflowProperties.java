package com.payflow.payment;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("payflow")
public record PayflowProperties(
        String eligibilityTarget,
        String pspTarget,
        Dynamo dynamo,
        Psp psp,
        Recon recon,
        /* Negative control for the chaos run: no API idempotency and a fresh PSP key per attempt. */
        boolean unsafeMode) {

    public record Dynamo(String endpoint, String region, String table, boolean createTable) {}

    public record Psp(long deadlineMs, int maxAttempts, long backoffMs,
                      int breakerWindow, int breakerFailurePercent, long breakerOpenMs) {}

    /** In-flight payments untouched for staleAfterMs are handed to the reconciler. */
    public record Recon(long intervalMs, long staleAfterMs) {}
}
