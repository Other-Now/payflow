package com.payflow.payment.grpc;

import com.payflow.payment.domain.Payment;
import com.payflow.proto.EligibilityGrpc;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.MethodDecision;
import com.payflow.proto.PaymentMethod;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class EligibilityClient {

    /** Eligibility couldn't be determined. Nothing was charged, so the client may simply retry. */
    public static class UnavailableException extends RuntimeException {
        public UnavailableException(Throwable cause) {
            super("eligibility service unavailable: " + cause.getMessage(), cause);
        }
    }

    private static final long DEADLINE_MS = 300;

    private final EligibilityGrpc.EligibilityBlockingStub stub;

    public EligibilityClient(@Qualifier("eligibility") ManagedChannel channel) {
        this.stub = EligibilityGrpc.newBlockingStub(channel);
    }

    public Map<PaymentMethod, MethodDecision> check(Payment p) {
        try {
            return stub.withDeadlineAfter(DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .getEligibleMethods(EligibilityRequest.newBuilder()
                            .setCountry(p.country()).setCurrency(p.currency()).setLob(p.lob())
                            .setDevice(p.device()).setAmountMinor(p.amountMinor()).build())
                    .getDecisionsList().stream()
                    .collect(Collectors.toMap(MethodDecision::getMethod, d -> d));
        } catch (StatusRuntimeException e) {
            throw new UnavailableException(e);
        }
    }
}
