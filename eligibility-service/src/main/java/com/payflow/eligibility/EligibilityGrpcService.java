package com.payflow.eligibility;

import com.payflow.proto.EligibilityGrpc;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.EligibilityResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Component;

@Component
public class EligibilityGrpcService extends EligibilityGrpc.EligibilityImplBase {

    private final EligibilityRules rules;

    public EligibilityGrpcService(EligibilityRules rules) {
        this.rules = rules;
    }

    @Override
    public void getEligibleMethods(EligibilityRequest r, StreamObserver<EligibilityResponse> out) {
        if (r.getAmountMinor() <= 0 || r.getCountry().length() != 2 || r.getCurrency().length() != 3) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("need amount_minor > 0, 2-letter country, 3-letter currency").asRuntimeException());
            return;
        }
        out.onNext(EligibilityResponse.newBuilder().addAllDecisions(rules.decide(r)).build());
        out.onCompleted();
    }
}
