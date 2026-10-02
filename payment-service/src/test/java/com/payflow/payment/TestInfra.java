package com.payflow.payment;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.payflow.proto.EligibilityGrpc;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.EligibilityResponse;
import com.payflow.proto.MethodDecision;
import com.payflow.proto.PaymentMethod;
import com.payflow.psp.MockPsp;
import com.payflow.psp.MockPspServer;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.springframework.test.context.DynamicPropertyRegistry;

import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Everything payment-service talks to, started once per JVM and in-process:
 * DynamoDB Local (no Docker needed), the real mock PSP over gRPC, and a fake
 * eligibility service whose answers a test can change.
 */
public final class TestInfra {

    public static final MockPsp PSP = new MockPsp();
    /** Methods the fake eligibility service currently denies. */
    public static final Set<PaymentMethod> DENIED = ConcurrentHashMap.newKeySet();

    private static int dynamoPort, pspPort, eligibilityPort;
    private static boolean started;

    private TestInfra() {}

    public static synchronized void register(DynamicPropertyRegistry r) {
        start();
        r.add("payflow.dynamo.endpoint", () -> "http://localhost:" + dynamoPort);
        r.add("payflow.psp-target", () -> "localhost:" + pspPort);
        r.add("payflow.eligibility-target", () -> "localhost:" + eligibilityPort);
    }

    private static void start() {
        if (started) return;
        try {
            dynamoPort = freePort();
            DynamoDBProxyServer dynamo = ServerRunner.createServerFromCommandLineArgs(
                    new String[]{"-inMemory", "-port", Integer.toString(dynamoPort)});
            dynamo.start();

            Server psp = MockPspServer.start(PSP, 0);
            pspPort = psp.getPort();

            Server elig = NettyServerBuilder.forPort(0).addService(new FakeEligibility()).build().start();
            eligibilityPort = elig.getPort();
            started = true;
        } catch (Exception e) {
            throw new IllegalStateException("could not start test infrastructure", e);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static final class FakeEligibility extends EligibilityGrpc.EligibilityImplBase {
        @Override
        public void getEligibleMethods(EligibilityRequest r, StreamObserver<EligibilityResponse> out) {
            EligibilityResponse.Builder b = EligibilityResponse.newBuilder();
            for (PaymentMethod m : new PaymentMethod[]{PaymentMethod.CARD, PaymentMethod.WALLET, PaymentMethod.GIFT_CARD}) {
                boolean ok = !DENIED.contains(m);
                b.addDecisions(MethodDecision.newBuilder().setMethod(m).setEligible(ok).setReason(ok ? "" : "denied_by_test"));
            }
            out.onNext(b.build());
            out.onCompleted();
        }
    }
}
