package com.payflow.psp;

import com.payflow.proto.AuthStatus;
import com.payflow.proto.AuthorizeRequest;
import com.payflow.proto.OperationRequest;
import com.payflow.proto.PaymentMethod;
import com.payflow.proto.PaymentProviderGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockPspTest {

    MockPsp psp;
    Server server;
    ManagedChannel channel;
    PaymentProviderGrpc.PaymentProviderBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        psp = new MockPsp();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(psp.provider).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        stub = PaymentProviderGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    static AuthorizeRequest auth(String key, long amount) {
        return AuthorizeRequest.newBuilder().setIdempotencyKey(key).setMethod(PaymentMethod.CARD)
                .setAmountMinor(amount).setCurrency("USD").setToken("tok_ok").setOrderRef("o1").build();
    }

    static OperationRequest op(String key) {
        return OperationRequest.newBuilder().setIdempotencyKey(key).setMethod(PaymentMethod.CARD).build();
    }

    @Test
    void authorizeIsIdempotentOnKey() {
        var first = stub.authorize(auth("k1", 500));
        var replay = stub.authorize(auth("k1", 500));
        assertThat(replay.getProviderRef()).isEqualTo(first.getProviderRef());
        assertThat(psp.ledger().getEntriesList()).singleElement()
                .satisfies(e -> assertThat(e.getAuthorizeCalls()).isEqualTo(2));
    }

    @Test
    void keyReuseWithDifferentAmountIsRejected() {
        stub.authorize(auth("k1", 500));
        assertThatThrownBy(() -> stub.authorize(auth("k1", 900))).isInstanceOf(StatusRuntimeException.class);
    }

    @Test
    void voidOnUnknownKeyTombstonesIt() {
        assertThat(stub.void_(op("late")).getStatus()).isEqualTo(AuthStatus.VOIDED);
        // the authorize that was "in flight" arrives afterwards and cannot place a hold
        assertThat(stub.authorize(auth("late", 500)).getStatus()).isEqualTo(AuthStatus.VOIDED);
    }

    @Test
    void stateMachineOnlyMovesForward() {
        stub.authorize(auth("k1", 500));
        assertThat(stub.capture(op("k1")).getStatus()).isEqualTo(AuthStatus.CAPTURED);
        assertThat(stub.void_(op("k1")).getStatus()).isEqualTo(AuthStatus.CAPTURED); // too late to void
        assertThat(stub.refund(op("k1")).getStatus()).isEqualTo(AuthStatus.REFUNDED);
        assertThat(stub.refund(op("k1")).getStatus()).isEqualTo(AuthStatus.REFUNDED); // idempotent
        assertThat(stub.getStatus(op("nope")).getStatus()).isEqualTo(AuthStatus.NOT_FOUND);
    }
}
