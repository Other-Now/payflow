package com.payflow.eligibility;

import com.payflow.proto.Device;
import com.payflow.proto.EligibilityGrpc;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.MethodDecision;
import com.payflow.proto.PaymentMethod;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real gRPC round trips against the rules in application.yml. */
@SpringBootTest(properties = "payflow.grpc.port=0")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EligibilityServiceTest {

    @Autowired
    GrpcServerLifecycle server;

    ManagedChannel channel;
    EligibilityGrpc.EligibilityBlockingStub stub;

    @BeforeAll
    void connect() {
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        stub = EligibilityGrpc.newBlockingStub(channel);
    }

    @AfterAll
    void close() {
        channel.shutdownNow();
    }

    static EligibilityRequest.Builder us() {
        return EligibilityRequest.newBuilder().setCountry("US").setCurrency("USD")
                .setLob(LineOfBusiness.HOTEL).setAmountMinor(25_000).setDevice(Device.IOS);
    }

    Map<PaymentMethod, MethodDecision> decide(EligibilityRequest.Builder r) {
        return stub.getEligibleMethods(r.build()).getDecisionsList().stream()
                .collect(Collectors.toMap(MethodDecision::getMethod, d -> d));
    }

    @Test
    void usHotelOnIosGetsEverything() {
        assertThat(decide(us()).values()).hasSize(3).allMatch(MethodDecision::getEligible);
    }

    @Test
    void giftCardsCannotBuyFlights() {
        var d = decide(us().setLob(LineOfBusiness.AIR));
        assertThat(d.get(PaymentMethod.GIFT_CARD).getReason()).isEqualTo("lob_not_supported");
        assertThat(d.get(PaymentMethod.CARD).getEligible()).isTrue();
    }

    @Test
    void walletIsDeviceAndAmountGated() {
        assertThat(decide(us().setDevice(Device.ANDROID)).get(PaymentMethod.WALLET).getReason())
                .isEqualTo("device_not_supported");
        assertThat(decide(us().setAmountMinor(500_001)).get(PaymentMethod.WALLET).getReason())
                .isEqualTo("amount_over_limit");
    }

    @Test
    void countryAndCurrencyRules() {
        assertThat(decide(us().setCountry("KP")).get(PaymentMethod.CARD).getReason()).isEqualTo("country_blocked");
        var eur = decide(us().setCountry("DE").setCurrency("EUR"));
        assertThat(eur.get(PaymentMethod.GIFT_CARD).getReason()).isEqualTo("currency_not_supported");
        assertThat(eur.get(PaymentMethod.WALLET).getReason()).isEqualTo("country_not_supported");
        assertThat(eur.get(PaymentMethod.CARD).getEligible()).isTrue();
    }

    @Test
    void malformedRequestIsInvalidArgument() {
        assertThatThrownBy(() -> stub.getEligibleMethods(us().setAmountMinor(0).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
