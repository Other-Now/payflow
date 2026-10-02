package com.payflow.payment.grpc;

import com.payflow.payment.PayflowProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcConfig {

    @Bean(destroyMethod = "shutdownNow")
    @Qualifier("eligibility")
    ManagedChannel eligibilityChannel(PayflowProperties props) {
        return connectEagerly(ManagedChannelBuilder.forTarget(props.eligibilityTarget()).usePlaintext().build());
    }

    @Bean(destroyMethod = "shutdownNow")
    @Qualifier("psp")
    ManagedChannel pspChannel(PayflowProperties props) {
        return connectEagerly(ManagedChannelBuilder.forTarget(props.pspTarget()).usePlaintext().build());
    }

    /**
     * Channels connect lazily by default, so the first call after a deploy pays
     * for DNS + TCP + HTTP/2 setup inside its deadline (and failed the 300 ms
     * eligibility deadline in tests). Start connecting at boot instead.
     */
    private static ManagedChannel connectEagerly(ManagedChannel ch) {
        ch.getState(true);
        return ch;
    }
}
