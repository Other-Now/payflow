package com.payflow.payment.grpc;

import com.payflow.payment.PayflowProperties;
import com.payflow.proto.AuthorizeRequest;
import com.payflow.proto.OperationRequest;
import com.payflow.proto.PaymentMethod;
import com.payflow.proto.PaymentProviderGrpc;
import com.payflow.proto.ProviderResult;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Calls a payment provider and classifies what happened. The classification is
 * the whole point: it decides whether the saga may move on.
 *
 * <ul>
 *   <li>{@link Outcome.Answered}: the provider replied; its status is the truth.</li>
 *   <li>{@link Outcome.Unknown}: at least one request left this process and no reply
 *       came back (deadline, connection drop). The charge MAY have happened.
 *       Only the reconciler, asking GetStatus, may resolve this.</li>
 *   <li>{@link Outcome.NotSent}: the circuit breaker refused before anything was
 *       sent. Safe to treat as a decline.</li>
 * </ul>
 *
 * Retrying is safe only because every attempt carries the same idempotency key.
 */
@Component
public class ProviderClient {

    public sealed interface Outcome {
        /** @param keyUsed the idempotency key of the attempt that got this answer */
        record Answered(ProviderResult result, String keyUsed) implements Outcome {}
        record Unknown(String why) implements Outcome {}
        record NotSent(String why) implements Outcome {}
    }

    private final PaymentProviderGrpc.PaymentProviderBlockingStub stub;
    private final PayflowProperties.Psp cfg;
    private final boolean unsafeMode;
    private final CircuitBreakerRegistry breakers;
    private final Retry retry;

    public ProviderClient(@Qualifier("psp") ManagedChannel channel, PayflowProperties props) {
        this.stub = PaymentProviderGrpc.newBlockingStub(channel);
        this.cfg = props.psp();
        this.unsafeMode = props.unsafeMode();
        this.breakers = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .slidingWindowSize(cfg.breakerWindow())
                .minimumNumberOfCalls(cfg.breakerWindow() / 2)
                .failureRateThreshold(cfg.breakerFailurePercent())
                .waitDurationInOpenState(Duration.ofMillis(cfg.breakerOpenMs()))
                .permittedNumberOfCallsInHalfOpenState(3)
                .recordException(ProviderClient::isTransient)
                .build());
        this.retry = Retry.of("psp", RetryConfig.custom()
                .maxAttempts(cfg.maxAttempts())
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(Duration.ofMillis(cfg.backoffMs()), 2.0, 0.5))
                .retryOnException(ProviderClient::isTransient)
                .build());
    }

    /** One circuit breaker per provider: a dead wallet provider must not stop card payments. */
    public CircuitBreaker breaker(PaymentMethod m) {
        return breakers.circuitBreaker(m.name());
    }

    public Outcome authorize(PaymentMethod method, String key, long amountMinor, String currency,
                             String token, String orderRef) {
        AtomicReference<String> lastKey = new AtomicReference<>(key);
        Supplier<String> keyFor = unsafeMode
                ? () -> { lastKey.set(key + ":" + UUID.randomUUID()); return lastKey.get(); } // the classic bug: a new key per retry
                : () -> key;
        Outcome o = call(method, s -> s.authorize(AuthorizeRequest.newBuilder()
                .setIdempotencyKey(keyFor.get()).setMethod(method).setAmountMinor(amountMinor)
                .setCurrency(currency).setToken(token).setOrderRef(orderRef).build()), true);
        // unsafe mode remembers whichever attempt answered, like a naive client would
        return o instanceof Outcome.Answered a ? new Outcome.Answered(a.result(), lastKey.get()) : o;
    }

    public Outcome capture(PaymentMethod m, String key) {
        return call(m, s -> s.capture(op(m, key)), true);
    }

    public Outcome voidAuth(PaymentMethod m, String key) {
        return call(m, s -> s.void_(op(m, key)), true);
    }

    public Outcome refund(PaymentMethod m, String key) {
        return call(m, s -> s.refund(op(m, key)), true);
    }

    /** Read-only, so it bypasses the breaker: the reconciler must be able to ask even when writes are failing. */
    public Outcome status(PaymentMethod m, String key) {
        return call(m, s -> s.getStatus(op(m, key)), false);
    }

    private Outcome call(PaymentMethod method, Function<PaymentProviderGrpc.PaymentProviderBlockingStub, ProviderResult> rpc,
                         boolean guarded) {
        AtomicInteger sent = new AtomicInteger();
        Supplier<ProviderResult> attempt = () -> {
            sent.incrementAndGet();
            return rpc.apply(stub.withDeadlineAfter(cfg.deadlineMs(), TimeUnit.MILLISECONDS));
        };
        if (guarded) attempt = CircuitBreaker.decorateSupplier(breaker(method), attempt);
        try {
            return new Outcome.Answered(Retry.decorateSupplier(retry, attempt).get(), null);
        } catch (CallNotPermittedException e) {
            // The breaker may open between retries; earlier attempts may still have landed.
            return sent.get() == 0 ? new Outcome.NotSent("circuit_open") : new Outcome.Unknown("circuit_open_after_attempts");
        } catch (StatusRuntimeException e) {
            return new Outcome.Unknown(e.getStatus().getCode().name());
        }
    }

    private static OperationRequest op(PaymentMethod m, String key) {
        return OperationRequest.newBuilder().setIdempotencyKey(key).setMethod(m).build();
    }

    private static boolean isTransient(Throwable t) {
        if (!(t instanceof StatusRuntimeException e)) return false;
        Status.Code c = e.getStatus().getCode();
        return c == Status.Code.UNAVAILABLE || c == Status.Code.DEADLINE_EXCEEDED;
    }
}
