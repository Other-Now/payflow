package com.payflow.payment.api;

import com.payflow.payment.grpc.EligibilityClient;
import com.payflow.payment.saga.PaymentOrchestrator;
import com.payflow.payment.store.PaymentStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(PaymentService.BadRequestException.class)
    ProblemDetail badRequest(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(PaymentService.UnprocessableException.class)
    ProblemDetail unprocessable(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    @ExceptionHandler(PaymentService.NotFoundException.class)
    ProblemDetail notFound(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    /** Lost an optimistic-lock race (e.g. capture vs void), or the op isn't legal from this state. */
    @ExceptionHandler({PaymentStore.StaleVersionException.class, PaymentOrchestrator.IllegalTransitionException.class})
    ProblemDetail conflict(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    /** Nothing was charged; retrying with the same key is safe. */
    @ExceptionHandler(EligibilityClient.UnavailableException.class)
    ProblemDetail unavailable(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
