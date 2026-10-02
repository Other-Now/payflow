package com.payflow.payment.api;

import com.payflow.proto.Device;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.PaymentMethod;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.List;

public record CreatePaymentRequest(
        @NotBlank String orderRef,
        @NotNull @Pattern(regexp = "[A-Z]{2}") String country,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotNull LineOfBusiness lob,
        @NotNull Device device,
        @Positive long amountMinor,
        @NotEmpty @Size(max = 3) List<@Valid TenderRequest> tenders) {

    public record TenderRequest(
            @NotNull PaymentMethod method,
            @Positive long amountMinor,
            @NotBlank String token) {}
}
