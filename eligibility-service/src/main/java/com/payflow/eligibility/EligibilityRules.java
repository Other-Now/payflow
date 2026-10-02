package com.payflow.eligibility;

import com.payflow.proto.Device;
import com.payflow.proto.EligibilityRequest;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.MethodDecision;
import com.payflow.proto.PaymentMethod;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Eligibility rules are data (application.yml), not code: one rule per payment
 * method, and every field is an optional constraint. Adding a market or turning
 * a method off is a config change, not a deploy of new logic.
 */
@ConfigurationProperties("payflow.eligibility")
public record EligibilityRules(Map<PaymentMethod, MethodRule> methods) {

    public record MethodRule(
            Set<String> allowedCurrencies,
            Set<String> allowedCountries,
            Set<String> blockedCountries,
            Set<Device> allowedDevices,
            Set<LineOfBusiness> blockedLobs,
            Long maxAmountMinor) {

        /** Returns the first reason this checkout is not allowed, or empty if it is. */
        Optional<String> deny(EligibilityRequest r) {
            if (blockedCountries != null && blockedCountries.contains(r.getCountry())) return Optional.of("country_blocked");
            if (allowedCountries != null && !allowedCountries.contains(r.getCountry())) return Optional.of("country_not_supported");
            if (allowedCurrencies != null && !allowedCurrencies.contains(r.getCurrency())) return Optional.of("currency_not_supported");
            if (allowedDevices != null && !allowedDevices.contains(r.getDevice())) return Optional.of("device_not_supported");
            if (blockedLobs != null && blockedLobs.contains(r.getLob())) return Optional.of("lob_not_supported");
            if (maxAmountMinor != null && r.getAmountMinor() > maxAmountMinor) return Optional.of("amount_over_limit");
            return Optional.empty();
        }
    }

    public EligibilityRules {
        methods = methods == null ? Map.of() : new TreeMap<>(methods); // stable response order
    }

    public List<MethodDecision> decide(EligibilityRequest r) {
        List<MethodDecision> out = new ArrayList<>(methods.size());
        methods.forEach((method, rule) -> {
            Optional<String> reason = rule.deny(r);
            out.add(MethodDecision.newBuilder()
                    .setMethod(method)
                    .setEligible(reason.isEmpty())
                    .setReason(reason.orElse(""))
                    .build());
        });
        return out;
    }
}
