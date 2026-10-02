package com.payflow.eligibility;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EligibilityApplication {
    public static void main(String[] args) {
        SpringApplication.run(EligibilityApplication.class, args);
    }
}
