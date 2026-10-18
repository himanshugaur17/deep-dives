package com.example.mvvacuum.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.poc")
public record PocProperties(
        boolean enabled,
        boolean resetSchema,
        int refreshCycles,
        boolean pauseBetweenCycles
) {
    public PocProperties {
        if (refreshCycles < 0) {
            throw new IllegalArgumentException("app.poc.refresh-cycles must be zero or greater");
        }
    }
}
