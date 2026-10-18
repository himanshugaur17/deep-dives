package com.example.mvvacuum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@ConfigurationPropertiesScan
public class PostgresMvVacuumPocApplication {

    public static void main(String[] args) {
        try (ConfigurableApplicationContext ignored = SpringApplication.run(
                PostgresMvVacuumPocApplication.class,
                args)) {
            // Closing the non-web context also closes the JDBC connection pool.
        }
    }
}
