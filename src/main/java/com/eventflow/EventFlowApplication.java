package com.eventflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * WHAT IS THIS FILE?
 * ===================
 * This is the entry point of our entire application. When you run this,
 * Spring Boot starts up and does the following automatically:
 *
 * 1. Scans all packages under com.eventflow for classes annotated with
 *    @Component, @Service, @Controller, @Repository, @Configuration
 * 2. Creates instances (beans) of those classes and wires them together
 *    (this is called "dependency injection")
 * 3. Starts an embedded Tomcat web server on port 8080
 * 4. Starts Kafka consumers listening for messages
 * 5. Connects to Cassandra
 *
 * @SpringBootApplication is a shortcut that combines:
 *   - @Configuration: this class can define beans
 *   - @EnableAutoConfiguration: auto-configure based on dependencies in pom.xml
 *   - @ComponentScan: scan for components in this package and sub-packages
 *
 * @EnableScheduling: allows us to use @Scheduled for periodic tasks
 *   (like flushing aggregation windows every 60 seconds)
 */
@SpringBootApplication
@EnableScheduling
public class EventFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventFlowApplication.class, args);
    }
}
