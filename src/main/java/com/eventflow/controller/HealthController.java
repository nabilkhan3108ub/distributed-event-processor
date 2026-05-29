package com.eventflow.controller;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HEALTH CONTROLLER
 * ==================
 *
 * WHAT IS A HEALTH CHECK?
 * A health check endpoint tells external systems "I'm alive and working."
 *
 * WHO CALLS IT?
 * 1. Docker: checks if the container is healthy (restarts if not)
 * 2. Kubernetes: decides whether to send traffic to this instance
 * 3. Load balancers: removes unhealthy instances from the pool
 * 4. Monitoring tools: alerts the team when something is down
 *
 * OUR HEALTH CHECK VERIFIES:
 * 1. The application itself is running (if it can respond, it's running)
 * 2. Cassandra connection is alive (try a simple query)
 * 3. Kafka producer is functional (check for the producer instance)
 *
 * If ALL checks pass → 200 OK with {"status": "UP"}
 * If ANY check fails → 503 Service Unavailable with details about what's down
 *
 * WHY /health AND NOT /api/v1/health?
 * Health checks are infrastructure-level, not part of the business API.
 * They don't need API keys or rate limiting. Keeping them at the root
 * path (/health) is a convention that ops tools expect.
 */
@RestController
public class HealthController {

    private final CqlSession cqlSession;
    private final KafkaTemplate<?, ?> kafkaTemplate;

    public HealthController(CqlSession cqlSession, KafkaTemplate<?, ?> kafkaTemplate) {
        this.cqlSession = cqlSession;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * GET /health — System health check
     *
     * Returns 200 if everything is healthy, 503 if something is down.
     *
     * RESPONSE FORMAT:
     * {
     *   "status": "UP",           // or "DOWN"
     *   "timestamp": "2026-05-23T10:15:00Z",
     *   "components": {
     *     "cassandra": "UP",      // or "DOWN: <error message>"
     *     "kafka": "UP"
     *   }
     * }
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new LinkedHashMap<>();
        Map<String, String> components = new LinkedHashMap<>();

        boolean allHealthy = true;

        // Check Cassandra
        try {
            // Execute a lightweight query — if this succeeds, Cassandra is reachable
            cqlSession.execute("SELECT now() FROM system.local");
            components.put("cassandra", "UP");
        } catch (Exception e) {
            components.put("cassandra", "DOWN: " + e.getMessage());
            allHealthy = false;
        }

        // Check Kafka
        try {
            // If the producer factory exists and hasn't thrown errors, Kafka is likely up
            // A more thorough check would list topics, but that's slower
            if (kafkaTemplate.getProducerFactory() != null) {
                components.put("kafka", "UP");
            } else {
                components.put("kafka", "DOWN: no producer factory");
                allHealthy = false;
            }
        } catch (Exception e) {
            components.put("kafka", "DOWN: " + e.getMessage());
            allHealthy = false;
        }

        health.put("status", allHealthy ? "UP" : "DOWN");
        health.put("timestamp", Instant.now().toString());
        health.put("components", components);

        // 200 if healthy, 503 if not
        if (allHealthy) {
            return ResponseEntity.ok(health);
        } else {
            return ResponseEntity.status(503).body(health);
        }
    }
}
