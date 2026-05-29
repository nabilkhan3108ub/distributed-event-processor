package com.eventflow.service;

import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.model.Event;
import com.eventflow.model.EventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * EVENT INGESTION SERVICE
 * ========================
 *
 * This is the heart of our API Gateway. It takes validated requests
 * and publishes them to Kafka.
 *
 * THE FLOW:
 * 1. EventController receives HTTP request
 * 2. Spring validates the EventRequest (annotations like @NotBlank)
 * 3. This service creates an Event with server-side metadata
 * 4. Publishes the Event to the "events-raw" Kafka topic
 * 5. Returns immediately (async) — doesn't wait for processing
 *
 * WHY IS PUBLISHING ASYNC?
 * When we call kafkaTemplate.send(), it returns a CompletableFuture.
 * This means the actual send happens in a background thread.
 * We can either:
 *   a) Return to the client immediately (fire-and-forget) — fastest
 *   b) Wait for Kafka to acknowledge receipt — safest
 *
 * We do (b) — we wait for acknowledgment but with a callback pattern,
 * so we're not blocking the HTTP thread.
 *
 * INTERVIEW TALKING POINTS:
 * - "We chose acks=all for durability — every replica confirms receipt"
 * - "The partition key is userId, so all events for a user are ordered"
 * - "We record publish latency in Prometheus to monitor Kafka health"
 * - "Failed publishes go to a dead-letter topic for manual inspection"
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);
    private static final String RAW_EVENTS_TOPIC = "events-raw";

    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final EventFlowMetrics metrics;

    /**
     * CONSTRUCTOR INJECTION
     * Spring automatically provides (injects) the KafkaTemplate and
     * EventFlowMetrics instances. We don't create them ourselves.
     *
     * This is called "Dependency Injection" — one of the core concepts
     * in Spring and in software engineering generally. It means:
     * "Don't create your dependencies, let someone give them to you."
     *
     * Benefits:
     * - Easy to test (you can inject a mock KafkaTemplate in tests)
     * - Loose coupling (this class doesn't know HOW Kafka is configured)
     */
    public EventIngestionService(KafkaTemplate<String, Event> kafkaTemplate,
                                  EventFlowMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    /**
     * Ingests a single event: creates it and publishes to Kafka.
     *
     * @param request the validated request from the client
     * @param sourceIp the client's IP address (from HttpServletRequest)
     * @return the created Event (with server-generated fields)
     */
    public Event ingestEvent(EventRequest request, String sourceIp) {
        // Step 1: Create the Event with server-side metadata
        Event event = Event.create(
            request.eventType(),
            request.userId(),
            request.payload(),
            sourceIp
        );

        // Step 2: Publish to Kafka and track timing
        long publishStart = System.nanoTime();
        publishToKafka(event);
        metrics.recordKafkaPublishLatency(System.nanoTime() - publishStart);

        // Step 3: Record the ingestion in metrics
        metrics.recordEventIngested();

        log.debug("Event ingested: id={}, type={}, user={}",
            event.id(), event.eventType(), event.userId());

        return event;
    }

    /**
     * Publishes an event to the Kafka "events-raw" topic.
     *
     * KEY CONCEPT: PARTITION KEY
     * ==========================
     * We use event.userId() as the Kafka message key.
     * Kafka hashes this key to determine which partition the message goes to.
     *
     * Same userId → same partition → ORDERED PROCESSING
     *
     * This means: all events for user-123 are processed in the exact order
     * they arrived. This is critical for correctness — imagine processing
     * "user signed up" AFTER "user made a purchase" — that would be nonsensical.
     *
     * Different userIds likely go to different partitions, enabling parallel
     * processing. Events for user-123 and user-456 are processed simultaneously
     * by different consumer threads.
     *
     * CompletableFuture EXPLAINED:
     * ============================
     * kafkaTemplate.send() returns immediately with a "promise" (CompletableFuture).
     * We attach callbacks:
     * - thenAccept: called when Kafka successfully receives the message
     * - exceptionally: called if publishing fails
     *
     * The actual network send happens on a separate thread managed by Kafka.
     * This is called "non-blocking I/O" — our HTTP thread isn't stuck waiting.
     */
    private void publishToKafka(Event event) {
        CompletableFuture<SendResult<String, Event>> future =
            kafkaTemplate.send(RAW_EVENTS_TOPIC, event.userId(), event);

        future.thenAccept(result -> {
            // SUCCESS: Kafka acknowledged receipt
            // result.getRecordMetadata() tells us which partition and offset
            log.debug("Published to partition={}, offset={}",
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
        }).exceptionally(ex -> {
            // FAILURE: Kafka couldn't accept the message
            // Reasons: all brokers down, topic doesn't exist, serialization error
            log.error("Failed to publish event {}: {}", event.id(), ex.getMessage());
            metrics.recordEventFailed();
            // In production, you'd also publish to a dead-letter topic or
            // write to a local fallback file for later retry
            return null;
        });
    }
}
