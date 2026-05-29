package com.eventflow.consumer;

import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.model.Event;
import com.eventflow.processor.AggregationProcessor;
import com.eventflow.processor.AnomalyDetector;
import com.eventflow.processor.EnrichmentProcessor;
import com.eventflow.repository.EventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * EVENT CONSUMER — The Processing Pipeline
 * ==========================================
 *
 * This class CONSUMES events from Kafka and runs them through our
 * processing pipeline:
 *
 *   Kafka "events-raw" topic
 *          │
 *          ▼
 *   1. ENRICH (add category, metadata)
 *          │
 *          ▼
 *   2. AGGREGATE (count per user/type/window)
 *          │
 *          ▼
 *   3. DETECT ANOMALIES (check event rate)
 *          │
 *          ▼
 *   4. SAVE TO CASSANDRA (persist the enriched event)
 *
 * HOW @KafkaListener WORKS:
 * =========================
 * When you put @KafkaListener on a method, Spring Boot:
 * 1. Creates a Kafka consumer (or multiple, based on concurrency setting)
 * 2. Subscribes to the specified topic(s)
 * 3. Continuously polls Kafka for new messages
 * 4. For each message, calls this method with the message data
 * 5. Tracks which messages have been processed (offsets)
 *
 * CONCURRENCY:
 * In KafkaConfig, we set concurrency = 3. This means Spring creates
 * 3 consumer threads, each processing messages from different partitions.
 * With 6 partitions and 3 consumers, each consumer handles 2 partitions.
 *
 * CONSUMER GROUP:
 * All 3 consumers share the group ID "eventflow-processors".
 * Kafka assigns partitions to consumers within a group:
 *   - Consumer 1: partitions 0, 1
 *   - Consumer 2: partitions 2, 3
 *   - Consumer 3: partitions 4, 5
 *
 * If Consumer 2 crashes, Kafka REBALANCES:
 *   - Consumer 1: partitions 0, 1, 2
 *   - Consumer 3: partitions 3, 4, 5
 *
 * DEAD LETTER QUEUE (DLQ):
 * If processing fails (exception), we catch it and send the failed
 * event to the "events-dead-letter" topic. This prevents a bad event
 * from blocking the entire pipeline.
 *
 * Without a DLQ:
 *   Bad event → exception → retry → exception → retry → forever blocked
 *
 * With a DLQ:
 *   Bad event → exception → send to DLQ → continue processing next event
 *   Later, an engineer examines the DLQ to fix the issue.
 *
 * INTERVIEW TALKING POINT:
 * "Each stage in our pipeline is idempotent — processing the same event
 * twice produces the same result. This lets us safely retry on failure
 * without corrupting data. Failed events go to a dead-letter topic
 * for manual inspection."
 */
@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final EnrichmentProcessor enrichmentProcessor;
    private final AggregationProcessor aggregationProcessor;
    private final AnomalyDetector anomalyDetector;
    private final EventRepository eventRepository;
    private final EventFlowMetrics metrics;
    private final KafkaTemplate<String, Event> kafkaTemplate;

    /**
     * CONSTRUCTOR INJECTION: Spring provides all 6 dependencies automatically.
     * We don't create any of these — Spring manages their lifecycle.
     */
    public EventConsumer(EnrichmentProcessor enrichmentProcessor,
                         AggregationProcessor aggregationProcessor,
                         AnomalyDetector anomalyDetector,
                         EventRepository eventRepository,
                         EventFlowMetrics metrics,
                         KafkaTemplate<String, Event> kafkaTemplate) {
        this.enrichmentProcessor = enrichmentProcessor;
        this.aggregationProcessor = aggregationProcessor;
        this.anomalyDetector = anomalyDetector;
        this.eventRepository = eventRepository;
        this.metrics = metrics;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * THE MAIN PROCESSING METHOD
     * ===========================
     * Called automatically by Spring for every message in "events-raw" topic.
     *
     * ConsumerRecord contains:
     * - key(): the partition key (userId) — we set this when producing
     * - value(): the actual Event object (deserialized from JSON)
     * - partition(): which Kafka partition this came from
     * - offset(): the message's position in the partition log
     * - timestamp(): when Kafka received the message
     *
     * @KafkaListener parameters:
     * - topics: which topic(s) to read from
     * - groupId: consumer group name (shared across all instances)
     * - containerFactory: which factory to use (we configured ours in KafkaConfig)
     */
    @KafkaListener(
        topics = "events-raw",
        groupId = "eventflow-processors",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processEvent(ConsumerRecord<String, Event> record) {
        Event event = record.value();
        long processingStart = System.nanoTime();

        try {
            log.debug("Processing event: id={}, type={}, user={}, partition={}, offset={}",
                event.id(), event.eventType(), event.userId(),
                record.partition(), record.offset());

            // STEP 1: ENRICH
            // Add category, metadata, processing timestamp
            Event enrichedEvent = enrichmentProcessor.enrich(event);

            // STEP 2: AGGREGATE
            // Increment the windowed counter for this user/type combination
            aggregationProcessor.aggregate(enrichedEvent);

            // STEP 3: ANOMALY DETECTION
            // Record this event for rate-based anomaly checking
            anomalyDetector.observe(enrichedEvent);

            // STEP 4: PERSIST TO CASSANDRA
            // Save the fully enriched event for later querying
            eventRepository.saveEvent(enrichedEvent);

            // Record success metrics
            metrics.recordEventProcessed();
            metrics.recordProcessingLatency(System.nanoTime() - processingStart);

        } catch (Exception e) {
            // Something went wrong — send to Dead Letter Queue
            log.error("Failed to process event {}: {}", event.id(), e.getMessage(), e);
            metrics.recordEventFailed();
            sendToDeadLetterQueue(event, e);
        }
    }

    /**
     * DEAD LETTER QUEUE
     * ==================
     * When processing fails, we send the event to a separate topic
     * so it doesn't block the main pipeline.
     *
     * In production, you'd also:
     * - Add the exception message to the event's metadata
     * - Set up alerting when the DLQ has messages
     * - Build a tool to replay DLQ events after fixing the bug
     */
    private void sendToDeadLetterQueue(Event event, Exception error) {
        try {
            kafkaTemplate.send("events-dead-letter", event.userId(), event);
            log.warn("Event {} sent to dead-letter queue (reason: {})",
                event.id(), error.getMessage());
        } catch (Exception dlqError) {
            // If even DLQ fails, log and move on — we can't block forever
            log.error("Failed to send event {} to DLQ: {}",
                event.id(), dlqError.getMessage());
        }
    }
}
