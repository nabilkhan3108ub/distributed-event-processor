package com.eventflow.metrics;

import io.micrometer.core.instrument.*;
import org.springframework.stereotype.Component;
import java.time.Duration;

/**
 * PROMETHEUS METRICS
 * ===================
 *
 * This class defines all the custom metrics our application exposes.
 * Prometheus scrapes these every 15 seconds and stores the time series.
 * Grafana then visualizes them on our dashboard.
 *
 * THREE TYPES OF METRICS:
 *
 * 1. COUNTER: a value that only goes UP (like an odometer)
 *    - events_ingested_total: total events received since startup
 *    - events_processed_total: total events processed by the pipeline
 *    - events_failed_total: total events that failed processing
 *    Use when: counting occurrences of something
 *
 * 2. GAUGE: a value that goes UP and DOWN (like a speedometer)
 *    - kafka_consumer_lag: how far behind the consumer is
 *    - active_connections: current number of client connections
 *    Use when: measuring a current state
 *
 * 3. TIMER/HISTOGRAM: measures the DISTRIBUTION of values
 *    - ingestion_latency: how long it takes to process a request
 *    - cassandra_write_latency: how long Cassandra writes take
 *    Automatically gives you: count, sum, avg, p50, p95, p99, max
 *    Use when: measuring latency or size distributions
 *
 * WHY CUSTOM METRICS MATTER FOR INTERVIEWS:
 * "We instrumented our pipeline with 4 key metrics: ingestion throughput,
 * end-to-end latency, consumer lag, and error rate. When consumer lag
 * exceeded 10,000 events, our Grafana alert fired and we scaled the
 * consumer group from 3 to 6 instances."
 * That sentence alone shows production thinking.
 */
@Component  // Spring creates one instance and manages it
public class EventFlowMetrics {

    // ==================== COUNTERS ====================

    private final Counter eventsIngestedCounter;
    private final Counter eventsProcessedCounter;
    private final Counter eventsFailedCounter;
    private final Counter rateLimitedCounter;
    private final Counter anomaliesDetectedCounter;

    // ==================== TIMERS ====================

    private final Timer ingestionLatencyTimer;
    private final Timer kafkaPublishTimer;
    private final Timer cassandraWriteTimer;
    private final Timer processingTimer;

    /**
     * Constructor: registers all metrics with Micrometer's registry.
     *
     * MeterRegistry is Micrometer's central class. It adapts metrics
     * to different monitoring systems (Prometheus, Datadog, CloudWatch, etc.)
     * We configured Prometheus in pom.xml, so MeterRegistry formats
     * metrics in Prometheus's text format automatically.
     *
     * NAMING CONVENTION:
     * - Use dots as separators: eventflow.events.ingested
     * - Prometheus converts dots to underscores: eventflow_events_ingested
     * - Add _total suffix for counters: eventflow_events_ingested_total
     * - Add _seconds suffix for timers: eventflow_ingestion_latency_seconds
     *
     * TAGS:
     * Tags let you filter metrics. "status" tag with values "success"/"error"
     * means you can graph successful events separately from failures.
     */
    public EventFlowMetrics(MeterRegistry registry) {

        // Count of all events received at the API gateway
        this.eventsIngestedCounter = Counter.builder("eventflow.events.ingested")
            .description("Total events ingested through the API")
            .register(registry);

        // Count of events successfully processed through the pipeline
        this.eventsProcessedCounter = Counter.builder("eventflow.events.processed")
            .description("Total events processed successfully")
            .register(registry);

        // Count of events that failed processing
        this.eventsFailedCounter = Counter.builder("eventflow.events.failed")
            .description("Total events that failed processing")
            .register(registry);

        // Count of requests rejected by the rate limiter
        this.rateLimitedCounter = Counter.builder("eventflow.requests.rate_limited")
            .description("Total requests rejected by rate limiter")
            .register(registry);

        // Count of anomalies detected
        this.anomaliesDetectedCounter = Counter.builder("eventflow.anomalies.detected")
            .description("Total anomalies detected")
            .register(registry);

        // How long it takes from receiving an HTTP request to returning a response
        this.ingestionLatencyTimer = Timer.builder("eventflow.ingestion.latency")
            .description("Time to ingest an event (validate + publish to Kafka)")
            .publishPercentiles(0.5, 0.95, 0.99)  // Report p50, p95, p99
            .publishPercentileHistogram()
            .register(registry);

        // How long the Kafka publish call takes
        this.kafkaPublishTimer = Timer.builder("eventflow.kafka.publish.latency")
            .description("Time to publish a message to Kafka")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        // How long Cassandra writes take
        this.cassandraWriteTimer = Timer.builder("eventflow.cassandra.write.latency")
            .description("Time to write to Cassandra")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        // Total processing time (enrich + aggregate + anomaly detect + write)
        this.processingTimer = Timer.builder("eventflow.processing.latency")
            .description("Total stream processing time per event")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
    }

    // ==================== PUBLIC METHODS ====================
    // Other classes call these to record metrics

    public void recordEventIngested() {
        eventsIngestedCounter.increment();
    }

    public void recordEventProcessed() {
        eventsProcessedCounter.increment();
    }

    public void recordEventFailed() {
        eventsFailedCounter.increment();
    }

    public void recordRateLimited() {
        rateLimitedCounter.increment();
    }

    public void recordAnomalyDetected() {
        anomaliesDetectedCounter.increment();
    }

    /**
     * Records how long an operation took.
     *
     * Usage:
     *   long start = System.nanoTime();
     *   // ... do the operation ...
     *   metrics.recordIngestionLatency(System.nanoTime() - start);
     *
     * The Timer automatically converts nanoseconds to seconds for Prometheus
     * and updates the histogram buckets for percentile calculations.
     */
    public void recordIngestionLatency(long nanos) {
        ingestionLatencyTimer.record(Duration.ofNanos(nanos));
    }

    public void recordKafkaPublishLatency(long nanos) {
        kafkaPublishTimer.record(Duration.ofNanos(nanos));
    }

    public void recordCassandraWriteLatency(long nanos) {
        cassandraWriteTimer.record(Duration.ofNanos(nanos));
    }

    public void recordProcessingLatency(long nanos) {
        processingTimer.record(Duration.ofNanos(nanos));
    }
}
