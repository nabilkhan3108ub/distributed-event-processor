package com.eventflow.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * THE CORE EVENT MODEL
 * ====================
 * This is the central data structure of our entire system. Every piece of data
 * that flows through EventFlow is represented as an Event.
 *
 * Think of it like a standardized envelope: no matter what kind of data you're
 * sending (a page view, a button click, a sensor reading), it gets wrapped
 * in this envelope with consistent metadata.
 *
 * WHY A RECORD?
 * Java 17 records are immutable data classes. Once created, an Event cannot be
 * modified. This is critical in distributed systems because:
 * 1. Multiple threads can safely read the same Event without locks
 * 2. You can't accidentally corrupt an event mid-pipeline
 * 3. It's clear that transformations create NEW events, not modify existing ones
 *
 * FIELDS EXPLAINED:
 * - id: UUID (Universally Unique ID) — guaranteed unique across all servers
 *        without needing a central ID generator (which would be a bottleneck)
 * - eventType: what happened ("page_view", "click", "purchase", "sensor_reading")
 * - userId: who did it — this becomes our Cassandra PARTITION KEY
 * - timestamp: when it happened — this becomes our Cassandra CLUSTERING COLUMN
 * - payload: the actual data as key-value pairs (flexible schema)
 * - sourceIp: where the request came from (added by our API gateway)
 * - processedAt: when our pipeline finished processing it
 */
@JsonInclude(JsonInclude.Include.NON_NULL) // Don't include null fields in JSON output
public record Event(
    UUID id,
    String eventType,
    String userId,
    Instant timestamp,
    Map<String, Object> payload,
    String sourceIp,
    Instant processedAt
) {
    /**
     * Creates a new Event with server-generated metadata.
     * The client sends eventType, userId, and payload.
     * We add id, timestamp, and sourceIp on the server side.
     *
     * WHY GENERATE ID ON SERVER?
     * If we let clients generate IDs, two things go wrong:
     * 1. Malicious clients could send duplicate IDs to overwrite data
     * 2. Clients might use sequential IDs which cause "hot partitions"
     *    in Cassandra (all writes going to one node)
     * UUIDs are random, so they distribute evenly across the cluster.
     */
    public static Event create(String eventType, String userId,
                                Map<String, Object> payload, String sourceIp) {
        return new Event(
            UUID.randomUUID(),      // Random unique ID
            eventType,
            userId,
            Instant.now(),          // Server timestamp (not client's clock)
            payload,
            sourceIp,
            null                    // Not processed yet
        );
    }

    /**
     * Creates a copy of this event with the processedAt timestamp set.
     * Called after the processing pipeline completes.
     * Since records are immutable, we create a new instance.
     */
    public Event withProcessedAt(Instant processedAt) {
        return new Event(id, eventType, userId, timestamp, payload, sourceIp, processedAt);
    }
}
