package com.eventflow.repository;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.model.AnomalyEvent;
import com.eventflow.model.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * EVENT REPOSITORY — Cassandra Read/Write Operations
 * ====================================================
 *
 * This class is the ONLY place in our codebase that talks to Cassandra.
 * All database operations go through here.
 *
 * PREPARED STATEMENTS:
 * ====================
 * Notice we create PreparedStatements in the constructor, not on every call.
 *
 * A PreparedStatement is like a compiled SQL query template:
 * "INSERT INTO events_by_user (...) VALUES (?, ?, ?, ...)"
 *
 * The ?s are placeholders. Cassandra parses and optimizes the query ONCE.
 * On each call, we just fill in the values (bind variables).
 *
 * Benefits:
 * 1. PERFORMANCE: parsing/planning happens once, not on every request
 * 2. SECURITY: prevents CQL injection (like SQL injection in SQL databases)
 * 3. EFFICIENCY: Cassandra caches the query plan server-side
 *
 * INTERVIEW TALKING POINT:
 * "We use prepared statements for all Cassandra operations. They're parsed
 * once at startup and reused with bound values, giving us both performance
 * and injection safety."
 *
 * @Repository tells Spring this class does data access operations.
 */
@Repository
public class EventRepository {

    private static final Logger log = LoggerFactory.getLogger(EventRepository.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter HOUR_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");

    private final CqlSession session;
    private final EventFlowMetrics metrics;
    private final ObjectMapper objectMapper;

    // Prepared statements — created once, reused thousands of times
    private final PreparedStatement insertEventStmt;
    private final PreparedStatement selectEventsByUserStmt;
    private final PreparedStatement updateAggregationStmt;
    private final PreparedStatement selectAggregationsStmt;
    private final PreparedStatement insertAnomalyStmt;
    private final PreparedStatement selectAnomaliesStmt;

    public EventRepository(CqlSession session, EventFlowMetrics metrics) {
        this.session = session;
        this.metrics = metrics;
        this.objectMapper = new ObjectMapper();

        // ==================== PREPARE ALL STATEMENTS ====================

        // INSERT event into events_by_user table
        this.insertEventStmt = session.prepare(
            "INSERT INTO events_by_user " +
            "(user_id, time_bucket, event_timestamp, event_id, event_type, payload, source_ip, processed_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );

        // SELECT events for a user within a specific time bucket
        // We query one day at a time because our partition key includes time_bucket
        this.selectEventsByUserStmt = session.prepare(
            "SELECT * FROM events_by_user " +
            "WHERE user_id = ? AND time_bucket = ? " +
            "AND event_timestamp >= ? AND event_timestamp <= ? " +
            "LIMIT ?"
        );

        // UPDATE aggregation counter (COUNTER columns use UPDATE, not INSERT)
        this.updateAggregationStmt = session.prepare(
            "UPDATE aggregations " +
            "SET event_count = event_count + ? " +
            "WHERE user_id = ? AND event_type = ? AND time_bucket = ?"
        );

        // SELECT aggregations for a user
        this.selectAggregationsStmt = session.prepare(
            "SELECT * FROM aggregations " +
            "WHERE user_id = ? AND event_type = ? AND time_bucket = ?"
        );

        // INSERT detected anomaly
        this.insertAnomalyStmt = session.prepare(
            "INSERT INTO anomalies " +
            "(time_bucket, detected_at, anomaly_id, user_id, metric, " +
            "observed_value, expected_value, z_score, severity) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        // SELECT recent anomalies
        this.selectAnomaliesStmt = session.prepare(
            "SELECT * FROM anomalies WHERE time_bucket = ? LIMIT ?"
        );

        log.info("All Cassandra prepared statements initialized");
    }

    // ==================== WRITE OPERATIONS ====================

    /**
     * Saves an event to Cassandra.
     *
     * TIME_BUCKET CALCULATION:
     * We convert the event timestamp to a date string like "2026-05-23".
     * This becomes part of the partition key.
     *
     * WHY?
     * Without time bucketing, all events for user-123 (from 2020 to 2026)
     * would be in ONE partition. That could be millions of rows — too big.
     * With daily bucketing, each partition has at most one day of events.
     *
     * To query events for user-123 from the last 7 days, we make 7 queries
     * (one per day bucket) and combine the results. This is fast because
     * each query hits a small, bounded partition.
     */
    public void saveEvent(Event event) {
        long start = System.nanoTime();

        String timeBucket = formatDate(event.timestamp());
        String payloadJson = serializePayload(event.payload());

        BoundStatement bound = insertEventStmt.bind(
            event.userId(),
            timeBucket,
            event.timestamp(),
            event.id(),
            event.eventType(),
            payloadJson,
            event.sourceIp(),
            event.processedAt()
        );

        session.execute(bound);
        metrics.recordCassandraWriteLatency(System.nanoTime() - start);

        log.debug("Saved event {} to Cassandra (bucket={})", event.id(), timeBucket);
    }

    /**
     * Increments the aggregation counter.
     *
     * COUNTER COLUMNS ARE SPECIAL:
     * You can't INSERT into a counter column — you can only UPDATE (increment/decrement).
     * This is because Cassandra distributes counters across replicas, and
     * the final value is the SUM of all increments across all replicas.
     *
     * Example: if 3 replicas each receive "increment by 1", the counter becomes 3.
     */
    public void incrementAggregation(String userId, String eventType,
                                      Instant windowStart, long count) {
        String timeBucket = formatDate(windowStart);

        BoundStatement bound = updateAggregationStmt.bind(
            count,          // increment amount
            userId,
            eventType,
            timeBucket
        );

        session.execute(bound);
    }

    /**
     * Saves a detected anomaly to Cassandra.
     */
    public void saveAnomaly(AnomalyEvent anomaly) {
        String timeBucket = formatHour(anomaly.detectedAt());

        BoundStatement bound = insertAnomalyStmt.bind(
            timeBucket,
            anomaly.detectedAt(),
            UUID.randomUUID(),
            anomaly.userId(),
            anomaly.metric(),
            anomaly.observedValue(),
            anomaly.expectedValue(),
            anomaly.zScore(),
            anomaly.severity()
        );

        session.execute(bound);
        log.info("Anomaly saved: user={}, z-score={:.2f}, severity={}",
            anomaly.userId(), anomaly.zScore(), anomaly.severity());
    }

    // ==================== READ OPERATIONS ====================

    /**
     * Queries events for a user within a time range.
     *
     * MULTI-BUCKET QUERY:
     * Since our partition key includes time_bucket (daily), querying
     * "events from the last 3 days" requires querying 3 partitions.
     *
     * We iterate over each day in the range and query each bucket.
     * Results are merged and sorted by timestamp.
     *
     * IN PRODUCTION: you'd use async queries (executeAsync) to query
     * all buckets in parallel, then merge results. We keep it simple here.
     */
    public List<Event> findEventsByUser(String userId, Instant start, Instant end, int limit) {
        List<Event> allEvents = new ArrayList<>();

        // Generate all date buckets between start and end
        LocalDate startDate = start.atOffset(ZoneOffset.UTC).toLocalDate();
        LocalDate endDate = end.atOffset(ZoneOffset.UTC).toLocalDate();

        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            String timeBucket = date.format(DATE_FORMAT);

            BoundStatement bound = selectEventsByUserStmt.bind(
                userId, timeBucket, start, end, limit
            );

            ResultSet rs = session.execute(bound);
            for (Row row : rs) {
                allEvents.add(rowToEvent(row));
            }
        }

        // Sort by timestamp descending (newest first) and limit
        allEvents.sort((a, b) -> b.timestamp().compareTo(a.timestamp()));
        if (allEvents.size() > limit) {
            allEvents = allEvents.subList(0, limit);
        }

        return allEvents;
    }

    /**
     * Gets anomalies for the current hour.
     */
    public List<AnomalyEvent> getRecentAnomalies(int hours, int limit) {
        List<AnomalyEvent> anomalies = new ArrayList<>();
        Instant now = Instant.now();

        for (int i = 0; i < hours; i++) {
            Instant bucketTime = now.minusSeconds(i * 3600L);
            String timeBucket = formatHour(bucketTime);

            BoundStatement bound = selectAnomaliesStmt.bind(timeBucket, limit);
            ResultSet rs = session.execute(bound);

            for (Row row : rs) {
                anomalies.add(new AnomalyEvent(
                    row.getString("user_id"),
                    row.getString("metric"),
                    row.getDouble("observed_value"),
                    row.getDouble("expected_value"),
                    row.getDouble("z_score"),
                    row.getInstant("detected_at"),
                    row.getString("severity")
                ));
            }
        }

        return anomalies;
    }

    // ==================== HELPER METHODS ====================

    private Event rowToEvent(Row row) {
        Map<String, Object> payload = deserializePayload(row.getString("payload"));
        return new Event(
            row.getUuid("event_id"),
            row.getString("event_type"),
            row.getString("user_id"),
            row.getInstant("event_timestamp"),
            payload,
            row.getString("source_ip"),
            row.getInstant("processed_at")
        );
    }

    private String formatDate(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).toLocalDate().format(DATE_FORMAT);
    }

    private String formatHour(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).format(HOUR_FORMAT);
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize payload, using empty object");
            return "{}";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializePayload(String json) {
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }
}
