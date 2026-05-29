package com.eventflow.service;

import com.eventflow.model.AnomalyEvent;
import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * EVENT QUERY SERVICE
 * ====================
 *
 * This is the READ side of our system. While EventIngestionService handles
 * WRITES (putting events into Kafka), this service handles READS
 * (getting events out of Cassandra for the query API).
 *
 * CQRS PATTERN (Command Query Responsibility Segregation):
 * =======================================================
 * Our system naturally follows CQRS:
 * - COMMANDS (writes): HTTP POST → Kafka → Processors → Cassandra
 * - QUERIES (reads):   HTTP GET → this service → Cassandra → response
 *
 * The write path and read path are completely independent. This means:
 * 1. High write load doesn't slow down reads (different code paths)
 * 2. We can optimize the read path separately (caching, indexes)
 * 3. We could even use different databases for reads vs writes
 *
 * INTERVIEW TALKING POINT:
 * "Our architecture follows CQRS — the ingestion path writes through
 * Kafka to Cassandra asynchronously, while the query path reads directly
 * from Cassandra. This decoupling means ingestion at 50K events/sec
 * doesn't degrade query latency."
 *
 * This service is thin — it delegates to EventRepository for actual
 * Cassandra queries. Its job is to apply business logic:
 * - Default time ranges
 * - Result limits
 * - Parameter validation
 */
@Service
public class EventQueryService {

    private static final Logger log = LoggerFactory.getLogger(EventQueryService.class);

    // Default and maximum limits to prevent expensive queries
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;
    private static final int DEFAULT_HOURS = 24;
    private static final int MAX_HOURS = 168;  // 7 days

    private final EventRepository eventRepository;

    public EventQueryService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    /**
     * Queries events for a specific user within a time range.
     *
     * @param userId the user whose events to retrieve
     * @param hours  how many hours back to search (default: 24)
     * @param limit  maximum number of events to return (default: 100)
     * @return list of events, newest first
     *
     * WHY WE CAP HOURS AND LIMIT:
     * Without limits, a client could request "all events for the last year"
     * which would scan millions of Cassandra partitions and potentially
     * crash the node. Sensible defaults protect the system.
     */
    public List<Event> getEventsByUser(String userId, Integer hours, Integer limit) {
        int effectiveHours = clamp(hours != null ? hours : DEFAULT_HOURS, 1, MAX_HOURS);
        int effectiveLimit = clamp(limit != null ? limit : DEFAULT_LIMIT, 1, MAX_LIMIT);

        Instant end = Instant.now();
        Instant start = end.minus(effectiveHours, ChronoUnit.HOURS);

        log.debug("Querying events: user={}, range=[{} to {}], limit={}",
            userId, start, end, effectiveLimit);

        return eventRepository.findEventsByUser(userId, start, end, effectiveLimit);
    }

    /**
     * Gets recently detected anomalies.
     *
     * @param hours how many hours back to check (default: 1)
     * @param limit maximum number of anomalies to return
     * @return list of anomaly events
     */
    public List<AnomalyEvent> getRecentAnomalies(Integer hours, Integer limit) {
        int effectiveHours = clamp(hours != null ? hours : 1, 1, 24);
        int effectiveLimit = clamp(limit != null ? limit : 50, 1, 200);

        return eventRepository.getRecentAnomalies(effectiveHours, effectiveLimit);
    }

    /**
     * Clamps a value between min and max.
     * clamp(150, 1, 100) → 100
     * clamp(-5, 1, 100) → 1
     * clamp(50, 1, 100) → 50
     */
    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
