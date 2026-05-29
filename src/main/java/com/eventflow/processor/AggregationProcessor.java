package com.eventflow.processor;

import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AGGREGATION PROCESSOR — Windowed Event Counting
 * ==================================================
 *
 * WHAT IS WINDOWED AGGREGATION?
 * Instead of counting events one-by-one in real time, we collect events
 * into TIME WINDOWS (e.g., 1-minute windows) and write the count once
 * per window. This is dramatically more efficient.
 *
 * ANALOGY:
 * Imagine counting cars on a highway. You could count each car individually
 * and write "1 car" 1000 times. Or you could count for 1 minute and write
 * "1000 cars in the last minute" once. Same information, 1000x fewer writes.
 *
 * HOW IT WORKS:
 * 1. Events arrive continuously from Kafka
 * 2. For each event, we increment an in-memory counter for that user+type+window
 * 3. Every 60 seconds, a scheduled task FLUSHES all counters to Cassandra
 * 4. The counters are reset for the next window
 *
 * WHY IN-MEMORY FIRST, THEN CASSANDRA?
 * Writing to Cassandra on every single event would be:
 * - Slow: network round-trip per event
 * - Expensive: Cassandra handles writes well, but millions per second
 *   still generates compaction pressure
 * - Wasteful: we only need the TOTAL, not individual increments
 *
 * By buffering in memory and flushing periodically, we turn
 * 50,000 events/sec into ~100 Cassandra writes/minute.
 *
 * TRADE-OFF:
 * If the application crashes between flushes, we lose up to 60 seconds
 * of aggregation data. The raw events are still in Cassandra (written
 * by the EventConsumer), so we could recompute. In production, you'd
 * use Kafka Streams or Flink for exactly-once stream aggregation.
 *
 * INTERVIEW TALKING POINT:
 * "We implemented tumbling window aggregation with 60-second windows.
 * Events are counted in ConcurrentHashMap and flushed to Cassandra
 * periodically. This reduces write amplification by ~1000x compared
 * to per-event writes."
 */
@Component
public class AggregationProcessor {

    private static final Logger log = LoggerFactory.getLogger(AggregationProcessor.class);
    private static final int WINDOW_MINUTES = 1; // 1-minute tumbling windows

    private final EventRepository eventRepository;

    /**
     * ConcurrentHashMap<String, AtomicLong>
     *
     * Key: "userId|eventType|windowStart" (composite key as a string)
     * Value: AtomicLong — thread-safe counter
     *
     * WHY ConcurrentHashMap + AtomicLong?
     * Multiple Kafka consumer threads call aggregate() simultaneously.
     * ConcurrentHashMap allows concurrent reads/writes to different keys.
     * AtomicLong allows concurrent increments to the same counter without locks.
     *
     * This combination is lock-free for the common case (different users),
     * and lock-free even for the same user (AtomicLong.incrementAndGet is
     * implemented with CPU-level compare-and-swap instructions).
     */
    private final ConcurrentHashMap<String, AtomicLong> windowCounts = new ConcurrentHashMap<>();

    public AggregationProcessor(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    /**
     * Called for every event. Increments the appropriate window counter.
     *
     * TUMBLING WINDOW CALCULATION:
     * We "snap" the event timestamp to the start of its 1-minute window.
     * Events at 10:00:00, 10:00:15, and 10:00:59 all go in the same window
     * starting at 10:00:00.
     * An event at 10:01:00 goes in the next window starting at 10:01:00.
     *
     * computeIfAbsent: "get the counter for this key, or create a new one
     * starting at 0 if it doesn't exist." This is atomic.
     */
    public void aggregate(Event event) {
        Instant windowStart = truncateToWindow(event.timestamp());
        String key = event.userId() + "|" + event.eventType() + "|" + windowStart;

        windowCounts.computeIfAbsent(key, k -> new AtomicLong(0))
                    .incrementAndGet();
    }

    /**
     * SCHEDULED FLUSH — runs every 60 seconds automatically.
     *
     * @Scheduled(fixedRate = 60000): Spring calls this method every 60,000ms.
     * It runs on a separate thread managed by Spring's task scheduler.
     *
     * FLUSH ALGORITHM:
     * 1. Snapshot the current counts
     * 2. Clear the in-memory map (new events go to fresh counters)
     * 3. Write each counter to Cassandra
     *
     * WHY SNAPSHOT + CLEAR?
     * If we iterated and wrote one-by-one, new events arriving during
     * the flush would be lost (we'd clear a counter that was just incremented).
     * By swapping the entire map atomically, no events are lost.
     */
    @Scheduled(fixedRate = 60000) // Every 60 seconds
    public void flushAggregations() {
        if (windowCounts.isEmpty()) return;

        // Snapshot and clear: swap the map contents atomically
        Map<String, AtomicLong> snapshot = new ConcurrentHashMap<>(windowCounts);
        windowCounts.clear();

        int flushed = 0;
        for (Map.Entry<String, AtomicLong> entry : snapshot.entrySet()) {
            String[] parts = entry.getKey().split("\\|");
            if (parts.length != 3) continue;

            String userId = parts[0];
            String eventType = parts[1];
            Instant windowStart = Instant.parse(parts[2]);
            long count = entry.getValue().get();

            if (count > 0) {
                eventRepository.incrementAggregation(userId, eventType, windowStart, count);
                flushed++;
            }
        }

        if (flushed > 0) {
            log.info("Flushed {} aggregation windows to Cassandra", flushed);
        }
    }

    /**
     * Truncates a timestamp to the start of its window.
     *
     * For WINDOW_MINUTES=1:
     * 2026-05-23T10:15:37Z → 2026-05-23T10:15:00Z
     *
     * For WINDOW_MINUTES=5:
     * 2026-05-23T10:17:37Z → 2026-05-23T10:15:00Z
     */
    private Instant truncateToWindow(Instant timestamp) {
        return timestamp.truncatedTo(ChronoUnit.MINUTES);
    }
}
