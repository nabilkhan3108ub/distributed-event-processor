package com.eventflow.processor;

import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.model.AnomalyEvent;
import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ANOMALY DETECTOR — Z-Score Based
 * ==================================
 *
 * WHAT IS ANOMALY DETECTION?
 * Anomaly detection finds unusual patterns in data. If a user normally
 * generates 10 events per minute, and suddenly generates 500, something
 * is wrong. It could be:
 *   - A bot attacking your API
 *   - A bug in the client app sending duplicate events
 *   - A DDoS (Distributed Denial of Service) attack
 *   - A legitimate viral spike (good, but you still want to know)
 *
 * HOW Z-SCORE WORKS:
 * ==================
 * Z-score measures "how far from normal" a value is.
 *
 * Formula: z = (observed - mean) / standard_deviation
 *
 * - mean = average value over recent history
 * - standard_deviation = how much values typically vary from the mean
 *
 * EXAMPLE:
 * If a user normally sends 10±3 events/minute (mean=10, stddev=3):
 *   - 12 events/minute → z = (12-10)/3 = 0.67 → NORMAL
 *   - 15 events/minute → z = (15-10)/3 = 1.67 → a bit high but okay
 *   - 25 events/minute → z = (25-10)/3 = 5.0  → ANOMALY! (5 stddevs away)
 *
 * THRESHOLDS:
 *   z > 2.0 → unusual (5% chance of normal occurrence) → LOW severity
 *   z > 3.0 → very unusual (0.3% chance) → MEDIUM severity
 *   z > 5.0 → almost certainly anomalous → HIGH severity
 *
 * WE USE A RUNNING AVERAGE (Exponential Moving Average):
 * Instead of keeping ALL historical data, we maintain a running mean
 * and variance. Each new observation updates them:
 *   new_mean = old_mean * 0.9 + new_value * 0.1
 *
 * This "forgets" old data gradually (what happened last week matters less
 * than what happened in the last hour).
 *
 * INTERVIEW TALKING POINT:
 * "We use exponential moving average with a decay factor of 0.1 for
 * anomaly detection. It adapts to changing baselines — if a user
 * gradually increases their activity, the baseline shifts. But sudden
 * spikes are caught because the EMA hasn't adapted yet."
 */
@Component
public class AnomalyDetector {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetector.class);

    // Z-score threshold for flagging anomalies
    private static final double ANOMALY_THRESHOLD = 3.0;

    // Smoothing factor for exponential moving average
    // 0.1 means: 10% weight to new data, 90% weight to history
    // Lower = more stable baseline, slower to adapt
    // Higher = faster adaptation, more false positives
    private static final double ALPHA = 0.1;

    // Minimum observations before we start detecting
    // We need some history to calculate meaningful statistics
    private static final int MIN_OBSERVATIONS = 10;

    private final EventRepository eventRepository;
    private final EventFlowMetrics metrics;

    /**
     * Per-user statistics tracker.
     * Key: userId
     * Value: UserStats (running mean, variance, observation count)
     */
    private final ConcurrentHashMap<String, UserStats> userStats = new ConcurrentHashMap<>();

    /**
     * Per-user event counter for the current window.
     * Reset every check interval.
     */
    private final ConcurrentHashMap<String, AtomicLong> currentWindowCounts = new ConcurrentHashMap<>();

    public AnomalyDetector(EventRepository eventRepository, EventFlowMetrics metrics) {
        this.eventRepository = eventRepository;
        this.metrics = metrics;
    }

    /**
     * Called for every event. Increments the per-user counter.
     * The actual anomaly check happens periodically (every 30 seconds).
     */
    public void observe(Event event) {
        currentWindowCounts
            .computeIfAbsent(event.userId(), k -> new AtomicLong(0))
            .incrementAndGet();
    }

    /**
     * SCHEDULED CHECK — runs every 30 seconds.
     *
     * For each user who had events in the last 30 seconds:
     * 1. Get their event count for this window
     * 2. Compare it to their historical average (z-score)
     * 3. If anomalous, create an AnomalyEvent and save to Cassandra
     * 4. Update their running statistics
     */
    @Scheduled(fixedRate = 30000)  // Every 30 seconds
    public void checkForAnomalies() {
        if (currentWindowCounts.isEmpty()) return;

        // Snapshot and clear (same pattern as AggregationProcessor)
        ConcurrentHashMap<String, AtomicLong> snapshot = new ConcurrentHashMap<>(currentWindowCounts);
        currentWindowCounts.clear();

        for (var entry : snapshot.entrySet()) {
            String userId = entry.getKey();
            double eventCount = entry.getValue().get();

            UserStats stats = userStats.computeIfAbsent(userId, k -> new UserStats());

            // Only check if we have enough history
            if (stats.observationCount >= MIN_OBSERVATIONS) {
                double zScore = stats.calculateZScore(eventCount);

                if (Math.abs(zScore) > ANOMALY_THRESHOLD) {
                    String severity = AnomalyEvent.calculateSeverity(Math.abs(zScore));

                    AnomalyEvent anomaly = new AnomalyEvent(
                        userId,
                        "event_rate",
                        eventCount,
                        stats.mean,
                        zScore,
                        Instant.now(),
                        severity
                    );

                    // Save to Cassandra for dashboard and alerting
                    eventRepository.saveAnomaly(anomaly);
                    metrics.recordAnomalyDetected();

                    log.warn("ANOMALY DETECTED: user={}, rate={}, expected={:.1f}, z-score={:.2f}, severity={}",
                        userId, eventCount, stats.mean, zScore, severity);
                }
            }

            // Update running statistics with this observation
            stats.update(eventCount);
        }
    }

    /**
     * INNER CLASS: UserStats
     * ======================
     * Tracks the running mean and variance for one user's event rate.
     *
     * EXPONENTIAL MOVING AVERAGE (EMA):
     * Instead of keeping all historical values in memory, we maintain
     * a running estimate that gives more weight to recent observations.
     *
     * For mean:
     *   mean = (1 - alpha) * old_mean + alpha * new_value
     *   With alpha = 0.1:
     *     mean = 0.9 * old_mean + 0.1 * new_value
     *
     * For variance (how spread out values are):
     *   diff = new_value - old_mean
     *   variance = (1 - alpha) * old_variance + alpha * diff²
     *
     * Standard deviation = sqrt(variance)
     *
     * This approach:
     * - Uses O(1) memory per user (just mean + variance)
     * - Adapts to gradual changes in behavior
     * - Catches sudden spikes immediately
     */
    private static class UserStats {
        double mean = 0.0;
        double variance = 0.0;
        int observationCount = 0;

        /**
         * Calculates Z-score: how many standard deviations
         * the observed value is from the expected mean.
         */
        double calculateZScore(double observed) {
            double stddev = Math.sqrt(variance);
            if (stddev < 1.0) stddev = 1.0;  // Avoid division by zero
            return (observed - mean) / stddev;
        }

        /**
         * Updates running statistics with a new observation.
         */
        void update(double observed) {
            observationCount++;

            if (observationCount == 1) {
                // First observation: set mean directly, variance is 0
                mean = observed;
                variance = 0;
            } else {
                // Exponential moving average update
                double diff = observed - mean;
                mean = (1 - ALPHA) * mean + ALPHA * observed;
                variance = (1 - ALPHA) * variance + ALPHA * diff * diff;
            }
        }
    }
}
