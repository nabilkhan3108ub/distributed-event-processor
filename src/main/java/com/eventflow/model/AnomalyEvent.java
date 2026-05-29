package com.eventflow.model;

import java.time.Instant;

/**
 * ANOMALY EVENT
 * ==============
 * Represents a detected anomaly — something unusual in the event stream.
 *
 * WHAT IS ANOMALY DETECTION?
 * If a user normally generates 10 events per minute, and suddenly generates
 * 500 events per minute, that's anomalous. It might be:
 * - A bot or scraper attacking your site
 * - A bug in the client app sending duplicate events
 * - A legitimate spike (viral content)
 *
 * We use Z-SCORE for detection:
 * z = (observed_value - mean) / standard_deviation
 *
 * If z > 3.0, the observation is more than 3 standard deviations from the mean,
 * which happens less than 0.3% of the time in normal data. We flag it as anomalous.
 *
 * Fields:
 * - userId: who the anomaly is about
 * - metric: what was anomalous (e.g., "event_rate")
 * - observedValue: the actual value we saw
 * - expectedValue: the historical mean
 * - zScore: how many standard deviations away from normal
 * - detectedAt: when we detected it
 * - severity: HIGH (z > 5), MEDIUM (z > 3), LOW (z > 2)
 */
public record AnomalyEvent(
    String userId,
    String metric,
    double observedValue,
    double expectedValue,
    double zScore,
    Instant detectedAt,
    String severity
) {
    public static String calculateSeverity(double zScore) {
        if (zScore > 5.0) return "HIGH";
        if (zScore > 3.0) return "MEDIUM";
        return "LOW";
    }
}
