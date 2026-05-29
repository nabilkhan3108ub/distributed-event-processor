package com.eventflow.model;

import java.time.Instant;

/**
 * AGGREGATION RESULT
 * ===================
 * Represents a pre-computed summary of events over a time window.
 *
 * WHAT IS AGGREGATION?
 * Instead of storing every single event and computing totals on-the-fly
 * (which is slow with millions of events), we pre-compute summaries
 * as events arrive. This is called "stream processing."
 *
 * Example: "In the last hour, user-123 had 47 page_view events."
 *
 * Fields:
 * - userId: whose events were aggregated
 * - eventType: what kind of events (page_view, click, etc.)
 * - windowStart/windowEnd: the time range this aggregation covers
 * - count: how many events occurred in this window
 * - windowMinutes: the size of the aggregation window (e.g., 60 = 1 hour)
 */
public record AggregationResult(
    String userId,
    String eventType,
    Instant windowStart,
    Instant windowEnd,
    long count,
    int windowMinutes
) {}
