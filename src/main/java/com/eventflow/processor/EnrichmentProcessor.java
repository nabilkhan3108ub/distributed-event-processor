package com.eventflow.processor;

import com.eventflow.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * ENRICHMENT PROCESSOR
 * =====================
 *
 * WHAT IS ENRICHMENT?
 * When a raw event arrives, it only has what the client sent (eventType,
 * userId, payload). Enrichment adds ADDITIONAL context from server-side
 * data sources to make the event more useful for analysis.
 *
 * REAL-WORLD EXAMPLES:
 * - Netflix enriches streaming events with user's plan tier, region, and device type
 * - Uber enriches ride events with driver rating, car type, and surge pricing zone
 * - Salesforce enriches API events with account tier and feature flags
 *
 * IN EVENTFLOW, we enrich with:
 * - Server processing timestamp
 * - Event category (derived from event type)
 * - Payload size (useful for monitoring)
 *
 * In a production system, you'd also call external services:
 * - GeoIP lookup to get city/country from the source IP
 * - User profile service to get the user's tier/segment
 * - Feature flag service to check what experiments are active
 */
@Component
public class EnrichmentProcessor {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentProcessor.class);

    /**
     * Enriches an event with server-side metadata.
     * Returns a NEW Event (events are immutable).
     */
    public Event enrich(Event event) {
        // Create enriched payload by copying original and adding new fields
        Map<String, Object> enrichedPayload = new HashMap<>(event.payload());

        // Add server-side enrichments
        enrichedPayload.put("_enriched", true);
        enrichedPayload.put("_category", categorize(event.eventType()));
        enrichedPayload.put("_payloadSize", event.payload().size());
        enrichedPayload.put("_processedBy", "enrichment-v1");

        // Return new event with enriched payload and processing timestamp
        return new Event(
            event.id(),
            event.eventType(),
            event.userId(),
            event.timestamp(),
            enrichedPayload,
            event.sourceIp(),
            Instant.now()  // Mark when processing completed
        );
    }

    /**
     * Categorizes events by type.
     * "page_view" and "click" → "engagement"
     * "purchase" and "add_to_cart" → "commerce"
     * etc.
     *
     * This makes aggregation queries easier:
     * "Show me all engagement events" instead of listing every type.
     */
    private String categorize(String eventType) {
        return switch (eventType.toLowerCase()) {
            case "page_view", "click", "scroll", "hover" -> "engagement";
            case "purchase", "add_to_cart", "remove_from_cart", "checkout" -> "commerce";
            case "login", "logout", "signup", "password_reset" -> "auth";
            case "search", "filter", "sort" -> "discovery";
            case "error", "crash", "timeout" -> "error";
            default -> "other";
        };
    }
}
