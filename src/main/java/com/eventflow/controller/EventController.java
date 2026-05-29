package com.eventflow.controller;

import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.model.*;
import com.eventflow.service.EventIngestionService;
import com.eventflow.service.EventQueryService;
import com.eventflow.service.RateLimiterService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * EVENT CONTROLLER — REST API Endpoints
 * =======================================
 *
 * This is the API GATEWAY — the single entry point for all client requests.
 * Every HTTP request to EventFlow hits this controller first.
 *
 * WHAT IS A REST API?
 * REST (Representational State Transfer) is a set of conventions:
 * - Resources are identified by URLs: /api/v1/events
 * - HTTP methods define actions: GET (read), POST (create), PUT (update), DELETE (remove)
 * - Responses use standard HTTP status codes: 200 (OK), 201 (Created), 400 (Bad Request)
 * - Data format is typically JSON
 *
 * ANNOTATIONS EXPLAINED:
 *
 * @RestController: combines @Controller + @ResponseBody
 *   - @Controller: this class handles HTTP requests
 *   - @ResponseBody: return values are automatically converted to JSON
 *
 * @RequestMapping("/api/v1"): all endpoints start with /api/v1
 *   - The "/v1" is API versioning — if we make breaking changes,
 *     we create /api/v2 while keeping /v1 working for old clients
 *
 * @PostMapping: handles HTTP POST requests (for creating/sending data)
 * @GetMapping: handles HTTP GET requests (for reading data)
 *
 * @RequestBody: tells Spring to parse the JSON body into a Java object
 * @RequestParam: reads URL query parameters (?userId=123&hours=24)
 * @RequestHeader: reads HTTP headers (X-API-Key for authentication)
 *
 * @Valid: triggers validation annotations on the request DTO
 *   If @NotBlank or @Size fails, Spring returns 400 before our code runs
 *
 * GATEWAY RESPONSIBILITIES:
 * 1. Rate limiting — is this client sending too many requests?
 * 2. Authentication — does this client have a valid API key?
 * 3. Validation — is the request data well-formed?
 * 4. Routing — send to ingestion service or query service?
 * 5. Metrics — record request count and latency
 *
 * INTERVIEW TALKING POINT:
 * "Our API Gateway handles cross-cutting concerns: rate limiting with
 * a token bucket algorithm, API key authentication, request validation,
 * and Prometheus metrics. The ingestion endpoint returns 202 Accepted
 * for async processing, while query endpoints return 200 OK synchronously."
 */
@RestController
@RequestMapping("/api/v1")
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final EventIngestionService ingestionService;
    private final EventQueryService queryService;
    private final RateLimiterService rateLimiterService;
    private final EventFlowMetrics metrics;

    public EventController(EventIngestionService ingestionService,
                           EventQueryService queryService,
                           RateLimiterService rateLimiterService,
                           EventFlowMetrics metrics) {
        this.ingestionService = ingestionService;
        this.queryService = queryService;
        this.rateLimiterService = rateLimiterService;
        this.metrics = metrics;
    }

    // ==================== INGESTION ENDPOINTS ====================

    /**
     * POST /api/v1/events — Ingest a single event
     *
     * REQUEST:
     * POST /api/v1/events
     * Headers: Content-Type: application/json, X-API-Key: your-key
     * Body: {"eventType": "page_view", "userId": "user-123", "payload": {"page": "/home"}}
     *
     * RESPONSE: 202 Accepted
     * Body: {"id": "uuid", "eventType": "page_view", "userId": "user-123", ...}
     *
     * WHY 202 AND NOT 200?
     * - 200 OK = "I processed your request and here's the result"
     * - 202 Accepted = "I received your request and will process it later"
     *
     * We return 202 because the event is published to Kafka but NOT yet
     * processed by the pipeline or stored in Cassandra. The client knows
     * their event was safely received (Kafka acknowledged it), but processing
     * happens asynchronously.
     *
     * WHY @Valid?
     * When Spring sees @Valid, it checks all the validation annotations
     * on EventRequest (@NotBlank, @Size, etc.) BEFORE calling our method.
     * If validation fails, Spring throws MethodArgumentNotValidException,
     * which our GlobalExceptionHandler catches and returns a 400.
     *
     * HttpServletRequest gives us access to the raw HTTP request:
     * - getRemoteAddr(): client's IP address
     * - getHeader(): any HTTP header
     */
    @PostMapping("/events")
    public ResponseEntity<?> ingestEvent(
            @Valid @RequestBody EventRequest request,
            @RequestHeader(value = "X-API-Key", defaultValue = "anonymous") String apiKey,
            HttpServletRequest httpRequest) {

        long start = System.nanoTime();

        // STEP 1: RATE LIMITING
        // Check if this client has exceeded their request quota
        if (!rateLimiterService.tryAcquire(apiKey)) {
            metrics.recordRateLimited();
            log.warn("Rate limited: apiKey={}", apiKey);
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of(
                    "error", "Rate limit exceeded",
                    "message", "Too many requests. Please retry after a moment.",
                    "retryAfterMs", 1000
                ));
        }

        // STEP 2: INGEST (validate + publish to Kafka)
        String clientIp = getClientIp(httpRequest);
        Event event = ingestionService.ingestEvent(request, clientIp);

        // STEP 3: RECORD METRICS
        metrics.recordIngestionLatency(System.nanoTime() - start);

        // STEP 4: RETURN 202 ACCEPTED
        // Include the event in the response so the client knows the
        // server-generated ID and timestamp
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(event);
    }

    /**
     * POST /api/v1/events/batch — Ingest multiple events at once
     *
     * REQUEST:
     * POST /api/v1/events/batch
     * Body: {"events": [
     *   {"eventType": "click", "userId": "user-456", "payload": {"button": "signup"}},
     *   {"eventType": "page_view", "userId": "user-456", "payload": {"page": "/pricing"}}
     * ]}
     *
     * RESPONSE: 202 Accepted
     * Body: {"accepted": 2, "events": [...]}
     *
     * WHY BATCH?
     * Each HTTP request has overhead (TCP connection, TLS handshake, headers).
     * If a client needs to send 1000 events:
     *   - 1000 individual requests: 1000 × ~50ms overhead = ~50 seconds
     *   - 10 batches of 100: 10 × ~50ms = ~0.5 seconds
     * That's a 100x improvement in network efficiency.
     *
     * Netflix, Uber, and Google all use batching for high-volume event streams.
     */
    @PostMapping("/events/batch")
    public ResponseEntity<?> ingestBatch(
            @Valid @RequestBody BatchEventRequest batchRequest,
            @RequestHeader(value = "X-API-Key", defaultValue = "anonymous") String apiKey,
            HttpServletRequest httpRequest) {

        // Rate limit check for the whole batch
        if (!rateLimiterService.tryAcquire(apiKey)) {
            metrics.recordRateLimited();
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "Rate limit exceeded"));
        }

        String clientIp = getClientIp(httpRequest);

        // Ingest each event in the batch
        List<Event> events = batchRequest.events().stream()
            .map(req -> ingestionService.ingestEvent(req, clientIp))
            .toList();

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
            "accepted", events.size(),
            "events", events
        ));
    }

    // ==================== QUERY ENDPOINTS ====================

    /**
     * GET /api/v1/events — Query stored events
     *
     * REQUEST:
     * GET /api/v1/events?userId=user-123&hours=24&limit=50
     *
     * RESPONSE: 200 OK
     * Body: [{"id": "...", "eventType": "page_view", ...}, ...]
     *
     * @RequestParam: extracts values from the URL query string.
     *   required=true: the request fails with 400 if userId is missing
     *   required=false: the parameter is optional
     *
     * WHY THIS IS SYNCHRONOUS (200, not 202):
     * The client is asking for data RIGHT NOW. They need to wait for
     * the Cassandra query to complete before getting a response.
     * This is different from ingestion where we fire-and-forget.
     *
     * Cassandra reads by partition key (userId + time bucket) are fast:
     * typically under 5ms for a single partition, because the data is
     * sorted on disk and the partition is located by hash lookup.
     */
    @GetMapping("/events")
    public ResponseEntity<List<Event>> queryEvents(
            @RequestParam(required = true) String userId,
            @RequestParam(required = false) Integer hours,
            @RequestParam(required = false) Integer limit) {

        List<Event> events = queryService.getEventsByUser(userId, hours, limit);
        return ResponseEntity.ok(events);
    }

    /**
     * GET /api/v1/events/anomalies — Get detected anomalies
     *
     * REQUEST:
     * GET /api/v1/events/anomalies?hours=1&limit=50
     *
     * RESPONSE: 200 OK
     * Body: [{"userId": "...", "zScore": 5.2, "severity": "HIGH", ...}]
     */
    @GetMapping("/events/anomalies")
    public ResponseEntity<List<AnomalyEvent>> getAnomalies(
            @RequestParam(required = false) Integer hours,
            @RequestParam(required = false) Integer limit) {

        List<AnomalyEvent> anomalies = queryService.getRecentAnomalies(hours, limit);
        return ResponseEntity.ok(anomalies);
    }

    // ==================== HELPERS ====================

    /**
     * Extracts the real client IP address.
     *
     * WHY X-Forwarded-For?
     * When requests pass through load balancers or reverse proxies,
     * the proxy's IP replaces the client's IP. The proxy adds the
     * original client IP in the X-Forwarded-For header.
     *
     * Example:
     * Client (1.2.3.4) → Load Balancer (10.0.0.1) → Our App
     * Without X-Forwarded-For: we see 10.0.0.1 (the LB, not the client)
     * With X-Forwarded-For: header contains "1.2.3.4" → we see the real client
     */
    private String getClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isEmpty()) {
            // X-Forwarded-For can contain multiple IPs: "client, proxy1, proxy2"
            // The first one is the original client
            return xForwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
