package com.eventflow.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TOKEN BUCKET RATE LIMITER
 * ==========================
 *
 * This is one of the most asked algorithms in FAANG interviews.
 * Let me explain it with a simple analogy:
 *
 * ANALOGY: Imagine a bucket that holds 100 tokens (coins).
 * - Every second, 10 new tokens are added to the bucket
 * - When a request arrives, it takes 1 token from the bucket
 * - If the bucket is empty, the request is REJECTED (429 Too Many Requests)
 * - The bucket can never hold more than 100 tokens (capacity limit)
 *
 * WHY THIS ALGORITHM?
 * 1. It allows BURSTS: if no requests came for 10 seconds, the bucket
 *    has 100 tokens, so 100 rapid requests are allowed. This handles
 *    legitimate traffic spikes.
 * 2. It enforces a SUSTAINED RATE: over time, you can't exceed
 *    10 requests/second because that's how fast tokens refill.
 * 3. It's THREAD-SAFE: we use ConcurrentHashMap and atomic operations,
 *    so multiple requests from the same client are handled correctly
 *    even when processed by different threads simultaneously.
 *
 * REAL-WORLD USAGE:
 * - AWS API Gateway uses token bucket
 * - Stripe uses a variant called "leaky bucket"
 * - Google Cloud uses token bucket for all API rate limiting
 *
 * HOW IT WORKS STEP BY STEP:
 * 1. Client sends a request with API key "abc123"
 * 2. We look up (or create) a Bucket for "abc123"
 * 3. We calculate how many tokens should have been added since the last request
 *    (based on time elapsed × refill rate)
 * 4. We add those tokens (capped at capacity)
 * 5. If tokens >= 1, we remove one token and ALLOW the request
 * 6. If tokens < 1, we REJECT the request
 */
@Service  // Tells Spring to create one instance of this class and manage it
public class RateLimiterService {

    /**
     * ConcurrentHashMap: a thread-safe Map.
     * Key = API key (identifies the client)
     * Value = TokenBucket (tracks that client's tokens)
     *
     * WHY ConcurrentHashMap?
     * In a web server, multiple HTTP requests arrive simultaneously on different
     * threads. A regular HashMap would cause race conditions (two threads
     * modifying it at the same time = corrupted data). ConcurrentHashMap
     * uses fine-grained locking so different keys can be accessed concurrently.
     */
    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    // Configuration — in production, these would come from a config file
    private static final int MAX_TOKENS = 100;        // Bucket capacity
    private static final int REFILL_RATE = 20;        // Tokens added per second
    // This means: burst of up to 100 requests, sustained rate of 20/second

    /**
     * Checks if a request from the given API key should be allowed.
     *
     * @param apiKey the client's API key
     * @return true if the request is allowed, false if rate-limited
     *
     * computeIfAbsent: "get the bucket for this key, or create a new one if
     * it doesn't exist." This is atomic — no race condition even if two
     * requests from the same new client arrive simultaneously.
     */
    public boolean tryAcquire(String apiKey) {
        TokenBucket bucket = buckets.computeIfAbsent(apiKey,
            k -> new TokenBucket(MAX_TOKENS, REFILL_RATE));
        return bucket.tryConsume();
    }

    /**
     * Returns how many tokens are remaining for a given API key.
     * Useful for the X-RateLimit-Remaining response header.
     */
    public int getRemainingTokens(String apiKey) {
        TokenBucket bucket = buckets.get(apiKey);
        return bucket != null ? bucket.getAvailableTokens() : MAX_TOKENS;
    }

    /**
     * INNER CLASS: TokenBucket
     * ========================
     * Each client gets their own bucket instance.
     *
     * WHY IS THIS A SEPARATE CLASS?
     * Encapsulation — the bucket manages its own state (tokens, timestamps).
     * The RateLimiterService only knows to call tryConsume().
     */
    private static class TokenBucket {
        private final int capacity;       // Maximum tokens the bucket can hold
        private final int refillRate;     // Tokens added per second
        private double tokens;            // Current number of tokens (double for precision)
        private long lastRefillTime;      // Nanosecond timestamp of last refill

        TokenBucket(int capacity, int refillRate) {
            this.capacity = capacity;
            this.refillRate = refillRate;
            this.tokens = capacity;           // Start with a full bucket
            this.lastRefillTime = System.nanoTime();
        }

        /**
         * THE CORE ALGORITHM — try to consume one token.
         *
         * synchronized: only one thread at a time can enter this method
         * for THIS bucket instance. This prevents two threads from both
         * seeing "1 token left" and both consuming it (which would allow
         * one extra request).
         *
         * STEP BY STEP:
         * 1. Calculate elapsed time since last refill
         * 2. Add (elapsed × refillRate) tokens, capped at capacity
         * 3. Update the last refill timestamp
         * 4. If we have at least 1 token, consume it and return true
         * 5. Otherwise, return false (request denied)
         */
        synchronized boolean tryConsume() {
            // Step 1: How much time has passed?
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillTime) / 1_000_000_000.0;

            // Step 2: Add tokens based on elapsed time
            // Math.min ensures we never exceed capacity
            tokens = Math.min(capacity, tokens + (elapsedSeconds * refillRate));

            // Step 3: Update timestamp
            lastRefillTime = now;

            // Step 4 & 5: Try to consume
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;   // Request ALLOWED
            }
            return false;      // Request DENIED — rate limited
        }

        synchronized int getAvailableTokens() {
            // Refill before checking (same logic as tryConsume, without consuming)
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillTime) / 1_000_000_000.0;
            double available = Math.min(capacity, tokens + (elapsedSeconds * refillRate));
            return (int) available;
        }
    }
}
