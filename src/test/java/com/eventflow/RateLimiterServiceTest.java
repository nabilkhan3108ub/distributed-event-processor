package com.eventflow;

import com.eventflow.service.RateLimiterService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RateLimiterServiceTest {

    private RateLimiterService rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new RateLimiterService();
    }

    @Nested
    @DisplayName("Basic Token Consumption")
    class BasicConsumption {

        @Test
        @DisplayName("first request should always succeed (bucket starts full)")
        void firstRequest_succeeds() {
            assertTrue(rateLimiter.tryAcquire("new-client"));
        }

        @Test
        @DisplayName("should allow requests up to capacity")
        void requestsUpToCapacity_allSucceed() {
            String client = "test-client";
            int successCount = 0;
            for (int i = 0; i < 100; i++) {
                if (rateLimiter.tryAcquire(client)) {
                    successCount++;
                }
            }
            assertEquals(100, successCount,
                    "All 100 requests should succeed when bucket starts at capacity 100");
        }

        @Test
        @DisplayName("should reject request when bucket is empty")
        void emptyBucket_rejectsRequest() {
            String client = "test-client";
            for (int i = 0; i < 100; i++) {
                rateLimiter.tryAcquire(client);
            }
            assertFalse(rateLimiter.tryAcquire(client),
                    "Request #101 should be rejected when bucket is empty");
        }
    }

    @Nested
    @DisplayName("Per-Client Isolation")
    class ClientIsolation {

        @Test
        @DisplayName("different clients should have separate buckets")
        void differentClients_separateBuckets() {
            for (int i = 0; i < 100; i++) {
                rateLimiter.tryAcquire("client-A");
            }
            assertTrue(rateLimiter.tryAcquire("client-B"),
                    "Client B should not be affected by Client A's usage");
        }

        @Test
        @DisplayName("should track many clients independently")
        void manyClients_allIndependent() {
            for (int clientId = 0; clientId < 50; clientId++) {
                String client = "client-" + clientId;
                for (int req = 0; req < 10; req++) {
                    assertTrue(rateLimiter.tryAcquire(client),
                            "Client " + clientId + " request " + req + " should succeed");
                }
            }
        }
    }

    @Nested
    @DisplayName("Token Refill Behavior")
    class TokenRefill {

        @Test
        @DisplayName("tokens should refill after waiting")
        void tokensRefillOverTime() throws InterruptedException {
            String client = "refill-client";
            for (int i = 0; i < 100; i++) {
                rateLimiter.tryAcquire(client);
            }
            assertFalse(rateLimiter.tryAcquire(client));
            Thread.sleep(150);
            assertTrue(rateLimiter.tryAcquire(client),
                    "Request should succeed after waiting for token refill");
        }

        @Test
        @DisplayName("bucket should not exceed maximum capacity after long wait")
        void bucketCappedAtMaxCapacity() throws InterruptedException {
            String client = "cap-client";
            for (int i = 0; i < 10; i++) {
                rateLimiter.tryAcquire(client);
            }
            Thread.sleep(500);
            int successCount = 0;
            for (int i = 0; i < 150; i++) {
                if (rateLimiter.tryAcquire(client)) {
                    successCount++;
                }
            }
            assertTrue(successCount <= 100,
                    "Bucket should never exceed capacity. Got: " + successCount);
        }
    }

    @Nested
    @DisplayName("Concurrent Access Safety")
    class ConcurrencySafety {

        @Test
        @DisplayName("should handle concurrent requests without exceeding capacity")
        void concurrentRequests_respectCapacity() throws InterruptedException {
            String client = "concurrent-client";
            int threadCount = 20;
            int requestsPerThread = 10;

            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch endGate = new CountDownLatch(threadCount);

            AtomicInteger totalAccepted = new AtomicInteger(0);
            AtomicInteger totalRejected = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startGate.await();
                        for (int r = 0; r < requestsPerThread; r++) {
                            if (rateLimiter.tryAcquire(client)) {
                                totalAccepted.incrementAndGet();
                            } else {
                                totalRejected.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endGate.countDown();
                    }
                });
            }

            startGate.countDown();
            endGate.await();
            executor.shutdown();

            int accepted = totalAccepted.get();
            int rejected = totalRejected.get();

            assertTrue(accepted <= 110,
                    "Accepted count should not greatly exceed capacity. Got: " + accepted);
            assertTrue(rejected > 0,
                    "Some requests should have been rejected. All 200 were accepted!");
            assertEquals(200, accepted + rejected,
                    "Total should equal total requests sent");
        }
    }
}
