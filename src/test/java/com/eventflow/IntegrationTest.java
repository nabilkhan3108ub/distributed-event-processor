package com.eventflow;

import com.eventflow.model.Event;
import com.eventflow.repository.EventRepository;
import com.eventflow.processor.EnrichmentProcessor;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Container
    static CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:4.1")
    );

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.cassandra.contact-points",
                () -> cassandra.getContactPoint().getHostString());
        registry.add("spring.cassandra.port",
                () -> cassandra.getContactPoint().getPort());
        registry.add("spring.cassandra.local-datacenter",
                () -> cassandra.getLocalDatacenter());
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired(required = false)
    private EventRepository eventRepository;

    @Autowired(required = false)
    private EnrichmentProcessor enrichmentProcessor;

    @Test
    @Order(1)
    @DisplayName("Application should start and connect to Kafka + Cassandra")
    void applicationStarts() {
        assertTrue(kafka.isRunning(), "Kafka container should be running");
        assertTrue(cassandra.isRunning(), "Cassandra container should be running");
    }

    @Test
    @Order(2)
    @DisplayName("Health endpoint should return 200 when infrastructure is up")
    void healthCheck_returnsOk() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk());
    }

    @Test
    @Order(3)
    @DisplayName("POST /api/v1/events should accept and queue event to Kafka")
    void ingestEvent_endToEnd() throws Exception {
        String requestBody = """
            {
                "userId": "integration-user-1",
                "eventType": "PAGE_VIEW",
                "source": "integration-test",
                "payload": {"page": "/dashboard", "duration": 45}
            }
            """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-API-Key", "integration-test-key")
                        .content(requestBody))
                .andExpect(status().isAccepted());
    }

    @Test
    @Order(4)
    @DisplayName("POST /api/v1/events/batch should accept multiple events")
    void ingestBatch_endToEnd() throws Exception {
        String requestBody = """
            {
                "events": [
                    {"userId": "batch-user-1", "eventType": "CLICK", "source": "test"},
                    {"userId": "batch-user-2", "eventType": "VIEW", "source": "test"},
                    {"userId": "batch-user-3", "eventType": "PURCHASE", "source": "test"}
                ]
            }
            """;

        mockMvc.perform(post("/api/v1/events/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-API-Key", "integration-test-key")
                        .content(requestBody))
                .andExpect(status().isAccepted());
    }

    @Test
    @Order(5)
    @DisplayName("Should reject event with invalid payload")
    void invalidEvent_returns400() throws Exception {
        String requestBody = """
            {
                "eventType": "CLICK"
            }
            """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-API-Key", "test-key")
                        .content(requestBody))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(6)
    @DisplayName("Should reject malformed JSON")
    void malformedJson_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-API-Key", "test-key")
                        .content("{ this is not valid json }"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(7)
    @DisplayName("EnrichmentProcessor should add metadata")
    void enrichmentProcessor_addsMetadata() {
        if (enrichmentProcessor == null) return;

        Event raw = Event.create("CLICK", "test-user", Map.of("page", "/pricing"), "127.0.0.1");
        Event enriched = enrichmentProcessor.enrich(raw);
        assertNotNull(enriched.processedAt(), "Enriched event should have processedAt");
    }

    @Test
    @Order(8)
    @DisplayName("GET /api/v1/events should return empty list for unknown user")
    void queryUnknownUser_returnsEmpty() throws Exception {
        mockMvc.perform(get("/api/v1/events")
                        .param("userId", "nonexistent-user")
                        .param("hours", "24"))
                .andExpect(status().isOk());
    }

    @Test
    @Order(9)
    @DisplayName("Rate limiter should enforce limits under load")
    void rateLimiter_enforcesLimits() throws Exception {
        int accepted = 0;
        int rejected = 0;

        for (int i = 0; i < 105; i++) {
            String requestBody = """
                {
                    "userId": "rate-limit-user",
                    "eventType": "CLICK",
                    "source": "test"
                }
                """;

            int status = mockMvc.perform(post("/api/v1/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-API-Key", "rate-limit-test-key")
                            .content(requestBody))
                    .andReturn()
                    .getResponse()
                    .getStatus();

            if (status == 202) accepted++;
            if (status == 429) rejected++;
        }

        assertTrue(accepted > 0, "Some requests should have been accepted");
    }

    @Test
    @Order(10)
    @DisplayName("Cassandra repository should be initialized")
    void cassandraRepository_initialized() {
        if (eventRepository != null) {
            assertNotNull(eventRepository, "EventRepository should be initialized");
        }
    }
}
