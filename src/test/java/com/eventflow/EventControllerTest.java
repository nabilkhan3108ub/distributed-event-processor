package com.eventflow;

import com.eventflow.controller.EventController;
import com.eventflow.model.Event;
import com.eventflow.model.AnomalyEvent;
import com.eventflow.service.EventIngestionService;
import com.eventflow.service.EventQueryService;
import com.eventflow.service.RateLimiterService;
import com.eventflow.metrics.EventFlowMetrics;
import com.eventflow.exception.GlobalExceptionHandler;
import com.eventflow.model.EventRequest;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.hasSize;

class EventControllerTest {

    private MockMvc mockMvc;
    private EventIngestionService ingestionService;
    private EventQueryService queryService;
    private RateLimiterService rateLimiterService;
    private EventFlowMetrics metrics;

    @BeforeEach
    void setUp() {
        ingestionService = mock(EventIngestionService.class);
        queryService = mock(EventQueryService.class);
        rateLimiterService = mock(RateLimiterService.class);
        metrics = mock(EventFlowMetrics.class);

        EventController controller = new EventController(
                ingestionService, queryService, rateLimiterService, metrics
        );

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Nested
    @DisplayName("POST /api/v1/events")
    class PostEvent {

        @Test
        @DisplayName("should return 202 Accepted for a valid event")
        void validEvent_returns202() throws Exception {
            when(rateLimiterService.tryAcquire(anyString())).thenReturn(true);

            Event mockEvent = Event.create("CLICK", "user-abc", Map.of("page", "/home"), "127.0.0.1");
            when(ingestionService.ingestEvent(any(EventRequest.class), anyString()))
                    .thenReturn(mockEvent);

            String requestBody = """
                {
                    "userId": "user-abc",
                    "eventType": "CLICK",
                    "source": "web-app",
                    "payload": {"page": "/home", "button": "signup"}
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-API-Key", "test-key")
                            .content(requestBody))
                    .andExpect(status().isAccepted());

            verify(ingestionService, times(1))
                    .ingestEvent(any(EventRequest.class), anyString());
        }

        @Test
        @DisplayName("should return 400 Bad Request when userId is missing")
        void missingUserId_returns400() throws Exception {
            when(rateLimiterService.tryAcquire(anyString())).thenReturn(true);

            String requestBody = """
                {
                    "eventType": "CLICK",
                    "source": "web-app"
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-API-Key", "test-key")
                            .content(requestBody))
                    .andExpect(status().isBadRequest());

            verify(ingestionService, never()).ingestEvent(any(EventRequest.class), anyString());
        }

        @Test
        @DisplayName("should return 429 Too Many Requests when rate limited")
        void rateLimited_returns429() throws Exception {
            when(rateLimiterService.tryAcquire(anyString())).thenReturn(false);

            String requestBody = """
                {
                    "userId": "user-abc",
                    "eventType": "CLICK",
                    "source": "web-app"
                }
                """;

            mockMvc.perform(post("/api/v1/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-API-Key", "test-key")
                            .content(requestBody))
                    .andExpect(status().isTooManyRequests());

            verify(ingestionService, never()).ingestEvent(any(EventRequest.class), anyString());
        }
    }

    @Nested
    @DisplayName("POST /api/v1/events/batch")
    class PostBatchEvents {

        @Test
        @DisplayName("should accept a batch of valid events")
        void validBatch_returns202() throws Exception {
            when(rateLimiterService.tryAcquire(anyString())).thenReturn(true);

            Event mockEvent = Event.create("CLICK", "user-1", Map.of(), "127.0.0.1");
            when(ingestionService.ingestEvent(any(EventRequest.class), anyString()))
                    .thenReturn(mockEvent);

            String requestBody = """
                {
                    "events": [
                        {"userId": "user-1", "eventType": "CLICK", "source": "web"},
                        {"userId": "user-2", "eventType": "VIEW", "source": "mobile"},
                        {"userId": "user-3", "eventType": "PURCHASE", "source": "web"}
                    ]
                }
                """;

            mockMvc.perform(post("/api/v1/events/batch")
                            .contentType(MediaType.APPLICATION_JSON)
                            .header("X-API-Key", "test-key")
                            .content(requestBody))
                    .andExpect(status().isAccepted());
        }
    }

    @Nested
    @DisplayName("GET /api/v1/events")
    class GetEvents {

        @Test
        @DisplayName("should return events for a valid user query")
        void validQuery_returnsEvents() throws Exception {
            List<Event> mockEvents = List.of(
                    Event.create("CLICK", "user-abc", Map.of("page", "/home"), "127.0.0.1"),
                    Event.create("VIEW", "user-abc", Map.of("page", "/products"), "127.0.0.1")
            );

            when(queryService.getEventsByUser(eq("user-abc"), nullable(Integer.class), nullable(Integer.class)))
                    .thenReturn(mockEvents);

            mockMvc.perform(get("/api/v1/events")
                            .param("userId", "user-abc")
                            .param("hours", "24")
                            .param("limit", "100"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("should return 400 when userId is missing")
        void missingUserId_returns400() throws Exception {
            mockMvc.perform(get("/api/v1/events"))
                    .andExpect(status().isBadRequest());
        }
    }

    @Nested
    @DisplayName("GET /api/v1/events/anomalies")
    class GetAnomalies {

        @Test
        @DisplayName("should return anomalies")
        void validQuery_returnsAnomalies() throws Exception {
            List<AnomalyEvent> mockAnomalies = List.of(
                    new AnomalyEvent("user-abc", "event_rate", 500.0, 10.0, 3.5, Instant.now(), "HIGH")
            );

            when(queryService.getRecentAnomalies(nullable(Integer.class), nullable(Integer.class)))
                    .thenReturn(mockAnomalies);

            mockMvc.perform(get("/api/v1/events/anomalies")
                            .param("hours", "24")
                            .param("limit", "50"))
                    .andExpect(status().isOk());
        }
    }
}
