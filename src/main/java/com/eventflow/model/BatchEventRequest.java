package com.eventflow.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * BATCH EVENT REQUEST
 * ====================
 * Allows clients to send up to 100 events in a single HTTP request.
 *
 * WHY BATCHING?
 * Every HTTP request has overhead: TCP handshake, TLS negotiation, HTTP headers.
 * If a client needs to send 1,000 events, making 1,000 separate HTTP calls
 * wastes network round-trips. With batching, they make 10 calls of 100 events each.
 *
 * This is exactly how systems at Netflix and Google work — their SDKs
 * buffer events locally and flush them in batches.
 *
 * @Valid on the list means: validate EACH EventRequest in the list individually.
 * If event #47 in a batch of 100 has a missing userId, the whole batch is rejected.
 * This is a design choice — you could alternatively accept partial batches.
 */
public record BatchEventRequest(

    @NotEmpty(message = "events list must not be empty")
    @Size(max = 100, message = "batch size must not exceed 100 events")
    @Valid  // Validate each individual EventRequest inside the list
    List<EventRequest> events
) {}
