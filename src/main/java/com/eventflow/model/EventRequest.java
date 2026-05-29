package com.eventflow.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Map;

/**
 * EVENT REQUEST DTO (Data Transfer Object)
 * =========================================
 *
 * WHAT IS A DTO?
 * A DTO is a simple object that carries data between layers. It's different
 * from our Event model because:
 * - Event is our INTERNAL model (has server-generated fields like id, timestamp)
 * - EventRequest is our EXTERNAL model (only has fields the client should send)
 *
 * WHY SEPARATE THEM?
 * Security and clarity. If we used Event directly as the API input:
 * 1. A client could set their own ID (security risk)
 * 2. A client could set sourceIp to someone else's IP (spoofing)
 * 3. A client could set processedAt to bypass our pipeline
 *
 * By using a separate DTO, the client can ONLY send what we allow.
 *
 * VALIDATION ANNOTATIONS:
 * These annotations (from Jakarta Validation) automatically check the input.
 * If any check fails, Spring returns a 400 Bad Request with a clear error message.
 * The developer doesn't write any if/else validation code — the framework handles it.
 *
 * @NotBlank: the field must not be null AND must contain at least one non-whitespace character
 * @NotNull: the field must not be null (but can be empty)
 * @Size: limits the length of strings or size of collections
 */
public record EventRequest(

    @NotBlank(message = "eventType is required")
    @Size(max = 50, message = "eventType must be under 50 characters")
    String eventType,

    @NotBlank(message = "userId is required")
    @Size(max = 100, message = "userId must be under 100 characters")
    String userId,

    @NotNull(message = "payload is required")
    Map<String, Object> payload
) {}
