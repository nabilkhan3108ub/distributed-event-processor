package com.eventflow.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GLOBAL EXCEPTION HANDLER
 * ==========================
 *
 * WHAT IS THIS?
 * When an error occurs ANYWHERE in our controllers, Spring looks for
 * a method here that can handle that type of exception.
 *
 * Without this class, Spring returns ugly default error pages.
 * With it, we return clean, consistent JSON error responses.
 *
 * @RestControllerAdvice: combines @ControllerAdvice + @ResponseBody
 * It applies to ALL controllers (EventController, HealthController, etc.)
 *
 * @ExceptionHandler(SomeException.class): tells Spring to call this
 * method when SomeException is thrown by any controller.
 *
 * EXAMPLE:
 * If a client sends {"eventType": "", "userId": "", "payload": null}
 * Spring's @Valid check throws MethodArgumentNotValidException.
 * Our handler catches it and returns:
 * {
 *   "status": 400,
 *   "error": "Validation Failed",
 *   "messages": [
 *     "eventType: eventType is required",
 *     "userId: userId is required",
 *     "payload: payload is required"
 *   ],
 *   "timestamp": "2026-05-23T10:15:00Z"
 * }
 *
 * WHY CONSISTENT ERROR RESPONSES MATTER:
 * When all errors have the same JSON structure, API clients can
 * write ONE error-handling code path instead of handling different
 * formats for different endpoints.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles validation errors from @Valid on request bodies.
     *
     * When @NotBlank, @Size, @NotNull etc. fail, Spring throws
     * MethodArgumentNotValidException with details about each field
     * that failed validation.
     *
     * We extract those details and return a clean error response.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(
            MethodArgumentNotValidException ex) {

        // Extract error messages for each invalid field
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
            .map(field -> field.getField() + ": " + field.getDefaultMessage())
            .toList();

        log.warn("Validation failed: {}", errors);

        return ResponseEntity.badRequest().body(errorResponse(
            HttpStatus.BAD_REQUEST.value(),
            "Validation Failed",
            errors
        ));
    }

    /**
     * Handles missing required query parameters.
     *
     * Example: GET /api/v1/events without userId parameter
     * Spring throws MissingServletRequestParameterException
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<Map<String, Object>> handleMissingParam(
            MissingServletRequestParameterException ex) {

        String message = "Required parameter '" + ex.getParameterName() + "' is missing";
        log.warn(message);

        return ResponseEntity.badRequest().body(errorResponse(
            HttpStatus.BAD_REQUEST.value(),
            message,
            List.of(message)
        ));
    }

    /**
     * Handles all other unexpected exceptions.
     *
     * This is the CATCH-ALL — any exception not handled by a more
     * specific handler ends up here.
     *
     * IMPORTANT: We log the full stack trace for debugging but return
     * a generic message to the client. Never expose internal errors
     * to clients — they could reveal database names, file paths, or
     * other sensitive information.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        log.error("Unhandled exception: {}", ex.getMessage(), ex);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR.value(),
            "Internal Server Error",
            List.of("An unexpected error occurred. Please try again later.")
        ));
    }

    /**
     * Builds a consistent error response body.
     * Every error response from our API has the same structure.
     */
    private Map<String, Object> errorResponse(int status, String error, List<String> messages) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        body.put("error", error);
        body.put("messages", messages);
        body.put("timestamp", Instant.now().toString());
        return body;
    }
}
