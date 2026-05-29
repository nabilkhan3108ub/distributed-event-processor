package com.eventflow.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * CASSANDRA CONFIGURATION
 * ========================
 *
 * This class:
 * 1. Creates a connection (session) to our Cassandra cluster
 * 2. Creates the keyspace and tables if they don't exist
 * 3. Retries connection if Cassandra isn't ready yet
 *
 * CASSANDRA TERMINOLOGY:
 * - Keyspace: like a "database" in MySQL. Contains tables.
 * - Table: like a SQL table, but the design rules are VERY different.
 * - Session: a connection pool to the cluster. One session per application.
 *   (Creating sessions is expensive, so we reuse one.)
 *
 * CRITICAL CONCEPT: CASSANDRA DATA MODELING
 * ==========================================
 * In SQL, you design tables based on the DATA (normalization — remove duplicates).
 * In Cassandra, you design tables based on the QUERIES you need to answer.
 *
 * This means:
 * - It's NORMAL to have the same data in multiple tables (denormalization)
 * - Each table is optimized for ONE specific query pattern
 * - JOINs don't exist — if you need to query data two ways, you store it twice
 *
 * OUR SCHEMA HAS 3 TABLES:
 *
 * 1. events_by_user: for "get all events for user X in time range Y-Z"
 *    Partition key = (user_id, time_bucket)
 *    Clustering column = timestamp DESC
 *
 * 2. aggregations: for "get hourly event counts for user X"
 *    Pre-computed by our AggregationProcessor.
 *    Uses COUNTER columns for distributed atomic counting.
 *
 * 3. anomalies: for "get all anomalies detected in the last hour"
 *    Partition key = time_bucket (hourly), so recent anomalies are fast to query.
 */
@Configuration
public class CassandraConfig {

    private static final Logger log = LoggerFactory.getLogger(CassandraConfig.class);

    // Maximum number of times to retry connecting to Cassandra.
    // Cassandra can take 60-90 seconds to start in Docker.
    private static final int MAX_RETRIES = 10;
    private static final int RETRY_DELAY_SECONDS = 10;

    @Value("${cassandra.contact-points:localhost}")
    private String contactPoints;

    @Value("${cassandra.port:9042}")
    private int port;

    @Value("${cassandra.datacenter:datacenter1}")
    private String datacenter;

    @Value("${cassandra.keyspace:eventflow}")
    private String keyspace;

    private CqlSession session;

    /**
     * Creates a driver config with generous timeouts.
     * Default is 2 seconds which is too short when Cassandra is still warming up.
     */
    private DriverConfigLoader buildDriverConfig() {
        return DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
            .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(30))
            .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(30))
            .build();
    }

    /**
     * Creates and returns the Cassandra session with retry logic.
     *
     * INTERVIEW TALKING POINT: "In a containerized environment, services
     * start in parallel. Even with depends_on health checks, Cassandra
     * might not be fully ready when the app connects. We implement
     * retry-with-backoff to handle this gracefully."
     */
    @Bean
    public CqlSession cqlSession() {
        log.info("Connecting to Cassandra at {}:{}", contactPoints, port);

        DriverConfigLoader config = buildDriverConfig();

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.info("Cassandra connection attempt {}/{}", attempt, MAX_RETRIES);

                // First, connect WITHOUT a keyspace to create it
                CqlSession initSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPoints, port))
                    .withLocalDatacenter(datacenter)
                    .withConfigLoader(config)
                    .build();

                // Create keyspace with replication
                initSession.execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                    " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
                );

                initSession.close();

                // Now connect WITH the keyspace
                session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(contactPoints, port))
                    .withLocalDatacenter(datacenter)
                    .withKeyspace(keyspace)
                    .withConfigLoader(config)
                    .build();

                createTables();
                log.info("Cassandra connected and schema initialized on attempt {}", attempt);
                return session;

            } catch (Exception e) {
                log.warn("Cassandra connection attempt {}/{} failed: {}",
                        attempt, MAX_RETRIES, e.getMessage());

                if (attempt == MAX_RETRIES) {
                    throw new RuntimeException(
                        "Failed to connect to Cassandra after " + MAX_RETRIES + " attempts", e);
                }

                try {
                    log.info("Waiting {} seconds before retry...", RETRY_DELAY_SECONDS);
                    Thread.sleep(RETRY_DELAY_SECONDS * 1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting to retry Cassandra", ie);
                }
            }
        }

        throw new RuntimeException("Failed to connect to Cassandra");
    }

    /**
     * Creates all tables. Safe to call multiple times (IF NOT EXISTS).
     */
    private void createTables() {

        // TABLE 1: events_by_user
        session.execute("""
            CREATE TABLE IF NOT EXISTS events_by_user (
                user_id       TEXT,
                time_bucket   TEXT,
                event_timestamp TIMESTAMP,
                event_id      UUID,
                event_type    TEXT,
                payload       TEXT,
                source_ip     TEXT,
                processed_at  TIMESTAMP,
                PRIMARY KEY ((user_id, time_bucket), event_timestamp, event_id)
            ) WITH CLUSTERING ORDER BY (event_timestamp DESC, event_id ASC)
              AND compaction = {'class': 'TimeWindowCompactionStrategy',
                                'compaction_window_unit': 'DAYS',
                                'compaction_window_size': 1}
            """);

        // TABLE 2: aggregations (COUNTER table)
        // IMPORTANT: Counter tables can ONLY have primary key columns + counter columns.
        // No other column types allowed. This is a Cassandra rule.
        session.execute("""
            CREATE TABLE IF NOT EXISTS aggregations (
                user_id       TEXT,
                event_type    TEXT,
                time_bucket   TEXT,
                event_count   COUNTER,
                PRIMARY KEY ((user_id, event_type, time_bucket))
            )
            """);

        // TABLE 3: anomalies
        session.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                time_bucket   TEXT,
                detected_at   TIMESTAMP,
                anomaly_id    UUID,
                user_id       TEXT,
                metric        TEXT,
                observed_value DOUBLE,
                expected_value DOUBLE,
                z_score       DOUBLE,
                severity      TEXT,
                PRIMARY KEY (time_bucket, detected_at, anomaly_id)
            ) WITH CLUSTERING ORDER BY (detected_at DESC, anomaly_id ASC)
            """);

        log.info("All Cassandra tables created successfully");
    }

    @PreDestroy
    public void close() {
        if (session != null && !session.isClosed()) {
            session.close();
            log.info("Cassandra session closed");
        }
    }
}
