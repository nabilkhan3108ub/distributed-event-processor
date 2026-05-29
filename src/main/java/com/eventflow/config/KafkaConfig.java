package com.eventflow.config;

import com.eventflow.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * KAFKA CONFIGURATION
 * ====================
 *
 * This class tells Spring how to connect to Kafka and how to
 * serialize/deserialize messages.
 *
 * WHAT IS @Configuration?
 * It tells Spring: "this class contains bean definitions."
 * A bean is just an object that Spring manages. When another class
 * needs a KafkaTemplate, Spring knows to use the one created here.
 *
 * WHAT IS @Bean?
 * It marks a method whose return value should be registered as a
 * Spring-managed object. Other classes can then inject it with @Autowired.
 *
 * KEY KAFKA CONCEPTS CONFIGURED HERE:
 *
 * 1. PRODUCER: sends messages TO Kafka
 *    - Key serializer: String (we use userId as the key for partitioning)
 *    - Value serializer: JSON (our Event object gets converted to JSON)
 *    - Acks = "all": the producer waits for ALL replicas to confirm receipt
 *      (strongest durability guarantee; slower but no data loss)
 *    - Idempotent = true: if a network retry causes the same message to be
 *      sent twice, Kafka deduplicates it (exactly-once on the producer side)
 *
 * 2. CONSUMER: reads messages FROM Kafka
 *    - Group ID: consumers in the same group share the work (each partition
 *      is read by exactly one consumer in the group)
 *    - Auto offset reset = "earliest": when a new consumer group starts,
 *      read from the beginning (don't skip old messages)
 *
 * 3. TOPICS: named channels for different event types
 *    - Partitions = 6: allows up to 6 parallel consumers
 *    - Replicas = 3: each message is stored on 3 different brokers
 *      (survives up to 2 broker failures)
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ==================== PRODUCER CONFIG ====================

    /**
     * ProducerFactory creates Kafka producer instances.
     * It's a factory pattern — instead of creating producers directly,
     * we configure a factory that can create them with the right settings.
     */
    @Bean
    public ProducerFactory<String, Event> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        // WHERE to connect (list of Kafka broker addresses)
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // HOW to convert the key (userId) into bytes for Kafka
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // HOW to convert our Event object into bytes (JSON format)
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // DURABILITY: wait for all replicas to acknowledge
        // "all" = strongest guarantee, "1" = only leader, "0" = fire and forget
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // EXACTLY-ONCE: enable idempotent producer
        // If a network glitch causes a retry, Kafka won't store the duplicate
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // PERFORMANCE: batch multiple messages before sending
        // linger.ms = 5: wait up to 5ms to collect more messages into a batch
        // This trades tiny latency for much higher throughput
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // COMPRESSION: compress batches before sending over the network
        // "lz4" is fast compression that works well in all environments
        // It reduces network bandwidth between producer and broker
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * KafkaTemplate is the main class you use to SEND messages.
     * Think of it like JdbcTemplate for databases — it wraps the complex
     * producer API into simple send() calls.
     *
     * Usage: kafkaTemplate.send("events-raw", userId, event);
     *        - "events-raw": the topic name
     *        - userId: the partition key (events for same user go to same partition)
     *        - event: the actual data (serialized to JSON automatically)
     */
    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ==================== CONSUMER CONFIG ====================

    @Bean
    public ConsumerFactory<String, Event> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // CONSUMER GROUP: all consumers with the same group ID share the work
        // Kafka assigns each partition to exactly one consumer in the group
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "eventflow-processors");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Tell the JSON deserializer which class to convert messages into
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.eventflow.model");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Event.class.getName());

        // WHERE to start reading when a new consumer group is created
        // "earliest" = from the beginning (don't miss old events)
        // "latest" = only new events (skip history)
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MANUAL COMMIT: we control when offsets are committed
        // This prevents marking messages as "processed" before they actually are
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * ConcurrentKafkaListenerContainerFactory creates the containers
     * that run our @KafkaListener methods.
     *
     * Concurrency = 3: Spring creates 3 consumer threads.
     * With 6 partitions and 3 consumers, each consumer handles 2 partitions.
     * If you scale to 6 consumers, each handles 1 partition (maximum parallelism).
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Event> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Event> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);  // 3 parallel consumer threads
        return factory;
    }

    // ==================== TOPIC DEFINITIONS ====================

    /**
     * Topics are created automatically when the application starts.
     *
     * WHY 6 PARTITIONS?
     * - More partitions = more parallelism (up to 6 consumers reading simultaneously)
     * - Too many partitions = more overhead (leader elections, memory per partition)
     * - 6 is a good starting point for a demo; production systems use 12-64
     *
     * WHY 3 REPLICAS?
     * - Each message is copied to 3 different Kafka brokers
     * - If 2 brokers crash simultaneously, your data is still safe
     * - We run a 3-broker cluster, so RF=3 means every broker has every message
     */
    @Bean
    public NewTopic rawEventsTopic() {
        return TopicBuilder.name("events-raw")
            .partitions(6)
            .replicas(3)
            .build();
    }

    @Bean
    public NewTopic enrichedEventsTopic() {
        return TopicBuilder.name("events-enriched")
            .partitions(6)
            .replicas(3)
            .build();
    }

    @Bean
    public NewTopic anomalyTopic() {
        return TopicBuilder.name("events-anomalies")
            .partitions(3)      // Fewer partitions — anomalies are rare
            .replicas(3)
            .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        /**
         * DEAD LETTER TOPIC
         * If processing an event fails (bad data, parsing error), we don't
         * want to block the pipeline. Instead, we send the failed event here
         * for later inspection. This is called a Dead Letter Queue (DLQ).
         *
         * Without a DLQ, a single bad event would cause the consumer to
         * retry forever, blocking all subsequent events in that partition.
         */
        return TopicBuilder.name("events-dead-letter")
            .partitions(3)
            .replicas(3)
            .build();
    }
}
