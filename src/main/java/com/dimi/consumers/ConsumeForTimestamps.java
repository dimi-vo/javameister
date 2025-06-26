package com.dimi.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConsumeForTimestamps {
    private static final String TOPIC = "demo";
    private static final Logger log = LoggerFactory.getLogger(ConsumeForTimestamps.class.getName());

    private static Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "MY_CLIENT");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        return props;
    }

    public static void main(String[] args) {
        final Properties properties = getProperties();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // Get all partitions for the topic
            List<PartitionInfo> partitions = consumer.partitionsFor(TOPIC);
            List<TopicPartition> topicPartitions = partitions
                    .stream()
                    .map(p -> new TopicPartition(TOPIC, p.partition()))
                    .collect(Collectors.toList());

            // Get start of today in UTC
            OffsetDateTime startOfDayUtc = OffsetDateTime.now(ZoneOffset.UTC).toLocalDate().atStartOfDay().plusDays(1).atOffset(ZoneOffset.UTC);
            long epochMillis = startOfDayUtc.toInstant().toEpochMilli();

            log.info("Start of day (UTC): {}, epochMillis: {}", startOfDayUtc, epochMillis);

            // Build map for offset lookup
            Map<TopicPartition, Long> timestampsToSearch = topicPartitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> epochMillis));

            // Fetch offsets for the timestamp
            Map<TopicPartition, OffsetAndTimestamp> startOffsets = consumer.offsetsForTimes(timestampsToSearch);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            long totalMessagesSinceStartOfDay = 0;

            for (TopicPartition tp : topicPartitions) {
                OffsetAndTimestamp offsetAndTimestamp = startOffsets.get(tp);
                Long endOffset = endOffsets.get(tp);

                if (offsetAndTimestamp == null || endOffset == null) {
                    log.warn("Missing offset info for partition {}. Skipping.", tp);
                    continue;
                }

                long startOffset = offsetAndTimestamp.offset();
                long messageCount = endOffset - startOffset;

                log.info("Partition {}: startOffset={}, endOffset={}, count={}",
                        tp.partition(), startOffset, endOffset, messageCount);

                totalMessagesSinceStartOfDay += messageCount;
            }

            log.info("Total messages produced since start of day across all partitions: {}", totalMessagesSinceStartOfDay);

            if (totalMessagesSinceStartOfDay <= 500) {
                log.info("Message count is within expected threshold (<= 500).");
            } else {
                log.warn("Message count exceeds expected threshold (> 500)!");
            }

        } catch (Exception e) {
            log.warn("Error while checking daily message count: {}", e.getLocalizedMessage());
        }
    }
}
