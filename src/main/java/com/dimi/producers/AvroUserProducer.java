package com.dimi.producers;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class AvroUserProducer {

    private static final String TOPIC = "dv_users";
    private static final Logger log = Logger.getLogger(AvroUserProducer.class.getName());

    public static Properties readConfig(final String configFile) throws IOException {
        // reads the client configuration from client.properties
        // and returns it as a Properties object
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }

        final Properties config = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            config.load(inputStream);
        }

        return config;
    }

    public static void main(String[] args) throws IOException {
        final Properties properties = readConfig("src/main/resources/avroProducer.properties");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        try (Producer<String, com.example.User> producer = new KafkaProducer<>(properties)) {
            for (int counter = 0; counter < 100; counter++) {
                com.example.User value = com.example.User.newBuilder()
                        .setName("User number " + counter)
                        .setAge(ThreadLocalRandom.current().nextInt(18, 81)) // 81 is exclusive
                        .build();

                ProducerRecord<String, com.example.User> producerRecord = new ProducerRecord<>(TOPIC, String.valueOf(counter), value);

                producer.send(producerRecord, (metadata, e) -> {
                    if (e != null) {
                        log.warning(e.getLocalizedMessage());
                        return;
                    }
                    log.info("Sent to topic: " + metadata.topic() + ", partition: " + metadata.partition());
                });

                sleep();
            }
        } catch (Exception e) {
            log.warning("Something went sideways: " + e.getLocalizedMessage());
            log.warning("Something went sideways: " + Arrays.toString(e.getStackTrace()));
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Thread Sleep interrupted: " + e.getLocalizedMessage());
        }
    }
}
