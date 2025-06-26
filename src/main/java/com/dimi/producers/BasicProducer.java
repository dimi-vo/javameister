package com.dimi.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class BasicProducer {

    private static final String TOPIC = "test";
    private static final Logger log = Logger.getLogger(BasicProducer.class.getName());

    private static Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "MY_CLIENT");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        return props;
    }

    public static void main(String[] args) {
        final Properties properties = getProperties();

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int counter = 0; counter < 1000; counter++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(counter), "Counter = " + counter);

                producer.send(record, ((recordMetadata, e) -> {
                    if (e != null) {
                        System.out.println(e.getLocalizedMessage());
                        return;
                    }
                    System.out.println("Topic: " + record.topic() + " - Partition " + record.value());
                }));
                sleep();
            }
        } catch (Exception e) {
            System.out.println("Something went sideways: {}" + e.getLocalizedMessage());
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread Sleep interrupted: {}" + e.getLocalizedMessage());
        }
    }
}
