package com.dimi.producers.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class NonTransactionalProducer {

    private final String TOPIC;
    private static final Logger log = Logger.getLogger(NonTransactionalProducer.class.getName());

    private final int millis;

    public NonTransactionalProducer(int millis, String topic) {
        this.millis = millis;
        this.TOPIC = topic;

    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public void send() {
        final Properties properties = getProperties();

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> transactionalRecord = new ProducerRecord<>(TOPIC, "non-transactional-message", "[producer,message]:" + "producer" + "," + i);
                producer.send(transactionalRecord, ((recordMetadata, e) -> {
                    System.out.println(transactionalRecord.key() + " written to partition: " + recordMetadata.partition());
                }));
                sleep();
            }

        } catch (Exception e) {
            System.out.println("Something went sideways: {}" + e.getLocalizedMessage());
        }
    }

    private void sleep() {
        if (this.millis == 0) return;
        try {
            Thread.sleep(this.millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread Sleep interrupted: {}" + e.getLocalizedMessage());
        }
    }

}
