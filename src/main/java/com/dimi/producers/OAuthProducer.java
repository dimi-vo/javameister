package com.dimi.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class OAuthProducer {


    public static Properties getProperties(String filePath) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(filePath)) {
            properties.load(input);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static void main(String[] args) {
        final String filePath = "src/main/resources/oauthproducer.properties";
        final String TOPIC = "demo-1";

        Properties props = getProperties(filePath);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {


            for (int counter = 0; counter < 24; counter++) {
                ProducerRecord<String, String> transactionalRecord = new ProducerRecord<>(TOPIC, String.valueOf(counter), "Counter = " + counter);

                System.out.println(transactionalRecord);

                producer.send(transactionalRecord, ((recordMetadata, e) -> {
                    System.out.println(transactionalRecord.key() + " written to partition: " + recordMetadata.partition());
                }));
//                sleep();
            }
        } catch (Exception e) {
            throw e;
        }


    }
}
