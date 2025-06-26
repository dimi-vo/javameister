package com.dimi.producers;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ContinuousProducer {

    private static final int MESSAGE_SIZE_BYTES = 102_000 * 10; // 0.1 MB
    public static final String TOPIC = "test";
    private static final Logger log = LoggerFactory.getLogger(ContinuousProducer.class);
    private static AtomicInteger atomicRef = new AtomicInteger(1);


    private static Properties getProperties() {
        Properties props = new Properties();
//        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:19092,broker-2:19093,broker-3:19094");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "MY_CLIENT");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "1");
//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "6000");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "100");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(MESSAGE_SIZE_BYTES));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return props;
    }

    private static void createSliderUI() {
        JFrame frame = new JFrame("Kafka Producer Throttle Demo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(500, 150);

        JLabel label = new JLabel("Throughput: 0.1 MB/s (1 msg/s)", JLabel.CENTER);

        final JSlider slider = getjSlider(label);

        frame.setLayout(new BorderLayout());
        frame.add(label, BorderLayout.NORTH);
        frame.add(slider, BorderLayout.CENTER);
        frame.setVisible(true);
    }

    private static JSlider getjSlider(JLabel label) {
        JSlider slider = new JSlider(0, 10, 1); // from 0 to 100 MB/s in 0.1 increments
        slider.setMajorTickSpacing(10);
        slider.setMinorTickSpacing(1);
        slider.setPaintLabels(true);
        slider.setPaintTicks(true);

        slider.addChangeListener(e -> {
            atomicRef.set(slider.getValue());
            label.setText(String.format("Messages per second: %d", atomicRef.get()));
        });
        return slider;
    }

    public static void main(String[] args) {
        ProducerPerformance producerPerformance = new ProducerPerformance();
        Properties props = getProperties();


        SwingUtilities.invokeLater(ContinuousProducer::createSliderUI);

        new Thread(() -> producerPerformance.init(props, atomicRef)).start();
    }

}
