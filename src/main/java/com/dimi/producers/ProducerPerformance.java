package com.dimi.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dimi.producers.ThrottleUtils.printMetrics;

public class ProducerPerformance {


    public void init(Properties properties, AtomicInteger messagesPerSecond) {
        System.out.println(messagesPerSecond.get());
        ProducerPerformance perf = new ProducerPerformance();
        perf.start(properties, messagesPerSecond);
    }

    void start(Properties properties, AtomicInteger messagesPerSecond) {

        ConfigPostProcessor config = new ConfigPostProcessor();
        KafkaProducer<Long, byte[]> producer = createKafkaProducer(properties);

        /* setup perf test */
        byte[] payload = new byte[config.recordSize];
        // not thread-safe, do not share with other threads
        SplittableRandom random = new SplittableRandom(0);
        ProducerRecord<Long, byte[]> record;
        stats = new Stats(config.numRecords, 5000);

        ThroughputThrottler throttler = new ThroughputThrottler(() -> messagesPerSecond, System.currentTimeMillis());

        for (long i = 0; i < config.numRecords; i++) {

            payload = generateRandomPayload(config.recordSize, payload, random, config.payloadMonotonic, i);

            record = new ProducerRecord<>(ContinuousProducer.TOPIC, i, payload);

            long sendStartMs = System.currentTimeMillis();
            cb = new PerfCallback(sendStartMs, payload.length, stats);
            producer.send(record, cb);

            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }

        printProducerMetrics(config, producer);
    }

    private void printProducerMetrics(ConfigPostProcessor config, KafkaProducer<Long, byte[]> producer) {
        if (!config.shouldPrintMetrics) {
            producer.close();

            stats.printTotal();
        } else {
            // Make sure all messages are sent before printing out the stats and the metrics
            // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
            // expects this class to work with older versions of the client jar that don't support flush().
            producer.flush();

            /* print final results */
            stats.printTotal();

            /* print out metrics */
            printMetrics(producer.metrics());
            producer.close();
        }
    }

    KafkaProducer<Long, byte[]> createKafkaProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    Callback cb;

    Stats stats;

    static byte[] generateRandomPayload(Integer recordSize, byte[] payload,
                                        SplittableRandom random, boolean payloadMonotonic, long recordValue) {
        if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else if (payloadMonotonic) {
            payload = Long.toString(recordValue).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("no payload File Path or record Size or payload-monotonic option provided");
        }
        return payload;
    }

    // Visible for testing
    static class Stats {
        private final long start;
        private final int[] latencies;
        private final long sampling;
        private final long reportingInterval;
        private long iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long windowStart;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = numRecords / Math.min(numRecords, 500000);
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (this.iteration % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public long totalCount() {
            return this.count;
        }

        public long currentWindowCount() {
            return this.windowCount;
        }

        public long iteration() {
            return this.iteration;
        }

        public long bytes() {
            return this.bytes;
        }

        public int index() {
            return this.index;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    static final class PerfCallback implements Callback {
        private final long start;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            // It will only be counted when the sending is successful, otherwise the number of sent records may be
            // magically printed when the sending fails.
            if (exception == null) {
                this.stats.record(latency, bytes, now);
                this.stats.iteration++;
            }
            if (exception != null)
                exception.printStackTrace();
        }
    }

    static final class ConfigPostProcessor {
        final Long numRecords;
        final Integer recordSize;
        final boolean payloadMonotonic;
        final boolean shouldPrintMetrics;

        public ConfigPostProcessor() {
            this.numRecords = 10_000L;
            this.recordSize = 102_000; //0.1MB
            this.payloadMonotonic = true;
            this.shouldPrintMetrics = true;
        }
    }
}
