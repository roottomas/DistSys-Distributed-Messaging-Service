package sd2526.trab.api.server.replication;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Subscribes to a Kafka topic and delivers records to a processor.
 */
public class KafkaSubscriber {
    private static final Logger Log = Logger.getLogger(KafkaSubscriber.class.getName());
    private static final String KAFKA_ADDR = "kafka:9092";

    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public interface RecordProcessor {
        void onReceive(long offset, String value);
    }

    public KafkaSubscriber(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDR);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "messages-rep-" + topic + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(topic));
    }

    /**
     * Starts consuming in a background thread, delivering records to the processor.
     */
    public void start(RecordProcessor processor) {
        Thread t = new Thread(() -> {
            while (running) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : records) {
                        processor.onReceive(record.offset(), record.value());
                    }
                } catch (Exception e) {
                    if (running) {
                        Log.warning("Kafka consumer error: " + e.getMessage());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }, "kafka-subscriber");
        t.setDaemon(true);
        t.start();
    }
}