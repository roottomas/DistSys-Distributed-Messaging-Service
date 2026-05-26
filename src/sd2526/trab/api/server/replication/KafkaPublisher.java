package sd2526.trab.api.server.replication;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Publishes operations to a Kafka topic for state machine replication.
 */
public class KafkaPublisher {
    private static final Logger Log = Logger.getLogger(KafkaPublisher.class.getName());
    private static final String KAFKA_ADDR = "kafka:9092";

    private final KafkaProducer<String, String> producer;

    private KafkaPublisher(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public static KafkaPublisher createPublisher() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_ADDR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaPublisher(new KafkaProducer<>(props));
    }

    /**
     * Publishes an operation to the given topic.
     * 
     * @return the offset of the published record, or -1 on failure.
     */
    public long publish(String topic, String value) {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, value));
            RecordMetadata meta = future.get();
            Log.info("Published to " + topic + " offset=" + meta.offset());
            return meta.offset();
        } catch (Exception e) {
            Log.warning("Failed to publish to " + topic + ": " + e.getMessage());
            return -1;
        }
    }
}
