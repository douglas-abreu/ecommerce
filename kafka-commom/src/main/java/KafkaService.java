import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;
    private final Class<T> type;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> config ) {
        this(parse, groupId, type, config);
        try{
            consumer.subscribe(Collections.singletonList(topic));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> config) {
        this(parse, groupId, type, config);
        try{
            consumer.subscribe(topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> config) {
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, type, config));
    }

    private Properties getProperties(String groupId, Class<T> type, Map<String, String> config) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(config);
        return properties;
    }

    public void run() throws Exception {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
