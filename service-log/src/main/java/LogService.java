import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws Exception {
        var logService = new LogService();
        var kafkaService = new KafkaService(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        );
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.println("----------------------");
        System.out.println("LOG " + record.topic());
        System.out.println(record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        Thread.sleep(1000);
        System.out.println("Order processed!");
    }
}
