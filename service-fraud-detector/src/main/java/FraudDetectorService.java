import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException {
        try{
            var fraudService = new FraudDetectorService();
            var kafkaService = new KafkaService<Order>(
                    FraudDetectorService.class.getSimpleName(),
                    "ECOMMERCE_NEW_ORDER",
                    fraudService::parse,
                    Order.class,
                    Map.of()
            );
            kafkaService.run();
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws InterruptedException {
        System.out.println("----------------------");
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(1000);
        System.out.println("Order processed!");
    }

}
