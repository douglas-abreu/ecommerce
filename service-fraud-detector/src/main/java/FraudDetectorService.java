import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
        } catch (RuntimeException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private final KafkaDispatcher<Order> orderDispacher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, ExecutionException {
        var order = record.value();
        System.out.println("----------------------");
        System.out.println(order);
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        Thread.sleep(1000);
        if(isFraud(order)){
            System.out.println("Fraud detected!!!");
            orderDispacher.send(
                    "ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Order processed!");
            orderDispacher.send(
                    "ECOMMERCE_ORDER_ACCEPTED", order.getUserId(), order);
        }

    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
