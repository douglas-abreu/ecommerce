import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) {
        try{
            var orderDispatcher = new KafkaDispatcher<Order>();
            var emailDispatcher = new KafkaDispatcher<Email>();
            var orderID = UUID.randomUUID().toString();
            var userID = UUID.randomUUID().toString();
            var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
            var order = new Order(userID, orderID, amount);
            var subject = "Thanks for your pruchase, processing email";
            var body = "Your orden has been processed successfully.";
            var email = new Email(subject, body);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", userID, order);
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userID, email);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
