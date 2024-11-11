import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table if not exists Users(" +
                "uuid varchar(200) primary key, " +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        try{
            var createUserService = new CreateUserService();
            var kafkaService = new KafkaService<>(
                    CreateUserService.class.getSimpleName(),
                    "ECOMMERCE_NEW_ORDER",
                    createUserService::parse,
                    Order.class,
                    Map.of()
            );
            kafkaService.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, SQLException {
        var order = record.value();
        System.out.println("----------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(order);
        Thread.sleep(1000);
        if(isNewUser(order.getEmail())){
            createNewUser(order.getEmail());
        }

    }

    private void createNewUser(String email) throws SQLException {
        var insertion = connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?,?)");
        insertion.setString(1, UUID.randomUUID().toString());
        insertion.setString(2, email);
        insertion.execute();
        System.out.println("New user created with email: " + email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var query = connection.prepareStatement("select uuid from Users where " +
                "email = ? limit 1");
        query.setString(1, email);
        var results = query.executeQuery();
        return !results.next();
    }


}
