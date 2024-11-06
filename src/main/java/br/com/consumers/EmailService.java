package br.com.consumers;

import br.com.kafka.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

    public static void main(String[] args) throws InterruptedException {
        var emailService = new EmailService();
        var kafkaService = new KafkaService(
            EmailService.class.getSimpleName(),
            "ECOMMERCE_SEND_EMAIL",
            emailService::parse
        );
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) throws InterruptedException {
        System.out.println("----------------------");
        System.out.println("Sending email.");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        Thread.sleep(1000);
        System.out.println("Email Sent");
    }

}
