public class Email {
    private final String subject, body;

    public Email(String header, String body) {
        this.subject = header;
        this.body = body;
    }
}
