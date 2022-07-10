package eventDeliverySystem.user;

public class UserEvent {

    public static UserEvent successful(Tag tag, String topicName) {
        return new UserEvent(true, tag, topicName,null);
    }

    public static UserEvent failed(Tag tag, String topicName, Exception cause) {
        return new UserEvent(false, tag, topicName, cause);
    }

    public final boolean success;
    public final Tag tag;
    public final String topicName;
    private final Exception cause;

    private UserEvent(boolean success, Tag tag, String topicName, Exception cause) {
        this.success = success;
        this.tag = tag;
        this.topicName = topicName;
        this.cause = cause;
    }

    public Exception getCause() {
        if (success)
            throw new IllegalStateException("Successful UserEvents don't have a cause");

        return cause;
    }

    public enum Tag {
        MESSAGE_SENT, MESSAGE_RECEIVED;
    }
}
