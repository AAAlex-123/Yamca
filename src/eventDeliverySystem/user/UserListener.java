package eventDeliverySystem.user;

public interface UserListener {

    void onMessageSent(UserEvent e);

    void onMessageReceived(UserEvent e);

    void onTopicCreated(UserEvent e);

    void onTopicDeleted(UserEvent e);
}
