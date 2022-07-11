package eventDeliverySystem.user;

public interface UserListener {

    void onMessageSent(UserEvent e);

    void onMessageReceived(UserEvent e);

    void onTopicCreated(UserEvent e);

    void onTopicDeleted(UserEvent e);

    void onTopicListened(UserEvent e);

    void onTopicLoaded(UserEvent e);

    void onTopicListenStopped(UserEvent e);
}
