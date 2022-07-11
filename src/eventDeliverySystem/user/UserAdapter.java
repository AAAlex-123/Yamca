package eventDeliverySystem.user;

public abstract class UserAdapter implements UserListener {

    protected UserAdapter() {}

    @Override
    public void onMessageSent(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onMessageReceived(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicCreated(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicDeleted(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicListened(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicListenStopped(UserEvent e) {
        // empty so that it can be selectively implemented
    }
}
