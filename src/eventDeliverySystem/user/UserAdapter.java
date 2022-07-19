package eventDeliverySystem.user;

/**
 * An abstract adapter class for receiving user events. The methods in this class are empty. This
 * class exists as convenience for creating listener objects.
 *
 * @author Alex Mandelias
 *
 * @see UserListener
 */
public abstract class UserAdapter implements UserListener {

    /** Constructs a UserAdapter */
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
    public void onServerTopicDeleted(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicListened(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicLoaded(UserEvent e) {
        // empty so that it can be selectively implemented
    }

    @Override
    public void onTopicListenStopped(UserEvent e) {
        // empty so that it can be selectively implemented
    }
}
