package eventDeliverySystem.client;

/**
 * An event that indicates that a user-related event has occurred.
 * <p>
 * A {@code UserEvent} is passed to every {@code UserListener} or {@code UserAdapter} object which
 * is registered to receive such user events using the User's {@code addUserListener} method. Each
 * such listener receives that {@code UserEvent} which describes the event.
 * </p>
 *
 * @author Alex Mandelias
 */
public final class UserEvent {

    /**
     * Creates a successful user event with the given Tag that is associated with a Topic.
     *
     * @param tag the Tag of the event
     * @param topicName the name of the Topic the event is associated with
     *
     * @return the successful event
     */
    static UserEvent successful(Tag tag, String topicName) {
        return new UserEvent(true, tag, topicName,null);
    }

    /**
     * Creates a failed user event with the given Tag that is associated with a Topic.
     *
     * @param tag the Tag of the event
     * @param topicName the name of the Topic the event is associated with
     * @param cause the Throwable that caused the underlying operation to fire a failed event. This
     *              Throwable can later be retrieved with the {@code getCause()} method.
     *
     * @return the successful event
     */
    static UserEvent failed(Tag tag, String topicName, Throwable cause) {
        return new UserEvent(false, tag, topicName, cause);
    }

    /** Represents whether the underlying operation that fired this event was successful or not */
    public final boolean success;

    /**
     * Indicates the type of this event. Each Tag invokes a different methods on the Listener.
     *
     * @see Tag
     */
    public final Tag tag;

    /** The name of the Topic associated this event is associated with */
    public final String topicName;

    private final Throwable cause;

    private UserEvent(boolean success, Tag tag, String topicName, Throwable cause) {
        this.success = success;
        this.tag = tag;
        this.topicName = topicName;
        this.cause = cause;
    }

    /**
     * Returns the Throwable that caused the underlying operation to fire this failed event.
     *
     * @return the cause
     *
     * @throws IllegalStateException if this event is not failed
     */
    public Throwable getCause() {
        if (success)
            throw new IllegalStateException("Successful UserEvents don't have a cause");

        return cause;
    }

    /**
     * Represents the type of the user event. Events with different tags cause different Listener
     * methods to be called.
     *
     * @author Alex Mandelias
     */
    public enum Tag {

        /** Used with events fired when the user has sent a message */
        MESSAGE_SENT,

        /** Used with events fired when the user receives a message from the server */
        MESSAGE_RECEIVED,

        /** Used with events fired when the user has created a topic */
        TOPIC_CREATED,

        /** Used with events fired when the user is notified a listened topic has been deleted */
        TOPIC_DELETED,

        /** Used with events fired when the user deleted a topic from the server */
        SERVER_TOPIC_DELETED,

        /** Used with events fired when the user has listened to a topic */
        TOPIC_LISTENED,

        /** Used with events fired when the user has loaded a topic for the first time */
        TOPIC_LOADED,

        /** Used with events fired when the user has stopped listening to a topic */
        TOPIC_LISTEN_STOPPED;
    }
}
