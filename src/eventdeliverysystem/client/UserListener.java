package eventdeliverysystem.client;

/**
 * The listener interface for receiving events on a User.
 * <p>
 * The class that is interested in processing a user event either implements this interface or
 * extends the abstract {@code UserAdapter} class, overriding only the methods of interest.
 * <p>
 * The listener object created from that class is then registered with a User using the User's
 * {@code addUserListener} method. A user event is generated whenever a User operation (such as
 * sending a message, creating a topic or listening to a topic) terminates (either successfully or
 * unsuccessfully) or when the server communicates with the User (such as receiving a message, or
 * having a topic deleted from another User). When a user event occurs, the relevant method in the
 * listener object is invoked, and the {@code UserEvent} is passed to it.
 *
 * @author Alex Mandelias
 */
public interface UserListener {

	/**
	 * Called when a Message has been sent.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onMessageSent(UserEvent e);

	/**
	 * Called when a Message has been received.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onMessageReceived(UserEvent e);

	/**
	 * Called when a Topic has been created.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onTopicCreated(UserEvent e);

	/**
	 * Called when a Topic has been deleted locally following a deletion from the server.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onTopicDeleted(UserEvent e);

	/**
	 * Called when a Topic has been deleted from the server.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onServerTopicDeleted(UserEvent e);

	/**
	 * Called when a Topic has been listened to.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onTopicListened(UserEvent e);

	/**
	 * Called when a Topic has been loaded for the first time.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onTopicLoaded(UserEvent e);

	/**
	 * Called when a Topic is no longer being listened to.
	 *
	 * @param e the UserEvent associated with that event
	 */
	void onTopicListenStopped(UserEvent e);
}
