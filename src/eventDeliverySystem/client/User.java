package eventDeliverySystem.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.dao.IProfileDAO;
import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.client.UserEvent.Tag;
import eventDeliverySystem.util.LG;

/**
 * Facade for the different components that make up a User. Only objects of this class are needed to
 * interact with the client side of the event delivery system. The other public classes of this
 * package allow for more intricate interactions between the system and the surrounding application.
 * <p>
 * A single object of this class needs to be created on start up, which can be then be reused with
 * the help of the {@code switchToExistingProfile} and {@code switchToNewProfile} methods.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public final class User {

	private final CompositeListener listener = new CompositeListener();
	private final UserStub userStub = new UserStub();

	private final IProfileDAO profileDao;
	private Profile                 currentProfile = null;

	private final Publisher publisher;
	private final Consumer  consumer;

	/**
	 * Retrieves the user's data and saved posts, establishes the connection to the server,
	 * prepares to receive and send posts and returns the new User object.
	 *
	 * @param serverIP    the IP of the server
	 * @param serverPort  the port of the server
	 * @param profileDao  the Profile Data Access Object for this User
	 * @param profileName the name of the existing profile
	 *
	 * @return the new User
	 *
	 * @throws IOException if an I/O error occurs while interacting with the IProfileDAO object
	 * @throws ServerException      if the connection to the server could not be established
	 * @throws UnknownHostException if no IP address for the host could be found, or if a scope_id
	 * 								was specified for a global IPv6 address while resolving the
	 * 								defaultServerIP.
	 */
	public static User loadExisting(String serverIP, int serverPort, IProfileDAO profileDao,
	        String profileName) throws IOException {
		final User user = new User(serverIP, serverPort, profileDao);
		user.switchToExistingProfile(profileName);
		return user;
	}

	/**
	 * Creates a new User in the file system and returns the new User object.
	 *
	 * @param serverIP   the IP of the server
	 * @param serverPort the port of the server
	 * @param profileDao the Profile Data Access Object for this User
	 * @param name       the name of the new Profile
	 *
	 * @return the new User
	 *
	 * @throws IOException if an I/O error occurs while interacting with the IProfileDAO object
	 * @throws ServerException      if the connection to the server could not be established
	 * @throws UnknownHostException if no IP address for the host could be found, or if a scope_id
	 * 								was specified for a global IPv6 address while resolving the
	 * 								defaultServerIP.
	 */
	public static User createNew(String serverIP, int serverPort, IProfileDAO profileDao,
	        String name) throws IOException {
		final User user = new User(serverIP, serverPort, profileDao);
		user.switchToNewProfile(name);
		return user;
	}

	private User(String serverIP, int port, IProfileDAO profileDao) throws UnknownHostException {
		this.profileDao = profileDao;

		publisher = new Publisher(serverIP, port, userStub);
		consumer = new Consumer(serverIP, port, userStub);

		addUserListener(new BasicListener());
	}

	/**
	 * Returns the name of this User's current Profile.
	 *
	 * @return the current Profile's name
	 */
	public String getCurrentProfileName() {
		return currentProfile.getName();
	}

	/**
	 * Returns a Collection of this User's Topics.
	 *
	 * @return a Collection of this User's Topics
	 */
	public Collection<AbstractTopic> getAllTopics() {
		return new HashSet<>(currentProfile.getTopics());
	}

	/**
	 * Returns the number of unread Posts for a Topic.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @return the unread count for that Topic
	 */
	public int getUnreadCount(String topicName) {
		return currentProfile.getUnread(topicName);
	}

	/**
	 * Switches this User to manage a new Profile.
	 *
	 * @param profileName the name of the new Profile
	 *
	 * @throws ServerException     if the connection to the server could not be established
	 * @throws IOException if an I/O error occurs while interacting with the IProfileDAO object
	 */
	public void switchToNewProfile(String profileName) throws IOException {
		currentProfile = new Profile(profileName);
		profileDao.createNewProfile(profileName);
		consumer.setTopics(new HashSet<>(currentProfile.getTopics()));
	}

	/**
	 * Switches this User to manage an existing Profile.
	 *
	 * @param profileName the name of the existing Profile
	 *
	 * @throws ServerException     if the connection to the server could not be established
	 * @throws IOException if an I/O error occurs while interacting with the IProfileDAO object
	 * @throws NoSuchElementException if no Profile with that name exists
	 */
	public void switchToExistingProfile(String profileName) throws IOException {
		currentProfile = new Profile(profileName);
		profileDao.loadProfile(profileName).forEach(currentProfile::addTopic);
		consumer.setTopics(new HashSet<>(currentProfile.getTopics()));
	}

	/**
	 * Sends a post to a specific topic on the server. This operation fires a user event with the
	 * {@code MESSAGE_SENT} tag when it's completed. Every user that is subscribed to this Topic
	 * receives a user event with the {@code MESSAGE_RECEIVED} tag.
	 *
	 * @param post the Post to post
	 * @param topicName the name of the Topic to which to post
	 */
	public void post(Post post, String topicName) {
		LG.sout("User#post(%s, %s)", post, topicName);
		LG.in();

		if (userIsNotSubscribed(topicName)) {
			userStub.fireEvent(UserEvent.failed(Tag.MESSAGE_SENT, topicName,
					new NoSuchElementException("This User can't post to Topic " + topicName
											   + " because they aren't subscribed to it")));
		} else {
			publisher.push(post, topicName);
		}

		LG.out();
	}

	/**
	 * Creates a topic on the server. This operation fires a user event with the
	 * {@code TOPIC_CREATED} tag when it's completed.
	 *
	 * @param topicName the name of the Topic to create
	 */
	public void createTopic(String topicName) {
		LG.sout("User#createTopic(%s)", topicName);
		LG.in();

		publisher.createTopic(topicName);

		LG.out();
	}

	/**
	 * Deletes a topic on the server. This operation fires a user event with the
	 * {@code SERVER_TOPIC_DELETED} tag when it's completed. Every user that is subscribed to this
	 * Topic receives a user event with the {@code TOPIC_DELETED} tag.
	 *
	 * @param topicName the name of the Topic to delete
	 */
	public void deleteTopic(String topicName) {
		LG.sout("User#deleteTopic(%s)", topicName);
		LG.in();

		if (userIsNotSubscribed(topicName)) {
			userStub.fireEvent(UserEvent.failed(Tag.TOPIC_DELETED, topicName,
					new NoSuchElementException("This User can't delete Topic " + topicName
											   + " because they aren't subscribed to it")));
		} else {
			publisher.deleteTopic(topicName);
		}

		LG.out();
	}

	/**
	 * Pulls all new Posts from a Topic, adds them to the Profile and saves them to
	 * the file system. Posts that have already been pulled are not pulled again.
	 *
	 * @param topicName the name of the Topic from which to pull
	 *
	 * @throws IOException if an I/O error occurs while interacting with the IProfileDAO object
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void pull(String topicName) throws IOException, NoSuchElementException {
		LG.sout("User#pull from Topic '%s'", topicName);
		LG.in();
		final List<Post> newPosts = consumer.pull(topicName); // sorted from earliest to latest
		LG.sout("newPosts=%s", newPosts);
		currentProfile.updateTopic(topicName, newPosts);
		currentProfile.clearUnread(topicName);

		for (final Post post : newPosts) {
			LG.sout("Saving Post '%s'", post);
			profileDao.savePostForCurrentProfile(post, topicName);
		}

		LG.out();
	}

	/**
	 * Registers this user to listen for posts on a Topic. THis operation fires a user event with
	 * the {@code TOPIC_LISTENED} tag.
	 *
	 * @param topicName the name of the Topic to listen for
	 */
	public void listenForNewTopic(String topicName) {
		LG.sout("User#listenForNewTopic(%s)", topicName);
		LG.in();

		consumer.listenForNewTopic(topicName);

		LG.out();
	}

	/**
	 * Stops this user from listening for a Topic. This operation fires a user event with the
	 * {@code TOPIC_LISTEN_STOPPED} tag.
	 *
	 * @param topicName the name of the Topic to stop listening for
	 */
	public void stopListeningForTopic(String topicName) {
		LG.sout("User#stopListeningForTopic(%s)", topicName);
		LG.in();

		if (userIsNotSubscribed(topicName)) {
			userStub.fireEvent(UserEvent.failed(Tag.TOPIC_LISTEN_STOPPED, topicName,
					new NoSuchElementException("This User can't unsubscribe from Topic " + topicName
											   + " because they aren't subscribed to it")));
		} else {
			consumer.stopListeningForTopic(topicName);
		}

		LG.out();
	}

	/**
	 * Registers a listener to receive user events from this User.
	 *
	 * @param l the listener
	 */
	public void addUserListener(UserListener l) {
		listener.addListener(l);
	}

	private boolean userIsNotSubscribed(String topicName) {
		return currentProfile.getTopics().stream()
							 .noneMatch(topic -> topic.getName().equals(topicName));
	}

	/**
	 * Exposes the event processing capabilities of the User. Objects of this class are injected
	 * wherever needed to allow for events to be fired to the User without exposing its whole
	 * interface.
	 *
	 * @author Alex Mandelias
	 */
	final class UserStub {

		/**
		 * Fires a user event by forwarding it to its associated User.
		 *
		 * @param e the event to fire
		 */
		void fireEvent(UserEvent e) {
			switch (e.tag) {
			case MESSAGE_SENT:
				User.this.listener.onMessageSent(e);
				break;
			case MESSAGE_RECEIVED:
				User.this.listener.onMessageReceived(e);
				break;
			case TOPIC_CREATED:
				User.this.listener.onTopicCreated(e);
				break;
			case TOPIC_DELETED:
				User.this.listener.onTopicDeleted(e);
				break;
			case SERVER_TOPIC_DELETED:
				User.this.listener.onServerTopicDeleted(e);
				break;
			case TOPIC_LISTENED:
				User.this.listener.onTopicListened(e);
				break;
			case TOPIC_LOADED:
				User.this.listener.onTopicLoaded(e);
				break;
			case TOPIC_LISTEN_STOPPED:
				User.this.listener.onTopicListenStopped(e);
				break;
			default:
				throw new IllegalArgumentException(
						"You forgot to put a case for the new UserEvent#Tag enum");
			}
		}
	}

	private static class CompositeListener implements UserListener {

		private final Set<UserListener> listeners = new HashSet<>();

		void addListener(UserListener l) {
			listeners.add(l);
		}

		@Override
		public void onMessageSent(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onMessageSent(e));
		}

		@Override
		public void onMessageReceived(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onMessageReceived(e));
		}

		@Override
		public void onTopicCreated(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onTopicCreated(e));
		}

		@Override
		public void onTopicDeleted(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onTopicDeleted(e));
		}

		@Override
		public void onServerTopicDeleted(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onServerTopicDeleted(e));
		}

		@Override
		public void onTopicListened(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onTopicListened(e));
		}

		@Override
		public void onTopicLoaded(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onTopicLoaded(e));
		}

		@Override
		public void onTopicListenStopped(UserEvent e) {
			CompositeListener.log(e);

			listeners.forEach(l -> l.onTopicListenStopped(e));
		}

		private static void log(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);
		}
	}

	private final class BasicListener implements UserListener {

		@Override
		public void onMessageSent(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				LG.sout("MESSAGE FAILED TO SEND AT '%s'", e.topicName);
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onMessageReceived(UserEvent e) {
			if (e.success) {
				String topicName = e.topicName;
				currentProfile.markUnread(topicName);
				LG.sout("YOU HAVE A NEW MESSAGE AT '%s'", topicName);
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onTopicCreated(UserEvent e) {
			if (e.success) {
				listenForNewTopic(e.topicName);
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onTopicDeleted(UserEvent e) {
			if (e.success) {
				removeTopicLocally(e);
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onServerTopicDeleted(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onTopicListened(UserEvent e) {
			if (e.success) {
				currentProfile.addTopic(e.topicName);
				try {
					profileDao.createTopicForCurrentProfile(e.topicName);
				} catch (IOException e1) {
					User.this.userStub.fireEvent(UserEvent.failed(e.tag, e.topicName, e1));
				}
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onTopicLoaded(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				e.getCause().printStackTrace();
			}
		}

		@Override
		public void onTopicListenStopped(UserEvent e) {
			if (e.success) {
				removeTopicLocally(e);
			} else {
				e.getCause().printStackTrace();
			}
		}

		private void removeTopicLocally(UserEvent e) {
			currentProfile.removeTopic(e.topicName);
			try {
				profileDao.deleteTopicFromCurrentProfile(e.topicName);
			} catch (IOException e1) {
				User.this.userStub.fireEvent(UserEvent.failed(e.tag, e.topicName, e1));
			}
		}
	}
}
