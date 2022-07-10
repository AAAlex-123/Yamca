package eventDeliverySystem.user;

import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import app.CrappyUserUI;
import eventDeliverySystem.client.Consumer;
import eventDeliverySystem.client.Publisher;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.Profile;
import eventDeliverySystem.filesystem.ProfileFileSystem;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.util.LG;

/**
 * A class that manages the actions of the user by communicating with the server
 * and retrieving / committing posts to the file system.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public class User {

	private final BasicListener listener = new BasicListener();

	private final ProfileFileSystem profileFileSystem;
	private Profile                 currentProfile;

	private final Publisher publisher;
	private final Consumer  consumer;

	/**
	 * Retrieve the user's data and the saved posts, establish connection to the
	 * server and prepare to receive and send posts.
	 *
	 * @param serverIP              the IP of the server
	 * @param serverPort            the port of the server
	 * @param profilesRootDirectory the root directory of all the Profiles in the
	 *                              file system
	 * @param profileName           the name of the existing profile
	 *
	 * @return the new User
	 *
	 * @throws ServerException      if the connection to the server fails
	 * @throws FileSystemException  if an I/O error occurs while interacting with
	 *                              the file system
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global
	 *                              IPv6address while resolving the defaultServerIP.
	 */
	public static User loadExisting(String serverIP, int serverPort, Path profilesRootDirectory,
	        String profileName) throws ServerException, FileSystemException, UnknownHostException {
		final User user = new User(serverIP, serverPort, profilesRootDirectory);
		user.switchToExistingProfile(profileName);
		return user;
	}

	/**
	 * Creates a new User in the file system and returns a new User object.
	 *
	 * @param serverIP              the IP of the server
	 * @param serverPort            the port of the server
	 * @param profilesRootDirectory the root directory of all the Profiles in the
	 *                              file system
	 * @param name                  the name of the new Profile
	 *
	 * @return the new User
	 *
	 * @throws ServerException      if the connection to the server fails
	 * @throws FileSystemException  if an I/O error occurs while interacting with
	 *                              the file system
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global
	 *                              IPv6address while resolving the defaultServerIP.
	 */
	public static User createNew(String serverIP, int serverPort, Path profilesRootDirectory,
	        String name) throws ServerException, FileSystemException, UnknownHostException {
		final User user = new User(serverIP, serverPort, profilesRootDirectory);
		user.switchToNewProfile(name);
		return user;
	}

	private User(String serverIP, int port, Path profilesRootDirectory)
	        throws FileSystemException, UnknownHostException {
		profileFileSystem = new ProfileFileSystem(profilesRootDirectory);

		final UserStub userStub = new UserStub();
		publisher = new Publisher(serverIP, port, userStub);
		consumer = new Consumer(serverIP, port, userStub);

	}

	/**
	 * Returns this User's current Profile.
	 *
	 * @return the current Profile
	 */
	public Profile getCurrentProfile() {
		return currentProfile;
	}

	/**
	 * Switches this User to manage a new Profile.
	 *
	 * @param profileName the name of the new Profile
	 *
	 * @throws ServerException     if the connection to the server fails
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public void switchToNewProfile(String profileName) throws ServerException, FileSystemException {
		currentProfile = profileFileSystem.createNewProfile(profileName);
		consumer.setTopics(new HashSet<>(currentProfile.getTopics()));
	}

	/**
	 * Switches this User to manage an existing.
	 *
	 * @param profileName the name of an existing Profile
	 *
	 * @throws ServerException     if the connection to the server fails
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public void switchToExistingProfile(String profileName)
	        throws ServerException, FileSystemException {
		currentProfile = profileFileSystem.loadProfile(profileName);
		consumer.setTopics(new HashSet<>(currentProfile.getTopics()));
	}

	/**
	 * Posts a Post to a Topic.
	 *
	 * @param post      the Post to post
	 * @param topicName the name of the Topic to which to post
	 *
	 * @see Publisher#push(Post, String)
	 */
	public void post(Post post, String topicName) {
		publisher.push(post, topicName);
	}

	/**
	 * Attempts to push a new Topic. If this succeeds,
	 * {@link #listenForNewTopic(String)} is called.
	 *
	 * @param topicName the name of the Topic to create
	 *
	 * @return {@code true} if it was successfully created, {@code false} otherwise
	 *
	 * @throws ServerException     if the connection to the server fails
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 *
	 * @throws IllegalArgumentException if a Topic with the same name already exists
	 */
	public boolean createTopic(String topicName) throws ServerException, FileSystemException {
		LG.sout("User#createTopic(%s)", topicName);
		LG.in();

		// TODO: replace code below with listener
		final boolean success = publisher.createTopic(topicName);
		LG.sout("success=%s", success);
		if (success)
			listenForNewTopic(topicName);

		LG.out();
		return success;
	}

	public boolean deleteTopic(String topicName) throws ServerException, FileSystemException {
		LG.sout("User#deleteTopic(%s)", topicName);
		LG.in();
		final boolean success = publisher.deleteTopic(topicName);
		LG.sout("success=%s", success);
		if (success) {
			consumer.stopListeningForTopic(topicName);
			currentProfile.removeTopic(topicName);
			profileFileSystem.deleteTopic(topicName);
		}

		LG.out();
		return success;
	}

	/**
	 * Pulls all new Posts from a Topic, adds them to the Profile and saves them to
	 * the file system. Posts that have already been pulled are not pulled again.
	 *
	 * @param topicName the name of the Topic from which to pull
	 *
	 * @throws FileSystemException    if an I/O error occurs while interacting with
	 *                                the file system
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void pull(String topicName) throws FileSystemException {
		LG.sout("User#pull from Topic '%s'", topicName);
		LG.in();
		final List<Post> newPosts = consumer.pull(topicName); // sorted from earliest to latest
		LG.sout("newPosts=%s", newPosts);
		currentProfile.updateTopic(topicName, newPosts);

		for (final Post post : newPosts) {
			LG.sout("Saving Post '%s'", post);
			profileFileSystem.savePost(post, topicName);
		}
		LG.out();
	}

	/**
	 * Registers a new Topic for which new Posts will be pulled and adds it to the
	 * Profile and file system. The pulled topics will be added to the Profile and
	 * saved to the file system.
	 *
	 * @param topicName the name of the Topic to listen for
	 *
	 * @return {@code true} if the User successfully started listening to the Topic, {@code false}
	 *         otherwise
	 *
	 * @throws ServerException          if the connection to the server fails
	 * @throws FileSystemException      if an I/O error occurs while interacting
	 *                                  with the file system
	 * @throws NullPointerException     if topic == null
	 * @throws IllegalArgumentException if a Topic with the same name already exists
	 */
	public boolean listenForNewTopic(String topicName) throws ServerException, FileSystemException {
		boolean success = consumer.listenForNewTopic(topicName);
		if (success) {
			currentProfile.addTopic(topicName);
			profileFileSystem.createTopic(topicName);
		}

		return success;
	}

	public void addUserListener(UserListener l) {
		listener.addListener(l);
	}

	public void processEvent(UserEvent e) {
		switch (e.tag) {
		case MESSAGE_SENT:
			listener.onMessageSent(e);
			break;
		case MESSAGE_RECEIVED:
			listener.onMessageReceived(e);
			break;
		case TOPIC_CREATED:
			listener.onTopicCreated(e);
			break;
		default:
			throw new IllegalArgumentException(
					"You forgot to put a case for the new UserEvent#Tag enum");
		}
	}

	private class BasicListener implements UserListener {

		private final Set<UserListener> listeners = new HashSet<>();

		public void addListener(UserListener l) {
			listeners.add(l);
		}

		@Override
		public void onMessageSent(UserEvent e) {
			String topicName = e.topicName;
			if (!e.success)
				LG.sout("MESSAGE FAILED TO SEND AT '%s'", topicName);

			listeners.forEach(l -> l.onMessageSent(e));
		}

		@Override
		public void onMessageReceived(UserEvent e) {
			String topicName = e.topicName;
			currentProfile.markUnread(topicName);
			LG.sout("YOU HAVE A NEW MESSAGE AT '%s'", topicName);

			listeners.forEach(l -> l.onMessageReceived(e));
		}

		@Override
		public void onTopicCreated(UserEvent e) {
			listeners.forEach(l -> l.onTopicCreated(e));
		}
	}

	public class UserStub {

		private UserStub() {}

		public void fireEvent(UserEvent e) {
			User.this.processEvent(e);
		}
	}
}
