package eventDeliverySystem.user;

import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.client.Consumer;
import eventDeliverySystem.client.Publisher;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.Profile;
import eventDeliverySystem.filesystem.ProfileFileSystem;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.user.UserEvent.Tag;
import eventDeliverySystem.util.LG;

/**
 * A class that manages the actions of the user by communicating with the server
 * and retrieving / committing posts to the file system.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public class User {

	private final CompositeListener listener = new CompositeListener();
	private final UserStub userStub = new UserStub(this);

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

		publisher = new Publisher(serverIP, port, userStub);
		consumer = new Consumer(serverIP, port, userStub);

		addUserListener(new MessageSentListener());
		addUserListener(new MessageReceivedListener());
		addUserListener(new CreateTopicListener());
		addUserListener(new DeleteTopicListener());
		addUserListener(new ServerDeleteTopicListener());
		addUserListener(new ListenForTopicListener());
		addUserListener(new LoadTopicListener());
		addUserListener(new StopListeningForTopicListener());
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
		LG.sout("User#post(%s, %s)", post, topicName);
		LG.in();

		if (!isUserSubscribed(topicName)) {
			userStub.fireEvent(UserEvent.failed(Tag.MESSAGE_SENT, topicName,
					new NoSuchElementException("This User can't post to Topic " + topicName
											   + " because they aren't subscribed to it")));
		} else {
			publisher.push(post, topicName);
		}

		LG.out();
	}

	/**
	 * Attempts to push a new Topic. If this succeeds,
	 * {@link #listenForNewTopic(String)} is called.
	 *
	 * @param topicName the name of the Topic to create
	 */
	public void createTopic(String topicName) {
		LG.sout("User#createTopic(%s)", topicName);
		LG.in();

		publisher.createTopic(topicName);

		LG.out();
	}

	public void deleteTopic(String topicName) {
		LG.sout("User#deleteTopic(%s)", topicName);
		LG.in();

		if (!isUserSubscribed(topicName)) {
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
	 * @throws FileSystemException    if an I/O error occurs while interacting with
	 *                                the file system
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void pull(String topicName) throws FileSystemException, NoSuchElementException {
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
	 */
	public void listenForNewTopic(String topicName) {
		LG.sout("User#listenForNewTopic(%s)", topicName);
		LG.in();

		consumer.listenForNewTopic(topicName);

		LG.out();
	}

	public void stopListeningForTopic(String topicName) {
		LG.sout("User#stopListeningForTopic(%s)", topicName);
		LG.in();

		if (!isUserSubscribed(topicName)) {
			userStub.fireEvent(UserEvent.failed(Tag.TOPIC_LISTEN_STOPPED, topicName,
					new NoSuchElementException("This User can't unsubscribe from Topic " + topicName
											   + " because they aren't subscribed to it")));
		} else {
			consumer.stopListeningForTopic(topicName);
		}

		LG.out();
	}

	public void addUserListener(UserListener l) {
		listener.addListener(l);
	}

	private boolean isUserSubscribed(String topicName) {
		return currentProfile.getTopics().stream().anyMatch(topic -> topic.getName().equals(topicName));
	}

	private void processEvent(UserEvent e) {
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
		case TOPIC_DELETED:
			listener.onTopicDeleted(e);
			break;
		case SERVER_TOPIC_DELETED:
			listener.onServerTopicDeleted(e);
			break;
		case TOPIC_LISTENED:
			listener.onTopicListened(e);
			break;
		case TOPIC_LOADED:
			listener.onTopicLoaded(e);
			break;
		case TOPIC_LISTEN_STOPPED:
			listener.onTopicListenStopped(e);
			break;
		default:
			throw new IllegalArgumentException(
					"You forgot to put a case for the new UserEvent#Tag enum");
		}
	}

	private void removeTopicLocally(UserEvent e) {
		currentProfile.removeTopic(e.topicName);
		try {
			profileFileSystem.deleteTopic(e.topicName);
		} catch (FileSystemException e1) {
			User.this.userStub.fireEvent(UserEvent.failed(e.tag, e.topicName, e1));
		}
	}

	public static class UserStub {

		private final User user;

		private UserStub(User user) {
			this.user = user;
		}

		public void fireEvent(UserEvent e) {
			user.processEvent(e);
		}
	}

	private class CompositeListener implements UserListener {

		private final Set<UserListener> listeners = new HashSet<>();

		public void addListener(UserListener l) {
			listeners.add(l);
		}

		@Override
		public void onMessageSent(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onMessageSent(e));
		}

		@Override
		public void onMessageReceived(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onMessageReceived(e));
		}

		@Override
		public void onTopicCreated(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onTopicCreated(e));
		}

		@Override
		public void onTopicDeleted(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onTopicDeleted(e));
		}

		@Override
		public void onServerTopicDeleted(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onServerTopicDeleted(e));
		}

		@Override
		public void onTopicListened(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onTopicListened(e));
		}

		@Override
		public void onTopicLoaded(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onTopicLoaded(e));
		}

		@Override
		public void onTopicListenStopped(UserEvent e) {
			LG.header("%s - %s - %s", e.tag, e.topicName, e.success);

			listeners.forEach(l -> l.onTopicListenStopped(e));
		}
	}

	private class MessageSentListener extends UserAdapter {
		@Override
		public void onMessageSent(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				LG.sout("MESSAGE FAILED TO SEND AT '%s'", e.topicName);
				e.getCause().printStackTrace();
			}
		}
	}

	private class MessageReceivedListener extends UserAdapter {
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
	}

	private class CreateTopicListener extends UserAdapter {
		@Override
		public void onTopicCreated(UserEvent e) {
			if (e.success) {
				listenForNewTopic(e.topicName);
			} else {
				e.getCause().printStackTrace();
			}
		}
	}

	private class DeleteTopicListener extends UserAdapter {
		@Override
		public void onTopicDeleted(UserEvent e) {
			if (e.success) {
				removeTopicLocally(e);
			} else {
				e.getCause().printStackTrace();
			}
		}
	}

	private class ServerDeleteTopicListener extends UserAdapter {
		@Override
		public void onServerTopicDeleted(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				e.getCause().printStackTrace();
			}
		}
	}

	private class ListenForTopicListener extends UserAdapter {
		@Override
		public void onTopicListened(UserEvent e) {
			if (e.success) {
				currentProfile.addTopic(e.topicName);
				try {
					profileFileSystem.createTopic(e.topicName);
				} catch (FileSystemException e1) {
					User.this.userStub.fireEvent(UserEvent.failed(e.tag, e.topicName, e1));
				}
			} else {
				e.getCause().printStackTrace();
			}
		}
	}

	private class LoadTopicListener extends UserAdapter {
		@Override
		public void onTopicLoaded(UserEvent e) {
			if (e.success) {
				// do nothing
			} else {
				e.getCause().printStackTrace();
			}
		}
	}

	private class StopListeningForTopicListener extends UserAdapter {
		@Override
		public void onTopicListenStopped(UserEvent e) {
			if (e.success) {
				removeTopicLocally(e);
			} else {
				e.getCause().printStackTrace();
			}
		}
	}
}
