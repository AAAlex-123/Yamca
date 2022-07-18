package eventDeliverySystem.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.thread.Callback;
import eventDeliverySystem.user.User.UserStub;
import eventDeliverySystem.user.UserEvent;
import eventDeliverySystem.user.UserEvent.Tag;
import eventDeliverySystem.datastructures.Message.MessageType;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.datastructures.Topic;
import eventDeliverySystem.server.Broker;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.thread.PullThread;
import eventDeliverySystem.util.LG;
import eventDeliverySystem.util.Subscriber;

/**
 * A client-side process which is responsible for listening for a set of Topics
 * and pulling Posts from them by connecting to a remote server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 *
 * @see Broker
 */
public class Consumer extends ClientNode implements AutoCloseable, Subscriber {

	private final TopicManager topicManager;

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 * @param userStub   the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 */
	public Consumer(String serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		this(InetAddress.getByName(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 * @param userStub   the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	public Consumer(byte[] serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		this(InetAddress.getByAddress(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param ip       the InetAddress of the default broker
	 * @param port     the port of the default broker
	 * @param userStub the UserSub object that will be notified when data arrives
	 */
	private Consumer(InetAddress ip, int port, UserStub userStub) {
		super(ip, port, userStub);
		topicManager = new TopicManager();
	}

	@Override
	public void close() throws ServerException {
		topicManager.close();
	}

	/**
	 * Changes the Topics that this Consumer listens to. All connections regarding
	 * the previous Topics are closed and new ones are established.
	 *
	 * @param newTopics the new Topics to listen for
	 *
	 * @throws ServerException if an I/O error occurs while closing existing
	 *                         connections
	 */
	public void setTopics(Set<Topic> newTopics) throws ServerException {
		topicManager.close();

		for (final Topic topic : newTopics)
			listenForTopic(topic, true);
	}

	/**
	 * Returns all Posts from a Topic which have not been previously pulled.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @return a List with all the Posts not yet pulled, sorted from earliest to
	 *         latest
	 *
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public List<Post> pull(String topicName) {
		return topicManager.fetch(topicName);
	}

	/**
	 * Registers a new Topic for this Consumer to continuously fetch new Posts
	 * from by creating a new Thread that initialises that connection.
	 *
	 * @param topicName the name of the Topic to fetch from
	 */
	public void listenForNewTopic(String topicName) {
		LG.sout("listenForNewTopic(%s)", topicName);
		listenForTopic(new Topic(topicName), false);
	}

	/**
	 * Closes this Consumer's connection for a Topic by creating a new Thread that closes that
	 * connection.
	 *
	 * @param topicName the name of the Topic this Consumer already listens to
	 */
	public void stopListeningForTopic(String topicName) {
		LG.sout("Consumer#stopListeningForTopic(%s)", topicName);
		Thread thread = new StopListeningForTopicThread(topicName);
		thread.start();
	}

	/**
	 * Registers an existing Topic for this Consumer to continuously fetch new Posts
	 * from by creating a new Thread that initialises that connection.
	 *
	 * @param topic    the Topic to fetch from
	 * @param existing {@code true} is the Topic already exists, {@code false} if it has just been
	 *                 created prior to this method call
	 */
	private void listenForTopic(Topic topic, boolean existing) {
		LG.sout("Consumer#listenForTopic(%s)", topic);
		topic.subscribe(this);
		Thread thread = existing
						? new ListenForExistingTopicThread(topic)
						: new ListenForNewTopicThread(topic);
		thread.start();
	}

	@Override
	public synchronized void notify(PostInfo postInfo, String topicName) {
		LG.sout("Consumer#notify(%s, %s)", postInfo, topicName);
		// do nothing
	}

	@Override
	public synchronized void notify(Packet packet, String topicName) {
		LG.sout("Consumer#notify(%s, %s)", packet, topicName);
		if (packet.isFinal())
			userStub.fireEvent(UserEvent.successful(Tag.MESSAGE_RECEIVED, topicName));
	}

	private static class TopicManager implements AutoCloseable {

		private static class TopicData {
			private final Topic topic;
			private long pointer;
			private Socket      socket;

			public TopicData(Topic topic) {
				this.topic = topic;
				pointer = topic.getLastPostId();
				socket = null;
			}
		}

		private final Map<String, TopicData> tdMap = new HashMap<>();

		/**
		 * Returns all Posts from a Topic which have not been previously fetched.
		 *
		 * @param topicName the name of the Topic
		 *
		 * @return a List with all the Posts not yet fetched, sorted from earliest to
		 *         latest
		 *
		 * @throws NoSuchElementException if no Topic with the given name exists
		 */
		public List<Post> fetch(String topicName) {
			LG.sout("Consumer#fetch(%s)", topicName);
			LG.in();
			if (!tdMap.containsKey(topicName))
				throw new NoSuchElementException(ClientNode.getTopicDNEString(topicName));

			final TopicData td = tdMap.get(topicName);

			LG.sout("td.pointer=%d", td.pointer);
			final List<Post> newPosts = td.topic.getPostsSince(td.pointer);
			LG.sout("newPosts.size()=%d", newPosts.size());

			td.topic.clear();
			td.pointer = td.topic.getLastPostId();

			LG.out();
			return newPosts;
		}

		/**
		 * Adds a Topic to this Manager and registers its socket from where to fetch.
		 *
		 * @param topic  the Topic
		 * @param socket the socket from where it will fetch
		 *
		 * @throws IllegalArgumentException if this Manager already has a socket for a
		 *                                  Topic with the same name.
		 */
		public void addSocket(Topic topic, Socket socket) {
			LG.sout("TopicManager#addSocket(%s, %s)", topic, socket);
			add(topic);
			tdMap.get(topic.getName()).socket = socket;
		}

		/**
		 * Removes a Topic from this Manager and closes its associated socket.
		 *
		 * @param topicName the name of the Topic to remove
		 *
		 * @throws IOException if an I/O Exception occurs while closing the socket
		 * @throws NoSuchElementException if this Manager doesn't have a Topic with the given name
		 */
		public void removeSocket(String topicName) throws IOException, NoSuchElementException {
			LG.sout("TopicManager#removeSocket(%s)", topicName);
			if (!tdMap.containsKey(topicName))
				throw new NoSuchElementException(ClientNode.getTopicDNEString(topicName));

			tdMap.get(topicName).socket.close();

			tdMap.remove(topicName);
		}

		/**
		 * Partially adds a Topic to this Manager by creating its associated TopicData object.
		 *
		 * @param topic the Topic
		 *
		 * @throws IllegalArgumentException if this Manager already has a socket for a
		 *                                  Topic with the same name.
		 */
		private void add(Topic topic) {
			final String topicName = topic.getName();
			if (tdMap.containsKey(topicName))
				throw new IllegalArgumentException(ClientNode.getTopicAEString(topicName));

			tdMap.put(topicName, new TopicManager.TopicData(topic));
		}

		@Override
		public void close() throws ServerException {
			try {
				for (final TopicManager.TopicData td : tdMap.values())
					td.socket.close();
			} catch (IOException e) {
				// TODO: maybe come up with a better message
				throw new ServerException(ClientNode.CONNECTION_TO_SERVER_LOST_STRING, e);
			}

			tdMap.clear();
		}
	}

	private class ListenForNewTopicThread extends ClientThread {

		private final Topic topic;

		public ListenForNewTopicThread(Topic topic) {
			super(Tag.TOPIC_LISTENED, MessageType.INITIALISE_CONSUMER, topic.getName());
			this.topic = topic;
		}

		@Override
		protected void doWork(boolean success, Socket socket, ObjectOutputStream oos,
							  ObjectInputStream ois) throws IOException {

			if (!success) {
				socket.close();
				throw new ServerException(ClientNode.getTopicDNEString(topicName));
			}

			final Thread pullThread = new PullThread(ois, topic, (cbSuccess, cbTopicName, cbCause) -> {
				if (cbSuccess) {
					topicManager.addSocket(topic, socket);
					if (cbCause instanceof EOFException) {
						userStub.fireEvent(UserEvent.successful(Tag.TOPIC_DELETED, cbTopicName));
					} else if (cbCause instanceof SocketException) {
						userStub.fireEvent(UserEvent.successful(Tag.TOPIC_LISTEN_STOPPED, cbTopicName));
					}
				}
			});
			pullThread.start();
		}

		@Override
		protected Object getMessageValue() {
			return topic.getToken();
		}
	}

	private class ListenForExistingTopicThread extends ClientThread {

		// NOTE: only tag changes between the ListenFor???TopicThreads to differentiate between events

		private final Topic topic;

		public ListenForExistingTopicThread(Topic topic) {
			super(Tag.TOPIC_LOADED, MessageType.INITIALISE_CONSUMER, topic.getName());
			this.topic = topic;
		}

		@Override
		protected void doWork(boolean success, Socket socket, ObjectOutputStream oos,
							  ObjectInputStream ois) throws IOException {
			if (!success) {
				socket.close();
				throw new ServerException(ClientNode.getTopicDNEString(topicName));
			}

			final Thread pullThread = new PullThread(ois, topic, (cbSuccess, cbTopicName, cbCause) -> {
				if (cbSuccess) {
					topicManager.addSocket(topic, socket);
					if (cbCause instanceof EOFException) {
						userStub.fireEvent(UserEvent.successful(Tag.TOPIC_DELETED, cbTopicName));
					} else if (cbCause instanceof SocketException) {
						userStub.fireEvent(UserEvent.successful(Tag.TOPIC_LISTEN_STOPPED, cbTopicName));
					}
				}
			});
			pullThread.start();
		}

		@Override
		protected Object getMessageValue() {
			return topic.getToken();
		}
	}

	private class StopListeningForTopicThread extends Thread {

		private final Tag eventTag = Tag.TOPIC_LISTEN_STOPPED;
		private final String topicName;

		public StopListeningForTopicThread(String topicName) {
			this.topicName = topicName;
		}

		@Override
		public void run() {
			try {
				Consumer.this.topicManager.removeSocket(topicName);
			} catch (NoSuchElementException | IOException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
			}
		}
	}
}
