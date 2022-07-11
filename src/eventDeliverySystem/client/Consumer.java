package eventDeliverySystem.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.user.User.UserStub;
import eventDeliverySystem.user.UserEvent;
import eventDeliverySystem.user.UserEvent.Tag;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
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

	private final UserStub userStub;
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
		super(ip, port);
		topicManager = new TopicManager();
		this.userStub = userStub;
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
			listenForTopic(topic);
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
		listenForTopic(new Topic(topicName));
	}

	/**
	 * Closes this Consumer's connection for a Topic.
	 *
	 * @param topicName the name of the Topic this Consumer already listens to
	 *
	 * @return {@code true} if this Consumer successfully started listening to the Topic,
	 *         {@code false} if no Topic with such name exists
	 *
	 * @throws ServerException          if a connection to the server fails
	 * @throws NoSuchElementException if this Consumer does not listen to a Topic with the given name
	 */
	public void stopListeningForTopic(String topicName) throws ServerException, NoSuchElementException {
		try {
			topicManager.removeSocket(topicName);
		} catch (IOException e) {
			throw new ServerException(topicName, e);
		}
	}

	/**
	 * Registers an existing Topic for this Consumer to continuously fetch new Posts
	 * from by creating a new Thread that initialises that connection.
	 *
	 * @param topic Topic to fetch from
	 */
	private void listenForTopic(Topic topic) {
		LG.sout("Consumer#listenForTopic(%s)", topic);
		topic.subscribe(this);
		Thread thread = new ListenForTopicThread(topic);
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
			private long        pointer;
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
				throw new NoSuchElementException("No Topic with name " + topicName + " found");

			final TopicData td = tdMap.get(topicName);

			LG.sout("td.pointer=%d", td.pointer);
			final List<Post> newPosts = td.topic.getPostsSince(td.pointer);

			LG.sout("newPosts.size()=%d", newPosts.size());
			td.pointer = td.topic.getLastPostId();

			td.topic.clear();

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
				throw new NoSuchElementException("No Topic with name " + topicName + " found");

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
				throw new IllegalArgumentException(
				        "Topic with name " + topicName + " already exists");

			tdMap.put(topicName, new TopicManager.TopicData(topic));
		}

		@Override
		public void close() throws ServerException {
			try {
				for (final TopicManager.TopicData td : tdMap.values())
					td.socket.close();
			} catch (IOException e) {
				// TODO: maybe come up with a better message
				throw new ServerException("Connection to server lost", e);
			}

			tdMap.clear();
		}
	}

	private class ListenForTopicThread extends Thread {

		private final Tag eventTag = Tag.TOPIC_LISTENED;

		private final Topic topic;

		public ListenForTopicThread(Topic topic) {
			this.topic = topic;
		}

		@Override
		public void run() {
			LG.sout("ListenForTopicThread#run()");
			LG.in();

			final String topicName = topic.getName();

			final ConnectionInfo actualBrokerCI;
			try {
				actualBrokerCI = topicCIManager.getConnectionInfoForTopic(topicName);
			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
				return;
			}

			try {
				// 'socket' closes at close()
				final Socket socket = new Socket(
						actualBrokerCI.getAddress(), actualBrokerCI.getPort());

				final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.flush();
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

				oos.writeObject(new Message(MessageType.INITIALISE_CONSUMER, topic.getToken()));

				boolean success = ois.readBoolean();

				if (!success) {
					socket.close();
					throw new ServerException("Topic " + topicName + " does not exist");
				}

				topicManager.addSocket(topic, socket);
				new PullThread(ois, topic).start();

			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
			} catch (final IOException e) {
				Throwable e1 = new ServerException("Connection to server lost", e);
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e1));
			}

			LG.out();
		}
	}
}
