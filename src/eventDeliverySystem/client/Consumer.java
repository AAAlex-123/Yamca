package eventDeliverySystem.client;

import static eventDeliverySystem.datastructures.Message.MessageType.INITIALISE_CONSUMER;

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

import eventDeliverySystem.User.UserSub;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.datastructures.Topic;
import eventDeliverySystem.server.Broker;
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

	private final UserSub      usersub;
	private final TopicManager topicManager;

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 * @param usersub    the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 */
	public Consumer(String serverIP, int serverPort, UserSub usersub) throws UnknownHostException {
		this(InetAddress.getByName(serverIP), serverPort, usersub);
	}

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 * @param usersub    the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	public Consumer(byte[] serverIP, int serverPort, UserSub usersub) throws UnknownHostException {
		this(InetAddress.getByAddress(serverIP), serverPort, usersub);
	}

	/**
	 * Constructs a Consumer that will connect to a specific default broker.
	 *
	 * @param ip      the InetAddress of the default broker
	 * @param port    the port of the default broker
	 * @param usersub the UserSub object that will be notified when data arrives
	 */
	private Consumer(InetAddress ip, int port, UserSub usersub) {
		super(ip, port);
		topicManager = new TopicManager();
		this.usersub = usersub;
	}

	@Override
	public void close() throws IOException {
		topicManager.close();
	}

	/**
	 * Changes the Topics that this Consumer listens to. All connections regarding
	 * the previous Topics are closed and new ones are established.
	 *
	 * @param newTopics the new Topics to listen for
	 *
	 * @throws IOException if an I/O error occurs while closing existing connections
	 */
	public void setTopics(Set<Topic> newTopics) throws IOException {
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
	 * Registers a new Topic for this Consumer to continuously fetch new Posts from.
	 *
	 * @param topicName the name of the Topic to fetch from
	 *
	 * @throws IOException              if a connection to the server fails
	 * @throws IllegalArgumentException if this Consumer already listens to a Topic
	 *                                  with the same name
	 */
	public void listenForNewTopic(String topicName) throws IOException {
		LG.sout("listenForNewTopic(%s)", topicName);
		LG.in();
		listenForTopic(new Topic(topicName));
		LG.out();
	}

	/**
	 * Registers an existing Topic for this Consumer to continuously fetch new Posts
	 * from.
	 *
	 * @param topic Topic to fetch from
	 *
	 * @throws IOException              if a connection to the server fails
	 * @throws IllegalArgumentException if this Consumer already listens to a Topic
	 *                                  with the same name
	 */
	@SuppressWarnings("resource") // 'socket' closes at close()
	private void listenForTopic(Topic topic) throws IOException {
		LG.sout("listenForTopic(%s)", topic);
		LG.in();

		topic.subscribe(this);

		Socket       socket    = null;
		final String topicName = topic.getName();

		final ConnectionInfo ci = topicCIManager.getConnectionInfoForTopic(topicName);

		try {
			socket = new Socket(ci.getAddress(), ci.getPort()); // 'socket' closes at close()
			topicManager.addSocket(topic, socket);

			final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			oos.writeObject(new Message(INITIALISE_CONSUMER, topic.getToken()));

			new PullThread(ois, topic).start();

		} catch (final IOException e) {
			throw new IOException("Fatal error: can't connect to server for topic " + topicName, e);
		}
		LG.out();
	}

	@Override
	synchronized public void notify(PostInfo postInfo, String topicName) {
		LG.sout("Consumer#notify(%s, %s)", postInfo, topicName);
		// do nothing
	}

	@Override
	synchronized public void notify(Packet packet, String topicName) {
		LG.sout("Consumer#notify(%s, %s)", packet, topicName);
		if (packet.isFinal())
			usersub.notify(topicName);
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
			LG.sout("Consumer#addSocket(%s, %s)", topic, socket);
			LG.in();
			add(topic);
			tdMap.get(topic.getName()).socket = socket;
			LG.out();
		}

		private void add(Topic topic) {
			final String topicName = topic.getName();
			if (tdMap.containsKey(topicName))
				throw new IllegalArgumentException(
				        "Topic with name " + topicName + " already exists");

			tdMap.put(topicName, new TopicManager.TopicData(topic));
		}

		@Override
		public void close() throws IOException {
			for (final TopicManager.TopicData td : tdMap.values())
				td.socket.close();

			tdMap.clear();
		}
	}
}
