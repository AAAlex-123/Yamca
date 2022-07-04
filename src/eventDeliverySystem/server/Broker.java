package eventDeliverySystem.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.datastructures.Topic.TopicToken;
import eventDeliverySystem.thread.PullThread;
import eventDeliverySystem.thread.PushThread;
import eventDeliverySystem.thread.PushThread.Protocol;
import eventDeliverySystem.util.LG;
import eventDeliverySystem.util.PortManager;

/**
 * A remote component that forms the backbone of the EventDeliverySystem.
 * Brokers act as part of a distributed server that services Publishers and
 * Consumers.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public class Broker implements Runnable, AutoCloseable {

	private static final int MAX_CONNECTIONS = 64;

	private final BrokerTopicManager btm;

	// no need to synchronise because these practically immutable after startup
	// since no new broker can be constructed after startup
	private final List<Socket>         brokerConnections = new LinkedList<>();
	private final List<ConnectionInfo> brokerCI = new LinkedList<>();

	private final ServerSocket clientRequestSocket;
	private final ServerSocket brokerRequestSocket;

	/**
	 * Create a new leader broker. This is necessarily the first step to initialize
	 * the server network.
	 */
	public Broker(Path topicsRootDirectory) throws FileSystemException {
		btm = new BrokerTopicManager(topicsRootDirectory);

		try {
			clientRequestSocket = new ServerSocket(PortManager.getNewAvailablePort(),
			        Broker.MAX_CONNECTIONS);
			brokerRequestSocket = new ServerSocket(PortManager.getNewAvailablePort(),
			        Broker.MAX_CONNECTIONS);
		} catch (final IOException e) {
			throw new UncheckedIOException("Could not open server socket: ", e);
		}

		LG.sout("Broker connected at:");
		LG.socket("Client", clientRequestSocket);
		LG.socket("Broker", brokerRequestSocket);
	}

	/**
	 * Create a non-leader broker and connect it to the server network.
	 *
	 * @param leaderIP   the IP of the leader broker
	 * @param leaderPort the port of the leader broker
	 *
	 * @throws UncheckedIOException if the connection to the leader broker fails
	 */
	@SuppressWarnings("resource")
	public Broker(String leaderIP, int leaderPort) {
		this();
		try {
			final Socket leaderConnection = new Socket(leaderIP, leaderPort);

			final ObjectOutputStream oos = new ObjectOutputStream(
			        leaderConnection.getOutputStream());

			oos.writeObject(ConnectionInfo.forServerSocket(clientRequestSocket));
			brokerConnections.add(leaderConnection);
		} catch (final IOException ioe) {
			throw new UncheckedIOException("Couldn't connect to leader broker ", ioe);
		}
	}

	/**
	 * Begins listening for and new requests by clients and connection requests from
	 * other brokers.
	 */
	@Override
	public void run() {

		final Runnable clientRequestThread = () -> {
			LG.sout("Start: ClientRequestThread");
			while (true)
				try {
					@SuppressWarnings("resource")
					final Socket socket = clientRequestSocket.accept();
					new ClientRequestHandler(socket).start();

				} catch (final IOException e) {
					e.printStackTrace();
					System.exit(-1); // serious error when waiting, close broker
				}
		};

		final Runnable brokerRequestThread = () -> {
			LG.sout("Start: BrokerRequestThread");
			while (true)
				try {
					@SuppressWarnings("resource") // closes at Broker#close
					final Socket socket = brokerRequestSocket.accept();
					new BrokerRequestHandler(socket).start();

				} catch (final IOException e) {
					e.printStackTrace();
					System.exit(-1); // serious error when waiting, close broker
				}
		};

		new Thread(clientRequestThread, "ClientRequestThread").start();
		new Thread(brokerRequestThread, "BrokerRequestThread").start();

		LG.sout("Broker#run end");
	}

	/** Closes all connections to this broker */
	@Override
	public synchronized void close() {
		try {
			btm.close();

			for (final Socket brokerSocket : brokerConnections)
				brokerSocket.close();

		} catch (final IOException ioe) {
			ioe.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// ========== THREADS ==========

	/**
	 * A thread which continuously reads new client requests and assigns worker
	 * threads to fulfil them when necessary.
	 *
	 * @author Alex Mandelias
	 * @author Dimitris Tsirmpas
	 */
	private class ClientRequestHandler extends Thread {

		private final Socket socket;

		public ClientRequestHandler(Socket socket) {
			super("ClientRequestHandler-" + socket.getInetAddress() + "-" + socket.getLocalPort());
			this.socket = socket;
		}

		@Override
		public void run() {

			LG.ssocket("Starting ClientRequestHandler for Socket", socket);

			try {
				final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.flush();
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

				final Message message = (Message) ois.readObject();
				LG.sout("Creating thread for message type: %s", message.getType());

				LG.in();
				switch (message.getType()) {
				case DATA_PACKET_SEND: {
					String topicName = (String) message.getValue();
					LG.sout("DATA_PACKET_SEND '%s'", topicName);
					LG.in();

					BrokerTopic topic = getTopic(topicName);
					new PullThread(ois, topic).run();

					socket.close();
					LG.out();
					break;
				}

				case INITIALISE_CONSUMER: {
					final TopicToken topicToken = (TopicToken) message.getValue();
					final String     topicName  = topicToken.getName();
					LG.sout("INITIALISE_CONSUMER '%s'", topicName);
					LG.in();

					// previous code was cringe :D

					registerConsumer(topicName, oos);

					// send existing topics that the consumer does not have
					final long idOfLast = topicToken.getLastId();
					LG.sout("idOfLast=%d", idOfLast);

					final List<PostInfo> piList = new LinkedList<>();
					final Map<Long, Packet[]> packetMap = new HashMap<>();
					getTopic(topicName).getPostsSince(idOfLast, piList, packetMap);

					LG.sout("piList=%s", piList);
					LG.sout("packetMap=%s", packetMap);
					new PushThread(oos, piList, packetMap, Protocol.KEEP_ALIVE).run();

					BrokerTopic topic = getTopic(topicName);
					new BrokerPushThread(topic, oos).start();

					LG.out();
					break;
				}

				case BROKER_DISCOVERY: {
					String topicName = (String) message.getValue();
					LG.sout("BROKER_DISCOVERY '%s'", topicName);
					LG.in();
					new BrokerDiscoveryThread(oos, topicName).run();

					socket.close();
					LG.out();
					break;
				}

				case CREATE_TOPIC: {
					String topicName = (String) message.getValue();
					LG.sout("CREATE_TOPIC '%s'", topicName);
					LG.in();

					final boolean topicExists = topicExists(topicName);

					LG.sout("topicExists=%s", topicExists);
					if (!topicExists)
						addTopic(topicName);

					oos.writeBoolean(!topicExists);

					socket.close();
					LG.out();
					break;
				}

				default:
					throw new IllegalArgumentException(
					        "You forgot to put a case for the new Message enum");
				}
				LG.out();

			} catch (final IOException ioe) {
				// do nothing, ignore this client
			} catch (final ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private boolean topicExists(String topicName) {
			return btm.topicExists(topicName);
		}

		private BrokerTopic getTopic(String topicName) throws NoSuchElementException {
			return btm.getTopic(topicName);
		}

		private boolean addTopic(String topicName) {
			try {
				btm.addTopic(topicName);
				return true;
			} catch (FileSystemException e) {
				return false;
			}
		}

		private void registerConsumer(String topicName, ObjectOutputStream oos) {
			btm.registerConsumer(topicName, oos);
		}
	}

	private class BrokerRequestHandler extends Thread {

		private final Socket socket;

		public BrokerRequestHandler(Socket socket) {
			super("BrokerRequestHandler-" + socket.getInetAddress() + "-" + socket.getLocalPort());
			this.socket = socket;
		}

		@Override
		public void run() {

			LG.ssocket("Starting BrokerRequestHandler for Socket", socket);

			ConnectionInfo brokerCIForClient;
			try {
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				brokerCIForClient = (ConnectionInfo) ois.readObject();

			} catch (ClassNotFoundException | IOException e) {
				// do nothing, ignore this broker
				e.printStackTrace();
				try {
					socket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				return;
			}

			// only add socket and CI if no exception was thrown
			brokerConnections.add(socket);

			LG.sout("brokerCIForCilent=%s", brokerCIForClient);
			brokerCI.add(brokerCIForClient);
		}
	}

	/**
	 * A Thread for discovering the actual Broker for a Topic.
	 *
	 * @author Alex Mandelias
	 */
	private class BrokerDiscoveryThread extends Thread {

		private final ObjectOutputStream oos;
		private final String             topicName;

		/**
		 * Constructs the Thread that, when run, will write the ConnectionInfo of the
		 * Broker responsible for the requested Topic to the given output stream.
		 *
		 * @param stream    the output stream to which to write the ConnectionInfo
		 * @param topicName the name of the Topic
		 */
		public BrokerDiscoveryThread(ObjectOutputStream stream, String topicName) {
			super("BrokerDiscoveryThread-" + topicName);
			oos = stream;
			this.topicName = topicName;
		}

		@Override
		public void run() {

			LG.sout("BrokerDiscoveryThread#run()");
			LG.in();

			LG.sout("topicName=%s", topicName);
			final ConnectionInfo brokerInfo = getAssignedBroker();
			LG.sout("brokerInfo=%s", brokerInfo);

			try {
				oos.writeObject(brokerInfo);
			} catch (final IOException e) {
				// do nothing
			}
			LG.out();
		}

		private ConnectionInfo getAssignedBroker() {
			final int brokerCount = brokerConnections.size();

			final int hash        = AbstractTopic.hashForTopic(topicName);
			final int brokerIndex = Math.abs(hash % (brokerCount + 1));

			// last index (out of range normally) => this broker is responsible for the topic
			// this works because the default broker is the only broker that processes such requests.
			if (brokerIndex == brokerCount)
				return ConnectionInfo.forServerSocket(clientRequestSocket);

			// else send the broker from the other connections
			return brokerCI.get(brokerIndex);
		}
	}
}
