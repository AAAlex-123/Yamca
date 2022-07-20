package eventDeliverySystem.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.dao.ITopicDAO;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.datastructures.Topic.TopicToken;
import eventDeliverySystem.thread.PullThread;
import eventDeliverySystem.thread.PushThread;
import eventDeliverySystem.thread.PushThread.Protocol;
import eventDeliverySystem.util.LG;
import eventDeliverySystem.datastructures.Subscriber;

/**
 * A remote component that forms the backbone of the EventDeliverySystem. Brokers act as part of a
 * distributed server that services Publishers and Consumers.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public final class Broker implements Runnable, AutoCloseable {

	private static final int MAX_CONNECTIONS = 64;

	private final BrokerTopicManager btm;

	// no need to synchronise because these practically immutable after startup
	// since no new broker can be constructed after startup
	private final List<Socket>         brokerConnections = new LinkedList<>();
	private final List<ConnectionInfo> brokerCI = new LinkedList<>();

	private final ServerSocket clientRequestSocket;
	private final ServerSocket brokerRequestSocket;

	/**
	 * Create a new leader broker. This is necessarily the first step to initialize the server
	 * network.
	 *
	 * @param postDao the ITopicDAO object responsible for this Broker's Posts.
	 *
	 * @throws IOException if the server could not be started
	 *
	 * @see ITopicDAO
	 */
	public Broker(ITopicDAO postDao) throws IOException {
		btm = new BrokerTopicManager(postDao);
		btm.forEach(brokerTopic -> brokerTopic.subscribe(new BrokerTopicSubscriber(brokerTopic)));

		try {
			clientRequestSocket = new ServerSocket(29621, // PortManager.getNewAvailablePort(),
			        Broker.MAX_CONNECTIONS);
			brokerRequestSocket = new ServerSocket(29622, // PortManager.getNewAvailablePort(),
			        Broker.MAX_CONNECTIONS);
		} catch (final IOException e) {
			throw new UncheckedIOException("Could not open server socket: ", e);
		}

		LG.sout("Broker connected at:");
		LG.sout("Server IP   - %s", InetAddress.getLocalHost().getHostAddress());
		LG.socket("Client", clientRequestSocket);
		LG.socket("Broker", brokerRequestSocket);
	}

	/**
	 * Create a non-leader broker and connect it to the server network.
	 *
	 * @param postDao    the ITopicDAO object responsible for this Broker's Posts.
	 * @param leaderIP   the IP of the leader broker
	 * @param leaderPort the port of the leader broker
	 *
	 * @throws IOException if this server could not be started or the connection to the leader
	 * 					   broker could not be established.
	 *
	 * @see ITopicDAO
	 */
	@SuppressWarnings("resource")
	public Broker(ITopicDAO postDao, String leaderIP, int leaderPort) throws IOException {
		this(postDao);
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

	/** Starts listening for new requests by clients and connection requests from other brokers */
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

	private final class ClientRequestHandler extends Thread {

		private final Socket socket;

		private ClientRequestHandler(Socket socket) {
			super("ClientRequestHandler-" + socket.getInetAddress() + "-" + socket.getLocalPort());
			this.socket = socket;
		}

		@Override
		public void run() {

			LG.ssocket("Starting ClientRequestHandler for Socket", socket);
			LG.in();

			try {
				final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.flush();
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

				final Message message = (Message) ois.readObject();
				LG.sout("Creating thread for message type: %s", message.getType());

				final String start = "%s '%s'";
				final String end = "/%s '%s'";

				LG.in();
				switch (message.getType()) {
				case DATA_PACKET_SEND: {
					String topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();

					boolean success = topicExists(topicName);
					oos.writeBoolean(success);
					oos.flush();

					if (success)
						new PullThread(ois, getTopic(topicName)).run();

					socket.close();
					LG.out();
					LG.sout(end, message.getType(), topicName);
					break;
				}

				case INITIALISE_CONSUMER: {
					final TopicToken topicToken = (TopicToken) message.getValue();
					final String     topicName  = topicToken.getName();
					LG.sout(start, message.getType(), topicName);
					LG.in();

					final boolean success = registerConsumer(topicName, socket);

					LG.sout("success=%s", success);

					oos.writeBoolean(success);
					oos.flush();

					if (success) {
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
					}

					LG.out();
					LG.sout(end, message.getType(), topicName);
					break;
				}

				case BROKER_DISCOVERY: {
					String topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);

					LG.sout("topicName=%s", topicName);
					final ConnectionInfo brokerInfo = getAssignedBroker(topicName);
					LG.sout("brokerInfo=%s", brokerInfo);

					try {
						oos.writeObject(brokerInfo);
						oos.flush();
					} catch (final IOException e) {
						// do nothing
					}

					socket.close();
					LG.out();
					LG.sout(end, message.getType(), topicName);
					break;
				}

				case CREATE_TOPIC: {
					String topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();
					final boolean topicExists = topicExists(topicName);

					LG.sout("topicExists=%s", topicExists);
					final boolean success;
					if (topicExists) {
						success = false;
					} else {
						success = addTopic(topicName);
					}

					if (success) {
						BrokerTopic bt = getTopic(topicName);
						bt.subscribe(new BrokerTopicSubscriber(bt));
					}

					oos.writeBoolean(success);
					oos.flush();

					socket.close();
					LG.out();
					LG.sout(end, message.getType(), topicName);
					break;
				}

				case DELETE_TOPIC: {
					String topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();
					final boolean topicExists = topicExists(topicName);

					LG.sout("topicExists=%s", topicExists);
					final boolean success;
					if (!topicExists) {
						success = false;
					} else {
						success = removeTopic(topicName);
					}

					LG.sout("success=%s", topicExists);

					oos.writeBoolean(success);
					oos.flush();

					socket.close();
					LG.out();
					LG.sout(end, message.getType(), topicName);
					break;
				}

				default: {
					throw new IllegalArgumentException(
					        "You forgot to put a case for the new Message enum");
				}
				}
				LG.out();

			} catch (final IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}

			LG.out();
			LG.ssocket("Finishing ClientRequestHandler for Socket", socket);
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
			} catch (IOException | IllegalArgumentException e) {
				return false;
			}
		}

		private boolean removeTopic(String topicName) {
			try {
				btm.removeTopic(topicName);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NoSuchElementException e) {
				// do nothing specific to NoSuchElementException
				// should never occur
			}
			return false;
		}

		private boolean registerConsumer(String topicName, Socket socket) {
			try {
				btm.registerConsumer(topicName, socket);
				return true;
			} catch (NoSuchElementException e) {
				return false;
			}
		}

		private ConnectionInfo getAssignedBroker(String topicName) {
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

	private final class BrokerRequestHandler extends Thread {

		private final Socket socket;

		private BrokerRequestHandler(Socket socket) {
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

	private final class BrokerTopicSubscriber implements Subscriber {

		private final BrokerTopic brokerTopic;

		private BrokerTopicSubscriber(BrokerTopic brokerTopic) {
			this.brokerTopic = brokerTopic;
		}

		@Override
		public void notify(PostInfo postInfo, String topicName) {
			// do nothing
		}

		@Override
		public void notify(Packet packet, String topicName) {
			if (packet.isFinal()) {
				try {
					brokerTopic.savePostToTFS(packet.getPostId());
				} catch (IOException e) {
					close();
				}
			}
		}
	}
}
