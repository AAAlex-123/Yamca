package eventDeliverySystem.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import eventDeliverySystem.dao.ITopicDAO;
import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.AbstractTopic.TopicToken;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.datastructures.Subscriber;
import eventDeliverySystem.thread.PullThread;
import eventDeliverySystem.thread.PushThread;
import eventDeliverySystem.thread.PushThread.Protocol;
import eventDeliverySystem.util.LG;

/**
 * A remote component that forms the backbone of the EventDeliverySystem. Brokers act as part of a
 * distributed server that services Publishers and Consumers.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public final class Broker implements Runnable, AutoCloseable {

	private static final int BACKLOG = 50;

	private final BrokerTopicManager btm;

	// no need to synchronise because these practically immutable after startup
	// since no new broker can be constructed after startup
	private final List<Socket> brokerConnections = new LinkedList<>(); // CHECK is this needed?
	private final List<ConnectionInfo> brokerCI = new LinkedList<>();

	private final ServerSocket clientRequestSocket;
	private final ServerSocket brokerRequestSocket;

	/**
	 * Create a new leader broker. This is necessarily the first step to initialize the server
	 * network.
	 *
	 * @param postDao the ITopicDAO object responsible for this Broker's Posts.
	 * @param clientRequestSocket the unbound ServerSocket that will listen for incoming requests
	 * 		from Clients
	 * @param brokerRequestSocket the unbound ServerSocket that will listen for incoming requests
	 * 		from Brokers
	 *
	 * @throws IOException if the server could not be started
	 * @see ITopicDAO
	 */
	public Broker(ITopicDAO postDao, ServerSocket clientRequestSocket,
			ServerSocket brokerRequestSocket) throws IOException {
		btm = new BrokerTopicManager(postDao);
		btm.forEach(brokerTopic -> brokerTopic.subscribe(new BrokerTopicSubscriber(brokerTopic)));

		this.clientRequestSocket = clientRequestSocket;
		this.brokerRequestSocket = brokerRequestSocket;

		this.clientRequestSocket.bind(new InetSocketAddress((InetAddress) null, 0), Broker.BACKLOG);
		this.brokerRequestSocket.bind(new InetSocketAddress((InetAddress) null, 0), Broker.BACKLOG);

		LG.sout("Broker connected at:");
		LG.sout("Server IP   - %s", InetAddress.getLocalHost().getHostAddress());
		LG.socket("Client", clientRequestSocket);
		LG.socket("Broker", brokerRequestSocket);
	}

	/**
	 * Create a non-leader broker and connect it to the server network.
	 *
	 * @param postDao the ITopicDAO object responsible for this Broker's Posts.
	 * @param clientRequestSocket the unbound ServerSocket that will listen for incoming requests
	 * 		from Clients
	 * @param brokerRequestSocket the unbound ServerSocket that will listen for incoming requests
	 * 		from Brokers
	 * @param leaderIP the IP of the leader broker
	 * @param leaderPort the port of the leader broker
	 *
	 * @throws IOException if this server could not be started or the connection to the leader
	 * 		broker could not be established.
	 * @see ITopicDAO
	 */
	public Broker(ITopicDAO postDao, ServerSocket clientRequestSocket,
			ServerSocket brokerRequestSocket, String leaderIP, int leaderPort) throws IOException {
		this(postDao, clientRequestSocket, brokerRequestSocket);

		final Socket leaderConnection = new Socket(leaderIP, leaderPort); // closes at Broker#close
		final ObjectOutputStream oos = new ObjectOutputStream(leaderConnection.getOutputStream());

		oos.writeObject(ConnectionInfo.forServerSocket(clientRequestSocket));
		brokerConnections.add(leaderConnection);
	}

	/** Starts listening for new requests by clients and connection requests from other brokers */
	@Override
	public void run() {

		Thread clientRequestThread = new Thread(() -> {
			LG.sout("ClientRequestThread#run()");
			try {
				while (true) {
					final Socket socket = clientRequestSocket.accept(); // closes at Broker#close
					new ClientRequestHandler(socket).start();
				}
			} catch (final IOException e) {
				e.printStackTrace();
			}
			LG.sout("/ClientRequestThread#run()");
		}, "Client Request Thread");

		Thread brokerRequestThread = new Thread(() -> {
			LG.sout("BrokerRequestThread#run()");
			try {
				while (true) {
					final Socket socket = brokerRequestSocket.accept(); // closes at Broker#close
					new BrokerRequestHandler(socket).start();
				}
			} catch (final IOException e) {
				e.printStackTrace();
			}
			LG.sout("/BrokerRequestThread#run()");
		}, "Broker Request Thread");

		clientRequestThread.start();
		brokerRequestThread.start();

		LG.sout("/Broker#run");
	}

	/** Closes all connections to this broker */
	@Override
	public synchronized void close() {
		LG.sout("Broker#close()");
		try {
			btm.close();

			for (final Socket brokerSocket : brokerConnections) {
				brokerSocket.close();
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	// ========== THREADS ==========

	private final class ClientRequestHandler extends Thread {

		private final Socket socket;

		private ClientRequestHandler(Socket socket) {
			super("ClientRequestHandler-" + socket);
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

				final String start = "%s '%s'";
				final String topicName;

				switch (message.getType()) {
				case DATA_PACKET_SEND: {
					topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();

					boolean success = topicExists(topicName);
					LG.sout("success=%s", success);
					oos.writeBoolean(success);
					oos.flush();

					if (success) {
						new PullThread(ois, getTopic(topicName)).run();
					}

					socket.close();
					break;
				}

				case INITIALISE_CONSUMER: {
					final TopicToken topicToken = (TopicToken) message.getValue();
					topicName = topicToken.getName();
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
						getPostsFromTopicSince(topicName, idOfLast, piList, packetMap);

						LG.sout("piList=%s", piList);
						LG.sout("packetMap=%s", packetMap);
						new PushThread(oos, piList, packetMap, Protocol.KEEP_ALIVE).run();

						new BrokerPushThread(getTopic(topicName), oos).start();
					}

					break;
				}

				case BROKER_DISCOVERY: {
					topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);

					final ConnectionInfo brokerInfo = getAssignedBroker(topicName);
					LG.sout("brokerInfo=%s", brokerInfo);

					oos.writeObject(brokerInfo);
					oos.flush();

					socket.close();
					break;
				}

				case CREATE_TOPIC: {
					topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();

					final boolean success = !topicExists(topicName) && addTopic(topicName);
					LG.sout("success=%s", success);
					oos.writeBoolean(success);
					oos.flush();

					if (success) {
						subscribeToTopic(topicName);
					}

					socket.close();
					break;
				}

				case DELETE_TOPIC: {
					topicName = (String) message.getValue();
					LG.sout(start, message.getType(), topicName);
					LG.in();

					final boolean success = topicExists(topicName) && removeTopic(topicName);
					LG.sout("success=%s", success);
					oos.writeBoolean(success);
					oos.flush();

					socket.close();
					break;
				}

				default: {
					throw new IllegalArgumentException(
							"You forgot to put a case for the new Message enum");
				}
				}

				LG.out();
				final String end = "/%s '%s'";
				LG.sout(end, message.getType(), topicName);
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

		private boolean registerConsumer(String topicName, Socket consumerSocket) {
			try {
				btm.registerConsumer(topicName, consumerSocket);
				return true;
			} catch (NoSuchElementException e) {
				return false;
			}
		}

		private void getPostsFromTopicSince(String topicName, long idOfLast, List<PostInfo> piList,
				Map<? super Long, Packet[]> packetMap) {
			btm.getPostsFromTopicSince(topicName, idOfLast, piList, packetMap);
		}

		private void subscribeToTopic(String topicName) {
			BrokerTopic brokerTopic = btm.getTopic(topicName);
			Subscriber subscriber = new BrokerTopicSubscriber(brokerTopic);
			btm.addSubscriberToTopic(topicName, subscriber);
		}

		private ConnectionInfo getAssignedBroker(String topicName) {
			final int brokerCount = brokerCI.size();

			final int hash = AbstractTopic.hashForTopic(topicName);
			final int brokerIndex = Math.abs(hash % (brokerCount + 1));

			// last index (out of range normally) => this broker is responsible for the topic. this
			// works because the default broker is the only broker that processes such requests.
			if (brokerIndex == brokerCount) {
				return ConnectionInfo.forServerSocket(clientRequestSocket);
			}

			// else send the broker from the other connections
			return brokerCI.get(brokerIndex);
		}
	}

	private final class BrokerRequestHandler extends Thread {

		private final Socket socket;

		private BrokerRequestHandler(Socket socket) {
			super("BrokerRequestHandler-" + socket);
			this.socket = socket;
		}

		@Override
		public void run() {

			LG.ssocket("Starting BrokerRequestHandler for Socket", socket);

			try {
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				final ConnectionInfo brokerCIForClient = (ConnectionInfo) ois.readObject();

				brokerConnections.add(socket);

				LG.sout("brokerCIForClient=%s", brokerCIForClient);
				brokerCI.add(brokerCIForClient);
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
				try {
					socket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
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
