package eventdeliverysystem.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import eventdeliverysystem.client.User.UserStub;
import eventdeliverysystem.client.UserEvent.Tag;
import eventdeliverysystem.datastructures.ConnectionInfo;
import eventdeliverysystem.datastructures.Message;
import eventdeliverysystem.datastructures.Message.MessageType;
import eventdeliverysystem.server.Broker;
import eventdeliverysystem.util.LG;

/**
 * A superclass for all client-side Nodes that connect to and send / receive data from a server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 * @see Broker
 */
abstract class ClientNode {

	/** The string {@code "Connection to server lost"} */
	protected static final String CONNECTION_TO_SERVER_LOST_STRING = "Connection to server lost";

	/**
	 * Returns the string {@code "Topic %s does not exist"} formatted with the topicName.
	 *
	 * @param topicName the name of the Topic that does not exist
	 *
	 * @return the formatted string
	 */
	protected static String getTopicDNEString(String topicName) {
		return String.format("Topic %s does not exist", topicName);
	}

	/**
	 * Returns the string {@code "Topic %s already exists"} formatted with the topicName.
	 *
	 * @param topicName the name of the Topic that already exists
	 *
	 * @return the formatted string
	 */
	protected static String getTopicAEString(String topicName) {
		return String.format("Topic %s already exists", topicName);
	}

	/**
	 * This Client Node's Connection Info Manager that manages the information about this Node's
	 * connections to brokers.
	 *
	 * @see CIManager
	 */
	protected final CIManager topicCIManager;

	/**
	 * This Client Node's User Stub which is used to fire events to the User when appropriate.
	 *
	 * @see UserStub
	 */
	protected final UserStub userStub;

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP the IP of the default broker, interpreted by {@link
	 *        InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 * @param userStub the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or if a scope_id
	 * 		was specified for a global IPv6 address while resolving the defaultServerIP.
	 */
	protected ClientNode(String serverIP, int serverPort, UserStub userStub)
			throws UnknownHostException {
		this(InetAddress.getByName(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP the IP of the default broker, interpreted by {@link
	 *        InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 * @param userStub the UserSub object that will be notified when data arrives
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	protected ClientNode(byte[] serverIP, int serverPort, UserStub userStub)
			throws UnknownHostException {
		this(InetAddress.getByAddress(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param ip the InetAddress of the default broker
	 * @param port the port of the default broker
	 * @param userStub the UserSub object that will be notified when data arrives
	 */
	protected ClientNode(InetAddress ip, int port, UserStub userStub) {
		topicCIManager = new CIManager(ip, port);
		this.userStub = userStub;
	}

	/**
	 * Abstract Thread superclass for all Threads that ClientNode subclasses use to communicate with
	 * Brokers. This class provides a templated {@code run} method which is customized by the other
	 * two protected methods of this class.
	 *
	 * @author Alex Mandelias
	 * @see #run()
	 * @see #doWorkAndMaybeCloseSocket(boolean, Socket, ObjectOutputStream, ObjectInputStream)
	 * @see #getMessageValue()
	 */
	protected abstract class ClientThread extends Thread {

		/** The tag of any user event fired by this thread */
		protected final Tag eventTag;

		/** The message type that will be sent with the message object by this thread */
		protected final MessageType messageType;

		/** The name of the Topic this thread is associated with */
		protected final String topicName;

		/**
		 * Constructs a ClientThread.
		 *
		 * @param eventTag the tag of the user event that will be fired
		 * @param messageType the type of the message that will be sent to the broker
		 * @param topicName the name of the Topic this thread is associated with
		 *
		 * @see #run()
		 */
		protected ClientThread(Tag eventTag, MessageType messageType, String topicName) {
			super(String.format("ClientThread - %s - %s - %s", eventTag, messageType, topicName));
			this.eventTag = eventTag;
			this.messageType = messageType;
			this.topicName = topicName;
		}

		/**
		 * Templated {@code run} method for all Client Threads. This method does the following:
		 * <ul>
		 *     <li>Sets up a connection to the actual broker for the Topic with that name</li>
		 *     <li>Sends a message with the given type to the server. The value is obtained from
		 *     the {@code getMessageValue} method.</li>
		 *     <li>Reads the server's response.</li>
		 *     <li>Calls the {@code doWork} method passing the server's response to it.</li>
		 *     <li>Fires a successful user event with the given tag. If an exception is thrown at
		 *     any point, a failed user event is fired instead.</li>
		 * </ul>
		 */
		@Override
		public final void run() {
			LG.sout("%s#un()", getClass().getName());
			LG.in();

			final ConnectionInfo actualBrokerCI;
			try {
				actualBrokerCI = topicCIManager.getConnectionInfoForTopic(topicName);
			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
				return;
			}
			LG.sout("actualBrokerCI=%s", actualBrokerCI);

			try {
				Socket socket = new Socket(actualBrokerCI.getAddress(), actualBrokerCI.getPort());
				final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.flush();

				// don't remove the following line even if the ois isn't used
				// https://stackoverflow.com/questions/72920493/
				final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

				oos.writeObject(new Message(messageType, getMessageValue()));

				boolean success = ois.readBoolean();

				doWorkAndMaybeCloseSocket(success, socket, oos, ois);

				userStub.fireEvent(UserEvent.successful(eventTag, topicName));
			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
			} catch (final IOException e) {
				Throwable e1 = new ServerException(ClientNode.CONNECTION_TO_SERVER_LOST_STRING, e);
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e1));
			}

			LG.out();
		}

		/**
		 * Allows clients to perform specific work after the initial communication with the Broker.
		 * See the {@code run} method's documentation for an exact description of when and how this
		 * method is called. This method is also responsible for closing the provided connection.
		 *
		 * @param success the Broker's response, {@code true} if it was successful, {@code false}
		 * 		otherwise
		 * @param socket the connection that was established with the Broker
		 * @param oos the opened output stream of the connection which can be used to send further
		 * 		data
		 * @param ois the opened input stream of the connection which can be used to receive
		 * 		further data
		 *
		 * @throws IOException if an I/O Error occurs while doing client-specific work. Such IO
		 * 		exceptions should be left uncaught by this method as the ClientThread is responsible
		 * 		for catching them
		 * @see #run()
		 */
		protected abstract void doWorkAndMaybeCloseSocket(boolean success, Socket socket,
				ObjectOutputStream oos, ObjectInputStream ois) throws IOException;

		/**
		 * Allows clients to provide a different value for the message that is sent to the Broker.
		 * the default implementation sets the value to the {@code topicName}. See the {@code run}
		 * method's documentation for an exact description of when and how this method is called.
		 *
		 * @return the value for the message
		 *
		 * @see #run()
		 */
		protected Serializable getMessageValue() {
			return topicName;
		}
	}
}
