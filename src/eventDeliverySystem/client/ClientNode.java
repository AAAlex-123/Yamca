package eventDeliverySystem.client;

import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Message.MessageType;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.user.User.UserStub;
import eventDeliverySystem.user.UserEvent;
import eventDeliverySystem.user.UserEvent.Tag;
import eventDeliverySystem.util.LG;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import eventDeliverySystem.server.Broker;

/**
 * A superclass for all client-side Nodes that connect to and send / receive
 * data from a remote server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 *
 * @see Broker
 */
abstract class ClientNode {

	protected static final String CONNECTION_TO_SERVER_LOST_STRING = "Connection to server lost";

	protected static String getTopicDNEString(String topicName) {
		return String.format("Topic %s does not exist", topicName);
	}

	protected static String getTopicAEString(String topicName) {
		return String.format("Topic %s already exists", topicName);
	}

	/**
	 * This Client Node's Connection Info Manager that manages the information about
	 * this Node's connections to brokers.
	 *
	 * @see CIManager
	 */
	protected final CIManager topicCIManager;
	protected final UserStub userStub;

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 * @param userStub   the UserSub object that will be notified when data arrives
	 *
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 */
	protected ClientNode(String serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		this(InetAddress.getByName(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 * @param userStub   the UserSub object that will be notified when data arrives
	 *
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	protected ClientNode(byte[] serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		this(InetAddress.getByAddress(serverIP), serverPort, userStub);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param ip   the InetAddress of the default broker
	 * @param port the port of the default broker
	 * @param userStub   the UserSub object that will be notified when data arrives
	 *
	 */
	protected ClientNode(InetAddress ip, int port, UserStub userStub) {
		topicCIManager = new CIManager(ip, port);
		this.userStub = userStub;
	}

	protected abstract class ClientThread extends Thread {

		protected final Tag eventTag;
		protected final MessageType messageType;
		protected final String topicName;

		protected ClientThread(Tag eventTag, MessageType messageType, String topicName) {
			super(String.format("ClientThread - %s - %s - %s", eventTag, messageType, topicName));
			this.eventTag = eventTag;
			this.messageType = messageType;
			this.topicName = topicName;
		}

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

				doWork(success, socket, oos, ois);

				userStub.fireEvent(UserEvent.successful(eventTag, topicName));

			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e));
			} catch (final IOException e) {
				Throwable e1 = new ServerException(ClientNode.CONNECTION_TO_SERVER_LOST_STRING, e);
				userStub.fireEvent(UserEvent.failed(eventTag, topicName, e1));
			}

			LG.out();
		}

		protected abstract void doWork(boolean success, Socket socket, ObjectOutputStream oos,
									   ObjectInputStream ois) throws IOException;

		protected Object getMessageValue() {
			return topicName;
		}
	}
}
