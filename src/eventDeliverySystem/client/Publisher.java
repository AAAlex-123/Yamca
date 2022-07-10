package eventDeliverySystem.client;

import static eventDeliverySystem.datastructures.Message.MessageType.CREATE_TOPIC;
import static eventDeliverySystem.datastructures.Message.MessageType.DATA_PACKET_SEND;
import static eventDeliverySystem.datastructures.Message.MessageType.DELETE_TOPIC;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eventDeliverySystem.user.User.UserStub;
import eventDeliverySystem.datastructures.ConnectionInfo;
import eventDeliverySystem.datastructures.Message;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.server.Broker;
import eventDeliverySystem.server.ServerException;
import eventDeliverySystem.thread.PushThread;
import eventDeliverySystem.thread.PushThread.Protocol;
import eventDeliverySystem.user.UserEvent;
import eventDeliverySystem.user.UserEvent.Tag;
import eventDeliverySystem.util.LG;

/**
 * A client-side process which is responsible for creating Topics and pushing
 * Posts to them by connecting to a remote server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirbas
 *
 * @see Broker
 */
public class Publisher extends ClientNode {

	private final UserStub userStub;

	/**
	 * Constructs a Publisher.
	 *
	 * @param defaultServerIP   the IP of the default broker, interpreted as
	 *                          {@link InetAddress#getByName(String)}.
	 * @param defaultServerPort the port of the default broker
	 * @param userStub          the UserStub object that will be notified if a push
	 *                          fails
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 */
	public Publisher(String defaultServerIP, int defaultServerPort, UserStub userStub)
	        throws UnknownHostException {
		this(InetAddress.getByName(defaultServerIP), defaultServerPort, userStub);
	}

	/**
	 * Constructs a Publisher.
	 *
	 * @param defaultServerIP   the IP of the default broker, interpreted as
	 *                          {@link InetAddress#getByAddress(byte[])}.
	 * @param defaultServerPort the port of the default broker
	 * @param userStub          the UserStub object that will be notified if a push
	 *                          fails
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	public Publisher(byte[] defaultServerIP, int defaultServerPort, UserStub userStub)
	        throws UnknownHostException {
		this(InetAddress.getByAddress(defaultServerIP), defaultServerPort, userStub);
	}

	/**
	 * Constructs a Publisher.
	 *
	 * @param ip       the InetAddress of the default broker
	 * @param port     the port of the default broker
	 * @param userStub the UserStub object that will be notified if a push fails
	 */
	private Publisher(InetAddress ip, int port, UserStub userStub) {
		super(ip, port);
		this.userStub = userStub;
	}

	/**
	 * Pushes a Post by creating a new Thread that connects to the actual Broker and
	 * starts a PushThread.
	 *
	 * @param post      the Post
	 * @param topicName the name of the Topic to which to push the Post
	 */
	public void push(Post post, String topicName) {
		LG.sout("Publisher#push(%s, %s)", post, topicName);
		Thread thread = new PostThread(post, topicName);
		thread.start();
	}

	/**
	 * Request that the remote server create a new Topic with the specified name by
	 * connecting to the actual Broker for the Topic.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @return {@code true} if Topic was successfully created, {@code false} if an
	 *         IOException occurred while transmitting the request or if a Topic
	 *         with that name already exists
	 *
	 * @throws ServerException if a connection to the server fails
	 */
	public boolean createTopic(String topicName) throws ServerException {

		final ConnectionInfo actualBrokerCI = topicCIManager.getConnectionInfoForTopic(topicName);

		try (Socket socket = new Socket(actualBrokerCI.getAddress(), actualBrokerCI.getPort())) {
			final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			oos.writeObject(new Message(CREATE_TOPIC, topicName));

			return ois.readBoolean(); // true or false, successful creation or not

		} catch (final IOException e) {
			throw new ServerException(topicName, e);
		}
	}

	/**
	 * Request that the remote server delete the existing Topic with the specified name, by
	 * connecting to the actual Broker for the Topic.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @return {@code true} if Topic was successfully deleted, {@code false} if an
	 *         IOException occurred while transmitting the request or no Topic
	 *         with that name exists
	 *
	 * @throws ServerException if a connection to the server fails
	 */
	public boolean deleteTopic(String topicName) throws ServerException {

		final ConnectionInfo actualBrokerCI = topicCIManager.getConnectionInfoForTopic(topicName);

		try (Socket socket = new Socket(actualBrokerCI.getAddress(), actualBrokerCI.getPort())) {
			final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			oos.writeObject(new Message(DELETE_TOPIC, topicName));

			return ois.readBoolean(); // true or false, successful deletion or not

		} catch (final IOException e) {
			throw new ServerException(topicName, e);
		}
	}

	private class PostThread extends Thread {

		private final Post   post;
		private final String topicName;

		/**
		 * Constructs a new PostThread that connects to the actual Broker and starts a
		 * PushThread to post the Post.
		 *
		 * @param post      the Post
		 * @param topicName the name of the Topic to which to push the Post
		 */
		public PostThread(Post post, String topicName) {
			super("PostThread");
			this.post = post;
			this.topicName = topicName;
		}

		@Override
		public void run() {
			LG.sout("PostThread#run()");
			LG.in();

			final ConnectionInfo actualBrokerCI;
			try {
				actualBrokerCI = topicCIManager.getConnectionInfoForTopic(topicName);
			} catch (ServerException e) {
				userStub.fireEvent(UserEvent.failed(Tag.MESSAGE_SENT, topicName, e));
				return;
			}

			LG.sout("actualBrokerCI=%s", actualBrokerCI);
			try (Socket socket = new Socket(actualBrokerCI.getAddress(),
			        actualBrokerCI.getPort())) {

				final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				oos.flush();

				// don't remove the following line: https://stackoverflow.com/questions/72920493/
				final ObjectInputStream unused = new ObjectInputStream(socket.getInputStream());

				oos.writeObject(new Message(DATA_PACKET_SEND, topicName));

				final PostInfo            postInfo  = post.getPostInfo();
				final List<PostInfo>      postInfos = new LinkedList<>();
				final Map<Long, Packet[]> packets   = new HashMap<>();

				postInfos.add(postInfo);
				packets.put(postInfo.getId(), Packet.fromPost(post));

				final PushThread pushThread = new PushThread(oos, topicName, postInfos,
						packets, Protocol.NORMAL, (success, callbackTopicName, cause) -> {
							if (success)
								userStub.fireEvent(
										UserEvent.successful(Tag.MESSAGE_SENT, callbackTopicName));
							else
								userStub.fireEvent(
										UserEvent.failed(Tag.MESSAGE_SENT, callbackTopicName, cause));
						});

				pushThread.run();

			} catch (final IOException e) {
				userStub.fireEvent(UserEvent.failed(Tag.MESSAGE_SENT, topicName, e));
			}
			LG.out();
		}
	}
}
