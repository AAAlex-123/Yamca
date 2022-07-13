package eventDeliverySystem.client;

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
import eventDeliverySystem.datastructures.Message.MessageType;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.server.Broker;
import eventDeliverySystem.server.ServerException;
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
		super(ip, port, userStub);
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
		Thread thread = new Publisher.PushThread(post, topicName);
		thread.start();
	}

	/**
	 * Request that the remote server create a new Topic with the specified name by
	 * creating a new Thread that connects to the actual Broker for the Topic.
	 *
	 * @param topicName the name of the new Topic
	 */
	public void createTopic(String topicName) {
		LG.sout("Publisher#createTopic(%s)", topicName);
		Thread thread = new CreateTopicThread(topicName);
		thread.start();
	}

	/**
	 * Request that the remote server delete the existing Topic with the specified name by
	 * creating a new Thread that connects to the actual Broker for the Topic.
	 *
	 * @param topicName the name of the new Topic
	 */
	public void deleteTopic(String topicName) {
		LG.sout("Publisher#deleteTopic(%s)", topicName);
		Thread thread = new DeleteTopicThread(topicName);
		thread.start();
	}

	private class PushThread extends ClientThread {

		private final Post post;

		/**
		 * Constructs a new PostThread that connects to the actual Broker and starts a
		 * PushThread to post the Post.
		 *
		 * @param post      the Post
		 * @param topicName the name of the Topic to which to push the Post
		 */
		public PushThread(Post post, String topicName) {
			super(Tag.MESSAGE_SENT, MessageType.DATA_PACKET_SEND, topicName);
			this.post = post;
		}

		@Override
		protected void doWork(boolean success, Socket socket, ObjectOutputStream oos,
							  ObjectInputStream ois) throws IOException {
			try {
				if (!success)
					throw new ServerException(ClientNode.getTopicDNEString(topicName));

				final PostInfo postInfo = post.getPostInfo();
				final List<PostInfo> postInfos = new LinkedList<>();
				final Map<Long, Packet[]> packets = new HashMap<>();

				postInfos.add(postInfo);
				packets.put(postInfo.getId(), Packet.fromPost(post));

				final Thread pushThread = new eventDeliverySystem.thread.PushThread(oos, topicName,
						postInfos, packets, Protocol.NORMAL, (callbackSuccess, callbackTopicName,
															  callbackCause) -> {
					if (callbackSuccess)
						userStub.fireEvent(UserEvent.successful(eventTag, callbackTopicName));
					else {
						Exception e = new ServerException(ClientNode.CONNECTION_TO_SERVER_LOST_STRING,
								callbackCause);
						userStub.fireEvent(UserEvent.failed(eventTag, callbackTopicName, e));
					}
				});

				pushThread.run();

			} finally {
				socket.close();
			}
		}
	}

	private class CreateTopicThread extends ClientThread {

		public CreateTopicThread(String topicName) {
			super(Tag.TOPIC_CREATED, MessageType.CREATE_TOPIC, topicName);
		}

		@Override
		protected void doWork(boolean success, Socket socket, ObjectOutputStream oos,
							  ObjectInputStream ois) throws IOException {
			try {
				if (!success)
					throw new ServerException(ClientNode.getTopicAEString(topicName));
			} finally {
				socket.close();
			}
		}
	}

	private class DeleteTopicThread extends ClientThread {

		public DeleteTopicThread(String topicName) {
			super(Tag.TOPIC_DELETED, MessageType.DELETE_TOPIC, topicName);
		}

		@Override
		protected void doWork(boolean success, Socket socket, ObjectOutputStream oos,
							  ObjectInputStream ois) throws IOException {
			try {
				if (!success)
					throw new ServerException(ClientNode.getTopicDNEString(topicName));
			} finally {
				socket.close();
			}
		}
	}
}
