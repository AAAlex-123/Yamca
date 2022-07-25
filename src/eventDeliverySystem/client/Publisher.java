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

import eventDeliverySystem.client.User.UserStub;
import eventDeliverySystem.datastructures.Message.MessageType;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.server.Broker;
import eventDeliverySystem.thread.PushThread.Protocol;
import eventDeliverySystem.client.UserEvent.Tag;
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
final class Publisher extends ClientNode {

	/**
	 * Constructs a Publisher.
	 *
	 * @param serverIP   the IP of the default broker, interpreted as
	 *                          {@link InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 * @param userStub          the UserStub object that will be notified if a push
	 *                          fails
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 */
	Publisher(String serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		super(serverIP, serverPort, userStub);
	}

	/**
	 * Constructs a Publisher.
	 *
	 * @param serverIP   the IP of the default broker, interpreted as
	 *                          {@link InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 * @param userStub          the UserStub object that will be notified if a push
	 *                          fails
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 */
	Publisher(byte[] serverIP, int serverPort, UserStub userStub) throws UnknownHostException {
		super(serverIP, serverPort, userStub);
	}

	/**
	 * Pushes a Post by creating a new Thread that connects to the actual Broker and
	 * starts a PushThread.
	 *
	 * @param post      the Post
	 * @param topicName the name of the Topic to which to push the Post
	 */
	void push(Post post, String topicName) {
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
	void createTopic(String topicName) {
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
	void deleteTopic(String topicName) {
		LG.sout("Publisher#deleteTopic(%s)", topicName);
		Thread thread = new DeleteTopicThread(topicName);
		thread.start();
	}

	private final class PushThread extends ClientThread {

		private final Post post;

		/**
		 * Constructs a new PostThread that connects to the actual Broker and starts a
		 * PushThread to post the Post.
		 *
		 * @param post      the Post
		 * @param topicName the name of the Topic to which to push the Post
		 */
		private PushThread(Post post, String topicName) {
			super(Tag.MESSAGE_SENT, MessageType.DATA_PACKET_SEND, topicName);
			this.post = post;
		}

		@Override
		protected void doWorkAndMaybeCloseSocket(boolean success, Socket socket, ObjectOutputStream oos,
												 ObjectInputStream ois) throws IOException {
			try {
				if (!success)
					throw new ServerException(ClientNode.getTopicDNEString(topicName));

				final PostInfo postInfo = post.getPostInfo();
				final List<PostInfo> postInfoList = new LinkedList<>();
				postInfoList.add(postInfo);

				final Map<Long, Packet[]> packetMap = new HashMap<>();
				packetMap.put(postInfo.getId(), Packet.fromPost(post));

				final Thread pushThread = new eventDeliverySystem.thread.PushThread(oos, topicName,
						postInfoList, packetMap, Protocol.NORMAL, (callbackSuccess, callbackTopicName,
															  callbackCause) -> {
					if (!callbackSuccess) {
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

	private final class CreateTopicThread extends ClientThread {

		private CreateTopicThread(String topicName) {
			super(Tag.TOPIC_CREATED, MessageType.CREATE_TOPIC, topicName);
		}

		@Override
		protected void doWorkAndMaybeCloseSocket(boolean success, Socket socket, ObjectOutputStream oos,
												 ObjectInputStream ois) throws IOException {
			try {
				if (!success)
					throw new ServerException(ClientNode.getTopicAEString(topicName));
			} finally {
				socket.close();
			}
		}
	}

	private final class DeleteTopicThread extends ClientThread {

		private DeleteTopicThread(String topicName) {
			super(Tag.SERVER_TOPIC_DELETED, MessageType.DELETE_TOPIC, topicName);
		}

		@Override
		protected void doWorkAndMaybeCloseSocket(boolean success, Socket socket, ObjectOutputStream oos,
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
