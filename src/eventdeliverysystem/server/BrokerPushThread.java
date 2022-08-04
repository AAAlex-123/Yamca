package eventdeliverysystem.server;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import eventdeliverysystem.datastructures.AbstractTopic;
import eventdeliverysystem.datastructures.Packet;
import eventdeliverysystem.datastructures.PostInfo;
import eventdeliverysystem.datastructures.Subscriber;
import eventdeliverysystem.util.LG;

/**
 * A Thread responsible for streaming newly received packets for a BrokerTopic to a single
 * Consumer.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
final class BrokerPushThread extends Thread implements Subscriber {

	private static final long TIMEOUT_MILLIS = 10_000L;

	private static final long NO_CURRENT_POST_ID = -1L;
	private final Deque<PostInfo> postInfoList = new LinkedList<>();
	private final Map<Long, List<Packet>> buffers = new HashMap<>();
	private final Queue<Object> queue = new LinkedList<>();
	private final AbstractTopic topic;
	private final ObjectOutputStream oos;
	private long currentPostId = BrokerPushThread.NO_CURRENT_POST_ID;

	/**
	 * Constructs the Thread that writes some Posts to a stream. This Thread is subscribed to a
	 * Topic and is notified each time there is new data in the Topic.
	 *
	 * @param topic the Topic to subscribe to
	 * @param stream the output stream to which to write the data
	 */
	BrokerPushThread(AbstractTopic topic, ObjectOutputStream stream) {
		super("BrokerPushThread-" + topic.getName());
		this.topic = topic;
		oos = stream;
	}

	@Override
	public void run() {
		topic.subscribe(this);

		while (true) {
			try {
				synchronized (queue) {
					while (queue.isEmpty()) {
						queue.wait(BrokerPushThread.TIMEOUT_MILLIS, 0);
					}
				}

			} catch (final InterruptedException e) {
				try {
					oos.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				Thread.currentThread().interrupt();
			}

			boolean queueNotEmpty;
			do {
				try {
					synchronized (queue) {
						oos.writeObject(queue.remove());
					}
				} catch (final IOException e) {
					return; // topic deleted -> socket closed by broker
				}

				synchronized (queue) {
					queueNotEmpty = !queue.isEmpty();
				}
			} while (queueNotEmpty);
		}
	}

	@Override
	public synchronized void notify(PostInfo postInfo, String topicName) {
		LG.sout("BrokerPushThread#notify(%s)", postInfo);

		// if no post is being streamed
		if (currentPostId == BrokerPushThread.NO_CURRENT_POST_ID) {
			// set post as current being streamed
			currentPostId = postInfo.getId();
			// start streaming post
			queue.add(postInfo);
		} else {
			// add this post to buffer
			postInfoList.addLast(postInfo);
			buffers.put(postInfo.getId(), new LinkedList<>());
		}

		synchronized (queue) {
			queue.notifyAll();
		}
	}

	@Override
	public synchronized void notify(Packet packet, String topicName) {
		LG.sout("BrokerPushThread#notify(%s)", packet);

		// if no post is being streamed
		assert currentPostId != BrokerPushThread.NO_CURRENT_POST_ID;

		// if packet does not belong to post being streamed
		long incomingPostId = packet.getPostId();
		if (incomingPostId != currentPostId) {

			// add packet to buffer because it's not being streamed
			buffers.get(incomingPostId).add(packet);
		} else {
			// stream packet
			queue.add(packet);

			// if current post is fully streamed
			if (packet.isFinal()) {

				// start streaming next post
				boolean finalReached;
				do {

					// if no posts left in buffer, mark current as none
					// wait next post info
					if (postInfoList.isEmpty()) {
						currentPostId = BrokerPushThread.NO_CURRENT_POST_ID;
						break;
					}

					// take next Post
					final PostInfo curr = postInfoList.removeFirst();

					// start streaming post
					queue.add(curr);

					// set as current
					currentPostId = curr.getId();

					// stream all packets in buffer
					finalReached = emptyBufferOfCurrentPost();

					// keep streaming the next post in buffer if the previous has been fully
					// streamed
				} while (finalReached);
			}
		}

		synchronized (queue) {
			queue.notifyAll();
		}
	}

	private boolean emptyBufferOfCurrentPost() {

		final List<Packet> buffer = buffers.get(currentPostId);
		final Packet lastPacket = buffer.get(buffer.size() - 1);

		for (final Packet packetInBuffer : buffer) {

			// stream packet
			queue.add(packetInBuffer);

			assert !(packetInBuffer.isFinal() && (packetInBuffer != lastPacket));
		}

		// whether this post has been fully streamed
		boolean fullyStreamed = lastPacket.isFinal();
		if (fullyStreamed) {
			buffers.remove(currentPostId);
		}

		return fullyStreamed;
	}
}
