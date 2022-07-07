package eventDeliverySystem.server;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.util.LG;
import eventDeliverySystem.util.Subscriber;

/**
 * A thread that receives packets for a certain Topic and streams them to a
 * Consumer.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
class BrokerPushThread extends Thread implements Subscriber {

	private static final long NO_CURRENT_POST_ID = -1;

	private long                          currentPostId;
	private final Deque<PostInfo>         postInfos;
	private final Map<Long, List<Packet>> buffers;

	private final Queue<Object>      queue;
	private final ObjectOutputStream oos;

	private final Object monitor = new Object();

	/**
	 * Constructs the Thread that, when run, will write some Posts to a stream. This
	 * Thread is subscribed to a Topic and is notified each time there is new data
	 * in the Topic.
	 *
	 * @param topic  the Topic to subscribe to
	 * @param stream the output stream to which to write the data
	 */
	public BrokerPushThread(AbstractTopic topic, ObjectOutputStream stream) {
		super("BrokerPushThread-" + topic.getName());
		topic.subscribe(this);

		currentPostId = BrokerPushThread.NO_CURRENT_POST_ID;
		postInfos = new LinkedList<>();
		buffers = new HashMap<>();

		queue = new LinkedList<>();
		oos = stream;
	}

	@Override
	public void run() {
		boolean isEmpty;
		while (true) {
			synchronized (queue) {
				isEmpty = queue.isEmpty();
			}

			while (isEmpty) {
				try {
					synchronized (monitor) {
						monitor.wait();
					}
				} catch (final InterruptedException e) {}

				synchronized (queue) {
					isEmpty = queue.isEmpty();
				}
			}

			do {
				try {
					synchronized (queue) {
						oos.writeObject(queue.remove());
					}
				} catch (final IOException e) {
					e.printStackTrace();
				}

				synchronized (queue) {
					isEmpty = queue.isEmpty();
				}
			} while (!isEmpty);
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
			postInfos.addLast(postInfo);
			buffers.put(postInfo.getId(), new LinkedList<>());
		}

		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	@Override
	public synchronized void notify(Packet packet, String topicName) {
		LG.sout("BrokerPushThread#notify(%s)", packet);

		// if no post is being streamed
		assert currentPostId != BrokerPushThread.NO_CURRENT_POST_ID;

		// if packet belongs to post being streamed
		if (packet.getPostId() == currentPostId) {
			// stream packet
			queue.add(packet);

			// if current post is fully streamed
			if (packet.isFinal()) {

				// start streaming next post
				boolean finalReached;
				do {

					// if no posts left in buffer, mark current as none
					// wait next post info
					if (postInfos.isEmpty()) {
						currentPostId = BrokerPushThread.NO_CURRENT_POST_ID;
						break;
					}

					// take next Post
					final PostInfo curr = postInfos.removeFirst();

					// start streaming post
					queue.add(curr);

					// set as current
					currentPostId = curr.getId();

					// stream all packets in buffer
					finalReached = emptyBufferOfCurrentPost();

					// keep streaming the next post in buffer if the previous has been fully streamed
				} while (finalReached);
			}
		} else {
			// add packet to buffer because it's not being streamed
			buffers.get(packet.getPostId()).add(packet);
		}

		synchronized (monitor) {
			monitor.notifyAll();
		}
	}

	private boolean emptyBufferOfCurrentPost() {
		boolean finalReached = false;

		final List<Packet> buffer = buffers.get(currentPostId);
		for (final Packet packetInBuffer : buffer) {

			assert !finalReached;

			// stream packet
			queue.add(packetInBuffer);

			// mark if this post has been fully streamed
			finalReached |= packetInBuffer.isFinal();
		}

		if (finalReached)
			buffers.remove(currentPostId);

		return finalReached;
	}
}
