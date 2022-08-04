package eventdeliverysystem.thread;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eventdeliverysystem.datastructures.Packet;
import eventdeliverysystem.datastructures.PostInfo;
import eventdeliverysystem.util.LG;

/**
 * A Thread that writes some Posts to a stream.
 *
 * @author Alex Mandelias
 */
public final class PushThread extends Thread {

	/**
	 * Defines the different Protocols used by the PushThread to push data.
	 *
	 * @author Alex Mandelias
	 */
	public enum Protocol {

		/** Tell the Pull Thread to receive a set amount of data and stop */
		NORMAL,

		/** Tell the Pull Thread to always wait to receive data */
		KEEP_ALIVE,
	}

	private final ObjectOutputStream oos;
	private final String topicName;
	private final List<PostInfo> postInfoList;
	private final Map<Long, Packet[]> packetMap;
	private final Protocol protocol;
	private final Callback callback;

	/**
	 * Constructs the Thread that writes some Posts to a stream.
	 *
	 * @param stream the output stream to which to write the Posts
	 * @param postInfoList the PostInfo objects to write to the stream
	 * @param packetMap the array of Packets to write for each PostInfo object
	 * @param protocol the protocol to use when pushing, which alters the behaviour of the Pull
	 * 		Thread
	 *
	 * @see Protocol
	 */
	public PushThread(ObjectOutputStream stream, List<PostInfo> postInfoList,
			Map<Long, Packet[]> packetMap, Protocol protocol) {
		this(stream, null, postInfoList, packetMap, protocol, null);
	}

	/**
	 * Constructs the Thread that, when run, will write some Posts to a stream.
	 *
	 * @param stream the output stream to which to write the Posts
	 * @param topicName the name of the Topic that corresponds to the stream
	 * @param postInfoList the PostInfo objects to write to the stream
	 * @param packetMap the array of Packets to write for each PostInfo object
	 * @param protocol the protocol to use when pushing, which alters the behaviour of the Pull
	 * 		Thread
	 * @param callback the callback to call right before finishing execution
	 *
	 * @throws NullPointerException if a callback is provided but topicName is {@code null}
	 * @see Protocol
	 * @see Callback
	 */
	public PushThread(ObjectOutputStream stream, String topicName, List<PostInfo> postInfoList,
			Map<Long, Packet[]> packetMap, Protocol protocol, Callback callback) {
		super("PushThread-" + postInfoList.size() + '-' + protocol);

		if (callback != null && topicName == null) {
			throw new NullPointerException("topicName can't be null if a callback is provided");
		}

		oos = stream;
		this.topicName = topicName;
		this.postInfoList = Collections.unmodifiableList(postInfoList);
		this.packetMap = Collections.unmodifiableMap(packetMap);
		this.protocol = protocol;
		this.callback = callback;
	}

	@Override
	public void run() {
		LG.sout("%s#run()", getName());
		LG.in();

		try {

			LG.sout("protocol=%s, posts.size()=%d", protocol, postInfoList.size());
			LG.in();

			final int postCount =
					protocol == Protocol.NORMAL ? postInfoList.size() : Integer.MAX_VALUE;

			oos.writeInt(postCount);

			for (final PostInfo postInfo : postInfoList) {
				LG.sout("postInfo=%s", postInfo);
				oos.writeObject(postInfo);

				final Packet[] packetArray = packetMap.get(postInfo.getId());
				for (final Packet packet : packetArray) {
					oos.writeObject(packet);
				}
			}

			oos.flush();

			if (callback != null) {
				callback.onCompletion(true, topicName, null);
			}

			LG.out();
		} catch (final IOException e) {
			LG.exception(e);

			if (callback != null) {
				callback.onCompletion(false, topicName, e);
			}
		}

		LG.out();
		LG.sout("#%s#run()", getName());
	}
}
