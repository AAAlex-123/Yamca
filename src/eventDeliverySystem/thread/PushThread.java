package eventDeliverySystem.thread;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.util.LG;

/**
 * A Thread that writes some Posts to a stream.
 *
 * @author Alex Mandelias
 */
public class PushThread extends Thread {

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

	private final ObjectOutputStream  oos;
	private final String              topicName;
	private final List<PostInfo>      postInfos;
	private final Map<Long, Packet[]> packets;
	private final Protocol            protocol;
	private final Callback            callback;

	/**
	 * Constructs the Thread that writes some Posts to a stream.
	 *
	 * @param stream    the output stream to which to write the Posts
	 * @param postInfos the PostInfo objects to write to the stream
	 * @param packets   the array of Packets to write for each PostInfo object
	 * @param protocol  the protocol to use when pushing, which alters the behaviour of the Pull
	 *                  Thread
	 *
	 * @see Protocol
	 */
	public PushThread(ObjectOutputStream stream, List<PostInfo> postInfos,
	        Map<Long, Packet[]> packets, Protocol protocol) {
		this(stream, null, postInfos, packets, protocol, null);
	}

	/**
	 * Constructs the Thread that, when run, will write some Posts to a stream.
	 *
	 * @param stream    the output stream to which to write the Posts
	 * @param topicName the name of the Topic that corresponds to the stream
	 * @param postInfos the PostInfo objects to write to the stream
	 * @param packets   the array of Packets to write for each PostInfo object
	 * @param protocol  the protocol to use when pushing, which alters the behaviour
	 *                  of the Pull Thread
	 * @param callback  the callback to call right before finishing execution
	 *
	 * @throws NullPointerException if a callback is provided but topicName is {@code null}
	 *
	 * @see Protocol
	 * @see Callback
	 */
	public PushThread(ObjectOutputStream stream, String topicName, List<PostInfo> postInfos,
	        Map<Long, Packet[]> packets, Protocol protocol, Callback callback) {
		super("PushThread-" + postInfos.size() + "-" + protocol);

		if (callback != null && topicName == null)
			throw new NullPointerException("topicName can't be null if a callback is provided");

		oos = stream;
		this.topicName = topicName;
		this.postInfos = postInfos;
		this.packets = packets;
		this.protocol = protocol;
		this.callback = callback;
	}

	@Override
	public void run() {
		LG.sout("%s#run()", getName());
		LG.in();

		try {

			LG.sout("protocol=%s, posts.size()=%d", protocol, postInfos.size());
			LG.in();

			final int postCount = protocol == Protocol.NORMAL
					? postInfos.size()
					: Integer.MAX_VALUE;

			oos.writeInt(postCount);

			for (final PostInfo postInfo : postInfos) {
				LG.sout("postInfo=%s", postInfo);
				oos.writeObject(postInfo);

				final Packet[] packetArray = packets.get(postInfo.getId());
				for (final Packet packet : packetArray)
					oos.writeObject(packet);
			}

			oos.flush();

			if (callback != null)
				callback.onCompletion(true, topicName, null);

			LG.out();

		} catch (final IOException e) {
			LG.err("IOException in PushThread#run()%n");
			e.printStackTrace();

			if (callback != null)
				callback.onCompletion(false, topicName, e);
		}

		LG.out();
		LG.sout("/%s#run()", getName());
	}
}
