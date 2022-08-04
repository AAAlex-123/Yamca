package eventDeliverySystem.thread;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
	 * Defines the different Protocols used to push data.
	 *
	 * @author Alex Mandelias
	 */
	public enum Protocol {

		/** Tell Pull Thread to receive a set amount of data and stop */
		NORMAL,

		/**
		 * Tell Pull Thread to always wait to receive data. Subsequent Push Threads to
		 * the same output stream should use the 'WITHOUT_COUNT' protocol.
		 */
		KEEP_ALIVE,

		/**
		 * Just send the data, don't send the amount of data. Should be used in
		 * conjunction with 'KEEP_ALIVE', for pushing after the Pull Thread has started.
		 */
		WITHOUT_COUNT;
	}

	private final ObjectOutputStream  oos;
	private final Optional<String>    topicName;
	private final List<PostInfo>      postInfos;
	private final Map<Long, Packet[]> packets;
	private final Protocol            protocol;
	private final Optional<Callback>  callback;

	/**
	 * Constructs the Thread that, when run, will write some Posts to a stream.
	 *
	 * @param stream    the output stream to which to write the Posts
	 * @param postInfos the PostInfo objects to write to the stream
	 * @param packets   the array of Packets to write for each PostInfo object
	 * @param protocol  the protocol to use when pushing, which alters the behaviour
	 *                  of the Pull Thread
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
	 * @param callback  the callback to call when this thread finishes execution
	 *
	 * @see Protocol
	 * @see Callback
	 */
	public PushThread(ObjectOutputStream stream, String topicName, List<PostInfo> postInfos,
	        Map<Long, Packet[]> packets, Protocol protocol, Callback callback) {
		super("PushThread-" + postInfos.size() + "-" + protocol);
		oos = stream;
		this.topicName = Optional.ofNullable(topicName);
		this.postInfos = postInfos;
		this.packets = packets;
		this.protocol = protocol;
		this.callback = Optional.ofNullable(callback);
	}

	@Override
	public void run() {
		LG.sout("%s#run()", getName());
		LG.in();

		boolean success;
		try {

			LG.sout("protocol=%s, posts.size()=%d", protocol, postInfos.size());
			LG.in();

			if (protocol != Protocol.WITHOUT_COUNT) {
				final int postCount;
				if (protocol == Protocol.KEEP_ALIVE)
					postCount = Integer.MAX_VALUE;
				else
					postCount = postInfos.size();

				oos.writeInt(postCount);
			}

			for (final PostInfo postInfo : postInfos) {
				LG.sout("postInfo=%s", postInfo);
				oos.writeObject(postInfo);

				final Packet[] packetArray = packets.get(postInfo.getId());
				for (final Packet packet : packetArray)
					oos.writeObject(packet);
			}

			success = true;
			LG.out();

		} catch (final IOException e) {
			System.err.printf("IOException in PushThread#run()%n");
			e.printStackTrace();
			success = false;
		}

		callback.orElse(Callback.EMPTY).onCompletion(success, topicName.orElse(null));

		LG.out();
		LG.sout("/%s#run()", getName());
	}

	/**
	 * Provides a way for the PushThread to pass a message when it has finished
	 * executing. Right before a PushThread returns, it calls the
	 * {@link Callback#onCompletion(boolean, String)} method of the Callback
	 * provided, if it exists.
	 *
	 * @author Alex Mandelias
	 */
	@FunctionalInterface
	public static interface Callback {

		/** A Callback that does nothing */
		static final Callback EMPTY = (success, topicName) -> {};

		/**
		 * The code to call when the PushThread finishes executing.
		 *
		 * @param success   {@code true} if the PushThread terminates successfully,
		 *                  {@code false} otherwise
		 * @param topicName the name of the Topic to which the PushThread pushed
		 */
		void onCompletion(boolean success, String topicName);
	}
}
