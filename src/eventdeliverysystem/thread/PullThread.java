package eventdeliverysystem.thread;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.SocketException;

import eventdeliverysystem.datastructures.AbstractTopic;
import eventdeliverysystem.datastructures.Packet;
import eventdeliverysystem.datastructures.PostInfo;
import eventdeliverysystem.util.LG;

/**
 * A Thread that reads some Posts from a stream and then posts them to a Topic as they arrive.
 *
 * @author Alex Mandelias
 */
public final class PullThread extends Thread {

	private final ObjectInputStream ois;
	private final AbstractTopic topic;
	private final Callback callback;

	/**
	 * Constructs the Thread that reads some Posts from a stream and posts them to a Topic.
	 *
	 * @param stream the input stream from which to read the Posts
	 * @param topic the Topic in which the new Posts will be added
	 */
	public PullThread(ObjectInputStream stream, AbstractTopic topic) {
		this(stream, topic, null);
	}

	/**
	 * Constructs the Thread that reads some Posts from a stream and posts them to a Topic and
	 * additionally calls a callback right before finishing execution.
	 *
	 * @param stream the input stream from which to read the Posts
	 * @param topic the Topic in which the new Posts will be added
	 * @param callback the callback to call right before finishing execution
	 *
	 * @see Callback
	 */
	public PullThread(ObjectInputStream stream, AbstractTopic topic, Callback callback) {
		super("PullThread-" + topic.getName());
		ois = stream;
		this.topic = topic;
		this.callback = callback;
	}

	@Override
	public void run() {
		LG.sout("%s#run()", getName());

		try {
			final int postCount = ois.readInt();
			LG.sout("postCount=%d", postCount);

			for (int i = 0; i < postCount; i++) {

				final PostInfo postInfo = (PostInfo) ois.readObject();

				LG.in();
				LG.sout("postInfo=%s", postInfo);
				topic.post(postInfo);

				Packet packet;
				do {
					packet = (Packet) ois.readObject();

					LG.sout("packet=%s", packet);

					topic.post(packet);
				} while (!packet.isFinal());

				LG.out();
			}

			if (callback != null) {
				callback.onCompletion(true, topic.getName(), null);
			}
		} catch (final EOFException | SocketException e) {
			LG.sout("EOF/SOCKET EXCEPTION ON PULL THREAD");
			e.printStackTrace();

			if (callback != null) {
				callback.onCompletion(true, topic.getName(), e);
			}
		} catch (final ClassNotFoundException | IOException e) {
			LG.err("IOException in PullThread#run()");
			e.printStackTrace();

			if (callback != null) {
				callback.onCompletion(false, topic.getName(), e);
			}
		}

		LG.sout("#%s#run()", getName());
	}
}
