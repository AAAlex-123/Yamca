package eventDeliverySystem.thread;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.util.LG;

/**
 * A Thread that reads some Posts from a stream and then posts them to a Topic.
 *
 * @author Alex Mandelias
 */
public class PullThread extends Thread {

	private final ObjectInputStream ois;
	private final AbstractTopic     topic;
	private final Callback callback;

	/**
	 * Constructs the Thread that, when run, will read some Posts from a stream and
	 * post them to a Topic.
	 *
	 * @param stream the input stream from which to read the Posts
	 * @param topic  the Topic in which the new Posts will be added
	 */
	public PullThread(ObjectInputStream stream, AbstractTopic topic) {
		this(stream, topic, null);
	}

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

		} catch (final EOFException e) {
			LG.sout("EOF EXCEPTION ON PULL THREAD");
			if (callback != null) {
				callback.onCompletion(true, topic.getName(), e);
			}
		} catch (final ClassNotFoundException | IOException e) {
			LG.err("IOException in PullThread#run()");
			e.printStackTrace();

			if (callback != null)
				callback.onCompletion(false, topic.getName(), e);
		}

		LG.sout("/%s#run()", getName());
	}
}
