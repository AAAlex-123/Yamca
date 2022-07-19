package eventDeliverySystem.datastructures;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Abstract superclass of all Topics.
 *
 * @author Alex Mandelias
 */
public abstract class AbstractTopic implements Iterable<Post> {

	/** Constant to be used when no post exists and an ID is needed */
	public static final long FETCH_ALL_POSTS = -1L;

	/**
	 * Creates a simple Topic that contains the given Posts and can be posted to. This method is
	 * intended to be used for reading a Topic from a ITopicDAO object and then using a copy
	 * constructor of a concrete Topic to obtain the desired Topic subclass.
	 *
	 * @param name the name of the simple Topic
	 * @param posts the Posts to add to the Topic
	 *
	 * @return the Topic
	 */
	public static AbstractTopic createSimple(String name, List<Post> posts) {
		AbstractTopic simple = new SimpleTopic(name);
		posts.forEach(post -> {
			simple.post(post.getPostInfo());
			for (Packet packet : Packet.fromPost(post))
				simple.post(packet);
		});
		return simple;
	}

	private final String          name;
	private final Set<Subscriber> subscribers;

	/**
	 * Constructs an empty Topic with no subscribers.
	 *
	 * @param name the name of the new Topic
	 */
	protected AbstractTopic(String name) {
		// TODO: throw NPE if name == null
		// TODO: add @throws in javadoc
		this.name = name;
		subscribers = new HashSet<>();
	}

	/**
	 * Returns this Topic's name.
	 *
	 * @return the name
	 */
	public final String getName() {
		return name;
	}

	/**
	 * Adds a Subscriber to this Topic.
	 *
	 * @param sub the Subscriber to add
	 */
	public final void subscribe(Subscriber sub) {
		subscribers.add(sub);
	}

	/**
	 * Removes a Subscriber from this Topic.
	 *
	 * @param sub the Subscriber to remove
	 *
	 * @return {@code true} if the Subscriber was subscribed to this Topic,
	 *         {@code false} otherwise
	 */
	public final boolean unsubscribe(Subscriber sub) {
		return subscribers.remove(sub);
	}

	/**
	 * Posts a PostInfo to this Topic and notifies all subscribers.
	 *
	 * @param postInfo the PostInfo
	 */
	public final synchronized void post(PostInfo postInfo) {
		postHook(postInfo);
		for (final Subscriber sub : subscribers)
			sub.notify(postInfo, name);
	}

	/**
	 * Posts a Packet to this Topic and notifies all subscribers.
	 *
	 * @param packet the Packet
	 */
	public final synchronized void post(Packet packet) {
		postHook(packet);
		for (final Subscriber sub : subscribers)
			sub.notify(packet, name);
	}

	/**
	 * Allows each subclass to specify how the template method is implemented. This
	 * method is effectively synchronized.
	 *
	 * @param postInfo the PostInfo
	 *
	 * @see AbstractTopic#post(PostInfo)
	 */
	protected abstract void postHook(PostInfo postInfo);

	/**
	 * Allows each subclass to specify how the template method is implemented. This
	 * method is effectively synchronized.
	 *
	 * @param packet the Packet
	 *
	 * @see AbstractTopic#post(Packet)
	 */
	protected abstract void postHook(Packet packet);

	/**
	 * Returns the hash that a Topic with a given name would have. Since a Topic's
	 * hash is determined solely by its name, this method returns the same result as
	 * Topic#hashCode(), when given the name of the Topic, and can be used when an
	 * instance of Topic is not available, but its name is known.
	 *
	 * @param topicName the name of the Topic for which to compute the hash
	 *
	 * @return a hash code value for this Topic
	 */
	public static int hashForTopic(String topicName) {
		try {
			final MessageDigest a = MessageDigest.getInstance("md5");
			final byte[]        b = a.digest(topicName.getBytes());

			// big brain stuff
			final int    FOUR = 4;
			final int    c    = FOUR;
			final int    d    = b.length / c;
			final byte[] e    = new byte[c];
			for (int f = 0; f < e.length; f++)
				for (int g = 0; g < d; g++)
					e[f] ^= (b[(d * f) + g]);

			final BigInteger h = new BigInteger(e);
			return h.intValueExact();

		} catch (NoSuchAlgorithmException | ArithmeticException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return String.format("AbstractTopic[name=%s, subCount=%d]", name, subscribers.size());
	}

	@Override
	public int hashCode() {
		return AbstractTopic.hashForTopic(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof AbstractTopic))
			return false;
		final AbstractTopic other = (AbstractTopic) obj;
		return Objects.equals(name, other.name); // same name == same Topic, can't have duplicate names
	}

	private static final class SimpleTopic extends AbstractTopic {

		private final List<Post> posts = new LinkedList<>();

		private SimpleTopic(String name) {
			super(name);
		}

		private PostInfo currPI;
		private final List<Packet> currPackets = new LinkedList<>();

		@Override
		public void postHook(PostInfo postInfo) {
			if (!currPackets.isEmpty() || (currPI != null))
				throw new IllegalStateException("Received PostInfo while more Packets remain");

			currPI = postInfo;
		}

		@Override
		public void postHook(Packet packet) {
			currPackets.add(packet);

			if (packet.isFinal()) {
				final Packet[] data          = currPackets.toArray(new Packet[currPackets.size()]);
				final Post     completedPost = Post.fromPackets(data, currPI);
				posts.add(completedPost);

				currPI = null;
				currPackets.clear();
			}
		}

		@Override
		public Iterator<Post> iterator() {
			return posts.iterator();
		}
	}
}
