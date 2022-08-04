package eventdeliverysystem.datastructures;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
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

	private final Object postLock = new Object();

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
			for (Packet packet : Packet.fromPost(post)) {
				simple.post(packet);
			}
		});
		return simple;
	}

	private final String name;
	private final Set<Subscriber> subscribers;

	/**
	 * Constructs an empty Topic with no subscribers.
	 *
	 * @param name the name of the new Topic
	 */
	protected AbstractTopic(String name) {
		this.name = name;
		subscribers = new HashSet<>();
	}

	/**
	 * Returns a Topic Token for this Abstract Topic.
	 *
	 * @return the Topic Token
	 *
	 * @see TopicToken
	 */
	public final TopicToken getToken() {
		return new TopicToken(this);
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
	 * Returns the ID of the most recent post in this Topic.
	 *
	 * @return the most recent Post's ID or {@link AbstractTopic#FETCH_ALL_POSTS} if there are no
	 * 		Posts in this Topic
	 */
	protected abstract long getLastPostId();

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
	 * @return {@code true} if the Subscriber was subscribed to this Topic, {@code false} otherwise
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
		for (final Subscriber sub : subscribers) {
			sub.notify(postInfo, name);
		}
	}

	/**
	 * Posts a Packet to this Topic and notifies all subscribers.
	 *
	 * @param packet the Packet
	 */
	public final synchronized void post(Packet packet) {
		postHook(packet);
		for (final Subscriber sub : subscribers) {
			sub.notify(packet, name);
		}
	}

	/**
	 * Allows each subclass to specify how the template method is implemented. This method is
	 * effectively synchronized.
	 *
	 * @param postInfo the PostInfo
	 *
	 * @see AbstractTopic#post(PostInfo)
	 */
	protected abstract void postHook(PostInfo postInfo);

	/**
	 * Allows each subclass to specify how the template method is implemented. This method is
	 * effectively synchronized.
	 *
	 * @param packet the Packet
	 *
	 * @see AbstractTopic#post(Packet)
	 */
	protected abstract void postHook(Packet packet);

	/**
	 * Returns the hash that a Topic with a given name would have. Since a Topic's hash is
	 * determined solely by its name, this method returns the same result as Topic#hashCode(), when
	 * given the name of the Topic, and can be used when an instance of Topic is not available, but
	 * its name is known.
	 *
	 * @param topicName the name of the Topic for which to compute the hash
	 *
	 * @return a hash code value for this Topic
	 */
	public static int hashForTopic(String topicName) {
		try {
			final MessageDigest a = MessageDigest.getInstance("md5");
			final byte[] b = a.digest(topicName.getBytes(StandardCharsets.UTF_8));

			// big brain stuff
			final int FOUR = 4;
			final int c = FOUR;
			final int d = b.length / c;
			final byte[] e = new byte[c];
			for (int f = 0; f < e.length; f++) {
				for (int g = 0; g < d; g++) {
					e[f] = (byte) (e[f] ^ (b[(d * f) + g]));
				}
			}

			final BigInteger h = new BigInteger(e);
			return h.intValueExact();
		} catch (NoSuchAlgorithmException | ArithmeticException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final String toString() {
		return String.format("AbstractTopic[name=%s, subCount=%d]", name, subscribers.size());
	}

	@Override
	public int hashCode() {
		return AbstractTopic.hashForTopic(name);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof AbstractTopic)) {
			return false;
		}
		final AbstractTopic other = (AbstractTopic) obj;
		return Objects.equals(name,
				other.name); // same name == same Topic, can't have duplicate names
	}

	private static final class SimpleTopic extends AbstractTopic {

		private static final Packet[] ZERO_LENGTH_PACKET_ARRAY = new Packet[0];

		private final List<Post> posts = new LinkedList<>();

		private SimpleTopic(String name) {
			super(name);
		}

		private PostInfo currPI = null;
		private final List<Packet> currPackets = new LinkedList<>();

		@Override
		public long getLastPostId() {
			return posts.get(posts.size() - 1).getPostInfo().getId();
		}

		@Override
		public void postHook(PostInfo postInfo) {
			if (!currPackets.isEmpty()) {
				throw new IllegalStateException("Received PostInfo while more Packets remain");
			}

			currPI = postInfo;
		}

		@Override
		public void postHook(Packet packet) {
			currPackets.add(packet);

			if (packet.isFinal()) {
				final Packet[] data = currPackets.toArray(SimpleTopic.ZERO_LENGTH_PACKET_ARRAY);
				final Post completedPost = Post.fromPackets(data, currPI);
				posts.add(completedPost);

				currPackets.clear();
			}
		}

		@Override
		public Iterator<Post> iterator() {
			return posts.iterator();
		}

		@Override
		public int hashCode() {
			// hash only by name, like superclass
			return super.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!super.equals(obj)) {
				return false;
			}
			return (obj instanceof SimpleTopic);
		}
	}

	/**
	 * Encapsulates a Token that identifies the last Post in a Topic. It is used when sending new
	 * Topic so that only the necessary data is sent between the server and the client.
	 *
	 * @author Alex Mandelias
	 * @author Dimitris Tsirmpas
	 */
	public static final class TopicToken implements Serializable {

		private static final long serialVersionUID = 1L;

		private final String topicName;
		private final long lastId;

		private TopicToken(AbstractTopic abstractTopic) {
			topicName = abstractTopic.getName();
			lastId = abstractTopic.getLastPostId();
		}

		/**
		 * Returns this TopicToken's topicName.
		 *
		 * @return the topicName
		 */
		public String getName() {
			return topicName;
		}

		/**
		 * Returns this TopicToken's lastId.
		 *
		 * @return the lastId
		 */
		public long getLastId() {
			return lastId;
		}
	}
}
