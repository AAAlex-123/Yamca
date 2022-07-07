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

import eventDeliverySystem.util.Subscriber;

/**
 * Abstract superclass of all Topics.
 *
 * @author Alex Mandelias
 */
public abstract class AbstractTopic implements Iterable<Post> {

	/** Constant to be used when no post exists and an ID is needed */
	public static final long FETCH_ALL_POSTS = -1L;

	private final String          name;
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
}
