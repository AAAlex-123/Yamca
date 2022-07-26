package eventDeliverySystem.datastructures;

import java.io.Serializable;

/**
 * Represents a fragment of a Post. Each Packet contains enough information so that the original
 * Post can be restored given the array of Packets.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public final class Packet implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final int PACKET_SIZE = (int) Math.floor(512.0 * StrictMath.pow(2.0, 10.0));

	/**
	 * Breaks a Post into an array of Packets.
	 *
	 * @param post the Post
	 *
	 * @return an array of Packets which collectively stores the original Post. Every Packet has the
	 * 		same id as the Post and invoking {@link #isFinal()} on the last one returns {@code true}.
	 *
	 * @see Post#fromPackets(Packet[], PostInfo)
	 */
	public static Packet[] fromPost(Post post) {
		final byte[] src = post.getData();
		final long id = post.getPostInfo().getId();

		final int packetCount = (int) Math.ceil((double) src.length / Packet.PACKET_SIZE);
		final Packet[] packets = new Packet[packetCount];

		int srcPointer = 0;
		for (int i = 0; i < packetCount; i++) {
			final boolean isFinal = i == (packetCount - 1);

			final int length = Math.min(Packet.PACKET_SIZE, src.length - srcPointer);
			final byte[] payload = new byte[length];

			System.arraycopy(src, srcPointer, payload, 0, length);
			srcPointer += length;

			packets[i] = new Packet(isFinal, payload, id);
		}

		return packets;
	}

	private final boolean isFinal;
	private final byte[] payload;
	private final long postId;

	private Packet(boolean isFinal, byte[] payload, long postId) {
		this.isFinal = isFinal;
		this.payload = payload;
		this.postId = postId;
	}

	/**
	 * Returns whether there are more remaining packets for the associated Post.
	 *
	 * @return {@code true} if this is the last packet for the Post, {@code false} otherwise
	 */
	public boolean isFinal() {
		return isFinal;
	}

	/**
	 * Returns this Packet's payload.
	 *
	 * @return the payload
	 */
	public byte[] getPayload() {
		return payload.clone();
	}

	/**
	 * Returns the id of the Post this Packet is a part of.
	 *
	 * @return the id of its associated Post
	 */
	public long getPostId() {
		return postId;
	}

	@Override
	public String toString() {
		return String.format("Packet [isFinal=%s, payload.length=%s, postId=%s]", isFinal,
				payload.length, postId);
	}
}
