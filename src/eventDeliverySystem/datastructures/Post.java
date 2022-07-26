package eventDeliverySystem.datastructures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * Encapsulates a Post, its data and its associated PostInfo object.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 * @see PostInfo
 */
public final class Post {

	/**
	 * Extracts the data of some Packets into a single Post.
	 *
	 * @param packets an array of packets that stores the data of a Post
	 * @param postInfo the PostInfo object associated with that Post
	 *
	 * @return the packets' contents as a Post
	 *
	 * @throws IllegalArgumentException if the packets array has length 0
	 * @throws IllegalStateException if Packets with different IDs are found in the packets array
	 * @see Packet#fromPost(Post)
	 */
	public static Post fromPackets(Packet[] packets, PostInfo postInfo) {
		if (packets.length == 0) {
			throw new IllegalArgumentException("Tried to create a Post object with no data");
		}

		final int byteCount = Stream.of(packets).mapToInt(p -> p.getPayload().length).sum();
		final byte[] data = new byte[byteCount];

		final long idOfFirst = packets[0].getPostId();

		int dataPointer = 0;
		for (final Packet curr : packets) {
			if (curr.getPostId() != idOfFirst) {
				throw new IllegalStateException(
						String.format("Tried to combine packet with ID %d with packet with ID %d",
								idOfFirst, curr.getPostId()));
			}

			final byte[] payload = curr.getPayload();
			final int length = payload.length;
			System.arraycopy(payload, 0, data, dataPointer, length);
			dataPointer += length;
		}

		return new Post(data, postInfo);
	}

	/**
	 * Constructs a Post with from a File.
	 *
	 * @param file the File whose data will be encapsulated in a Post
	 * @param posterName the name of the poster of the File
	 *
	 * @return the Post that encapsulates the contents of the File
	 *
	 * @throws FileNotFoundException if the File could not be found
	 * @throws IOException if an I/O Error occurs
	 */
	public static Post fromFile(File file, String posterName) throws IOException {

		byte[] data;
		try (FileInputStream fis = new FileInputStream(file)) {
			List<Integer> bytes = new LinkedList<>();

			int nextByte = fis.read();
			while (nextByte != -1) {
				bytes.add(nextByte);
				nextByte = fis.read();
			}

			data = new byte[bytes.size()];
			int index = 0;
			for (Integer b : bytes) {
				data[index] = b.byteValue();
				index++;
			}
		}

		final String fileName = file.getName();
		final String fileExtension = fileName.substring(fileName.lastIndexOf('.') + 1);

		return new Post(data, posterName, fileExtension);
	}

	/**
	 * Constructs a Post from a String of text, a plain-text message.
	 *
	 * @param text the message
	 * @param posterName the name of the poster of the plain-message
	 *
	 * @return the Post that encapsulates the plain-text message
	 */
	public static Post fromText(String text, String posterName) {
		return new Post(text.getBytes(StandardCharsets.UTF_8), posterName, "~txt");
	}

	private final byte[] data;
	private final PostInfo postInfo;

	/**
	 * Constructs a new Post with the specified info.
	 *
	 * @param data the contents of this post
	 * @param postInfo the PostInfo object associated with this Post
	 */
	public Post(byte[] data, PostInfo postInfo) {
		this.data = data.clone();
		this.postInfo = postInfo;
	}

	/**
	 * Constructs a new Post with a random ID.
	 *
	 * @param data the data to be encapsulated in this Post
	 * @param posterName the name of the poster of this Post
	 * @param fileExtension the file extension associated with the data of this Post. Plain-text
	 * 		messages have a file extension of '{@code ~txt}'
	 */
	private Post(byte[] data, String posterName, String fileExtension) {
		this(data, new PostInfo(posterName, fileExtension, ThreadLocalRandom.current().nextLong()));
	}

	/**
	 * Returns a clone of this Post's data.
	 *
	 * @return a clone of the data
	 */
	public byte[] getData() {
		return data.clone();
	}

	/**
	 * Returns this Post's postInfo.
	 *
	 * @return the postInfo
	 */
	public PostInfo getPostInfo() {
		return postInfo;
	}

	@Override
	public String toString() {
		return String.format("Post [data.length=%s, postInfo=%s]", data.length, postInfo);
	}
}
