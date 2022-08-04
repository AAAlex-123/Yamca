package eventdeliverysystem.client;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import eventdeliverysystem.datastructures.AbstractTopic;
import eventdeliverysystem.datastructures.Packet;
import eventdeliverysystem.datastructures.Post;
import eventdeliverysystem.datastructures.PostInfo;
import eventdeliverysystem.util.LG;

/**
 * An extension of the Abstract Topic that stores data as required by Users. The Posts are stored
 * as-is, as Post objects.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
final class UserTopic extends AbstractTopic {

	private static final Packet[] ZERO_LENGTH_PACKET_ARRAY = new Packet[0];
	private static final Post dummyPost;

	static {
		PostInfo dummyPI = new PostInfo(null, null, AbstractTopic.FETCH_ALL_POSTS);
		dummyPost = new Post(new byte[0], dummyPI);
	}

	// first element is the first post added
	private final List<Post> postList = new LinkedList<>();
	private final Map<Long, Integer> indexPerPostId = new HashMap<>();

	/**
	 * Constructs a Topic that contains the posts of another Topic.
	 *
	 * @param abstractTopic the Topic whose Posts will be posted to this Topic
	 */
	UserTopic(AbstractTopic abstractTopic) {
		this(abstractTopic.getName());
		for (Post post : abstractTopic) {
			post(post);
		}
	}

	/**
	 * Creates a new, empty, Topic.
	 *
	 * @param name the Topic's unique name
	 */
	UserTopic(String name) {
		super(name);
		post(UserTopic.dummyPost);
	}

	@Override
	public long getLastPostId() {
		return postList.get(postList.size() - 1).getPostInfo().getId();
	}

	private final List<Packet> currPackets = new LinkedList<>();
	private PostInfo currPI = null;

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
			final Packet[] data = currPackets.toArray(UserTopic.ZERO_LENGTH_PACKET_ARRAY);
			final Post completedPost = Post.fromPackets(data, currPI);
			post(completedPost);

			currPackets.clear();
		}
	}

	/**
	 * Adds a list of Posts to this Topic.
	 *
	 * @param posts the Posts
	 */
	void postAll(List<Post> posts) {
		for (final Post post : posts) {
			post(post);
		}
	}

	private void post(Post post) {
		postList.add(post);
		indexPerPostId.put(post.getPostInfo().getId(), postList.size() - 1);
	}

	/** Clears this Topic by removing all Posts */
	void clear() {
		postList.clear();
		indexPerPostId.clear();
		post(UserTopic.dummyPost);
	}

	/**
	 * Returns the Posts in this Topic that were posted after the Post with the given ID. The Post
	 * with the given ID is not returned.
	 *
	 * @param lastPostId the ID of the Post.
	 *
	 * @return the Posts in this Topic that were posted after the Post with the given ID, sorted
	 * 		from earliest to latest
	 *
	 * @throws NoSuchElementException if no Post in this Topic has the given ID
	 */
	List<Post> getPostsSince(long lastPostId) throws NoSuchElementException {
		LG.sout("Topic#getPostsSince(%d)", lastPostId);
		LG.in();

		final Integer index = indexPerPostId.get(lastPostId);
		if (index == null) {
			throw new NoSuchElementException(
					"No post with id " + lastPostId + " found in this Topic");
		}

		final List<Post> postsAfterGivenPost =
				new LinkedList<>(postList.subList(index + 1, postList.size()));
		LG.sout("postsAfterGivenPost=%s", postsAfterGivenPost);
		LG.out();
		return postsAfterGivenPost;
	}

	/**
	 * Returns all Posts in this Topic.
	 *
	 * @return the Posts in this Topic, sorted from earliest to latest
	 */
	List<Post> getAllPosts() {
		return getPostsSince(AbstractTopic.FETCH_ALL_POSTS);
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
		return (obj instanceof UserTopic);
	}

	@Override
	public Iterator<Post> iterator() {
		return postList.subList(1, postList.size()).iterator();
	}
}
