package eventDeliverySystem.server;


import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;
import eventDeliverySystem.filesystem.ITopicDAO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An extension of the Abstract Topic that stores data as required by Brokers. The Posts are stored
 * disassembled as PostInfo and Packet objects.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
final class BrokerTopic extends AbstractTopic {

	private static final PostInfo dummyPostInfo;

	static {
		dummyPostInfo = new PostInfo(null, null, AbstractTopic.FETCH_ALL_POSTS);
	}

	private final ITopicDAO postDAO;

	private final List<PostInfo>          postInfoList = new LinkedList<>();
	private final Map<Long, List<Packet>> packetsPerPostInfoMap = new HashMap<>();
	private final Map<Long, Integer>      indexPerPostInfoId = new HashMap<>();

	/**
	 * Constructs a BrokerTopic that contains the posts of another Topic.
	 *
	 * @param abstractTopic the Topic whose Posts will be posted to this BrokerTopic
	 * @param postDAO the ITopicDAO object responsible for this BrokerTopic
	 */
	BrokerTopic(AbstractTopic abstractTopic, ITopicDAO postDAO) {
		this(abstractTopic.getName(), postDAO);
		abstractTopic.forEach(post -> {
			post(post.getPostInfo());
			for (Packet packet : Packet.fromPost(post))
				post(packet);
		});
	}

	/**
	 * Constructs an empty BrokerTopic.
	 *
	 * @param name the name of the new BrokerTopic
	 * @param postDAO the ITopicDAO object responsible for this BrokerTopic
	 */
	BrokerTopic(String name, ITopicDAO postDAO) {
		super(name);
		this.postDAO = postDAO;

		postInfoList.add(dummyPostInfo);
		indexPerPostInfoId.put(AbstractTopic.FETCH_ALL_POSTS, 0);
	}

	@Override
	public void postHook(PostInfo postInfo) {
		postInfoList.add(postInfo);

		final long         postId     = postInfo.getId();
		final List<Packet> packetList = new LinkedList<>();

		packetsPerPostInfoMap.put(postId, packetList);
		indexPerPostInfoId.put(postId, postInfoList.size() - 1);
	}

	@Override
	public void postHook(Packet packet) {
		final long postId = packet.getPostId();

		packetsPerPostInfoMap.get(postId).add(packet);
	}

	/**
	 * Adds to the given List and the Map all the PostInfo and Packet objects in this Topic.
	 *
	 * @param emptyPostInfoList          the empty list where the PostInfo objects will be added,
	 *                                   sorted from earliest to latest
	 * @param emptyPacketsPerPostInfoMap the empty map where the Packets of every PostInfo object
	 *                                   will be added
	 */
	void getAllPosts(List<PostInfo> emptyPostInfoList,
	        Map<Long, Packet[]> emptyPacketsPerPostInfoMap) {
		getPostsSince(AbstractTopic.FETCH_ALL_POSTS, emptyPostInfoList, emptyPacketsPerPostInfoMap);
	}

	/**
	 * Adds to the given List and the Map all the PostInfo and Packet objects in this Topic starting
	 * from a certain PostInfo object. The PostInfo with the given ID and its Packets are not
	 * returned.
	 *
	 * @param postId                     the ID of the PostInfo
	 * @param emptyPostInfoList          the empty list where the PostInfo objects will be added,
	 *                                   sorted from earliest to latest
	 * @param emptyPacketsPerPostInfoMap the empty map where the Packets of every PostInfo object
	 *                                   will be added
	 */
	synchronized void getPostsSince(long postId, List<PostInfo> emptyPostInfoList,
	        Map<Long, Packet[]> emptyPacketsPerPostInfoMap) {

		final Integer index = indexPerPostInfoId.get(postId);

		// broker is not persistent, consumer may have posts from previous session, not an error
		if (index == null)
			return;

		emptyPostInfoList.addAll(postInfoList.subList(index + 1, postInfoList.size()));

		for (PostInfo pi : emptyPostInfoList) {
			final long         id = pi.getId();
			final List<Packet> ls = packetsPerPostInfoMap.get(id);
			emptyPacketsPerPostInfoMap.put(id, ls.toArray(new Packet[ls.size()]));
		}
	}

	/**
	 * Saves a Post of this Topic to the ITopicDAO object of this BrokerTopic.
	 *
	 * @param postId the id of the Post to save
	 *
	 * @throws IOException if an I/O Error occurs while saving the Post.
	 */
	void savePostToTFS(long postId) throws IOException {
		PostInfo pi = postInfoList.get(indexPerPostInfoId.get(postId));
		final List<Packet> ls = packetsPerPostInfoMap.get(postId);
		Packet[] packets = ls.toArray(new Packet[ls.size()]);

		Post post = Post.fromPackets(packets, pi);
		postDAO.writePost(post, getName());
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		return (obj instanceof BrokerTopic);
	}

	@Override
	public Iterator<Post> iterator() {
		throw new UnsupportedOperationException("Not yet implemented");
	}
}
