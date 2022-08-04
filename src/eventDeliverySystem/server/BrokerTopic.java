package eventDeliverySystem.server;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Packet;
import eventDeliverySystem.datastructures.PostInfo;

/**
 * An extension of the Abstract Topic that stores data as required by Brokers.
 * The Posts are stored disassembled as PostInfo and Packet objects.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
class BrokerTopic extends AbstractTopic {

	private static final PostInfo dummyPostInfo;

	static {
		dummyPostInfo = new PostInfo(null, null, AbstractTopic.FETCH_ALL_POSTS);
	}

	private final List<PostInfo>          postInfoList;
	private final Map<Long, List<Packet>> packetsPerPostInfoMap;
	private final Map<Long, Integer>      indexPerPostInfoId;

	/**
	 * Constructs an empty BrokerTopic.
	 *
	 * @param name the name of the new BrokerTopic
	 */
	public BrokerTopic(String name) {
		super(name);
		postInfoList = new LinkedList<>();
		packetsPerPostInfoMap = new HashMap<>();
		indexPerPostInfoId = new HashMap<>();

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
	 * Fills the List and the Map with all of the PostInfo and Packet objects in
	 * this Topic.
	 *
	 * @param emptyPostInfoList          the empty list where the PostInfo objects
	 *                                   will be added, sorted from earliest to
	 *                                   latest
	 * @param emptyPacketsPerPostInfoMap the empty map where the Packets of every
	 *                                   PostInfo object will be added
	 */
	public void getAllPosts(List<PostInfo> emptyPostInfoList,
	        Map<Long, Packet[]> emptyPacketsPerPostInfoMap) {
		getPostsSince(AbstractTopic.FETCH_ALL_POSTS, emptyPostInfoList, emptyPacketsPerPostInfoMap);
	}

	/**
	 * Fills the List and the Map with all of the PostInfo and Packet objects in
	 * this Topic starting from a certain PostInfo object. The PostInfo with the
	 * given ID and its Packets are not returned.
	 *
	 * @param postId                     the ID of the PostInfo
	 * @param emptyPostInfoList          the empty list where the PostInfo objects
	 *                                   will be added, sorted from earliest to
	 *                                   latest
	 * @param emptyPacketsPerPostInfoMap the empty map where the Packets of every
	 *                                   PostInfo object will be added
	 */
	synchronized public void getPostsSince(long postId, List<PostInfo> emptyPostInfoList,
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
}
