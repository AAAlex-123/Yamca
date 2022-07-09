package eventDeliverySystem.filesystem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.Topic;

/**
 * A data structure holding information about a user and their subscribed
 * topics.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 */
public class Profile {

	private final String               name;
	private final Map<String, Topic>   topics;
	private final Map<String, Integer> unreadTopics;

	/**
	 * Creates a new, empty, Profile with the specified name.
	 *
	 * @param name the unique name of the Profile
	 */
	public Profile(String name) {
		this(name, new HashMap<>());
	}

	private Profile(String name, Map<String, Topic> topics) {
		this.name = name;
		this.topics = topics;
		unreadTopics = new HashMap<>();
	}

	/**
	 * Returns this Profile's name.
	 *
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns this Profile's topics.
	 *
	 * @return the topics
	 */
	public Set<Topic> getTopics() {
		return new HashSet<>(topics.values());
	}

	/**
	 * Adds a new, unique, Topic to this Profile.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @throws IllegalArgumentException if a Topic with the same name already exists
	 */
	public void addTopic(String topicName) {
		addTopic(new Topic(topicName));
	}

	/**
	 * Adds a new Topic to this Profile.
	 *
	 * @param topic the Topic
	 *
	 * @throws NullPointerException     if {@code topic == null}
	 * @throws IllegalArgumentException if a Topic with the same name already exists
	 */
	public void addTopic(Topic topic) {
		if (topic == null)
			throw new NullPointerException("Topic can't be null");

		final String topicName = topic.getName();

		assertTopicDoesNotExist(topicName);

		topics.put(topicName, topic);
		unreadTopics.put(topicName, 0);
	}

	/**
	 * Updates a Topic of this Profile with new Posts.
	 *
	 * @param topicName the name of the Topic to update
	 * @param posts     the new Posts to post to the Topic
	 *
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void updateTopic(String topicName, List<Post> posts) {
		assertTopicExists(topicName);

		topics.get(topicName).post(posts);
	}

	/**
	 * Removes a Topic from this Profile.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void removeTopic(String topicName) {
		assertTopicExists(topicName);

		topics.remove(topicName);
		unreadTopics.remove(topicName);
	}

	/**
	 * Marks a Topic as unread.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void markUnread(String topicName) {
		assertTopicExists(topicName);

		unreadTopics.put(topicName, unreadTopics.get(topicName) + 1);
	}

	/**
	 * Marks all posts in a Topic as read.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @throws NoSuchElementException if no Topic with the given name exists
	 */
	public void clearUnread(String topicName) {
		assertTopicExists(topicName);

		unreadTopics.put(topicName, 0);
	}

	@Override
	public String toString() {
		final List<Object> topicNames = Arrays.asList(topics.keySet().toArray());
		return String.format("Profile [name=%s, topics=%s]", name, topicNames);
	}

	private void assertTopicExists(String topicName) throws NoSuchElementException {
		if (!topics.containsKey(topicName))
			throw new NoSuchElementException("No Topic with name " + topicName + " found");
	}

	private void assertTopicDoesNotExist(String topicName) throws IllegalArgumentException {
		if (topics.containsKey(topicName))
			throw new NoSuchElementException("Topic with name " + topicName + " already exists");
	}
}
