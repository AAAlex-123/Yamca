package eventDeliverySystem.dao;

import java.io.IOException;
import java.util.Collection;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Post;

/**
 * Interface for a Data Access Object responsible for Topic entities.
 *
 * @author Alex Mandelias
 */
public interface ITopicDAO {

	/**
	 * Creates a new empty Topic in the file system.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @throws IOException if a topic with that name already exists in this DAO object
	 */
	void createTopic(String topicName) throws IOException;

	/**
	 * Deletes an {@link AbstractTopic} from the local File System. This operation is not atomic,
	 * meaning that if an Exception is thrown the local File System may still contain some of the
	 * Topic's files, leaving it in an ambiguous state.
	 *
	 * @param topicName the name of the Topic
	 *
	 * @throws IOException if an I/O error occurs while interacting with this DAO object
	 */
	void deleteTopic(String topicName) throws IOException;

	/**
	 * Adds a new {@link Post} to an existing {@link AbstractTopic}.
	 *
	 * @param post the new Post
	 * @param topicName the topic's name
	 *
	 * @throws IOException if an I/O error occurs while interacting with this DAO object
	 */
	void writePost(Post post, String topicName) throws IOException;

	/**
	 * Reads all Topics from the File System and returns them.
	 *
	 * @return a Collection including all the Posts loaded from the File System
	 *
	 * @throws IOException if an I/O error occurs while interacting with this DAO object
	 */
	Collection<AbstractTopic> readAllTopics() throws IOException;
}
