package eventDeliverySystem.dao;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Post;

/**
 * Interface for a Data Access Object responsible for Profile entities.
 *
 * @author Alex Mandelias
 */
public interface IProfileDAO {

	/**
	 * Creates a new, empty, Profile in this DAO. After this method returns, this DAO shall
	 * henceforth operate on that new Profile.
	 *
	 * @param profileName the name of the new Profile
	 *
	 * @throws IOException if an I/O error occurs while creating the new Profile
	 */
	void createNewProfile(String profileName) throws IOException;

	/**
	 * Loads all Topics for a Profile from this DAO. After this method returns, this DAO shall
	 * henceforth operate on that new Profile.
	 *
	 * @param profileName the name of the Profile to read
	 *
	 * @return a Collection including all the Topics loaded from this DAO
	 *
	 * @throws IOException if an I/O error occurs while loading the Topics of the Profile
	 * @throws NoSuchElementException if no Profile with that name exists
	 */
	Collection<AbstractTopic> loadProfile(String profileName) throws IOException;

	/**
	 * Creates a new Topic for the current Profile.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @throws IOException if an I/O error occurs while creating the new Topic
	 */
	void createTopicForCurrentProfile(String topicName) throws IOException;

	/**
	 * Deletes an existing Topic from the current Profile.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @throws IOException if an I/O error occurs while deleting the Topic
	 */
	void deleteTopicFromCurrentProfile(String topicName) throws IOException;

	/**
	 * Saves a Post in this DAO for the current Profile.
	 *
	 * @param post the Post to save
	 * @param topicName the name of the Topic in which to save
	 *
	 * @throws IOException if an I/O error occurs while saving the Post
	 */
	void savePostForCurrentProfile(Post post, String topicName) throws IOException;
}
