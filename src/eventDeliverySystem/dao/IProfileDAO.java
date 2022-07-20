package eventDeliverySystem.dao;

import java.io.IOException;

import eventDeliverySystem.client.Profile;
import eventDeliverySystem.datastructures.Post;

/**
 * Interface for a Data Access Object responsible for Profile entities.
 *
 * @author Alex Mandelias
 */
public interface IProfileDAO {

    /**
     * Creates a new, empty, Profile in this DAO.
     *
     * @param profileName the name of the new Profile
     *
     * @return the new Profile
     *
     * @throws IOException if an I/O error occurs while creating the new Profile
     */
    Profile createNewProfile(String profileName) throws IOException;

    /**
     * Reads a Profile from this DAO and returns it as a Profile object. After this method returns,
     * this DAO shall henceforth will operate on that new Profile.
     *
     * @param profileName the id of the Profile to read
     *
     * @return the Profile read
     *
     * @throws IOException if an I/O error occurs while loading the Profile
     */
    Profile loadProfile(String profileName) throws IOException;

    /**
     * Creates a new Topic for the current Profile.
     *
     * @param topicName the name of the new Topic
     *
     * @throws IOException if an I/O error occurs while creating the new Topic
     */
    void createTopic(String topicName) throws IOException;

    /**
     * Deletes an existing Topic from the current Profile.
     *
     * @param topicName the name of the new Topic
     *
     * @throws IOException if an I/O error occurs while deleting the Topic
     */
    void deleteTopic(String topicName) throws IOException;

    /**
     * Saves a Post in this DAO for the current Profile.
     *
     * @param post      the Post to save
     * @param topicName the name of the Topic in which to save
     *
     * @throws IOException if an I/O error occurs while saving the Post
     */
    void savePost(Post post, String topicName) throws IOException;
}
