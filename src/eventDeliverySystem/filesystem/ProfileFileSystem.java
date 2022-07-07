package eventDeliverySystem.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.Topic;

/**
 * Manages Profiles that are saved in directories in the file system.
 *
 * @author Alex Mandelias
 */
public class ProfileFileSystem {

	private final Path                         profilesRootDirectory;
	private final Map<String, TopicFileSystem> topicFileSystemMap = new HashMap<>();

	private String currentProfileName;

	/**
	 * Creates a new Profile File System for the specified root directory.
	 *
	 * @param profilesRootDirectory the root directory of the new file system whose
	 *                              sub-directories correspond to different Profiles
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public ProfileFileSystem(Path profilesRootDirectory) throws FileSystemException {
		if (!Files.exists(profilesRootDirectory)) {
			throw new FileSystemException(
					profilesRootDirectory, new IOException("Directory does not exist"));
		}

		this.profilesRootDirectory = profilesRootDirectory;

		getProfileNames().forEach(profileName -> {
			try {
				final Path topicDirectory = getTopicsDirectory(profileName);
				final TopicFileSystem tfs = new TopicFileSystem(topicDirectory);
				topicFileSystemMap.put(profileName, tfs);
			} catch (FileSystemException e) {
				// cannot be thrown since the path is taken from running 'ls'
				throw new RuntimeException("This should never happen");
			}
		});
	}

	/**
	 * Returns the all the Profile names found in the root directory.
	 *
	 * @return a collection of all the Profile names found
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public Stream<String> getProfileNames() throws FileSystemException {
		try {
			return Files.list(profilesRootDirectory).filter(Files::isDirectory)
		        .map(path -> path.getFileName().toString());
		} catch (IOException e) {
			throw new FileSystemException(profilesRootDirectory, e);
		}
	}

	/**
	 * Creates a new, empty, Profile in this File System.
	 *
	 * @param profileName the name of the new Profile
	 *
	 * @return the new Profile
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public Profile createNewProfile(String profileName) throws FileSystemException {
		Path topicsDirectory = getTopicsDirectory(profileName);
		try {
			Files.createDirectory(topicsDirectory);
		} catch (IOException e) {
			throw new FileSystemException(topicsDirectory, e);
		}

		topicFileSystemMap.put(profileName, new TopicFileSystem(topicsDirectory));

		changeProfile(profileName);

		return new Profile(profileName);
	}

	/**
	 * Reads a Profile from this File System and returns it as a Profile object.
	 * After this method returns, this file system will operate on the new Profile.
	 *
	 * @param profileName the id of the Profile to read
	 *
	 * @return the Profile read
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public Profile loadProfile(String profileName) throws FileSystemException {
		changeProfile(profileName);

		final Profile profile = new Profile(profileName);

		final TopicFileSystem tfs = getTopicFileSystemForCurrentUser();
		for (final AbstractTopic abstractTopic : tfs.readAllTopics())
			profile.addTopic(new Topic(abstractTopic));

		return profile;
	}

	/**
	 * Creates a new Topic for the current Profile.
	 *
	 * @param topicName the name of the new Topic
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public void createTopic(String topicName) throws FileSystemException {
		getTopicFileSystemForCurrentUser().createTopic(topicName);
	}

	/**
	 * Saves a Post in the file system for the current Profile.
	 *
	 * @param post      the Post to save
	 * @param topicName the name of the Topic in which to save
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the
	 *                             file system
	 */
	public void savePost(Post post, String topicName) throws FileSystemException {
		getTopicFileSystemForCurrentUser().writePost(post, topicName);
	}

	// ==================== PRIVATE METHODS ====================

	private void changeProfile(String profileName) throws NoSuchElementException {
		if (!topicFileSystemMap.containsKey(profileName))
			throw new NoSuchElementException("Profile " + profileName + " does not exist");

		currentProfileName = profileName;
	}

	private TopicFileSystem getTopicFileSystemForCurrentUser() {
		return topicFileSystemMap.get(currentProfileName);
	}

	private Path getTopicsDirectory(String profileName) {
		return profilesRootDirectory.resolve(profileName);
	}
}
