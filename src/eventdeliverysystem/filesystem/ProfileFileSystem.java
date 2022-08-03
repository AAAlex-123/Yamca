package eventdeliverysystem.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import eventdeliverysystem.dao.IProfileDAO;
import eventdeliverysystem.datastructures.AbstractTopic;
import eventdeliverysystem.datastructures.Post;

/**
 * An implementation of the {@code IProfileDAO} interface which saves Profiles in directories in the
 * machine's file system.
 *
 * @author Alex Mandelias
 */
public final class ProfileFileSystem implements IProfileDAO {

	private final Path profilesRootDirectory;
	private final Map<String, TopicFileSystem> topicFileSystemMap = new HashMap<>();

	// don't change directly, only through 'changeProfile(String)'
	private String currentProfileName = null;

	/**
	 * Creates a new Profile File System for the specified root directory.
	 *
	 * @param profilesRootDirectory the root directory of the new file system whose subdirectories
	 * 		correspond to different Profiles
	 *
	 * @throws FileSystemException if an I/O error occurs while interacting with the file system
	 */
	public ProfileFileSystem(Path profilesRootDirectory) throws FileSystemException {
		if (!Files.exists(profilesRootDirectory)) {
			throw new FileSystemException("Root directory for Users does not exist",
					new FileNotFoundException(
							"Directory " + profilesRootDirectory + " does not exist"),
					profilesRootDirectory);
		}

		this.profilesRootDirectory = profilesRootDirectory;

		getProfileNames().forEach(profileName -> {
			try {
				final Path topicDirectory = getTopicsDirectory(profileName);
				final TopicFileSystem tfs = new TopicFileSystem(topicDirectory);
				topicFileSystemMap.put(profileName, tfs);
			} catch (FileSystemException ignore) {
				// cannot be thrown since the path is, essentially, taken from running 'ls'
			}
		});
	}

	@Override
	public void createNewProfile(String profileName) throws FileSystemException {
		Path topicsDirectory = getTopicsDirectory(profileName);
		try {
			Files.createDirectory(topicsDirectory);
		} catch (IOException e) {
			throw new FileSystemException(
					"An I/O error occurred when creating Profile " + profileName, e,
					topicsDirectory);
		}

		topicFileSystemMap.put(profileName, new TopicFileSystem(topicsDirectory));

		changeProfile(profileName);
	}

	@Override
	public Collection<AbstractTopic> loadProfile(String profileName) throws FileSystemException {
		changeProfile(profileName);

		return getTopicFileSystemForCurrentProfile().readAllTopics();
	}

	@Override
	public void createTopicForCurrentProfile(String topicName) throws FileSystemException {
		getTopicFileSystemForCurrentProfile().createTopic(topicName);
	}

	@Override
	public void deleteTopicFromCurrentProfile(String topicName) throws FileSystemException {
		getTopicFileSystemForCurrentProfile().deleteTopic(topicName);
	}

	@Override
	public void savePostForCurrentProfile(Post post, String topicName) throws FileSystemException {
		getTopicFileSystemForCurrentProfile().writePost(post, topicName);
	}

	// ==================== PRIVATE METHODS ====================

	private Stream<String> getProfileNames() throws FileSystemException {
		try {
			return Files.list(profilesRootDirectory).filter(Files::isDirectory)
			            .map(path -> path.getFileName().toString());
		} catch (IOException e) {
			throw new FileSystemException("An I/O error occurred when retrieving the Profiles", e,
					profilesRootDirectory);
		}
	}

	private void changeProfile(String profileName) throws NoSuchElementException {
		if (!topicFileSystemMap.containsKey(profileName)) {
			throw new NoSuchElementException("Profile " + profileName + " does not exist");
		}

		currentProfileName = profileName;
	}

	private TopicFileSystem getTopicFileSystemForCurrentProfile() {
		return topicFileSystemMap.get(currentProfileName);
	}

	private Path getTopicsDirectory(String profileName) {
		return profilesRootDirectory.resolve(profileName);
	}
}
