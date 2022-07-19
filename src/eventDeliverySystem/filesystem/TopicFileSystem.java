package eventDeliverySystem.filesystem;

import eventDeliverySystem.datastructures.AbstractTopic;
import eventDeliverySystem.datastructures.ITopicDAO;
import eventDeliverySystem.datastructures.Post;
import eventDeliverySystem.datastructures.PostInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;


/**
 * Manages AbstractTopics that are saved in directories in the machine's file system.
 *
 * @author Alex Mandelias
 */
public class TopicFileSystem implements ITopicDAO {

	private static final Pattern PATTERN = Pattern
	        .compile("(?<postId>-?\\d+)-(?<posterName>\\w+)\\.(?<extension>.*)");
	private static final String  FORMAT  = "%d-%s.%s";

	private static final String HEAD                 = "HEAD";
	private static final String TOPIC_META_EXTENSION = ".meta";

	private final Path topicsRootDirectory;

	/**
	 * Constructs a new Topic File System for a given root directory.
	 *
	 * @param topicsRootDirectory the root directory of the new file system whose subdirectories
	 *                            correspond to different Topics
	 *
	 * @throws FileSystemException if the path given does not correspond to an existing directory
	 */
	public TopicFileSystem(Path topicsRootDirectory) throws FileSystemException {
		if (!Files.exists(topicsRootDirectory)) {
			throw new FileSystemException(
					topicsRootDirectory, new IOException("Directory does not exist"));
		}

		this.topicsRootDirectory = topicsRootDirectory;
	}

	@Override
	public void createTopic(String topicName) throws FileSystemException {
		final Path topicDirectory = resolveRoot(topicName);
		try {
			Files.createDirectory(topicDirectory);
		} catch (IOException e) {
			throw new FileSystemException(topicDirectory, e);
		}

		final Path head = getHead(topicName);
		TopicFileSystem.create(head);
	}

	@Override
	public void deleteTopic(String topicName) throws FileSystemException {
		final Path topicDirectory = resolveRoot(topicName);

		Path currentPath = topicDirectory;
		try (Stream<Path> directoryStream = Files.list(currentPath)) {
			for (Iterator<Path> iter = directoryStream.iterator(); iter.hasNext();) {
				currentPath = iter.next();
				Files.delete(currentPath);
			}

			Files.delete(topicDirectory);
		} catch (IOException e) {
			throw new FileSystemException(currentPath, e);
		}
	}

	@Override
	public void writePost(Post post, String topicName) throws FileSystemException {
		final Path fileForPost = writePost0(post, topicName);
		writePointerForPost(post, topicName);
		updateHeadForPost(fileForPost, topicName);
	}

	@Override
	public Collection<AbstractTopic> readAllTopics() throws FileSystemException {
		final Set<AbstractTopic> topics = new HashSet<>();

		for (Iterator<String> iter = getTopicNames().iterator(); iter.hasNext();)
			topics.add(readTopic(iter.next()));

		return topics;
	}

	// ==================== HELPERS FOR PATH ====================

	private Path resolveRoot(String topicName) {
		return TopicFileSystem.resolve(topicsRootDirectory, topicName);
	}

	private static Path resolve(Path directory, String filename) {
		return directory.resolve(filename);
	}

	// ==================== HELPERS FOR SAVE POST ====================

	private Path writePost0(Post post, String topicName) throws FileSystemException {
		final String fileName = TopicFileSystem.getFileNameFromPostInfo(post.getPostInfo());

		final Path topicDirectory = resolveRoot(topicName);
		final Path pathForPost    = TopicFileSystem.resolve(topicDirectory, fileName);

		TopicFileSystem.create(pathForPost);

		final byte[] data = post.getData();
		TopicFileSystem.write(pathForPost, data);

		return pathForPost;
	}

	private void writePointerForPost(Post post, String topicName) throws FileSystemException {
		final String fileName = TopicFileSystem.getFileNameFromPostInfo(post.getPostInfo());

		final Path   topicDirectory    = resolveRoot(topicName);
		final String metaFileName      = fileName + TopicFileSystem.TOPIC_META_EXTENSION;
		final Path   pointerToNextPost = TopicFileSystem.resolve(topicDirectory,
		        metaFileName);
		TopicFileSystem.create(pointerToNextPost);

		final Path   head         = getHead(topicName);
		final byte[] headContents = TopicFileSystem.read(head);
		TopicFileSystem.write(pointerToNextPost, headContents);
	}

	private void updateHeadForPost(Path fileForPost, String topicName) throws FileSystemException {
		final Path   head            = getHead(topicName);
		final byte[] newHeadContents = fileForPost.getFileName().toString().getBytes();
		TopicFileSystem.write(head, newHeadContents);
	}

	private Path getHead(String topicName) {
		final Path topicDirectory = resolveRoot(topicName);
		return TopicFileSystem.resolve(topicDirectory, TopicFileSystem.HEAD);
	}

	// ==================== HELPERS FOR LOAD POSTS FOR TOPIC ====================

	private Stream<String> getTopicNames() throws FileSystemException {
		try {
			return Files.list(topicsRootDirectory)
						.filter(Files::isDirectory)
						.map(path -> path.getFileName().toString());
		} catch (IOException e) {
			throw new FileSystemException(topicsRootDirectory, e);
		}
	}

	private AbstractTopic readTopic(String topicName) throws FileSystemException {
		final List<Post> loadedPosts = new LinkedList<>();

		final Path latestPost = getFirstPost(topicName); // from latest to earliest
		for (Path postFile = latestPost; postFile != null; postFile = getNextFile(postFile,
																				 topicName)) {
			final String   filename   = postFile.getFileName().toString();
			final PostInfo postInfo   = TopicFileSystem.getPostInfoFromFileName(filename);
			final Post     loadedPost = TopicFileSystem.readPost(postInfo, postFile);
			loadedPosts.add(loadedPost);
		}

		Collections.reverse(loadedPosts); // from earliest to latest

		return AbstractTopic.createSimple(topicName, loadedPosts);
	}

	// returns null if topic has no posts
	private Path getFirstPost(String topicName) throws FileSystemException {
		final Path   head         = getHead(topicName);
		final byte[] headContents = TopicFileSystem.read(head);

		if (headContents.length == 0)
			return null;

		final Path   topicDirectory = resolveRoot(topicName);
		final String firstPostFile  = new String(headContents);
		return TopicFileSystem.resolve(topicDirectory, firstPostFile);
	}

	// returns null if there is no next post
	private Path getNextFile(Path postFile, String topicName) throws FileSystemException {
		final Path pointerToNextPost = new File(
				postFile.toString() + TopicFileSystem.TOPIC_META_EXTENSION).toPath();

		final byte[] pointerToNextPostContents = TopicFileSystem.read(pointerToNextPost);

		if (pointerToNextPostContents.length == 0)
			return null;

		final Path   topicDirectory = resolveRoot(topicName);
		final String fileName       = new String(pointerToNextPostContents);
		return TopicFileSystem.resolve(topicDirectory, fileName);
	}

	private static Post readPost(PostInfo postInfo, Path postFile) throws FileSystemException {
		final byte[] data = TopicFileSystem.read(postFile);
		return new Post(data, postInfo);
	}

	// ==================== READ/WRITE ====================

	private static void create(Path pathForPost) throws FileSystemException {
		try {
		Files.createFile(pathForPost);
		} catch (IOException e) {
			throw new FileSystemException(pathForPost, e);
		}
	}

	private static byte[] read(Path head) throws FileSystemException {
		try {
			return Files.readAllBytes(head);
		} catch (IOException e) {
			throw new FileSystemException(head, e);
		}
	}

	private static void write(Path pointerToNextPost, byte[] data) throws FileSystemException {
		try {
			Files.write(pointerToNextPost, data);
		} catch (IOException e) {
			throw new FileSystemException(pointerToNextPost, e);
		}
	}

	// ==================== POST INFO ====================

	private static String getFileNameFromPostInfo(PostInfo postInfo) {
		final long   postId        = postInfo.getId();
		final String posterId      = postInfo.getPosterName();
		final String fileExtension = postInfo.getFileExtension();

		return String.format(TopicFileSystem.FORMAT, postId, posterId, fileExtension);
	}

	private static PostInfo getPostInfoFromFileName(String fileName) {
		final Matcher m = TopicFileSystem.PATTERN.matcher(fileName);

		if (!m.matches())
			throw new IllegalArgumentException("Bad filename: " + fileName);

		final long   postId        = Long.parseLong(m.group("postId"));
		final String posterId      = m.group("posterName");
		final String fileExtension = m.group("extension");

		return new PostInfo(posterId, fileExtension, postId);
	}
}
