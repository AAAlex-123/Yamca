package eventDeliverySystem.datastructures;

import java.io.Serializable;

/**
 * Contains information about a Post.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 *
 * @see Post
 */
public class PostInfo implements Serializable {

	private static final long serialVersionUID = 1;

	private final String posterName;
	private final String fileExtension;
	private final long   id;

	/**
	 * Constructs a new PostInfo that holds information associated with a Post.
	 *
	 * @param posterName    the unique name of the Post's poster
	 * @param fileExtension the extension of the associated Post's file,
	 *                      '{@code ~txt}' for plain-text messages
	 * @param id            the unique id of the Post
	 */
	public PostInfo(String posterName, String fileExtension, long id) {
		this.posterName = posterName;
		this.fileExtension = fileExtension;
		this.id = id;
	}

	/**
	 * Returns the name of the Poster of the Post associated with this PostInfo.
	 *
	 * @return the Poster's name.
	 */
	public String getPosterName() {
		return posterName;
	}

	/**
	 * Returns this PostInfo's fileExtension.
	 *
	 * @return the fileExtension
	 */
	public String getFileExtension() {
		return fileExtension;
	}

	/**
	 * Returns the ID of the Post associated with this PostInfo.
	 *
	 * @return the Post's id
	 */
	public long getId() {
		return id;
	}

	@Override
	public String toString() {
		return String.format("PostInfo [posterName=%s, fileExtension=%s, id=%s]", posterName,
		        fileExtension, id);
	}
}
