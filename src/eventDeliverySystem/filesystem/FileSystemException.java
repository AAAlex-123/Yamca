package eventDeliverySystem.filesystem;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Defines an IOException subclass that is associated with an I/O operation on a specific Path.
 *
 * @author Alex Mandelias
 */
public class FileSystemException extends IOException {

	/**
	 * Constructs a FileSystemException which is caused by an IOException.
	 *
	 * @param path  the path related to the Exception
	 * @param cause the underlying IOException
	 */
	public FileSystemException(Path path, IOException cause) {
		super("Error while interacting with path: " + path, cause);
	}
}
