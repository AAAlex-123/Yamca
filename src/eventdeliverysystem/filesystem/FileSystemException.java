package eventdeliverysystem.filesystem;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Defines an IOException subclass that is associated with an I/O operation on a specific Path.
 *
 * @author Alex Mandelias
 */
public final class FileSystemException extends IOException {

	/**
	 * Constructs a FileSystemException with the specified detail message, cause and path.
	 *
	 * @param message the detail message
	 * @param cause the underlying cause
	 * @param path the path related to the Exception
	 */
	FileSystemException(String message, IOException cause, Path path) {
		super(message + ": " + path, cause);
	}
}
