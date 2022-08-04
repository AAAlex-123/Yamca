package eventdeliverysystem.client;

import java.io.IOException;

/**
 * Signals that an I/O Exception related to a connection to a Server has occurred.
 *
 * @author Alex Mandelias
 */
final class ServerException extends IOException {

	/**
	 * Constructs a ServerException with the specified detail message.
	 *
	 * @param message the detail message
	 */
	ServerException(String message) {
		super(message);
	}

	/**
	 * Constructs a ServerException with the specified detail message and cause.
	 *
	 * @param message the detail message
	 * @param cause the underlying cause
	 */
	ServerException(String message, Throwable cause) {
		super(message, cause);
	}
}
