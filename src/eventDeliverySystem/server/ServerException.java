package eventDeliverySystem.server;

import java.io.IOException;

/**
 * Defines an IOException subclass tailored to connections to a server.
 *
 * @author Alex Mandelias
 */
public class ServerException extends IOException {

	/**
	 * Constructs a ServerExcpetion which is associated with any server.
	 *
	 * @param cause the underlying IOException
	 */
	public ServerException(IOException cause) {
		super("Fatal error: connection to server lost", cause);
	}

	/**
	 * Constructs a ServerExcpetion which is associated with the server for a Topic.
	 *
	 * @param topicName the name of the Topic
	 * @param cause     the underlying IOException
	 */
	public ServerException(String topicName, IOException cause) {
		super("Fatal error: can't connect to server for topic " + topicName, cause);
	}
}
