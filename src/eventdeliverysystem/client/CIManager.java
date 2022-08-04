package eventdeliverysystem.client;

import static eventdeliverysystem.datastructures.Message.MessageType.BROKER_DISCOVERY;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import eventdeliverysystem.datastructures.ConnectionInfo;
import eventdeliverysystem.datastructures.Message;

/**
 * Wrapper for a cache that communicates with the default Broker to obtain and store the
 * ConnectionInfo objects for many Topics.
 *
 * @author Alex Mandelias
 */
final class CIManager {

	private final Map<String, ConnectionInfo> cache = new HashMap<>();

	private final InetAddress defaultBrokerIP;
	private final int defaultBrokerPort;

	/**
	 * Constructs the CIManager given the ConnectionInfo to the default Broker.
	 *
	 * @param defaultBrokerIP the InetAddress of the default Broker to connect to
	 * @param defaultBrokerPort the Port of the default Broker to connect to
	 */
	CIManager(InetAddress defaultBrokerIP, int defaultBrokerPort) {
		this.defaultBrokerIP = defaultBrokerIP;
		this.defaultBrokerPort = defaultBrokerPort;
	}

	/**
	 * Communicates with the default Broker to fetch the ConnectionInfo associated with a Topic,
	 * which is then cached. Future requests for it will use the ConnectionInfo found in the cache.
	 *
	 * @param topicName the Topic for which to get the ConnectionInfo
	 *
	 * @return the ConnectionInfo for that Topic
	 *
	 * @throws ServerException if a connection to the server fails
	 */
	ConnectionInfo getConnectionInfoForTopic(String topicName) throws ServerException {
		final ConnectionInfo cachedAddress = cache.get(topicName);
		if (cachedAddress != null) {
			return cachedAddress;
		}

		final ConnectionInfo newAddress = getCIForTopic(topicName);
		cache.put(topicName, newAddress);
		return newAddress;
	}

	private ConnectionInfo getCIForTopic(String topicName) throws ServerException {
		try (Socket socket = new Socket(defaultBrokerIP, defaultBrokerPort)) {

			final ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(new Message(BROKER_DISCOVERY, topicName));
			oos.flush();
			final ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

			return (ConnectionInfo) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (final IOException e) {
			throw new ServerException("Connection to main server failed", e);
		}
	}
}
