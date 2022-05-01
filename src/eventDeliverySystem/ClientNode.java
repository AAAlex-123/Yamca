package eventDeliverySystem;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A superclass for all client-side Nodes that connect to and send / receive
 * data from a remote server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 *
 * @see Broker
 */
abstract class ClientNode implements AutoCloseable {

	/**
	 * This Client Node's Connection Info Manager that manages the information about
	 * this Node's connections to brokers.
	 *
	 * @see CIManager
	 */
	protected final CIManager topicCIManager;

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByName(String)}.
	 * @param serverPort the port of the default broker
	 *
	 * @throws UnknownHostException if no IP address for the host could be found, or
	 *                              if a scope_id was specified for a global IPv6
	 *                              address while resolving the defaultServerIP.
	 * @throws IOException          if an I/O error occurs while establishing
	 *                              connection to the server
	 */
	public ClientNode(String serverIP, int serverPort) throws IOException {
		this(InetAddress.getByName(serverIP), serverPort);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param serverIP   the IP of the default broker, interpreted by
	 *                   {@link InetAddress#getByAddress(byte[])}.
	 * @param serverPort the port of the default broker
	 *
	 * @throws UnknownHostException if IP address is of illegal length
	 * @throws IOException          if an I/O error occurs while establishing
	 *                              connection to the server
	 */
	public ClientNode(byte[] serverIP, int serverPort) throws IOException {
		this(InetAddress.getByAddress(serverIP), serverPort);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param ip   the InetAddress of the default broker
	 * @param port the port of the default broker
	 *
	 * @throws IOException if an I/O error occurs while establishing connection to
	 *                     the server
	 */
	protected ClientNode(InetAddress ip, int port) throws IOException {
		topicCIManager = new CIManager(new ConnectionInfo(ip, port));
	}

	@Override
	public final void close() throws IOException {
		topicCIManager.close();
		closeImpl();
	}

	/**
	 * Allows each subclass to optionally specify how to close itself. The default
	 * implementation does nothing.
	 *
	 * @throws IOException if an IOException occurs while closing this resource
	 */
	protected void closeImpl() throws IOException {}
}
