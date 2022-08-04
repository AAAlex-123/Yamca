package eventDeliverySystem.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

import eventDeliverySystem.server.Broker;

/**
 * A superclass for all client-side Nodes that connect to and send / receive
 * data from a remote server.
 *
 * @author Alex Mandelias
 * @author Dimitris Tsirmpas
 *
 * @see Broker
 */
abstract class ClientNode {

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
	 */
	protected ClientNode(String serverIP, int serverPort) throws UnknownHostException {
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
	 */
	protected ClientNode(byte[] serverIP, int serverPort) throws UnknownHostException {
		this(InetAddress.getByAddress(serverIP), serverPort);
	}

	/**
	 * Constructs a Client Node that will connect to a specific default broker.
	 *
	 * @param ip   the InetAddress of the default broker
	 * @param port the port of the default broker
	 */
	protected ClientNode(InetAddress ip, int port) {
		topicCIManager = new CIManager(ip, port);
	}
}
