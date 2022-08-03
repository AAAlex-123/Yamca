package eventdeliverysystem.datastructures;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * An immutable pair of <IPAddress, Port> representing a unique connection in the web.
 *
 * @author Dimitris Tsirmpas
 */
public final class ConnectionInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private final InetAddress address;
	private final int port;

	/**
	 * Creates a new ConnectionInfo representing the given Server Socket.
	 *
	 * @param connection the Server Socket whose details will be used.
	 *
	 * @return the ConnectionInfo
	 */
	public static ConnectionInfo forServerSocket(ServerSocket connection) {
		return new ConnectionInfo(connection.getInetAddress(), connection.getLocalPort());
	}

	/**
	 * Creates a new ConnectionInfo representing the given Socket.
	 *
	 * @param connection the Socket whose details will be used.
	 *
	 * @return the ConnectionInfo
	 */
	public static ConnectionInfo forSocket(Socket connection) {
		return new ConnectionInfo(connection.getInetAddress(), connection.getLocalPort());
	}

	private ConnectionInfo(InetAddress address, int port) {
		this.address = address;
		this.port = port;
	}

	/**
	 * Returns this ConnectionInfo's address.
	 *
	 * @return the address
	 */
	public InetAddress getAddress() {
		return address;
	}

	/**
	 * Returns this ConnectionInfo's port.
	 *
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return String.format("ConnectionInfo [address=%s, port=%d]", address, port);
	}
}
