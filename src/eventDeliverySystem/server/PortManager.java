package eventDeliverySystem.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A class that manages and distributes port numbers to the various processes in
 * this machine. Used to allow more than one client/server process to run in one
 * machine by providing them with available ports.
 *
 * @author Dimitris Tsirmpas
 */
final class PortManager {

	private static final int LOWEST_PORT  = 29170;
	private static final int HIGHEST_PORT = 29998;

	private PortManager() {}

	/**
	 * Returns a new port for this process in the range {@code 29170-29998} that is
	 * guaranteed to be available. This method is thread-safe.
	 *
	 * @return a new available port
	 */
	static synchronized int getNewAvailablePort() {
		final ThreadLocalRandom tlr = ThreadLocalRandom.current();
		int port;
		do
			port = tlr.nextInt(PortManager.LOWEST_PORT, PortManager.HIGHEST_PORT);
		while (!PortManager.isAvailable(port));

		return port;
	}

	/**
	 * Checks whether a port is available.
	 *
	 * @param port the port to be checked
	 *
	 * @return {@code true} if the port is available, {@code false} otherwise
	 *
	 * @implNote the method attempts to create a connection through the port and
	 *           relies on exception handling to determine if it's already binded.
	 *
	 * @see <a href=
	 *      "https://stackoverflow.com/questions/434718/sockets-discover-port-availability-using-java">stack
	 *      overflow</a>
	 */
	private static boolean isAvailable(int port) {
		try (ServerSocket testSocket = new ServerSocket(port)) {
			testSocket.setReuseAddress(true);
			return true;
		} catch (final IOException e) {
			return false;
		}
	}
}
