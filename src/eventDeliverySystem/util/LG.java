package eventDeliverySystem.util;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * A logger that prints output and debug messages to standard out.
 *
 * @author Alex Mandelias
 */
public class LG {

	private static int tab = 0;

	/**
	 * Prints {@code String.format(format + "\n", args)} according to the current
	 * indentation level
	 *
	 * @param format A format string
	 * @param args   Arguments referenced by the format specifiers in the format
	 *               string.
	 */
	public static void sout(String format, Object... args) {
		for (int i = 0; i < LG.tab; i++)
			System.out.print("\t");

		System.out.printf(format + "\n", args);
		System.out.flush();
	}

	/** Adds a level of indentation to all future prints */
	public static void in() {
		LG.tab++;
	}

	/** Removes a level of indentation from all future prints */
	public static void out() {
		LG.tab--;
	}

	/**
	 * Pretty-prints the {@code args} parameter of a {@code main} method.
	 *
	 * @param args the args parameter of a {@code main} method
	 */
	public static void args(String... args) {
		LG.sout("Arg count: %d", args.length);
		for (int i = 0; i < args.length; i++)
			LG.sout("Arg %5d: %s", i, args[i]);
	}

	/**
	 * Prints a Socket with a header.
	 *
	 * @param header the header of the output
	 * @param socket the socket
	 */
	public static void ssocket(String header, Socket socket) {
		LG.sout("%s:%n\t%s", header, socket);
	}

	/**
	 * Prints a Server Socket with a header.
	 *
	 * @param header       the header of the output
	 * @param serverSocket the Server Socket
	 */
	public static void ssocket(String header, ServerSocket serverSocket) {
		LG.sout("%s:%n%s", header, serverSocket);
	}

	/**
	 * Pretty-prints a Socket with a description.
	 *
	 * @param description the description of the Socket
	 * @param socket      the Socket
	 */
	public static void socket(String description, Socket socket) {
		LG.sout("%s IP   - %s%n%s Port - %d", description, socket.getInetAddress(), description,
		        socket.getLocalPort());
	}

	/**
	 * Pretty-prints a Server Socket with a description.
	 *
	 * @param description  the description of the Server Socket
	 * @param serverSocket the Server Socket
	 */
	public static void socket(String description, ServerSocket serverSocket) {
		LG.sout("%s IP   - %s%n%s Port - %d", description, serverSocket.getInetAddress(),
		        description,
		        serverSocket.getLocalPort());
	}
}
