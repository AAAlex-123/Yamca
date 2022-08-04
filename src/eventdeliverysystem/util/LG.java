package eventdeliverysystem.util;

import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A logger that prints output and debug messages to standard out.
 *
 * @author Alex Mandelias
 */
public final class LG {

	/**
	 * Sets the Logger's Output stream. All calls other than `LG.err` use this stream. This method
	 * must be called before attempting to log anything, or a {@code NullPointerException} will be
	 * thrown.
	 *
	 * @param out the output stream to use
	 *
	 * @throws NullPointerException if {@code out == null}
	 */
	public static void setOut(PrintStream out) {
		if (out == null) {
			throw new NullPointerException("Provided output stream cannot be null");
		}

		LG.out = out;
	}

	/**
	 * Sets the Logger's Error stream. All calls to `LG.err` use this stream. This method must be
	 * called before attempting to log anything, or a {@code NullPointerException} will be thrown.
	 *
	 * @param err the error stream to use
	 *
	 * @throws NullPointerException if {@code err == null}
	 */
	public static void setErr(PrintStream err) {
		if (err == null) {
			throw new NullPointerException("Provided error stream cannot be null");
		}

		LG.err = err;
	}

	/**
	 * Prints {@code String.format(format + "\n", args)} to {@code System.out} ignoring the current
	 * indentation level
	 *
	 * @param format A format string
	 * @param args Arguments referenced by the format specifiers in the format string.
	 *
	 * @throws NullPointerException if `setOut` has not been called prior to this call
	 */
	public static void header(String format, Object... args) {
		toOut(String.format("%s%n", format), args);
	}

	/**
	 * Prints {@code String.format(format + "\n", args)} to {@code System.out} according to the
	 * current indentation level
	 *
	 * @param format A format string
	 * @param args Arguments referenced by the format specifiers in the format string.
	 *
	 * @throws NullPointerException if `setOut` has not been called prior to this call
	 */
	public static void sout(String format, Object... args) {
		for (int i = 0; i < LG.tab; i++) {
			toOut("\t");
		}

		toOut(String.format("%s%n", format), args);
	}

	/**
	 * Prints {@code String.format("ERROR: " + format + "\n", args)} to {@code System.err} ignoring
	 * the current indentation level
	 *
	 * @param format A format string
	 * @param args Arguments referenced by the format specifiers in the format string.
	 *
	 * @throws NullPointerException if `setErr` has not been called prior to this call
	 */
	public static void err(String format, Object... args) {
		toErr(String.format("ERROR: %s%n", format), args);
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
	 *
	 * @throws NullPointerException if `setOut` has not been called prior to this call
	 */
	public static void args(String... args) {
		LG.sout("Arg count: %d", args.length);
		for (int i = 0; i < args.length; i++) {
			LG.sout("Arg %5d: %s", i, args[i]);
		}
	}

	/**
	 * Pretty-prints a Socket with a description.
	 *
	 * @param description the description of the Socket
	 * @param socket the Socket
	 *
	 * @throws NullPointerException if `setOut` has not been called prior to this call
	 */
	public static void socket(String description, Socket socket) {
		LG.sout("%s IP   - %s%n%s Port - %d", description, socket.getInetAddress(), description,
				socket.getLocalPort());
	}

	/**
	 * Pretty-prints a Server Socket with a description.
	 *
	 * @param description the description of the Server Socket
	 * @param serverSocket the Server Socket
	 *
	 * @throws NullPointerException if `setOut` has not been called prior to this call
	 */
	public static void socket(String description, ServerSocket serverSocket) {
		LG.sout("%s IP   - %s%n%s Port - %d", description, serverSocket.getInetAddress(),
				description, serverSocket.getLocalPort());
	}

	private static void toOut(String format, Object... args) {
		if (out == null) {
			throw new NullPointerException("`out` stream has not been set. Please call `setOut`");
		}

		out.printf(format, args);
		out.flush();
	}

	private static void toErr(String format, Object... args) {
		if (err == null) {
			throw new NullPointerException("`err` stream has not been set. Please call `setErr`");
		}

		err.printf(format, args);
		err.flush();
	}

	private static PrintStream out = null;
	private static PrintStream err = null;
	private static int tab = 0;

	private LG() {}
}
