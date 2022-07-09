package app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import eventDeliverySystem.util.LG;

/**
 * Runs a Client which can be configured by command-line arguments.
 *
 * @author Alex Mandelias
 */
public class Client {

	private static final String USAGE = "Usage:\n"
	        + "\t   java app.Client -c <name> <ip> <port> <user_dir>\n"
	        + "\tor java app.Client -l <name> <ip> <port> <user_dir>\n"
	        + "\n"
	        + "Options:\n"
	        + "\t-c\tcreate new user with the <name>\t\n"
	        + "\t-l\tload existing user with the <name>\n"
	        + "\n"
	        + "Where:\n"
	        + "\t<ip>\t\tthe ip of the server\n"
	        + "\t<port>\t\tthe port the server listens to (See 'Client Port' in the server console)\n"
	        + "\t<user_dir>\tthe directory in the file system to store the data";

	private Client() {}

	/**
	 * Runs a Client which can be configured by args. Run with no arguments for a
	 * help message.
	 *
	 * @param args run with no args to get information about args
	 */
	public static void main(String[] args) {
		LG.args(args);

		if (args.length != 5) {
			System.out.println(Client.USAGE);
			return;
		}

		final String type = args[0];
		final String name = args[1];

		boolean existing = type.equals("-l");
		switch (type) {
		case "-c":
		case "-l":
			break;
		default:
			System.out.println(Client.USAGE);
			return;
		}

		final String ip = args[2];
		int          port;
		try {
			port = Integer.parseInt(args[3]);
		} catch (final NumberFormatException e) {
			System.err.printf("Invalid port: %s", args[3]);
			return;
		}

		final Path dir = new File(args[4]).toPath();

		CrappyUserUI ui;
		try {
			ui = new CrappyUserUI(existing, name, ip, port, dir);
		} catch (final IOException e) {
			System.err.printf(
			        "There was an I/O error either while interacting with the file system or connecting to the server");
			e.printStackTrace();
			return;
		}
		ui.setVisible(true);
	}
}
