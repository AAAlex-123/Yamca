package app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import eventDeliverySystem.dao.IProfileDAO;
import eventDeliverySystem.dao.ITopicDAO;
import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.ProfileFileSystem;
import eventDeliverySystem.filesystem.TopicFileSystem;
import eventDeliverySystem.util.LG;

/**
 * Runs a Client which can be configured by command-line arguments.
 *
 * @author Alex Mandelias
 */
public class Client {

	// ARG_F_FLAG and ARG_PATH should be in the same position as ARG_IP and ARG_PORT respectively
	private static final int ARG_CL_FLAG = 0, ARG_NAME = 1, ARG_IP = 2, ARG_PORT = 3, ARG_F_FLAG = 2, ARG_PATH = 3, ARG_USER_DIR = 4;

	private static final String USAGE = "Usage:\n"
	        + "\t   java app.Client [-c|-l] <name> <ip> <port> <user_dir>\n"
	        + "\tor java app.Client [-c|-l] <name> -f <path> <user_dir>\n"
	        + "\n"
	        + "Options:\n"
	        + "\t-c\tcreate new user with the <name>\t\n"
	        + "\t-l\tload existing user with the <name>\n"
	        + "\t-f\tread connection configuration from file\n"
	        + "\n"
	        + "Where:\n"
	        + "\t<ip>\t\tthe ip of the server\n"
	        + "\t<port>\t\tthe port the server listens to (See 'Client Port' in the server console)\n"
	        + "\t<path>\t\tthe file with the configuration\t<user_dir>\tthe directory in the file system to store the data";

	private Client() {}

	/**
	 * Runs a Client which can be configured by args. Run with no arguments for a help message.
	 *
	 * @param args see {@code Server#Usage} for more information or run with no args
	 */
	public static void main(String[] args) {
		LG.args(args);

		if (args.length != 5) {
			LG.sout(Client.USAGE);
			return;
		}

		final String type = args[ARG_CL_FLAG];
		final String name = args[ARG_NAME];

		boolean existing = type.equals("-l");
		switch (type) {
		case "-c":
		case "-l":
			break;
		default:
			LG.sout(Client.USAGE);
			return;
		}

		final String ip;
		final String stringPort;
		final int port;

		if (args[ARG_F_FLAG].equals("-f")) {
			Properties props = new Properties();
			try (FileInputStream fis = new FileInputStream(args[ARG_PATH])){
				props.load(fis);
			} catch (FileNotFoundException e) {
				LG.err("Could not find configuration file: %s", args[ARG_PATH]);
				return;
			} catch (IOException e) {
				LG.err("Unexpected Error while reading configuration from file: %s. Please try "
					   + "manually inputting ip and port.", args[ARG_PATH]);
				return;
			}

			ip = props.getProperty("ip");
			stringPort = props.getProperty("port");

		} else {
			ip = args[ARG_IP];
			stringPort = args[ARG_PORT];
		}

		try {
			port = Integer.parseInt(stringPort);
			if (port < 0 || port > 65_535)
				throw new IllegalArgumentException();

		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException(e);

		} catch (IllegalArgumentException e) {
			LG.err("Invalid port number: %s", stringPort);
			return;
		}

		final Path dir = new File(args[ARG_USER_DIR]).toPath();

		IProfileDAO profileDao;
		try {
			profileDao = new ProfileFileSystem(dir);
		} catch (FileSystemException e) {
			LG.err("Path %s does not exist", dir);
			return;
		}

		CrappyUserUI ui;
		try {
			ui = new CrappyUserUI(existing, name, ip, port, profileDao);
		} catch (final IOException e) {
			LG.err("There was an I/O error either while interacting with the file system or"
				   + "connecting to the server");
			e.printStackTrace();
			return;
		}
		ui.setVisible(true);
	}
}
