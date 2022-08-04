package app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import eventdeliverysystem.dao.IProfileDAO;
import eventdeliverysystem.filesystem.FileSystemException;
import eventdeliverysystem.filesystem.ProfileFileSystem;
import eventdeliverysystem.util.LG;

/**
 * Runs a Client which can be configured by command-line arguments.
 *
 * @author Alex Mandelias
 */
final class Client {

	private static final String LINE_SEP = System.lineSeparator();
	private static final int MAX_PORT_NUMBER = 65_535;

	// ARG_F_FLAG and ARG_PATH should be in the same position as ARG_IP and ARG_PORT respectively
	private static final int ARG_CL_FLAG = 0;
	private static final int ARG_NAME = 1;
	private static final int ARG_IP = 2;
	private static final int ARG_PORT = 3;
	private static final int ARG_F_FLAG = 2;
	private static final int ARG_PATH = 3;
	private static final int ARG_USER_DIR = 4;

	private static final String USAGE = "Usage:" + LINE_SEP
	        + "\t   java app.Client [-c|-l] <name> <ip> <port> <user_dir>" + LINE_SEP
	        + "\tor java app.Client [-c|-l] <name> -f <path> <user_dir>" + LINE_SEP
	        + LINE_SEP
	        + "Options:" + LINE_SEP
	        + "\t-c\tcreate new user with the <name>\t" + LINE_SEP
	        + "\t-l\tload existing user with the <name>" + LINE_SEP
	        + "\t-f\tread connection configuration from file" + LINE_SEP
	        + LINE_SEP
	        + "Where:" + LINE_SEP
	        + "\t<ip>\t\tthe ip of the server" + LINE_SEP
	        + "\t<port>\t\tthe port the server listens to (See 'Client Port' in the server console)"
	                                    + LINE_SEP
	        + "\t<path>\t\tthe file with the configuration\t<user_dir>\tthe directory in the file"
	                                    + " system to store the data";

	private Client() {}

	/**
	 * Runs a Client which can be configured by args. Run with no arguments for a help message.
	 *
	 * @param args see {@code Server#Usage} for more information or run with no args
	 */
	public static void main(String[] args) {
		LG.setOut(System.out);
		LG.setErr(System.err);

		LG.args(args);

		if (args.length != 5) {
			LG.sout(Client.USAGE);
			return;
		}

		final String type = args[ARG_CL_FLAG];
		final String name = args[ARG_NAME];

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

		if ("-f".equals(args[ARG_F_FLAG])) {
			Properties props = new Properties();
			try (FileInputStream fis = new FileInputStream(args[ARG_PATH])) {
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

		final int port;
		try {
			port = Integer.parseInt(stringPort);
			if (port <= 0 || port > Client.MAX_PORT_NUMBER) {
				throw new IllegalArgumentException();
			}
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
			boolean existing = "-l".equals(type);
			ui = new CrappyUserUI(existing, name, ip, port, profileDao);
		} catch (final IOException e) {
			LG.err("There was an IO error either while interacting with the file system or"
			       + " connecting to the server");
			e.printStackTrace();
			return;
		}
		ui.setVisible(true);
	}
}
