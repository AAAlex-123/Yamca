package app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import eventDeliverySystem.datastructures.ITopicDAO;
import eventDeliverySystem.filesystem.FileSystemException;
import eventDeliverySystem.filesystem.TopicFileSystem;
import eventDeliverySystem.server.Broker;
import eventDeliverySystem.util.LG;

/**
 * Runs a Server which can be configured by command-line arguments.
 *
 * @author Dimitris Tsirmpas
 */
public class Server {

	// ARG_FLAG and ARG_PATH should be in the same position as ARG_IP and ARG_PORT respectively
	private static final int ARG_BROKER_DIR = 0, ARG_IP = 1, ARG_PORT = 2, ARG_FLAG = 1, ARG_PATH = 2;

	private static final String USAGE = "Usage:\n"
	        + "\t   java app.Server <broker_dir>\n"
	        + "\tor java app.Server <broker_dir> <ip> <port>\n"
	        + "\tor java app.Server <broker_dir> -f <path>\n"
	        + "\n"
	        + "Options:\n"
	        + "\t-f\tread connection configuration from file\n"
	        + "Where:\n"
	        + "\t<broker_dir>\t\t the directory where the topics will be saved for this server\n"
	        + "\t<ip>\t\tthe ip of the first server (run 'ipconfig' on the first server)\n"
	        + "\t<port>\t\tthe port the first server listens to (See 'Broker Port' in the first server's console)\n"
	        + "\t<path>\t\tthe file with the configuration";

	private Server() {}

	/**
	 * Starts a new broker as a process on the local machine. If more than one arg is provided the
	 * broker will attempt to connect to the leader broker. If exactly one arg is provided, the
	 * broker is considered the leader broker. When starting the server subsystem the first broker
	 * MUST be the leader.
	 *
	 * @param args see {@code Server#Usage} for more information or run with no args
	 */
	public static void main(String[] args) {
		LG.args(args);

		switch (args.length) {
			case 1:
			case 3:
				break;
			default:
				LG.sout(Server.USAGE);
				return;
		}

		final boolean leader = args.length == 1;
		final Path    path = new File(args[ARG_BROKER_DIR]).getAbsoluteFile().toPath();
		final String ip;
		final int port;

		if (leader) {
			ip = "not-used";
			port = -1;
		} else {
			final String stringPort;
			if (args[ARG_FLAG].equals("-f")) {
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
		}

		ITopicDAO postDao;
		try {
			postDao = new TopicFileSystem(path);
		} catch (FileSystemException e) {
			LG.err("Path %s does not exist", path);
			return;
		}

		try (Broker broker = leader ? new Broker(postDao) : new Broker(postDao, ip, port)) {
			final String brokerId = leader
					? "Leader"
					: Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000));

			final Thread thread = new Thread(broker, "Broker-" + brokerId);
			thread.start();
			thread.join();
		} catch (InterruptedException e) {
			// do nothing
		} catch (IOException e) {
			LG.err("I/O error associated with path %s", path);
			e.printStackTrace();
		}
	}
}
