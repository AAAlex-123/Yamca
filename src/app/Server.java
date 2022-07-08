package app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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

	private static final int ARG_PATH = 0, ARG_IP = 1, ARG_PORT = 2;

	private static final String USAGE = "Usage:\n"
	        + "\t   java app.Server <path>\n"
	        + "\tor java app.Server <path> <ip> <port>\n"
	        + "\n"
	        + "Arguments for servers after the first one:\n"
			+ "\t<path>\t\t the directory where the topics will be saved for this server\n"
	        + "\t<ip>\t\tthe ip of the first server (run 'ipconfig' on the first server)\n"
	        + "\t<port>\t\tthe port the first server listens to (See 'Broker Port' in the first server's console)\n";

	private Server() {}

	/**
	 * Starts a new broker as a process on the local machine. If args are provided
	 * the broker will attempt to connect to the leader broker. If not, the broker
	 * is considered the leader broker. When starting the server subsystem the first
	 * broker MUST be the leader.
	 *
	 * @param args empty if the broker is the leader, the IP address and port of the
	 *             leader otherwise.
	 */
	public static void main(String[] args) {
		LG.args(args);

		switch (args.length) {
			case 1:
			case 3:
				break;
			default:
				System.out.println(Server.USAGE);
				return;
		}

		final boolean leader = args.length == 1;
		final Path    path = new File(args[ARG_PATH]).getAbsoluteFile().toPath();
		final String ip;
		final int port;

		if (leader) {
			ip = "not-used";
			port = -1;
		} else {
			ip = args[ARG_IP];
			try {
				port = Integer.parseInt(args[ARG_PORT]);
				if (port < 0 || port > 65_535)
					throw new IllegalArgumentException();

			} catch (final NumberFormatException e) {
				throw new IllegalArgumentException(e);

			} catch (IllegalArgumentException e) {
				System.err.printf("Invalid port number: %s", args[ARG_PORT]);
				return;
			}
		}

		ITopicDAO postDao;
		try {
			postDao = new TopicFileSystem(path);
		} catch (FileSystemException e) {
			System.err.printf("Path %s does not exist", path);
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
			System.err.printf("Path %s does not exist", path);
		}
	}
}
