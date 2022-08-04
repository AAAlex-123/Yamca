package app;

import java.util.concurrent.ThreadLocalRandom;

import eventDeliverySystem.server.Broker;
import eventDeliverySystem.util.LG;

/**
 * Runs a Server which can be configured by command-line arguments.
 *
 * @author Dimitris Tsirmpas
 */
public class Server {

	private static final String USAGE = "Usage:\n"
	        + "\t   java app.Server\n"
	        + "\tor java app.Server <ip> <port>\n"
	        + "\n"
	        + "Arguments for servers after the first one:\n"
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

		final boolean leader;
		final String brokerId;

		String ip   = "";
		int    port = 0;

		if (args.length == 0) {
			leader = true;
			brokerId = "Main";

		} else if (args.length == 2) {
			leader = false;
			brokerId = Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000));

			ip = args[0];
			try {
				port = Integer.parseInt(args[1]);
			} catch (final NumberFormatException e) {
				System.err.printf("Invalid port: %s", args[1]);
				return;
			}

		} else {
			System.out.println(Server.USAGE);
			return;
		}

		try (Broker broker = leader ? new Broker() : new Broker(ip, port)) {
			final Thread thread = new Thread(broker, "Broker-" + brokerId);
			thread.start();
			thread.join();
		} catch (InterruptedException e) {
			// do nothing
		}
	}
}
