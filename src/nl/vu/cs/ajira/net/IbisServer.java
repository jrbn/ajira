package nl.vu.cs.ajira.net;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.server.Server;
import ibis.ipl.server.ServerProperties;
import ibis.ipl.support.management.AttributeDescription;
import ibis.util.TypedProperties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used as Ibis registry if monitoring is required. In the
 * ibis instances, the managementclient should be enabled, so set the
 * ibis.managementclient property.
 */

public class IbisServer {

	static final Logger log = LoggerFactory.getLogger(IbisServer.class);

	public static final int INTERVAL = 1000;

	private static class Shutdown extends Thread {
		private final Server server;

		Shutdown(Server server) {
			this.server = server;
		}

		@Override
		public void run() {
			server.end(-1);
		}
	}

	public static class Data {
		public long sent;
		public long rcvd;
		public long cputm;
		public long time;
	}

	private static HashMap<IbisIdentifier, Data> map = new HashMap<IbisIdentifier, Data>();

	/**
	 * Creates a new Server with the properties passed through the arguments.
	 * For each ibis instance from every pool it sets its arguments.
	 * 
	 * @param args
	 *            The properties of the server.
	 */
	public static void main(String[] args) {
		TypedProperties properties = new TypedProperties();
		properties.putAll(System.getProperties());
		Set<String> deadPools = new HashSet<String>();
		Set<IbisIdentifier> deadIbises = new HashSet<IbisIdentifier>();

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--port")) {
				i++;
				properties.setProperty(ServerProperties.PORT, args[i]);
			} else if (args[i].equalsIgnoreCase("--events")) {
				properties.setProperty(ServerProperties.PRINT_EVENTS, "true");
			} else {
				log.error("Unknown argument: " + args[i]);
				System.exit(1);
			}
		}

		// start a server
		Server server = null;
		try {
			server = new Server(properties);
		} catch (Throwable t) {
			log.error("Could not start Server: ", t);
			System.exit(1);
		}

		// print server description
		if (log.isInfoEnabled()) {
			log.info(server.toString());
		}

		// register shutdown hook
		Runtime.getRuntime().addShutdownHook(new Shutdown(server));

		AttributeDescription cpu = new AttributeDescription(
				"java.lang:type=OperatingSystem", "ProcessCpuTime");

		AttributeDescription sentBytes = new AttributeDescription("ibis",
				"bytesWritten");

		AttributeDescription receivedBytes = new AttributeDescription("ibis",
				"bytesRead");

		while (true) {

			// get list of ibises in the pool named "test"
			String[] pools = server.getRegistryService().getPools();
			long savedTime = 0;
			for (String pool : pools) {
				if (deadPools.contains(pool)) {
					continue;
				}
				int count = 0;
				IbisIdentifier[] ibises = server.getRegistryService()
						.getMembers(pool);

				// for each ibis, print these attributes
				if (ibises != null) {
					for (IbisIdentifier ibis : ibises) {
						Data oldData = map.get(ibis);
						if (oldData == null) {
							oldData = new Data();
							map.put(ibis, oldData);
						}
						try {
							Object[] objs = server.getManagementService()
									.getAttributes(ibis, cpu, sentBytes,
											receivedBytes);
							count++;
							long sent = Long.parseLong(objs[1].toString());
							long rcvd = Long.parseLong(objs[2].toString());
							long cputime = Long.parseLong(objs[0].toString());
							long time = System.currentTimeMillis();
							double factor = oldData != null ? 1000.0 / (time - oldData.time)
									: 1.0;
							if (log.isInfoEnabled()) {
								log.info(ibis
										+ " [cpu time, bytes sent, bytes read] = ["
										+ ((cputime - oldData.cputm) / 1.0e9)
										* factor + ", " + (sent - oldData.sent)
										* factor + ", " + (rcvd - oldData.rcvd)
										* factor + "]");
							}
							oldData.sent = sent;
							oldData.rcvd = rcvd;
							oldData.cputm = cputime;
							oldData.time = time;
						} catch (Exception e) {
							if (!deadIbises.contains(ibis)) {
								log.error("Could not get management info:", e);
								deadIbises.add(ibis);
							}
						}
					}
				}
				if (count == 0) {
					deadPools.add(pool);
				}
			}

			try {
				if (savedTime != 0) {
					long target = savedTime + INTERVAL;
					long toSleep = System.currentTimeMillis() - target;
					if (toSleep > 0) {
						Thread.sleep(toSleep);
					}
				} else {
					Thread.sleep(INTERVAL);
				}
				savedTime = System.currentTimeMillis();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}
}
