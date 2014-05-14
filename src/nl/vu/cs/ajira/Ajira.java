package nl.vu.cs.ajira;

import ibis.ipl.server.Server;
import ibis.ipl.server.ServerProperties;
import ibis.util.TypedProperties;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;

import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.CachedFilesMerger;
import nl.vu.cs.ajira.buckets.WritableTuple;
import nl.vu.cs.ajira.chains.ChainHandlerManager;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputLayerRegistry;
import nl.vu.cs.ajira.datalayer.buckets.BucketsLayer;
import nl.vu.cs.ajira.datalayer.chainsplits.ChainSplitLayer;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.mgmt.MemoryManager;
import nl.vu.cs.ajira.mgmt.NodeHouseKeeper;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.mgmt.WebServer;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.SubmissionCache;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.submissions.SubmissionRegistry;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the main class that allows the user to interface with the program. It
 * allows the user to start the cluster, submit the jobs, wait to their
 * completion and so on. Starting the cluster using this object should be the
 * first thing to do.
 * <p>
 * Note: for now, there are some serious limitations when using Ajira as a
 * cluster: all classes used must be on Ajira's classpath, jobs can interfere
 * with other jobs (by crashing, for instance).
 */
public class Ajira {

	static final Logger log = LoggerFactory.getLogger(Ajira.class);

	private final Configuration conf = new Configuration();
	private Context globalContext = null;
	private boolean localMode;

	/**
	 * In cluster mode, the cluster is started separately, and jobs can be
	 * submitted by communicating with the server node of the cluster.
	 */
	private final boolean clusterMode;

	/**
	 * Constructor for use in the non-cluster-mode case.
	 */
	public Ajira() {
		this(false);
	}

	/**
	 * Constructor.
	 * 
	 * @param clusterMode
	 *            whether to start in cluster mode or not.
	 */
	public Ajira(boolean clusterMode) {
		this.clusterMode = clusterMode;
	}

	/**
	 * Returns whether the current instance was the elected server of the
	 * cluster and therefore can accept submissions. Only one node of the
	 * cluster will return true to this call.
	 * 
	 * @return true if it is, false otherwise.
	 */
	public boolean amItheServer() {
		return localMode || globalContext.getNetworkLayer().isServer();
	}

	/**
	 * This method returns an object that contains all the configuration
	 * parameters of the cluster. This object can be modified before the cluster
	 * is started in order to change some built-in parameters or add custom
	 * parameters that should be visible to all the cluster.
	 * 
	 * @return the configuration file
	 */
	public Configuration getConfiguration() {
		return conf;
	}

	/**
	 * This method shutdowns the entire cluster. The current JVM is terminated
	 * and a signal is send to all the other nodes to perform the same.
	 */
	public void shutdown() {
		log.info("Framework is shutting down ...");
		globalContext.getInputLayerRegistry().close();
		if (!localMode) {
			globalContext.getNetworkLayer().signalTermination();
			try {
				// Send message to everyone that should stop
				globalContext.getNetworkLayer().stopIbis();
				System.exit(0);
			} catch (Throwable e) {
				log.error("Error in shutting down", e);
				System.exit(1);
			}
			log.info("...done");
		} else {
			log.info("...done");
			System.exit(0);
		}
	}

	/**
	 * This method starts the entire cluster. Should be invoked on every node
	 * that participate in the computation. After it is finished, the cluster is
	 * ready to accept incoming jobs.
	 */
	public void startup() throws Exception {
		long time = System.currentTimeMillis();

		/***** CREATE TMP DIR IF NOT PRESENT *****/
		String tmpDir = System.getProperty("java.io.tmpdir");
		File tmpFile = new File(tmpDir);
		if (!tmpFile.exists()) {
			if (log.isInfoEnabled()) {
				log.info("Create tmp dir ...");
			}
			if (!Utils.createRecursevily(tmpFile)) {
				log.warn("tmp dir not created!");
			}
		}

		/***** NET *******/
		NetworkLayer net = NetworkLayer.getInstance();
		boolean serverMode = true;

		@SuppressWarnings("unchecked")
		Class<WritableContainer<WritableTuple>> clazz = (Class<WritableContainer<WritableTuple>>) (Class<?>) WritableContainer.class;
		Factory<WritableContainer<WritableTuple>> bufferFactory = new Factory<WritableContainer<WritableTuple>>(
				clazz, Consts.TUPLES_CONTAINER_MAX_BUFFER_SIZE);
		MemoryManager.getInstance().registerFactory(bufferFactory);

		String ibisPoolSize = System.getProperty("ibis.pool.size");
		localMode = !clusterMode
				&& (ibisPoolSize == null || ibisPoolSize.equals("1"));

		if (!localMode) {
			if (log.isDebugEnabled()) {
				log.debug("Starting the network layer ...");
			}

			/**** START IBIS IF REQUESTED ****/
			if (conf.getBoolean(Consts.START_IBIS, false)) {
				TypedProperties properties = new TypedProperties();
				properties.putAll(System.getProperties());
				properties.setProperty(ServerProperties.PRINT_EVENTS, "true");
				new Server(properties);
			}

			net.setBufferFactory(bufferFactory);
			net.startIbis(clusterMode);
			ArrayList<WritableContainer<WritableTuple>> l = new ArrayList<WritableContainer<WritableTuple>>(
					Consts.STARTING_SIZE_FACTORY);
			for (int i = 0; i < Consts.STARTING_SIZE_FACTORY; i++) {
				l.add(bufferFactory.get());
			}
			while (l.size() != 0) {
				bufferFactory.release(l.remove(l.size() - 1));
			}

			serverMode = net.isServer();
			if (log.isDebugEnabled()) {
				log.debug("...done");
			}
		}

		if (log.isInfoEnabled() && serverMode) {
			log.info("Cluster starting up");
		}

		StatisticsCollector stats = new StatisticsCollector(conf, net);

		globalContext = new Context();

		/**** OTHER SHARED DATA STRUCTURES ****/
		CachedFilesMerger merger = new CachedFilesMerger();
		Buckets tuplesContainer = new Buckets(stats, globalContext, merger,
				net, bufferFactory);
		ActionFactory ap = new ActionFactory();
		DataProvider dp = new DataProvider();
		SubmissionCache cache = new SubmissionCache(net);
		ChainHandlerManager manager = ChainHandlerManager.getInstance();

		SubmissionRegistry registry = new SubmissionRegistry(net, stats,
				manager.getChainsToProcess(), tuplesContainer, ap, dp, cache,
				conf);

		/**** INIT INPUT LAYERS ****/
		InputLayerRegistry inputRegistry = new InputLayerRegistry();
		inputRegistry.registerLayer(new BucketsLayer(), false);
		inputRegistry.registerLayer(new DummyLayer(), false);
		inputRegistry.registerLayer(ChainSplitLayer.getInstance(), false);
		Class<? extends InputLayer> input = InputLayer
				.getDefaultInputLayerClass(conf);
		inputRegistry.registerLayer(input.newInstance(), true);

		/**** INIT CONTEXT ****/
		ChainNotifier notifier = new ChainNotifier();
		globalContext.init(localMode, inputRegistry, tuplesContainer, registry,
				manager, notifier, merger, net, stats, ap, dp, cache, conf);
		notifier.init(globalContext);

		/**** START PROCESSING THREADS ****/
		manager.setContext(globalContext);
		int nProcs = Runtime.getRuntime().availableProcessors();
		if (nProcs > 2) {
			nProcs /= 2;
		}
		int i = conf.getInt(Consts.N_PROC_THREADS, nProcs);
		manager.startChainHandlers(i);

		/**** START SORTING MERGE THREADS ****/
		i = conf.getInt(Consts.N_MERGE_THREADS, 1);
		merger.setNumberThreads(i);
		for (int j = 0; j < i; ++j) {
			if (log.isDebugEnabled()) {
				log.debug("Starting Sorting Merge threads " + j + " ...");
			}
			Thread thread = new Thread(merger);
			thread.setName("Merge sort " + j);
			thread.start();
		}

		/**** START COMMUNICATION THREADS ****/
		if (log.isDebugEnabled()) {
			log.debug("Starting Sending/receiving threads ...");
		}
		net.startupConnections(globalContext);

		/***** LOAD STORAGE *****/
		if (log.isDebugEnabled()) {
			log.debug("Starting registered input layers ...");
		}
		inputRegistry.startup(globalContext);

		/***** LOAD WEB INTERFACE *****/
		if (conf.getBoolean(WebServer.WEBSERVER_START, true) && serverMode) {
			WebServer www = new WebServer();
			www.startWebServer(globalContext);
			String addr = www.getAddress();
			if (log.isInfoEnabled() && addr != null) {
				log.info("Ajira WebInterface available at: " + addr);
			}
		}

		/***** HOUSE KEEPING *****/
		if (log.isDebugEnabled()) {
			log.debug("Start housekeeping thread ...");
		}
		NodeHouseKeeper hk = new NodeHouseKeeper(globalContext);
		Thread thread = new Thread(hk);
		thread.setName("Housekeeping");
		thread.setDaemon(true);
		thread.start();

		if (serverMode) {
			net.waitUntilAllReady();
			log.info("Time to startup the cluster (ms): "
					+ (System.currentTimeMillis() - time));
		} else {
			net.signalReady();
			if (log.isInfoEnabled()) {
				log.info("Startup successful");
			}
		}
	}

	/**
	 * This method returns <code>true</code> if this is a single-node run,
	 * <code>false</code> otherwise.
	 * 
	 * @return whether this is a single-node run
	 */
	public boolean isLocalMode() {
		return localMode;
	}

	/**
	 * This methods returns the node ID of the machine in the cluster. For
	 * example, if there are three machines, then it will return 0, 1, or 2.
	 * 
	 * @return The node ID of the current machine
	 */
	public int getMyNodeID() {
		if (localMode)
			return 0;
		else
			return globalContext.getNetworkLayer().getMyPartition();
	}

	/**
	 * This method returns the number of nodes in the Ajira cluster.
	 * 
	 * @return the number of nodes.
	 */
	public int getNumberNodes() {
		if (localMode) {
			return 1;
		}
		return globalContext.getNetworkLayer().getNumberNodes();
	}

	/**
	 * @return The global context used by the framework.
	 */
	public Context getContext() {
		return globalContext;
	}

	public int getBucketNo() {
		return globalContext.getUserBucketNo();
	}

	/**
	 * This method is used to launch a job in the cluster. It waits until the
	 * job is terminated (or has failed).
	 * 
	 * @param job
	 *            The specification of the job to launch
	 * @return The corresponding submission object that contains informations
	 *         and statistics about the processed job.
	 */
	public Submission waitForCompletion(Job job) {
		Submission sub = globalContext.getSubmissionsRegistry()
				.waitForCompletion(globalContext, job);
		globalContext.getSubmissionsRegistry().getStatistics(sub);
		return sub;
	}

	/**
	 * Starts Ajira in cluster mode.
	 * 
	 * @param args
	 *            cluster arguments.
	 */
	public static void main(String[] args) {
		// Use the cluster-mode Ajira constructor.
		Ajira ajira = new Ajira(true);

		// First argument is a filename to store some cluster information in.
		// Intention is that a client only needs access to this file to be able
		// to
		// submit a job.
		if (args.length == 0) {
			log.error("A cluster info file name must be specified");
			System.exit(1);
		}
		File clusterInfoFile = new File(args[0]);
		for (int i = 1; i < args.length; i++) {
			if (args[i].equals("-pool")) {
				System.setProperty("ibis.pool.name", args[++i]);
			} else if (args[i].equals("-server")) {
				System.setProperty("ibis.server.address", args[++i]);
			} else {
				log.error("Unknown argument: " + args[i]);
				System.exit(1);
			}
		}

		String poolName = System.getProperty("ibis.pool.name");
		String serverAddress = System.getProperty("ibis.server.address");

		if (poolName == null) {
			System.setProperty("ibis.pool.name", args[0]);
			poolName = args[0];
		}
		if (serverAddress == null) {
			log.error("an ibis server address must be specified");
			System.exit(1);
		}

		try {
			ajira.startup();
		} catch (Throwable e) {
			log.error("Could not start up Ajira", e);
			System.exit(1);
		}
		if (ajira.amItheServer()) {
			// Write the contact info for the cluster to the specified file.
			// Terminate cluster if this does not work for some reason.
			OutputStream out = null;
			try {
				out = new BufferedOutputStream(new FileOutputStream(
						clusterInfoFile));
			} catch (FileNotFoundException e) {
				log.error("Could not create cluster info file", e);
				ajira.shutdown();
				System.exit(1);
			}
			Properties p = new Properties();
			p.setProperty("ibis.server.address", serverAddress);
			p.setProperty("ibis.pool.name", poolName + "-server");
			try {
				p.store(out, "Ajira Cluster properties file");
				out.close();
			} catch (Throwable e) {
				log.error("Could not write properties file", e);
				ajira.shutdown();
				System.exit(1);
			}
		}
	}
}
