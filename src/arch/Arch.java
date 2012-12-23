package arch;

import ibis.ipl.server.Server;
import ibis.ipl.server.ServerProperties;
import ibis.util.TypedProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.actions.ActionFactory;
import arch.buckets.Buckets;
import arch.buckets.CachedFilesMerger;
import arch.chains.Chain;
import arch.chains.ChainHandler;
import arch.chains.ChainNotifier;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.InputLayerRegistry;
import arch.datalayer.buckets.BucketsLayer;
import arch.dummylayer.DummyLayer;
import arch.net.NetworkLayer;
import arch.statistics.StatisticsCollector;
import arch.storage.Factory;
import arch.storage.SubmissionCache;
import arch.storage.container.CheckedConcurrentWritableContainer;
import arch.storage.container.WritableContainer;
import arch.submissions.Job;
import arch.submissions.Submission;
import arch.submissions.SubmissionRegistry;
import arch.utils.Configuration;
import arch.utils.Consts;
import arch.utils.Utils;
import arch.webinterface.WebServer;

public class Arch {

	static final Logger log = LoggerFactory.getLogger(Arch.class);

	private Context globalContext;
	private boolean localMode;

	/**
	 * Returns whether the current instance was the elected server of the
	 * cluster.
	 * 
	 * @return true if it is, false otherwise.
	 */
	public boolean isFirst() {
		return localMode || globalContext.getNetworkLayer().isServer();
	}

	/**
	 * This method shutdowns the entire cluster. The current JVM is terminated
	 * and a signal is send to all the other nodes to perform the same.
	 */
	public void shutdown() {
		log.info("Framework is shutting down ...");
		if (!localMode) {
			try {
				// Send message to everyone that should stop
				globalContext.getNetworkLayer().signalTermination();
				globalContext.getNetworkLayer().stopIbis();
			} catch (IOException e) {
				log.error("Error in shutting down", e);
				System.exit(1);
			}
		}
		log.info("...done");
	}

	/**
	 * This method starts the entire cluster. Should be invoked on every node
	 * that participate in the computation. After it is finished, the cluster is
	 * ready to accept incoming jobs.
	 * 
	 * @param conf
	 *            A configuration object that contains some possible
	 *            initialization parameters.
	 */
	public void startup(Configuration conf) {
		try {
			long time = System.currentTimeMillis();

			/***** CREATE TMP DIR IF NOT PRESENT *****/
			String tmpDir = System.getProperty("java.io.tmpdir");
			File tmpFile = new File(tmpDir);
			if (!tmpFile.exists()) {
				log.info("Create tmp dir ...");
				if (!Utils.createRecursevily(tmpFile)) {
					log.warn("tmp dir not created!");
				}
			}

			/***** BUFFER FACTORY *****/
			@SuppressWarnings("unchecked")
			Class<WritableContainer<Tuple>> clazz = (Class<WritableContainer<Tuple>>) (Class<?>) WritableContainer.class;
			Factory<WritableContainer<Tuple>> bufferFactory = new Factory<WritableContainer<Tuple>>(
					clazz, true, false, Consts.TUPLES_CONTAINER_BUFFER_SIZE);

			/***** NET *******/
			NetworkLayer net = NetworkLayer.getInstance();
			boolean serverMode = true;

			String ibisPoolSize = System.getProperty("ibis.pool.size");
			localMode = ibisPoolSize == null || ibisPoolSize.equals("1");
			if (!localMode) {
				log.debug("Starting the network layer ...");

				/**** START IBIS IF REQUESTED ****/
				if (conf.getBoolean(Consts.START_IBIS, false)) {
					try {
						TypedProperties properties = new TypedProperties();
						properties.putAll(System.getProperties());
						properties.setProperty(ServerProperties.PRINT_EVENTS,
								"true");
						new Server(properties);
					} catch (Exception e) {
						log.error("Error starting ibis", e);
					}
				}

				net.setBufferFactory(bufferFactory);
				net.startIbis();
				ArrayList<WritableContainer<Tuple>> l = new ArrayList<WritableContainer<Tuple>>(
						Consts.STARTING_SIZE_FACTORY);
				for (int i = 0; i < Consts.STARTING_SIZE_FACTORY; i++) {
					l.add(bufferFactory.get());
				}
				while (l.size() != 0) {
					bufferFactory.release(l.remove(l.size() - 1));
				}

				serverMode = net.isServer();
				log.debug("...done");
			}

			if (serverMode) {
				log.info("Cluster starting up");
			}

			/**** OTHER SHARED DATA STRUCTURES ****/
			WritableContainer<Chain> chainsToProcess = new CheckedConcurrentWritableContainer<Chain>(
					Consts.SIZE_BUFFERS_CHAINS_PROCESS);
			List<ChainHandler> listHandlers = new ArrayList<ChainHandler>();
			StatisticsCollector stats = new StatisticsCollector(conf, net);
			CachedFilesMerger merger = new CachedFilesMerger();
			Buckets tuplesContainer = new Buckets(stats, bufferFactory, merger,
					net);
			ActionFactory ap = new ActionFactory();
			DataProvider dp = new DataProvider();
			Factory<Tuple> defaultTupleFactory = new Factory<Tuple>(
					Tuple.class, (Object[]) null);
			SubmissionCache cache = new SubmissionCache(net);

			SubmissionRegistry registry = new SubmissionRegistry(net, stats,
					chainsToProcess, tuplesContainer, ap, dp, cache, conf);

			/**** INIT INPUT LAYERS ****/
			InputLayer input = InputLayer.getImplementation(conf);
			InputLayerRegistry inputRegistry = new InputLayerRegistry();
			inputRegistry.add(Consts.DEFAULT_INPUT_LAYER_ID, input);
			inputRegistry.add(Consts.BUCKET_INPUT_LAYER_ID, new BucketsLayer());
			inputRegistry.add(Consts.DUMMY_INPUT_LAYER_ID, new DummyLayer());

			/**** INIT CONTEXT ****/
			globalContext = new Context();
			ChainNotifier notifier = new ChainNotifier(globalContext);
			globalContext.init(localMode, inputRegistry, tuplesContainer,
					registry, chainsToProcess, listHandlers, notifier, merger,
					net, stats, ap, dp, defaultTupleFactory, cache, conf);

			/**** START PROCESSING THREADS ****/
			int i = conf.getInt(Consts.N_PROC_THREADS, 1);
			for (int j = 0; j < i; ++j) {
				log.debug("Starting Chain Handler " + j + " ...");
				ChainHandler handler = new ChainHandler(globalContext);
				Thread thread = new Thread(handler);
				thread.setName("Chain Handler " + j);
				thread.start();
				listHandlers.add(handler);
			}

			/**** START SORTING MERGE THREADS ****/
			i = conf.getInt(Consts.N_MERGE_THREADS, 1);
			merger.setNumberThreads(i);
			for (int j = 0; j < i; ++j) {
				log.debug("Starting Sorting Merge threads " + j + " ...");
				Thread thread = new Thread(merger);
				thread.setName("Merge sort " + j);
				thread.start();
			}

			/**** START COMMUNICATION THREADS ****/
			log.debug("Starting Sending/receiving threads ...");
			net.startupConnections(globalContext);

			/**** START STATISTICS THREAD ****/
			log.debug("Starting statistics thread ...");
			Thread thread = new Thread(stats);
			thread.setName("Statistics Printer");
			thread.start();

			/***** LOAD STORAGE *****/
			log.debug("Starting registered input layers ...");
			inputRegistry.startup(globalContext);

			/***** LOAD WEB INTERFACE *****/
			if (conf.getBoolean(WebServer.WEBSERVER_START, false)) {
				log.debug("Starting Web Server on port " + 8080 + "...");
				WebServer www = new WebServer();
				www.startWebServer(globalContext);
			}

			if (serverMode) {
				net.waitUntilAllReady();
				log.info("Time to startup the cluster (ms): "
						+ (System.currentTimeMillis() - time));
				log.info("Cluster ready to accept requests");
			} else {
				net.signalReady();
				log.info("Startup successful");
			}

		} catch (Exception e) {
			log.error("Error creation program", e);
		}
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
	 * This method is used to launch a job in the cluster. It waits until the
	 * job is terminated (or has failed).
	 * 
	 * @param job
	 *            The specification of the job to launch
	 * @return The corresponding submission object that contains informations
	 *         and statistics about the processed job.
	 * @throws Exception
	 */
	public Submission waitForCompletion(Job job) throws Exception {
		Submission sub = globalContext.getSubmissionsRegistry()
				.waitForCompletion(globalContext, job);
		globalContext.getSubmissionsRegistry().getStatistics(job, sub);
		return sub;
	}
}