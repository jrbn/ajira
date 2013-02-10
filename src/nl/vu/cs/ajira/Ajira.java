package nl.vu.cs.ajira;

import ibis.ipl.server.Server;
import ibis.ipl.server.ServerProperties;
import ibis.util.TypedProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.CachedFilesMerger;
import nl.vu.cs.ajira.buckets.TupleSerializer;
import nl.vu.cs.ajira.chains.ChainHandlerManager;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputLayerRegistry;
import nl.vu.cs.ajira.datalayer.buckets.BucketsLayer;
import nl.vu.cs.ajira.datalayer.chainsplits.ChainSplitLayer;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.storage.SubmissionCache;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.submissions.SubmissionRegistry;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.Utils;
import nl.vu.cs.ajira.webinterface.WebServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jacopo Urbani
 * 
 *         This is the main class that allows the user to interface with the
 *         program. It allows the user to start the cluster, submit the jobs,
 *         wait to their completion and so on. Starting the cluster using this
 *         object should be the first thing to do.
 */
public class Ajira {

	static final Logger log = LoggerFactory.getLogger(Ajira.class);

	private Configuration conf = new Configuration();
	private Context globalContext = null;
	private boolean localMode;

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
		if (!localMode) {
			try {
				// Send message to everyone that should stop
				globalContext.getNetworkLayer().signalTermination();
				globalContext.getNetworkLayer().stopIbis();
			} catch (IOException e) {
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
	 * 
	 * @param conf
	 *            A configuration object that contains some possible
	 *            initialization parameters.
	 */
	public void startup() {
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

			/***** NET *******/
			NetworkLayer net = NetworkLayer.getInstance();
			StatisticsCollector stats = new StatisticsCollector(conf, net);
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

				@SuppressWarnings("unchecked")
				Class<WritableContainer<TupleSerializer>> clazz = (Class<WritableContainer<TupleSerializer>>) (Class<?>) WritableContainer.class;
				Factory<WritableContainer<TupleSerializer>> bufferFactory = new Factory<WritableContainer<TupleSerializer>>(
						clazz, Consts.TUPLES_CONTAINER_BUFFER_SIZE);
				net.setBufferFactory(bufferFactory);
				net.startIbis();
				ArrayList<WritableContainer<TupleSerializer>> l = new ArrayList<WritableContainer<TupleSerializer>>(
						Consts.STARTING_SIZE_FACTORY);
				for (int i = 0; i < Consts.STARTING_SIZE_FACTORY; i++) {
					l.add(bufferFactory.get());
				}
				while (l.size() != 0) {
					bufferFactory.release(l.remove(l.size() - 1));
				}

				serverMode = net.isServer();
				log.debug("...done");

				/**** START STATISTICS THREAD ****/
				log.debug("Starting statistics thread ...");
				Thread thread = new Thread(stats);
				thread.setName("Statistics Printer");
				thread.start();
			}

			if (serverMode) {
				log.info("Cluster starting up");
			}

			/**** OTHER SHARED DATA STRUCTURES ****/
			CachedFilesMerger merger = new CachedFilesMerger();
			Buckets tuplesContainer = new Buckets(stats, merger, net);
			ActionFactory ap = new ActionFactory();
			DataProvider dp = new DataProvider();
			SubmissionCache cache = new SubmissionCache(net);
			ChainHandlerManager manager = ChainHandlerManager.getInstance();

			SubmissionRegistry registry = new SubmissionRegistry(net, stats,
					manager.getChainsToProcess(), tuplesContainer, ap, dp,
					cache, conf);

			/**** INIT INPUT LAYERS ****/
			InputLayer input = InputLayer.getImplementation(conf);
			InputLayerRegistry inputRegistry = new InputLayerRegistry();
			inputRegistry.add(Consts.DEFAULT_INPUT_LAYER_ID, input);
			inputRegistry.add(Consts.BUCKET_INPUT_LAYER_ID, new BucketsLayer());
			inputRegistry.add(Consts.DUMMY_INPUT_LAYER_ID, new DummyLayer());
			inputRegistry.add(Consts.SPLITS_INPUT_LAYER,
					ChainSplitLayer.getInstance());

			/**** INIT CONTEXT ****/
			globalContext = new Context();
			ChainNotifier notifier = new ChainNotifier(globalContext);
			globalContext.init(localMode, inputRegistry, tuplesContainer,
					registry, manager, notifier, merger, net, stats, ap, dp,
					cache, conf);

			/**** START PROCESSING THREADS ****/
			manager.setContext(globalContext);
			int i = conf.getInt(Consts.N_PROC_THREADS, 1);
			manager.startChainHandlers(i);

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

			/***** LOAD STORAGE *****/
			log.debug("Starting registered input layers ...");
			inputRegistry.startup(globalContext);

			/***** LOAD WEB INTERFACE *****/
			if (conf.getBoolean(WebServer.WEBSERVER_START, true)) {
				WebServer www = new WebServer();
				www.startWebServer(globalContext);
				log.info("Ajira WebInterface available at: " + www.getAddress());
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
	 */
	public Submission waitForCompletion(Job job) {
		Submission sub = globalContext.getSubmissionsRegistry()
				.waitForCompletion(globalContext, job);
		globalContext.getSubmissionsRegistry().getStatistics(sub);
		return sub;
	}
}