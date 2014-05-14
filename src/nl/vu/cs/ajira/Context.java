package nl.vu.cs.ajira;

import ibis.ipl.WriteMessage;
import ibis.smartsockets.util.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.CachedFilesMerger;
import nl.vu.cs.ajira.chains.ChainHandlerManager;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputLayerRegistry;
import nl.vu.cs.ajira.mgmt.StatisticsCollector;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.storage.SubmissionCache;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.submissions.SubmissionRegistry;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.UniqueCounter;

/**
 * This class contains all the variables, objects, constants that might be
 * useful to the different components of the cluster. In general, this context
 * is visible within the architecture, but should not visible to the user, who
 * has access to the ActionContext instead.
 */
public class Context {

	private static final long BUCKET_INIT = 1000000;
	private static final long USER_BUCKET_INIT = 100;
	private static final long CHAIN_INIT = 1;

	/**
	 * Private class for a set of crashed submissions. An entry remains in this
	 * set for one hour. That should be long enough for all activity for that
	 * submission to die out.
	 */
	private static class CrashedSubmissions implements Runnable {

		/** Time to keep a crashed submission. Currently set to 1 hour. */
		private static final int TIME_TO_KEEP = 1000 * 60 * 60;

		/**
		 * How often do we purge the crashed submission set? Currently set to 5
		 * minutes.
		 */
		private static final int INTERVAL = 5 * 1000 * 60;

		/**
		 * Maps crashed submissions to the time we were notified that it
		 * crashed.
		 */
		private final Map<Integer, Long> crashedSubmissions = new HashMap<Integer, Long>();

		/**
		 * Maps crashed submissions to the time we were notified that it
		 * crashed.
		 */
		private final Map<Integer, Throwable> throwables = new HashMap<Integer, Throwable>();

		/**
		 * Adds the specified submission to the crashed submissions.
		 * 
		 * @param submissionId
		 *            a submission that crashed.
		 * @return whether the submission was actually added.
		 */
		public synchronized boolean addCrashedSubmission(int submissionId,
				Throwable e) {
			if (!crashedSubmissions.containsKey(submissionId)) {
				crashedSubmissions
						.put(submissionId, System.currentTimeMillis());
				throwables.put(submissionId, e);
				return true;
			}
			return false;
		}

		/**
		 * Returns true if the specified submission crashed.
		 * 
		 * @param submissionId
		 *            the submission to examine.
		 * @return whether the specified submission crashed.
		 */
		public synchronized boolean hasCrashed(int submissionId) {
			return crashedSubmissions.containsKey(submissionId);
		}

		/**
		 * Purges the crashed-submission set each INTERVAL.
		 */
		@Override
		public void run() {
			for (;;) {
				synchronized (this) {
					Set<Entry<Integer, Long>> entries = new HashSet<Entry<Integer, Long>>(
							crashedSubmissions.entrySet());
					long t = System.currentTimeMillis();
					for (Entry<Integer, Long> e : entries) {
						if (t - e.getValue() > TIME_TO_KEEP) {
							crashedSubmissions.remove(e.getKey());
						}
					}
				}
				try {
					Thread.sleep(INTERVAL);
				} catch (InterruptedException e) {
					// ignore
				}
			}
		}
	}

	private boolean localMode;
	private InputLayerRegistry input;
	private Configuration conf;
	private Buckets container;
	private SubmissionRegistry registry;
	private NetworkLayer net;
	private StatisticsCollector stats;
	private ActionFactory actionProvider;
	private DataProvider dataProvider;
	private SubmissionCache cache;
	private ChainNotifier chainNotifier;
	private CachedFilesMerger merger;
	private CrashedSubmissions crashedSubmissions;
	private ChainHandlerManager manager;

	private UniqueCounter counter;

	/**
	 * This method initializes the global context. Called when the program is
	 * starting. In the parameters there are all internal datastructures that
	 * can be accessed globally.
	 * 
	 * @param localMode
	 * @param input
	 * @param container
	 * @param registry
	 * @param manager
	 * @param notifier
	 * @param merger
	 * @param net
	 * @param stats
	 * @param actionProvider
	 * @param dataProvider
	 * @param cache
	 * @param conf
	 */
	public void init(boolean localMode, InputLayerRegistry input,
			Buckets container, SubmissionRegistry registry,
			ChainHandlerManager manager, ChainNotifier notifier,
			CachedFilesMerger merger, NetworkLayer net,
			StatisticsCollector stats, ActionFactory actionProvider,
			DataProvider dataProvider, SubmissionCache cache, Configuration conf) {
		counter = localMode ? new UniqueCounter() : new UniqueCounter(
				net.getNumberNodes(), net.getMyPartition());

		this.localMode = localMode;
		this.input = input;
		this.conf = conf;
		this.container = container;
		this.registry = registry;
		this.manager = manager;
		this.chainNotifier = notifier;
		this.net = net;
		this.stats = stats;
		this.actionProvider = actionProvider;
		this.dataProvider = dataProvider;
		this.cache = cache;
		this.merger = merger;

		initializeCounter(Consts.BUCKETCOUNTER_NAME, BUCKET_INIT);
		initializeCounter(Consts.CHAINCOUNTER_NAME, CHAIN_INIT);

		crashedSubmissions = new CrashedSubmissions();
		ThreadPool.createNew(crashedSubmissions, "Died-submissions-purger");
	}

	/**
	 * Returns whether the specified submission has crashed.
	 * 
	 * @param submissionId
	 *            the submission
	 * @return whether the specified submission has crashed
	 */
	public boolean hasCrashed(int submissionId) {
		return crashedSubmissions.hasCrashed(submissionId);
	}

	/**
	 * Remove all left-overs of a failed submission.
	 * 
	 * @param submissionNode
	 *            the node that submitted the job
	 * @param submissionId
	 *            the failed submission
	 * @param e
	 *            the exception that caused the submission to crash.
	 */
	public void cleanupSubmission(int submissionNode, int submissionId,
			Throwable e) {

		if (!crashedSubmissions.addCrashedSubmission(submissionId, e)) {
			return;
		}

		manager.submissionFailed(submissionId);

		cache.clearAll(submissionId);

		if (net.getMyPartition() == submissionNode) {
			registry.killSubmission(submissionId, e);
		}
	}

	public void killSubmission(int submissionNode, int submissionId, Throwable e) {
		WriteMessage msg = null;
		try {
			msg = net.getMessageToBroadcast();
			msg.writeByte((byte) 2);
			msg.writeBoolean(true);
			msg.writeInt(submissionId);
			msg.writeInt(submissionNode);
			msg.writeObject(e);
			msg.finish();
		} catch (IOException ex) {
			// What else ... we could not communicate failure.
			if (msg != null) {
				msg.finish(ex);
			}
		} finally {
			cleanupSubmission(submissionNode, submissionId, e);
		}
	}

	public CachedFilesMerger getMergeSortThreadsInfo() {
		return merger;
	}

	public ChainHandlerManager getChainHandlerManager() {
		return manager;
	}

	public SubmissionCache getSubmissionCache() {
		return cache;
	}

	public StatisticsCollector getStatisticsCollector() {
		return stats;
	}

	public NetworkLayer getNetworkLayer() {
		return net;
	}

	public InputLayer getInputLayer(Class<? extends InputLayer> inputLayer) {
		return input.getLayer(inputLayer);
	}

	public InputLayerRegistry getInputLayerRegistry() {
		return input;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public Buckets getBuckets() {
		return container;
	}

	public SubmissionRegistry getSubmissionsRegistry() {
		return registry;
	}

	public Submission getSubmission(int submissionId) {
		return registry.getSubmission(submissionId);
	}

	public ChainNotifier getChainNotifier() {
		return chainNotifier;
	}

	public ActionFactory getActionsProvider() {
		return actionProvider;
	}

	public DataProvider getDataProvider() {
		return dataProvider;
	}

	/**
	 * Returns an unique counter.
	 * 
	 * @param name
	 *            the name of the counter
	 * @return Returns a globally unique ID for the counter specified in input.
	 *         It can be used for various purposes.
	 */
	public long getUniqueCounter(String name) {
		return counter.getCounter(name);
	}

	/**
	 * Returns whether the program is launched on a single machine or there is a
	 * cluster of machines. This makes a difference because if there are more
	 * machines, then the network layer is used, otherwise everything is done
	 * locally.
	 * 
	 * @return whether the program runs on a single machine.
	 */
	public boolean isLocalMode() {
		return localMode;
	}

	private void initializeCounter(String name, long init) {
		counter.init(name, init);
	}

	/**
	 * This method is used to get new counters to assign to buckets. It starts
	 * from the value defined in the constant BUCKET_INIT (in the same object).
	 * These IDs are globally unique. This method is not accessible to the user.
	 * 
	 * @param submissionId
	 * @return a new ID for a bucket.
	 */
	public int getBucketCounter(int submissionId) {
		String name = Consts.BUCKETCOUNTER_NAME + submissionId;
		synchronized (counter) {
			if (!counter.hasCounter(name)) {
				initializeCounter(name, BUCKET_INIT);
			}
		}
		return (int) getUniqueCounter(name);
	}

	/**
	 * This method is used to get new counters to assign to the chains that
	 * constitute the branches of the program. These values are globally unique
	 * and start from the constant CHAIN_INIT.
	 * 
	 * @param submissionId
	 * @return a new ID for a chain.
	 */
	public long getChainCounter(int submissionId) {
		String name = Consts.CHAINCOUNTER_NAME + submissionId;
		synchronized (counter) {
			if (!counter.hasCounter(name)) {
				initializeCounter(name, CHAIN_INIT);
			}
		}
		return getUniqueCounter(name);
	}

	public int getUserBucketNo() {
		String name = Consts.BUCKETCOUNTER_NAME + "-user";
		synchronized (counter) {
			if (!counter.hasCounter(name)) {
				initializeCounter(name, USER_BUCKET_INIT);
			}
		}
		return (int) getUniqueCounter(name);
	}
}
