package nl.vu.cs.ajira;

import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.buckets.CachedFilesMerger;
import nl.vu.cs.ajira.chains.ChainHandlerManager;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputLayerRegistry;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
import nl.vu.cs.ajira.storage.SubmissionCache;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.submissions.SubmissionRegistry;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;
import nl.vu.cs.ajira.utils.UniqueCounter;

/**
 * @author Jacopo Urbani
 * 
 *         This class contains all the variables, objects, constants that might
 *         be useful to the different components of the cluster. In general,
 *         this context is visible within the architecture, but should not
 *         visible to the user, who has access to the ActionContext instead.
 */
public class Context {

	private static final long BUCKET_INIT = 100;
	private static final long CHAIN_INIT = 1;

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
	 * @param chainsToProcess
	 * @param listHandlers
	 * @param notifier
	 * @param merger
	 * @param net
	 * @param stats
	 * @param actionProvider
	 * @param dataProvider
	 * @param defaultTupleFactory
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

	public InputLayer getInputLayer(int idInputLayer) {
		return input.getInputLayer(idInputLayer);
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
	 * Returns an unique counter
	 * 
	 * @param name
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
	 * @return
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
	 * These IDs are globally unique. This method is not accessable to the user.
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
}
