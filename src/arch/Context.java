package arch;

import java.util.List;

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
import arch.net.NetworkLayer;
import arch.statistics.StatisticsCollector;
import arch.storage.Factory;
import arch.storage.SubmissionCache;
import arch.storage.container.WritableContainer;
import arch.submissions.Submission;
import arch.submissions.SubmissionRegistry;
import arch.utils.Configuration;
import arch.utils.Consts;
import arch.utils.UniqueCounter;

/**
 * @author Jacopo Urbani
 * 
 *         This class contains all the variables, objects, constants that might
 *         be useful to the different components of the cluster. In general,
 *         this context is visible within the architecture, but should not
 *         visible to the user, who has access to the ActionContext instead.
 */
/**
 * @author Jacopo Urbani
 * 
 */
public class Context {

	private static final long BUCKET_INIT = 100;
	private static final long CHAIN_INIT = 1;

	private boolean localMode;
	private InputLayerRegistry input;
	private Configuration conf;
	private Buckets container;
	private SubmissionRegistry registry;
	private WritableContainer<Chain> chainsToProcess;
	private NetworkLayer net;
	private StatisticsCollector stats;
	private ActionFactory actionProvider;
	private DataProvider dataProvider;
	private Factory<Tuple> defaultTupleFactory;
	private SubmissionCache cache;
	private ChainNotifier chainNotifier;
	private List<ChainHandler> handlers;
	private CachedFilesMerger merger;

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
			WritableContainer<Chain> chainsToProcess,
			List<ChainHandler> listHandlers, ChainNotifier notifier,
			CachedFilesMerger merger, NetworkLayer net,
			StatisticsCollector stats, ActionFactory actionProvider,
			DataProvider dataProvider, Factory<Tuple> defaultTupleFactory,
			SubmissionCache cache, Configuration conf) {
		counter = localMode ? new UniqueCounter() : new UniqueCounter(
				net.getNumberNodes(), net.getMyPartition());

		this.localMode = localMode;
		this.input = input;
		this.conf = conf;
		this.container = container;
		this.registry = registry;
		this.chainsToProcess = chainsToProcess;
		this.chainNotifier = notifier;
		this.net = net;
		this.stats = stats;
		this.actionProvider = actionProvider;
		this.dataProvider = dataProvider;
		this.defaultTupleFactory = defaultTupleFactory;
		this.cache = cache;
		this.merger = merger;
		this.handlers = listHandlers;

		initializeCounter(Consts.BUCKETCOUNTER_NAME, BUCKET_INIT);
		initializeCounter(Consts.CHAINCOUNTER_NAME, CHAIN_INIT);
	}

	public CachedFilesMerger getMergeSortThreadsInfo() {
		return merger;
	}

	public List<ChainHandler> getListChainHandlers() {
		return handlers;
	}

	public SubmissionCache getSubmissionCache() {
		return cache;
	}

	public Factory<Tuple> getDeFaultTupleFactory() {
		return defaultTupleFactory;
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

	public WritableContainer<Chain> getChainsToProcess() {
		return chainsToProcess;
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
