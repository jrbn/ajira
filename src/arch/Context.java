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

	public Buckets getTuplesBuckets() {
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

	public void cleanupSubmission(int submissionNode, int idSubmission) {
		// FIXME: Every node receives this.
		System.exit(1);
	}

	public long getUniqueCounter(String name) {
		return counter.getCounter(name);
	}

	public boolean isLocalMode() {
		return localMode;
	}

	public void initializeCounter(String name, long init) {
		counter.init(name, init);
	}

	public void deleteCounter(String name) {
		counter.removeCounter(name);
	}

	public int getBucketCounter(int submissionId) {
		String name = Consts.BUCKETCOUNTER_NAME + submissionId;
		synchronized (counter) {
			if (!counter.hasCounter(name)) {
				initializeCounter(name, BUCKET_INIT);
			}
		}
		return (int) getUniqueCounter(name);
	}

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
