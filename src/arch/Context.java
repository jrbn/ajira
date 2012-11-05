package arch;

import java.util.List;

import arch.actions.ActionsProvider;
import arch.actions.ControllersProvider;
import arch.buckets.Buckets;
import arch.buckets.CachedFilesMerger;
import arch.chains.Chain;
import arch.chains.ChainHandler;
import arch.chains.ChainNotifier;
import arch.chains.ChainResolver;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.InputLayerRegistry;
import arch.net.NetworkLayer;
import arch.storage.Factory;
import arch.storage.SubmissionCache;
import arch.storage.container.WritableContainer;
import arch.submissions.SubmissionRegistry;
import arch.utils.Configuration;

public class Context {

	private InputLayerRegistry input;
	private Configuration conf;
	private Buckets container;
	private SubmissionRegistry registry;
	private WritableContainer<Chain> chainsToResolve;
	private WritableContainer<Chain> chainsToProcess;
	private NetworkLayer net;
	private StatisticsCollector stats;
	private ActionsProvider actionProvider;
	private ControllersProvider controllersProvider;
	private DataProvider dataProvider;
	private Factory<Tuple> defaultTupleFactory;
	private SubmissionCache cache;
	private ChainNotifier chainNotifier;
	private List<ChainResolver> resolvers;
	private List<ChainHandler> handlers;
	private CachedFilesMerger merger;

	public void init(InputLayerRegistry input, Buckets container,
			SubmissionRegistry registry,
			WritableContainer<Chain> chainsToResolve,
			List<ChainResolver> listResolvers,
			WritableContainer<Chain> chainsToProcess,
			List<ChainHandler> listHandlers, ChainNotifier notifier,
			CachedFilesMerger merger, NetworkLayer net,
			StatisticsCollector stats, ActionsProvider actionProvider,
			ControllersProvider controllersProvider, DataProvider dataProvider,
			Factory<Tuple> defaultTupleFactory, SubmissionCache cache,
			Configuration conf) {
		this.input = input;
		this.conf = conf;
		this.container = container;
		this.registry = registry;
		this.chainsToResolve = chainsToResolve;
		this.chainsToProcess = chainsToProcess;
		this.chainNotifier = notifier;
		this.net = net;
		this.stats = stats;
		this.actionProvider = actionProvider;
		this.controllersProvider = controllersProvider;
		this.dataProvider = dataProvider;
		this.defaultTupleFactory = defaultTupleFactory;
		this.cache = cache;
		this.merger = merger;
		this.resolvers = listResolvers;
		this.handlers = listHandlers;
	}

	public CachedFilesMerger getMergeSortThreadsInfo() {
		return merger;
	}

	public List<ChainResolver> getListChainResolvers() {
		return resolvers;
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

	public WritableContainer<Chain> getChainsToResolve() {
		return chainsToResolve;
	}

	public WritableContainer<Chain> getChainsToProcess() {
		return chainsToProcess;
	}

	public ChainNotifier getChainNotifier() {
		return chainNotifier;
	}

	public ActionsProvider getActionsProvider() {
		return actionProvider;
	}

	public ControllersProvider getControllersProvider() {
		return controllersProvider;
	}

	public DataProvider getDataProvider() {
		return dataProvider;
	}

	public void cleanupSubmission(int submissionNode, int idSubmission) {
		// FIXME: Every node receives this.
		System.exit(1);
	}
}