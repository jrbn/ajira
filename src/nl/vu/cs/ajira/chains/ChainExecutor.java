package nl.vu.cs.ajira.chains;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.datalayer.chainsplits.ChainSplitLayer;
import nl.vu.cs.ajira.datalayer.chainsplits.ChainSplitLayer.SplitIterator;
import nl.vu.cs.ajira.net.NetworkLayer;
import nl.vu.cs.ajira.statistics.StatisticsCollector;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

public class ChainExecutor implements ActionContext, ActionOutput {

	private Context context;
	private StatisticsCollector stats;

	private List<SplitIterator> openedStreams = new ArrayList<>();
	private int[] rawSizes = new int[Consts.MAX_N_ACTIONS];
	private Action[] actions = new Action[Consts.MAX_N_ACTIONS];
	private boolean[] roots = new boolean[Consts.MAX_N_ACTIONS];
	private int nActions;

	// Used for branching
	private int[] cRuntimeBranching = new int[Consts.MAX_N_ACTIONS];
	private int smallestRuntimeAction;
	private boolean stopProcess;
	private TupleIterator itr;

	private int currentAction;
	private int submissionNode;
	private int submissionId;
	private Chain chain;
	private WritableContainer<Chain> chainsBuffer;
	private NetworkLayer net;

	private final Chain supportChain = new Chain();
	private final Tuple supportTuple = TupleFactory.newTuple();

	private boolean transferComputation = false;
	private int transferNodeId;
	private int transferBucketId;
	private boolean localMode;

	public ChainExecutor(Context context, boolean localMode) {
		this.context = context;
		this.localMode = localMode;
		this.chainsBuffer = context.getChainsToProcess();
		this.net = context.getNetworkLayer();
		this.stats = context.getStatisticsCollector();
	}

	public ChainExecutor(Context context, boolean localMode, Chain chain) {
		this(context, localMode);
		init(chain);
	}

	@Override
	public Object getObjectFromCache(Object key) {
		return context.getSubmissionCache().getObjectFromCache(submissionId,
				key);
	}

	@Override
	public void putObjectInCache(Object key, Object value) {
		context.getSubmissionCache().putObjectInCache(submissionId, key, value);
	}

	@Override
	public long getCounter(String counterId) {
		return context.getUniqueCounter(counterId);
	}

	@Override
	public void incrCounter(String counterId, long value) {
		context.getStatisticsCollector().addCounter(submissionNode,
				submissionId, counterId, value);
	}

	@Override
	public int getNewBucketID() {
		return context.getBucketCounter(submissionId);
	}

	@Override
	public List<Object[]> retrieveCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			return context.getSubmissionCache().retrieveCacheObjects(
					submissionId, keys);
		}
		return null;
	}

	@Override
	public void broadcastCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			context.getSubmissionCache().broadcastCacheObjects(submissionId,
					keys);
		}
	}

	@Override
	public boolean isLocalMode() {
		return context.isLocalMode();
	}

	@Override
	public int getMyNodeId() {
		return context.getNetworkLayer().getMyPartition();
	}

	@Override
	public int getNumberNodes() {
		return context.getNetworkLayer().getNumberNodes();
	}

	@Override
	public int getSystemParamInt(String prop, int defaultValue) {
		return context.getConfiguration().getInt(prop, defaultValue);
	}

	@Override
	public boolean getSystemParamBoolean(String prop, boolean defaultValue) {
		return context.getConfiguration().getBoolean(prop, defaultValue);
	}

	@Override
	public String getSystemParamString(String prop, String defaultValue) {
		return context.getConfiguration().get(prop, defaultValue);
	}

	@Override
	public Object getSystemParam(String prop, Object defaultValue) {
		return context.getConfiguration().get(prop, defaultValue);
	}

	void init(Chain chain) {
		nActions = currentAction = 0;
		this.chain = chain;
		this.submissionNode = chain.getSubmissionNode();
		this.submissionId = chain.getSubmissionId();

		this.smallestRuntimeAction = -1;
		this.stopProcess = false;
		this.transferComputation = false;
		openedStreams.clear();
	}

	void addAction(Action action, boolean root, int chainRawSize) {
		actions[nActions] = action;
		roots[nActions] = root;
		rawSizes[nActions] = chainRawSize;
		nActions++;
	}

	void moveComputation(int nodeId, int bucketId) {
		this.transferComputation = true;
		this.transferNodeId = nodeId;
		this.transferBucketId = bucketId;
	}

	void startProcess() throws Exception {
		while (currentAction < nActions) {
			actions[currentAction++].startProcess(this);
		}
		currentAction = -1;
	}

	@Override
	public void output(Tuple tuple) throws Exception {
		if (currentAction < nActions - 1) {
			currentAction++;
			actions[currentAction].process(tuple, this, this);
			currentAction--;
		}
	}

	@Override
	public void output(SimpleData... data) throws Exception {
		supportTuple.set(data);
		output(supportTuple);
	}

	void stopProcess() throws Exception {
		currentAction = 0;
		stopProcess = true;
		while (currentAction < nActions) {
			actions[currentAction].stopProcess(this, this);
			cRuntimeBranching[currentAction] = 0;
			currentAction++;
		}

		if (openedStreams.size() > 0) {
			for (SplitIterator itr : openedStreams) {
				itr.close();
			}
		}

		if (transferComputation) {
			if (roots[nActions - 1]) {
				chain.setRawSize(rawSizes[nActions - 1]);
				chain.copyTo(supportChain);
				// supportChain.setTotalChainChildren(0);
				supportChain.setInputLayer(Consts.BUCKET_INPUT_LAYER_ID);
				supportTuple.set(new TInt(transferBucketId), new TInt(
						transferNodeId));
				supportChain.setInputTuple(supportTuple);
				if (localMode)
					chainsBuffer.add(supportChain);
				else
					net.sendChain(supportChain);
			} else {
				transferComputation = false;
			}
		}
	}

	@Override
	public boolean isPrincipalBranch() {
		return roots[currentAction];
	}

	@Override
	public void branch(List<ActionConf> actions) throws Exception {
		chain.setRawSize(rawSizes[currentAction]);
		chain.branch(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
		supportChain.setActions(actions, this);
		if (!stopProcess && currentAction > 0) {
			cRuntimeBranching[currentAction]++;
			if (currentAction > smallestRuntimeAction) {
				smallestRuntimeAction = currentAction;
			}
		}

		if (localMode)
			chainsBuffer.add(supportChain);
		else
			net.sendChain(supportChain);

		stats.addCounter(chain.getSubmissionNode(), chain.getSubmissionId(),
				"Chains Dynamically Generated", 1);
	}

	@Override
	public void branch(ActionConf action) throws Exception {
		chain.setRawSize(rawSizes[currentAction]);
		chain.branch(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
		supportChain.setAction(action, this);
		if (!stopProcess && currentAction > 0) {
			cRuntimeBranching[currentAction]++;
			if (currentAction > smallestRuntimeAction) {
				smallestRuntimeAction = currentAction;
			}
		}

		if (localMode)
			chainsBuffer.add(supportChain);
		else
			net.sendChain(supportChain);

		stats.addCounter(chain.getSubmissionNode(), chain.getSubmissionId(),
				"Chains Dynamically Generated", 1);
	}

	@Override
	public ActionOutput split(List<ActionConf> actions) throws Exception {
		chain.branchFromRoot(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
		supportChain.setActions(actions, this);
		SplitIterator itr = ChainSplitLayer.getInstance().registerNewSplit();
		supportChain.setInputLayer(Consts.SPLITS_INPUT_LAYER);
		supportChain
				.setInputTuple(TupleFactory.newTuple(new TInt(itr.getId())));
		if (localMode)
			chainsBuffer.add(supportChain);
		else
			net.sendChain(supportChain);
		stats.addCounter(chain.getSubmissionNode(), chain.getSubmissionId(),
				"Chains Dynamically Generated", 1);

		openedStreams.add(itr);

		return itr;
	}

	@Override
	public ActionOutput split(ActionConf action) throws Exception {
		chain.branchFromRoot(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
		supportChain.setAction(action, this);
		SplitIterator itr = ChainSplitLayer.getInstance().registerNewSplit();
		supportChain.setInputLayer(Consts.SPLITS_INPUT_LAYER);
		supportChain
				.setInputTuple(TupleFactory.newTuple(new TInt(itr.getId())));

		stats.addCounter(chain.getSubmissionNode(), chain.getSubmissionId(),
				"Chains Dynamically Generated", 1);

		openedStreams.add(itr);
		if (localMode)
			chainsBuffer.add(supportChain);
		else
			net.sendChain(supportChain);
		return itr;
	}

	@Override
	public Bucket getBucket(final int bucketId, final boolean sort,
			byte[] sortingFields, byte[] signature) {
		return context.getBuckets().getOrCreateBucket(submissionNode,
				submissionId, bucketId, sort, sortingFields, signature);
	}

	@Override
	public Bucket startTransfer(int nodeId, int bucketId, boolean sort,
			byte[] sortingFields, byte[] signature) {
		return context.getBuckets().startTransfer(submissionNode, submissionId,
				nodeId, bucketId, sort, sortingFields, signature, this);
	}

	@Override
	public void finishTransfer(int nodeId, int bucketId, boolean sort,
			byte[] sortingFields, boolean decreaseCounter, byte[] signature)
			throws IOException {

		int children = chain.getTotalChainChildren();
		if (children != 0 && currentAction < smallestRuntimeAction) {
			// Check whether some intermediate nodes after have derived some
			// info. If they do, we need to decrease the counter.
			for (int i = smallestRuntimeAction; i < nActions; ++i) {
				if (currentAction > cRuntimeBranching[i]) {
					children -= cRuntimeBranching[i];
				}
			}
		}

		context.getBuckets().finishTransfer(this.submissionNode, submissionId,
				nodeId, bucketId, chain.getChainId(), chain.getParentChainId(),
				children, roots[currentAction], sort, sortingFields, signature,
				decreaseCounter);
	}

	int getNActions() {
		return nActions;
	}

	@Override
	public int getSubmissionId() {
		return submissionId;
	}

	boolean isChainFullyExecuted() {
		return !transferComputation;
	}

	void setInputIterator(TupleIterator itr) {
		this.itr = itr;
	}

	@Override
	public TupleIterator getInputIterator() {
		return itr;
	}
}
