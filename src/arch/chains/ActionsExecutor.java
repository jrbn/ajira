package arch.chains;

import java.io.IOException;
import java.util.List;

import arch.Context;
import arch.actions.Action;
import arch.actions.ActionConf;
import arch.actions.ActionContext;
import arch.actions.ActionOutput;
import arch.buckets.Bucket;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ActionsExecutor implements ActionContext, ActionOutput {

	private Context context;

	private int[] rawSizes = new int[Consts.MAX_N_ACTIONS];
	private Action[] actions = new Action[Consts.MAX_N_ACTIONS];
	private boolean[] roots = new boolean[Consts.MAX_N_ACTIONS];
	private int nActions;

	private int currentAction;
	private int submissionNode;
	private int submissionId;
	private Chain chain;
	private WritableContainer<Chain> chainsBuffer;

	private final Chain supportChain = new Chain();

	public ActionsExecutor(Context context,
			WritableContainer<Chain> chainsBuffer) {
		this.context = context;
		this.chainsBuffer = chainsBuffer;
	}

	public ActionsExecutor(Context context,
			WritableContainer<Chain> chainsBuffer, Chain chain) {
		this(context, chainsBuffer);
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

	}

	void addAction(Action action, boolean root, int chainRawSize) {
		actions[nActions] = action;
		roots[nActions] = root;
		rawSizes[nActions] = chainRawSize;
		nActions++;
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

	void stopProcess() throws Exception {
		currentAction = 0;
		while (currentAction < nActions) {
			actions[currentAction].stopProcess(this, this);
			currentAction++;
		}
	}

	@Override
	public boolean isBranchingAllowed() {
		return roots[currentAction]/* && chain.getReplicatedFactor() > 0 */;
	}

	@Override
	public void branch(List<ActionConf> actions) throws Exception {
		if (isBranchingAllowed()) {
			chain.setRawSize(rawSizes[currentAction]);
			chain.branch(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
			supportChain.addActions(actions, this);
			chainsBuffer.add(supportChain);
		} else {
			throw new Exception("Branching is not allowed");
		}
	}

	@Override
	public void branch(ActionConf action) throws Exception {
		if (isBranchingAllowed()) {
			chain.setRawSize(rawSizes[currentAction]);
			chain.branch(supportChain, getCounter(Consts.CHAINCOUNTER_NAME));
			supportChain.addAction(action, this);
			chainsBuffer.add(supportChain);
		} else {
			throw new Exception("Branching is not allowed");
		}
	}

	@Override
	public Bucket getBucket(final int bucketId, final String sortingFunction) {
		return context.getTuplesBuckets().getOrCreateBucket(submissionNode,
				submissionId, bucketId, sortingFunction, null);
	}

	@Override
	public Bucket startTransfer(int nodeId, int bucketId, String sortingFunction) {
		return context.getTuplesBuckets().startTransfer(submissionNode,
				submissionId, nodeId, bucketId, sortingFunction, null, this);
	}

	@Override
	public void finishTransfer(int nodeId, int bucketId,
			String sortingFunction, boolean decreaseCounter) throws IOException {
		context.getTuplesBuckets().finishTransfer(this.submissionNode,
				submissionId, nodeId, bucketId, chain.getChainId(),
				chain.getParentChainId(), chain.getTotalChainChildren(),
				roots[currentAction], sortingFunction, null, decreaseCounter);
	}

	int getNActions() {
		return nActions;
	}

	@Override
	public int getSubmissionId() {
		return submissionId;
	}
}
