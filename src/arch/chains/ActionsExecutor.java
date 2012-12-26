package arch.chains;

import java.util.List;

import arch.Context;
import arch.actions.Action;
import arch.actions.ActionConf;
import arch.actions.ActionContext;
import arch.actions.Output;
import arch.data.types.Tuple;
import arch.utils.Consts;

public class ActionsExecutor implements ActionContext, Output {

	private Context context;

	private int[] rawSizes = new int[Consts.MAX_N_ACTIONS];
	private Action[] actions = new Action[Consts.MAX_N_ACTIONS];
	private boolean[] roots = new boolean[Consts.MAX_N_ACTIONS];
	private int nActions;

	private int currentAction;
	private boolean blockProcessing;
	private int submissionNode;
	private int submissionId;

	public ActionsExecutor(Context context) {
		this.context = context;
	}

	public ActionsExecutor(Context context, int submissionNode, int submissionId) {
		this(context);
		init(submissionNode, submissionId);
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
		return (int) getCounter(Consts.BUCKETCOUNTER_NAME);
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

	void init(int submissionNode, int submissionId) {
		blockProcessing = false;
		currentAction = 0;
		this.submissionNode = submissionNode;
		this.submissionId = submissionId;
	}

	void addAction(Action action, boolean root, int chainRawSize) {
		actions[nActions] = action;
		roots[nActions] = root;
		rawSizes[nActions] = chainRawSize;
		nActions++;
	}

	void startProcess() throws Exception {
		currentAction = 0;
		while (currentAction < nActions && !blockProcessing) {
			actions[currentAction++].startProcess(this);
			blockProcessing = actions[currentAction].blockProcessing();
		}
	}

	@Override
	public void output(Tuple tuple) throws Exception {
		actions[currentAction].process(tuple, this, this);
		if (currentAction < nActions - 1) {
			currentAction++;
			output(tuple);
			currentAction--;
		}

	}

	void stopProcess() throws Exception {
		currentAction = 0;
		while (currentAction < nActions
				&& !actions[currentAction].blockProcessing()) {
			actions[currentAction++].stopProcess(this, null);
		}
	}

	@Override
	public boolean isBranchingAllowed() {
		return roots[currentAction];
	}

	@Override
	public void branch(List<ActionConf> actions) throws Exception {
		if (isBranchingAllowed()) {
			// TODO:
		} else {
			throw new Exception("Branching is not allowed");
		}
	}

	/***** TODO: TO CHANGE ******/

	// public Bucket getBucket(int bucketId, String sortingFunction) {
	// return context.getTuplesBuckets().getOrCreateBucket(submissionNode,
	// submissionId, sortingFunction, null);
	// }
	//
	// public Bucket startTransfer(int nodeId, int bucketId, String
	// sortingFunction) {
	// return context.getTuplesBuckets().startTransfer(this.submissionNode,
	// submissionId, nodeId, bucketId, sortingFunction, null);
	// }
	//
	// public void finishTransfer(int nodeId, int bucketId,
	// String sortingFunction, boolean decreaseCounter) throws IOException {
	// context.getTuplesBuckets().finishTransfer(this.submissionNode,
	// submissionId, nodeId, bucketId, chain.getChainId(),
	// chain.getParentChainId(), chain.getChainChildren(),
	// chain.getReplicatedFactor(), isChainRoot, sortingFunction,
	// null, decreaseCounter);
	// }

	boolean getBlockProcessing() {
		return blockProcessing;
	}

	int getNActions() {
		return nActions;
	}
}
