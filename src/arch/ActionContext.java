package arch;

import java.io.IOException;
import java.util.List;

import arch.buckets.Bucket;
import arch.chains.Chain;

public class ActionContext {
	
	Context context;

	int nodeId;
	int submissionId;
	boolean isChainRoot;
	Chain currentChain;

	public ActionContext(Context context) {
		this.context = context;
	}

	public ActionContext(Context context, Chain chain) {
		this(context);
		nodeId = chain.getSubmissionNode();
		submissionId = chain.getSubmissionId();
		currentChain = chain;
	}

	public ActionContext(Context context, int submissionNode, int submissionId) {
		this(context);
		this.nodeId = submissionNode;
		this.submissionId = submissionId;
	}

	public void setCurrentChainRoot(boolean value) {
		isChainRoot = value;
	}

	public Object getObjectFromCache(Object key) {
		return context.getSubmissionCache().getObjectFromCache(submissionId,
				key);
	}

	public void putObjectInCache(Object key, Object value) {
		context.getSubmissionCache().putObjectInCache(submissionId, key, value);
	}

	public long getUniqueCounter(String name) {
		return context.getUniqueCounter(name);
	}

	public void incrCounter(String counterId, long value) {
		context.getStatisticsCollector().addCounter(nodeId, submissionId,
				counterId, value);
	}

	public long getNewChainID() {
		return getUniqueCounter(Context.CHAINCOUNTER_NAME);
	}

	public int getNewBucketID() {
		return (int) getUniqueCounter(Context.BUCKETCOUNTER_NAME);
	}
	
	public List<Object[]> retrieveRemoteCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			return context.getSubmissionCache().retrieveCacheObjects(
					submissionId, keys);
		}
		return null;
	}

	// public boolean executeRemoteCode(String code) {
	// return context.getNetworkLayer().executeRemoteCode(nodeId,
	// submissionId, code);
	// }

	public void broadcastCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			context.getSubmissionCache().broadcastCacheObjects(submissionId,
					keys);
		}
	}

	public void sendCacheObject(int node, Object key, Object value) {
		context.getSubmissionCache().sendCacheObject(submissionId, node, key,
				value);
	}

	public void cleanup() {
		context.getTuplesBuckets().clearSubmission(submissionId);
	}

	public int getMyNodeId() {
		return context.getNetworkLayer().getMyPartition();
	}

	public boolean isLocalMode() {
		return context.isLocalMode();
	}

	public boolean isCurrentChainRoot() {
		return isChainRoot;
	}

	public int getNumberNodes() {
		return context.getNetworkLayer().getNumberNodes();
	}

	public int getParamInt(String prop, int defaultValue) {
		return context.getConfiguration().getInt(prop, defaultValue);
	}

	public Bucket getBucket(int bucketId, String sortingFunction) {
		return context.getTuplesBuckets().getOrCreateBucket(nodeId,
				submissionId, sortingFunction, null);
	}

	public Bucket startTransfer(int nodeId, int bucketId, String sortingFunction) {
		return context.getTuplesBuckets().startTransfer(this.nodeId,
				submissionId, nodeId, bucketId, sortingFunction, null);
	}

	public void finishTransfer(int nodeId, int bucketId,
			String sortingFunction, boolean decreaseCounter) throws IOException {
		context.getTuplesBuckets().finishTransfer(this.nodeId, submissionId,
				nodeId, bucketId, currentChain.getChainId(),
				currentChain.getParentChainId(),
				currentChain.getChainChildren(),
				currentChain.getReplicatedFactor(), isChainRoot,
				sortingFunction, null, decreaseCounter);
	}
}
