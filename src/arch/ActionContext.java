package arch;

import java.io.IOException;
import java.util.List;

import arch.actions.ActionFactory;
import arch.buckets.Buckets;
import arch.data.types.DataProvider;
import arch.utils.Configuration;

public class ActionContext {

	long chainIDCounter = 100;
	int bucketIDCounter = 100;
	Context context;
	DataProvider dp;

	int nodeId;
	int submissionId;

	boolean isChainRoot;

	public ActionContext(Context context, DataProvider dp) {
		this.context = context;
		this.dp = dp;
	}

	public ActionContext(Context context, DataProvider dp, int nodeId,
			int submissionId) {
		this(context, dp);
		setCurrentChainInfo(nodeId, submissionId);
	}

	public void setCurrentChainInfo(int nodeId, int submissionId) {
		this.nodeId = nodeId;
		this.submissionId = submissionId;
	}

	public Configuration getConfiguration() {
		return context.getConfiguration();
	}

	public Object getObjectFromCache(Object key) {
		return context.getSubmissionCache().getObjectFromCache(submissionId,
				key);
	}

	public Buckets getBuckets() {
		return context.getTuplesBuckets();
	}

	public void putObjectInCache(Object key, Object value) {
		context.getSubmissionCache().putObjectInCache(submissionId, key, value);
	}

	public long getUniqueCounter(String name) throws IOException {
		return context.getUniqueCounter(name);
	}

	public void incrCounter(String counterId, long value) {
		context.getStatisticsCollector().addCounter(nodeId, submissionId,
				counterId, value);
	}

	public ActionFactory getActionsProvider() {
		return context.getActionsProvider();
	}

	public long getNewChainID() {
		return chainIDCounter++;
	}

	public int getNewBucketID() {
		return ++bucketIDCounter;
	}

	public DataProvider getDataProvider() {
		return dp;
	}

	public void broadcastCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			context.getSubmissionCache().broadcastCacheObjects(submissionId,
					keys);
		}
	}

	public List<Object[]> retrieveRemoteCacheObjects(Object... keys) {
		if (context.getNetworkLayer().getNumberNodes() > 1) {
			return context.getSubmissionCache().retrieveCacheObjects(
					submissionId, keys);
		}
		return null;
	}

	public boolean executeRemoteCode(String code) {
		return context.getNetworkLayer().executeRemoteCode(nodeId,
				submissionId, code);
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

	public void setCurrentChainRoot(boolean value) {
		isChainRoot = value;
	}

	public boolean isCurrentChainRoot() {
		return isChainRoot;
	}

	public int getNumberNodes() {
		return context.getNetworkLayer().getNumberNodes();
	}
}
