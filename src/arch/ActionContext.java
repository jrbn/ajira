package arch;

import java.io.IOException;
import java.util.List;

import arch.actions.ActionFactory;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.DataProvider;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.net.NetworkLayer;
import arch.storage.Factory;
import arch.submissions.SubmissionRegistry;
import arch.utils.Configuration;

public class ActionContext {

	long chainIDCounter;
	int bucketIDCounter;
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
		this.nodeId = nodeId;
		this.submissionId = submissionId;
	}

	public void setCurrentChain(Chain chain) {
		nodeId = chain.getSubmissionNode();
		submissionId = chain.getSubmissionId();
	}

	public void setStartingChainID(long chainIDCounter) {
		this.chainIDCounter = chainIDCounter;
	}

	public void setStartingBucketID(int bucketIDCounter) {
		this.bucketIDCounter = bucketIDCounter;
	}

	public Factory<Tuple> getDeFaultTupleFactory() {
		return context.getDeFaultTupleFactory();
	}

	public Configuration getConfiguration() {
		return context.getConfiguration();
	}

	public SubmissionRegistry getSubmissionsRegistry() {
		return context.getSubmissionsRegistry();
	}

	public Object getObjectFromCache(Object key) {
		return context.getSubmissionCache().getObjectFromCache(submissionId,
				key);
	}

	public void putObjectInCache(Object key, Object value) {
		context.getSubmissionCache().putObjectInCache(submissionId, key, value);
	}

	public Context getGlobalContext() {
		return context;
	}

	public NetworkLayer getNetworkLayer() {
		return context.getNetworkLayer();
	}

	public Buckets getTuplesBuckets() {
		return context.getTuplesBuckets();
	}

	public InputLayer getInputLayer(int i) {
		return context.getInputLayer(i);
	}

	public long getUniqueCounter(String name) throws IOException {
		return context.getUniqueCounter(name);
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

	public StatisticsCollector getStatisticsCollector() {
		return context.getStatisticsCollector();
	}

	public void incrCounter(String counterId, long value) {
		context.getStatisticsCollector().addCounter(nodeId, submissionId,
				counterId, value);
	}

	public int getSubmissionID() {
		return submissionId;
	}

	public void broadcastCacheObjects(Object... keys) {
		if (getNetworkLayer().getNumberNodes() > 1) {
			context.getSubmissionCache().broadcastCacheObjects(submissionId,
					keys);
		}
	}

	public List<Object[]> retrieveRemoteCacheObjects(Object... keys) {
		if (getNetworkLayer().getNumberNodes() > 1) {
			return context.getSubmissionCache().retrieveCacheObjects(
					submissionId, keys);
		}
		return null;
	}

	public boolean executeRemoteCode(String code) {
		return getNetworkLayer().executeRemoteCode(nodeId, submissionId, code);
	}

	public void sendCacheObject(int node, Object key, Object value) {
		context.getSubmissionCache().sendCacheObject(submissionId, node, key,
				value);
	}

	public void cleanup() {
		context.getTuplesBuckets().clearSubmission(submissionId);
	}

	public Object getMyNodeId() {
		return context.getNetworkLayer().getMyPartition();
	}

	public void setCurrentChainRoot(boolean value) {
		isChainRoot = value;
	}

	public boolean isCurrentChainRoot() {
		return isChainRoot;
	}
}
