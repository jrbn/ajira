package nl.vu.cs.ajira.actions;

import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.submissions.JobProperties;

public interface ActionContext {

	public Context getContext();

	public JobProperties getJobProperties();

	public Class<? extends InputLayer> getInputLayer();

	public void cleanup();

	public long getCounter(String counterId);

	public void incrCounter(String counterId, long value);

	public boolean isLocalMode();

	public int getMyNodeId();

	public int getNumberNodes();

	public int getSubmissionId();

	public int getSystemParamInt(String prop, int defaultValue);

	public boolean getSystemParamBoolean(String prop, boolean defaultValue);

	public String getSystemParamString(String prop, String defaultValue);

	public Object getObjectFromCache(Object key);

	public void putObjectInCache(Object key, Object value);

	public List<Object[]> retrieveCacheObjects(Object... keys);

	public void broadcastCacheObjects(Object... keys) throws IOException;

	public int getNewBucketID();

	public boolean isPrincipalBranch();

	void waitFor(int token);

	void signal(int token) throws IOException;

	// TODO: To remove in something safer
	TupleIterator getInputIterator();

	Bucket getBucket(int bucketId, boolean sort, boolean streaming,
			byte[] sortingFields, byte[] signature);

	Bucket startTransfer(int nodeId, int bucketId, boolean sort,
			byte[] sortingFields, byte[] signature, boolean streaming)
			throws IOException;

	void finishTransfer(int nodeId, int bucketId, boolean sort,
			byte[] sortingFields, boolean decreaseCounter, byte[] signature,
			boolean streaming) throws IOException;
}
