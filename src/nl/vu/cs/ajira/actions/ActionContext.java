package nl.vu.cs.ajira.actions;

import java.io.IOException;
import java.util.List;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.datalayer.TupleIterator;

public interface ActionContext {

	public long getCounter(String counterId);

	public void incrCounter(String counterId, long value);

	public boolean isLocalMode();

	public int getMyNodeId();

	public int getNumberNodes();

	public int getSubmissionId();

	public int getSystemParamInt(String prop, int defaultValue);

	public boolean getSystemParamBoolean(String prop, boolean defaultValue);

	public String getSystemParamString(String prop, String defaultValue);

	public Object getSystemParam(String prop, Object defaultValue);

	public Object getObjectFromCache(Object key);

	public void putObjectInCache(Object key, Object value);

	public List<Object[]> retrieveCacheObjects(Object... keys);

	public void broadcastCacheObjects(Object... keys);

	public int getNewBucketID();

	public boolean isPrincipalBranch();

	// TODO: To remove in something safer
	TupleIterator getInputIterator();

	Bucket getBucket(int bucketId, String sortingFunction, byte[] sortingFields);

	Bucket startTransfer(int nodeId, int bucketId, String sortingFunction,
			byte[] sortingFields);

	void finishTransfer(int nodeId, int bucketId, String sortingFunction,
			byte[] sortingFields, boolean decreaseCounter) throws IOException;
}
