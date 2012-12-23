package arch.actions;

import java.util.List;

public interface ActionContext {

	public long getCounter(String counterId);

	public void incrCounter(String counterId, long value);

	public int getNewBucketID();

	public boolean isLocalMode();

	public int getMyNodeId();

	public int getNumberNodes();

	public int getSystemParamInt(String prop, int defaultValue);

	public boolean getSystemParamBoolean(String prop, boolean defaultValue);

	public String getSystemParamString(String prop, String defaultValue);

	public Object getSystemParam(String prop, Object defaultValue);

	public Object getObjectFromCache(Object key);

	public void putObjectInCache(Object key, Object value);

	public List<Object[]> retrieveCacheObjects(Object... keys);

	public void broadcastCacheObjects(Object... keys);

}
