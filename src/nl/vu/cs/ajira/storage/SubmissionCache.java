package nl.vu.cs.ajira.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.net.NetworkLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubmissionCache {

	static final Logger log = LoggerFactory.getLogger(SubmissionCache.class);

	Map<Integer, Map<Object, Object>> submissionsCache = new HashMap<Integer, Map<Object, Object>>();
	NetworkLayer net;

	public SubmissionCache(NetworkLayer net) {
		this.net = net;
	}

	public void putObjectInCache(int submissionId, Object key, Object value) {
		Map<Object, Object> sc = null;
		synchronized (submissionsCache) {
			sc = submissionsCache.get(submissionId);
			if (sc == null) {
				sc = new HashMap<Object, Object>();
				submissionsCache.put(submissionId, sc);
				submissionsCache.notifyAll();
			}
		}

		synchronized (sc) {
			if (value == null) {
				if (log.isDebugEnabled()) {
					log.debug("Removing object with key " + key
							+ " from submissionCache");
				}
				sc.remove(key);
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Adding object with key " + key
							+ " to submissionCache");
				}
				sc.put(key, value);
			}
			sc.notifyAll();
		}
	}

	public Object getObjectFromCache(int submissionId, Object key) {
		return getObjectFromCache(submissionId, key, false);
	}

	public Object getObjectFromCache(int submissionId, Object key, boolean wait) {
		if (wait) {
			try {
				Map<Object, Object> sc = null;
				synchronized (submissionsCache) {
					while ((sc = submissionsCache.get(submissionId)) == null)
						submissionsCache.wait();
				}

				Object value = null;
				synchronized (sc) {
					while ((value = sc.get(key)) == null) {
						sc.wait();
					}
				}
				return value;
			} catch (Exception e) {
				log.error("Strange exception", e);
			}

			return null;
		} else {
			Map<Object, Object> sc = null;
			synchronized (submissionsCache) {
				sc = submissionsCache.get(submissionId);
			}

			if (sc != null) {
				synchronized (sc) {
					return sc.get(key);
				}
			}

			return null;
		}
	}

	public void clearAll(int submissionId) {
		Map<Object, Object> map = submissionsCache.remove(submissionId);
		int size = 0;
		if (map != null)
			size = map.size();
		if (size > 0) {
			log.info("Remove " + size + " from the cache since submission "
					+ submissionId + " is finished");
		}
	}

	public void broadcastCacheObjects(int submissionId, Object... keys) {
		Object[] values = new Object[keys.length];
		for (int i = 0; i < keys.length; i++) {
			values[i] = getObjectFromCache(submissionId, keys[i]);
		}
		net.broadcastObjects(submissionId, keys, values);
	}

	public void broadcastCacheObject(int submissionId, Object key) {
		Object value = getObjectFromCache(submissionId, key);
		net.broadcastObject(submissionId, key, value);
	}

	public List<Object[]> retrieveCacheObjects(int submissionId, Object... keys) {
		return net.retrieveObjects(submissionId, keys);
	}

	public void sendCacheObject(int submissionId, int node, Object key,
			Object value) {
		net.sendObject(submissionId, node, key, value);
	}
}
