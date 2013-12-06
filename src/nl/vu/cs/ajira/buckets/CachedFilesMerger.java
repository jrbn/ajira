package nl.vu.cs.ajira.buckets;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the threads responsible for cache file merging.
 */
public class CachedFilesMerger implements Runnable {
	static final Logger log = LoggerFactory.getLogger(CachedFilesMerger.class);
	private final List<SortedBucketCache> requests = new ArrayList<SortedBucketCache>();
	public int threads = 0;
	public int activeThreads = 0;

	/**
	 * This method is used to add/register a new request to merge cached files.
	 * 
	 * @param cache
	 *            the requesting cache.
	 */
	public synchronized void newRequest(SortedBucketCache cache) {
		if (log.isDebugEnabled()) {
			log.debug("Got a new request for bucket " + cache.getKey());
		}
		requests.add(cache);
		notify();
	}

	/**
	 * Sets the number of threads to deal with all the requests handled to the
	 * merger.
	 * 
	 * @param threads
	 *            Number of threads
	 */
	public void setNumberThreads(int threads) {
		this.threads = threads;
		this.activeThreads = threads;
	}

	/**
	 * Thread run() method. Starts threads (up to the maximum set number of
	 * threads) that will process the requests to merge cached files.
	 */
	@Override
	public void run() {

		SortedBucketCache cache = null;
		while (true) {

			synchronized (this) {
				// Wait until there is a new request
				while (requests.size() == 0) {
					activeThreads--;
					try {
						wait();
					} catch (InterruptedException e) {
						// Ignore
					}
					activeThreads++;
				}

				cache = requests.remove(0);
			}

			if (log.isDebugEnabled()) {
				log.debug("Handle merge request from bucket " + cache.getKey());
			}

			MetaData result = null;
			try {
				// Request a cache (collection of files) that have
				// to be merged.
				SortedBucketCache f = cache.mergeRequest();
				if (f != null) {
					if (log.isDebugEnabled()) {
						log.debug("Start merging for bucket " + cache.getKey());
					}
					result = f.dump();
					cache.mergeDone(result, null);
					if (log.isDebugEnabled()) {
						log.debug("Done with merging for bucket "
								+ cache.getKey());
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("But got nothing for bucket "
								+ cache.getKey());
					}
				}
			} catch (Throwable e) {
				if (log.isDebugEnabled()) {
					log.debug(
							"Got exception while merging for bucket "
									+ cache.getKey(), e);
				}
				// Pass any exception on to the requester.
				cache.mergeDone(result, e);
			}
		}
	}
}
