package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.storage.containers.WritableContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the implementation of the bucket's iterator; allows to
 * traverse/read all its tuples.
 */
public class BucketIterator extends TupleIterator {
	static final Logger log = LoggerFactory.getLogger(BucketIterator.class);

	WritableContainer<WritableTuple> tuples = null;

	long key;
	Bucket bucket;
	boolean isUsed;
	private SimpleData[] signature;
	private WritableTuple serializer = new WritableTuple();
	private boolean done = false;
	private Buckets buckets;

	/**
	 * Initialization function.
	 * 
	 * @param c
	 *            Current action context
	 * @param bucket
	 *            Bucket to iterate on.
	 * @param buckets
	 *            (not used) Bucket's wrapper class
	 * @see Buckets
	 */
	void init(ActionContext c, int idBucket, Buckets buckets) {
		super.init(c, "Buckets");
		this.buckets = buckets;
		key = Buckets.getKey(c.getSubmissionId(), idBucket);
		bucket = buckets.getExistingBucket(key, false);
		if (bucket != null) {
			initWithBucket();
		}
		// Copy serializer of bucket, as it may contain sorting info which
		// affects
		// serialization.
		this.isUsed = false;
		this.done = false;
	}

	private void initWithBucket() {
		serializer = bucket.getSerializer();
		byte[] sig = bucket.getSignature();
		this.signature = new SimpleData[sig.length];
		if (log.isDebugEnabled()) {
			log.debug("initializing iterator for bucket " + bucket.getKey());
		}
		for (int i = 0; i < sig.length; ++i) {
			int j = i;
			if (j >= sig.length) {
				j = sig.length - 1;
			}
			this.signature[i] = DataProvider.get().get(sig[j]);
		}
	}

	/**
	 * Fetches a chunk from the bucket and stores it in the iterator's buffer,
	 * 'tuples' -- fetch & read
	 * 
	 * @see #getTuple
	 * 
	 * @return True/false if the buffer still contains elements or not
	 */
	@Override
	public boolean next() throws Exception {
		isUsed = true;

		// If the local buffer is finished, get tuples from the bucket
		if (tuples == null || tuples.getNElements() == 0) {
			if (tuples != null) {
				bucket.releaseContainer(tuples);
			}
			long time = System.currentTimeMillis();

			tuples = bucket.removeWChunk(null);

			if (log.isDebugEnabled()) {
				log.debug("Bucket  " + bucket.getKey() + " delivering "
						+ tuples.getNElements() + " entries, time merging: "
						+ (System.currentTimeMillis() - time));
			}

			if (tuples.getNElements() == 0) {
				bucket.releaseContainer(tuples);
				tuples = null;
				done = true;
				return false;
			}
		}

		return true;
	}

	/**
	 * Registers both, this iterator and the chain's notifier into the bucket.
	 * When the bucket is finished the iterator will be marked as ready by the
	 * chain's notifier.
	 * 
	 * @param notifier
	 *            Chain's notifier
	 */
	@Override
	public void registerReadyNotifier(ChainNotifier notifier) {
		buckets.registerReadyNotifier(key, notifier, this);
	}

	/**
	 * Gets a tuple from the iterator's buffer.
	 * 
	 * @param tuple
	 *            The tuple that is being removed
	 */
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		if (done) {
			throw new Exception("getTuple() called while next() returned false");
		}

		tuple.set(signature);
		serializer.setTuple(tuple);
		if (!tuples.remove(serializer)) {
			if (log.isDebugEnabled()) {
				log.error("Remove returns false!");
			}
			throw new Exception("Internal error: tuples.remove() returns false");
		}
	}

	/**
	 * Checks if the bucket is finished.
	 * 
	 * @return True/false whether the bucket is finished or not
	 */
	@Override
	public boolean isReady() {
		if (bucket == null) {
			bucket = buckets.getExistingBucket(key, false);
			if (bucket != null) {
				initWithBucket();
			}
		}
		if (bucket == null) {
			return false;
		}
		return bucket.isFinished() || (!bucket.isSorted() && bucket.hasData());
	}

	/**
	 * Returns the associated bucket.
	 * 
	 * @return The bucket
	 */
	public Bucket getBucket() {
		return bucket;
	}

	@Override
	public String toString() {
		return "Iterator for bucket "
				+ (bucket == null ? "(no bucket yet)" : bucket.getKey())
				+ (tuples == null ? "" : (" tuples.getNElements = " + tuples
						.getNElements()));
	}
}
