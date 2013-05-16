package nl.vu.cs.ajira.buckets;

import java.util.Arrays;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the implementation of the bucket's iterator; allows to
 * traverse/read all its tuples.
 */
public class BucketIterator extends TupleIterator {
	static final Logger log = LoggerFactory.getLogger(BucketIterator.class);

	WritableContainer<WritableTuple> tuples = new WritableContainer<WritableTuple>(
			Consts.TUPLES_CONTAINER_MAX_BUFFER_SIZE);

	Bucket bucket;
	boolean isUsed;
	private SimpleData[] signature;
	private WritableTuple serializer;

	/**
	 * Initialization function.
	 * 
	 * @param c
	 *            Current action context
	 * @param bucket
	 *            Bucket to iterate on.
	 * @param signature
	 *            The signature used for defining the sort order between the
	 *            fields
	 * @param buckets
	 *            (not used) Bucket's wrapper class
	 * @see Buckets
	 */
	void init(ActionContext c, Bucket bucket, byte[] signature, Buckets buckets) {
		super.init(c, "Buckets");
		tuples.clear();
		this.bucket = bucket;
		this.isUsed = false;
		this.signature = new SimpleData[signature.length];
		if (log.isDebugEnabled()) {
			log.debug("initializing iterator for bucket " + bucket.getKey() + ", signature.length = " + signature.length);
		}
		for (int i = 0; i < signature.length; ++i) {
			this.signature[i] = DataProvider.get().get(signature[i]);
		}
		serializer = bucket.getTupleSerializer();
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
		if (tuples.getNElements() == 0) {
			long time = System.currentTimeMillis();
			tuples.clear();

			bucket.removeWChunk(tuples);

			if (log.isDebugEnabled()) {
				log.debug("Bucket  " + bucket.getKey() + " delivering "
						+ tuples.getNElements() + " entries, time merging: "
						+ (System.currentTimeMillis() - time));
			}
		}

		return tuples.getNElements() > 0;
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
		bucket.registerFinishedNotifier(notifier, this);
	}

	/**
	 * Gets a tuple from the iterator's buffer.
	 * 
	 * @param tuple
	 *            The tuple that is being removed
	 */
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		try {
		tuple.set(signature);
		serializer.setTuple(tuple);
		if (! tuples.remove(serializer)) {
			log.error("Remove returns false!");
			throw new Exception("Internal error");
		}
		/*
		if (log.isDebugEnabled()) {
			log.debug("Tuple is " + tuple.toString());
		}
		*/
		} catch(Exception e) {
			log.error("Bucket = " + bucket.getKey() + ", tuple.nElements = " + tuple.getNElements() + ", signature.length = " + signature.length
					+ ", tuple = " + Arrays.toString(tuple.getSignature()));
			throw e;
		}
	}

	/**
	 * Checks if the bucket is finished.
	 * 
	 * @return True/false whether the bucket is finished or not
	 */
	@Override
	public boolean isReady() {
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
