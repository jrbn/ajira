package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.storage.container.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the implementation of 
 * the bucket's iterator; allows to traverse/read 
 * all its tuples.
 */
public class BucketIterator extends TupleIterator {
	static final Logger log = LoggerFactory.getLogger(BucketIterator.class);

	WritableContainer<Tuple> tuples = new WritableContainer<Tuple>(true, false,
			Consts.TUPLES_CONTAINER_BUFFER_SIZE);

	Bucket bucket;
	int idSubmission;
	int idBucket;
	Buckets buckets;
	boolean isUsed;

	/**
	 * Initialization function.
	 * 
	 * @param bucket
	 * 		Bucket to iterate on.
	 * @param idSubmission
	 * 		Submission id
	 * @param idBucket
	 * 		Bucket id
	 * @param buckets (not used)
	 * 		Bucket's wrapper class
	 * 		@see Buckets
	 */
	void init(Bucket bucket, int idSubmission, int idBucket, Buckets buckets) {
		tuples.clear();
		this.bucket = bucket;
		this.idSubmission = idSubmission;
		this.idBucket = idBucket;
		this.buckets = buckets;
		this.isUsed = false;
	}

	/**
	 * Next: fetches a chunk from the bucket to iterate
	 * over (the chunk gets stored in the iterator's
	 * buffer, 'tuples')
	 * 
	 * @return	
	 * 		True/false if the buffer still contains 
	 * 		elements or not
	 */
	@Override
	public boolean next() throws Exception {
		isUsed = true;
		
		// If the local buffer is finished, get tuples from the bucket
		if (tuples.getNElements() == 0) {
			long time = System.currentTimeMillis();
			
			bucket.removeChunk(tuples);
			
			if (log.isDebugEnabled()) {
				log.debug("Bucket  " + bucket.getKey() + " delivering "
						+ tuples.getNElements() + " entries, time merging: "
						+ (System.currentTimeMillis() - time));
			}
		}

		return tuples.getNElements() > 0;
	}

	/**
	 * Registers both, this iterator and the chain's notifier
	 * into the bucket. When the bucket is finished the 
	 * iterator will be marked as ready by the chain's
	 * notifier.
	 * 
	 * @param notifier 
	 * 		Chain's notifier
	 */
	@Override
	public void registerReadyNotifier(ChainNotifier notifier) {
		bucket.registerFinishedNotifier(notifier, this);
	}

	/**
	 * Gets a tuple from the iterator's buffer.
	 * 
	 * @param tuple
	 * 		The tuple that is being removed
	 */
	@Override
	public void getTuple(Tuple tuple) throws Exception {
		if (!tuples.remove(tuple))
			throw new Exception("error");
	}
	
	/**
	 * Checks if the bucket is finished.
	 * 
	 * @return
	 * 		True/false wether the bucket is finished
	 * 		or not
	 */
	@Override
	public boolean isReady() {
		return bucket.isFinished();
	}

	@Override
	public String toString() {
		return "Iterator for bucket "
				+ (bucket == null ? "(no bucket yet)" : bucket.getKey())
				+ (tuples == null ? "" : (" tuples.getNElements = " + tuples
						.getNElements()));
	}
}
