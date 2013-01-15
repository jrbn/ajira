package arch.buckets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.chains.ChainNotifier;
import arch.data.types.Tuple;
import arch.datalayer.TupleIterator;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class BucketIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(BucketIterator.class);

	WritableContainer<Tuple> tuples = new WritableContainer<Tuple>(true, false,
			Consts.TUPLES_CONTAINER_BUFFER_SIZE);

	Bucket bucket;
	int idSubmission;
	int idBucket;
	Buckets buckets;
	boolean isUsed;

	public void init(Bucket bucket, int idSubmission, int idBucket,
			Buckets buckets/* , boolean removeDuplicates */) {
		tuples.clear();
		this.bucket = bucket;
		this.idSubmission = idSubmission;
		this.idBucket = idBucket;
		this.buckets = buckets;
		this.isUsed = false;
	}

	@Override
	public boolean next() throws Exception {
		isUsed = true;
		// If the local buffer is finished, get tuples from the bucket
		if (tuples.getNElements() == 0) {
			long time = System.currentTimeMillis();
			bucket.removeWChunk(tuples);
			if (log.isDebugEnabled()) {
				log.debug("Bucket  " + bucket.getKey() + " delivering "
						+ tuples.getNElements() + " entries, time merging: "
						+ (System.currentTimeMillis() - time));
			}
		}

		return tuples.getNElements() > 0;

	}

	@Override
	public void registerReadyNotifier(ChainNotifier notifier) {
		bucket.registerFinishedNotifier(notifier, this);
	}

	@Override
	public void getTuple(Tuple tuple) throws Exception {
		if (!tuples.remove(tuple))
			throw new Exception("error");
	}

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
