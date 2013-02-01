package nl.vu.cs.ajira.buckets;

import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.storage.containers.WritableContainer;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketIterator extends TupleIterator {

	static final Logger log = LoggerFactory.getLogger(BucketIterator.class);

	WritableContainer<SerializedTuple> tuples = new WritableContainer<SerializedTuple>(
			Consts.TUPLES_CONTAINER_BUFFER_SIZE);

	Bucket bucket;
	boolean isUsed;
	private SimpleData[] signature;
	private SerializedTuple serializer = new SerializedTuple();

	void init(Bucket bucket, byte[] signature, Buckets buckets) {
		tuples.clear();
		this.bucket = bucket;
		this.isUsed = false;
		this.signature = new SimpleData[signature.length];
		for (int i = 0; i < signature.length; ++i) {
			this.signature[i] = DataProvider.getInstance().get(signature[i]);
		}
	}

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

	@Override
	public void registerReadyNotifier(ChainNotifier notifier) {
		bucket.registerFinishedNotifier(notifier, this);
	}

	@Override
	public void getTuple(Tuple tuple) throws Exception {
		tuple.set(signature);
		serializer.setTuple(tuple);
		if (!tuples.remove(serializer))
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
