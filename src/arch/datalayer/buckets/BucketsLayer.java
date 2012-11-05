package arch.datalayer.buckets;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.buckets.BucketIterator;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;

public class BucketsLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(BucketsLayer.class);

	Factory<TInt> intFactory = new Factory<TInt>(TInt.class);

	@Override
	protected void load(Context context) throws IOException {
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		TupleIterator itr = null;
		try {
			TInt submissionBucket = intFactory.get();
			TInt numberBucket = intFactory.get();

			if (tuple.getNElements() == 4
					&& tuple.getType(2) == submissionBucket.getIdDatatype()) {
				TInt submissionNode = new TInt();
				tuple.get(submissionNode);
				tuple.get(submissionBucket, 1);
				tuple.get(numberBucket, 2);
				itr = context.getTuplesBuckets().getIterator(
						submissionNode.getValue(), submissionBucket.getValue(),
						numberBucket.getValue());
			} else {
				tuple.get(submissionBucket, 0);
				tuple.get(numberBucket, 1);
				itr = context.getTuplesBuckets().getIterator(
						submissionBucket.getValue(), numberBucket.getValue());
			}

			intFactory.release(submissionBucket);
			intFactory.release(numberBucket);
		} catch (Exception e) {
			log.error("Error retrieving the tuple iterator", e);
		}

		return itr;
	}

	@Override
	public int[] getLocations(Tuple tuple, Chain chain, Context context) {
		int[] range = new int[2];

		try {
			TInt locationBucket = intFactory.get();
			tuple.get(locationBucket, tuple.getNElements() - 1);
			if (locationBucket.getValue() == -1
					|| locationBucket.getValue() == -2) { // All buckets or
				// multiple
				range[0] = 0;
				range[1] = context.getNetworkLayer().getNumberNodes() - 1;
			} else {
				range[0] = locationBucket.getValue();
				range[1] = locationBucket.getValue();
			}
			intFactory.release(locationBucket);
		} catch (Exception e) {
			log.error("Error parsing tuple", e);
		}

		return range;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		context.getTuplesBuckets().releaseIterator((BucketIterator) itr, false);
	}
}