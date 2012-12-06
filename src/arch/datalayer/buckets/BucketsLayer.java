package arch.datalayer.buckets;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.buckets.BucketIterator;
import arch.chains.Chain;
import arch.chains.ChainLocation;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;
import arch.storage.Factory;

public class BucketsLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(BucketsLayer.class);

	Factory<TInt> intFactory = new Factory<TInt>(TInt.class);

	// Factory<TBoolean> booleanFactory = new Factory<TBoolean>(TBoolean.class);

	@Override
	protected void load(Context context) throws IOException {
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		TupleIterator itr = null;
		try {
			TInt submissionBucket = intFactory.get();
			TInt numberBucket = intFactory.get();
			// TBoolean removeDuplicates = booleanFactory.get();

			tuple.get(submissionBucket, numberBucket/* , removeDuplicates */);

			itr = context.getTuplesBuckets().getIterator(
					submissionBucket.getValue(), numberBucket.getValue()/*
																		 * ,
																		 * removeDuplicates
																		 * .
																		 * getValue
																		 * ()
																		 */);

			intFactory.release(submissionBucket);
			intFactory.release(numberBucket);
			// booleanFactory.release(removeDuplicates);
		} catch (Exception e) {
			log.error("Error retrieving the tuple iterator", e);
		}

		return itr;
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, Chain chain, Context context) {
		try {
			TInt locationBucket = intFactory.get();
			tuple.get(locationBucket, tuple.getNElements() - 1);
			if (locationBucket.getValue() == -1
					|| locationBucket.getValue() == -2) { // All buckets or
				intFactory.release(locationBucket);
				return ChainLocation.ALL_NODES;
			} else {
				intFactory.release(locationBucket);
				return new ChainLocation(locationBucket.getValue());
			}
		} catch (Exception e) {
			log.error("Error parsing tuple", e);
		}

		return null;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		context.getTuplesBuckets().releaseIterator((BucketIterator) itr, false);
	}
}