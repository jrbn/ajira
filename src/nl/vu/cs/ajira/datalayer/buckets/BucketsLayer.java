package nl.vu.cs.ajira.datalayer.buckets;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.buckets.BucketIterator;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.chains.ChainLocation;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.storage.Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BucketsLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(BucketsLayer.class);

	Factory<TInt> intFactory = new Factory<TInt>(TInt.class);

	Buckets buckets;

	@Override
	protected void load(Context context) throws IOException {
		buckets = context.getBuckets();
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		TupleIterator itr = null;
		try {
			TInt numberBucket = intFactory.get();
			tuple.get(numberBucket);
			itr = buckets.getIterator(context.getSubmissionId(),
					numberBucket.getValue());
			intFactory.release(numberBucket);
		} catch (Exception e) {
			log.error("Error retrieving the tuple iterator", e);
		}

		return itr;
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, ActionContext context) {
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
		buckets.releaseIterator((BucketIterator) itr);
	}

	@Override
	public String getName() {
		return "BucketsLayer";
	}
}