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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketsLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(BucketsLayer.class);

	Buckets buckets;

	@Override
	protected void load(Context context) throws IOException {
		buckets = context.getBuckets();
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		TupleIterator itr = null;
		try {
			itr = buckets
					.getIterator(context, ((TInt) tuple.get(0)).getValue());
		} catch (Exception e) {
			log.error("Error retrieving the tuple iterator", e);
		}

		return itr;
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, ActionContext context) {
		try {
			int location = ((TInt) tuple.get(tuple.getNElements() - 1))
					.getValue();
			if (location == -1 || location == -2) {
				return ChainLocation.ALL_NODES;
			} else {
				return new ChainLocation(location);
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
}