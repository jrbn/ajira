package nl.vu.cs.ajira.datalayer.buckets;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.buckets.BucketIterator;
import nl.vu.cs.ajira.buckets.Buckets;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketsLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(BucketsLayer.class);

	Buckets buckets;

	/**
	 * It initialize the buckets for the current context.
	 */
	@Override
	protected void load(Context context) throws IOException {
		buckets = context.getBuckets();
	}

	/**
	 * Returns the iterator for the buckets.
	 */
	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		return buckets.getIterator(context, ((TInt) tuple.get(0)).getValue());
	}

	/**
	 * Returns the ChainLocation for the location with the value 
	 * of the last element of the tuple. If it does not exist
	 * it constructs a new ChainLocation.
	 */
	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		int location = ((TInt) tuple.get(tuple.getNElements() - 1))
				.getValue();
		if (location == -1 || location == -2) {
			return Location.ALL_NODES;
		} else {
			return new Location(location);
		}
	}

	/**
	 * Releases the the iterator passed through the parameter. 
	 */
	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		buckets.releaseIterator((BucketIterator) itr);
	}
}