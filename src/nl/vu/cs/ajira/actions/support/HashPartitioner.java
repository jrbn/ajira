package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.data.types.Tuple;

public class HashPartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple, int nnodes) throws Exception {
		return (tuple.hashCode() & Integer.MAX_VALUE) % nnodes;
	}
}
