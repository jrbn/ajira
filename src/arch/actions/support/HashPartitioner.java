package arch.actions.support;

import arch.data.types.Tuple;

public class HashPartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple, int nnodes) throws Exception {
		return (tuple.getHash() & Integer.MAX_VALUE) % nnodes;
	}
}
