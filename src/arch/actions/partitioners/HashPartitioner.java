package arch.actions.partitioners;

import arch.data.types.Tuple;

public class HashPartitioner extends Partitioner {

	@Override
	protected int partition(Tuple tuple, int nnodes) throws Exception {
		return (tuple.getHash() & Integer.MAX_VALUE) % nnodes;
	}
}
