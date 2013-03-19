package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.data.types.Tuple;

public class HashPartitioner extends Partitioner {

	@Override
	public int partition(Tuple tuple, int nnodes, byte[] partition_fields)
			throws Exception {
		if (partition_fields == null) {
			return (tuple.hashCode() & Integer.MAX_VALUE) % nnodes;
		} else {
			return (tuple.hashCode(partition_fields) & Integer.MAX_VALUE)
					% nnodes;
		}
	}
}
