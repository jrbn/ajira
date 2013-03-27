package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.data.types.Tuple;

/**
 * A partitioner that partitions according to the hash code of the tuple.
 */
public class HashPartitioner extends Partitioner {

	/**
	 * Determines the partition according to the hash code and the required
	 * number of partitions (see {@link Partitioner#init(nl.vu.cs.ajira.actions.ActionContext, int, byte[])}.
	 * @param tuple
	 * 		the tuple to assign to a partition
	 * @return
	 * 		the partition number.
	 */
	@Override
	public int partition(Tuple tuple) {
		if (partition_fields == null) {
			return (tuple.hashCode() & Integer.MAX_VALUE) % npartitions;
		} else {
			return (tuple.hashCode(partition_fields) & Integer.MAX_VALUE)
					% npartitions;
		}
	}
}