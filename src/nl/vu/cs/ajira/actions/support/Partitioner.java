package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This is the base class of a partitioner. A <code>Partitioner</code> divides
 * its input into so-called partitions. The {@link #partition(Tuple)} method
 * determines to which partition a tuple is assigned to.
 */
public abstract class Partitioner {

	/** The required number of partitions. */
	protected int npartitions;
	
	/** The fields to use when assigning a tuple to a partition. */
	protected byte[] partition_fields;
	
	/**
	 * This method initializes the partitioner.
	 * @param context
	 * 		the action context in which this partitioner runs
	 * @param npartitions
	 * 		the required number of partitions
	 * @param partition_fields
	 * 		the fields of the tuple that are to be used for partitioning
	 */
	public void init(ActionContext context, int npartitions, byte[] partition_fields) {
		this.npartitions = npartitions;
		this.partition_fields = partition_fields;
	}

	/**
	 * Determines the partition to which the specified tuple is to be assigned.
	 * @param tuple
	 * 		the tuple to be assigned to a partition
	 * @return
	 * 		the partition number
	 * @throws Exception
	 */
	abstract public int partition(Tuple tuple) throws Exception;
}
