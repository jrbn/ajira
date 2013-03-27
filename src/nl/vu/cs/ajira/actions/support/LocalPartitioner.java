package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This partitioner assigns all input tuples to the partition corresponding to
 * the current node.
 */
public class LocalPartitioner extends Partitioner {

	/** Partition number for the current node. */
	private int myNode;

	@Override
	public void init(ActionContext context, int npartitions, byte[] partition_fields) {
		super.init(context, npartitions, partition_fields);
		myNode = context.getMyNodeId();
	}

	@Override
	public int partition(Tuple tuple) {
		return myNode;
	}
}
