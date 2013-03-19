package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;

public abstract class Partitioner {

	public void init(ActionContext context) {
	}

	abstract public int partition(Tuple tuple, int nnodes,
			byte[] partition_fields) throws Exception;
}
