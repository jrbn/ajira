package nl.vu.cs.ajira.actions.support;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;

public class LocalPartitioner extends Partitioner {

	int myNode;

	@Override
	public void init(ActionContext context) {
		myNode = context.getMyNodeId();
	}

	@Override
	public int partition(Tuple tuple, int nnodes) throws Exception {
		return myNode;
	}
}
