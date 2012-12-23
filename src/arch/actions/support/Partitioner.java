package arch.actions.support;

import arch.actions.ActionContext;
import arch.data.types.Tuple;

public abstract class Partitioner {

	public void init(ActionContext context) {
	}

	abstract public int partition(Tuple tuple, int nnodes) throws Exception;
}
